import asyncio
import logging
import secrets
import time
import traceback
from contextlib import asynccontextmanager
from typing import Optional

# ==========================================
# GLOBAL LOGGING CONFIGURATION (ROOT)
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True,
)
logger = logging.getLogger("SoundboxGateway")

from fastapi import FastAPI, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse

from app.container import container
from app.core.config import settings
from app.domain.models import AckStatus, Currency, Transaction
from app.services.parser_service import BankNotificationParser


async def mqtt_queue_worker():
    """Consumes incoming MQTT telemetry efficiently without polling."""
    while True:
        try:
            topic, data = await container.mqtt_incoming_queue.get()
            try:
                await container.telemetry_service.handle_packet(topic, data)
            finally:
                container.mqtt_incoming_queue.task_done()
        except asyncio.CancelledError:
            logger.info("MQTT Queue Worker cancelled. Shutting down...")
            break
        except Exception as e:
            logger.error("MQTT Consumer Exception: %s\n%s", e, traceback.format_exc())
            await asyncio.sleep(0.5)


async def transaction_watchdog_worker():
    """
    ពិនិត្យមើល Transactions ដែលហួសកំណត់ ACK Timeout។
    """
    while True:
        try:
            await asyncio.sleep(5.0)
            now = time.time()
            expired = [
                (k, v)
                for k, v in list(container.correlation_registry.items())
                if now - v.get("timestamp", 0) > settings.ack_timeout_seconds
            ]
            for key, meta in expired:
                container.correlation_registry.pop(key, None)
                txid = meta.get("txid")
                if not txid:
                    continue

                current_tx = await container.tx_repo.get_tx_by_id(txid)
                if current_tx and (
                    current_tx.get("is_played") is True
                    or current_tx.get("playback_status") == AckStatus.SPEAKER_PLAYED.value
                ):
                    logger.info("ℹ️ [WATCHDOG] TxID %s has already played via ACK. Timeout skipped.", txid)
                    continue

                await container.tx_repo.mark_play_timeout(txid, timeout_status=AckStatus.TIMEOUT.value)
                logger.warning("⚠️ [WATCHDOG] Transaction %s (Key: %s) marked as %s", txid, key, AckStatus.TIMEOUT.value)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("Watchdog exception: %s", e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await container.initialize()
    worker_task = asyncio.create_task(mqtt_queue_worker())
    watchdog_task = asyncio.create_task(transaction_watchdog_worker())

    yield

    worker_task.cancel()
    watchdog_task.cancel()
    await asyncio.gather(worker_task, watchdog_task, return_exceptions=True)
    await container.shutdown()


app = FastAPI(
    title="OST Soundbox System Gateway",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health", status_code=status.HTTP_200_OK)
async def health():
    return {
        "status": "online",
        "broker": "connected" if container.mqtt_publisher and container.mqtt_publisher.is_connected() else "disconnected",
        "active_correlations": len(container.correlation_registry),
        "timezone": "Asia/Phnom_Penh",
        "timestamp": int(time.time()),
    }


@app.post("/webhook/telegram-userbot", status_code=status.HTTP_200_OK)
async def unified_telegram_webhook(request: Request):
    try:
        payload = await request.json()
        logger.info("📥 [WEBHOOK RECEIVED] Payload: %s", payload)

        msg = payload.get("message") or payload.get("channel_post") or payload

        raw_chat_id = (
            payload.get("chat_id")
            or payload.get("telegram_chat_id")
            or (msg.get("chat", {}).get("id") if isinstance(msg, dict) else "")
        )
        chat_id = str(raw_chat_id).strip()

        raw_text = str(
            payload.get("raw_message")
            or payload.get("text")
            or (msg.get("text") if isinstance(msg, dict) else "")
        ).strip()

        logger.info("🔍 [WEBHOOK EXTRACTED] Chat ID: '%s' | Message Text: '%s'", chat_id, raw_text)

        if not chat_id or not raw_text:
            logger.warning("⚠️ [WEBHOOK IGNORED] Missing chat_id or content")
            return {"status": "ignored", "reason": "Missing chat_id or content"}

        # ១. Parse សារធនាគារ
        tx: Optional[Transaction] = BankNotificationParser.parse(raw_text)
        if not tx:
            logger.warning("⚠️ [WEBHOOK IGNORED] Text does not match bank notification pattern: '%s'", raw_text)
            return {"status": "ignored", "reason": "Not recognized as bank pattern"}

        logger.info("✅ [PARSER SUCCESS] TxID: %s | Amount: %s | Currency: %s", tx.txid, tx.amount, tx.currency)

        # ២. ពិនិត្យស្ទួនតាមរយៈ Redis
        if await container.dedup_service.is_duplicate(tx.txid):
            logger.warning("🛑 [DEDUP IGNORED] Duplicate TxID: %s", tx.txid)
            return {"status": "ignored", "reason": "Duplicate transaction ID"}

        # ៣. Broadcast ទៅកាន់ Soundbox
        sent = await container.broadcast_service.broadcast(tx, chat_id, raw_text=raw_text)
        if not sent:
            await container.dedup_service.release(tx.txid)
            logger.warning("⚠️ [BROADCAST FAILED] No active devices found or dispatch failed for Chat ID: %s", chat_id)
            return {"status": "ignored", "reason": "Broadcast bypassed (no active devices/offline)"}

        curr_val = tx.currency.value if isinstance(tx.currency, Currency) else str(tx.currency)
        logger.info("🔊 [BROADCAST SUCCESS] Dispatched to: %s", sent)

        return {
            "status": "success",
            "broadcast_to": sent,
            "amount": tx.amount,
            "currency": curr_val,
            "txid": tx.txid,
        }

    except Exception as e:
        logger.error("❌ Webhook Exception: %s\n%s", e, traceback.format_exc())
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})


@app.post("/api/devices/{device_sn}/push-static-khqr", status_code=status.HTTP_200_OK)
async def api_push_static_khqr(
    device_sn: str,
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
):
    if not x_api_key or not secrets.compare_digest(x_api_key, settings.api_secret_key):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API Key")

    result = await container.khqr_service.sync_static_khqr(device_sn)
    if not result.get("success"):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=result.get("error"))

    return {"status": "success", "data": result}