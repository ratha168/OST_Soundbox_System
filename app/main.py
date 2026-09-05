import asyncio
import logging
import time
import traceback
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse

from app.container import container
from app.core.config import settings
from app.domain.models import Transaction
from app.services.parser_service import BankNotificationParser

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s"
)
logger = logging.getLogger("SoundboxGateway")


async def mqtt_queue_worker():
    while True:
        try:
            if not container.mqtt_incoming_queue.empty():
                topic, data = container.mqtt_incoming_queue.get_nowait()
                await container.telemetry_service.handle_packet(topic, data)
                container.mqtt_incoming_queue.task_done()
            else:
                await asyncio.sleep(0.05)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"MQTT Consumer Exception: {e}\n{traceback.format_exc()}")
            await asyncio.sleep(0.5)


async def transaction_watchdog_worker():
    while True:
        try:
            await asyncio.sleep(5.0)
            now = time.time()
            expired = [
                (k, v) for k, v in list(container.correlation_registry.items())
                if now - v["timestamp"] > settings.ack_timeout_seconds
            ]
            for key, meta in expired:
                container.correlation_registry.pop(key, None)
                await container.tx_repo.mark_play_timeout(meta["txid"])
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Watchdog exception: {e}")


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
    version="0.0.8",
    lifespan=lifespan
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
        msg = payload.get("message") or payload.get("channel_post") or payload

        chat_id = str(
            payload.get("chat_id")
            or payload.get("telegram_chat_id")
            or (msg.get("chat", {}).get("id") if isinstance(msg, dict) else "")
        ).strip()
        raw_text = str(
            payload.get("raw_message")
            or payload.get("text")
            or (msg.get("text") if isinstance(msg, dict) else "")
        ).strip()

        if not chat_id or not raw_text:
            return {"status": "ignored", "reason": "Missing chat_id or content"}

        # ១. Parser សារធនាគារ (ABA, ACLEDA, CMC)
        tx: Optional[Transaction] = BankNotificationParser.parse(raw_text)
        if not tx:
            return {"status": "ignored", "reason": "Not recognized as bank pattern"}

        # ២. ពិនិត្យស្ទួនតាមរយៈ Redis (Atomic SETNX)
        if container.dedup_service.is_duplicate(tx.txid):
            logger.warning(f"🛑 Duplicate TxID Ignored: {tx.txid}")
            return {"status": "ignored", "reason": "Duplicate transaction ID"}

        # ៣. Broadcast ទៅកាន់ Speaker តាម Protocol (HEMI / Feishu)
        sent = await container.broadcast_service.broadcast(tx, chat_id, raw_text=raw_text)
        if not sent:
            container.dedup_service.release(tx.txid)
            return {"status": "ignored", "reason": "Broadcast bypassed (no active devices/offline)"}

        return {
            "status": "success",
            "broadcast_to": sent,
            "amount": tx.amount,
            "currency": tx.currency,
            "txid": tx.txid,
        }

    except Exception as e:
        logger.error(f"Webhook Exception: {e}\n{traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})


@app.post("/api/devices/{device_sn}/push-static-khqr", status_code=status.HTTP_200_OK)
async def api_push_static_khqr(
    device_sn: str,
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
):
    if x_api_key != settings.api_secret_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API Key")

    result = await container.khqr_service.sync_static_khqr(device_sn)
    if not result.get("success"):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=result.get("error"))

    return {"status": "success", "data": result}