import asyncio
import io
import json
import logging
import os
import queue
import time
import traceback
from contextlib import asynccontextmanager
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import asyncpg
import httpx
import qrcode
from dotenv import load_dotenv
from fastapi import (
    BackgroundTasks,
    FastAPI,
    Header,
    HTTPException,
    Request,
    Response,
    status,
)
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings
from starlette.middleware.base import BaseHTTPMiddleware

from app.Telegram.telegram_parser import BankNotificationParser, Transaction, parse_bank_message
from app.Mqtt.mqtt_publisher import SoundboxMQTTPublisher
from app.Redis.dedup import is_duplicate_transaction

load_dotenv()

# ==============================================================================
# 1. ENTERPRISE CONFIGURATION
# ==============================================================================
class Settings(BaseSettings):
    database_url: str = os.getenv("DATABASE_URL", "postgresql://postgres:fDdiFw_KB2930otN@postgres:5432/postgres")
    mqtt_broker: str = os.getenv("MQTT_HOST", os.getenv("MQTT_BROKER", "mosquitto"))
    mqtt_port: int = int(os.getenv("MQTT_PORT", 1883))
    mqtt_user: Optional[str] = os.getenv("MQTT_USER", os.getenv("MQTT_USERNAME", "gateway_user"))
    mqtt_password: Optional[str] = os.getenv("MQTT_PASSWORD", "GatewaySecurePass2026")
    telegram_bot_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    api_secret_key: str = os.getenv("API_SECRET_KEY", "your-api-key")
    cache_ttl: float = 15.0
    http_timeout_seconds: float = 10.0
    ack_timeout_seconds: float = 20.0

    @property
    def telegram_api_url(self) -> str:
        return f"https://api.telegram.org/bot{self.telegram_bot_token}"

    @property
    def bot_id_prefix(self) -> str:
        return self.telegram_bot_token.split(":")[0] if ":" in self.telegram_bot_token else ""

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()

# ==============================================================================
# 2. LOGGING & ENUMS
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s"
)
logger = logging.getLogger("ost_soundbox_enterprise")

class AckStatus(str, Enum):
    PENDING = "PENDING"
    MQTT_DELIVERED = "MQTT_DELIVERED"
    SPEAKER_PLAYED = "SPEAKER_PLAYED"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"

class SenderType(str, Enum):
    BANK_SENDER = "BANK_SENDER"
    AUTHORIZED_ADMIN = "AUTHORIZED_ADMIN"
    UNAUTHORIZED = "UNAUTHORIZED"

# ==============================================================================
# 3. GLOBAL STATE & CORRELATION REGISTRY
# ==============================================================================
db_pool: Optional[asyncpg.Pool] = None
mqtt_pub: Optional[SoundboxMQTTPublisher] = None
http_client: Optional[httpx.AsyncClient] = None
welcome_cache: Dict[str, float] = {}

# Thread-safe telemetry & ACK event queue
mqtt_incoming_queue: queue.Queue = queue.Queue()

# In-Memory Correlation Table: Maps (device_sn, message_id) -> txid
correlation_registry: Dict[str, Dict[str, Any]] = {}


def format_signal_strength(signal_val: Any) -> str:
    try:
        val = int(signal_val)
        if val >= -65:
            return f"Excellent ({val} dBm)"
        elif val >= -75:
            return f"Good ({val} dBm)"
        elif val >= -85:
            return f"Fair ({val} dBm)"
        elif val < -85:
            return f"Poor ({val} dBm)"
    except Exception:
        pass
    return f"{signal_val} dBm" if signal_val else ""


def on_mqtt_message_raw(topic: str, data: dict):
    mqtt_incoming_queue.put((topic, data))


async def mqtt_queue_worker():
    """Consumes telemetry packets and hardware ACKs from MQTT thread."""
    logger.info("⚡ MQTT Event Loop Consumer Worker Started.")
    while True:
        try:
            if not mqtt_incoming_queue.empty():
                topic, data = mqtt_incoming_queue.get_nowait()
                await process_incoming_mqtt_packet(topic, data)
                mqtt_incoming_queue.task_done()
            else:
                await asyncio.sleep(0.05)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Worker Error: {e}\n{traceback.format_exc()}")
            await asyncio.sleep(0.5)


async def transaction_watchdog_worker():
    """Sweeps pending transactions that missed speaker playback ACK within timeout window."""
    logger.info("🛡️ Transaction ACK Watchdog Service Active.")
    while True:
        try:
            await asyncio.sleep(5.0)
            now = time.time()
            expired_keys = []

            for key, meta in list(correlation_registry.items()):
                if now - meta["timestamp"] > settings.ack_timeout_seconds:
                    expired_keys.append((key, meta))

            if expired_keys and db_pool:
                async with db_pool.acquire() as conn:
                    for key, meta in expired_keys:
                        correlation_registry.pop(key, None)
                        # បើមិនទាន់ Speaker Ack ទេ ដំឡើងទៅជា TIMEOUT
                        await conn.execute(
                            """
                            UPDATE transactions 
                            SET ack_status = CASE 
                                    WHEN ack_status = 'MQTT_DELIVERED' THEN 'PLAY_TIMEOUT'
                                    ELSE ack_status 
                                END
                            WHERE txid = $1 AND device_ack = FALSE
                            """,
                            meta["txid"]
                        )
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Watchdog exception: {e}")


async def process_incoming_mqtt_packet(topic: str, data: dict):
    if not db_pool:
        return

    try:
        raw_sn = data.get("device_sn")
        if not raw_sn:
            topic_clean = topic.strip("/").split("/")[0]
            raw_sn = topic_clean if topic_clean != "pubmsg" else None

        device_sn = str(raw_sn).strip() if raw_sn else "unknown"
        packet_type = str(data.get("packet_type", "")).lower()
        content = data.get("content", {})
        msg_id = str(data.get("message_id", "")).strip()

        async with db_pool.acquire() as conn:
            # ១. BOOT TELEMETRY PACKET
            if "device_info" in packet_type or "boot" in packet_type or "battery_percent" in content:
                bat_pct = content.get("battery_percent")
                battery_str = f"{bat_pct}%" if bat_pct is not None else ""
                sig_raw = content.get("signal_value") if content.get("signal_value") is not None else content.get("wifi_signal")
                signal_str = format_signal_strength(sig_raw)
                v_4g = str(content.get("4g_fw_version") or "")
                v_wifi = str(content.get("wifi_fw_version") or "")

                await conn.execute(
                    """
                    INSERT INTO devices (device_id, device_name, is_active, battery, signal, version_4g, version_wifi, last_online, created_at, updated_at)
                    VALUES ($1, $2, TRUE, $3, $4, $5, $6, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ON CONFLICT (device_id) DO UPDATE
                    SET battery = COALESCE(NULLIF(EXCLUDED.battery, ''), devices.battery),
                        signal = COALESCE(NULLIF(EXCLUDED.signal, ''), devices.signal),
                        version_4g = COALESCE(NULLIF(EXCLUDED.version_4g, ''), devices.version_4g),
                        version_wifi = COALESCE(NULLIF(EXCLUDED.version_wifi, ''), devices.version_wifi),
                        last_online = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    device_sn, f"HEMI_{device_sn}", battery_str, signal_str, v_4g, v_wifi
                )
                logger.info(f"📊 [TELEMETRY] Device {device_sn} Synced (Battery: {battery_str}, Signal: {signal_str})")

            # ២. HARDWARE PLAYBACK ACK
            else:
                resp_status = content.get("response_status") or content.get("play_status") or "success"
                is_success = str(resp_status).lower() in ["success", "ok", "0", "true", "play_end", "finish"]

                # ស្វែងរកតាមរយៈ Correlation ID
                registry_key = f"{device_sn}:{msg_id}"
                matched_tx = correlation_registry.pop(registry_key, None)
                target_txid = matched_tx["txid"] if matched_tx else None

                if target_txid:
                    await conn.execute(
                        """
                        UPDATE transactions 
                        SET device_ack = $1,
                            ack_status = $2,
                            ack_at = CURRENT_TIMESTAMP
                        WHERE txid = $3
                        """,
                        is_success, AckStatus.SPEAKER_PLAYED.value if is_success else AckStatus.FAILED.value, target_txid
                    )
                    logger.info(f"🎯 [EXACT MATCH ACK] Hardware Played TxID: {target_txid} on SN: {device_sn}")
                else:
                    # Fallback: Update Transaction ចុងក្រោយបង្អស់របស់ Device នោះ
                    await conn.execute(
                        """
                        UPDATE transactions 
                        SET device_ack = $1,
                            ack_status = $2,
                            ack_at = CURRENT_TIMESTAMP
                        WHERE ctid = (
                            SELECT ctid FROM transactions 
                            WHERE device_id = $3 
                            ORDER BY created_at DESC 
                            LIMIT 1
                        )
                        """,
                        is_success, AckStatus.SPEAKER_PLAYED.value if is_success else AckStatus.FAILED.value, device_sn
                    )
                    logger.info(f"⚡ [FALLBACK ACK] Updated Latest Tx on SN: {device_sn}")

    except Exception as e:
        logger.error(f"Error handling MQTT Packet: {e}\n{traceback.format_exc()}")


# ==============================================================================
# 4. LIFESPAN MANAGEMENT
# ==============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool, mqtt_pub, http_client

    logger.info("🚀 Initializing Production Resource Pools...")
    db_pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=5,
        max_size=30,
        command_timeout=10.0
    )

    http_client = httpx.AsyncClient(
        timeout=settings.http_timeout_seconds,
        limits=httpx.Limits(max_keepalive_connections=30, max_connections=100)
    )

    mqtt_pub = SoundboxMQTTPublisher(
        broker_host=settings.mqtt_broker,
        broker_port=settings.mqtt_port,
        username=settings.mqtt_user,
        password=settings.mqtt_password,
        on_ack_received=on_mqtt_message_raw
    )
    mqtt_pub.connect()

    worker_task = asyncio.create_task(mqtt_queue_worker())
    watchdog_task = asyncio.create_task(transaction_watchdog_worker())

    yield

    worker_task.cancel()
    watchdog_task.cancel()
    await asyncio.gather(worker_task, watchdog_task, return_exceptions=True)

    if mqtt_pub:
        mqtt_pub.disconnect()
    if http_client:
        await http_client.aclose()
    if db_pool:
        await db_pool.close()


app = FastAPI(
    title="OST Enterprise Soundbox Gateway",
    version="2.0.0",
    lifespan=lifespan
)

# ==============================================================================
# 5. CORE TRANSACTION BROADCASTER
# ==============================================================================
async def broadcast_soundbox_notification(tx: Transaction, chat_id: str, raw_text: str = "") -> Optional[List[str]]:
    if not db_pool:
        return None

    # Redis Anti-Duplicate
    try:
        if is_duplicate_transaction(tx.txid):
            logger.warning(f"🛑 Duplicate TxID Ignored: {tx.txid}")
            return None
    except Exception as e:
        logger.error(f"Redis dedup error: {e}")

    async with db_pool.acquire() as conn:
        clean_chat_id = str(chat_id).strip()
        alt_chat_id = clean_chat_id.replace("-100", "-") if clean_chat_id.startswith("-100") else clean_chat_id

        devices = await conn.fetch(
            """
            SELECT device_id, device_name 
            FROM devices 
            WHERE (chat_id = $1 OR chat_id = $2) AND is_active = TRUE
            """,
            clean_chat_id, alt_chat_id
        )

        if not devices:
            logger.warning(f"⚠️ No active device bound to Chat ID: {clean_chat_id}")
            return None

        primary_device_id = devices[0]["device_id"]
        unique_msg_id = str(int(time.time() * 1000))[-10:]

        # 1. Write Initial State (PENDING / MQTT_DELIVERED)
        try:
            await conn.execute(
                """
                INSERT INTO transactions (
                    device_id, txid, chat_id, amount, currency, raw_payload, 
                    device_ack, ack_status, ack_at, created_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, TRUE, $7, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                primary_device_id, str(tx.txid).strip(), clean_chat_id, float(tx.amount), tx.currency, raw_text, AckStatus.MQTT_DELIVERED.value
            )
        except asyncpg.UniqueViolationError:
            logger.warning(f"Transaction {tx.txid} already present in DB.")
            return None

        # 2. Register Correlation Key for Hardware Return Match
        for dev in devices:
            sn = str(dev["device_id"]).strip()
            correlation_registry[f"{sn}:{unique_msg_id}"] = {
                "txid": tx.txid,
                "timestamp": time.time()
            }

        # 3. Parallel Dispatch via MQTT QoS 1
        sent_devices = []
        for dev in devices:
            sn = str(dev["device_id"]).strip()
            topic = f"/LLZN/{sn}"
            payload = {
                "message_id": unique_msg_id,
                "time_stamp": str(int(time.time())),
                "device_sn": sn,
                "packet_type": "payment",
                "content": {
                    "play_payment_amount": float(tx.amount),
                    "currency_type": "USD" if tx.currency == "USD" else "KHR"
                }
            }

            res = await mqtt_pub.publish_voice_payload(topic=topic, payload=payload, qos=1)
            if res.get("success"):
                sent_devices.append(sn)
                logger.info(f"🔊 [BROADCAST SUCCESS] Dispatched to SN {sn} (Latency: {res.get('latency_ms')}ms)")
            else:
                logger.error(f"❌ Failed to dispatch MQTT to SN {sn}: {res.get('error')}")

        return sent_devices

# ==============================================================================
# 6. ROUTERS
# ==============================================================================
@app.get("/health", status_code=status.HTTP_200_OK)
@app.get("/", status_code=status.HTTP_200_OK)
async def health():
    return {
        "status": "online",
        "broker": "connected" if mqtt_pub and mqtt_pub.is_connected() else "disconnected",
        "active_correlations": len(correlation_registry),
        "timestamp": int(time.time())
    }

# @app.post("/webhook/telegram-bot", status_code=status.HTTP_200_OK)


@app.post("/webhook/telegram-userbot", status_code=status.HTTP_200_OK)
async def unified_telegram_webhook(request: Request):
    try:
        payload = await request.json()
        msg = payload.get("message") or payload.get("channel_post") or payload

        chat_id = str(payload.get("chat_id") or payload.get("telegram_chat_id") or (msg.get("chat", {}).get("id") if isinstance(msg, dict) else "")).strip()
        raw_text = str(payload.get("raw_message") or payload.get("text") or (msg.get("text") if isinstance(msg, dict) else "")).strip()

        if not chat_id or not raw_text:
            return {"status": "ignored", "reason": "Missing chat_id or content"}

        tx: Optional[Transaction] = BankNotificationParser.parse(raw_text)
        if not tx:
            return {"status": "ignored", "reason": "Not recognized as bank pattern"}

        sent = await broadcast_soundbox_notification(tx, chat_id, raw_text=raw_text)
        if not sent:
            return {"status": "ignored", "reason": "Broadcast bypassed (duplicate/offline)"}

        return {"status": "success", "broadcast_to": sent, "amount": tx.amount, "currency": tx.currency, "txid": tx.txid}

    except Exception as e:
        logger.error(f"Webhook Exception: {e}\n{traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})