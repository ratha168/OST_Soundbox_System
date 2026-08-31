import asyncio
import io
import json
import logging
import os
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

# ==============================================================================
# SUBFOLDER / MODULE IMPORTS
# ==============================================================================
from app.Telegram.telegram_parser import BankNotificationParser, Transaction, parse_bank_message
from app.Mqtt.mqtt_publisher import SoundboxMQTTPublisher
from app.Redis.dedup import is_duplicate_transaction

load_dotenv()

# ==============================================================================
# 1. APPLICATION SETTINGS & CONFIGURATION
# ==============================================================================
class Settings(BaseSettings):
    database_url: str = os.getenv("DATABASE_URL", "postgresql://postgres:fDdiFw_KB2930otN@postgres:5432/postgres")
    mqtt_broker: str = os.getenv("MQTT_HOST", "mosquitto")
    mqtt_port: int = int(os.getenv("MQTT_PORT", 1883))
    mqtt_user: Optional[str] = os.getenv("MQTT_USER", "gateway_user")
    mqtt_password: Optional[str] = os.getenv("MQTT_PASSWORD", "GatewaySecurePass2026")
    telegram_bot_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    api_secret_key: str = os.getenv("API_SECRET_KEY", "your-api-key")
    cache_ttl: float = 15.0
    http_timeout_seconds: float = 10.0
    app_env: str = "production"

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
# 2. LOGGING CONFIGURATION
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s"
)
logger = logging.getLogger("ost_soundbox_gateway")

# ==============================================================================
# 3. GLOBAL RESOURCES
# ==============================================================================
db_pool: Optional[asyncpg.Pool] = None
mqtt_pub: Optional[SoundboxMQTTPublisher] = None
http_client: Optional[httpx.AsyncClient] = None
welcome_cache: Dict[str, float] = {}


class SenderType(str, Enum):
    BANK_SENDER = "BANK_SENDER"
    AUTHORIZED_ADMIN = "AUTHORIZED_ADMIN"
    UNAUTHORIZED = "UNAUTHORIZED"


# ==============================================================================
# 4. MQTT INCOMING TELEMETRY & ACK HANDLER
# ==============================================================================
async def process_device_telemetry_or_ack(topic: str, data: dict):
    """
    ទទួល និងកត់ត្រា Boot Info (ថ្ម, សេវា, Version) និង ACK Status ពី Topic {SN}/pubmsg
    """
    if not db_pool:
        return

    try:
        # ច្រោះយក Device SN ចេញពី Payload ឬ Topic Name
        raw_sn = data.get("device_sn")
        if not raw_sn:
            topic_clean = topic.strip("/").split("/")[0]
            raw_sn = topic_clean if topic_clean != "pubmsg" else None
        
        device_sn = str(raw_sn).strip() if raw_sn else "unknown"
        packet_type = str(data.get("packet_type", "")).lower()
        content = data.get("content", {})
        msg_id = str(data.get("message_id", "")).strip()

        logger.info(f"📥 Incoming MQTT packet on [{topic}] from SN: {device_sn} (Type: {packet_type})")

        async with db_pool.acquire() as conn:
            # ១. ករណីជា Boot Package / Telemetry Report (ស្ថានភាពថ្ម សេវា Version 4G/WiFi)
            if any(k in packet_type for k in ["boot", "status", "info", "heartbeat"]) or "battery" in content or "bat" in content:
                battery = str(content.get("battery") or content.get("bat") or "")
                signal = str(content.get("signal") or content.get("csq") or content.get("rssi") or "")
                v_4g = str(content.get("version_4g") or content.get("4g_ver") or content.get("4g_version") or "")
                v_wifi = str(content.get("version_wifi") or content.get("wifi_ver") or content.get("wifi_version") or "")

                await conn.execute(
                    """
                    UPDATE devices 
                    SET battery = COALESCE(NULLIF($1, ''), battery),
                        signal = COALESCE(NULLIF($2, ''), signal),
                        version_4g = COALESCE(NULLIF($3, ''), version_4g),
                        version_wifi = COALESCE(NULLIF($4, ''), version_wifi),
                        last_online = CURRENT_TIMESTAMP
                    WHERE device_id = $5
                    """,
                    battery, signal, v_4g, v_wifi, device_sn
                )
                logger.info(f"💾 Updated Device Info for SN {device_sn}: Battery={battery}%, Signal={signal}")

            # ២. ករណីជា ACK សំឡេងទូទាត់ប្រាក់ (Payment Broadcast Response)
            if "payment" in packet_type or "rsp" in packet_type or "response_status" in content:
                resp_status = content.get("response_status", "success")
                is_success = (str(resp_status).lower() == "success")

                await conn.execute(
                    """
                    UPDATE transactions 
                    SET device_ack = $1,
                        ack_status = $2,
                        ack_at = CURRENT_TIMESTAMP
                    WHERE device_id = $3 
                      AND (txid = $4 OR raw_payload LIKE '%' || $4 || '%')
                    """,
                    is_success, resp_status, device_sn, msg_id
                )
                logger.info(f"🔔 Updated Transaction ACK for SN {device_sn} | MsgID: {msg_id} | Status: {resp_status}")

    except Exception as e:
        logger.error(f"Error handling device MQTT packet: {e}\n{traceback.format_exc()}")


def on_mqtt_ack_callback(topic: str, data: dict):
    """
    Bridge ពី Sync Paho-MQTT Thread ទៅកាន់ Async Event Loop
    """
    try:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            asyncio.create_task(process_device_telemetry_or_ack(topic, data))
        else:
            asyncio.run(process_device_telemetry_or_ack(topic, data))
    except Exception as e:
        logger.error(f"MQTT Callback execution error: {e}")


# ==============================================================================
# 5. LIFESPAN MANAGEMENT
# ==============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool, mqtt_pub, http_client

    logger.info("Initializing AsyncPG PostgreSQL pool...")
    try:
        db_pool = await asyncpg.create_pool(
            dsn=settings.database_url,
            min_size=2,
            max_size=20,
            command_timeout=10.0
        )
        logger.info("PostgreSQL Database pool established successfully.")
    except Exception as e:
        logger.critical(f"Failed to connect to PostgreSQL: {e}\n{traceback.format_exc()}")

    logger.info("Initializing persistent Async HTTP Client session pool...")
    try:
        http_client = httpx.AsyncClient(
            timeout=settings.http_timeout_seconds,
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=50)
        )
    except Exception as e:
        logger.error(f"Failed to initialize HTTP client: {e}")

    logger.info(f"Connecting to MQTT Broker at {settings.mqtt_broker}:{settings.mqtt_port}...")
    try:
        mqtt_pub = SoundboxMQTTPublisher(
            broker_host=settings.mqtt_broker,
            broker_port=settings.mqtt_port,
            username=settings.mqtt_user,
            password=settings.mqtt_password,
            on_ack_received=on_mqtt_ack_callback
        )
        mqtt_pub.connect()
        logger.info("MQTT Client initiated connection loop.")
    except Exception as e:
        logger.critical(f"MQTT connection startup error: {e}\n{traceback.format_exc()}")

    yield

    logger.info("Gracefully shutting down gateway services...")
    if mqtt_pub:
        try:
            mqtt_pub.disconnect()
        except Exception as e:
            logger.error(f"Error disconnecting MQTT: {e}")
    if http_client:
        try:
            await http_client.aclose()
        except Exception as e:
            logger.error(f"Error closing HTTP Client: {e}")
    if db_pool:
        try:
            await db_pool.close()
        except Exception as e:
            logger.error(f"Error closing DB pool: {e}")


app = FastAPI(
    title="OST Soundbox System Gateway",
    version="1.0.0",
    lifespan=lifespan
)

# ==============================================================================
# 6. FULL REQUEST / RESPONSE LOGGING MIDDLEWARE
# ==============================================================================
class FullLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        start_time = time.time()
        client_ip = request.client.host if request.client else "unknown"
        method = request.method
        url = str(request.url)

        req_body_bytes = await request.body()
        req_body_str = ""
        if req_body_bytes:
            try:
                req_body_str = req_body_bytes.decode("utf-8")
            except Exception:
                req_body_str = f"<binary data: {len(req_body_bytes)} bytes>"

        logger.info(f">>> [INCOMING REQUEST] {method} {url} | IP: {client_ip} | Body: {req_body_str}")

        try:
            response = await call_next(request)
            process_time = (time.time() - start_time) * 1000.0

            res_body_bytes = b""
            async for chunk in response.body_iterator:
                res_body_bytes += chunk

            res_body_str = ""
            try:
                res_body_str = res_body_bytes.decode("utf-8")
            except Exception:
                res_body_str = f"<binary response: {len(res_body_bytes)} bytes>"

            logger.info(
                f"<<< [RESPONSE] {method} {url} | Status: {response.status_code} "
                f"| Latency: {process_time:.2f}ms | Body: {res_body_str}"
            )

            return Response(
                content=res_body_bytes,
                status_code=response.status_code,
                headers=dict(response.headers),
                media_type=response.media_type
            )

        except Exception as exc:
            process_time = (time.time() - start_time) * 1000.0
            logger.error(
                f"!!! [UNHANDLED EXCEPTION] {method} {url} | Latency: {process_time:.2f}ms "
                f"| Error: {str(exc)}\n{traceback.format_exc()}"
            )
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={"status": "error", "message": "Internal Server Error", "detail": str(exc)}
            )

app.add_middleware(FullLoggingMiddleware)

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    logger.error(f"Validation Error on {request.url}: {exc.errors()} | Body: {exc.body}")
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"status": "error", "message": "Validation Error", "details": exc.errors()}
    )

# ==============================================================================
# 7. PYDANTIC SCHEMAS
# ==============================================================================
class TelegramWebhookPayload(BaseModel):
    message: Optional[Dict[str, Any]] = None
    channel_post: Optional[Dict[str, Any]] = None
    my_chat_member: Optional[Dict[str, Any]] = None

    class Config:
        extra = "ignore"


class UserBotPayload(BaseModel):
    telegram_chat_id: Optional[Any] = None
    chat_id: Optional[Any] = None
    raw_message: Optional[str] = None
    text: Optional[str] = None
    telegram_user_id: Optional[Any] = None
    user_id: Optional[Any] = None
    username: Optional[str] = None
    full_name: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    is_bot: Optional[bool] = False
    is_verified: Optional[bool] = False
    forward_from_chat_id: Optional[Any] = None

    class Config:
        extra = "ignore"

# ==============================================================================
# 8. BUSINESS LOGIC & SERVICES
# ==============================================================================
class TelegramNotificationService:
    @staticmethod
    def normalize_chat_id(chat_id: str | int) -> str:
        c_str = str(chat_id).strip()
        if c_str.startswith("-") and not c_str.startswith("-100"):
            clean_num = c_str.lstrip("-")
            return f"-100{clean_num}"
        return c_str

    @staticmethod
    def should_send_welcome(chat_id: str) -> bool:
        try:
            current_time = time.time()
            clean_chat_id = TelegramNotificationService.normalize_chat_id(chat_id)

            expired = [k for k, v in welcome_cache.items() if current_time - v > settings.cache_ttl]
            for k in expired:
                welcome_cache.pop(k, None)

            if clean_chat_id in welcome_cache:
                logger.info(f"Duplicate welcome event suppressed for chat_id: {clean_chat_id}")
                return False

            welcome_cache[clean_chat_id] = current_time
            return True
        except Exception as e:
            logger.error(f"Error in should_send_welcome check: {e}")
            return True

    @classmethod
    async def send_welcome_qr(cls, chat_id: str):
        clean_chat_id = cls.normalize_chat_id(chat_id)
        if not cls.should_send_welcome(clean_chat_id):
            return

        caption = (
            "សូមស្វាគមន៍មកកាន់ប្រព័ន្ធសំឡេង OST Soundbox System!\n"
            "លោកអ្នកកំពុងតែរៀបចំក្នុងការដំឡើងឧបករណ៍ Soundbox របស់យើងខ្ញុំ។ សូមចម្លងលេខកូដខាងក្រោមនេះ "
            "ដើម្បីយកទៅបំពេញ ឬតភ្ជាប់ទៅក្នុង 「Verification Code」 នៅក្នុងប្រព័ន្ធយើងខ្ញុំ\n\n"
            "លេខកូដ (telegram code) របស់អ្នកគឺ\n"
            "Please copy telegram code to complete the setup:\n"
            f"`{clean_chat_id}`"
        )

        try:
            qr = qrcode.QRCode(version=1, box_size=10, border=2)
            qr.add_data(clean_chat_id)
            qr.make(fit=True)
            img = qr.make_image(fill_color="black", back_color="white")

            img_byte_arr = io.BytesIO()
            img.save(img_byte_arr, format="PNG")
            img_bytes = img_byte_arr.getvalue()

            files = {"photo": ("qrcode.png", img_bytes, "image/png")}
            data = {
                "chat_id": clean_chat_id,
                "caption": caption,
                "parse_mode": "Markdown"
            }

            if http_client:
                res = await http_client.post(
                    f"{settings.telegram_api_url}/sendPhoto",
                    data=data,
                    files=files
                )
                if res.status_code != 200:
                    logger.warning(f"Telegram Photo failed ({res.status_code}: {res.text}), fallback to text...")
                    await cls.send_text_message(clean_chat_id, caption)
                else:
                    logger.info(f"Welcome QR successfully sent to Chat: {clean_chat_id}")
        except Exception as e:
            logger.error(f"Exception in send_welcome_qr: {e}\n{traceback.format_exc()}")
            await cls.send_text_message(clean_chat_id, caption)

    @classmethod
    async def send_text_message(cls, chat_id: str, text: str):
        if not http_client:
            return
        clean_chat_id = cls.normalize_chat_id(chat_id)
        payload = {
            "chat_id": clean_chat_id,
            "text": text,
            "parse_mode": "Markdown"
        }
        try:
            res = await http_client.post(
                f"{settings.telegram_api_url}/sendMessage",
                json=payload
            )
            logger.info(f"Telegram sendMessage status: {res.status_code} for chat: {clean_chat_id}")
            if res.status_code != 200:
                logger.error(f"Telegram sendMessage error detail: {res.text}")
        except Exception as e:
            logger.error(f"Exception in send_text_message: {e}\n{traceback.format_exc()}")


async def authenticate_and_classify_sender(
    db_conn: asyncpg.Connection,
    chat_id: str,
    payload: UserBotPayload
) -> SenderType:
    try:
        sender_id = str(payload.user_id or payload.telegram_user_id or "").strip()
        if not sender_id:
            return SenderType.UNAUTHORIZED

        forward_sender_id = str(payload.forward_from_chat_id).strip() if payload.forward_from_chat_id is not None else None

        # ១. ផ្ទៀងផ្ទាត់ official_bank_bots តាម bot_id
        is_official_bank_id = await db_conn.fetchval(
            """
            SELECT EXISTS(
                SELECT 1 FROM official_bank_bots 
                WHERE is_active = TRUE AND (bot_id = $1 OR ($2::text IS NOT NULL AND bot_id = $2))
            )
            """,
            sender_id,
            forward_sender_id
        )

        is_verified_bot = bool(payload.is_bot and payload.is_verified)

        if is_official_bank_id or is_verified_bot:
            await db_conn.execute(
                """
                INSERT INTO group_users (chat_id, user_id, username, first_name, last_name, full_name, is_bot, is_authorized, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, TRUE, CURRENT_TIMESTAMP)
                ON CONFLICT (chat_id, user_id) DO UPDATE
                SET is_authorized = TRUE,
                    username = EXCLUDED.username,
                    full_name = EXCLUDED.full_name,
                    updated_at = CURRENT_TIMESTAMP
                """,
                chat_id, sender_id, payload.username, payload.first_name, payload.last_name, payload.full_name, bool(payload.is_bot)
            )
            logger.info(f"Identity Verified: AUTO-ALLOWED Bank Sender ID: {sender_id} in Chat: {chat_id}")
            return SenderType.BANK_SENDER

        # ២. ពិនិត្យ Authorized User / Admin
        user_record = await db_conn.fetchrow(
            """
            SELECT is_authorized 
            FROM group_users 
            WHERE chat_id = $1 AND user_id = $2
            """,
            chat_id, sender_id
        )

        if user_record and user_record["is_authorized"]:
            return SenderType.AUTHORIZED_ADMIN

        return SenderType.UNAUTHORIZED
    except Exception as e:
        logger.error(f"Error during sender classification: {e}\n{traceback.format_exc()}")
        return SenderType.UNAUTHORIZED


async def broadcast_soundbox_notification(tx: Transaction, chat_id: str, raw_text: str = "") -> Optional[List[str]]:
    if not db_pool:
        logger.error("Cannot broadcast soundbox: Database pool is None.")
        return None

    # 1. ពិនិត្យ De-duplication តាមរយៈ Redis
    try:
        if is_duplicate_transaction(tx.txid):
            logger.warning(f"DUPLICATE DETECTED (Redis): Suppressed TxID: {tx.txid}")
            return None
    except Exception as e:
        logger.error(f"Redis de-duplication check error: {e}")

    try:
        async with db_pool.acquire() as conn:
            clean_chat_id = str(chat_id).strip()
            devices = await conn.fetch(
                """
                SELECT device_id, device_name 
                FROM devices 
                WHERE chat_id = $1 AND is_active = TRUE
                """,
                clean_chat_id
            )

            if not devices:
                logger.warning(f"No active Soundbox device registered for Telegram Chat ID: {clean_chat_id}")
                return None

            primary_device_id = devices[0]["device_id"]
            unique_msg_id = str(int(time.time() * 1000))[-10:]

            # 2. កត់ត្រា Transaction ចូល PostgreSQL
            try:
                await conn.execute(
                    """
                    INSERT INTO transactions (device_id, txid, chat_id, amount, currency, raw_payload)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    """,
                    primary_device_id, tx.txid, clean_chat_id, float(tx.amount), tx.currency, raw_text
                )
                logger.info(f"Transaction recorded in Database successfully: TxID {tx.txid}")
            except asyncpg.UniqueViolationError:
                logger.warning(f"DUPLICATE DETECTED (DB Unique Key): Transaction {tx.txid} already exists.")
                return None

            # 3. Broadcast ទៅកាន់ HEMI Screen Device តាមស្តង់ដារ Protocol V1.1
            sent_devices = []
            for dev in devices:
                sn = str(dev["device_id"]).strip()
                if mqtt_pub:
                    topic = f"/LLZN/{sn}"
                    payload = {
                        "message_id": unique_msg_id,
                        "time_stamp": str(int(time.time())),
                        "device_sn": sn,
                        "packet_type": "payment",
                        "content": {
                            "play_payment_amount": float(tx.amount)
                        }
                    }

                    res = await mqtt_pub.publish_voice_payload(topic=topic, payload=payload, qos=1)
                    
                    if res.get("success"):
                        sent_devices.append(sn)
                        logger.info(f"HEMI payment broadcast sent to device SN {sn} on {topic} (Latency: {res.get('latency_ms')}ms)")
                    else:
                        logger.error(f"MQTT publish failed to device {sn}: {res.get('message') or res.get('error')}")
                else:
                    logger.error("MQTT Publisher instance is not available.")

            return sent_devices

    except Exception as e:
        logger.error(f"Exception during broadcast soundbox flow: {e}", exc_info=True)
        return None


# ==============================================================================
# 9. ROUTERS & WEBHOOK HANDLERS
# ==============================================================================
@app.get("/health", status_code=status.HTTP_200_OK)
@app.get("/", status_code=status.HTTP_200_OK)
async def health_check():
    db_status = "disconnected"
    if db_pool:
        try:
            async with db_pool.acquire() as conn:
                await conn.execute("SELECT 1")
                db_status = "connected"
        except Exception:
            db_status = "error"

    mqtt_status = "connected" if (mqtt_pub and mqtt_pub.is_connected()) else "disconnected"

    return {
        "status": "healthy" if (db_status == "connected" and mqtt_status == "connected") else "degraded",
        "service": "OST Soundbox Gateway",
        "database": db_status,
        "mqtt": mqtt_status,
        "timestamp": int(time.time())
    }


@app.post("/webhook/telegram-bot", status_code=status.HTTP_200_OK)
async def telegram_webhook(payload: TelegramWebhookPayload, background_tasks: BackgroundTasks):
    try:
        data = payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()

        if not db_pool:
            raise HTTPException(status_code=500, detail="Database connection pool uninitialized")

        async with db_pool.acquire() as conn:
            # ១. EVENT: my_chat_member
            my_chat_member = data.get("my_chat_member")
            if my_chat_member:
                chat_id = str(my_chat_member.get("chat", {}).get("id", "")).strip()
                old_status = my_chat_member.get("old_chat_member", {}).get("status")
                new_status = my_chat_member.get("new_chat_member", {}).get("status")
                from_user = my_chat_member.get("from", {})

                if old_status in ["left", "kicked", None] and new_status in ["member", "administrator"]:
                    background_tasks.add_task(TelegramNotificationService.send_welcome_qr, chat_id)

                    if from_user:
                        admin_id = str(from_user.get("id", "")).strip()
                        admin_username = from_user.get("username")
                        admin_first = from_user.get("first_name", "")
                        admin_last = from_user.get("last_name", "")
                        admin_name = f"{admin_first} {admin_last}".strip()

                        await conn.execute(
                            """
                            INSERT INTO group_users (chat_id, user_id, username, first_name, last_name, full_name, is_bot, is_authorized, updated_at)
                            VALUES ($1, $2, $3, $4, $5, $6, FALSE, FALSE, CURRENT_TIMESTAMP)
                            ON CONFLICT (chat_id, user_id) DO UPDATE
                            SET is_authorized = FALSE,
                                username = EXCLUDED.username,
                                first_name = EXCLUDED.first_name,
                                last_name = EXCLUDED.last_name,
                                full_name = EXCLUDED.full_name,
                                updated_at = CURRENT_TIMESTAMP
                            """,
                            chat_id, admin_id, admin_username, admin_first, admin_last, admin_name
                        )
                        logger.info(f"Auto-synced Group Admin ID {admin_id} to Chat {chat_id} (is_authorized=FALSE)")

                    return {"status": "success", "action": "sent_welcome_qr_and_synced_admin", "chat_id": chat_id}
                else:
                    return {"status": "ignored", "reason": f"Member status updated from {old_status} to {new_status}"}

            # ២. ទាញយក MESSAGE ឬ CHANNEL_POST
            message = data.get("message") or data.get("channel_post")
            if not message:
                return {"status": "ignored", "reason": "No valid message payload"}

            chat_id = str(message.get("chat", {}).get("id", "")).strip()
            if not chat_id:
                return {"status": "ignored", "reason": "No chat ID found"}

            from_user = message.get("from", {})
            raw_text = str(message.get("text", "")).strip()

            # ៣. EVENT: new_chat_members
            if "new_chat_members" in message:
                new_members = message.get("new_chat_members", [])
                bot_prefix = settings.bot_id_prefix
                users_to_insert = []

                for m in new_members:
                    m_id = str(m.get("id", "")).strip()
                    m_username = m.get("username")
                    m_first = m.get("first_name", "")
                    m_last = m.get("last_name", "")
                    m_name = f"{m_first} {m_last}".strip()
                    is_bot_user = bool(m.get("is_bot", False))

                    if is_bot_user and m_id == bot_prefix:
                        background_tasks.add_task(TelegramNotificationService.send_welcome_qr, chat_id)
                    else:
                        users_to_insert.append((chat_id, m_id, m_username, m_first, m_last, m_name, is_bot_user))

                if users_to_insert:
                    await conn.executemany(
                        """
                        INSERT INTO group_users (chat_id, user_id, username, first_name, last_name, full_name, is_bot, is_authorized, updated_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, FALSE, CURRENT_TIMESTAMP)
                        ON CONFLICT (chat_id, user_id) DO NOTHING
                        """,
                        users_to_insert
                    )
                    logger.info(f"Batch auto-synced {len(users_to_insert)} new members in Chat {chat_id} (is_authorized=FALSE)")

                return {"status": "success", "action": "processed_new_chat_members", "count": len(users_to_insert)}

            # Auto-Sync អ្នកដែលផ្ញើសារទូទៅចូល DB
            if from_user and not from_user.get("is_bot"):
                sender_id = str(from_user.get("id", "")).strip()
                s_first = from_user.get("first_name", "")
                s_last = from_user.get("last_name", "")
                s_name = f"{s_first} {s_last}".strip()

                await conn.execute(
                    """
                    INSERT INTO group_users (chat_id, user_id, username, first_name, last_name, full_name, is_bot, is_authorized, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, FALSE, FALSE, CURRENT_TIMESTAMP)
                    ON CONFLICT (chat_id, user_id) DO NOTHING
                    """,
                    chat_id, sender_id, from_user.get("username"), s_first, s_last, s_name
                )

            # ៤. EVENT: COMMANDS
            if raw_text.lower().startswith(("/id", "/chatid", "/setup", "/start", "/pay")):
                background_tasks.add_task(TelegramNotificationService.send_welcome_qr, chat_id)
                return {"status": "success", "action": "sent_welcome_qr_command", "chat_id": chat_id}

            if not raw_text:
                return {"status": "ignored", "reason": "No text content"}

            # ៥. PARSE & BROADCAST TRANSACTION
            tx: Optional[Transaction] = BankNotificationParser.parse(raw_text)
            if not tx:
                return {"status": "ignored", "reason": "Payment pattern not matched"}

            sent_devices = await broadcast_soundbox_notification(tx, chat_id, raw_text=raw_text)
            if sent_devices is None:
                return {"status": "ignored", "reason": "No active devices or duplicate transaction"}

            return {
                "status": "success",
                "broadcast_to_devices": sent_devices,
                "amount": tx.amount,
                "currency": tx.currency,
                "txid": tx.txid
            }

    except Exception as e:
        logger.error(f"Error processing Telegram webhook: {e}\n{traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})


@app.post("/webhook/telegram-userbot", status_code=status.HTTP_200_OK)
async def telegram_userbot_webhook(request: Request):
    try:
        payload = await request.json()

        chat_id = str(payload.get("chat_id") or payload.get("telegram_chat_id") or "").strip()
        raw_text = str(payload.get("raw_message") or payload.get("text") or "").strip()
        user_id = str(payload.get("user_id") or payload.get("telegram_user_id") or "").strip()

        if not chat_id:
            return {"status": "ignored", "reason": "Missing chat_id/telegram_chat_id"}

        if not raw_text:
            return {"status": "ignored", "reason": "Empty message"}

        if not db_pool:
            raise HTTPException(status_code=500, detail="Database pool not ready")

        userbot_obj = UserBotPayload(
            chat_id=chat_id,
            text=raw_text,
            user_id=user_id,
            username=payload.get("username"),
            first_name=payload.get("first_name"),
            last_name=payload.get("last_name"),
            full_name=payload.get("full_name"),
            is_bot=payload.get("is_bot", False),
            is_verified=payload.get("is_verified", False),
            forward_from_chat_id=payload.get("forward_from_chat_id")
        )

        async with db_pool.acquire() as conn:
            # ១. ផ្ទៀងផ្ទាត់ Identity & Anti-Fraud
            sender_type = await authenticate_and_classify_sender(conn, chat_id, userbot_obj)

            # ២. Allow Bank Sender ឬ Authorized Admin
            if sender_type in [SenderType.BANK_SENDER, SenderType.AUTHORIZED_ADMIN]:
                tx: Optional[Transaction] = BankNotificationParser.parse(raw_text)
                if not tx:
                    return {"status": "ignored", "reason": "Not a recognized bank notification format"}

                sent_devices = await broadcast_soundbox_notification(tx, chat_id, raw_text=raw_text)
                if sent_devices is None:
                    return {"status": "ignored", "reason": "Device inactive or duplicate transaction"}

                return {
                    "status": "success",
                    "sender_role": sender_type.value,
                    "broadcast_to": sent_devices,
                    "amount": tx.amount,
                    "currency": tx.currency,
                    "txid": tx.txid
                }

            # ៣. Block Unauthorized Sender
            else:
                logger.warning(f"FRAUD PREVENTED: Blocked unauthorized sender ID {user_id} in Chat {chat_id}")
                return {
                    "status": "rejected",
                    "reason": f"Anti-Fraud: Sender ID {user_id} is not authorized."
                }

    except Exception as e:
        logger.error(f"Error handling Telegram userbot webhook: {e}\n{traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})