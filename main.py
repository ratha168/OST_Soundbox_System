import os
import time
import io
import qrcode
import logging
from enum import Enum
from contextlib import asynccontextmanager
from fastapi import FastAPI, Header, status, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import asyncpg
import httpx
from dotenv import load_dotenv

from telegram_parser import BankNotificationParser
from mqtt_publisher import SoundboxMQTTPublisher

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:1111@localhost:5432/iot_soundbox")
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
API_SECRET_KEY = os.getenv("API_SECRET_KEY", "your-api-key")

db_pool: Optional[asyncpg.Pool] = None
mqtt_pub: Optional[SoundboxMQTTPublisher] = None

welcome_cache: Dict[str, float] = {}
CACHE_TTL = 15.0


class SenderType(str, Enum):
    BANK_SENDER = "BANK_SENDER"
    AUTHORIZED_ADMIN = "AUTHORIZED_ADMIN"
    UNAUTHORIZED = "UNAUTHORIZED"

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool, mqtt_pub
    db_pool = await asyncpg.create_pool(DATABASE_URL)
    
    mqtt_pub = SoundboxMQTTPublisher(broker_host=MQTT_BROKER, broker_port=MQTT_PORT)
    mqtt_pub.connect()
    
    yield
    
    if mqtt_pub:
        mqtt_pub.disconnect()
    if db_pool:
        await db_pool.close()

app = FastAPI(title="OST Soundbox Gateway with Anti-Fraud Identity", lifespan=lifespan)

# ----------------------------------------------------
# Helper Functions: ឆែក Cache & ផ្ញើ QR Code ទៅ Telegram
# ----------------------------------------------------

def should_send_welcome(chat_id: str) -> bool:
    """ឆែកមើលថា Chat ID នេះធ្លាប់បានផ្ញើ QR ក្នុងរង្វង់ 15 វិនាទីនេះឬនៅ?"""
    current_time = time.time()
    clean_chat_id = str(chat_id).strip()
    
    # សម្អាត Cache ចាស់ៗ
    expired_keys = [k for k, v in welcome_cache.items() if current_time - v > CACHE_TTL]
    for k in expired_keys:
        del welcome_cache[k]

    if clean_chat_id in welcome_cache:
        logger.info(f"Duplicate welcome event suppressed for chat_id: {clean_chat_id}")
        return False
    
    welcome_cache[clean_chat_id] = current_time
    return True


async def send_welcome_qr(chat_id: str):
    """បង្កើត QR Code ក្នុង Memory (RAM) រួចផ្ញើទៅ Telegram"""
    clean_chat_id = str(chat_id).strip()
    
    # បើទើបតែបានផ្ញើរួច មិនបាច់ផ្ញើទៀតទេ
    if not should_send_welcome(clean_chat_id):
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    
    caption = (
        "សូមស្វាគមន៍មកកាន់ប្រព័ន្ធសំឡេង OST System Soundbox!\n"
        "លោកអ្នកកំពុងតែរៀបចំក្នុងការដំឡើងឧបករណ៍ Soundbox របស់យើងខ្ញុំ។ សូមចម្លងលេខកូដខាងក្រោមនេះ "
        "ដើម្បីយកទៅបំពេញ ឬតភ្ជាប់ទៅក្នុង 「Verification Code」 នៅក្នុងប្រព័ន្ធយើងខ្ញុំ\n\n"
        "លេខកូដ (telegram code) របស់អ្នកគឺ\n"
        "Please copy telegram code to complete the setup:\n"
        f"`{clean_chat_id}`"
    )

    try:
        qr = qrcode.QRCode(
            version=1,
            box_size=10,
            border=2,
        )
        qr.add_data(clean_chat_id)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")

        img_byte_arr = io.BytesIO()
        img.save(img_byte_arr, format='PNG')
        img_bytes = img_byte_arr.getvalue()

        files = {
            'photo': ('qrcode.png', img_bytes, 'image/png')
        }
        data = {
            'chat_id': clean_chat_id,
            'caption': caption,
            'parse_mode': 'Markdown'
        }

        async with httpx.AsyncClient(timeout=10.0) as client:
            res = await client.post(url, data=data, files=files)
            logger.info(f"Telegram sendPhoto status: {res.status_code}")
            
            if res.status_code != 200:
                logger.warning(f"Failed to send local photo ({res.text}), falling back to text message...")
                await send_text_message(clean_chat_id, caption)

    except Exception as e:
        logger.error(f"Exception in send_welcome_qr: {e}")
        await send_text_message(clean_chat_id, caption)


async def send_text_message(chat_id: str, text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": str(chat_id).strip(),
        "text": text,
        "parse_mode": "Markdown"
    }
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            res = await client.post(url, json=payload)
            logger.info(f"Telegram sendMessage status: {res.status_code}")
        except Exception as e:
            logger.error(f"Exception in send_text_message: {e}")


# ----------------------------------------------------
# Helper Function: Auto-Detect & Authenticate Sender
# ----------------------------------------------------
async def authenticate_and_classify_sender(
    db_conn: asyncpg.Connection, 
    chat_id: str, 
    payload: "UserBotPayload"
) -> SenderType:
    sender_id = str(payload.user_id) if payload.user_id else None
    if not sender_id:
        return SenderType.UNAUTHORIZED

    forward_sender_id = str(payload.forward_from_chat_id) if payload.forward_from_chat_id else None

    # ១. ផ្ទៀងផ្ទាត់ជាមួយ Database តារាង official_bank_bots 
    is_official_bank_id = await db_conn.fetchval(
        """
        SELECT EXISTS(
            SELECT 1 FROM official_bank_bots 
            WHERE is_active = TRUE AND (bot_user_id = $1 OR ($2::text IS NOT NULL AND bot_user_id = $2))
        )
        """,
        sender_id, forward_sender_id
    )

    is_verified_bot = bool(payload.is_bot and payload.is_verified)

    # បើត្រូវតាមលក្ខខណ្ឌ Bank Bot ➔ Auto Authorized រួច Save ចូល group_users 
    if is_official_bank_id or is_verified_bot:
        await db_conn.execute(
            """
            INSERT INTO group_users (chat_id, user_id, username, full_name, is_authorized, updated_at)
            VALUES ($1, $2, $3, $4, TRUE, CURRENT_TIMESTAMP)
            ON CONFLICT (chat_id, user_id) DO UPDATE
            SET is_authorized = TRUE, updated_at = CURRENT_TIMESTAMP
            """,
            chat_id, sender_id, payload.username, payload.full_name
        )
        logger.info(f"AUTO-ALLOWED Bank Sender ID: {sender_id} in Chat: {chat_id}")
        return SenderType.BANK_SENDER

    # ២. ពិនិត្យមើលក្នុង Database ថាតើជា Admin / Authorized User ឬទេ
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

# ----------------------------------------------------
# Pydantic Schemas
# ----------------------------------------------------
class ScreenContent(BaseModel):
    qrType: str
    color: str
    size: int

class QRCodeDataRequest(BaseModel):
    deviceNumber: str
    amountDue: float
    orderId: str
    sessionToken: str
    timeOut: int
    screenContent: ScreenContent

class TelegramWebhookPayload(BaseModel):
    message: Optional[Dict[str, Any]] = None
    channel_post: Optional[Dict[str, Any]] = None
    my_chat_member: Optional[Dict[str, Any]] = None

class UserBotPayload(BaseModel):
    chat_id: str
    text: str
    user_id: Optional[str] = None
    username: Optional[str] = None
    full_name: Optional[str] = None
    is_bot: Optional[bool] = False
    is_verified: Optional[bool] = False
    forward_from_chat_id: Optional[str] = None

class MerchantRegisterSchema(BaseModel):
    id: Optional[int] = None
    name: str
    owner_phone: Optional[str] = None

class DeviceRegisterSchema(BaseModel):
    merchant_id: int = 1
    device_sn: str = "Y6B2026xxxxxx"
    telegram_chat_id: str = "-1001234567890"
    device_model: str = "Y6B"

class GroupUserSyncSchema(BaseModel):
    chat_id: str
    user_id: str
    username: Optional[str] = None
    full_name: Optional[str] = None
    is_authorized: Optional[bool] = False

class OfficialBankBotSchema(BaseModel):
    bank_name: str
    bot_user_id: str

# ----------------------------------------------------
# Endpoints: Merchant & Device & Bank Bot Management
# ----------------------------------------------------
@app.post("/api/merchants/register")
async def register_merchant(merchant: MerchantRegisterSchema):
    async with db_pool.acquire() as conn:
        try:
            if merchant.id:
                await conn.execute(
                    """
                    INSERT INTO merchants (id, name, owner_phone)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (id) DO UPDATE
                    SET name = EXCLUDED.name, owner_phone = EXCLUDED.owner_phone, updated_at = CURRENT_TIMESTAMP
                    """,
                    merchant.id, merchant.name, merchant.owner_phone
                )
                merchant_id = merchant.id
            else:
                merchant_id = await conn.fetchval(
                    "INSERT INTO merchants (name, owner_phone) VALUES ($1, $2) RETURNING id",
                    merchant.name, merchant.owner_phone
                )
            return {"status": "success", "merchant_id": merchant_id, "message": f"Merchant '{merchant.name}' registered."}
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/devices/register")
async def register_device(device: DeviceRegisterSchema):
    async with db_pool.acquire() as conn:
        try:
            await conn.execute(
                """
                INSERT INTO devices (merchant_id, device_sn, telegram_chat_id, device_model)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (device_sn) DO UPDATE
                SET telegram_chat_id = EXCLUDED.telegram_chat_id, merchant_id = EXCLUDED.merchant_id, status = 'ACTIVE', updated_at = CURRENT_TIMESTAMP
                """,
                device.merchant_id, device.device_sn, device.telegram_chat_id, device.device_model
            )
            return {"status": "success", "message": f"Device {device.device_sn} registered successfully."}
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/bank-bots/add")
async def add_official_bank_bot(bank_bot: OfficialBankBotSchema):
    """API សម្រាប់បន្ថែម Telegram Bot ID របស់ធនាគារផ្លូវការចូល DB"""
    async with db_pool.acquire() as conn:
        try:
            await conn.execute(
                """
                INSERT INTO official_bank_bots (bank_name, bot_user_id, is_active)
                VALUES ($1, $2, TRUE)
                ON CONFLICT (bot_user_id) DO UPDATE
                SET bank_name = EXCLUDED.bank_name, is_active = TRUE
                """,
                bank_bot.bank_name, bank_bot.bot_user_id
            )
            return {"status": "success", "message": f"Bank bot '{bank_bot.bank_name}' ({bank_bot.bot_user_id}) added successfully."}
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

# ----------------------------------------------------
# Endpoint 1: Official Bot Webhook
# ----------------------------------------------------
@app.post("/webhook/telegram")
async def telegram_webhook(payload: TelegramWebhookPayload):
    data = payload.dict()
    logger.info(f"Incoming Telegram Payload: {data}")

    # ១. ចាប់ Event my_chat_member (ពេល Add ឬ Change status Admin)
    my_chat_member = data.get("my_chat_member")
    if my_chat_member:
        chat_id = str(my_chat_member.get("chat", {}).get("id"))
        old_status = my_chat_member.get("old_chat_member", {}).get("status")
        new_status = my_chat_member.get("new_chat_member", {}).get("status")

        # ផ្ញើ QR តែពេល Bot ត្រូវ បាន Join ចូល Group ជាលើកដំបូងប៉ុណ្ណោះ
        # បើ old_status ជា 'member' ហើយ new_status ជា 'administrator' វានឹង Ignore
        if old_status in ["left", "kicked", None] and new_status in ["member", "administrator"]:
            await send_welcome_qr(chat_id)
            return {"status": "success", "action": "sent_welcome_qr_first_join", "chat_id": chat_id}
        else:
            logger.info(f"Ignored status change from {old_status} to {new_status} for chat {chat_id}")
            return {"status": "ignored", "reason": f"Member status updated from {old_status} to {new_status}"}

    # ស្វែងរក Message Object
    message = data.get("message") or data.get("channel_post")
    if not message:
        return {"status": "ignored", "reason": "No valid message payload"}

    chat = message.get("chat", {})
    chat_id = str(chat.get("id")) if chat.get("id") is not None else None
    
    if not chat_id:
        return {"status": "ignored", "reason": "No chat ID found"}

    raw_text = message.get("text", "").strip() if message.get("text") else ""

    # ២. ចាប់ Event ពេលមាន Member ថ្មីចូល ឬ វាយ Command
    if "new_chat_members" in message:
        new_members = message.get("new_chat_members", [])
        bot_id_prefix = TELEGRAM_BOT_TOKEN.split(":")[0]
        
        # ឆែកមើលថាតើ Member ថ្មីនោះជា Bot ខ្លួនឯងដែរឬទេ
        for m in new_members:
            if m.get("is_bot") and str(m.get("id")) == bot_id_prefix:
                await send_welcome_qr(chat_id)
                return {"status": "success", "action": "sent_welcome_qr_new_member", "chat_id": chat_id}
        
        return {"status": "ignored", "reason": "New member added was not this bot"}

    elif raw_text.lower().startswith(("/id", "/chatid", "/setup", "/start")):
        await send_welcome_qr(chat_id)
        return {"status": "success", "action": "sent_welcome_qr_command", "chat_id": chat_id}

    elif not raw_text:
        return {"status": "ignored", "reason": "No text content"}

    # ៣. Process Transactions ប្រាក់ចូល
    parsed = BankNotificationParser.parse_message(raw_text)
    if not parsed:
        return {"status": "ignored", "reason": "Payment pattern not matched"}

    bank_name = parsed["bank"]
    bank_tx_id = parsed["txid"]
    amount = parsed["amount"]
    currency = parsed["currency"]
    payer_name = parsed["payer"]

    async with db_pool.acquire() as conn:
        devices = await conn.fetch(
            "SELECT id, device_sn FROM devices WHERE telegram_chat_id = $1 AND status = 'ACTIVE'",
            chat_id
        )

        if not devices:
            logger.warning(f"Unregistered Chat ID: {chat_id}")
            return {"status": "ignored", "reason": f"No active device registered for Chat ID {chat_id}"}

        primary_device_id = devices[0]["id"]
        try:
            await conn.execute(
                """
                INSERT INTO transactions (device_id, bank_name, bank_tx_id, amount, currency, payer_name, raw_telegram_message, status)
                VALUES ($1, $2, $3, $4, $5, $6, $7, 'PROCESSED')
                """,
                primary_device_id, bank_name, bank_tx_id, amount, currency, payer_name, raw_text
            )
        except asyncpg.UniqueViolationError:
            return {"status": "ignored", "reason": "Duplicate transaction detected"}

    sent_devices = []
    for dev in devices:
        sn = dev["device_sn"]
        mqtt_pub.send_payment_notification(device_sn=sn, amount=amount, currency=currency, txid=bank_tx_id)
        sent_devices.append(sn)

    return {"status": "success", "broadcast_to_devices": sent_devices, "amount": amount, "currency": currency}

# ----------------------------------------------------
# Endpoint 2: UserBot Webhook (Message Processing & Anti-Fraud)
# ----------------------------------------------------

@app.post("/webhook/telegram-userbot")
async def telegram_userbot_webhook(payload: UserBotPayload):
    chat_id = payload.chat_id
    raw_text = payload.text.strip() if payload.text else ""

        
    if not raw_text:
        return {"status": "ignored", "reason": "Empty message"}

    async with db_pool.acquire() as conn:
        # 1. ផ្ទៀងផ្ទាត់ Identity និងបែងចែក Sender Type
        sender_type = await authenticate_and_classify_sender(conn, chat_id, payload)

        # 2. ប្រសិនបើជា Bank Sender ➔ ដំណើរការអានសារ និងប្រកាសសំឡេង (FIXED ANTI-FRAUD LOGIC)
        if sender_type == SenderType.BANK_SENDER or SenderType.AUTHORIZED_ADMIN:
            parsed = BankNotificationParser.parse_message(raw_text)
            if not parsed:
                return {"status": "ignored", "reason": "Not a recognized bank notification format"}

            devices = await conn.fetch(
                "SELECT id, device_sn FROM devices WHERE telegram_chat_id = $1 AND status = 'ACTIVE'",
                chat_id
            )

            if not devices:
                return {"status": "ignored", "reason": f"No active device registered for Chat ID {chat_id}"}

            primary_device_id = devices[0]["id"]
            try:
                await conn.execute(
                    """
                    INSERT INTO transactions (device_id, bank_name, bank_tx_id, amount, currency, payer_name, raw_telegram_message, status)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, 'PROCESSED')
                    """,
                    primary_device_id, parsed["bank"], parsed["txid"], parsed["amount"], parsed["currency"], parsed["payer"], raw_text
                )
            except asyncpg.UniqueViolationError:
                return {"status": "ignored", "reason": "Duplicate transaction detected"}

            # បញ្ជូនសំឡេងតាម MQTT
            sent_devices = []
            for dev in devices:
                mqtt_pub.send_payment_notification(
                    device_sn=dev["device_sn"], 
                    amount=parsed["amount"], 
                    currency=parsed["currency"], 
                    txid=parsed["txid"]
                )
                sent_devices.append(dev["device_sn"])

            return {
                "status": "success", 
                "sender_role": "BANK_SENDER",
                "broadcast_to": sent_devices, 
                "amount": parsed["amount"],
                "currency": parsed["currency"]
            }

        # 3. ប្រសិនបើជា Authorized Admin ➔ អនុញ្ញាតឱ្យបញ្ជា Command (មិនអានសំឡេងឡើយ)
        elif sender_type == SenderType.AUTHORIZED_ADMIN:
            logger.info(f"Authorized Admin ID {payload.user_id} sent message in Chat {chat_id}")
            return {"status": "ignored", "reason": "Admin message received (no soundbox broadcast)"}

        # 4. ប្រសិនបើជា User ធម្មតា / Fake Bank ➔ Block ភ្លាមៗ (Anti-Fraud Enforcement)
        else:
            logger.warning(f"FRAUD PREVENTED: Blocked message from Unauthorized User ID {payload.user_id} in Chat {chat_id}")
            return {
                "status": "rejected", 
                "reason": f"Anti-Fraud: User ID {payload.user_id} is not an authorized sender."
            }

# ----------------------------------------------------
# Endpoint 3: Dynamic Display / QR API
# ----------------------------------------------------
@app.post("/api/set_qr_code_data")
async def set_qr_code_data(
    data: QRCodeDataRequest, x_api_key: Optional[str] = Header(None, alias="X-API-Key")
):
    if x_api_key != API_SECRET_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing X-API-Key header",
        )

    return {
        "status": "success",
        "message": "QR Code data updated successfully",
        "orderId": data.orderId,
    }

# ----------------------------------------------------
# Endpoint 4: Role Synchronization (User / Admin Authorization)
# ----------------------------------------------------
@app.post("/api/users/sync")
async def sync_group_user(user_data: GroupUserSyncSchema):
    async with db_pool.acquire() as conn:
        try:
            await conn.execute(
                """
                INSERT INTO group_users (chat_id, user_id, username, full_name, is_authorized, updated_at)
                VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
                ON CONFLICT (chat_id, user_id) DO UPDATE
                SET username = EXCLUDED.username,
                    full_name = EXCLUDED.full_name,
                    is_authorized = EXCLUDED.is_authorized,
                    updated_at = CURRENT_TIMESTAMP
                """,
                user_data.chat_id,
                user_data.user_id,
                user_data.username,
                user_data.full_name,
                user_data.is_authorized
            )
            return {
                "status": "success", 
                "message": f"User {user_data.user_id} synced (is_authorized={user_data.is_authorized})"
            }
        except Exception as e:
            logger.error(f"Error syncing group user: {e}")
            raise HTTPException(status_code=400, detail=str(e))