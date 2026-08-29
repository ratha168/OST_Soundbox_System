from fastapi import FastAPI, Request, HTTPException, Header, Depends
import asyncpg
import os
import logging
from contextlib import asynccontextmanager
from pydantic import BaseModel, Field
from typing import Optional

from app.telegram_parser import AdvancedBankNotificationParser
from app.mqtt_publisher import AdvancedSoundboxMQTTPublisher

# --- LOGGING CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("IoT_FastAPI")

# --- ENVIRONMENT CONFIGURATIONS ---
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:fDdiFw_KB2930otN@postgres:5432/postgres")
API_SECRET_KEY = os.getenv("API_SECRET_KEY", "your-api-key")
MQTT_BROKER = os.getenv("MQTT_BROKER", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))

db_pool = None
mqtt_publisher = None

# --- LIFESPAN (DATABASE POOL & MQTT MANAGEMENT) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool, mqtt_publisher
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
        logger.info("PostgreSQL database connection pool established successfully.")
        
        try:
            mqtt_publisher = AdvancedSoundboxMQTTPublisher(
                broker_host=MQTT_BROKER,
                broker_port=MQTT_PORT,
                client_id="fastapi_soundbox_gateway"
            )
            mqtt_publisher.connect()
            logger.info("MQTT Publisher initialized and running.")
        except Exception as me:
            logger.warning(f"MQTT Broker connection warning (will continue): {me}")

        yield
    except Exception as e:
        logger.error(f"Failed during lifespan startup: {e}")
        raise e
    finally:
        if mqtt_publisher:
            try:
                mqtt_publisher.disconnect()
            except Exception:
                pass
        if db_pool:
            await db_pool.close()
            logger.info("PostgreSQL database connection pool closed.")

app = FastAPI(
    title="IoT Soundbox & Telegram Gateway API",
    version="2.0.0",
    lifespan=lifespan
)

# --- PYDANTIC SCHEMAS (VALIDATION) ---
class UserSyncSchema(BaseModel):
    chat_id: str = Field(..., description="Telegram Chat/Group ID")
    user_id: str = Field(..., description="Telegram User ID")
    username: Optional[str] = Field(None, description="Telegram Username")
    full_name: Optional[str] = Field(None, description="User Full Name")
    is_authorized: bool = Field(False, description="Authorization status")

class UserbotMessageSchema(BaseModel):
    chat_id: str
    text: str
    user_id: Optional[str] = None
    username: Optional[str] = None
    full_name: Optional[str] = None
    is_bot: bool = False
    is_verified: bool = False
    forward_from_chat_id: Optional[str] = None

# --- SECURITY DEPENDENCY ---
async def verify_api_key(x_api_key: str = Header(None)):
    if not x_api_key or x_api_key != API_SECRET_KEY:
        logger.warning("Unauthorized access attempt with invalid or missing API Key.")
        raise HTTPException(status_code=403, detail="Forbidden: Invalid or missing API Key")
    return x_api_key

# --- API ENDPOINTS ---

@app.get("/health", dependencies=[Depends(verify_api_key)])
async def health_check():
    """ពិនិត្យស្ថានភាពដំណើរការរបស់ API និង Database Connection"""
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    try:
        async with db_pool.acquire() as connection:
            await connection.fetchval("SELECT 1")
        return {
            "status": "healthy", 
            "database": "connected",
            "mqtt": "connected" if mqtt_publisher and mqtt_publisher.is_connected() else "disconnected"
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/webhook/telegram-userbot", dependencies=[Depends(verify_api_key)])
async def telegram_userbot_webhook(payload: UserbotMessageSchema):
    """ទទួលព្រឹត្តិការណ៍សារ និងទិន្នន័យផ្សេងៗពី Userbot រួច Parse និងបញ្ជូនទៅ MQTT Soundbox"""
    logger.info(f"Userbot Message Received -> Chat: {payload.chat_id} | Sender: {payload.user_id}")
    
    # 1. Parse Bank Transaction Notification
    parsed = AdvancedBankNotificationParser.parse_message(payload.text)
    if not parsed:
        logger.debug(f"Message in Chat {payload.chat_id} is not a recognizable bank transaction.")
        return {
            "status": "ignored", 
            "action": "non_transaction_message",
            "chat_id": payload.chat_id
        }

    logger.info(
        f"💳 Bank Notification Parsed -> Bank: {parsed['bank']} | "
        f"Amount: {parsed['amount']} {parsed['currency']} | TxID: {parsed['txid']} | Payer: {parsed['payer']}"
    )

    # 2. Database Lookup & Deduplication
    if not db_pool:
        # Fallback if DB pool is unavailable: Broadcast directly
        if mqtt_publisher:
            mqtt_publisher.broadcast_payment_notification(
                amount=parsed["amount"],
                currency=parsed["currency"],
                txid=parsed["txid"]
            )
        return {
            "status": "success",
            "action": "broadcast_without_db",
            "transaction": parsed
        }

    try:
        async with db_pool.acquire() as conn:
            # Query for linked active soundbox device by telegram_chat_id
            device = await conn.fetchrow(
                "SELECT id, device_sn, status FROM devices WHERE telegram_chat_id = $1 AND status = 'ACTIVE'",
                payload.chat_id
            )

            if device:
                device_id = device["id"]
                device_sn = device["device_sn"]

                # Record transaction and prevent duplicate payment alerts
                tx_record = await conn.fetchrow(
                    """
                    INSERT INTO transactions (device_id, bank_name, bank_tx_id, amount, currency, payer_name, raw_telegram_message, status)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, 'PROCESSED')
                    ON CONFLICT (bank_name, bank_tx_id) DO NOTHING
                    RETURNING id
                    """,
                    device_id,
                    parsed["bank"],
                    parsed["txid"],
                    parsed["amount"],
                    parsed["currency"],
                    parsed["payer"],
                    payload.text
                )

                if tx_record:
                    # Targeted soundbox notification via MQTT
                    is_published = False
                    if mqtt_publisher:
                        is_published = mqtt_publisher.send_payment_notification(
                            device_sn=device_sn,
                            amount=parsed["amount"],
                            currency=parsed["currency"],
                            txid=parsed["txid"]
                        )

                    logger.info(f"🔊 Payment Alert sent to Device [{device_sn}] (TxID: {parsed['txid']}, Published: {is_published})")
                    return {
                        "status": "success",
                        "action": "targeted_payment_sent",
                        "device_sn": device_sn,
                        "transaction": parsed,
                        "mqtt_published": is_published
                    }
                else:
                    logger.warning(f"⚠️ Duplicate Transaction Ignored: {parsed['bank']} - {parsed['txid']}")
                    return {
                        "status": "duplicate",
                        "message": f"Duplicate transaction {parsed['txid']} ignored",
                        "transaction": parsed
                    }

            else:
                # No specific device linked -> Broadcast or log
                logger.info(f"No specific active device linked to Chat {payload.chat_id}. Broadcasting to global topic...")
                is_published = False
                if mqtt_publisher:
                    is_published = mqtt_publisher.broadcast_payment_notification(
                        amount=parsed["amount"],
                        currency=parsed["currency"],
                        txid=parsed["txid"]
                    )

                return {
                    "status": "success",
                    "action": "global_broadcast",
                    "chat_id": payload.chat_id,
                    "transaction": parsed,
                    "mqtt_published": is_published
                }

    except Exception as e:
        logger.error(f"Database error during userbot transaction processing: {e}")
        return {
            "status": "error",
            "message": str(e),
            "transaction": parsed
        }

@app.post("/api/users/sync", dependencies=[Depends(verify_api_key)])
async def sync_user(payload: UserSyncSchema):
    """ធ្វើសមកាលកម្ម (Sync) និងរក្សាទុកព័ត៌មានសមាជិក Group ចូលទៅក្នុង PostgreSQL"""
    try:
        async with db_pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO group_users (chat_id, user_id, username, full_name, is_authorized, updated_at)
                VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
                ON CONFLICT (chat_id, user_id) 
                DO UPDATE SET 
                    username = EXCLUDED.username, 
                    full_name = EXCLUDED.full_name, 
                    updated_at = CURRENT_TIMESTAMP
                """,
                payload.chat_id, 
                payload.user_id, 
                payload.username, 
                payload.full_name, 
                payload.is_authorized
            )
            
        logger.info(f"Successfully synced user -> Chat: {payload.chat_id} | User ID: {payload.user_id}")
        return {
            "status": "success", 
            "message": f"User {payload.user_id} synced successfully (is_authorized={payload.is_authorized})"
        }
    except Exception as e:
        logger.error(f"Database error during user sync: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")