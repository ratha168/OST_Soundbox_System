from fastapi import FastAPI, Request, HTTPException, Header, Depends
import asyncpg
import os
import logging
from contextlib import asynccontextmanager
from pydantic import BaseModel, Field
from typing import Optional

# --- LOGGING CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("IoT_FastAPI")

# --- ENVIRONMENT CONFIGURATIONS ---
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:fDdiFw_KB2930otN@postgres:5432/postgres")
API_SECRET_KEY = os.getenv("API_SECRET_KEY", "your-api-key")

db_pool = None

# --- LIFESPAN (DATABASE POOL MANAGEMENT) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
        logger.info("PostgreSQL database connection pool established successfully.")
        yield
    except Exception as e:
        logger.error(f"Failed to create database connection pool: {e}")
        raise e
    finally:
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
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/webhook/telegram-userbot", dependencies=[Depends(verify_api_key)])
async def telegram_userbot_webhook(payload: UserbotMessageSchema):
    """ទទួលព្រឹត្តិការណ៍សារ និងទិន្នន័យផ្សេងៗពី Userbot"""
    logger.info(f"Userbot Message Received -> Chat: {payload.chat_id} | Sender: {payload.user_id}")
    
    # អាចបន្ថែម Business Logic សម្រាប់ចាត់ចែងសារ ឬបញ្ជូនបន្តទៅ MQTT Broker ត្រង់ចំណុចនេះ
    
    return {
        "status": "success", 
        "action": "userbot_message_processed",
        "chat_id": payload.chat_id
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