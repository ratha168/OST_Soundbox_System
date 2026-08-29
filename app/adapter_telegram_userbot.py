import asyncio
import os
import logging
import httpx
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError

# --- LOGGING CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("Telegram_UserBot")

load_dotenv()

# --- CONFIGURATION ---
API_ID = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")
API_SECRET_KEY = os.getenv("API_SECRET_KEY", "your-api-key")

if not API_ID or not API_HASH:
    raise ValueError("សូមពិនិត្យមើល TELEGRAM_API_ID និង TELEGRAM_API_HASH នៅក្នុង .env!")

API_ID = int(API_ID)

FASTAPI_WEBHOOK_URL = os.getenv(
    "FASTAPI_USERBOT_URL", 
    "http://iot_fastapi_gateway:8000/webhook/telegram-userbot"
)
FASTAPI_SYNC_USER_URL = os.getenv(
    "FASTAPI_SYNC_USER_URL", 
    "http://iot_fastapi_gateway:8000/api/users/sync"
)

# --- HTTP CLIENT WITH API KEY HEADERS ---
HEADERS = {
    "X-API-Key": API_SECRET_KEY,
    "Content-Type": "application/json"
}
http_client = httpx.AsyncClient(headers=HEADERS, timeout=10.0)

# --- TELEGRAM CLIENT ---
client = TelegramClient("userbot_session", API_ID, API_HASH)


@client.on(events.NewMessage)
async def handle_new_message(event):
    if event.is_group or event.is_channel:
        raw_text = event.message.message or ""
        if not raw_text.strip():
            return

        chat_id = str(event.chat_id)
        sender = await event.get_sender()
        
        sender_id = str(sender.id) if sender else None
        username = getattr(sender, 'username', None) if sender else None
        first_name = getattr(sender, 'first_name', '') if sender else ''
        last_name = getattr(sender, 'last_name', '') if sender else ''
        full_name = f"{first_name or ''} {last_name or ''}".strip()

        payload = {
            "chat_id": chat_id,
            "text": raw_text,
            "user_id": sender_id,
            "username": username,
            "full_name": full_name
        }

        try:
            res = await http_client.post(FASTAPI_WEBHOOK_URL, json=payload)
            logger.info(f"Userbot Message Sent -> Chat: {chat_id} | Status: {res.status_code}")
        except Exception as e:
            logger.error(f"Failed to post message to FastAPI: {e}")


@client.on(events.ChatAction)
async def handle_chat_action(event):
    if event.user_joined or event.user_added:
        chat_id = str(event.chat_id)
        users = await event.get_users()

        for user in users:
            if getattr(user, 'bot', False):
                continue

            user_payload = {
                "chat_id": chat_id,
                "user_id": str(user.id),
                "username": user.username,
                "full_name": f"{user.first_name or ''} {user.last_name or ''}".strip(),
                "is_authorized": False
            }
            try:
                res = await http_client.post(FASTAPI_SYNC_USER_URL, json=user_payload)
                logger.info(f"Synced Member -> User ID: {user.id} | Status: {res.status_code}")
            except Exception as e:
                logger.error(f"Failed to sync member {user.id}: {e}")


async def sync_existing_group_members():
    logger.info("Starting background sync for existing group members...")
    try:
        async for dialog in client.iter_dialogs():
            if dialog.is_group:
                chat_id = str(dialog.id)
                logger.info(f"Scanning group: {dialog.name} ({chat_id})")
                try:
                    async for user in client.iter_participants(dialog.id):
                        if user.bot:
                            continue

                        payload = {
                            "chat_id": chat_id,
                            "user_id": str(user.id),
                            "username": user.username,
                            "full_name": f"{user.first_name or ''} {user.last_name or ''}".strip(),
                            "is_authorized": False
                        }
                        try:
                            await http_client.post(FASTAPI_SYNC_USER_URL, json=payload)
                        except Exception:
                            pass
                        
                        await asyncio.sleep(0.05)

                except FloodWaitError as e:
                    logger.warning(f"Telegram Rate Limit! Waiting {e.seconds}s...")
                    await asyncio.sleep(e.seconds)
                except Exception as err:
                    logger.error(f"Error scanning group {dialog.name}: {err}")
    except Exception as e:
        logger.error(f"Fatal error during existing member sync: {e}")


async def main():
    logger.info("Starting Telegram UserBot Service...")
    await client.start()
    logger.info("Telegram UserBot Connected & Actively Listening!")

    asyncio.create_task(sync_existing_group_members())

    try:
        await client.run_until_disconnected()
    finally:
        await http_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())