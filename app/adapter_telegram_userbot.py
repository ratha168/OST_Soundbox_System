import asyncio
import io
import json
import logging
import os
import time
import httpx
import qrcode
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.tl.types import MessageActionChatAddUser, MessageActionChatJoinedByLink

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("Userbot_Engine")

API_ID = int(os.getenv("TELEGRAM_API_ID") or 34687255)
API_HASH = os.getenv("TELEGRAM_API_HASH") or "0c8a94e104d60fe54bf05605122ae878"
SESSION_NAME = os.getenv("TELEGRAM_USERBOT_SESSION", "userbot_session")

FASTAPI_USERBOT_URL = os.getenv("FASTAPI_USERBOT_URL") or "http://fastapi-gateway:8000/webhook/telegram-userbot"
FASTAPI_SYNC_USER_URL = os.getenv("FASTAPI_SYNC_USER_URL") or "http://fastapi-gateway:8000/api/users/sync"
API_SECRET_KEY = os.getenv("API_SECRET_KEY", "your-api-key")

AUTH_HEADERS = {
    "X-API-Key": API_SECRET_KEY,
    "Content-Type": "application/json"
}

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

welcome_throttle = {}
THROTTLE_SECONDS = 5.0  # កាត់បន្ថយមកត្រឹម 5s

async def send_welcome_qr(peer_target, chat_id: str):
    clean_chat_id = str(chat_id).strip()
    now = time.time()
    
    # Throttle ការពារ spam
    if clean_chat_id in welcome_throttle and (now - welcome_throttle[clean_chat_id] < THROTTLE_SECONDS):
        return
    welcome_throttle[clean_chat_id] = now

    caption = (
        "សូមស្វាគមន៍មកកាន់ប្រព័ន្ធសំឡេង OST System Soundbox!\n"
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
        img_byte_arr.seek(0)
        img_byte_arr.name = "soundbox_setup_qr.png"

        if hasattr(peer_target, 'respond'):
            await peer_target.respond(caption, file=img_byte_arr, parse_mode="md")
        else:
            await client.send_file(peer_target, file=img_byte_arr, caption=caption, parse_mode="md")

        logger.info(f"==> Successfully sent Welcome QR to Chat ID: {clean_chat_id}")
    except Exception as e:
        logger.error(f"Failed to deliver Welcome QR: {e}")

@client.on(events.ChatAction)
async def handle_chat_action(event):
    chat_id = str(event.chat_id)
    me = await client.get_me()
    
    # ពិនិត្យប្រសិនបើជា Action Add User ឬ Userbot ខ្លួនឯងត្រូវបាន Add ចូល Group
    if event.user_added or event.user_joined:
        if event.user_id == me.id or event.added_by:
            logger.info(f"[CHAT_ACTION] Userbot added/joined to Chat: {chat_id}. Sending Welcome QR...")
            await send_welcome_qr(event, chat_id)

@client.on(events.NewMessage(incoming=None, outgoing=None))
async def handle_incoming_message(event):
    chat_id = str(event.chat_id)
    raw_text = (event.message.message or "").strip()
    clean_lower = raw_text.lower()

    # ពិនិត្យករណី Message ជា Service Action (Added to group)
    if isinstance(event.message.action, (MessageActionChatAddUser, MessageActionChatJoinedByLink)):
        logger.info(f"[MESSAGE_ACTION] Detected AddUser action in Chat {chat_id}. Sending Welcome QR...")
        await send_welcome_qr(event, chat_id)
        return

    # User Commands សម្រាប់ហៅកូដឡើងវិញដោយដៃ
    if clean_lower in ["/id", "/qr", "/code", "/setup", "/start", "id", "qr", "code", "setup"]:
        logger.info(f"[COMMAND] User requested QR/ID in Chat {chat_id}")
        await send_welcome_qr(event, chat_id)
        return

    if not raw_text:
        return

    sender = await event.get_sender()
    sender_id = str(sender.id) if sender else None

    payload = {
        "telegram_chat_id": chat_id,
        "telegram_user_id": sender_id,
        "raw_message": raw_text,
        "username": getattr(sender, "username", None),
        "full_name": f"{getattr(sender, 'first_name', '')} {getattr(sender, 'last_name', '')}".strip()
    }

    async with httpx.AsyncClient(timeout=10.0) as http_client:
        try:
            res = await http_client.post(FASTAPI_USERBOT_URL, json=payload, headers=AUTH_HEADERS)
            logger.info(f"Forwarded -> Status: {res.status_code}")
        except Exception as e:
            logger.error(f"Forwarding error: {e}")

async def main():
    logger.info("Starting Telethon Userbot Client...")
    await client.start()
    me = await client.get_me()
    logger.info(f"Connected as: @{me.username or me.id} ({me.first_name})")
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())