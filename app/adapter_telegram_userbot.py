import asyncio
import io
import json
import logging
import os
import time
import httpx
import qrcode
from dotenv import load_dotenv
from telethon import TelegramClient, events, utils
from telethon.tl import types

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("telegram_userbot")

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

STATE_FILE = os.getenv("USERBOT_STATE_FILE", "userbot_state.json")
welcome_throttle = {}
THROTTLE_SECONDS = 30.0

greeted_chats = set()
last_qr_messages = {}


def load_state():
    global greeted_chats, last_qr_messages
    if os.path.exists(STATE_FILE) and os.path.isfile(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                data = json.load(f)
                greeted_chats = set(data.get("greeted_chats", []))
                last_qr_messages = data.get("last_qr_messages", {})
                logger.info(f"Loaded userbot state: {len(greeted_chats)} greeted chats")
        except Exception as e:
            logger.warning(f"Could not load state file {STATE_FILE}: {e}")


def save_state():
    try:
        with open(STATE_FILE, "w") as f:
            json.dump({
                "greeted_chats": list(greeted_chats),
                "last_qr_messages": last_qr_messages
            }, f)
    except Exception as e:
        logger.debug(f"Failed to save state file: {e}")


load_state()


def can_send_welcome(chat_id: str, is_user_command: bool = False) -> bool:
    now = time.time()
    clean_id = str(chat_id).strip()

    expired = [k for k, v in welcome_throttle.items() if now - v > THROTTLE_SECONDS]
    for k in expired:
        del welcome_throttle[k]

    if not is_user_command and clean_id in welcome_throttle:
        return False

    welcome_throttle[clean_id] = now
    return True


async def resolve_peer_safely(event, chat_id: str):
    """ស្វែងរក Entity/Peer ត្រឹមត្រូវដើម្បីផ្ញើសារ/រូបភាពដោយគ្មានបញ្ហា Invalid Peer"""
    if event is not None:
        try:
            input_chat = await event.get_input_chat()
            if input_chat:
                return input_chat
        except Exception:
            pass
        
        try:
            chat = await event.get_chat()
            if chat:
                return chat
        except Exception:
            pass

    clean_str = str(chat_id).strip()
    try:
        raw_id = int(clean_str)
        entity = await client.get_entity(raw_id)
        return entity
    except Exception:
        pass

    try:
        if clean_str.startswith("-") and not clean_str.startswith("-100"):
            conv_id = int(f"-100{clean_str.lstrip('-')}")
            entity = await client.get_entity(conv_id)
            return entity
    except Exception:
        pass

    return int(clean_str) if clean_str.lstrip("-").isdigit() else clean_str


async def cleanup_all_old_qrs(peer, chat_id: str, keep_msg_id: int = None):
    clean_chat_id = str(chat_id).strip()
    try:
        me = await client.get_me()
        old_tracked = last_qr_messages.get(clean_chat_id)
        if old_tracked and old_tracked != keep_msg_id:
            try:
                await client.delete_messages(peer, old_tracked)
            except Exception:
                pass

        delete_ids = []
        async for msg in client.iter_messages(peer, limit=20, from_user=me.id):
            if keep_msg_id and msg.id == keep_msg_id:
                continue
            text = msg.message or ""
            if msg.media and ("OST System Soundbox" in text or "telegram code" in text):
                delete_ids.append(msg.id)

        if delete_ids:
            await client.delete_messages(peer, delete_ids)
    except Exception as e:
        logger.debug(f"Could not purge old QR messages in {clean_chat_id}: {e}")


async def send_welcome_qr(event, chat_id: str, is_user_command: bool = False):
    clean_chat_id = str(chat_id).strip()
    if not can_send_welcome(clean_chat_id, is_user_command=is_user_command):
        return

    greeted_chats.add(clean_chat_id)
    save_state()

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

        # ដោះស្រាយ Peer ឱ្យត្រូវ
        peer = await resolve_peer_safely(event, clean_chat_id)
        
        # ផ្ញើរូបភាព QR Code ទៅកាន់ Telegram Group
        sent_msg = None
        if event is not None and hasattr(event, "respond"):
            try:
                sent_msg = await event.respond(caption, file=img_byte_arr, parse_mode="md")
            except Exception:
                pass

        if not sent_msg:
            sent_msg = await client.send_file(peer, file=img_byte_arr, caption=caption, parse_mode="md")

        new_msg_id = getattr(sent_msg, "id", None)
        if new_msg_id:
            last_qr_messages[clean_chat_id] = new_msg_id
            save_state()
            await cleanup_all_old_qrs(peer, clean_chat_id, keep_msg_id=new_msg_id)

        if is_user_command and event and hasattr(event, "message") and getattr(event.message, "id", None):
            try:
                await event.message.delete()
            except Exception:
                pass

        logger.info(f"Successfully sent single Welcome QR code to Chat ID {clean_chat_id}")
    except Exception as e:
        logger.error(f"Error sending Welcome QR to chat {clean_chat_id}: {e}")


async def sync_single_user(chat_id: str, user):
    """Sync User/Bot ទៅកាន់ FastAPI Backend"""
    if not user:
        return
    
    first_name = getattr(user, "first_name", "") or ""
    last_name = getattr(user, "last_name", "") or ""
    full_name = f"{first_name} {last_name}".strip()
    u_id = str(getattr(user, "id", ""))
    c_id = str(chat_id)

    user_payload = {
        "telegram_chat_id": c_id,
        "chat_id": c_id,
        "telegram_user_id": u_id,
        "user_id": u_id,
        "username": getattr(user, "username", None),
        "first_name": first_name,
        "last_name": last_name,
        "full_name": full_name,
        "is_bot": getattr(user, "bot", False)
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as http_client:
            res = await http_client.post(FASTAPI_SYNC_USER_URL, json=user_payload, headers=AUTH_HEADERS)
            if res.status_code == 200:
                logger.info(f"Successfully synced user -> Chat: {c_id} | User ID: {u_id} ({full_name})")
            else:
                logger.warning(f"Sync user failed {u_id}: Status {res.status_code} - {res.text}")
    except Exception as e:
        logger.error(f"Error syncing user {u_id}: {e}")


async def sync_all_group_members(dialog):
    try:
        title = getattr(dialog, "name", getattr(dialog, "title", str(dialog.id)))
        logger.info(f"Scanning group: {title} ({dialog.id})")
        async for user in client.iter_participants(dialog):
            await sync_single_user(str(dialog.id), user)
    except Exception as e:
        logger.error(f"Could not sync participants for dialog {getattr(dialog, 'name', str(dialog))}: {e}")


async def sync_all_dialogs():
    try:
        async for dialog in client.iter_dialogs():
            if dialog.is_group or dialog.is_channel:
                await sync_all_group_members(dialog)
    except Exception as e:
        logger.error(f"Error in sync_all_dialogs: {e}")


async def periodic_dialog_sync_task():
    while True:
        try:
            await asyncio.sleep(180)
            await sync_all_dialogs()
        except Exception as e:
            logger.error(f"Error in periodic dialog sync: {e}")


# ១. Handle Incoming Messages
@client.on(events.NewMessage)
async def handle_new_message(event):
    chat_id = str(event.chat_id)
    raw_text = (event.message.message or "").strip()
    clean_lower = raw_text.lower()

    sender = await event.get_sender()
    if sender:
        asyncio.create_task(sync_single_user(chat_id, sender))

    setup_keywords = ["/id", "/qr", "/code", "/setup", "/start", "id", "qr", "code", "setup"]
    is_command = clean_lower in setup_keywords or any(clean_lower.startswith(k + " ") for k in ["/id", "/qr", "/code", "/setup", "id", "code"])

    if is_command:
        logger.info(f"Setup command received in Chat {chat_id}. Sending QR...")
        await send_welcome_qr(event, chat_id, is_user_command=True)
        return

    action = getattr(event.message, "action", None)
    if action is not None:
        logger.info(f"Detected service action in Chat {chat_id}. Sending Welcome QR...")
        await send_welcome_qr(event, chat_id, is_user_command=False)
        return

    if chat_id not in greeted_chats:
        await send_welcome_qr(event, chat_id, is_user_command=False)

    if not raw_text:
        return

    sender_id = str(sender.id) if sender else None
    username = getattr(sender, "username", None)
    first_name = getattr(sender, "first_name", "")
    last_name = getattr(sender, "last_name", "")
    full_name = f"{first_name or ''} {last_name or ''}".strip()
    is_bot = getattr(sender, "bot", False)

    forward_from_chat_id = None
    if event.message.forward and event.message.forward.chat:
        forward_from_chat_id = str(event.message.forward.chat.id)

    payload = {
        "telegram_chat_id": chat_id,
        "chat_id": chat_id,
        "telegram_user_id": sender_id,
        "user_id": sender_id,
        "raw_message": raw_text,
        "text": raw_text,
        "username": username,
        "full_name": full_name,
        "first_name": first_name,
        "last_name": last_name,
        "is_bot": is_bot,
        "forward_from_chat_id": forward_from_chat_id
    }

    async with httpx.AsyncClient(timeout=12.0) as http_client:
        try:
            res = await http_client.post(FASTAPI_USERBOT_URL, json=payload, headers=AUTH_HEADERS)
            if res.status_code == 200:
                logger.info(f"Forwarded message to FastAPI [Chat: {chat_id} | Status: 200]")
            else:
                logger.warning(f"FastAPI returned status {res.status_code}: {res.text}")
        except Exception as e:
            logger.error(f"Failed to forward message to FastAPI: {e}")


# ២. Handle Chat Action Events
@client.on(events.ChatAction)
async def handle_chat_action(event):
    chat_id = str(event.chat_id)
    logger.info(f"ChatAction triggered in Chat {chat_id}. Sending Welcome QR...")
    
    await send_welcome_qr(event, chat_id, is_user_command=False)

    try:
        me = await client.get_me()

        if event.user_added and (event.user_id == me.id or (isinstance(event.user_id, list) and me.id in event.user_id)):
            input_chat = await event.get_input_chat()
            logger.info(f"Userbot added to NEW group ({chat_id}). Scanning all members...")
            async for user in client.iter_participants(input_chat):
                await sync_single_user(chat_id, user)
            return

        if event.user_joined or event.user_added:
            users = await event.get_users()
            for user in users:
                logger.info(f"New member joined/added in group {chat_id}: {getattr(user, 'first_name', user.id)}")
                await sync_single_user(chat_id, user)
    except Exception as e:
        logger.error(f"Error handling ChatAction sync: {e}")


# ៣. Startup & Main Loop
async def main():
    logger.info("==================================================")
    logger.info("Starting Telegram UserBot Service (Telethon)...")
    logger.info(f"Connecting with API_ID: {API_ID} | Session: {SESSION_NAME}")

    await client.start()

    me = await client.get_me()
    logger.info(f"Telegram UserBot Connected as: @{me.username or me.phone or me.id} ({me.first_name})")
    logger.info("Listening for group joins and incoming bank transactions...")
    logger.info("==================================================")

    asyncio.create_task(sync_all_dialogs())
    asyncio.create_task(periodic_dialog_sync_task())

    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())