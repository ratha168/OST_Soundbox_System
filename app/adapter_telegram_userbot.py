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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("telegram_userbot")

# --- TELEGRAM USERBOT CONFIG ---
API_ID = int(os.getenv("TELEGRAM_API_ID") or 34687255)
API_HASH = os.getenv("TELEGRAM_API_HASH") or "0c8a94e104d60fe54bf05605122ae878"
SESSION_NAME = os.getenv("TELEGRAM_USERBOT_SESSION", "userbot_session")

FASTAPI_USERBOT_URL = os.getenv("FASTAPI_USERBOT_URL") or "http://fastapi-gateway:8000/webhook/telegram-userbot"
FASTAPI_SYNC_USER_URL = os.getenv("FASTAPI_SYNC_USER_URL") or "http://fastapi-gateway:8000/api/users/sync"
API_SECRET_KEY = os.getenv("API_SECRET_KEY", "your-api-key")

# Common Headers សម្រាប់ផ្ញើទៅ FastAPI Gateway
AUTH_HEADERS = {
    "X-API-Key": API_SECRET_KEY,
    "Content-Type": "application/json"
}

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

# Local persistence file
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
                logger.info(f"Loaded userbot state: {len(greeted_chats)} greeted chats from {STATE_FILE}")
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


async def cleanup_all_old_qrs(chat_id: str, keep_msg_id: int = None):
    clean_chat_id = str(chat_id).strip()
    try:
        target_peer = int(clean_chat_id) if clean_chat_id.lstrip("-").isdigit() else clean_chat_id
        me = await client.get_me()

        old_tracked = last_qr_messages.get(clean_chat_id)
        if old_tracked and old_tracked != keep_msg_id:
            try:
                await client.delete_messages(target_peer, old_tracked)
                logger.info(f"Deleted tracked old QR message ID {old_tracked} in Chat {clean_chat_id}")
            except Exception:
                pass

        delete_ids = []
        async for msg in client.iter_messages(target_peer, limit=30, from_user=me.id):
            if keep_msg_id and msg.id == keep_msg_id:
                continue
            text = msg.message or ""
            if msg.media and ("OST System Soundbox" in text or "Telegram Chat ID" in text or "soundbox_setup_qr" in text):
                delete_ids.append(msg.id)

        if delete_ids:
            await client.delete_messages(target_peer, delete_ids)
            logger.info(f"Purged {len(delete_ids)} older QR message(s) in Chat {clean_chat_id}: {delete_ids}")

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

        sent_msg = None
        if event and hasattr(event, "respond"):
            sent_msg = await event.respond(caption, file=img_byte_arr, parse_mode="md")
        else:
            try:
                target_peer = int(clean_chat_id) if clean_chat_id.lstrip("-").isdigit() else clean_chat_id
                entity = await client.get_entity(target_peer)
                sent_msg = await client.send_file(entity, file=img_byte_arr, caption=caption, parse_mode="md")
            except Exception:
                target_peer = int(clean_chat_id) if clean_chat_id.lstrip("-").isdigit() else clean_chat_id
                sent_msg = await client.send_file(target_peer, file=img_byte_arr, caption=caption, parse_mode="md")

        new_msg_id = getattr(sent_msg, "id", None)
        if new_msg_id:
            last_qr_messages[clean_chat_id] = new_msg_id
            save_state()
            await cleanup_all_old_qrs(clean_chat_id, keep_msg_id=new_msg_id)

        if is_user_command and event and hasattr(event, "message") and getattr(event.message, "id", None):
            try:
                await event.message.delete()
            except Exception:
                pass

        logger.info(f"Successfully sent single Welcome QR code to Chat ID {clean_chat_id} (Message ID: {new_msg_id})")
    except Exception as e:
        logger.error(f"Error sending Welcome QR image to chat {clean_chat_id}: {e}")
        try:
            target_peer = int(clean_chat_id) if clean_chat_id.lstrip("-").isdigit() else clean_chat_id
            await client.send_message(target_peer, caption, parse_mode="md")
        except Exception as te:
            logger.error(f"Failed to send fallback text message: {te}")


async def send_welcome_qr_to_dialog(dialog, chat_id: str):
    clean_chat_id = str(chat_id).strip()
    if not can_send_welcome(clean_chat_id, is_user_command=False):
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

        sent_msg = await client.send_file(dialog.entity, file=img_byte_arr, caption=caption, parse_mode="md")
        new_msg_id = getattr(sent_msg, "id", None)
        if new_msg_id:
            last_qr_messages[clean_chat_id] = new_msg_id
            save_state()
            await cleanup_all_old_qrs(clean_chat_id, keep_msg_id=new_msg_id)

        logger.info(f"Proactively sent Welcome QR code to new group '{dialog.name}' ({clean_chat_id})")
    except Exception as e:
        logger.error(f"Error proactively sending Welcome QR to group '{dialog.name}': {e}")


# ១. Handle Incoming Messages (Commands, Service Actions & Bank Notifications)
@client.on(events.NewMessage)
async def handle_new_message(event):
    chat_id = str(event.chat_id)
    raw_text = (event.message.message or "").strip()
    clean_lower = raw_text.lower()

    setup_keywords = ["/id", "/qr", "/code", "/setup", "/start", "id", "qr", "code", "setup"]
    is_command = clean_lower in setup_keywords or any(clean_lower.startswith(k + " ") for k in ["/id", "/qr", "/code", "/setup", "id", "code"])

    if is_command:
        logger.info(f"Setup command '{raw_text}' received in Chat {chat_id}. Sending QR code...")
        await send_welcome_qr(event, chat_id, is_user_command=True)
        return

    action = getattr(event.message, "action", None)
    if action is not None:
        logger.info(f"Detected service action in Chat {chat_id}: {action.__class__.__name__}. Sending Welcome QR...")
        await send_welcome_qr(event, chat_id, is_user_command=False)
        return

    if chat_id not in greeted_chats:
        logger.info(f"First message in un-greeted group {chat_id}. Auto-sending Welcome QR...")
        await send_welcome_qr(event, chat_id, is_user_command=False)

    if not raw_text:
        return

    sender = await event.get_sender()
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
        "chat_id": chat_id,
        "text": raw_text,
        "user_id": sender_id,
        "username": username,
        "full_name": full_name,
        "is_bot": is_bot,
        "forward_from_chat_id": forward_from_chat_id
    }

    # Forward to FastAPI ជាមួយ X-API-Key Header
    async with httpx.AsyncClient(timeout=12.0) as http_client:
        try:
            res = await http_client.post(FASTAPI_USERBOT_URL, json=payload, headers=AUTH_HEADERS)
            if res.status_code == 200:
                logger.info(f"Forwarded message to FastAPI [Chat: {chat_id} | Status: 200]")
            else:
                logger.warning(f"FastAPI UserBot Webhook returned status {res.status_code}: {res.text}")
        except Exception as e:
            logger.error(f"Failed to forward message to FastAPI: {e}")


# ២. Handle Chat Action Events
@client.on(events.ChatAction)
async def handle_chat_action(event):
    chat_id = str(event.chat_id)
    logger.info(f"ChatAction triggered in Chat {chat_id} (joined/added/created). Sending Welcome QR...")
    await send_welcome_qr(event, chat_id, is_user_command=False)

    try:
        users = await event.get_users()
        async with httpx.AsyncClient(timeout=10.0) as http_client:
            for user in users:
                user_payload = {
                    "chat_id": chat_id,
                    "user_id": str(user.id),
                    "username": user.username,
                    "full_name": f"{user.first_name or ''} {user.last_name or ''}".strip()
                }
                await http_client.post(FASTAPI_SYNC_USER_URL, json=user_payload, headers=AUTH_HEADERS)
    except Exception as e:
        logger.warning(f"User sync error on ChatAction: {e}")


# ៣. Raw Updates Listener
@client.on(events.Raw)
async def handle_raw_update(update):
    try:
        if isinstance(update, (types.UpdateNewMessage, types.UpdateNewChannelMessage)):
            msg = update.message
            if isinstance(msg, types.MessageService):
                action = msg.action
                if isinstance(action, (
                    types.MessageActionChatAddUser, 
                    types.MessageActionChatJoinedByLink,
                    types.MessageActionChatCreate,
                    types.MessageActionChannelCreate,
                    types.MessageActionChatMigrateTo
                )):
                    chat_id = str(utils.get_peer_id(msg.peer_id))
                    logger.info(f"Auto-detected Supergroup Join in Chat {chat_id} ({action.__class__.__name__}). Sending Welcome QR...")
                    await send_welcome_qr(None, chat_id, is_user_command=False)
        
        elif isinstance(update, (types.UpdateChatParticipantAdd, types.UpdateChatParticipants)):
            chat_id = str(getattr(update, 'chat_id', ''))
            if chat_id:
                formatted_chat_id = f"-{chat_id}" if not chat_id.startswith("-") else chat_id
                logger.info(f"Auto-detected Participant Add in Chat {formatted_chat_id}. Sending Welcome QR...")
                await send_welcome_qr(None, formatted_chat_id, is_user_command=False)
    except Exception as e:
        logger.debug(f"Raw update handling: {e}")


# ៤. Periodic Dialog Scanner
async def auto_scan_new_groups_loop():
    await asyncio.sleep(1.0)
    try:
        async for dialog in client.iter_dialogs(limit=50):
            is_grp = dialog.is_group or (dialog.is_channel and getattr(dialog.entity, "megagroup", False))
            if is_grp:
                greeted_chats.add(str(dialog.id))
        save_state()
        logger.info(f"Scanner initialized with {len(greeted_chats)} existing groups in state.")
    except Exception as e:
        logger.warning(f"Error initializing existing dialogs: {e}")

    while True:
        try:
            async for dialog in client.iter_dialogs(limit=25):
                is_grp = dialog.is_group or (dialog.is_channel and getattr(dialog.entity, "megagroup", False))
                if is_grp:
                    chat_id = str(dialog.id)
                    if chat_id not in greeted_chats:
                        logger.info(f"🆕 NEW GROUP DETECTED: '{dialog.name}' ({chat_id})! Auto-sending Welcome QR...")
                        greeted_chats.add(chat_id)
                        save_state()
                        await send_welcome_qr_to_dialog(dialog, chat_id)
        except Exception as e:
            logger.debug(f"Dialog scan loop error: {e}")
        await asyncio.sleep(15.0)


# ៥. Startup & Main Loop
async def main():
    logger.info("==================================================")
    logger.info("Starting Telegram UserBot Service (Telethon)...")
    logger.info(f"Connecting with API_ID: {API_ID} | Session: {SESSION_NAME}")
    
    await client.start()
    
    me = await client.get_me()
    logger.info(f"Telegram UserBot Connected as: @{me.username or me.phone or me.id} ({me.first_name})")
    logger.info("Listening for group joins and incoming bank transactions...")
    logger.info("==================================================")
    
    asyncio.create_task(auto_scan_new_groups_loop())
    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())