import asyncio
import os
import httpx
from dotenv import load_dotenv
from telethon import TelegramClient, events

load_dotenv()

# --- TELEGRAM CONFIG ---
API_ID = int(os.getenv("TELEGRAM_API_ID", 34687255))
API_HASH = os.getenv("TELEGRAM_API_HASH", "0c8a94e104d60fe54bf05605122ae878")

FASTAPI_USERBOT_URL = os.getenv("FASTAPI_USERBOT_URL", "http://localhost:8000/webhook/telegram-userbot")
FASTAPI_SYNC_USER_URL = os.getenv("FASTAPI_SYNC_USER_URL", "http://localhost:8000/api/users/sync")

client = TelegramClient("userbot_session", API_ID, API_HASH)

# ១. ចាប់យកសារដែលកើតមានក្នុង Group/Channel រួចផ្ញើព័ត៌មាន Sender + Text ទៅ FastAPI
@client.on(events.NewMessage)
async def handle_new_message(event):
    if event.is_group or event.is_channel:
        chat_id = str(event.chat_id)
        raw_text = event.message.message if event.message.message else ""

        if not raw_text:
            return

        sender = await event.get_sender()
        sender_id = str(sender.id) if sender else None
        username = getattr(sender, 'username', None)
        first_name = getattr(sender, 'first_name', '')
        last_name = getattr(sender, 'last_name', '')
        full_name = f"{first_name or ''} {last_name or ''}".strip()

        payload = {
            "chat_id": chat_id,
            "text": raw_text,
            "user_id": sender_id,
            "username": username,
            "full_name": full_name
        }

        async with httpx.AsyncClient(timeout=10.0) as http_client:
            try:
                res = await http_client.post(FASTAPI_USERBOT_URL, json=payload)
                print(f"[UserBot Listener] Chat ID: {chat_id} | Sender ID: {sender_id} | Response Status: {res.status_code}")
            except Exception as e:
                print(f"[UserBot Error]: Failed to post to FastAPI: {e}")

# ២. ចាប់យក Event ពេលមាន Member ថ្មី Join ឬត្រូវបាន Add ចូល Group
@client.on(events.ChatAction)
async def handle_chat_action(event):
    if event.user_joined or event.user_added:
        chat_id = str(event.chat_id)
        users = await event.get_users()
        
        async with httpx.AsyncClient(timeout=10.0) as http_client:
            for user in users:
                user_payload = {
                    "chat_id": chat_id,
                    "user_id": str(user.id),
                    "username": user.username,
                    "full_name": f"{user.first_name or ''} {user.last_name or ''}".strip()
                }
                try:
                    res = await http_client.post(FASTAPI_SYNC_USER_URL, json=user_payload)
                    print(f"[UserBot Sync Action] Synced New Member ID: {user.id} | Status: {res.status_code}")
                except Exception as e:
                    print(f"[UserBot Sync Error]: {e}")

# ៣. Scan សមាជិកដែលមានស្រាប់ក្នុង Group ទាំងអស់នៅពេល Startup
async def sync_existing_group_members():
    async with httpx.AsyncClient(timeout=10.0) as http_client:
        async for dialog in client.iter_dialogs():
            if dialog.is_group:
                chat_id = str(dialog.id)
                print(f"[Sync Process] Scanning group: {dialog.name} ({chat_id})...")
                async for user in client.iter_participants(dialog.id):
                    if user.bot:
                        continue
                    payload = {
                        "chat_id": chat_id,
                        "user_id": str(user.id),
                        "username": user.username,
                        "full_name": f"{user.first_name or ''} {user.last_name or ''}".strip()
                    }
                    try:
                        await http_client.post(FASTAPI_SYNC_USER_URL, json=payload)
                    except Exception:
                        pass
                print(f"[Sync Process] Finished group: {dialog.name}")

async def main():
    print("--------------------------------------------------")
    print("Starting Telegram UserBot Service...")
    await client.start()
    print("Telegram UserBot Connected & Actively Listening!")
    
    # ដំណើរកការ Sync សមាជិកចាស់ៗក្នុង Group ភ្លាមៗ
    await sync_existing_group_members()
    
    print("--------------------------------------------------")
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())