import asyncio
import io
import logging
import time
from typing import Any, Dict, Optional
import httpx
import qrcode
from telethon import TelegramClient, events
from app.core.config import settings

# កំណត់ logging ជាសកលជាមួយ force=True សម្រាប់ដំណើរការ userbot ដាច់ដោយឡែក
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True,
)
logger = logging.getLogger("UserbotEngine")

class WelcomeQrService:
    def __init__(self, throttle_window: float = 5.0):
        self._window = throttle_window
        self._throttle: Dict[str, float] = {}

    def should_throttle(self, chat_id: str) -> bool:
        now = time.time()
        if chat_id in self._throttle and (now - self._throttle[chat_id]) < self._window:
            return True
        self._throttle[chat_id] = now
        return False

    @staticmethod
    def generate_qr_stream(data: str) -> io.BytesIO:
        qr = qrcode.QRCode(version=1, box_size=10, border=2)
        qr.add_data(data)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        stream = io.BytesIO()
        img.save(stream, format="PNG")
        stream.seek(0)
        stream.name = "soundbox_setup.png"
        return stream

    async def send_welcome(self, client: TelegramClient, peer: Any, chat_id: str) -> None:
        if self.should_throttle(chat_id):
            return

        caption = (
            "សូមស្វាគមន៍មកកាន់ប្រព័ន្ធសំឡេង OST Soundbox System!\n"
            "លោកអ្នកកំពុងតែរៀបចំក្នុងការដំឡើងឧបករណ៍ Soundbox របស់យើងខ្ញុំ។ សូមចម្លងលេខកូដខាងក្រោមនេះ "
            "ដើម្បីយកទៅបំពេញ ឬតភ្ជាប់ទៅក្នុង 「Verification Code」 នៅក្នុងប្រព័ន្ធយើងខ្ញុំ\n\n"
            "លេខកូដ (telegram code) របស់អ្នកគឺ\n"
            "Please copy telegram code to complete the setup:\n"
            f"`{chat_id}`"
        )
        qr_file = self.generate_qr_stream(chat_id)

        try:
            if hasattr(peer, "respond"):
                await peer.respond(caption, file=qr_file, parse_mode="md")
            else:
                await client.send_file(peer, file=qr_file, caption=caption, parse_mode="md")
            logger.info("Welcome QR dispatched to chat %s", chat_id)
        except Exception as e:
            logger.error("Failed to send welcome QR: %s", e)


class ForwardingClient:
    def __init__(self, target_url: str, api_key: str):
        self._url = target_url
        self._headers = {"X-API-Key": api_key, "Content-Type": "application/json"}
        self._client: Optional[httpx.AsyncClient] = None

    async def start(self):
        self._client = httpx.AsyncClient(timeout=10.0, limits=httpx.Limits(max_keepalive_connections=20))

    async def stop(self):
        if self._client:
            await self._client.aclose()

    async def forward(self, payload: dict) -> bool:
        if not self._client:
            return False
        try:
            res = await self._client.post(self._url, json=payload, headers=self._headers)
            if res.status_code == 200:
                data = res.json()
                if data.get("status") == "success":
                    logger.info("Forwarded to Gateway: TxID %s | Devices: %s", data.get("txid"), data.get("broadcast_to"))
                return True
            logger.warning("Gateway returned HTTP %s for chat %s", res.status_code, payload.get("telegram_chat_id"))
            return False
        except Exception as e:
            logger.error("Forwarding exception: %s", e)
            return False


class UserbotManager:
    SETUP_COMMANDS = {"/id", "/qr", "/code", "/setup", "/start", "id", "qr", "code", "setup"}

    def __init__(self):
        self.client = TelegramClient(settings.telegram_session, settings.telegram_api_id, settings.telegram_api_hash)
        self.qr_service = WelcomeQrService()
        self.forwarder = ForwardingClient(settings.fastapi_userbot_url, settings.api_secret_key)
        self._me = None

    def _setup_handlers(self):
        @self.client.on(events.ChatAction)
        async def on_action(event):
            chat_id = str(event.chat_id)
            if event.user_added or event.user_joined:
                if (self._me and event.user_id == self._me.id) or event.added_by:
                    await self.qr_service.send_welcome(self.client, event, chat_id)

        @self.client.on(events.NewMessage)
        async def on_message(event):
            text = (event.message.message or "").strip()
            
            # 1. រំលង Service messages (Handled by ChatAction)
            if event.message.action or not text:
                return

            chat_id = str(event.chat_id)
            logger.info("📩 ទទួលបានសារពី Chat ID: %s | អត្ថបទ: %s", chat_id, text)

            # 2. ពិនិត្យពាក្យបញ្ជា Setup / QR code
            if text.lower() in self.SETUP_COMMANDS:
                await self.qr_service.send_welcome(self.client, event, chat_id)
                return

            # 3. រំលងសារផ្ទាល់ខ្លួនរបស់ Bot ដើម្បីកុំឱ្យ Loop
            if event.out and self._me and event.sender_id == self._me.id:
                return

            # 4. បញ្ជូនសារទូទាត់ប្រាក់ទៅកាន់ Gateway
            sender = await event.get_sender()
            payload = {
                "telegram_chat_id": chat_id,
                "telegram_user_id": str(sender.id) if sender else None,
                "raw_message": text,
                "username": getattr(sender, "username", None),
                "full_name": f"{getattr(sender, 'first_name', '')} {getattr(sender, 'last_name', '')}".strip(),
            }

            asyncio.create_task(self.forwarder.forward(payload))

    async def run(self):
        await self.forwarder.start()
        await self.client.start()
        self._me = await self.client.get_me()
        logger.info("Connected as @%s (%s)", self._me.username or self._me.id, self._me.first_name)
        self._setup_handlers()
        try:
            await self.client.run_until_disconnected()
        finally:
            await self.forwarder.stop()


if __name__ == "__main__":
    bot = UserbotManager()
    try:
        asyncio.run(bot.run())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Userbot stopped by admin.")