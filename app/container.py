import queue
from typing import Any, Dict, Optional
import asyncpg
import httpx
from app.core.config import settings
from app.infrastructure.dedup import RedisDedupService
from app.infrastructure.mqtt_client import AsyncMqttPublisher
from app.repositories.device_repository import DeviceRepository
from app.repositories.transaction_repository import TransactionRepository
from app.services.broadcast_service import BroadcastService
from app.services.khqr_service import KhqrService
from app.services.telemetry_service import TelemetryService


async def init_connection(conn: asyncpg.Connection):
    """បង្ខំឱ្យរាល់ Connection ទាំងអស់ប្រើប្រាស់ Timezone របស់ប្រទេសកម្ពុជា"""
    await conn.execute("SET timezone = 'Asia/Phnom_Penh';")


class ApplicationContainer:
    def __init__(self):
        self.db_pool: Optional[asyncpg.Pool] = None
        self.http_client: Optional[httpx.AsyncClient] = None
        self.mqtt_publisher: Optional[AsyncMqttPublisher] = None
        self.mqtt_incoming_queue: queue.Queue = queue.Queue()
        self.correlation_registry: Dict[str, Dict[str, Any]] = {}

        self.dedup_service: Optional[RedisDedupService] = None
        self.device_repo: Optional[DeviceRepository] = None
        self.tx_repo: Optional[TransactionRepository] = None
        self.broadcast_service: Optional[BroadcastService] = None
        self.telemetry_service: Optional[TelemetryService] = None
        self.khqr_service: Optional[KhqrService] = None

    async def initialize(self) -> None:
        # បង្កើត Database Pool ជាមួយ Timezone Hook
        self.db_pool = await asyncpg.create_pool(
            dsn=settings.database_url,
            min_size=5,
            max_size=30,
            command_timeout=10.0,
            init=init_connection,
        )
        self.http_client = httpx.AsyncClient(timeout=settings.http_timeout_seconds)
        self.dedup_service = RedisDedupService()

        self.device_repo = DeviceRepository(self.db_pool)
        self.tx_repo = TransactionRepository(self.db_pool)

        self.mqtt_publisher = AsyncMqttPublisher(
            broker_host=settings.mqtt_broker,
            broker_port=settings.mqtt_port,
            username=settings.mqtt_user,
            password=settings.mqtt_password,
            on_message_callback=lambda t, d: self.mqtt_incoming_queue.put((t, d)),
        )
        self.mqtt_publisher.start()

        self.telemetry_service = TelemetryService(
            self.device_repo, self.tx_repo, self.correlation_registry
        )
        self.broadcast_service = BroadcastService(
            self.device_repo, self.tx_repo, self.mqtt_publisher, self.correlation_registry
        )
        self.khqr_service = KhqrService(self.device_repo, self.mqtt_publisher)

    async def shutdown(self) -> None:
        if self.mqtt_publisher:
            self.mqtt_publisher.stop()
        if self.http_client:
            await self.http_client.aclose()
        if self.db_pool:
            await self.db_pool.close()


container = ApplicationContainer()