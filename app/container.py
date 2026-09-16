import asyncio
import logging
from typing import Any, Dict, Optional
import aiomqtt
import asyncpg
import redis.asyncio as aioredis

from app.core.config import settings
from app.infrastructure.dedup import RedisDedupService
from app.infrastructure.mqtt_client import AsyncMqttPublisher
from app.repositories.device_repository import DeviceRepository
from app.repositories.transaction_repository import TransactionRepository
from app.services.broadcast_service import BroadcastService
from app.services.khqr_service import KhqrService
from app.services.telemetry_service import TelemetryService

logger = logging.getLogger("Container")


class AppContainer:
    """Dependency Injection Container managing pools, clients, and services."""

    def __init__(self):
        # Database & Cache Clients
        self.db_pool: Optional[asyncpg.Pool] = None
        self.redis: Optional[aioredis.Redis] = None

        # MQTT Infrastructure
        self.mqtt_client: Optional[aiomqtt.Client] = None
        self.mqtt_publisher: Optional[AsyncMqttPublisher] = None
        self.mqtt_incoming_queue: asyncio.Queue = asyncio.Queue()

        # Repositories
        self.device_repo: Optional[DeviceRepository] = None
        self.tx_repo: Optional[TransactionRepository] = None

        # State & Services
        self.correlation_registry: Dict[str, Dict[str, Any]] = {}
        self.dedup_service: Optional[RedisDedupService] = None
        self.broadcast_service: Optional[BroadcastService] = None
        self.telemetry_service: Optional[TelemetryService] = None
        self.khqr_service: Optional[KhqrService] = None

        self._mqtt_listener_task: Optional[asyncio.Task] = None

    async def initialize(self) -> None:
        """Initializes connection pools, MQTT listeners, and dependent services."""
        logger.info("Initializing Application Container resources...")

        # 1. Initialize PostgreSQL Connection Pool
        self.db_pool = await asyncpg.create_pool(
            dsn=settings.database_url,
            min_size=5,
            max_size=20,
        )
        logger.info("Connected to PostgreSQL pool successfully.")

        # 2. Initialize Redis Client & DedupService
        redis_endpoint = getattr(settings, "redis_url", None)
        if not redis_endpoint:
            host = getattr(settings, "redis_host", "ost_redis")
            port = getattr(settings, "redis_port", 6379)
            pwd = getattr(settings, "redis_password", None)
            redis_endpoint = f"redis://:{pwd}@{host}:{port}/0" if pwd else f"redis://{host}:{port}/0"

        self.redis = aioredis.from_url(
            redis_endpoint,
            encoding="utf-8",
            decode_responses=True,
        )
        
        # បញ្ជូន self.redis ចូលទៅក្នុង RedisDedupService
        self.dedup_service = RedisDedupService(self.redis)
        logger.info("Connected to Redis and initialized RedisDedupService.")

        # 3. Initialize Repositories
        self.device_repo = DeviceRepository(self.db_pool)
        self.tx_repo = TransactionRepository(self.db_pool)

       # 4. Initialize MQTT Client & Publisher
        self.mqtt_client = aiomqtt.Client(
            hostname=settings.mqtt_host,
            port=settings.mqtt_port,
            username=settings.mqtt_username,
            password=settings.mqtt_password,
            identifier=f"ost_gateway_{settings.app_env}",
        )
        self.mqtt_publisher = AsyncMqttPublisher(self.mqtt_client)

        # 5. Initialize Services
        self.broadcast_service = BroadcastService(
            device_repo=self.device_repo,
            tx_repo=self.tx_repo,
            mqtt_publisher=self.mqtt_publisher,
            correlation_registry=self.correlation_registry,
        )
        self.telemetry_service = TelemetryService(
            device_repo=self.device_repo,
            tx_repo=self.tx_repo,
            correlation_registry=self.correlation_registry,
        )
        self.khqr_service = KhqrService(
            device_repo=self.device_repo,
            mqtt_publisher=self.mqtt_publisher,
        )

        # 6. Start Background MQTT Subscriber Listener
        self._mqtt_listener_task = asyncio.create_task(self._mqtt_subscriber_loop())
        logger.info("Application Container fully initialized.")

    async def _mqtt_subscriber_loop(self) -> None:
        """Listens to uplink MQTT messages and pushes to incoming queue."""
        reconnect_interval = 3
        while True:
            try:
                async with self.mqtt_client:
                    logger.info("MQTT Client connected to broker.")
                    await self.mqtt_client.subscribe("+/+/data")
                    await self.mqtt_client.subscribe("/LLZN/#")
                    await self.mqtt_client.subscribe("+/+/up")

                    async for message in self.mqtt_client.messages:
                        topic = str(message.topic)
                        payload = message.payload.decode("utf-8", errors="ignore")
                        await self.mqtt_incoming_queue.put((topic, payload))
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("MQTT connection lost: %s. Reconnecting in %ss...", e, reconnect_interval)
                await asyncio.sleep(reconnect_interval)

    async def shutdown(self) -> None:
        """Gracefully closes all active network and database handles."""
        logger.info("Shutting down Application Container...")

        if self._mqtt_listener_task:
            self._mqtt_listener_task.cancel()

        if self.redis:
            await self.redis.aclose()

        if self.db_pool:
            await self.db_pool.close()

        logger.info("All container resources released.")


container = AppContainer()