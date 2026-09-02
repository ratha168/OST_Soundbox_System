import logging
import time
from typing import Any, Dict, List, Optional
from app.domain.models import Transaction
from app.domain.suppliers import SupplierFactory
from app.infrastructure.mqtt_client import AsyncMqttPublisher
from app.repositories.device_repository import DeviceRepository
from app.repositories.transaction_repository import TransactionRepository

logger = logging.getLogger("BroadcastService")


class BroadcastService:
    """Orchestrates transaction routing to multiple soundbox suppliers."""

    def __init__(
        self,
        device_repo: DeviceRepository,
        tx_repo: TransactionRepository,
        mqtt_publisher: AsyncMqttPublisher,
        correlation_registry: Dict[str, Dict[str, Any]],
    ):
        self._device_repo = device_repo
        self._tx_repo = tx_repo
        self._mqtt_pub = mqtt_publisher
        self._correlation_registry = correlation_registry

    async def broadcast(self, tx: Transaction, chat_id: str, raw_text: str = "") -> Optional[List[str]]:
        devices = await self._device_repo.get_active_devices_by_chat_id(chat_id)
        if not devices:
            logger.warning(f"No active device registered to Chat ID: {chat_id}")
            return None

        primary_device_id = devices[0]["device_id"]
        unique_msg_id = str(int(time.time() * 1000))[-10:]

        inserted = await self._tx_repo.create_transaction(
            device_id=primary_device_id,
            txid=tx.txid,
            chat_id=chat_id,
            amount=tx.amount,
            currency=tx.currency,
            raw_payload=raw_text,
            ack_status="MQTT_DELIVERED",
        )
        if not inserted:
            logger.warning(f"Duplicate TxID '{tx.txid}' rejected by DB constraint.")
            return None

        dispatched_devices = []
        for dev in devices:
            sn = str(dev["device_id"]).strip()
            supplier_name = str(dev["supplier"]).strip()

            self._correlation_registry[f"{sn}:{unique_msg_id}"] = {
                "txid": tx.txid,
                "timestamp": time.time(),
            }

            supplier = SupplierFactory.get(supplier_name)
            topic = supplier.get_downlink_topic(sn)
            payload = supplier.build_payment_payload(
                device_sn=sn,
                amount=tx.amount,
                currency=tx.currency,
                message_id=unique_msg_id,
            )

            res = await self._mqtt_pub.publish(topic=topic, payload=payload, qos=1)
            if res.get("success"):
                dispatched_devices.append(sn)
                logger.info(f"Dispatched to {supplier_name.upper()} ({sn}) in {res.get('latency_ms')}ms")
            else:
                logger.error(f"Dispatch error to {sn}: {res.get('error')}")

        return dispatched_devices