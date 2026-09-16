import json
import logging
import time
import uuid
from typing import Any, Dict, List, Optional
from app.domain.models import Transaction, AckStatus, Currency
from app.domain.suppliers import SupplierFactory
from app.infrastructure.mqtt_client import AsyncMqttPublisher
from app.repositories.device_repository import DeviceRepository
from app.repositories.transaction_repository import TransactionRepository

logger = logging.getLogger("BroadcastService")


class BroadcastService:
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

    @staticmethod
    def _get_short_sn(device_sn: str) -> str:
        clean = str(device_sn).strip()
        return clean[-7:] if len(clean) >= 7 else clean

    @staticmethod
    def _resolve_supplier_name(dev: Dict[str, Any]) -> str:
        """Map supplier_id (1: Feishu, 2: Hemi) ឬអានពី supplier text"""
        supp = dev.get("supplier")
        if supp:
            return str(supp).strip()

        supplier_id = dev.get("supplier_id")
        if supplier_id == 2:
            return "Hemi"
        return "Feishu"

    async def broadcast(self, tx: Transaction, chat_id: str, raw_text: str = "") -> Optional[List[str]]:
        logger.info(f"🔎 [BROADCAST START] Querying devices for chat_id: '{chat_id}'")
        devices = await self._device_repo.get_active_devices_by_chat_id(chat_id)
        if not devices:
            logger.warning(f"⚠️ No active device registered to Chat ID: '{chat_id}'")
            return None

        logger.info(f"📱 Found {len(devices)} matching device(s) for chat_id: {chat_id}")
        primary_device_id = str(devices[0]["device_id"]).strip()
        unique_msg_id = uuid.uuid4().hex[:10]

        curr_str = tx.currency.value if isinstance(tx.currency, Currency) else str(tx.currency)

        # កត់ត្រា Transaction ចូល PostgreSQL
        try:
            inserted = await self._tx_repo.create_transaction(
                device_id=primary_device_id,
                txid=tx.txid,
                chat_id=chat_id,
                amount=tx.amount,
                currency=curr_str,
                raw_payload=raw_text,
                ack_status=AckStatus.MQTT_DELIVERED.value,
            )
        except Exception as e:
            logger.error(f"❌ DB insert error for TxID '{tx.txid}': {e}")
            inserted = True  # បន្តទៅ publish ទោះ record db បរាជ័យ ដើម្បីកុំឱ្យស្ងាត់សំឡេង

        if not inserted:
            logger.warning(f"⚠️ Duplicate TxID '{tx.txid}' rejected by DB constraint, continuing dispatch anyway...")

        dispatched_devices = []
        for dev in devices:
            full_sn = str(dev["device_id"]).strip()
            short_sn = self._get_short_sn(full_sn)
            supplier_name = self._resolve_supplier_name(dev)

            registry_entry = {
                "txid": tx.txid,
                "device_id": full_sn,
                "short_sn": short_sn,
                "timestamp": time.time(),
            }
            self._correlation_registry[f"{full_sn}:{unique_msg_id}"] = registry_entry
            self._correlation_registry[f"{short_sn}:{unique_msg_id}"] = registry_entry

            supplier = SupplierFactory.get(supplier_name)
            target_sn = short_sn if supplier_name.lower() == "feishu" else full_sn

            # កំណត់ Topic
            if supplier_name.lower() == "feishu":
                product_key = dev.get("product_key") or "XHKX8L740B"
                topic = f"{product_key}/{target_sn}/down"
            else:
                topic = supplier.get_downlink_topic(target_sn)

            payload = supplier.build_payment_payload(
                device_sn=target_sn,
                amount=tx.amount,
                currency=curr_str,
                message_id=unique_msg_id,
            )

            # ប្រាកដថា payload ជា string ឬ json format
            if isinstance(payload, dict):
                payload_str = json.dumps(payload)
            else:
                payload_str = str(payload)

            logger.info(f"🚀 Publishing to MQTT -> Topic: [{topic}] | Payload: {payload_str}")

            res = await self._mqtt_pub.publish(topic=topic, payload=payload_str, qos=1)
            if res.get("success"):
                dispatched_devices.append(full_sn)
                logger.info(f"✅ Dispatched to {supplier_name.upper()} ({full_sn}) in {res.get('latency_ms')}ms")
            else:
                logger.error(f"❌ Dispatch error to {full_sn}: {res.get('error')}")

        return dispatched_devices if dispatched_devices else None