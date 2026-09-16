import asyncio
import logging
import time
from typing import Any, Dict, Optional, Union
import aiomqtt

logger = logging.getLogger("MqttPublisher")


class AsyncMqttPublisher:
    """Thread-safe and asynchronous MQTT message dispatcher with connection safety."""

    def __init__(self, client: aiomqtt.Client):
        self._client = client
        self._lock = asyncio.Lock()

    def is_connected(self) -> bool:
        """ពិនិត្យស្ថានភាព Connection របស់ aiomqtt client"""
        try:
            # ផ្ទៀងផ្ទាត់ underlying paho-mqtt socket
            return bool(self._client._client and self._client._client.is_connected())
        except Exception:
            return True

    async def publish(
        self, topic: str, payload: Union[str, bytes], qos: int = 1
    ) -> Dict[str, Any]:
        start_time = time.time()
        clean_topic = str(topic).strip()

        # Encode Payload ប្រសិនបើជា String
        if isinstance(payload, str):
            payload_data = payload.encode("utf-8")
        else:
            payload_data = payload

        try:
            async with self._lock:
                # ប្រសិនបើ client មិនទាន់ស្ថិតក្នុង async context ត្រូវ wrap async with
                try:
                    await self._client.publish(topic=clean_topic, payload=payload_data, qos=qos)
                except (aiomqtt.MqttError, RuntimeError):
                    async with self._client:
                        await self._client.publish(topic=clean_topic, payload=payload_data, qos=qos)

            latency = round((time.time() - start_time) * 1000, 2)
            logger.info("📡 [MQTT SENT] Topic: %s (QoS %s) in %sms", clean_topic, qos, latency)
            return {"success": True, "latency_ms": latency}

        except Exception as e:
            logger.error("❌ Failed to publish payload to %s: %s", clean_topic, e, exc_info=True)
            return {"success": False, "error": str(e)}