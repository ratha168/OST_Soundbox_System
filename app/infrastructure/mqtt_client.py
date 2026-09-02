import asyncio
import json
import logging
import time
from typing import Any, Callable, Dict, List, Optional
import paho.mqtt.client as mqtt

logger = logging.getLogger("MqttPublisher")


class AsyncMqttPublisher:
    """Production Non-blocking MQTT Publisher with automatic reconnection."""

    def __init__(
        self,
        broker_host: str,
        broker_port: int = 1883,
        username: Optional[str] = None,
        password: Optional[str] = None,
        on_message_callback: Optional[Callable[[str, dict], None]] = None,
        listen_topics: Optional[List[str]] = None,
    ):
        self._broker_host = broker_host
        self._broker_port = broker_port
        self._username = username
        self._password = password
        self._on_message_callback = on_message_callback
        self._listen_topics = listen_topics or ["/LLZN/#", "+/data", "pubmsg/#"]

        self.client_id = f"ost_soundbox_gateway_{int(time.time() * 1000)}"
        
        try:
            self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=self.client_id)
        except AttributeError:
            self._client = mqtt.Client(client_id=self.client_id)

        if self._username and self._password:
            self._client.username_pw_set(self._username, self._password)

        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message
        self._is_connected = False

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            self._is_connected = True
            logger.info(f"Connected to MQTT Broker [{self._broker_host}:{self._broker_port}]")
            for topic in self._listen_topics:
                client.subscribe(topic, qos=1)
                logger.info(f"Subscribed to topic: {topic}")
        else:
            self._is_connected = False
            logger.error(f"Failed to connect to MQTT, return code: {rc}")

    def _on_disconnect(self, client, userdata, rc, properties=None):
        self._is_connected = False
        if rc != 0:
            logger.warning("Unexpected MQTT disconnection. Auto-reconnecting...")

    def _on_message(self, client, userdata, msg):
        try:
            payload_str = msg.payload.decode("utf-8").strip()
            if not payload_str:
                return
            data = json.loads(payload_str)
            if self._on_message_callback:
                self._on_message_callback(msg.topic, data)
        except Exception as e:
            logger.error(f"Failed to parse payload on {msg.topic}: {e}")

    def start(self) -> None:
        self._client.connect(self._broker_host, self._broker_port, keepalive=60)
        self._client.loop_start()

    def stop(self) -> None:
        self._client.loop_stop()
        self._client.disconnect()
        self._is_connected = False

    def is_connected(self) -> bool:
        return self._is_connected

    async def publish(
        self, topic: str, payload: Dict[str, Any], qos: int = 1, timeout: float = 2.0
    ) -> Dict[str, Any]:
        start_time = time.time()
        try:
            payload_bytes = json.dumps(payload, ensure_ascii=False)
            loop = asyncio.get_running_loop()

            def _sync_publish():
                info = self._client.publish(topic, payload_bytes, qos=qos)
                info.wait_for_publish(timeout=timeout)
                return info

            await loop.run_in_executor(None, _sync_publish)
            latency = (time.time() - start_time) * 1000.0
            return {"success": True, "latency_ms": round(latency, 2)}
        except Exception as e:
            latency = (time.time() - start_time) * 1000.0
            logger.error(f"MQTT publish failed to {topic}: {e}")
            return {"success": False, "error": str(e), "latency_ms": round(latency, 2)}