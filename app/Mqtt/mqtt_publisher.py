import json
import logging
import time
from typing import Callable, Optional
import paho.mqtt.client as mqtt

logger = logging.getLogger("soundbox_mqtt")

class SoundboxMQTTPublisher:
    def __init__(
        self,
        broker_host: str,
        broker_port: int = 1883,
        username: Optional[str] = None,
        password: Optional[str] = None,
        on_ack_received: Optional[Callable[[str, dict], None]] = None
    ):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.username = username
        self.password = password
        self.on_ack_received = on_ack_received
        
        self.client = mqtt.Client(client_id=f"fastapi_soundbox_{int(time.time())}")
        if self.username and self.password:
            self.client.username_pw_set(self.username, self.password)

        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self._is_connected = False

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self._is_connected = True
            logger.info("Connected to MQTT Broker successfully.")
            # Subscribe ទៅកាន់ Topic Response របស់ Device ទាំងអស់
            client.subscribe("+/pubmsg", qos=1)
            client.subscribe("/+/pubmsg", qos=1)
            logger.info("Subscribed to device ACK topics: +/pubmsg, /+/pubmsg")
        else:
            self._is_connected = False
            logger.error(f"Failed to connect to MQTT Broker, return code: {rc}")

    def _on_message(self, client, userdata, msg):
        try:
            topic = msg.topic
            payload_raw = msg.payload.decode("utf-8")
            data = json.loads(payload_raw)
            logger.info(f"MQTT Message Received on [{topic}]: {data}")

            if self.on_ack_received:
                self.on_ack_received(topic, data)
        except Exception as e:
            logger.error(f"Error handling incoming MQTT message on {msg.topic}: {e}")

    def connect(self):
        try:
            self.client.connect(self.broker_host, self.broker_port, keepalive=60)
            self.client.loop_start()
        except Exception as e:
            logger.error(f"Cannot start MQTT loop: {e}")

    def disconnect(self):
        try:
            self.client.loop_stop()
            self.client.disconnect()
        except Exception as e:
            logger.error(f"Error disconnecting MQTT: {e}")

    def is_connected(self) -> bool:
        return self._is_connected

    async def publish_voice_payload(self, topic: str, payload: dict, qos: int = 1) -> dict:
        start_time = time.time()
        try:
            payload_str = json.dumps(payload)
            info = self.client.publish(topic, payload_str, qos=qos)
            info.wait_for_publish(timeout=2.0)
            latency = (time.time() - start_time) * 1000.0
            return {"success": True, "latency_ms": round(latency, 2)}
        except Exception as e:
            latency = (time.time() - start_time) * 1000.0
            return {"success": False, "error": str(e), "latency_ms": round(latency, 2)}