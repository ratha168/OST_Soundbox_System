import os
import json
import time
import uuid
import logging
import asyncio
from typing import Optional, Dict, Any, Union
import paho.mqtt.client as mqtt

logger = logging.getLogger("Soundbox_MQTT_Advanced")


class AdvancedSoundboxMQTTPublisher:
    def __init__(
        self,
        broker_host: Optional[str] = None,
        broker_port: Optional[int] = None,
        client_id: Optional[str] = None,
        keepalive: int = 30,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self.broker_host = broker_host or os.getenv("MQTT_BROKER", "mosquitto")
        self.broker_port = int(broker_port or os.getenv("MQTT_PORT", 1883))
        self.keepalive = keepalive
        self.username = username or os.getenv("MQTT_USERNAME")
        self.password = password or os.getenv("MQTT_PASSWORD")
        
        # បង្កើត Unique Client ID ការពារកុំឱ្យជាន់គ្នា
        unique_suffix = uuid.uuid4().hex[:6]
        base_id = client_id or "fastapi_soundbox_publisher"
        self.client_id = f"{base_id}_{unique_suffix}"

        self._connected = False
        self._published_mid_tracker: Dict[int, asyncio.Event] = {}

        # 1. Initialize Paho-MQTT client
        try:
            self.client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                client_id=self.client_id,
                clean_session=True
            )
        except AttributeError:
            self.client = mqtt.Client(client_id=self.client_id, clean_session=True)

        # 2. Configure Credentials
        if self.username and self.password:
            self.client.username_pw_set(self.username, self.password)

        # 3. Callbacks
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_publish = self._on_publish

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        rc_code = rc if isinstance(rc, int) else rc.value
        if rc_code == 0:
            self._connected = True
            logger.info(
                f"[Soundbox_MQTT_Advanced]: MQTT Connected successfully as '{self.client_id}' to {self.broker_host}:{self.broker_port}"
            )
        else:
            self._connected = False
            reasons = {
                1: "Incorrect protocol version",
                2: "Invalid client identifier",
                3: "Server unavailable",
                4: "Bad username or password",
                5: "Not authorized",
            }
            logger.error(
                f"[Soundbox_MQTT_Advanced]: MQTT Connection refused: {reasons.get(rc_code, rc_code)}"
            )

    def _on_disconnect(self, client, userdata, *args, **kwargs):
        self._connected = False
        rc_code = 0
        if args:
            last_arg = args[1] if len(args) > 1 else args[0]
            rc_code = last_arg if isinstance(last_arg, int) else getattr(last_arg, "value", 0)

        if rc_code != 0:
            logger.warning(
                f"[Soundbox_MQTT_Advanced]: Unexpected MQTT disconnection (rc: {rc_code}). Attempting background reconnection..."
            )
        else:
            logger.info("[Soundbox_MQTT_Advanced]: Advanced MQTT Publisher disconnected cleanly.")

    def _on_publish(self, client, userdata, mid, reason_codes=None, properties=None):
        if mid in self._published_mid_tracker:
            self._published_mid_tracker[mid].set()

    def connect(self):
        try:
            logger.info(f"[Soundbox_MQTT_Advanced]: Connecting to MQTT Broker {self.broker_host}:{self.broker_port}...")
            self.client.connect(host=self.broker_host, port=self.broker_port, keepalive=self.keepalive)
            self.client.loop_start()
        except Exception as e:
            logger.error(f"[Soundbox_MQTT_Advanced]: Failed to initiate MQTT connection: {e}", exc_info=True)

    def disconnect(self):
        try:
            self.client.loop_stop()
            self.client.disconnect()
            self._connected = False
            logger.info("[Soundbox_MQTT_Advanced]: MQTT Publisher stopped cleanly.")
        except Exception as e:
            logger.error(f"[Soundbox_MQTT_Advanced]: Error during MQTT disconnect: {e}")

    start = connect
    stop = disconnect

    def is_connected(self) -> bool:
        return bool(self._connected)

    async def publish_voice_payload(
        self,
        topic: str,
        payload: Union[dict, list, str],
        qos: int = 1,
        retain: bool = False,
        timeout: float = 5.0,
    ) -> Dict[str, Any]:
        start_time = time.perf_counter()

        if not self._connected:
            latency = (time.perf_counter() - start_time) * 1000
            return {
                "success": False,
                "status": "error",
                "message": "MQTT Broker is not connected",
                "topic": topic,
                "latency_ms": round(latency, 2),
                "timestamp": int(time.time()),
            }

        if isinstance(payload, (dict, list)):
            formatted_payload = json.dumps(payload, ensure_ascii=False)
        else:
            formatted_payload = str(payload)

        try:
            msg_info = self.client.publish(topic=topic, payload=formatted_payload, qos=qos, retain=retain)
            publish_event = asyncio.Event()
            self._published_mid_tracker[msg_info.mid] = publish_event

            try:
                await asyncio.wait_for(publish_event.wait(), timeout=timeout)
                latency = (time.perf_counter() - start_time) * 1000
                return {
                    "success": True,
                    "status": "delivered",
                    "mid": msg_info.mid,
                    "topic": topic,
                    "qos": qos,
                    "retained": retain,
                    "latency_ms": round(latency, 2),
                    "payload_size_bytes": len(formatted_payload.encode("utf-8")),
                    "timestamp": int(time.time()),
                }
            except asyncio.TimeoutError:
                latency = (time.perf_counter() - start_time) * 1000
                return {
                    "success": False,
                    "status": "timeout",
                    "mid": msg_info.mid,
                    "topic": topic,
                    "message": f"Broker acknowledgment timed out after {timeout}s",
                    "latency_ms": round(latency, 2),
                    "timestamp": int(time.time()),
                }
            finally:
                self._published_mid_tracker.pop(msg_info.mid, None)

        except Exception as e:
            latency = (time.perf_counter() - start_time) * 1000
            return {
                "success": False,
                "status": "failed",
                "topic": topic,
                "error": str(e),
                "latency_ms": round(latency, 2),
                "timestamp": int(time.time()),
            }

    publish = publish_voice_payload


SoundboxMQTTPublisher = AdvancedSoundboxMQTTPublisher