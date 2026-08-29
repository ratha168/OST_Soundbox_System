import json
import logging
import time
from typing import Optional
import paho.mqtt.client as mqtt

logger = logging.getLogger("Soundbox_MQTT_Advanced")

class AdvancedSoundboxMQTTPublisher:
    def __init__(
        self, 
        broker_host: str = "mosquitto", 
        broker_port: int = 1883, 
        client_id: str = "fastapi_soundbox_publisher_adv",
        keepalive: int = 60,
        username: Optional[str] = None,
        password: Optional[str] = None
    ):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.keepalive = keepalive
        self._is_connected = False

        # 1. Initialize Paho-MQTT v2.x with fallback to v1.x
        try:
            self.client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2, 
                client_id=client_id
            )
        except AttributeError:
            self.client = mqtt.Client(client_id=client_id)

        # Optional Authentication
        if username and password:
            self.client.username_pw_set(username, password)

        # Register Callbacks
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_publish = self._on_publish

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            self._is_connected = True
            logger.info(f"MQTT Connected successfully to broker at {self.broker_host}:{self.broker_port}")
        else:
            self._is_connected = False
            logger.error(f"MQTT Connection refused with error code: {rc}")

    def _on_disconnect(self, client, userdata, flags, rc=None, properties=None):
        self._is_connected = False
        if rc != 0:
            logger.warning(f"Unexpected MQTT disconnection (rc: {rc}). Attempting background reconnection...")
        else:
            logger.info("MQTT Disconnected gracefully.")

    def _on_publish(self, client, userdata, mid, reason_code=None, properties=None):
        logger.debug(f"MQTT Message ID {mid} published successfully.")

    def connect(self):
        """Connect to the MQTT broker and start the background network loop."""
        try:
            logger.info(f"Connecting to MQTT Broker {self.broker_host}:{self.broker_port}...")
            self.client.connect(self.broker_host, self.broker_port, keepalive=self.keepalive)
            self.client.loop_start()
        except Exception as e:
            logger.error(f"Critical MQTT Connection Error: {e}")
            raise e

    def is_connected(self) -> bool:
        return self._is_connected

    def send_payment_notification(
        self, 
        device_sn: str, 
        amount: float, 
        currency: str, 
        txid: str, 
        timeout: float = 3.0
    ) -> bool:
        """Publish a targeted payment command to a specific soundbox device with synchronous confirmation."""
        topic = f"soundbox/device/{device_sn}/command"
        payload = {
            "cmd": "play_sound",
            "msg_id": txid,
            "amount": f"{amount:.2f}",
            "currency": currency,
            "voice_pack": "km_KH",
            "display_text": f"Received {currency} {amount:,.2f}",
            "timestamp": int(time.time())
        }
        
        return self._publish_payload(topic, payload, timeout=timeout)

    def broadcast_payment_notification(
        self, 
        amount: float, 
        currency: str, 
        txid: str, 
        timeout: float = 3.0
    ) -> bool:
        """Broadcast a payment command to all connected soundbox devices."""
        topic = "soundbox/global/command"
        payload = {
            "cmd": "play_sound",
            "msg_id": txid,
            "amount": f"{amount:.2f}",
            "currency": currency,
            "voice_pack": "km_KH",
            "display_text": f"Received {currency} {amount:,.2f}",
            "timestamp": int(time.time())
        }
        
        return self._publish_payload(topic, payload, timeout=timeout)

    def _publish_payload(self, topic: str, payload: dict, timeout: float) -> bool:
        """Internal helper to handle publishing, QoS 1, and delivery confirmation blocking timeout."""
        if not self._is_connected:
            logger.warning(f"Cannot publish to {topic}: MQTT client is currently disconnected.")
            return False

        try:
            message_json = json.dumps(payload)
            result = self.client.publish(topic, message_json, qos=1)
            
            # Wait for network socket transmission confirmation (QoS 1 acknowledgment)
            result.wait_for_publish(timeout=timeout)
            
            if result.is_published():
                logger.info(f"MQTT Published successfully -> Topic: {topic} | TxID: {payload.get('msg_id')}")
                return True
            else:
                logger.error(f"MQTT Publish timeout/failure -> Topic: {topic}")
                return false
        except Exception as e:
            logger.error(f"Exception occurred while publishing to {topic}: {e}")
            return False

    def disconnect(self):
        """Gracefully stop the network loop and disconnect from the broker."""
        try:
            self.client.loop_stop()
            self.client.disconnect()
            logger.info("Advanced MQTT Publisher disconnected cleanly.")
        except Exception as e:
            logger.error(f"Error during MQTT disconnection: {e}")