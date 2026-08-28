import json
import logging
import paho.mqtt.client as mqtt

logger = logging.getLogger(__name__)

class SoundboxMQTTPublisher:
    def __init__(self, broker_host: str = "localhost", broker_port: int = 1883):
        self.client = mqtt.Client()
        self.broker_host = broker_host
        self.broker_port = broker_port

    def connect(self):
        try:
            self.client.connect(self.broker_host, self.broker_port, keepalive=60)
            self.client.loop_start()
            logger.info(f"Connected to MQTT Broker: {self.broker_host}:{self.broker_port}")
        except Exception as e:
            logger.error(f"MQTT Connection Error: {e}")

    def send_payment_notification(self, device_sn: str, amount: float, currency: str, txid: str):
        # Y6B Soundbox Specific Topic Convention
        topic = f"soundbox/device/{device_sn}/command"
        payload = {
            "cmd": "play_sound",
            "msg_id": txid,
            "amount": str(amount),
            "currency": currency,
            "voice_pack": "km_KH", # សំឡេងភាសាខ្មែរ
            "display_text": f"Received {currency} {amount}"
        }
        result = self.client.publish(topic, json.dumps(payload), qos=1)
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            logger.info(f"Successfully published payment signal to {topic}")
        else:
            logger.error(f"Failed to publish MQTT message to {topic}")

    def broadcast_payment_notification(self, amount: float, currency: str, txid: str):
        # Topic សម្រាប់ Soundbox ទាំងអស់ Subscribe រួមគ្នា
        topic = "soundbox/global/command"
        payload = {
            "cmd": "play_sound",
            "msg_id": txid,
            "amount": str(amount),
            "currency": currency,
            "voice_pack": "km_KH",
            "display_text": f"Received {currency} {amount}"
        }
        result = self.client.publish(topic, json.dumps(payload), qos=1)
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            logger.info(f"Successfully broadcasted payment signal to {topic}")
        else:
            logger.error(f"Failed to broadcast MQTT message to {topic}")

    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()