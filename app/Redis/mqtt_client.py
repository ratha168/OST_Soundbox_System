import json
import paho.mqtt.publish as publish
import os

MQTT_BROKER = os.getenv("MQTT_HOST", "iot_mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_USER = os.getenv("MQTT_USER", "device_admin")
MQTT_PASS = os.getenv("MQTT_PASSWORD", "secret123")

def trigger_iot_device(device_id: str, tx_data: dict):
    topic = f"devices/{device_id}/command"
    payload = {
        "action": "PAYMENT_SUCCESS",
        "amount": tx_data["amount"],
        "currency": tx_data["currency"],
        "txid": tx_data["txid"],
        "payer": tx_data["payer"]
    }
    
    auth = {"username": MQTT_USER, "password": MQTT_PASS} if MQTT_USER else None
    
    publish.single(
        topic=topic,
        payload=json.dumps(payload),
        hostname=MQTT_BROKER,
        port=MQTT_PORT,
        auth=auth
    )