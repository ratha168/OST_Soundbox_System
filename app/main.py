import json
import logging
import os
from typing import Optional, Dict, Any
from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor, Json
import paho.mqtt.client as mqtt

from app.telegram_parser import parse_bank_message

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("IoT_FastAPI")

app = FastAPI(title="IoT Soundbox Gateway")

DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "fDdiFw_KB2930otN")

MQTT_BROKER = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_USER = os.getenv("MQTT_USER", "gateway_user")
MQTT_PASS = os.getenv("MQTT_PASSWORD", "GatewaySecurePass$@123")

def get_db():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

def publish_mqtt(topic: str, payload: dict):
    client = mqtt.Client()
    try:
        client.username_pw_set(MQTT_USER, MQTT_PASS)
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.publish(topic, json.dumps(payload))
        client.disconnect()
        logger.info(f"==> [MQTT PUBLISHED] Topic: {topic} | Payload: {json.dumps(payload)}")
    except Exception as e:
        logger.error(f"==> [MQTT ERROR] Failed to publish: {e}")

class UserbotPayload(BaseModel):
    telegram_chat_id: Optional[str] = None
    chat_id: Optional[str] = None
    telegram_user_id: Optional[str] = None
    user_id: Optional[str] = None
    raw_message: Optional[str] = None
    text: Optional[str] = None
    username: Optional[str] = None
    full_name: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    is_bot: Optional[bool] = False
    forward_from_chat_id: Optional[str] = None

@app.post("/webhook/telegram-userbot")
async def telegram_userbot_webhook(data: UserbotPayload, x_api_key: Optional[str] = Header(None)):
    chat_id = str(data.telegram_chat_id or data.chat_id or "").strip()
    raw_text = (data.raw_message or data.text or "").strip()

    logger.info("=" * 60)
    logger.info(f">>> [INCOMING REQUEST] Chat: {chat_id} | Message: {raw_text}")
    logger.info("=" * 60)

    parsed = parse_bank_message(raw_text)
    if not parsed:
        logger.warning(f"--- [PARSE FAILED] Non-bank message: '{raw_text}'")
        return {"status": "ignored", "reason": "Not a bank transaction message"}

    logger.info(f"+++ [PARSE SUCCESS] Result: {parsed}")

    conn = None
    cursor = None
    try:
        conn = get_db()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("SELECT device_sn, merchant_id, status FROM devices WHERE telegram_chat_id = %s LIMIT 1;", (chat_id,))
        device = cursor.fetchone()
        
        if not device:
            logger.warning(f"--- [DEVICE NOT FOUND] No device linked to Chat ID: {chat_id}")
            return {"status": "ignored", "reason": f"No device registered for chat {chat_id}", "parsed": parsed}

        device_sn = device["device_sn"]
        logger.info(f"+++ [DEVICE FOUND] SN: {device_sn}")

        json_data = Json({
            "raw_text": raw_text,
            "parsed": parsed,
            "sender_id": data.telegram_user_id or data.user_id,
            "username": data.username,
            "full_name": data.full_name
        })

        bank_tx_id = str(parsed.get("transaction_id")) if parsed.get("transaction_id") else None

        if bank_tx_id:
            cursor.execute(
                """
                INSERT INTO transactions (bank_name, bank_tx_id, amount, currency, device_sn, status, raw_payload)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (bank_name, bank_tx_id) DO NOTHING
                RETURNING id, created_at;
                """,
                (
                    str(parsed.get("bank") or "ABA"),
                    bank_tx_id,
                    float(parsed.get("amount") or 0.0),
                    str(parsed.get("currency") or "USD"),
                    str(device_sn),
                    "SUCCESS",
                    json_data
                )
            )
        else:
            cursor.execute(
                """
                INSERT INTO transactions (bank_name, bank_tx_id, amount, currency, device_sn, status, raw_payload)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (bank_name, bank_tx_id) DO NOTHING
                RETURNING id, created_at;
                """,
                (
                    str(parsed.get("bank") or "ABA"),
                    None,
                    float(parsed.get("amount") or 0.0),
                    str(parsed.get("currency") or "USD"),
                    str(device_sn),
                    "SUCCESS",
                    json_data
                )
            )

        tx_row = cursor.fetchone()
        conn.commit()

        if not tx_row:
            logger.warning(f"--- [DUPLICATE TRANSACTION] Bank Tx ID {bank_tx_id} already exists in DB. Skipped.")
            return {"status": "ignored", "reason": "Transaction already processed"}

        tx_id = tx_row['id']
        logger.info(f"+++ [DB SAVED] Transaction ID: {tx_id} at {tx_row['created_at']}")

        mqtt_topic = f"soundbox/{device_sn}/voice"
        voice_payload = {
            "amount": str(parsed["amount"]),
            "currency": str(parsed["currency"]),
            "bank": str(parsed["bank"]),
            "tx_id": str(tx_id)
        }
        publish_mqtt(mqtt_topic, voice_payload)

        res = {
            "status": "success",
            "transaction_id": tx_id,
            "device_sn": device_sn,
            "data": voice_payload
        }
        logger.info(f"<<< [RESPONSE SUCCESS] {res}\n" + "=" * 60)
        return res

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"!!! [GATEWAY EXCEPTION] {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.post("/api/users/sync")
async def sync_user(request: Request):
    return {"status": "ok"}