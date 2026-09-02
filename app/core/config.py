import os
from typing import Optional
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    database_url: str = os.getenv("DATABASE_URL", "postgresql://postgres:fDdiFw_KB2930otN@postgres:5432/postgres")
    redis_host: str = os.getenv("REDIS_HOST", "redis")
    redis_port: int = int(os.getenv("REDIS_PORT", 6379))
    
    mqtt_broker: str = os.getenv("MQTT_HOST", os.getenv("MQTT_BROKER", "mosquitto"))
    mqtt_port: int = int(os.getenv("MQTT_PORT", 1883))
    mqtt_user: Optional[str] = os.getenv("MQTT_USER", "gateway_user")
    mqtt_password: Optional[str] = os.getenv("MQTT_PASSWORD", "GatewaySecurePass2026")
    
    telegram_api_id: int = int(os.getenv("TELEGRAM_API_ID", 34687255))
    telegram_api_hash: str = os.getenv("TELEGRAM_API_HASH", "0c8a94e104d60fe54bf05605122ae878")
    telegram_session: str = os.getenv("TELEGRAM_USERBOT_SESSION", "userbot_session")
    
    fastapi_userbot_url: str = os.getenv("FASTAPI_USERBOT_URL", "http://fastapi-gateway:8000/webhook/telegram-userbot")
    api_secret_key: str = os.getenv("API_SECRET_KEY", "your-api-key")
    ack_timeout_seconds: float = 20.0
    http_timeout_seconds: float = 10.0

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()