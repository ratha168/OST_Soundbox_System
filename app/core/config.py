import os
from typing import Optional
from pydantic import Field, AliasChoices
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # App Settings
    app_env: str = "production"
    app_port: int = 8000
    api_secret_key: str = "your-api-key"

    # PostgreSQL Database (គាំទ្រទាំង ost_postgres និង postgres network alias)
    database_url: str = Field(
        default="postgresql://postgres:fDdiFw_KB2930otN@ost_postgres:5432/postgres",
        validation_alias=AliasChoices("DATABASE_URL", "database_url"),
    )

    # Redis Configurations
    redis_host: str = Field(
        default="ost_redis",
        validation_alias=AliasChoices("REDIS_HOST", "redis_host"),
    )
    redis_port: int = Field(
        default=6379,
        validation_alias=AliasChoices("REDIS_PORT", "redis_port"),
    )
    redis_password: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("REDIS_PASSWORD", "redis_password"),
    )
    redis_db: int = 0

    @property
    def redis_url(self) -> str:
        """ផ្គុំ Redis Connection URL ដោយស្វ័យប្រវត្តិ"""
        if self.redis_password:
            return f"redis://:{self.redis_password}@{self.redis_host}:{self.redis_port}/{self.redis_db}"
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"

    # MQTT Broker
    mqtt_host: str = Field(
        default="ost_mosquitto",
        validation_alias=AliasChoices("MQTT_HOST", "MQTT_BROKER", "mqtt_host", "mqtt_broker"),
    )
    mqtt_port: int = Field(
        default=1883,
        validation_alias=AliasChoices("MQTT_PORT", "mqtt_port"),
    )
    mqtt_username: Optional[str] = Field(
        default="gateway_user",
        validation_alias=AliasChoices("MQTT_USER", "MQTT_USERNAME", "mqtt_username", "mqtt_user"),
    )
    mqtt_password: Optional[str] = Field(
        default="GatewaySecurePass2026",
        validation_alias=AliasChoices("MQTT_PASSWORD", "mqtt_password"),
    )

    # Telegram Userbot
    telegram_api_id: int = Field(
        default=34687255,
        validation_alias=AliasChoices("TELEGRAM_API_ID", "telegram_api_id"),
    )
    telegram_api_hash: str = Field(
        default="0c8a94e104d60fe54bf05605122ae878",
        validation_alias=AliasChoices("TELEGRAM_API_HASH", "telegram_api_hash"),
    )
    telegram_session: str = Field(
        default="userbot_session",
        validation_alias=AliasChoices("TELEGRAM_USERBOT_SESSION", "TELEGRAM_SESSION", "telegram_session"),
    )
    telegram_phone: Optional[str] = None

    # Timeouts & Webhook Endpoint (ប្រើ host ost_api_gateway ឬ api-gateway)
    fastapi_userbot_url: str = Field(
        default="http://ost_api_gateway:8000/webhook/telegram-userbot",
        validation_alias=AliasChoices("FASTAPI_USERBOT_URL", "fastapi_userbot_url"),
    )
    ack_timeout_seconds: float = 20.0
    http_timeout_seconds: float = 10.0

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()