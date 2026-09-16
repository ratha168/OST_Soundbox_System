from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field


class AckStatus(str, Enum):
    PENDING = "PENDING"
    MQTT_DELIVERED = "MQTT_DELIVERED"
    SPEAKER_PLAYED = "SPEAKER_PLAYED"
    FAILED = "FAILED"
    TIMEOUT = "PLAY_TIMEOUT"


class Currency(str, Enum):
    KHR = "KHR"
    USD = "USD"


class SupplierType(str, Enum):
    FEISHU = "Feishu"
    HEMI = "Hemi"


class Transaction(BaseModel):
    """តំណាងឱ្យប្រតិបត្តិការទូទាត់ដែលទទួលបានពី Bank Notification"""
    txid: str
    amount: float = Field(gt=0, description="ចំនួនទឹកប្រាក់ត្រូវតែធំជាង 0")
    currency: Currency = Currency.KHR
    merchant_id: Optional[int] = None
    chat_id: Optional[str] = None
    raw_payload: Optional[str] = None
    ack_status: AckStatus = AckStatus.PENDING
    is_played: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class DeviceTelemetry(BaseModel):
    """តំណាងឱ្យទិន្នន័យ Telemetry ដែល Soundbox ផ្ញើមកតាមរយៈ getinfo ឬ boot"""
    sn: str
    imei: Optional[str] = None
    imsi: Optional[str] = None
    iccid: Optional[str] = None
    volume: Optional[int] = None
    vlver: Optional[str] = None
    lang: Optional[int] = None
    batt: Optional[int] = None  # តម្លៃគិតជា Millivolt (mV) ឧ. 4043
    adc: Optional[str] = None
    ssid: Optional[str] = None
    mac: Optional[str] = None
    signal: Optional[Any] = None
    verno: Optional[str] = None
    cmd: Optional[str] = None

    @property
    def battery_percentage(self) -> Optional[int]:
        """គណនាភាគរយថ្មស្វ័យប្រវត្តិចន្លោះ 3400mV (0%) ដល់ 4200mV (100%)"""
        if self.batt is None:
            return None
        try:
            mv = float(self.batt)
            if mv <= 100:
                return int(mv)
            return max(0, min(100, int(round(((mv - 3400) / 800) * 100))))
        except (ValueError, TypeError):
            return None