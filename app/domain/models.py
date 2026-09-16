from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional, Union
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
    """Represents a financial payment notification mapped for soundbox dispatch."""
    txid: str
    amount: float = Field(gt=0, description="ចំនួនទឹកប្រាក់ត្រូវតែធំជាង 0")
    currency: Currency = Currency.KHR
    bank: Optional[str] = "Unknown"
    payer: Optional[str] = None
    merchant_id: Optional[int] = None
    chat_id: Optional[str] = None
    raw_payload: Optional[str] = None
    ack_status: AckStatus = AckStatus.PENDING
    is_played: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        data = self.model_dump()
        data["bank_name"] = self.bank
        return data


class DeviceTelemetry(BaseModel):
    cmd: str = "getinfo"
    sn: str
    volume: Optional[Union[str, int]] = None
    vlver: Optional[str] = None
    lang: Optional[Union[str, int]] = None
    batt: Optional[Union[str, int]] = None
    verno: Optional[str] = None

    # សម្រាប់ម៉ូដែល 4G
    imei: Optional[str] = None
    imsi: Optional[str] = None
    iccid: Optional[str] = None
    signal: Optional[Union[str, int]] = None

    # សម្រាប់ម៉ូដែល Wi-Fi
    adc: Optional[Union[str, int]] = None
    ssid: Optional[str] = None
    mac: Optional[str] = None

    @property
    def battery_percentage(self) -> Optional[int]:
        if self.batt is None:
            return None
        try:
            mv = float(self.batt)
            # តាម Spec: 4100mV = 100%, 3400mV = 0%
            pct = int(round(((mv - 3400) / (4100 - 3400)) * 100))
            return max(0, min(100, pct))
        except (ValueError, TypeError):
            return None