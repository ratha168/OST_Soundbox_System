from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional,Union
from pydantic import BaseModel, Field, field_validator

@dataclass(frozen=True)
class Transaction:
    bank: str
    txid: str
    amount: float
    currency: str
    payer: str

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
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