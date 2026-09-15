from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field

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
    """Pydantic model for incoming 'getinfo' device telemetry (4G & WiFi)."""
    cmd: str
    sn: str
    
    # Shared Fields
    volume: Optional[str] = None
    batt: Optional[int] = None
    lang: Optional[int] = None
    verno: Optional[str] = None
    vlver: Optional[str] = None
    
    # 4G Specific Fields
    imei: Optional[str] = None
    imsi: Optional[str] = None
    iccid: Optional[str] = None
    signal: Optional[int] = None
    
    # WiFi Specific Fields
    adc: Optional[int] = None
    ssid: Optional[str] = None
    mac: Optional[str] = None

    @property
    def battery_percentage(self) -> str:
        """Converts millivolts to an estimated percentage string."""
        if not self.batt:
            return "0%"
        # Based on docs: > 4100mV is full, < 3400mV shuts down
        if self.batt >= 4100:
            return "100%"
        elif self.batt <= 3400:
            return "0%"
        else:
            pct = int(((self.batt - 3400) / (4100 - 3400)) * 100)
            return f"{pct}%"