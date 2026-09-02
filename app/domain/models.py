from dataclasses import asdict, dataclass
from typing import Any, Dict


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