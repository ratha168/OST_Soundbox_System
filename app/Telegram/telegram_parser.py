import re
from dataclasses import dataclass, asdict
from decimal import Decimal
from typing import Optional, Dict, Any, List, Pattern


@dataclass(frozen=True)
class Transaction:
    bank: str
    txid: str
    amount: float
    currency: str
    payer: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class CurrencyNormalizer:
    _MAPPING = {
        "KHR": "KHR",
        "៛": "KHR",
        "រៀល": "KHR",
        "USD": "USD",
        "$": "USD",
        "ដុល្លារ": "USD",
    }

    @classmethod
    def normalize(cls, raw: Optional[str], default: str = "USD") -> str:
        if not raw:
            return default
        return cls._MAPPING.get(raw.strip(), default.upper())


class BankPatternRule:
    __slots__ = ("bank_name", "pattern", "default_currency")

    def __init__(self, bank_name: str, regex_str: str, default_currency: Optional[str] = None):
        self.bank_name = bank_name
        self.pattern: Pattern = re.compile(regex_str, re.IGNORECASE | re.DOTALL)
        self.default_currency = default_currency

    def parse(self, text: str) -> Optional[Transaction]:
        match = self.pattern.search(text)
        if not match:
            return None

        data = match.groupdict()

        # Clean and parse amount safely using Decimal
        amount_clean = data["amount"].replace(",", "").strip()
        amount = float(Decimal(amount_clean))

        # Resolve and normalize currency
        raw_currency = data.get("currency")
        currency = CurrencyNormalizer.normalize(raw_currency, default=self.default_currency or "USD")

        return Transaction(
            bank=self.bank_name,
            txid=data["txid"].strip(),
            amount=amount,
            currency=currency,
            payer=data["payer"].strip()
        )


class BankNotificationParser:
    RULES: List[BankPatternRule] = [
        # 1. CMC / Canadia KHQR Merchant Pattern
        BankPatternRule(
            bank_name="CMC_KHQR",
            regex_str=(
                r"(?P<currency>KHR|USD)\s+(?P<amount>[\d,]+(?:\.\d{2})?)\s+"
                r"is\s+paid\s+by\s+.*?\s+"
                r"for\s+purchase\s+(?P<txid>[a-zA-Z0-9]+),\s*"
                r"from\s+(?P<payer>.+?),\s*at\s+"
            )
        ),
        # 2. ACLEDA / Wing Khmer Pattern
        BankPatternRule(
            bank_name="ACLEDA",
            regex_str=(
                r"បានទទួល\s+(?P<amount>[\d,]+(?:\.\d{2})?)\s+(?P<currency>រៀល|ដុល្លារ|\$|USD|KHR)\s+"
                r"ពី\s+(?P<payer>.+?),\s*"
                r"ថ្ងៃទី.+?,\s*"
                r"លេខយោង\s+(?P<txid>\w+)"
            )
        ),
        # 3. ABA KHQR Merchant Pattern
        BankPatternRule(
            bank_name="ABA_KHQR",
            regex_str=(
                r"(?P<currency>៛|\$)?\s*(?P<amount>[\d,]+(?:\.\d{2})?)\s+"
                r"paid\s+by\s+(?P<payer>.+?)(?:\s*\(\*\d+\))?\s+"
                r"on\s+.*?"
                r"Trx\.\s*ID:\s*(?P<txid>\w+)"
            ),
            default_currency="USD"
        ),
        # 4. ABA Standard App Notification
        BankPatternRule(
            bank_name="ABA",
            regex_str=r"Received\s+\$(?P<amount>\d+(?:\.\d{2})?)\s+from\s+(?P<payer>.+?)\s*\(Trx\s*ID:\s*(?P<txid>\w+)\)",
            default_currency="USD"
        ),
        # 5. ACLEDA English Pattern
        BankPatternRule(
            bank_name="ACLEDA",
            regex_str=r"Received\s+(?P<amount>[\d,]+(?:\.\d{2})?)\s+(?P<currency>KHR|USD)\s+from\s+(?P<payer>.+?)\s*\(Ref:\s*(?P<txid>\w+)\)"
        ),
    ]

    @classmethod
    def clean_text(cls, text: str) -> str:
        """Removes zero-width characters and normalizes Unicode whitespace."""
        text = re.sub(r"[\u200B-\u200D\uFEFF]", "", text)
        return " ".join(text.split())

    @classmethod
    def parse_message(cls, text: str) -> Optional[Dict[str, Any]]:
        """Parses notification text and returns a Dict (backward compatible)."""
        tx = cls.parse(text)
        return tx.to_dict() if tx else None

    @classmethod
    def parse(cls, text: str) -> Optional[Transaction]:
        """Parses notification text and returns a typed Transaction instance."""
        if not text:
            return None

        normalized_text = cls.clean_text(text)

        for rule in cls.RULES:
            result = rule.parse(normalized_text)
            if result:
                return result

        return None


# Module-level alias to satisfy `from app.telegram_parser import parse_bank_message` in main.py
def parse_bank_message(text: str) -> Optional[Dict[str, Any]]:
    return BankNotificationParser.parse_message(text)