import re
from abc import ABC
from decimal import Decimal, InvalidOperation
from typing import List, Optional, Pattern
from app.domain.models import Transaction


class TextCleaner:
    ZERO_WIDTH_REGEX: Pattern = re.compile(r"[\u200B-\u200D\uFEFF\u00A0]")

    @classmethod
    def clean(cls, text: Optional[str]) -> str:
        if not text:
            return ""
        return " ".join(cls.ZERO_WIDTH_REGEX.sub(" ", text).split())


class CurrencyNormalizer:
    _MAPPING = {
        "KHR": "KHR", "៛": "KHR", "រៀល": "KHR",
        "USD": "USD", "$": "USD", "ដុល្លារ": "USD",
    }

    @classmethod
    def normalize(cls, raw: Optional[str], default: str = "USD") -> str:
        if not raw:
            return default.upper()
        return cls._MAPPING.get(raw.strip(), default.upper())


class BaseBankExtractor(ABC):
    def __init__(self, bank_name: str, regex_str: str, default_currency: str = "USD"):
        self.bank_name = bank_name
        self.pattern = re.compile(regex_str, re.IGNORECASE | re.DOTALL)
        self.default_currency = default_currency

    def extract(self, text: str) -> Optional[Transaction]:
        match = self.pattern.search(text)
        if not match:
            return None
        data = match.groupdict()
        try:
            amount_clean = data["amount"].replace(",", "").strip()
            amount = float(Decimal(amount_clean))
        except (InvalidOperation, KeyError, ValueError):
            return None

        currency = CurrencyNormalizer.normalize(data.get("currency"), self.default_currency)
        return Transaction(
            bank=self.bank_name,
            txid=str(data.get("txid", "")).strip(),
            amount=amount,
            currency=currency,
            payer=str(data.get("payer", "")).strip(),
        )


class ABAPayWayExtractor(BaseBankExtractor):
    def __init__(self):
        super().__init__(
            bank_name="ABA_PayWay",
            regex_str=r"(?P<currency>៛|\$|KHR|USD)?\s*(?P<amount>[\d,]+(?:\.\d+)?)\s+paid\s+by\s+(?P<payer>.+?)(?:\s*\(\*\d+\))?\s+on\s+.*?Trx\.\s*ID:\s*(?P<txid>\w+)",
            default_currency="USD",
        )


class CanadiaCMCExtractor(BaseBankExtractor):
    def __init__(self):
        super().__init__(
            bank_name="CMC_KHQR",
            regex_str=r"(?P<currency>KHR|USD)\s+(?P<amount>[\d,]+(?:\.\d+)?)\s+is\s+paid\s+by\s+.*?\s+for\s+purchase\s+(?P<txid>[a-zA-Z0-9]+),\s*from\s+(?P<payer>.+?),\s*at\s+",
            default_currency="USD",
        )


class AcledaKhmerExtractor(BaseBankExtractor):
    def __init__(self):
        super().__init__(
            bank_name="ACLEDA",
            regex_str=r"បានទទួល\s+(?P<amount>[\d,]+(?:\.\d+)?)\s+(?P<currency>រៀល|ដុល្លារ|\$|USD|KHR)\s+ពី\s+(?P<payer>.+?),\s*ថ្ងៃទី.+?,\s*លេខយោង\s+(?P<txid>\w+)",
            default_currency="KHR",
        )


class BankNotificationParser:
    _EXTRACTORS: List[BaseBankExtractor] = [
        ABAPayWayExtractor(),
        CanadiaCMCExtractor(),
        AcledaKhmerExtractor(),
    ]

    @classmethod
    def parse(cls, raw_text: Optional[str]) -> Optional[Transaction]:
        cleaned = TextCleaner.clean(raw_text)
        if not cleaned:
            return None
        for extractor in cls._EXTRACTORS:
            result = extractor.extract(cleaned)
            if result:
                return result
        return None