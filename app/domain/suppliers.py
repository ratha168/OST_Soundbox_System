import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class BaseSoundboxSupplier(ABC):
    """Abstract Strategy interface for soundbox hardware vendors."""

    @abstractmethod
    def get_downlink_topic(self, device_sn: str) -> str:
        """MQTT command topic to dispatch payment announcements."""
        pass

    @abstractmethod
    def build_payment_payload(
        self, device_sn: str, amount: float, currency: str, message_id: str
    ) -> Dict[str, Any]:
        """Constructs vendor-compliant JSON voice payload."""
        pass


class HemiSupplier(BaseSoundboxSupplier):
    """Strategy implementation for HEMI Cloud Speakers."""

    def get_downlink_topic(self, device_sn: str) -> str:
        return f"/LLZN/{device_sn}"

    def build_payment_payload(
        self, device_sn: str, amount: float, currency: str, message_id: str
    ) -> Dict[str, Any]:
        return {
            "message_id": message_id,
            "time_stamp": str(int(time.time())),
            "device_sn": device_sn,
            "packet_type": "payment",
            "content": {
                "play_payment_amount": float(amount),
                "currency_type": "USD" if currency.upper() == "USD" else "KHR",
            },
        }


class FeishuSupplier(BaseSoundboxSupplier):
    """Strategy implementation for Feishu 3-digit audio-sliced speakers."""

    CODE_PROMPT_RECEIVED = "000"
    CODE_CURRENCY_USD = "001"
    CODE_CURRENCY_KHR = "002"
    CODE_CURRENCY_CENT = "003"

    SINGLE_DIGITS = {i: f"{10+i:03d}" for i in range(10)}  # 0->010 ... 9->019
    TEENS = {10 + i: f"{20+i:03d}" for i in range(10)}      # 10->020 ... 19->029
    TENS = {
        20: "030", 30: "031", 40: "032", 50: "033",
        60: "034", 70: "035", 80: "036", 90: "037",
    }

    CODE_HUNDRED = "100"
    CODE_THOUSAND = "101"
    CODE_TEN_THOUSAND = "102"
    CODE_HUNDRED_THOUSAND = "103"
    CODE_MILLION = "104"

    def get_downlink_topic(self, device_sn: str) -> str:
        # Feishu SubTopic: {ClientID}/data
        return f"{device_sn}/data"

    def _parse_khmer_number(self, n: int) -> List[str]:
        """Converts an integer into Khmer linguistic 3-digit WAV slices."""
        if n == 0:
            return [self.SINGLE_DIGITS[0]]

        codes = []
        if n >= 1_000_000:
            codes.extend(self._parse_khmer_number(n // 1_000_000))
            codes.append(self.CODE_MILLION)
            n %= 1_000_000

        if n >= 100_000:
            codes.extend(self._parse_khmer_number(n // 100_000))
            codes.append(self.CODE_HUNDRED_THOUSAND)
            n %= 100_000

        if n >= 10_000:
            codes.extend(self._parse_khmer_number(n // 10_000))
            codes.append(self.CODE_TEN_THOUSAND)
            n %= 10_000

        if n >= 1_000:
            codes.extend(self._parse_khmer_number(n // 1_000))
            codes.append(self.CODE_THOUSAND)
            n %= 1_000

        if n >= 100:
            codes.append(self.SINGLE_DIGITS[n // 100])
            codes.append(self.CODE_HUNDRED)
            n %= 100

        if n >= 20:
            codes.append(self.TENS[(n // 10) * 10])
            rem = n % 10
            if rem > 0:
                codes.append(self.SINGLE_DIGITS[rem])
        elif n >= 10:
            codes.append(self.TEENS[n])
        elif n > 0:
            codes.append(self.SINGLE_DIGITS[n])

        return codes

    def build_payment_payload(
        self, device_sn: str, amount: float, currency: str, message_id: str
    ) -> Dict[str, Any]:
        codes = [self.CODE_PROMPT_RECEIVED]
        curr = currency.upper()

        if curr == "KHR":
            int_amt = int(round(amount))
            codes.extend(self._parse_khmer_number(int_amt))
            codes.append(self.CODE_CURRENCY_KHR)
            amount_str = str(int_amt)
        elif curr == "USD":
            dollars = int(amount)
            cents = int(round((amount - dollars) * 100))
            codes.extend(self._parse_khmer_number(dollars))
            codes.append(self.CODE_CURRENCY_USD)
            if cents > 0:
                codes.extend(self._parse_khmer_number(cents))
                codes.append(self.CODE_CURRENCY_CENT)
            amount_str = f"{amount:.2f}"
        else:
            int_amt = int(round(amount))
            codes.extend(self._parse_khmer_number(int_amt))
            amount_str = str(int_amt)

        return {
            "cmd": "voice",
            "amount": amount_str,
            "playAudibleMsg": "-".join(codes),
        }


class SupplierFactory:
    """Factory creating and resolving vendor strategy instances."""

    _instances: Dict[str, BaseSoundboxSupplier] = {
        "hemi": HemiSupplier(),
        "feishu": FeishuSupplier(),
    }

    @classmethod
    def get(cls, supplier_name: Optional[str]) -> BaseSoundboxSupplier:
        name = (supplier_name or "hemi").strip().lower()
        return cls._instances.get(name, cls._instances["hemi"])