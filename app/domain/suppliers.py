import logging
import re
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

logger = logging.getLogger("SoundboxSuppliers")


class BaseSoundboxSupplier(ABC):
    """Abstract Strategy interface for soundbox hardware vendors."""

    @abstractmethod
    def get_downlink_topic(self, device_sn: str) -> str:
        pass

    @abstractmethod
    def build_payment_payload(
        self, device_sn: str, amount: float, currency: str, message_id: str
    ) -> Dict[str, Any]:
        pass

    @abstractmethod
    def build_get_info_payload(self) -> Dict[str, Any]:
        pass


class HemiSupplier(BaseSoundboxSupplier):
    """Strategy implementation for HEMI Cloud Speakers."""

    def get_downlink_topic(self, device_sn: str) -> str:
        clean_sn = device_sn.strip()
        return f"/LLZN/{clean_sn}"

    def build_payment_payload(
        self, device_sn: str, amount: float, currency: str, message_id: str
    ) -> Dict[str, Any]:
        clean_sn = device_sn.strip()
        curr = "USD" if str(currency).upper() == "USD" else "KHR"
        return {
            "message_id": message_id,
            "time_stamp": str(int(time.time())),
            "device_sn": clean_sn,
            "packet_type": "payment",
            "content": {
                "play_payment_amount": float(amount),
                "currency_type": curr,
            },
        }

    def build_get_info_payload(self) -> Dict[str, Any]:
        return {"cmd": "getinfo"}


class FeishuSupplier(BaseSoundboxSupplier):
    """
    Advanced Khmer & USD Voice Strategy for Feishu 4G/WiFi Cloud Soundbox.
    Slicing dictionary mapping according to minifs voice packs.
    """

    PRODUCT_ID = "XHKX8L740B"

    # ក្រុមទី ១៖ សំឡេងប្រព័ន្ធ និង រូបិយប័ណ្ណ (000 - 009)
    CODE_PROMPT_RECEIVED = "000"  # ទទួលប្រាក់
    CODE_CURRENCY_USD    = "001"  # ដុល្លារ
    CODE_CURRENCY_KHR    = "002"  # រៀល
    CODE_CURRENCY_CENT   = "003"  # សេន
    CODE_DOT             = "004"  # ចុច

    # ក្រុមទី ២៖ លេខរាយ 0 ដល់ 9 (010 - 019)
    DIGITS_MAP = {
        0: "010", 1: "011", 2: "012", 3: "013", 4: "014",
        5: "015", 6: "016", 7: "017", 8: "018", 9: "019"
    }

    # ក្រុមទី ៣៖ លេខ 10 ដល់ 19 (020 - 029)
    TEENS_MAP = {
        10: "020", 11: "021", 12: "022", 13: "023", 14: "024",
        15: "025", 16: "026", 17: "027", 18: "028", 19: "029"
    }

    # ក្រុមទី ៤៖ ខ្ទង់ដប់ 20 ដល់ 90 (030 - 037)
    TENS_MAP = {
        20: "030", 30: "031", 40: "032", 50: "033",
        60: "034", 70: "035", 80: "036", 90: "037"
    }

    # ក្រុមទី ៥៖ ខ្ទង់រាប់ធំៗរបស់ខ្មែរ (100+)
    CODE_HUNDRED          = "100"  # រយ
    CODE_THOUSAND         = "101"  # ពាន់
    CODE_TEN_THOUSAND     = "102"  # ម៉ឺន
    CODE_HUNDRED_THOUSAND = "103"  # សែន
    CODE_MILLION          = "104"  # លាន

    def get_downlink_topic(self, device_sn: str) -> str:
        """Downlink topic ស្តង់ដារសម្រាប់ Feishu Firmware គឺ /down"""
        raw_sn = device_sn.strip()
        clean_sn = raw_sn.split("/")[-1].strip() if "/" in raw_sn else raw_sn
        clean_sn = re.sub(r"[^A-Za-z0-9]", "", clean_sn)
        short_sn = clean_sn[-7:] if len(clean_sn) >= 7 else clean_sn
        return f"{self.PRODUCT_ID}/{short_sn}/down"

    def _parse_khmer_integer(self, n: int) -> List[str]:
        """បំប្លែងចំនួនលេខទៅជាកូដសំឡេងតាមវេយ្យាករណ៍រាប់លេខខ្មែរ"""
        if n == 0:
            return [self.DIGITS_MAP[0]]

        codes: List[str] = []

        if n >= 1_000_000:
            codes.extend(self._parse_khmer_integer(n // 1_000_000))
            codes.append(self.CODE_MILLION)
            n %= 1_000_000

        if n >= 100_000:
            codes.extend(self._parse_khmer_integer(n // 100_000))
            codes.append(self.CODE_HUNDRED_THOUSAND)
            n %= 100_000

        if n >= 10_000:
            codes.extend(self._parse_khmer_integer(n // 10_000))
            codes.append(self.CODE_TEN_THOUSAND)
            n %= 10_000

        if n >= 1_000:
            codes.extend(self._parse_khmer_integer(n // 1_000))
            codes.append(self.CODE_THOUSAND)
            n %= 1_000

        if n >= 100:
            codes.append(self.DIGITS_MAP[n // 100])
            codes.append(self.CODE_HUNDRED)
            n %= 100

        if n >= 20:
            tens = (n // 10) * 10
            codes.append(self.TENS_MAP[tens])
            units = n % 10
            if units > 0:
                codes.append(self.DIGITS_MAP[units])
        elif n >= 10:
            codes.append(self.TEENS_MAP[n])
        elif n > 0:
            codes.append(self.DIGITS_MAP[n])

        return codes

    def build_payment_payload(
        self, device_sn: str, amount: float, currency: str, message_id: str
    ) -> Dict[str, Any]:
        codes: List[str] = [self.CODE_PROMPT_RECEIVED]
        curr = currency.strip().upper()
        clean_sn = re.sub(r"[^A-Za-z0-9]", "", str(device_sn).strip())
        short_sn = clean_sn[-7:] if len(clean_sn) >= 7 else clean_sn

        try:
            val = max(0.0, float(amount))
        except (ValueError, TypeError):
            val = 0.0

        if curr == "USD":
            dollars = int(val)
            cents = int(round((val - dollars) * 100))

            if dollars > 0 or (dollars == 0 and cents == 0):
                codes.extend(self._parse_khmer_integer(dollars))
                codes.append(self.CODE_CURRENCY_USD)

            if cents > 0:
                codes.extend(self._parse_khmer_integer(cents))
                codes.append(self.CODE_CURRENCY_CENT)

            amount_str = f"{val:.2f}" if cents > 0 else str(dollars)
        else:
            int_amt = int(round(val))
            codes.extend(self._parse_khmer_integer(int_amt))
            codes.append(self.CODE_CURRENCY_KHR)
            amount_str = str(int_amt)

        slice_voice_str = "-".join(codes)

        # គាំទ្រ format ទាំងពីរ (cmd: playAudibleMsg និង broadcast) ដើម្បីឱ្យ firmware ចាប់បានភ្លាម
        payload = {
            "cmd": "playAudibleMsg",
            "msgid": message_id,
            "message_id": message_id,
            "sn": short_sn,
            "amount": amount_str,
            "currency": curr,
            "playAudibleMsg": slice_voice_str,
            "data": slice_voice_str,
        }

        logger.info(
            "Payload created for %s | Topic: %s | Slices: %s",
            device_sn, self.get_downlink_topic(device_sn), slice_voice_str
        )
        return payload

    def build_get_info_payload(self) -> Dict[str, Any]:
        return {"cmd": "getinfo"}


class SupplierFactory:
    """Factory resolving vendor strategy instances."""

    _instances: Dict[str, BaseSoundboxSupplier] = {
        "hemi": HemiSupplier(),
        "feishu": FeishuSupplier(),
    }

    @classmethod
    def get(cls, supplier_name: Optional[str]) -> BaseSoundboxSupplier:
        name = (supplier_name or "feishu").strip().lower()
        return cls._instances.get(name, cls._instances["feishu"])