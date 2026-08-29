import re
import logging
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field

logger = logging.getLogger("BankNotificationParser")

class ParsedTransaction(BaseModel):
    bank: str = Field(..., description="Identified bank or payment platform name")
    txid: str = Field(..., description="Unique transaction ID or reference number")
    amount: float = Field(..., description="Normalized numerical transaction amount")
    currency: str = Field(..., description="Currency code (KHR or USD)")
    payer: str = Field(..., description="Name of the sender/payer")

class AdvancedBankNotificationParser:
    """
    Advanced production-ready parser for parsing Cambodian banking notification 
    messages (ABA, ACLEDA, Canadia, KHQR) with built-in data normalization and logging.
    """

    @classmethod
    def parse_message(cls, text: str) -> Optional[Dict[str, Any]]:
        if not text or not isinstance(text, str):
            logger.warning("Received empty or non-string text for parsing.")
            return None

        clean_text = text.strip()

        # 1. CMC / Canadia KHQR Merchant Pattern
        cmc_pattern = (
            r"(?P<currency>KHR|USD)\s+(?P<amount>[\d,]+(?:\.\d{2})?)\s+"
            r"is\s+paid\s+by\s+.*?\s+"
            r"for\s+purchase\s+(?P<txid>[a-fA-F0-9]+),\s*"
            r"from\s+(?P<payer>.+?),\s*"
            r"at\s+"
        )
        match = re.search(cmc_pattern, clean_text, re.IGNORECASE)
        if match:
            return cls._build_result("CMC_KHQR", match)

        # 2. ACLEDA / Wing Khmer Pattern
        acleda_kh_pattern = (
            r"បានទទួល\s+(?P<amount>[\d,]+(?:\.\d{2})?)\s+(?P<currency>រៀល|ដុល្លារ|\$|USD|KHR)\s+"
            r"ពី\s+(?P<payer>.+?),\s*"
            r"ថ្ងៃទី.+?,\s*"
            r"លេខយោង\s+(?P<txid>\d+)"
        )
        match = re.search(acleda_kh_pattern, clean_text, re.IGNORECASE)
        if match:
            raw_curr = match.group("currency")
            currency = "KHR" if raw_curr in ["រៀល", "KHR"] else "USD"
            return cls._build_result("ACLEDA", match, override_currency=currency)

        # 3. ABA KHQR Pattern
        aba_khqr_pattern = (
            r"(?P<currency>៛|\$)?\s*(?P<amount>[\d,]+(?:\.\d{2})?)\s+"
            r"paid\s+by\s+(?P<payer>.+?)(?:\s*\(\*\d+\)\s*)?"
            r"on\s+.*?"
            r"Trx\.\s*ID:\s*(?P<txid>\d+)"
        )
        match = re.search(aba_khqr_pattern, clean_text, re.IGNORECASE)
        if match:
            raw_currency = match.group("currency")
            currency = "KHR" if raw_currency == "៛" else "USD"
            return cls._build_result("ABA_KHQR", match, override_currency=currency)

        # 4. ABA Standard Pattern
        aba_pattern = r"Received\s+\$(?P<amount>[\d,]+(?:\.\d{2})?)\s+from\s+(?P<payer>.+?)\s*\(Trx ID:\s*(?P<txid>\d+)\)"
        match = re.search(aba_pattern, clean_text, re.IGNORECASE)
        if match:
            return cls._build_result("ABA", match, override_currency="USD")

        # 5. ACLEDA English Pattern
        acleda_en_pattern = r"Received\s+(?P<amount>[\d,]+(?:\.\d{2})?)\s+(?P<currency>KHR|USD)\s+from\s+(?P<payer>.+?)\s*\(Ref:\s*(?P<txid>\w+)\)"
        match = re.search(acleda_en_pattern, clean_text, re.IGNORECASE)
        if match:
            return cls._build_result("ACLEDA", match)

        logger.debug(f"No matching bank notification pattern found for text: {clean_text[:50]}...")
        return None

    @staticmethod
    def _build_result(bank_name: str, match_obj: re.Match, override_currency: Optional[str] = None) -> Optional[Dict[str, Any]]:
        try:
            groups = match_obj.groupdict()
            amount_str = groups["amount"].replace(",", "")
            amount = float(amount_str)
            
            currency = override_currency
            if not currency and "currency" in groups and groups["currency"]:
                raw_curr = groups["currency"].upper()
                currency = "KHR" if raw_curr in ["KHR", "៛"] else "USD"
            elif not currency:
                currency = "USD"

            payer = re.sub(r'\s*\(\*\d+\)\s*$', '', groups["payer"]).strip()

            data = {
                "bank": bank_name,
                "txid": groups["txid"].strip(),
                "amount": amount,
                "currency": currency,
                "payer": payer
            }

            # Validate via Pydantic schema model
            validated = ParsedTransaction(**data)
            return validated.dict()
        
        except (ValueError, TypeError, KeyError) as e:
            logger.error(f"Error parsing match groups for {bank_name}: {e}")
            return None