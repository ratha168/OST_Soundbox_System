import logging
import re
from typing import Optional
from app.domain.models import Currency, Transaction

logger = logging.getLogger("BankParser")


class BankNotificationParser:
    """Parses incoming bank transaction text (ABA, ACLEDA, Bakong, Wing, TrueMoney, etc.)."""

    # 1. សារខ្មែរទូទៅ (Bakong KHQR, Wing, TrueMoney, etc.)
    # ឧទាហរណ៍៖ "បានទទួល 20,000 រៀល ... លេខយោង 62330363203"
    # ឬ "ទទួលបាន 5.00 ដុល្លារ ... លេខប្រតិបត្តិការ: 123456"
    KHMER_STD_PATTERN = re.compile(
        r"(?:បានទទួល|ទទួលបានប្រាក់|ទទួលបាន|ចំនួនទឹកប្រាក់)[:\s]*([\d,]+(?:\.\d{1,2})?)\s*(រៀល|ដុល្លារ|KHR|USD|\$|៛).*?(?:លេខយោង|លេខប្រតិបត្តិការ|លេខកូដ|TxID|Ref|APV)[:\s]*([A-Za-z0-9]+)",
        re.IGNORECASE | re.DOTALL,
    )

    # 2. ABA Bank USD
    ABA_USD_PATTERN = re.compile(
        r"(?:received|\+|amount:?)\s*\$\s*([\d,]+(?:\.\d{1,2})?).*?(?:Tran(?:saction)?[- ]?ID|Ref(?:\.|erence)?|APV|Txn)[:\s]*([A-Za-z0-9]+)",
        re.IGNORECASE | re.DOTALL,
    )

    # 3. ABA Bank KHR
    ABA_KHR_PATTERN = re.compile(
        r"(?:received|\+|amount:?)\s*([\d,]+(?:\.\d{1,2})?)\s*(?:KHR|៛|Riel).*?(?:Tran(?:saction)?[- ]?ID|Ref(?:\.|erence)?|APV|Txn)[:\s]*([A-Za-z0-9]+)",
        re.IGNORECASE | re.DOTALL,
    )

    # 4. ACLEDA / English Format
    ACLEDA_PATTERN = re.compile(
        r"(?:received|recieved|cr:?|amount:?)\s*(USD|KHR|\$|៛)\s*([\d,]+(?:\.\d{1,2})?).*?(?:Txn|Trans|ID|Ref)[:\s]*([A-Za-z0-9]+)",
        re.IGNORECASE | re.DOTALL,
    )

    # 5. Generic Fallback
    GENERIC_PATTERN = re.compile(
        r"([\d,]+(?:\.\d{1,2})?)\s*(USD|KHR|\$|៛|រៀល|ដុល្លារ).*?(?:លេខយោង|ID|Ref|Txn)[:\s]*([A-Za-z0-9]+)",
        re.IGNORECASE | re.DOTALL,
    )

    @classmethod
    def parse(cls, text: str) -> Optional[Transaction]:
        if not text:
            return None

        clean_text = text.replace("\xa0", " ").strip()

        # 1. ពិនិត្យសារភាសាខ្មែរ (បានទទួល ... រៀល/ដុល្លារ ... លេខយោង ...)
        match = cls.KHMER_STD_PATTERN.search(clean_text)
        if match:
            raw_amt, curr_str, txid = match.groups()
            amount = float(raw_amt.replace(",", ""))
            is_usd = curr_str.strip() in ("ដុល្លារ", "USD", "$")
            currency = Currency.USD if is_usd else Currency.KHR
            logger.info("✅ [PARSED KHMER BANK] %s %s | TxID: %s", amount, currency.value, txid)
            return Transaction(txid=txid.strip(), amount=amount, currency=currency, raw_payload=clean_text)

        # 2. ABA USD
        match = cls.ABA_USD_PATTERN.search(clean_text)
        if match:
            raw_amt, txid = match.groups()
            amount = float(raw_amt.replace(",", ""))
            logger.info("✅ [PARSED ABA USD] %s USD | TxID: %s", amount, txid)
            return Transaction(txid=txid.strip(), amount=amount, currency=Currency.USD, raw_payload=clean_text)

        # 3. ABA KHR
        match = cls.ABA_KHR_PATTERN.search(clean_text)
        if match:
            raw_amt, txid = match.groups()
            amount = float(raw_amt.replace(",", ""))
            logger.info("✅ [PARSED ABA KHR] %s KHR | TxID: %s", amount, txid)
            return Transaction(txid=txid.strip(), amount=amount, currency=Currency.KHR, raw_payload=clean_text)

        # 4. ACLEDA / English
        match = cls.ACLEDA_PATTERN.search(clean_text)
        if match:
            curr_str, raw_amt, txid = match.groups()
            amount = float(raw_amt.replace(",", ""))
            currency = Currency.USD if curr_str.upper() in ("USD", "$") else Currency.KHR
            logger.info("✅ [PARSED ACLEDA] %s %s | TxID: %s", amount, currency.value, txid)
            return Transaction(txid=txid.strip(), amount=amount, currency=currency, raw_payload=clean_text)

        # 5. Generic Fallback
        match = cls.GENERIC_PATTERN.search(clean_text)
        if match:
            raw_amt, curr_str, txid = match.groups()
            amount = float(raw_amt.replace(",", ""))
            is_usd = curr_str.strip() in ("ដុល្លារ", "USD", "$")
            currency = Currency.USD if is_usd else Currency.KHR
            logger.info("✅ [PARSED GENERIC] %s %s | TxID: %s", amount, currency.value, txid)
            return Transaction(txid=txid.strip(), amount=amount, currency=currency, raw_payload=clean_text)

        logger.warning("⚠️ Text did not match any banking pattern: %s", clean_text[:80])
        return None