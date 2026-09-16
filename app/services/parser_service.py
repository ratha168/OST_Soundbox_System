import logging
import re
from typing import Optional
from app.domain.models import Currency, Transaction

logger = logging.getLogger("BankParser")

class BankNotificationParser:
    """Parses incoming bank transaction text (ABA, ACLEDA, Canadia, etc.)."""

    # សម្គាល់ ABA Bank: e.g. "You have received $ 10.50 from ... Tran-ID: 178951507229433"
    ABA_USD_PATTERN = re.compile(
        r"(?:received|\+)\s*\$\s*([\d,]+(?:\.\d{1,2})?).*?(?:Tran(?:saction)?[- ]?ID|Ref(?:\.|erence)?|APV)[:\s]*([A-Za-z0-9]+)",
        re.IGNORECASE | re.DOTALL,
    )
    ABA_KHR_PATTERN = re.compile(
        r"(?:received|\+)\s*([\d,]+)\s*(?:KHR|៛|Riel).*?(?:Tran(?:saction)?[- ]?ID|Ref(?:\.|erence)?|APV)[:\s]*([A-Za-z0-9]+)",
        re.IGNORECASE | re.DOTALL,
    )

    # សម្គាល់ ACLEDA Bank: e.g. "ACLEDA: You received USD 5.00 ... Txn ID: 987654321"
    ACLEDA_PATTERN = re.compile(
        r"(?:received|recieved|cr:)\s*(USD|KHR)\s*([\d,]+(?:\.\d{1,2})?).*?(?:Txn|Trans|ID)[:\s]*([A-Za-z0-9]+)",
        re.IGNORECASE | re.DOTALL,
    )

    @classmethod
    def parse(cls, text: str) -> Optional[Transaction]:
        if not text:
            return None

        clean_text = text.replace("\xa0", " ").strip()

        # 1. Check ABA USD
        match = cls.ABA_USD_PATTERN.search(clean_text)
        if match:
            raw_amt, txid = match.groups()
            amount = float(raw_amt.replace(",", ""))
            logger.info("Parsed ABA USD Transaction | Amount: %s | TxID: %s", amount, txid)
            return Transaction(txid=txid.strip(), amount=amount, currency=Currency.USD)

        # 2. Check ABA KHR
        match = cls.ABA_KHR_PATTERN.search(clean_text)
        if match:
            raw_amt, txid = match.groups()
            amount = float(raw_amt.replace(",", ""))
            logger.info("Parsed ABA KHR Transaction | Amount: %s | TxID: %s", amount, txid)
            return Transaction(txid=txid.strip(), amount=amount, currency=Currency.KHR)

        # 3. Check ACLEDA / General Format
        match = cls.ACLEDA_PATTERN.search(clean_text)
        if match:
            curr_str, raw_amt, txid = match.groups()
            amount = float(raw_amt.replace(",", ""))
            currency = Currency.USD if curr_str.upper() == "USD" else Currency.KHR
            logger.info("Parsed ACLEDA Transaction | Currency: %s | Amount: %s | TxID: %s", currency.value, amount, txid)
            return Transaction(txid=txid.strip(), amount=amount, currency=currency)

        logger.debug("Raw text does not match any known banking pattern: %s", clean_text[:60])
        return None