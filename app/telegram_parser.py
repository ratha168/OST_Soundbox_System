import re
from typing import Any, Dict, Optional

def clean_amount(val_str: str) -> float:
    cleaned = re.sub(r'[^\d.]', '', val_str)
    return float(cleaned) if cleaned else 0.0

def parse_bank_message(text: str) -> Optional[Dict[str, Any]]:
    if not text or not isinstance(text, str):
        return None
    
    clean_text = text.strip()[:1000]

    # Pattern 1: $200,000 paid by SOK SOPHEAP (*936) on Aug 29, 08:32 PM via ABA PAY at SOKUNTHEA by S.OENG. Trx. ID: 178801033064695, APV: 373304.
    p1 = re.search(
        r'([\$៛]?)\s*([\d,]+(?:\.\d{1,2})?)\s*(USD|KHR|ដុល្លារ|រៀល)?\s+paid\s+by\s+(.*?)\s+on\s+(.*?)\s+via\s+(.*?)(?:\s+at\s+.*?)?(?:\.\s*Trx\.\s*ID:\s*([A-Za-z0-9]+))?',
        clean_text,
        re.IGNORECASE
    )
    if p1:
        cur_sym, amount_str, cur_text, payer, _, via_bank, trx_id = p1.groups()
        amount = clean_amount(amount_str)
        if amount <= 0:
            return None

        currency = "USD"
        if cur_sym == "៛" or (cur_text and cur_text.upper() in ["KHR", "រៀល"]):
            currency = "KHR"
        elif cur_sym == "$" or (cur_text and cur_text.upper() in ["USD", "ដុល្លារ"]):
            currency = "USD"
        elif amount > 500:
            currency = "KHR"

        bank = "ABA" if "ABA" in (via_bank or clean_text).upper() else "Bakong"
        
        # Backup trx_id check if regex group didn't catch it
        if not trx_id:
            trx_m = re.search(r'Trx\.\s*ID:\s*([A-Za-z0-9]+)', clean_text, re.IGNORECASE)
            trx_id = trx_m.group(1) if trx_m else None

        return {
            "amount": amount,
            "currency": currency,
            "bank": bank,
            "payer": (payer or "Customer").strip()[:100],
            "transaction_id": trx_id,
            "raw_text": clean_text
        }

    # Pattern 2: Fallback Pattern
    p2 = re.search(r'([\d,]+(?:\.\d{1,2})?)\s*(USD|KHR|\$|៛)', clean_text, re.IGNORECASE)
    if p2:
        amount = clean_amount(p2.group(1))
        cur_text = p2.group(2).upper()
        currency = "KHR" if cur_text in ["KHR", "៛"] or amount > 500 else "USD"
        return {
            "amount": amount,
            "currency": currency,
            "bank": "ABA" if "ABA" in clean_text.upper() else "Bakong",
            "payer": "Customer",
            "transaction_id": None,
            "raw_text": clean_text
        }

    return None