import re
from typing import Optional, Dict, Any

class BankNotificationParser:
    @staticmethod
    def parse_message(text: str) -> Optional[Dict[str, Any]]:
        # 1. CMC / Canadia KHQR Merchant Pattern (ឧទាហរណ៍: KHR 20,000.00 is paid by ... from YOEM CHANNA ...)
        cmc_pattern = (
            r"(?P<currency>KHR|USD)\s+(?P<amount>[\d,]+(?:\.\d{2})?)\s+"
            r"is\s+paid\s+by\s+.*?\s+"
            r"for\s+purchase\s+(?P<txid>[a-fA-F0-9]+),\s*"
            r"from\s+(?P<payer>.+?),\s*"
            r"at\s+"
        )
        cmc_match = re.search(cmc_pattern, text, re.IGNORECASE)
        if cmc_match:
            amount_str = cmc_match.group("amount").replace(",", "")
            return {
                "bank": "CMC_KHQR",
                "txid": cmc_match.group("txid"),
                "amount": float(amount_str),
                "currency": cmc_match.group("currency").upper(),
                "payer": cmc_match.group("payer").strip()
            }

        # 2. ACLEDA / Wing Khmer Pattern (ឧទាហរណ៍: បានទទួល 20,000 រៀល ពី ... លេខយោង 62210325382...)
        acleda_kh_pattern = (
            r"បានទទួល\s+(?P<amount>[\d,]+(?:\.\d{2})?)\s+(?P<currency>រៀល|ដុល្លារ|\$|USD|KHR)\s+"
            r"ពី\s+(?P<payer>.+?),\s*"
            r"ថ្ងៃទី.+?,\s*"
            r"លេខយោង\s+(?P<txid>\d+)"
        )
        acleda_kh_match = re.search(acleda_kh_pattern, text, re.IGNORECASE)
        if acleda_kh_match:
            raw_curr = acleda_kh_match.group("currency")
            currency = "KHR" if raw_curr == "រៀល" else "USD"
            amount_str = acleda_kh_match.group("amount").replace(",", "")
            
            return {
                "bank": "ACLEDA",
                "txid": acleda_kh_match.group("txid"),
                "amount": float(amount_str),
                "currency": currency,
                "payer": acleda_kh_match.group("payer").strip()
            }

        # 3. ABA KHQR Pattern (ឧទាហរណ៍: ៛40,000 paid by NORY VANAK (*428)... Trx. ID: 178640440969302)
        aba_khqr_pattern = (
            r"(?P<currency>៛|\$)?\s*(?P<amount>[\d,]+(?:\.\d{2})?)\s+"
            r"paid\s+by\s+(?P<payer>.+?)(?:\s*\(\*\d+\)\s*)?"
            r"on\s+.*?"
            r"Trx\.\s*ID:\s*(?P<txid>\d+)"
        )
        khqr_match = re.search(aba_khqr_pattern, text, re.IGNORECASE)
        if khqr_match:
            raw_currency = khqr_match.group("currency")
            currency = "KHR" if raw_currency == "៛" else "USD"
            amount_str = khqr_match.group("amount").replace(",", "")
            
            return {
                "bank": "ABA_KHQR",
                "txid": khqr_match.group("txid"),
                "amount": float(amount_str),
                "currency": currency,
                "payer": khqr_match.group("payer").strip()
            }

        # 4. ABA Standard Pattern (ឧទាហរណ៍: Received $2.50 from SOK CHAN (Trx ID: 987654321))
        aba_pattern = r"Received\s+\$(?P<amount>\d+(?:\.\d{2})?)\s+from\s+(?P<payer>.+?)\s*\(Trx ID:\s*(?P<txid>\d+)\)"
        aba_match = re.search(aba_pattern, text, re.IGNORECASE)
        if aba_match:
            return {
                "bank": "ABA",
                "txid": aba_match.group("txid"),
                "amount": float(aba_match.group("amount")),
                "currency": "USD",
                "payer": aba_match.group("payer").strip()
            }

        # 5. ACLEDA English Pattern (ឧទាហរណ៍: Received 10,000 KHR from CHAN THA (Ref: 123456))
        acleda_en_pattern = r"Received\s+(?P<amount>[\d,]+)\s+(?P<currency>KHR|USD)\s+from\s+(?P<payer>.+?)\s*\(Ref:\s*(?P<txid>\w+)\)"
        acleda_en_match = re.search(acleda_en_pattern, text, re.IGNORECASE)
        if acleda_en_match:
            amount_str = acleda_en_match.group("amount").replace(",", "")
            return {
                "bank": "ACLEDA",
                "txid": acleda_en_match.group("txid"),
                "amount": float(amount_str),
                "currency": acleda_en_match.group("currency"),
                "payer": acleda_en_match.group("payer").strip()
            }

        return None