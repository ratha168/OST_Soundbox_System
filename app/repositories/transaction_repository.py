import logging
from typing import Any, Dict, List, Optional
import asyncpg

logger = logging.getLogger("TransactionRepository")


class TransactionRepository:
    """Handles persistence and ACK state transitions for soundbox transactions."""

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def get_tx_by_id(self, txid: str) -> Optional[Dict[str, Any]]:
        """ទាញយកព័ត៌មាន Transaction តាម ID ដើម្បីពិនិត្យស្ថានភាពចាក់សំឡេង។"""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, device_id, amount, currency, is_played, playback_status, created_at
                FROM transactions
                WHERE id = $1
                """,
                str(txid).strip(),
            )
            return dict(row) if row else None

    async def create_transaction(
        self,
        device_id: str,
        txid: str,
        amount: float,
        currency: str,
        chat_id: Optional[str] = None,
        raw_payload: Optional[str] = None,
        raw_text: Optional[str] = None,
        ack_status: str = "MQTT_DELIVERED",
        status: Optional[str] = None,
    ) -> bool:
        """
        កត់ត្រា Transaction ថ្មីចូល PostgreSQL។
        ត្រឡប់ True ប្រសិនបើជោគជ័យ ឬ False ប្រសិនបើមានបញ្ហា/duplicate។
        """
        final_raw = raw_payload if raw_payload is not None else (raw_text or "")
        final_status = status if status is not None else ack_status

        query = """
            INSERT INTO transactions (
                id, device_id, amount, currency, raw_text, 
                is_played, playback_status, created_at, updated_at
            )
            VALUES (
                $1, $2, $3, $4, $5, 
                FALSE, $6, 
                (NOW() AT TIME ZONE 'Asia/Phnom_Penh'), 
                (NOW() AT TIME ZONE 'Asia/Phnom_Penh')
            )
            ON CONFLICT (id) DO UPDATE
            SET updated_at = (NOW() AT TIME ZONE 'Asia/Phnom_Penh')
            RETURNING id;
        """
        try:
            async with self._pool.acquire() as conn:
                res = await conn.fetchval(
                    query,
                    str(txid).strip(),
                    str(device_id).strip(),
                    float(amount),
                    str(currency).strip(),
                    final_raw,
                    final_status,
                )
                return bool(res)
        except Exception as e:
            logger.error("Failed to insert transaction %s: %s", txid, e)
            return False

    async def update_ack_by_txid(self, txid: str, is_success: bool, status_text: str) -> None:
        """Update ស្ថានភាពពេលទទួលបាន exact ACK ត្រូវតាម msgid/correlation key។"""
        query = """
            UPDATE transactions
            SET is_played = $2,
                playback_status = $3,
                updated_at = (NOW() AT TIME ZONE 'Asia/Phnom_Penh')
            WHERE id = $1;
        """
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(query, str(txid).strip(), is_success, status_text)
        except Exception as e:
            logger.error("Failed to update exact ACK for TxID %s: %s", txid, e)

    async def update_fallback_ack(self, device_sn: str, is_success: bool, status_text: str) -> None:
        """
        Fallback ACK: ស្វែងរក Transaction ចុងក្រោយបំផុតរបស់ Device ដើម្បី Update។
        """
        clean_sn = str(device_sn).strip()
        query = """
            UPDATE transactions
            SET is_played = $2,
                playback_status = $3,
                updated_at = (NOW() AT TIME ZONE 'Asia/Phnom_Penh')
            WHERE id = (
                SELECT id FROM transactions
                WHERE (device_id = $1 OR device_id LIKE '%' || $1)
                  AND (is_played = FALSE OR playback_status = 'MQTT_DELIVERED')
                ORDER BY created_at DESC
                LIMIT 1
            );
        """
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(query, clean_sn, is_success, status_text)
        except Exception as e:
            logger.error("Failed to update fallback ACK for SN %s: %s", clean_sn, e)

    async def mark_play_timeout(self, txid: str, timeout_status: str = "PLAY_TIMEOUT") -> None:
        """កំណត់ Transaction ថាផុតកំណត់ ប្រសិនបើមិនទាន់បានលេង។"""
        query = """
            UPDATE transactions
            SET playback_status = $2,
                updated_at = (NOW() AT TIME ZONE 'Asia/Phnom_Penh')
            WHERE id = $1 AND is_played = FALSE;
        """
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(query, str(txid).strip(), timeout_status)
        except Exception as e:
            logger.error("Failed to mark timeout for TxID %s: %s", txid, e)