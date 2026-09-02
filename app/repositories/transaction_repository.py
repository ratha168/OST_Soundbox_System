import asyncpg


class TransactionRepository:
    """PostgreSQL interactions for transactions table ensuring accurate Phnom Penh timestamps."""

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def create_transaction(
        self,
        device_id: str,
        txid: str,
        chat_id: str,
        amount: float,
        currency: str,
        raw_payload: str,
        ack_status: str,
    ) -> bool:
        async with self._pool.acquire() as conn:
            try:
                await conn.execute(
                    """
                    INSERT INTO transactions (
                        device_id, txid, chat_id, amount, currency, raw_payload, 
                        device_ack, ack_status, ack_at, created_at
                    )
                    VALUES (
                        $1, $2, $3, $4, $5, $6, TRUE, $7, 
                        (NOW() AT TIME ZONE 'Asia/Phnom_Penh'), 
                        (NOW() AT TIME ZONE 'Asia/Phnom_Penh')
                    )
                    """,
                    device_id, txid, chat_id, amount, currency, raw_payload, ack_status,
                )
                return True
            except asyncpg.UniqueViolationError:
                return False

    async def update_ack_by_txid(self, txid: str, is_success: bool, status: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE transactions 
                SET device_ack = $1, 
                    ack_status = $2, 
                    ack_at = (NOW() AT TIME ZONE 'Asia/Phnom_Penh')
                WHERE txid = $3
                """,
                is_success, status, txid,
            )

    async def update_fallback_ack(self, device_sn: str, is_success: bool, status: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE transactions 
                SET device_ack = $1, 
                    ack_status = $2, 
                    ack_at = (NOW() AT TIME ZONE 'Asia/Phnom_Penh')
                WHERE ctid = (
                    SELECT ctid FROM transactions 
                    WHERE device_id = $3 
                    ORDER BY created_at DESC 
                    LIMIT 1
                )
                """,
                is_success, status, device_sn,
            )

    async def mark_play_timeout(self, txid: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE transactions 
                SET ack_status = CASE 
                        WHEN ack_status = 'MQTT_DELIVERED' THEN 'PLAY_TIMEOUT'
                        ELSE ack_status 
                    END
                WHERE txid = $1 AND device_ack = FALSE
                """,
                txid,
            )