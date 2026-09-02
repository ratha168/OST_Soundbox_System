from typing import Any, Dict, List, Optional
import asyncpg


class DeviceRepository:
    """PostgreSQL interactions for the devices table using Phnom Penh Timezone."""

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def get_active_devices_by_chat_id(self, chat_id: str) -> List[Dict[str, Any]]:
        clean_chat = str(chat_id).strip()
        alt_chat = clean_chat.replace("-100", "-") if clean_chat.startswith("-100") else clean_chat

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT device_id, device_name, COALESCE(supplier, 'hemi') AS supplier 
                FROM devices 
                WHERE (chat_id = $1 OR chat_id = $2) AND is_active = TRUE
                """,
                clean_chat, alt_chat,
            )
            return [dict(r) for r in rows]

    async def get_device_by_sn(self, device_sn: str) -> Optional[Dict[str, Any]]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT device_id, device_name, khqr_data, shop_name, merchant_id, COALESCE(supplier, 'hemi') as supplier
                FROM devices 
                WHERE device_id = $1 AND is_active = TRUE
                """,
                device_sn,
            )
            return dict(row) if row else None

    async def upsert_telemetry(
        self, device_sn: str, battery: str, signal: str, fw_4g: str, fw_wifi: str
    ) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO devices (
                    device_id, device_name, is_active, battery, signal, 
                    version_4g, version_wifi, last_online, created_at, updated_at
                )
                VALUES (
                    $1, $2, TRUE, $3, $4, $5, $6, 
                    (NOW() AT TIME ZONE 'Asia/Phnom_Penh'), 
                    (NOW() AT TIME ZONE 'Asia/Phnom_Penh'), 
                    (NOW() AT TIME ZONE 'Asia/Phnom_Penh')
                )
                ON CONFLICT (device_id) DO UPDATE
                SET battery = COALESCE(NULLIF(EXCLUDED.battery, ''), devices.battery),
                    signal = COALESCE(NULLIF(EXCLUDED.signal, ''), devices.signal),
                    version_4g = COALESCE(NULLIF(EXCLUDED.version_4g, ''), devices.version_4g),
                    version_wifi = COALESCE(NULLIF(EXCLUDED.version_wifi, ''), devices.version_wifi),
                    last_online = (NOW() AT TIME ZONE 'Asia/Phnom_Penh'),
                    updated_at = (NOW() AT TIME ZONE 'Asia/Phnom_Penh')
                """,
                device_sn, f"DEV_{device_sn}", battery, signal, fw_4g, fw_wifi,
            )