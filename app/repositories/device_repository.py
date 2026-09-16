import logging
from typing import Any, Dict, List, Optional
import asyncpg

logger = logging.getLogger("DeviceRepository")


class DeviceRepository:
    """Manages soundbox registration, multi-field telemetry updates, and chat bindings."""

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def get_active_devices_by_chat_id(self, chat_id: str) -> List[Dict[str, Any]]:
        """
        ទាញយកឧបករណ៍ដែល active តាម Telegram Chat ID។
        ប្រើ supplier_id ជំនួសឱ្យ supplier ដើម្បីកុំឱ្យជួប UndefinedColumnError។
        """
        query = """
            SELECT 
                id, 
                device_id, 
                supplier_id, 
                is_active, 
                telegram_chat_id
            FROM devices
            WHERE telegram_chat_id = $1 AND is_active = TRUE;
        """
        clean_chat_id = str(chat_id).strip()
        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(query, clean_chat_id)
                logger.info("Found %s active device(s) for chat_id: %s", len(rows), clean_chat_id)
                return [dict(r) for r in rows]
        except Exception as e:
            logger.error("Failed to query devices by chat_id %s: %s", clean_chat_id, e)
            return []

    async def get_device_by_sn(self, device_sn: str) -> Optional[Dict[str, Any]]:
        """ទាញយកឧបករណ៍តាម Serial Number / Device ID (គាំទ្រទាំង match កន្ទុយ)"""
        clean_sn = str(device_sn).strip()
        query = """
            SELECT *
            FROM devices
            WHERE device_id = $1 OR device_id LIKE '%' || $1
            LIMIT 1;
        """
        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(query, clean_sn)
                return dict(row) if row else None
        except Exception as e:
            logger.error("Failed to fetch device by SN %s: %s", clean_sn, e)
            return None

    async def upsert_telemetry(
        self,
        device_sn: str,
        battery: Optional[str] = None,
        signal: Optional[str] = None,
        fw_4g: Optional[str] = None,
        fw_wifi: Optional[str] = None,
        imei: Optional[str] = None,
        imsi: Optional[str] = None,
        iccid: Optional[str] = None,
        volume: Optional[int] = None,
        vlver: Optional[str] = None,
        lang: Optional[int] = None,
        batt_mv: Optional[int] = None,
        adc: Optional[str] = None,
        ssid: Optional[str] = None,
        mac: Optional[str] = None,
    ) -> None:
        """Update ទិន្នន័យ Telemetry របស់ Device ចូល Database"""
        clean_sn = str(device_sn).strip()
        query = """
            UPDATE devices
            SET 
                battery = COALESCE(NULLIF($2, ''), battery),
                signal = COALESCE(NULLIF($3, ''), signal),
                firmware_version_4g = COALESCE(NULLIF($4, ''), firmware_version_4g),
                firmware_version_wifi = COALESCE(NULLIF($5, ''), firmware_version_wifi),
                imei = COALESCE(NULLIF($6, ''), imei),
                imsi = COALESCE(NULLIF($7, ''), imsi),
                iccid = COALESCE(NULLIF($8, ''), iccid),
                volume = COALESCE($9, volume),
                vlver = COALESCE(NULLIF($10, ''), vlver),
                lang = COALESCE($11, lang),
                batt_mv = COALESCE($12, batt_mv),
                adc = COALESCE(NULLIF($13, ''), adc),
                ssid = COALESCE(NULLIF($14, ''), ssid),
                mac = COALESCE(NULLIF($15, ''), mac),
                last_heartbeat = (NOW() AT TIME ZONE 'Asia/Phnom_Penh'),
                last_online = (NOW() AT TIME ZONE 'Asia/Phnom_Penh'),
                updated_at = (NOW() AT TIME ZONE 'Asia/Phnom_Penh')
            WHERE device_id = $1 OR device_id LIKE '%' || $1;
        """
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    query,
                    clean_sn, battery, signal, fw_4g, fw_wifi,
                    imei, imsi, iccid, volume, vlver, lang, batt_mv, adc, ssid, mac,
                )
        except Exception as e:
            logger.error("Failed to update telemetry for %s: %s", clean_sn, e)