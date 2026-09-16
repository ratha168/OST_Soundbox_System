import logging
import time
import uuid
from typing import Any, Dict, Optional
from app.infrastructure.mqtt_client import AsyncMqttPublisher
from app.repositories.device_repository import DeviceRepository

logger = logging.getLogger("KhqrService")


class KhqrService:
    """Manages Static KHQR sync directly to screen-enabled soundboxes (Hemi)."""

    def __init__(self, device_repo: DeviceRepository, mqtt_publisher: AsyncMqttPublisher):
        self._device_repo = device_repo
        self._mqtt_pub = mqtt_publisher

    @staticmethod
    def _is_hemi_device(device: Dict[str, Any]) -> bool:
        """ផ្ទៀងផ្ទាត់ថាតើជា Hemi តាម supplier_id == 2 ឬ supplier text"""
        supp_id = device.get("supplier_id")
        if supp_id == 2:
            return True
        supp_name = str(device.get("supplier", "")).strip().lower()
        return supp_name == "hemi"

    async def sync_static_khqr(self, device_sn: str) -> Dict[str, Any]:
        device = await self._device_repo.get_device_by_sn(device_sn)
        if not device:
            logger.warning("Device SN %s not found in database", device_sn)
            return {"success": False, "error": f"Device SN {device_sn} not found or inactive"}

        # ផ្ទៀងផ្ទាត់ Supplier (Hemi ទើបគាំទ្រ Color LCD Screen)
        if not self._is_hemi_device(device):
            supp_info = device.get("supplier") or device.get("supplier_id")
            logger.warning("Device SN %s is not a screen-enabled model (supplier=%s)", device_sn, supp_info)
            return {
                "success": False,
                "error": f"Device SN {device_sn} does not support screen display (supplier={supp_info})",
            }

        # ទាញយក KHQR string ដោយប្រើ get() ដើម្បីកុំឱ្យបោះ KeyError
        khqr_string = device.get("khqr_data") or device.get("static_qr")
        if not khqr_string:
            return {"success": False, "error": f"No khqr_data found for device {device_sn}"}

        shop_name = device.get("shop_name") or device.get("device_name") or "Scan to Pay"
        merchant_id = device.get("merchant_id")
        merchant_display_id = f"ID: {merchant_id}" if merchant_id else f"ID: {device_sn}"

        topic = f"/LLZN/{device_sn}"
        unique_msg_id = uuid.uuid4().hex[:10]

        payload = {
            "message_id": unique_msg_id,
            "time_stamp": str(int(time.time())),
            "device_sn": str(device_sn),
            "packet_type": "set_device_info",
            "content": {
                "screen_content_config": {
                    "main_screen_label_1_config": {"txt": "Scan to Pay", "hei": 24, "col": "000000"},
                    "main_screen_qrcode_1_config": {"txt": str(khqr_string).strip(), "hei": 210, "col": "000000"},
                    "main_screen_label_3_config": {"txt": str(shop_name), "hei": 24, "col": "000000"},
                    "main_screen_label_4_config": {"txt": str(merchant_display_id), "hei": 16, "col": "0000FF"},
                }
            },
        }

        res = await self._mqtt_pub.publish(topic=topic, payload=payload, qos=1)
        if res.get("success"):
            logger.info("✅ KHQR pushed to Screen Device %s on topic %s", device_sn, topic)
            return {"success": True, "device_sn": device_sn, "shop_name": shop_name}

        logger.error("Failed to push KHQR to device %s: %s", device_sn, res.get("error"))
        return {"success": False, "error": res.get("error")}