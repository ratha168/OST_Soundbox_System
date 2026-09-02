import logging
import time
from typing import Any, Dict
from app.infrastructure.mqtt_client import AsyncMqttPublisher
from app.repositories.device_repository import DeviceRepository

logger = logging.getLogger("KhqrService")


class KhqrService:
    """Manages Static KHQR sync directly to screen-enabled soundboxes."""

    def __init__(self, device_repo: DeviceRepository, mqtt_publisher: AsyncMqttPublisher):
        self._device_repo = device_repo
        self._mqtt_pub = mqtt_publisher

    async def sync_static_khqr(self, device_sn: str) -> Dict[str, Any]:
        device = await self._device_repo.get_device_by_sn(device_sn)
        if not device:
            return {"success": False, "error": f"Device SN {device_sn} not found or inactive"}

        if device["supplier"].lower() != "hemi":
            return {"success": False, "error": f"Device SN {device_sn} does not support screen display (supplier={device['supplier']})"}

        khqr_string = device["khqr_data"]
        if not khqr_string:
            return {"success": False, "error": f"No khqr_data found for device {device_sn}"}

        shop_name = device["shop_name"] or device["device_name"] or "Scan to Pay"
        merchant_display_id = f"ID: {device['merchant_id']}" if device["merchant_id"] else f"ID: {device_sn}"
        topic = f"/LLZN/{device_sn}"
        unique_msg_id = str(int(time.time() * 1000))[-10:]

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
            return {"success": True, "device_sn": device_sn, "shop_name": shop_name}
        return {"success": False, "error": res.get("error")}