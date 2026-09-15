import json
import logging
from typing import Any, Dict
from pydantic import ValidationError

from app.domain.models import DeviceTelemetry
from app.repositories.device_repository import DeviceRepository
from app.repositories.transaction_repository import TransactionRepository

logger = logging.getLogger("TelemetryService")

class TelemetryService:
    """Processes boot packets, battery/signal telemetry, and hardware playback ACKs."""

    def __init__(
        self,
        device_repo: DeviceRepository,
        tx_repo: TransactionRepository,
        correlation_registry: Dict[str, Dict[str, Any]],
    ):
        self._device_repo = device_repo
        self._tx_repo = tx_repo
        self._correlation_registry = correlation_registry

    @staticmethod
    def _format_signal(signal_val: Any) -> str:
        try:
            val = int(signal_val)
            if val >= -65:
                return f"Excellent ({val} dBm)"
            elif val >= -75:
                return f"Good ({val} dBm)"
            elif val >= -85:
                return f"Fair ({val} dBm)"
            elif val < -85:
                return f"Poor ({val} dBm)"
        except (ValueError, TypeError):
            pass
        return f"{signal_val} dBm" if signal_val else ""

    async def handle_packet(self, topic: str, raw_payload: str) -> None:
        """Single entry point for all incoming MQTT messages."""
        try:
            data = json.loads(raw_payload)
        except json.JSONDecodeError:
            logger.error("Failed to decode JSON from topic %s", topic)
            return

        raw_sn = data.get("device_sn") or data.get("sn")
        if not raw_sn:
            topic_clean = topic.strip("/").split("/")[0]
            raw_sn = topic_clean if topic_clean not in ["pubmsg", "LLZN", "data", "up"] else None

        device_sn = str(raw_sn).strip() if raw_sn else "unknown"
        packet_type = str(data.get("packet_type", "")).lower()
        cmd = str(data.get("cmd", "")).lower()
        content = data.get("content", {})
        msg_id = str(data.get("message_id", "")).strip()

        # 1. Telemetry Updates (Pydantic 'getinfo' OR raw 'boot' packets)
        if cmd == "getinfo":
            try:
                telemetry = DeviceTelemetry(**data)
                fw_4g = telemetry.verno if telemetry.imei else ""
                fw_wifi = telemetry.verno if telemetry.mac else ""
                sig_str = str(telemetry.signal) if telemetry.signal is not None else ""
                
                await self._device_repo.upsert_telemetry(
                    device_sn=telemetry.sn,
                    battery=telemetry.battery_percentage,
                    signal=sig_str,
                    fw_4g=fw_4g,
                    fw_wifi=fw_wifi
                )
                logger.info("Updated telemetry via getinfo for SN %s", telemetry.sn)
            except ValidationError as e:
                logger.error("Telemetry validation failed for %s: %s", device_sn, e)

        elif "device_info" in packet_type or "boot" in packet_type or "battery_percent" in content:
            bat_pct = content.get("battery_percent")
            if bat_pct is None and "batt" in data:
                try:
                    mv = int(data["batt"])
                    bat_pct = min(100, max(0, int((mv - 3500) / 7)))
                except (ValueError, TypeError):
                    bat_pct = None

            battery_str = f"{bat_pct}%" if bat_pct is not None else ""
            sig_raw = content.get("signal_value") or content.get("wifi_signal") or data.get("signal")
            signal_str = self._format_signal(sig_raw)
            fw_4g = str(content.get("4g_fw_version") or data.get("verno") or "")
            fw_wifi = str(content.get("wifi_fw_version") or "")

            await self._device_repo.upsert_telemetry(device_sn, battery_str, signal_str, fw_4g, fw_wifi)
            logger.info("Telemetry updated for SN %s (Batt: %s, Sig: %s)", device_sn, battery_str, signal_str)

        # 2. Hardware Playback Acknowledgments
        else:
            resp_status = content.get("response_status") or content.get("play_status") or data.get("status") or "success"
            is_success = str(resp_status).lower() in ["success", "ok", "0", "true", "play_end", "finish"]
            status_text = "SPEAKER_PLAYED" if is_success else "FAILED"

            matched_tx = self._correlation_registry.pop(f"{device_sn}:{msg_id}", None)
            if matched_tx:
                await self._tx_repo.update_ack_by_txid(matched_tx["txid"], is_success, status_text)
                logger.info("Exact ACK match: TxID %s on SN %s", matched_tx['txid'], device_sn)
            else:
                await self._tx_repo.update_fallback_ack(device_sn, is_success, status_text)
                logger.info("Fallback ACK: SN %s", device_sn)