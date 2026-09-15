import json
import logging
import re
from typing import Any, Dict, Optional, Union
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
    def _extract_sn_from_topic(topic: str) -> Optional[str]:
        """
        ទាញយក SN ពី Topic ទម្រង់ផ្សេងៗដូចជា:
        - pubmsg/BOX4G20250806000002
        - XHKX8L74OB/BOX4G20250806000002/data
        - /LLZN/SN123456
        """
        parts = [p for p in topic.strip("/").split("/") if p]
        ignored = {"pubmsg", "llzn", "data", "up", "getinfo", "xhkx8l740b"}

        for part in parts:
            clean = re.sub(r"[^A-Za-z0-9_-]", "", part)
            # មិនយក Product ID (XHKX8L740B) ឬពាក្យក្នុង ignored ធ្វើជា SN ឡើយ
            if clean.lower() not in ignored and not clean.upper().startswith("XHKX"):
                return clean
        return None

    @staticmethod
    def _format_signal(signal_val: Any) -> str:
        """បំប្លែងតម្លៃ Signal (ទាំង CSQ 0-31 ឬ dBm អវិជ្ជមាន)"""
        if signal_val is None or signal_val == "":
            return ""
        try:
            val = int(signal_val)
            # បើសិនជាតម្លៃ CSQ 4G (0 ដល់ 31)
            if 0 <= val <= 31:
                if val >= 20:
                    return f"Excellent (CSQ: {val})"
                elif val >= 15:
                    return f"Good (CSQ: {val})"
                elif val >= 10:
                    return f"Fair (CSQ: {val})"
                return f"Poor (CSQ: {val})"
            
            # បើសិនជាតម្លៃ WiFi RSSI (dBm អវិជ្ជមាន)
            if val >= -65:
                return f"Excellent ({val} dBm)"
            elif val >= -75:
                return f"Good ({val} dBm)"
            elif val >= -85:
                return f"Fair ({val} dBm)"
            return f"Poor ({val} dBm)"
        except (ValueError, TypeError):
            return str(signal_val)

    async def handle_packet(self, topic: str, raw_payload: Union[str, bytes, Dict[str, Any]]) -> None:
        """Single entry point for all incoming MQTT messages."""
        data: Dict[str, Any] = {}
        if isinstance(raw_payload, dict):
            data = raw_payload
        elif isinstance(raw_payload, (str, bytes)):
            try:
                data = json.loads(raw_payload)
            except json.JSONDecodeError:
                logger.error("Failed to decode JSON from topic %s | Data: %s", topic, raw_payload)
                return
        else:
            logger.error("Unsupported payload type on topic %s: %s", topic, type(raw_payload))
            return

        # ទាញយក Device SN ពី Payload ឬទាញចេញពី Topic
        raw_sn = data.get("device_sn") or data.get("sn")
        if not raw_sn:
            raw_sn = self._extract_sn_from_topic(topic)

        device_sn = str(raw_sn).strip() if raw_sn else "unknown"
        cmd = str(data.get("cmd", "")).strip().lower()
        packet_type = str(data.get("packet_type", "")).strip().lower()
        content = data.get("content", {})
        msg_id = str(data.get("message_id") or data.get("msgid") or "").strip()

        # ========================================================
        # 1. TELEMETRY: ឆ្លើយតបពី getinfo (Feishu 4G/WiFi)
        # ========================================================
        # 1. Telemetry Updates (ឆ្លើយតបពី getinfo របស់ Feishu)
        if cmd == "getinfo":
            # បើគ្មាន batt, volume, verno ទេ មានន័យថាជា Command ដែល Server បាញ់ចេញទៅ
            # ត្រូវ return ចោលភ្លាម កុំឱ្យកត់ត្រាចូល DB ដោយទទេស្អាត
            if "batt" not in data and "volume" not in data and "verno" not in data:
                return

            try:
                if "sn" not in data and device_sn != "unknown":
                    data["sn"] = device_sn

                telemetry = DeviceTelemetry(**data)
                fw_4g = telemetry.verno if telemetry.imei else ""
                fw_wifi = telemetry.verno if telemetry.mac else ""
                sig_str = self._format_signal(telemetry.signal)
                bat_val = f"{telemetry.battery_percentage}%" if telemetry.battery_percentage is not None else ""

                await self._device_repo.upsert_telemetry(
                    device_sn=telemetry.sn,
                    battery=bat_val,
                    signal=sig_str,
                    fw_4g=fw_4g,
                    fw_wifi=fw_wifi,
                )
                logger.info(
                    "✅ [GETINFO OK] SN: %s | Batt: %s (%smV) | Sig: %s | FW: %s",
                    telemetry.sn, bat_val, telemetry.batt, sig_str, telemetry.verno
                )
            except ValidationError as e:
                logger.error("Telemetry validation failed for %s: %s | Data: %s", device_sn, e, data)
            except Exception as ex:
                logger.error("Failed to persist getinfo telemetry for %s: %s", device_sn, ex)

        # ========================================================
        # 2. TELEMETRY: Boot Packets / Heartbeat (Hemi & Other devices)
        # ========================================================
        elif "device_info" in packet_type or "boot" in packet_type or "battery_percent" in content:
            bat_pct = content.get("battery_percent")
            if bat_pct is None and "batt" in data:
                try:
                    mv = float(data["batt"])
                    bat_pct = max(0, min(100, int(round(((mv - 3400) / 700) * 100))))
                except (ValueError, TypeError):
                    bat_pct = None

            battery_str = f"{bat_pct}%" if bat_pct is not None else ""
            sig_raw = content.get("signal_value") or content.get("wifi_signal") or data.get("signal")
            signal_str = self._format_signal(sig_raw)
            fw_4g = str(content.get("4g_fw_version") or data.get("verno") or "")
            fw_wifi = str(content.get("wifi_fw_version") or "")

            await self._device_repo.upsert_telemetry(device_sn, battery_str, signal_str, fw_4g, fw_wifi)
            logger.info("✅ [BOOT/TELEMETRY OK] SN: %s | Batt: %s | Sig: %s", device_sn, battery_str, signal_str)

        # ========================================================
        # 3. PLAYBACK ACKNOWLEDGMENTS: បញ្ជាក់ការបន្លឺសំឡេងរួចរាល់
        # ========================================================
        elif cmd in ("voice_ack", "ack", "payment_ack") or "status" in data or "response_status" in content:
            resp_status = (
                content.get("response_status")
                or content.get("play_status")
                or data.get("status")
                or "success"
            )
            is_success = str(resp_status).lower() in ["success", "ok", "0", "true", "play_end", "finish"]
            status_text = "SPEAKER_PLAYED" if is_success else "FAILED"

            matched_tx = self._correlation_registry.pop(f"{device_sn}:{msg_id}", None) if msg_id else None
            if matched_tx:
                await self._tx_repo.update_ack_by_txid(matched_tx["txid"], is_success, status_text)
                logger.info("✅ [EXACT ACK] TxID: %s on SN: %s (Status: %s)", matched_tx["txid"], device_sn, status_text)
            else:
                await self._tx_repo.update_fallback_ack(device_sn, is_success, status_text)
                logger.info("ℹ️ [FALLBACK ACK] SN: %s (Status: %s)", device_sn, status_text)

        else:
            logger.debug("Unhandled MQTT packet on %s: %s", topic, data)