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
        ទាញយក SN ចេញពីទម្រង់ Topic ផ្សេងៗ៖
        - pubmsg/BOX4G20250806000002
        - XHKX8L740B/0000002/data
        - sub/MB3W2511017601YYY
        - /LLZN/0000002/up
        """
        parts = [p for p in topic.strip("/").split("/") if p]
        ignored = {"pubmsg", "llzn", "data", "up", "getinfo", "set", "cmd", "sub", "pub", "down"}

        for part in parts:
            clean = re.sub(r"[^A-Za-z0-9_-]", "", part)
            # បើជា Key Model (មានអក្សរ និងលេខលាយគ្នាវែង) ឬពាក្យបញ្ជា ត្រូវរំលង
            if clean.lower() not in ignored and not clean.upper().startswith("XHKX8L"):
                return clean
        return None

    @staticmethod
    def _calc_battery_percentage(raw_batt: Any) -> str:
        """
        បម្លែងតម្លៃថ្មពី Millivolt (mV) ទៅជាភាគរយ % (ផ្អែកលើថ្ម Li-ion 3.4V - 4.2V)
        """
        if raw_batt is None or raw_batt == "":
            return ""
        try:
            val = float(raw_batt)
            if val > 100:
                pct = max(0, min(100, int(round(((val - 3400) / 800) * 100))))
                return f"{pct}%"
            pct = max(0, min(100, int(round(val))))
            return f"{pct}%"
        except (ValueError, TypeError):
            return str(raw_batt)

    @staticmethod
    def _format_signal(signal_val: Any) -> str:
        """បំប្លែងតម្លៃ Signal (ទាំង CSQ 0-31 ឬ dBm អវិជ្ជមាន)"""
        if signal_val is None or signal_val == "":
            return ""
        try:
            val = int(signal_val)
            if 0 <= val <= 31:
                if val >= 20:
                    return f"Excellent (CSQ: {val})"
                elif val >= 15:
                    return f"Good (CSQ: {val})"
                elif val >= 10:
                    return f"Fair (CSQ: {val})"
                return f"Poor (CSQ: {val})"

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

        # ទាញយក SN ពី Payload ឬ Topic
        topic_sn = self._extract_sn_from_topic(topic)
        payload_sn = data.get("device_sn") or data.get("sn")
        device_sn = str(payload_sn or topic_sn or "unknown").strip()

        cmd = str(data.get("cmd", "")).strip().lower()
        packet_type = str(data.get("packet_type", "")).strip().lower()
        content = data.get("content", {})
        msg_id = str(data.get("message_id") or data.get("msgid") or "").strip()

        # ========================================================
        # 1. TELEMETRY: ឆ្លើយតបពី getinfo (Feishu 4G/WiFi)
        # ========================================================
        if cmd == "getinfo":
            if "batt" not in data and "volume" not in data and "verno" not in data:
                return

            try:
                imei = str(data.get("imei") or "").strip()
                imsi = str(data.get("imsi") or "").strip()
                iccid = str(data.get("iccid") or "").strip()
                vlver = str(data.get("vlver") or "").strip()
                adc = str(data.get("adc") or "").strip()
                ssid = str(data.get("ssid") or "").strip()
                mac = str(data.get("mac") or "").strip()
                verno = str(data.get("verno") or "").strip()
                netmodel = str(data.get("netmodel", "")).upper()

                volume = int(data["volume"]) if data.get("volume") is not None and str(data["volume"]).isdigit() else None
                lang = int(data["lang"]) if data.get("lang") is not None and str(data["lang"]).isdigit() else None
                batt_mv = int(data["batt"]) if data.get("batt") is not None and str(data["batt"]).isdigit() else None

                bat_val = self._calc_battery_percentage(batt_mv)
                sig_str = self._format_signal(data.get("signal"))

                fw_4g = verno if imei or netmodel != "WIFI" else ""
                fw_wifi = verno if mac or netmodel == "WIFI" else ""

                await self._device_repo.upsert_telemetry(
                    device_sn=device_sn,
                    battery=bat_val,
                    signal=sig_str,
                    fw_4g=fw_4g,
                    fw_wifi=fw_wifi,
                    imei=imei,
                    imsi=imsi,
                    iccid=iccid,
                    volume=volume,
                    vlver=vlver,
                    lang=lang,
                    batt_mv=batt_mv,
                    adc=adc,
                    ssid=ssid,
                    mac=mac,
                )
                logger.info(
                    "✅ [GETINFO SYNC] SN: %s | IMEI: %s | Batt: %s (%smV) | Vol: %s | AudioVer: %s | Sig: %s",
                    device_sn, imei, bat_val, batt_mv, volume, vlver, sig_str
                )
            except Exception as ex:
                logger.error("Failed to persist getinfo telemetry for %s: %s", device_sn, ex, exc_info=True)

        # ========================================================
        # 2. PLAYBACK ACKNOWLEDGMENTS: បញ្ជាក់ការបន្លឺសំឡេងរួចរាល់
        # (Feishu / Hemi គាំទ្រទាំង play_ack, broadcast_ack, voice_ack, result, status)
        # ========================================================
        elif (
            cmd in ("voice_ack", "ack", "payment_ack", "play_ack", "broadcast_ack")
            or "status" in data
            or "result" in data
            or "play_status" in data
            or "response_status" in content
        ):
            resp_status = (
                data.get("status")
                or data.get("result")
                or data.get("play_status")
                or content.get("response_status")
                or content.get("play_status")
                or "success"
            )
            is_success = str(resp_status).lower() in ["success", "ok", "0", "true", "play_end", "finish"]
            status_text = "SPEAKER_PLAYED" if is_success else "FAILED"

            matched_tx = None
            if msg_id:
                matched_tx = self._correlation_registry.pop(f"{device_sn}:{msg_id}", None)
                if not matched_tx:
                    # fallback រកតាម short SN ប្រសិនបើ device_sn ជា full SN
                    short_sn = device_sn[-7:] if len(device_sn) >= 7 else device_sn
                    matched_tx = self._correlation_registry.pop(f"{short_sn}:{msg_id}", None)

            if matched_tx:
                await self._tx_repo.update_ack_by_txid(matched_tx["txid"], is_success, status_text)
                logger.info("✅ [EXACT ACK] TxID: %s on SN: %s (Status: %s)", matched_tx["txid"], device_sn, status_text)
            else:
                await self._tx_repo.update_fallback_ack(device_sn, is_success, status_text)
                logger.info("ℹ️ [FALLBACK ACK] SN: %s (Status: %s)", device_sn, status_text)

        # ========================================================
        # 3. TELEMETRY: Boot Packets / Heartbeat (Hemi & Other Devices)
        # ========================================================
        elif "device_info" in packet_type or "boot" in packet_type or "battery_percent" in content:
            raw_bat = content.get("battery_percent") or data.get("batt")
            battery_str = self._calc_battery_percentage(raw_bat)

            sig_raw = content.get("signal_value") or content.get("wifi_signal") or data.get("signal")
            signal_str = self._format_signal(sig_raw)

            fw_4g = str(content.get("4g_fw_version") or data.get("verno") or "")
            fw_wifi = str(content.get("wifi_fw_version") or "")

            await self._device_repo.upsert_telemetry(
                device_sn=device_sn,
                battery=battery_str,
                signal=signal_str,
                fw_4g=fw_4g,
                fw_wifi=fw_wifi,
                imei=str(data.get("imei") or "").strip(),
            )
            logger.info("✅ [BOOT/TELEMETRY OK] SN: %s | Batt: %s | Sig: %s", device_sn, battery_str, signal_str)

        else:
            logger.debug("Unhandled MQTT packet on %s: %s", topic, data)