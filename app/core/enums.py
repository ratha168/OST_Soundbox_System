from enum import Enum

class AckStatus(str, Enum):
    PENDING = "PENDING"
    MQTT_DELIVERED = "MQTT_DELIVERED"
    SPEAKER_PLAYED = "SPEAKER_PLAYED"
    FAILED = "FAILED"
    TIMEOUT = "PLAY_TIMEOUT"

class Currency(str, Enum):
    KHR = "KHR"
    USD = "USD"