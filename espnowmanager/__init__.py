"""espnowmanager - ESP-NOW for MicroPython on ESP32, with topics like MQTT, on top of wifimanager.

    from espnowmanager import ESPNowManager
    en = ESPNowManager([("gateway", "24:0a:c4:12:ab:cd")], group="home")
    en.publish("sensors/temp", 21.5)
    en.flush()
"""
from .frame import topic_matches
from .manager import (ESPNowManager, __version__, BROADCAST,
                      IDLE, NO_WIFI, SCANNING, ONLINE, OFFLINE)

__all__ = ["ESPNowManager", "topic_matches", "BROADCAST", "IDLE", "NO_WIFI", "SCANNING", "ONLINE", "OFFLINE"]
