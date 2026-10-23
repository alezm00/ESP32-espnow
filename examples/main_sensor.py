"""Battery sensor: wake up, send one reading to the hub, go back to deep sleep.

First boot (or BOOT button held): pairing with the hub (see main_async.py), then the hub and
its channel are remembered in /espnow_peers.json, so the next wake-ups take a few ms of radio time.
"""
import machine
from machine import Pin

from espnowmanager import ESPNowManager

SLEEP_MS = 60000

en = ESPNowManager(name="garden", group="my-house")   # channel="auto": the saved one, else searched

if not en.peers() or Pin(0, Pin.IN, Pin.PULL_UP).value() == 0:
    en.pair(30)                                         # HELLOs on every channel until the hub answers
    en.connect(timeout=30)

reading = {"t": 21.5, "battery": 3.91}                  # read your sensor here
mid = en.publish("sensors/garden", reading)             # to every peer: here, the hub
if not en.flush(timeout=5):                             # the hub's ACK (5 s: time for a channel search too)
    print("hub not reachable, still queued:", en.pending())
machine.deepsleep(SLEEP_MS)                             # RAM (and the queue) are lost: next reading next time
