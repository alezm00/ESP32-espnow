"""Commands with replies: every handler's return value is sent back to the node that asked.

This node (the "lamp") controls a LED. Another node, e.g. a remote with a button, asks it:

    en = ESPNowManager([("lamp", "24:0a:c4:12:ab:cd")], channel=6)
    en.on("cmd/+/reply")(lambda topic, payload, peer: print(peer, "says", payload))
    en.publish("cmd/led", "toggle", to="lamp")
"""
import asyncio

from machine import Pin
from espnowmanager import ESPNowManager

led = Pin(2, Pin.OUT)

en = ESPNowManager([("remote", "24:0a:c4:12:ab:ce")], channel=6)   # fixed channel: no search needed


@en.on("cmd/led", reply=True)
def cmd_led(topic, payload, peer):
    cmd = payload.decode().strip().lower()
    if cmd == "toggle":
        led.value(not led.value())
    elif cmd in ("on", "off"):
        led.value(cmd == "on")
    else:
        raise ValueError("expected on, off or toggle")   # replied as {"error": "ValueError: ..."}
    return {"led": "on" if led.value() else "off"}          # sent to `peer` on cmd/led/reply


@en.on("cmd/info", reply=True)
def cmd_info(topic, payload, peer):
    return {"peers": [p["name"] or p["mac"] for p in en.peers()], "queued": en.pending()}


@en.on("cmd/slow", reply="events")     # async handlers work too; fixed reply topic
async def cmd_slow(topic, payload, peer):
    await asyncio.sleep(2)
    return "slow job done"


async def main():
    await en.start()
    while True:
        await asyncio.sleep(60)


asyncio.run(main())
