"""asyncio hub: WiFi + ESP-NOW supervised in the background, nodes pair by themselves.

The hub is connected to your router, so its ESP-NOW channel is the router's: nodes using
channel="auto" find it by themselves (and again if the router changes channel).
Copy `wifimanager/` and `espnowmanager/` to /lib/ on the board, this file to /main.py.
"""
import asyncio

from wifimanager import WiFiManager
from espnowmanager import ESPNowManager

wm = WiFiManager(networks=[("MyHome", "my-home-password")])

en = ESPNowManager(
    wifi=wm,                      # the channel follows the WiFi; ESP-NOW waits while WiFi is down
    name="hub",
    group="my-house",             # only nodes of this group are talked to
    discovery=True,               # nodes that call en.pair() become peers (saved in /espnow_peers.json)
    on_peer=lambda peer, up: print("PEER", peer, "online" if up else "offline"),
)


@en.on("sensors/#")
def sensor(topic, payload, peer):
    print(peer, topic, payload.decode())


async def main():
    await en.start()              # starts the WiFiManager too, then everything runs in background
    while True:
        for p in en.peers():
            print("%-10s %s rssi=%s last seen %ss ago" % (p["name"], "up" if p["online"] else "down",
                                                         p["rssi"], p["last_seen"]))
        await asyncio.sleep(60)


asyncio.run(main())
