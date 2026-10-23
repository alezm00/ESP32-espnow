"""Plain blocking script, no asyncio in your code.

connect() blocks until the peer answers; check() in the main loop reads incoming frames, pings,
notices an unreachable peer, searches the channel again and resends what is queued.
"""
import time

from espnowmanager import ESPNowManager

en = ESPNowManager([("gateway", "24:0a:c4:12:ab:cd")],
                   on_message=lambda topic, payload, peer: print(peer, topic, payload))

en.connect(timeout=30)                # or en.connect() to wait forever

last = time.ticks_ms()
while True:
    en.check()                        # cheap: does nothing unless something is due. Call it often.
    if time.ticks_diff(time.ticks_ms(), last) >= 10000:
        last = time.ticks_ms()
        en.publish("uptime", time.ticks_ms() // 1000, to="gateway")   # sent now, or queued while away
    time.sleep_ms(20)
