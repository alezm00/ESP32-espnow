"""The asyncio driver on a real event loop and the real clock (fake radio, fake air)."""
import _setup  # noqa: F401
import asyncio
import unittest

import espnow
import network
from _util import rmtree, tmpdir
from wifimanager import WiFiManager, compat
from espnowmanager import ESPNowManager

AIR = espnow.AIR


def mac(n):
    return bytes((0x24, 0x0A, 0xC4, 0, 0, n))


def addr(n):
    return "24:0a:c4:00:00:%02x" % n


class AsyncTests(unittest.TestCase):
    def setUp(self):
        self._wlan = network.WLAN
        network.WLAN = espnow.NodeWLAN
        network.WORLD.reset()
        network.WORLD.add_ap("Home", "homepass12", delay_ms=50, channel=6)
        AIR.reset(compat.ticks_ms, lambda ms: None)
        self.dir = tmpdir()

    def tearDown(self):
        network.WLAN = self._wlan
        AIR.reset(lambda: 0, lambda ms: None)
        rmtree(self.dir)

    async def until(self, cond, ms=3000):
        for _ in range(ms // 20):
            if cond():
                return True
            await asyncio.sleep(0.02)
        return cond()

    def make(self, n, peers, wifi=False, **kw):
        AIR.add_node(mac(n))
        wm = None
        if wifi:
            wm = WiFiManager(None, config_file="%s/wifi-%d.json" % (self.dir, n), config_mode="never")
            wm.log.console = False
            wm.add_network("Home", "homepass12")
        en = ESPNowManager(peers, wifi=wm, peers_file="%s/peers-%d.json" % (self.dir, n), poll_ms=20,
                           scan_dwell_ms=30, **kw)
        en.log.console = False
        return en

    def test_background_supervision(self):
        events = []

        async def echo(topic, payload, peer):
            await asyncio.sleep(0.01)
            return payload.decode().upper()

        async def main():
            gw = self.make(1, [("node", addr(2))], wifi=True, heartbeat=1, backoff_max=1,
                           on_peer=lambda peer, up: events.append((peer, up)))
            node = self.make(2, [("gw", addr(1))])  # channel "auto": finds the gateway on channel 6
            node.subscribe("cmd/echo", echo, reply=True)
            got = []
            gw.subscribe("cmd/+/reply", lambda t, p, peer: got.append((t, p, peer)))
            self.assertFalse(await gw.start())                # starts the WiFiManager too; node is not up yet
            self.assertTrue(gw.wifi.is_connected())
            await node.start(wait=False)
            self.assertTrue(await gw.wait_online(timeout=10))
            self.assertTrue(await node.wait_online(timeout=10))
            self.assertEqual(node.status()["channel"], 6)

            mid = gw.publish("cmd/echo", "hi", to="node")    # async handler, reply sent by a task
            self.assertTrue(await gw.wait_delivered(mid, timeout=2))
            self.assertTrue(await self.until(lambda: got))
            self.assertEqual(got, [("cmd/echo/reply", b"HI", "node")])

            AIR.nodes[mac(2)].awake = False                  # loss detected in the background
            self.assertTrue(await self.until(lambda: gw.state == "offline", 3000))
            mid = gw.publish("t/y", "2", to="node")
            self.assertFalse(await gw.wait_delivered(mid, timeout=0.3))
            AIR.nodes[mac(2)].awake = True
            self.assertTrue(await gw.wait_delivered(timeout=10))
            self.assertEqual(events[-2:], [("node", False), ("node", True)])

            node.stop()
            self.assertTrue(await self.until(lambda: gw.peer("node")["online"] is False))  # BYE
            gw.stop()
            gw.wifi.stop()
            await asyncio.sleep(0.1)

        asyncio.run(main())

    def test_start_waits_for_the_first_round(self):
        async def main():
            lone = self.make(3, [("ghost", addr(9))], channel=1, peer_retries=2)
            self.assertFalse(await lone.start())             # tried, nobody answered
            self.assertEqual(lone.state, "offline")
            self.assertEqual(lone.peer("ghost")["online"], False)
            lone.stop()
            await asyncio.sleep(0.05)

        asyncio.run(main())


if __name__ == "__main__":
    unittest.main()
