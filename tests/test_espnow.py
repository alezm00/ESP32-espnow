import harness  # noqa: F401  (sets up sys.path, fakes, virtual clock)
import json
import unittest

from harness import AIR, W, Sim, addr, mac

import espnow
from espnowmanager import BROADCAST, ESPNowManager
from espnowmanager import frame as F

KEY = "0123456789abcdef"


class Base(unittest.TestCase):
    def setUp(self):
        self.sim = Sim()

    def tearDown(self):
        self.sim.close()

    def two(self, **kw):
        """Nodes 1 ("a") and 2 ("b"), peers of each other, on a fixed channel; both online."""
        kw.setdefault("channel", 1)
        a = self.sim.make(1, [("b", addr(2))], **kw)
        b = self.sim.make(2, [("a", addr(1))], **kw)
        self.assertTrue(self.sim.run([a, b], self.sim.online(a, b), 5000))
        return a, b

    def data(self, src, dst):
        """DATA frames sent from node `src` to node `dst` (or broadcast), decoded: [(topic, payload)]."""
        out = []
        for e in AIR.frames(mac(src), mac(dst) if isinstance(dst, int) else dst):
            f = F.decode(e[3])
            if f and f[0] == F.DATA:
                out.append(F.split_data(f[4]))
        return out


class SendTests(Base):
    def test_unicast_is_acknowledged_and_received(self):
        delivered = []
        a, b = self.two(on_delivered=delivered.append)
        mids = [a.publish("t/x", {"v": 1}, to="b"), a.publish("t/y", 21.5, to=addr(2)),
                a.publish("t/z", None, to=mac(2)), a.publish("t/w", b"\x00\xff", to="b")]
        self.assertEqual(delivered, mids)  # sent at once: the radio was idle
        self.assertEqual(a.pending(), 0)
        self.sim.run([a, b], ms=500)
        self.assertEqual(self.sim.mine(2), [("t/x", b'{"v": 1}', "a"), ("t/y", b"21.5", "a"), ("t/z", b"", "a"),
                                            ("t/w", b"\x00\xff", "a")])

    def test_publish_to_every_peer_and_broadcast(self):
        delivered = []
        a = self.sim.make(1, [("b", addr(2)), ("c", addr(3))], channel=1, on_delivered=delivered.append)
        b = self.sim.make(2, [("a", addr(1))], channel=1)
        c = self.sim.make(3, [("a", addr(1))], channel=1)
        d = self.sim.make(4, channel=1)   # does not know node 1
        mgrs = [a, b, c, d]
        self.assertTrue(self.sim.run(mgrs, self.sim.online(a, b, c), 5000))
        mid = a.publish("all", "x")
        self.assertEqual(delivered, [mid])  # once, after both copies were acknowledged
        a.publish("shout", "y", to=BROADCAST)
        self.sim.run(mgrs, ms=500)
        self.assertEqual(self.data(1, 2), [("all", b"x")])
        self.assertEqual(self.data(1, 3), [("all", b"x")])
        self.assertEqual(self.data(1, F.BROADCAST_MAC), [("shout", b"y")])
        self.assertEqual(sorted(g[0] for g in self.sim.got if g[1] == "shout"), [2, 3])  # not node 4
        self.assertIn("dropped a frame from unknown 24:0a:c4:00:00:01", self.sim.log(d))

    def test_invalid_publish_and_no_peer(self):
        a = self.sim.make(1, channel=1, max_len=40)
        for topic in ("a/+", "a/#", "", None):
            self.assertRaises(ValueError, a.publish, topic, "x")
        self.assertRaises(ValueError, a.publish, "t", "x", to="nobody")
        self.assertRaises(ValueError, a.publish, "t", "x" * 40, to=BROADCAST)
        a.publish("t", "x" * 26, to=BROADCAST)  # 6 + 1 + 1 + 26 = 34 bytes: fits
        mid = a.publish("t", "x")               # every peer, and there is none
        self.assertEqual(a.status()["dropped"], 1)
        self.assertFalse(a.is_pending(mid))
        self.assertIn("no peer to send message", self.sim.log(a))
        self.assertRaises(ValueError, ESPNowManager, channel=15)
        self.assertRaises(ValueError, ESPNowManager, max_len=espnow.MAX_DATA_LEN + 1)

    def test_unreachable_peer_keeps_its_messages_in_order_and_does_not_block_others(self):
        a = self.sim.make(1, [("b", addr(2)), ("c", addr(3))], channel=1, heartbeat=0)
        b = self.sim.make(2, [("a", addr(1))], channel=1)
        c = self.sim.make(3, [("a", addr(1))], channel=1)
        self.assertTrue(self.sim.run([a, b, c], self.sim.online(a, b, c), 5000))
        AIR.range(mac(1), mac(2), False)
        mids = [a.publish("q/%d" % i, i, to="b") for i in range(3)]
        mc = a.publish("q/c", "now", to="c")
        self.assertFalse(a.is_pending(mc))
        self.sim.run([a, b, c], ms=3000)
        self.assertEqual(a.peer("b")["online"], False)
        self.assertEqual(a.peer("b")["queued"], 3)
        self.assertTrue(all(a.is_pending(m) for m in mids))
        self.assertIn((1, "b", False), self.sim.peer_events)
        self.assertEqual(a.state, "online")  # c is still reachable
        AIR.range(mac(1), mac(2), True)
        self.assertTrue(self.sim.run([a, b, c], lambda: not a.pending(), 120000))
        self.assertEqual([t for t, p, peer in self.sim.mine(2)], ["q/0", "q/1", "q/2"])
        self.assertIn((1, "b", True), self.sim.peer_events[-3:])

    def test_full_queue_drops_the_oldest(self):
        a, b = self.two(queue_size=3)
        AIR.range(mac(1), mac(2), False)
        mids = [a.publish("t/%d" % i, i, to="b") for i in range(5)]
        self.assertEqual(a.pending(), 3)
        self.assertEqual(a.status()["dropped"], 2)
        self.assertFalse(a.is_pending(mids[0]))
        self.assertIn("queue full", self.sim.log(a))
        AIR.range(mac(1), mac(2), True)
        self.assertTrue(self.sim.run([a, b], lambda: not a.pending(), 120000))
        self.sim.run([a, b], ms=500)
        self.assertEqual([t for t, p, peer in self.sim.mine(2)], ["t/2", "t/3", "t/4"])

    def test_lost_ack_means_a_resend_but_one_delivery(self):
        a, b = self.two()
        AIR.lose_acks[mac(2)] = 1
        mid = a.publish("once", "x", to="b")
        self.assertTrue(a.is_pending(mid))
        self.assertTrue(self.sim.run([a, b], lambda: not a.pending(), 5000))
        self.sim.run([a, b], ms=500)
        self.assertEqual(self.data(1, 2), [("once", b"x"), ("once", b"x")])
        self.assertEqual(self.sim.mine(2), [("once", b"x", "a")])
        self.assertIn("duplicate message from a dropped", self.sim.log(b))

    def test_radio_error_is_transient(self):
        a, b = self.two()
        AIR.no_mem = 1
        mid = a.publish("t", "x", to="b")
        self.assertTrue(a.is_pending(mid))
        self.assertIn("ESP_ERR_ESPNOW_NO_MEM", a.status()["last_error"])
        self.assertTrue(self.sim.run([a, b], lambda: not a.pending(), 5000))
        self.assertEqual(a.peer("b")["online"], True)


class ReceiveTests(Base):
    def test_handlers_wildcards_and_on_message(self):
        a, b = self.two()
        mine = []
        b.subscribe("sensors/+/temp", lambda t, p, peer: mine.append((t, p, peer)))
        b.subscribe("alerts/#")  # no handler: on_message
        a.publish("sensors/kitchen/temp", "21", to="b")
        a.publish("alerts/fire", "!", to="b")
        a.publish("sensors/kitchen/hum", "40", to="b")
        self.sim.run([a, b], ms=500)
        self.assertEqual(mine, [("sensors/kitchen/temp", b"21", "a")])
        self.assertEqual(self.sim.mine(2), [("alerts/fire", b"!", "a"), ("sensors/kitchen/hum", b"40", "a")])
        self.assertTrue(b.unsubscribe("sensors/+/temp"))
        self.assertEqual(b.subscriptions(), ["alerts/#"])

    def test_failing_handler_does_not_stop_the_loop(self):
        a, b = self.two()
        got = []

        def handler(topic, payload, peer):
            if payload == b"bad":
                raise ValueError("bad value")
            got.append(payload)
        b.subscribe("x", handler)
        a.publish("x", "bad", to="b")
        a.publish("x", "good", to="b")
        self.sim.run([a, b], ms=500)
        self.assertEqual(got, [b"good"])
        self.assertIn("handler for 'x' failed: ValueError: bad value", self.sim.log(b))
        self.assertEqual(b.state, "online")

    def test_async_handler_in_blocking_mode(self):
        a, b = self.two()

        async def handler(topic, payload, peer):
            return payload.decode().upper()
        b.subscribe("echo", handler, reply=True)
        a.publish("echo", "hi", to="b")
        self.sim.run([a, b], ms=500)
        self.assertEqual(self.sim.mine(1), [("echo/reply", b"HI", "b")])

    def test_unknown_senders_are_dropped_unless_accepted(self):
        b = self.sim.make(2, channel=1)
        b.check()
        x = AIR.bare(mac(9))
        x.esp.add_peer(mac(2))
        x.esp.send(mac(2), F.encode(F.DATA, 0, 0, 1, F.data_body("t", b"1")))
        self.sim.run(b, ms=300)
        self.assertEqual(self.sim.mine(2), [])
        self.assertIn("dropped a frame from unknown " + addr(9), self.sim.log(b))
        b2 = self.sim.make(3, channel=1, accept_unknown=True)
        b2.check()
        x.esp.add_peer(mac(3))
        x.esp.send(mac(3), F.encode(F.DATA, 0, 0, 2, F.data_body("t", b"2")))
        self.sim.run(b2, ms=300)
        self.assertEqual(self.sim.mine(3), [("t", b"2", addr(9))])
        self.assertEqual([p["mac"] for p in b2.peers()], [addr(9)])
        self.assertFalse(b2.peers()[0]["saved"])

    def test_foreign_frames_go_to_on_raw_and_other_groups_are_ignored(self):
        raw = []
        b = self.sim.make(2, [addr(9)], channel=1, group="home", on_raw=lambda m, msg: raw.append((m, msg)))
        x = AIR.bare(mac(9))
        x.esp.add_peer(mac(2))
        self.sim.run(b, ms=200)
        x.esp.send(mac(2), b"plain espnow")
        x.esp.send(mac(2), F.encode(F.DATA, 0, F.group_id("office"), 1, F.data_body("t", b"1")))
        x.esp.send(mac(2), F.encode(F.DATA, 0, F.group_id("home"), 2, F.data_body("t", b"2")))
        self.sim.run(b, ms=300)
        self.assertEqual(raw, [(mac(9), b"plain espnow")])
        self.assertEqual(self.sim.mine(2), [("t", b"2", addr(9))])
        self.assertIn("of another group", self.sim.log(b))


class ReplyTests(Base):
    def test_reply_goes_back_to_the_sender_and_is_never_answered(self):
        a, b = self.two()
        calls = []

        def cmd(topic, payload, peer):
            calls.append((topic, peer))
            return "ok:" + payload.decode()
        a.subscribe("cmd/#", cmd, reply=True)  # a would answer the reply too, if it were not marked
        b.subscribe("cmd/#", cmd, reply=True)
        a.publish("cmd/led", "on", to="b")
        self.sim.run([a, b], ms=1000)
        self.assertEqual(calls, [("cmd/led", "a")])
        self.assertEqual(self.sim.mine(1), [("cmd/led/reply", b"ok:on", "b")])
        self.assertEqual(self.data(1, 2), [("cmd/led", b"on")])

    def test_fixed_reply_topic_none_and_errors(self):
        a, b = self.two(reply_suffix="/ack")
        b.subscribe("get", lambda t, p, peer: {"rssi": -60}, reply="state")

        @b.on("cmd/+", reply=True)
        def cmd(topic, payload, peer):
            if payload == b"boom":
                raise ValueError("bad command")
            return None
        a.publish("get", "", to="b")
        a.publish("cmd/a", "quiet", to="b")
        a.publish("cmd/b", "boom", to="b")
        self.sim.run([a, b], ms=1000)
        self.assertEqual([(t, json.loads(p)) for t, p, peer in self.sim.mine(1)],
                         [("state", {"rssi": -60}), ("cmd/b/ack", {"error": "ValueError: bad command"})])


class PresenceTests(Base):
    def test_heartbeat_keeps_idle_peers_online(self):
        a, b = self.two(heartbeat=20)
        n0 = len(AIR.frames(mac(1), mac(2)))
        self.sim.run([a, b], ms=100000)
        self.assertEqual((a.state, b.state), ("online", "online"))
        pings = len(AIR.frames(mac(1), mac(2))) - n0
        self.assertTrue(2 <= pings <= 6, pings)  # each side pings only when it heard nothing for 20 s

    def test_vanished_peer_is_detected_and_found_again(self):
        a, b = self.two(heartbeat=10, backoff_max=8)
        AIR.nodes[mac(2)].awake = False
        self.assertTrue(self.sim.run([a, b], lambda: a.state == "offline", 20000))
        self.assertIn((1, "b", False), self.sim.peer_events)
        mid = a.publish("t", "while away", to="b")
        self.sim.run([a, b], ms=20000)
        self.assertTrue(a.is_pending(mid))
        AIR.nodes[mac(2)].awake = True
        self.assertTrue(self.sim.run([a, b], lambda: not a.pending(), 15000))
        self.assertEqual(a.state, "online")
        self.assertEqual([e for e in self.sim.peer_events if e[0] == 1][-1], (1, "b", True))
        gaps = [e[0] for e in AIR.frames(mac(1), mac(2)) if not e[5]]
        gaps = [(gaps[i + 1] - gaps[i]) // 1000 for i in range(len(gaps) - 1)]
        self.assertTrue(max(gaps) <= 10, gaps)  # retries back off, capped near backoff_max

    def test_bye_marks_the_peer_offline_at_once(self):
        a, b = self.two()
        b.stop()
        self.sim.run(a, ms=100)
        self.assertEqual(a.peer("b")["online"], False)
        self.assertIn((1, "b", False), self.sim.peer_events)
        self.assertEqual(b.state, "idle")
        self.assertFalse(AIR.nodes[mac(2)].esp.active())


class ChannelTests(Base):
    def gateway(self, **kw):
        g = self.sim.make(1, [("s", addr(2))], wifi=True, **kw)
        self.sim.run(g, lambda: g.state not in ("idle", "no_wifi"), 30000)
        return g

    def test_follows_the_wifi_channel_and_disables_power_saving(self):
        g = self.gateway()
        s = self.sim.make(2, [("g", addr(1))], channel=6)
        self.assertTrue(self.sim.run([g, s], self.sim.online(g, s), 30000))
        st = g.status()
        self.assertEqual((st["channel"], st["channel_source"]), (6, "wifi"))
        self.assertEqual(g.wlan.pm, g.wlan.PM_NONE)
        self.assertEqual((self.sim.seq(1)[0], self.sim.seq(1)[-1]), ("no_wifi", "online"))
        self.assertTrue(any(" espnow: " in line for line in g.wifi.log.tail()))  # shared log

    def test_waits_for_wifi_and_queues_meanwhile(self):
        W.set_up("Home", False)
        g = self.sim.make(1, [("s", addr(2))], wifi=True)
        s = self.sim.make(2, [("g", addr(1))], channel=6)
        self.sim.run([g, s], ms=20000)
        self.assertEqual(g.state, "no_wifi")
        mid = g.publish("t", "x", to="s")
        self.sim.run([g, s], ms=2000)
        self.assertTrue(g.is_pending(mid))
        self.assertEqual(AIR.frames(mac(1), mac(2)), [])
        W.set_up("Home", True)
        self.assertTrue(self.sim.run([g, s], lambda: not g.pending(), 120000))
        self.assertEqual(self.sim.mine(2), [("t", b"x", "g")])

    def test_auto_channel_search_finds_the_peer_and_remembers_it(self):
        g = self.gateway()
        s = self.sim.make(2, [("g", addr(1))])  # channel="auto": starts on 1, the gateway is on 6
        self.assertTrue(self.sim.run([g, s], self.sim.online(g, s), 60000))
        self.assertIn("scanning", self.sim.seq(2))
        self.assertEqual(s.status()["channel"], 6)
        with open(self.sim.path(2)) as f:
            self.assertEqual(json.load(f)["channel"], 6)
        self.sim.states[2] = []
        s = self.sim.make(2, [("g", addr(1))])  # reboot: straight to the saved channel
        self.assertTrue(self.sim.run([g, s], self.sim.online(s), 5000))
        self.assertFalse("scanning" in self.sim.seq(2))

    def test_router_channel_change_is_followed_by_both_sides(self):
        g = self.gateway()
        s = self.sim.make(2, [("g", addr(1))])
        self.assertTrue(self.sim.run([g, s], self.sim.online(g, s), 60000))
        W.aps["Home"]["channel"] = 11
        self.assertTrue(self.sim.run([g, s], lambda: s.status()["channel"] == 11 and s.state == "online", 120000))
        self.assertIn("channel 11 (WiFi)", self.sim.log(g))
        mid = g.publish("t", "after", to="s")
        self.assertTrue(self.sim.run([g, s], lambda: not g.is_pending(mid), 10000))

    def test_failed_search_goes_home_and_backs_off(self):
        s = self.sim.make(2, [("g", addr(1))], backoff_max=20, scan_dwell_ms=100)
        self.sim.run(s, ms=120000)
        self.assertTrue(self.sim.run(s, lambda: s.state == "offline", 3000))  # not in the middle of a search
        self.assertEqual(s.status()["channel"], 1)
        self.assertIn("no peer answered on any channel", self.sim.log(s))
        scans = self.sim.states[2].count("scanning")
        self.assertTrue(4 <= scans <= 10, scans)  # 2, 4, 8, 16, 20, 20 ... s apart

    def test_fixed_channel_never_searches(self):
        s = self.sim.make(2, [("g", addr(1))], channel=4)
        self.sim.run(s, ms=60000)
        self.assertEqual((s.state, s.status()["channel"]), ("offline", 4))
        self.assertFalse("scanning" in self.sim.seq(2))
        self.assertEqual(set(e[4] for e in AIR.log), {4})


class DiscoveryTests(Base):
    def test_pairing_finds_the_hub_saves_it_and_survives_a_reboot(self):
        hub = self.sim.make(1, channel=3, group="home", name="hub", discovery=True)
        node = self.sim.make(2, group="home", name="kitchen")
        node.pair(30)
        self.assertTrue(self.sim.run([hub, node], self.sim.online(hub, node), 60000))
        self.assertEqual([(p["name"], p["mac"], p["saved"]) for p in hub.peers()], [("kitchen", addr(2), True)])
        self.assertEqual([p["name"] for p in node.peers()], ["hub"])
        self.assertIn("paired with kitchen", self.sim.log(hub))
        self.assertEqual(node.status()["channel"], 3)
        node.publish("hello", "world", to="hub")
        self.sim.run([hub, node], ms=500)
        self.assertEqual(self.sim.mine(1), [("hello", b"world", "kitchen")])
        self.sim.states[2] = []
        node = self.sim.make(2, group="home", name="kitchen")  # reboot, no pairing
        self.assertTrue(self.sim.run([hub, node], self.sim.online(node), 5000))
        self.assertEqual(node.peers()[0]["name"], "hub")
        self.assertFalse("scanning" in self.sim.seq(2))

    def test_other_groups_are_not_paired_and_the_window_closes(self):
        hub = self.sim.make(1, channel=1, group="home", discovery=True)
        other = self.sim.make(2, channel=1, group="office")
        other.pair(5)
        self.sim.run([hub, other], ms=10000)
        self.assertEqual((hub.peers(), other.peers()), ([], []))
        self.assertIn("pairing closed", self.sim.log(other))
        self.assertFalse(other.status()["pairing"])
        n = len(AIR.frames(mac(2)))
        self.sim.run([hub, other], ms=10000)
        self.assertEqual(len(AIR.frames(mac(2))), n)  # no more HELLOs
        late = self.sim.make(3, channel=1, group="home")  # same group, but not pairing: hub stays quiet
        self.sim.run([hub, late], ms=5000)
        self.assertEqual(late.peers(), [])

    def test_pairing_with_a_shared_key(self):
        hub = self.sim.make(1, channel=1, group="g", name="hub", discovery=True, lmk=KEY)
        self.sim.run(hub, ms=1000)  # its own HELLO went out before the node existed: the node must be answered
        node = self.sim.make(2, channel=1, group="g", lmk=KEY)
        node.pair(10)
        self.assertTrue(self.sim.run([hub, node], self.sim.online(hub, node), 10000))
        self.assertEqual(AIR.nodes[mac(1)].esp.peers[mac(2)][0], KEY.encode())  # encrypted from now on
        self.assertEqual(AIR.nodes[mac(2)].esp.peers[mac(1)][0], KEY.encode())
        node.publish("secret", "s3cr3t", to="hub")
        self.sim.run([hub, node], ms=500)
        self.assertEqual(self.sim.mine(1), [("secret", b"s3cr3t", addr(2))])
        with open(self.sim.path(2)) as f:
            self.assertEqual(json.load(f)["peers"][0]["lmk"], "30313233343536373839616263646566")

    def test_names_given_in_code_win_and_learned_names_are_updated(self):
        hub = self.sim.make(1, [("boss", addr(2))], channel=1, group="g", discovery=True)
        node = self.sim.make(2, channel=1, group="g", name="n1")
        node.pair(10)
        self.assertTrue(self.sim.run([hub, node], self.sim.online(hub, node), 10000))
        self.assertEqual(hub.peers()[0]["name"], "boss")
        self.assertEqual(node.peers()[0]["name"], None)  # the hub has no name: its MAC is used
        self.assertEqual(node.peers()[0]["mac"], addr(1))


class PeerTests(Base):
    def test_add_remove_and_lookup(self):
        a, b = self.two()
        self.assertEqual(a.add_peer(addr(5), "five"), "five")
        self.assertEqual(a.peer("five")["mac"], addr(5))
        self.assertEqual(a.peer(addr(5))["name"], "five")
        self.assertIsNone(a.peer("nobody"))
        with open(self.sim.path(1)) as f:
            self.assertEqual([p["name"] for p in json.load(f)["peers"]], ["five"])
        mid = a.publish("t", "x", to="five")  # nobody there: it stays queued...
        self.assertTrue(a.remove_peer("five"))  # ...and is dropped with its peer
        self.assertFalse(a.is_pending(mid))
        self.assertEqual(a.status()["dropped"], 1)
        self.assertFalse(a.remove_peer("five"))
        with open(self.sim.path(1)) as f:
            self.assertEqual(json.load(f)["peers"], [])
        self.assertEqual([p["name"] for p in a.peers()], ["b"])
        self.assertTrue(a.remove_peer("b"))  # a constructor peer: gone for now, back at boot
        self.assertRaises(ValueError, a.publish, "t", "x", to="b")

    def test_peer_table_limit(self):
        a = self.sim.make(1, channel=1)
        for i in range(espnow.MAX_TOTAL_PEER_NUM - 1):
            a.add_peer(bytes((2, 0, 0, 0, 0, i)), save=False)
        self.sim.run(a, ms=100)
        self.assertRaises(OSError, a.add_peer, addr(99))
        self.assertIn("peer table full", self.sim.log(a))

    def test_encryption_keys(self):
        a = self.sim.make(1, [("b", addr(2), KEY)], channel=1, pmk="pmk-pmk-pmk-pmk!")
        b = self.sim.make(2, [("a", addr(1), KEY)], channel=1, pmk="pmk-pmk-pmk-pmk!")
        c = self.sim.make(3, [("a", addr(1), "fedcba9876543210")], channel=1, pmk="pmk-pmk-pmk-pmk!")
        a.add_peer(addr(3), "c", KEY)
        self.assertTrue(self.sim.run([a, b, c], self.sim.online(b), 5000))
        self.assertEqual(AIR.nodes[mac(1)].esp.peers[mac(2)][0], KEY.encode())
        self.sim.run([a, b, c], ms=5000)
        self.assertEqual(a.peer("c")["online"], False)  # different key: frames do not get through
        with open(self.sim.path(1)) as f:
            self.assertEqual(json.load(f)["peers"][0]["lmk"], "30313233343536373839616263646566")


class DriverTests(Base):
    def test_blocking_connect_check_and_flush(self):
        b = self.sim.make(2, [("a", addr(1))], channel=1)
        b.check()
        a = self.sim.make(1, [("b", addr(2))], channel=1)
        got = []
        a.subscribe("in", lambda t, p, peer: got.append(p))
        self.assertTrue(a.connect(timeout=5))
        self.assertEqual(a.state, "online")
        b.publish("in", "hello", to="a")
        for _ in range(10):
            b.check()
            a.check()
            self.sim.advance(100)
        self.assertEqual(got, [b"hello"])
        AIR.nodes[mac(2)].awake = False
        mid = a.publish("out", "2", to="b")
        self.assertTrue(a.is_pending(mid))
        t0 = self.sim.t
        self.assertFalse(a.flush(timeout=5))
        self.assertTrue(5000 <= self.sim.t - t0 <= 6000, self.sim.t - t0)
        AIR.nodes[mac(2)].awake = True
        self.assertTrue(a.flush(timeout=60))

    def test_blocking_connect_timeout_and_no_peers(self):
        a = self.sim.make(1, [("b", addr(2))], channel=1)
        t0 = self.sim.t
        self.assertFalse(a.connect(timeout=10))
        self.assertTrue(10000 <= self.sim.t - t0 <= 11500, self.sim.t - t0)
        lone = self.sim.make(3, channel=1)
        self.assertTrue(lone.connect(timeout=1))  # nothing to wait for: broadcast-only node

    def test_blocking_connect_brings_up_the_wifi(self):
        g = self.sim.make(1, [("s", addr(2))], wifi=True)
        s = self.sim.make(2, [("g", addr(1))], channel=6)
        s.check()
        self.assertTrue(g.connect(timeout=60))
        self.assertTrue(g.wifi.is_connected())
        self.assertEqual(g.status()["channel"], 6)

    def test_deep_sleep_sensor_pattern(self):
        hub = self.sim.make(1, channel=3, group="h", name="hub", discovery=True)
        self.sim.background(hub)
        s = self.sim.make(2, group="h", name="garden")  # first boot: no peer yet, pair with the hub
        self.assertEqual(s.peers(), [])
        s.pair(30)
        self.assertTrue(s.connect(timeout=30))
        s.publish("sensors/garden", {"t": 21.5})
        self.assertTrue(s.flush(timeout=5))
        AIR.nodes[mac(2)].awake = False   # machine.deepsleep()
        self.sim.sleep(60000)
        s = self.sim.make(2, group="h", name="garden")  # the next wake-up
        t0 = self.sim.t
        s.publish("sensors/garden", {"t": 22.0})
        self.assertTrue(s.flush(timeout=5))
        self.assertTrue(self.sim.t - t0 < 100, self.sim.t - t0)  # saved hub and channel: straight there
        self.sim.sleep(200)  # the hub reads it at its next step
        self.assertEqual(self.sim.mine(1), [("sensors/garden", b'{"t": 21.5}', "garden"),
                                            ("sensors/garden", b'{"t": 22.0}', "garden")])

    def test_status_rxbuf_and_lost_frames(self):
        a, b = self.two()
        st = a.status()
        self.assertEqual((st["state"], st["mac"], st["channel"], st["channel_source"], st["peers"],
                          st["peers_online"]), ("online", addr(1), 1, "fixed", 1, 1))
        self.assertEqual(len(st["stats"]), 5)
        self.assertEqual(AIR.nodes[mac(2)].esp.rxbuf, 2048)  # set before ESP-NOW was activated
        for i in range(60):  # b does not run meanwhile: its receive buffer overflows
            a.publish("flood", "x" * 40, to="b")
        self.sim.run([a, b], ms=12000)
        self.assertIn("incoming frame(s) lost", self.sim.log(b))
        self.assertTrue(b.peer("a")["rssi"] is not None)


if __name__ == "__main__":
    unittest.main()
