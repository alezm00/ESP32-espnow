"""Virtual-clock harness: several ESPNowManagers (and their WiFiManagers) on the fake air, no sleeping."""
import _setup  # noqa: F401

import espnow
import network
from _util import rmtree, tmpdir
from wifimanager import WiFiManager, compat
from espnowmanager import ESPNowManager

_orig = (compat.ticks_ms, compat.sleep_ms_blocking, compat.run_for, network.WLAN)
W = network.WORLD
AIR = espnow.AIR


def mac(n):
    return bytes((0x24, 0x0A, 0xC4, 0, 0, n))


def addr(n):
    return "24:0a:c4:00:00:%02x" % n


class Sim:
    def __init__(self):
        self.t = 100000
        compat.ticks_ms = lambda: self.t
        compat.sleep_ms_blocking = self.sleep
        compat.run_for = self.sleep
        network.WLAN = espnow.NodeWLAN
        W.reset()
        W.add_ap("Neighbor", "neighborpw", rssi=-90)   # scans are never empty (an empty scan = "scan failed")
        W.add_ap("Home", "homepass12", delay_ms=1000, channel=6)
        AIR.reset(lambda: self.t, self.advance)
        self.dir = tmpdir()
        self.states = {}        # node number -> states, in order
        self.peer_events = []   # (node number, peer, online)
        self.got = []           # (node number, topic, payload, peer) seen by on_message
        self.bg = []            # managers stepped while a blocking call (connect, flush...) sleeps
        self._bg_at = []

    def close(self):
        compat.ticks_ms, compat.sleep_ms_blocking, compat.run_for, network.WLAN = _orig
        AIR.reset(lambda: 0, lambda ms: None)
        rmtree(self.dir)

    def advance(self, ms):
        self.t += ms

    def background(self, en):
        """Keep `en` running (on virtual time) whenever another manager's blocking call sleeps."""
        en._boot()
        self.bg.append(en)
        self._bg_at.append(self.t)

    def sleep(self, ms):
        end = self.t + ms
        while self.bg:
            for i, m in enumerate(self.bg):
                if m._kick:
                    m._kick = False
                    self._bg_at[i] = self.t
            nxt = min(self._bg_at)
            if nxt > end:
                break
            self.t = max(self.t, nxt)
            for i, m in enumerate(self.bg):
                if self._bg_at[i] <= self.t:
                    self._bg_at[i] = self.t + m._step()
        self.t = max(self.t, end)

    def path(self, n):
        return "%s/peers-%d.json" % (self.dir, n)

    def make(self, n, peers=None, wifi=False, radio_channel=1, **kw):
        """Node `n` (MAC mac(n)) running an ESPNowManager; wifi=True gives it a WiFiManager for 'Home'.

        Making node `n` again is a reboot: same MAC, same peers file, fresh radio.
        """
        AIR.add_node(mac(n), radio_channel)
        wm = None
        if wifi:
            wm = WiFiManager(None, config_file="%s/wifi-%d.json" % (self.dir, n), config_mode="never",
                             log_level="DEBUG")
            wm.log.console = False
            wm.add_network("Home", "homepass12")
        kw.setdefault("peers_file", self.path(n))
        kw.setdefault("log_level", "DEBUG")
        kw.setdefault("log_buffer", 500)
        kw.setdefault("on_state", lambda old, new: self.states.setdefault(n, []).append(new))
        kw.setdefault("on_peer", lambda peer, online: self.peer_events.append((n, peer, online)))
        kw.setdefault("on_message", lambda topic, payload, peer: self.got.append((n, topic, payload, peer)))
        en = ESPNowManager(peers, wifi=wm, **kw)
        en.log.console = False
        return en

    def run(self, mgrs, until=None, ms=60000):
        """Drive every supervisor on virtual time until `until()` is true or `ms` elapsed."""
        if not isinstance(mgrs, (list, tuple)):
            mgrs = [mgrs]
        try:
            return self._run(mgrs, until, ms)
        finally:
            if AIR.blocking_reads:  # every run checks it: tearDown failures abort MicroPython's runner
                raise AssertionError("a recv() with a timeout would block a real board")

    def _run(self, mgrs, until, ms):
        wms = [m.wifi for m in mgrs if m.wifi is not None]
        for wm in wms:
            wm._boot()
        for m in mgrs:
            m._boot()
        end = self.t + ms
        w_at = [self.t] * len(wms)
        m_at = [self.t] * len(mgrs)
        for _ in range(2000000):
            for i, wm in enumerate(wms):
                if self.t >= w_at[i]:
                    step = wm._next()
                    w_at[i] = self.t + (step if step is not None else 10 ** 9)
            for i, m in enumerate(mgrs):
                if m._kick:
                    m._kick = False
                    m_at[i] = self.t
                if self.t >= m_at[i]:
                    m_at[i] = self.t + m._step()
            if until is not None and until():
                return True
            if self.t >= end:
                break
            self.t = max(self.t, min(w_at + m_at + [end]))
        return until is None

    def online(self, *mgrs):
        return lambda: all(m.state == "online" for m in mgrs)

    def seq(self, n):
        out = []
        for s in self.states.get(n, []):
            if not out or out[-1] != s:
                out.append(s)
        return out

    def log(self, en):
        return "\n".join(en.log.tail())

    def mine(self, n):
        """(topic, payload, peer) received by node `n` through on_message."""
        return [g[1:] for g in self.got if g[0] == n]
