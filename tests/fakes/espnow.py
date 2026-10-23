"""Fake `espnow` module (MicroPython v1.29 API) for the unix-port tests: a scriptable radio "air".

    AIR.reset(clock, advance)       # clock() -> ms, advance(ms) moves a virtual clock
    node = AIR.add_node(mac)        # the next WLAN()/ESPNow() created belong to this node
    network.WLAN = NodeWLAN         # done by the harness: every node has its own radio

A frame reaches a node that is awake, has ESP-NOW active, listens on the sender's channel and is
in range. The node's channel is its WLAN's: the AP's channel while the STA is connected, else
the one set with `wlan.config(channel=n)`. Unicast frames are acknowledged by the receiver's radio
(even when its receive buffer is full, like the real MAC layer); a failed unicast costs some time.
Runs on the MicroPython unix port (stdlib only).
"""
import network

MAX_DATA_LEN = 1470  # v1.29 is built with ESP-IDF >= 5.4 (ESP-NOW v2)
ADDR_LEN = 6
KEY_LEN = 16
MAX_TOTAL_PEER_NUM = 20
MAX_ENCRYPT_PEER_NUM = 6
BROADCAST = b"\xff" * 6

# what check_esp_err() raises: OSError(-(code), "name")
NOT_INIT = (-12389, "ESP_ERR_ESPNOW_NOT_INIT")
ARG = (-12390, "ESP_ERR_ESPNOW_ARG")
NO_MEM = (-12391, "ESP_ERR_ESPNOW_NO_MEM")
FULL = (-12392, "ESP_ERR_ESPNOW_FULL")
NOT_FOUND = (-12393, "ESP_ERR_ESPNOW_NOT_FOUND")
EXIST = (-12395, "ESP_ERR_ESPNOW_EXIST")


def _err(e):
    return OSError(e[0], e[1])


class Node:
    def __init__(self, air, mac, channel):
        self.air = air
        self.mac = mac
        self.channel = channel   # radio channel while the STA is not connected
        self.wlan = None
        self.esp = None
        self.awake = True        # False: powered off / in deep sleep
        self.rssi = -50

    def radio_channel(self):
        w = self.wlan
        if w is not None and w.isconnected():
            return network.WORLD.aps[w._target[0]]["channel"]
        return self.channel

    def hears(self, src, channel):
        return (self.awake and self.esp is not None and self.esp._active
                and (self.wlan is None or self.wlan.active())
                and self.radio_channel() == channel
                and (src.mac, self.mac) not in self.air.blocked)


class Air:
    def __init__(self):
        self.reset(lambda: 0, lambda ms: None)

    def reset(self, clock, advance):
        self.clock = clock
        self.advance = advance
        self.nodes = {}
        self.current = None
        self.blocked = set()     # (from, to) pairs out of range
        self.lose_acks = {}      # mac -> how many ACKs from it get lost (the frame still arrives)
        self.no_mem = 0          # how many send()s raise ESP_ERR_ESPNOW_NO_MEM
        self.ok_ms = 1           # time a send() takes when acknowledged / when not
        self.fail_ms = 20
        self.log = []            # (time, from, to, msg, channel, acked)
        self.blocking_reads = 0  # recv() calls that would block on a board

    def add_node(self, mac, channel=1):
        """A node for `mac` (an existing one is reset: a reboot). The next WLAN()/ESPNow() belong to it."""
        n = self.nodes.get(mac)
        if n is None:
            n = self.nodes[mac] = Node(self, mac, channel)
        else:
            n.wlan = n.esp = None
            n.awake = True
        self.current = n
        return n

    def bare(self, mac, channel=1):
        """A plain ESP-NOW device without espnowmanager: send/receive by hand through `node.esp`."""
        n = self.add_node(mac, channel)
        e = ESPNow()
        e.active(True)
        e.add_peer(BROADCAST)
        return n

    def range(self, a, b, ok):
        for pair in ((a, b), (b, a)):
            if ok:
                self.blocked.discard(pair)
            else:
                self.blocked.add(pair)

    def frames(self, src=None, dst=None):
        return [e for e in self.log if (src is None or e[1] == src) and (dst is None or e[2] == dst)]

    def transmit(self, src, dst, msg, lmk):
        ch = src.radio_channel()
        if not src.awake:  # a node that is off sends nothing (its code may still run in a test)
            self.advance(self.fail_ms)
            return False
        if dst == BROADCAST:
            for n in list(self.nodes.values()):
                if n is not src and n.hears(src, ch):
                    n.esp._deliver(src.mac, msg)
            self.log.append((self.clock(), src.mac, dst, msg, ch, True))
            self.advance(self.ok_ms)
            return True
        n = self.nodes.get(dst)
        ok = n is not None and n.hears(src, ch) and n.esp._key_for(src.mac) == lmk
        if ok:
            n.esp._deliver(src.mac, msg)
            if self.lose_acks.get(dst):
                self.lose_acks[dst] -= 1
                ok = False
        self.log.append((self.clock(), src.mac, dst, msg, ch, ok))
        self.advance(self.ok_ms if ok else self.fail_ms)
        return ok


AIR = Air()


class ESPNow:
    def __init__(self):
        node = AIR.current
        if node is None:
            raise RuntimeError("fake espnow: AIR.add_node() first")
        self.node = node
        node.esp = self
        self._active = False
        self._rxbuf_cfg = 526    # firmware default for v1-sized frames (see modespnow.c)
        self.rxbuf = 0
        self.timeout_ms = 300000
        self.peers = {}          # mac -> lmk (None: not encrypted)
        self.pmk = None
        self.rx = []
        self.rx_bytes = 0
        self.cb = None
        self.peers_table = {}
        self.tx = self.tx_ok = self.tx_fail = self.rx_n = self.rx_dropped = 0

    # ---- setup ------------------------------------------------------------------------
    def active(self, flag=None):
        if flag is not None:
            if flag and not self._active:
                self.rxbuf = self._rxbuf_cfg
                self.rx, self.rx_bytes = [], 0
            elif not flag:
                self.peers = {}
                self.rx, self.rx_bytes = [], 0
                self.cb = None
            self._active = bool(flag)
        return self._active

    def config(self, *args, **kw):
        for k in kw:
            if k == "rxbuf":
                self._rxbuf_cfg = kw[k]   # used at the next active(True), as in modespnow.c
            elif k == "timeout_ms":
                self.timeout_ms = kw[k]
            elif k != "rate":
                raise TypeError("unexpected keyword argument '%s'" % k)
        if args:
            if args[0] == "rxbuf":
                return self._rxbuf_cfg
            if args[0] == "timeout_ms":
                return self.timeout_ms
            raise ValueError("unknown config param")

    def irq(self, callback):
        self.cb = callback

    def stats(self):
        return (self.tx, self.tx_ok, self.tx_fail, self.rx_n, self.rx_dropped)

    def _check(self):
        if not self._active:
            raise _err(NOT_INIT)

    # ---- peers ------------------------------------------------------------------------
    def set_pmk(self, pmk):
        self._check()
        if len(pmk) != KEY_LEN:
            raise ValueError("invalid buffer length")
        self.pmk = bytes(pmk)

    def add_peer(self, mac, lmk=None, channel=None, ifidx=None, encrypt=None):
        self._check()
        if len(mac) != ADDR_LEN:
            raise ValueError("invalid buffer length")
        if lmk and len(lmk) != KEY_LEN:
            raise ValueError("invalid buffer length")
        mac = bytes(mac)
        if mac in self.peers:
            raise _err(EXIST)
        if len(self.peers) >= MAX_TOTAL_PEER_NUM:
            raise _err(FULL)
        if lmk and len([k for k in self.peers.values() if k]) >= MAX_ENCRYPT_PEER_NUM:
            raise _err(FULL)
        self.peers[mac] = (bytes(lmk), channel or 0) if lmk else (None, channel or 0)

    def del_peer(self, mac):
        self._check()
        if bytes(mac) not in self.peers:
            raise _err(NOT_FOUND)
        del self.peers[bytes(mac)]

    def get_peer(self, mac):
        self._check()
        e = self.peers.get(bytes(mac))
        if e is None:
            raise _err(NOT_FOUND)
        return (bytes(mac), e[0] or b"\0" * 16, e[1], 0, e[0] is not None)

    def get_peers(self):
        return tuple(self.get_peer(m) for m in self.peers if m != BROADCAST)

    def peer_count(self):
        return (len(self.peers), len([e for e in self.peers.values() if e[0]]))

    def _key_for(self, mac):
        """What decrypts frames from `mac` here: (pmk, lmk) or None when not encrypted."""
        e = self.peers.get(mac)
        return (self.pmk, e[0]) if e and e[0] else None

    # ---- traffic ----------------------------------------------------------------------
    def send(self, mac, msg=None, sync=True):
        self._check()
        if msg is None:
            mac, msg = None, mac
        if len(msg) > MAX_DATA_LEN:
            raise _err(ARG)
        if AIR.no_mem:
            AIR.no_mem -= 1
            AIR.advance(2000)    # the firmware retries for 2 s before raising
            raise _err(NO_MEM)
        dests = [m for m in self.peers if m != BROADCAST] if mac is None else [bytes(mac)]
        all_ok = True
        for d in dests:
            if d not in self.peers:
                raise _err(NOT_FOUND)
            e = self.peers[d]
            key = (self.pmk, e[0]) if e[0] else None
            self.tx += 1
            ok = AIR.transmit(self.node, d, bytes(msg), key)
            if ok:
                self.tx_ok += 1
            else:
                self.tx_fail += 1
                all_ok = False
        return all_ok or not sync

    def _deliver(self, src, msg):
        size = 6 + 8 + 6 + len(msg)
        if self.rx_bytes + size >= self.rxbuf:
            self.rx_dropped += 1
            return
        self.rx.append((src, msg))
        self.rx_bytes += size
        self.rx_n += 1
        if self.cb is not None:
            self.cb(self)

    def any(self):
        self._check()
        return bool(self.rx)

    def recv(self, timeout_ms=None):
        self._check()
        if not self.rx:
            if timeout_ms is None or timeout_ms != 0:
                AIR.blocking_reads += 1  # on a board this blocks (up to 5 minutes by default)
            return (None, None)
        src, msg = self.rx.pop(0)
        self.rx_bytes -= 6 + 8 + 6 + len(msg)
        self.peers_table[src] = [self.node.rssi, AIR.clock()]
        return [src, msg]


_BaseWLAN = network.WLAN


class NodeWLAN(_BaseWLAN):
    """The fake WLAN, one per node: its own channel and MAC, and the `pm` setting."""
    PM_NONE = 0
    PM_PERFORMANCE = 1
    PM_POWERSAVE = 2

    def __init__(self, iface):
        _BaseWLAN.__init__(self, iface)
        self.node = AIR.current
        self.pm = NodeWLAN.PM_PERFORMANCE
        if self.node is not None and iface == _BaseWLAN.IF_STA:
            self.node.wlan = self

    def config(self, *args, **kw):
        if args:
            key = args[0]
            if key == "channel":
                return self.node.radio_channel()
            if key == "mac":
                return self.node.mac
            if key == "pm":
                return self.pm
            return _BaseWLAN.config(self, *args)
        if "channel" in kw:
            ch = kw.pop("channel")
            if self.isconnected():
                raise OSError(-12290, "ESP_ERR_WIFI_IF")  # the AP decides the channel
            self.node.channel = ch
        if "pm" in kw:
            self.pm = kw.pop("pm")
        if kw:
            _BaseWLAN.config(self, **kw)
