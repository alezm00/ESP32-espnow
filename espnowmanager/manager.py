"""ESPNowManager: peers, presence, offline queue, delivery tracking, channel search, pairing, commands/replies.

The whole supervision logic is ONE step function (`_step`) that returns "wait this many
milliseconds". Three thin drivers run it, exactly like WiFiManager and MQTTManager:

* asyncio  : `await en.start()`   (background task; also starts the WiFiManager, if any)
* blocking : `en.connect()`       (returns once a peer answers)
* polling  : `en.check()` in your own while-loop (also runs `wm.check()`)

The frames themselves are described in frame.py.
"""
import random
from binascii import unhexlify

import espnow
import network
from wifimanager import compat as C
from wifimanager.compat import ticks_add, ticks_diff
from wifimanager.log import Logger

from .frame import (BROADCAST_MAC, BYE, DATA, F_ANSWER, F_REPLY, HELLO, PING, data_body, decode, encode,
                    group_id, key_bytes, mac_bytes, mac_text, payload_bytes, split_data, topic_matches)
from .store import PeerStore

__version__ = "1.0.0"

# supervisor states (what `en.state` returns)
IDLE = "idle"
NO_WIFI = "no_wifi"
SCANNING = "scanning"
ONLINE = "online"
OFFLINE = "offline"

BROADCAST = "*"  # publish(..., to=BROADCAST): one broadcast frame, no ACK

_MAX_BURST = 20      # incoming frames processed per step
_DROPPED_KEEP = 16   # ids of dropped messages remembered for wait_delivered()
_SEEN_KEEP = 16      # message ids remembered per peer to drop duplicates
_SEEN_MS = 30000     # ... for this long (a peer that rebooted starts from a random id)
_RETRY_MS = 300      # pause between the quick retries before a peer is declared unreachable
_PAIR_HELLO_MS = 2000
_STATS_MS = 10000

# subscription record: [filter, handler, reply]
_F, _H, _R = 0, 1, 2


def _err_name(e):
    return str(e.args[1]) if len(e.args) > 1 else str(e)


def _is_broadcast(to):
    if isinstance(to, str):
        return to == BROADCAST or to.lower() == "ff:ff:ff:ff:ff:ff"
    return isinstance(to, (bytes, bytearray)) and bytes(to) == BROADCAST_MAC


def _parse_peer(item):
    """"aa:bb:..", 6 bytes, (name, mac[, lmk]) or {"mac":.., "name":.., "lmk":..} -> (mac, name, lmk)."""
    if isinstance(item, dict):
        mac, name, lmk = item.get("mac"), item.get("name"), item.get("lmk")
    elif isinstance(item, (str, bytes, bytearray)):
        mac, name, lmk = item, None, None
    else:
        item = tuple(item)
        if not 2 <= len(item) <= 3:
            raise ValueError("expected (name, mac[, lmk])")
        name, mac, lmk = item[0], item[1], item[2] if len(item) > 2 else None
    if name is not None and (not isinstance(name, str) or not name):
        raise ValueError("peer name must be a non-empty string")
    return mac_bytes(mac), name, key_bytes(lmk) if lmk else None


class _Peer:
    def __init__(self, mac, name=None, lmk=None, saved=False, fixed_name=False):
        self.mac = mac                # 6 bytes
        self.addr = mac_text(mac)
        self.name = name
        self.lmk = lmk                # 16 bytes, or None (not encrypted)
        self.saved = saved            # kept in peers_file (learned or added by code)
        self.fixed_name = fixed_name  # name given in code: a HELLO never renames it
        self.online = None            # None = not contacted yet
        self.last_ok = None           # last frame received from it or acknowledged by it
        self.fails = 0                # consecutive sends without ACK
        self.next_try = None          # no sends before this (quick retry / backoff)
        self.probe = False            # ping it at the next step (e.g. the channel changed)
        self.seen = []                # [(message id, ticks)] of recent DATA frames

    @property
    def label(self):
        return self.name or self.addr


class ESPNowManager:
    IDLE, NO_WIFI, SCANNING, ONLINE, OFFLINE = IDLE, NO_WIFI, SCANNING, ONLINE, OFFLINE
    BROADCAST = BROADCAST

    def __init__(self, peers=None, *, wifi=None, name=None, group=None,
                 channel="auto", scan_channels=range(1, 14), scan_dwell_ms=150,
                 discovery=False, hello_interval=30, accept_unknown=False,
                 pmk=None, lmk=None, peers_file="/espnow_peers.json",
                 queue_size=20, max_len=250, peer_retries=3, heartbeat=30,
                 backoff_base=2, backoff_max=60, poll_ms=50, rxbuf=None, reply_suffix="/reply",
                 log_level="INFO", log_file=None, log_max_size=8192, log_buffer=60, share_log=True,
                 on_message=None, on_delivered=None, on_peer=None, on_state=None, on_raw=None):
        if channel != "auto" and (not isinstance(channel, int) or not 1 <= channel <= 14):
            raise ValueError("channel must be 'auto' or 1-14")
        if queue_size < 1:
            raise ValueError("queue_size must be at least 1")
        if not 16 <= max_len <= espnow.MAX_DATA_LEN:
            raise ValueError("max_len must be 16-%d bytes" % espnow.MAX_DATA_LEN)
        if name is not None and (not isinstance(name, str) or not 1 <= len(name.encode()) <= 32):
            raise ValueError("name must be 1-32 bytes")

        self.wifi = wifi
        self.wlan = getattr(wifi, "wlan", None) or network.WLAN(network.WLAN.IF_STA)
        self.esp = espnow.ESPNow()
        self.name = name
        self.group = group
        self.channel = channel
        self.scan_channels = list(scan_channels)
        self.scan_dwell_ms = scan_dwell_ms
        self.discovery = discovery
        self.hello_interval = max(1, hello_interval)
        self.accept_unknown = accept_unknown
        self.queue_size = queue_size
        self.max_len = max_len
        self.peer_retries = max(1, peer_retries)
        self.heartbeat = heartbeat
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max
        self.poll_ms = max(10, poll_ms)
        self.rxbuf = rxbuf
        self.reply_suffix = reply_suffix
        self._pmk = key_bytes(pmk) if pmk else None
        self._lmk = key_bytes(lmk) if lmk else None  # given to peers learned by pairing
        self._gid = group_id(group)
        self._name_b = name.encode() if name else b""

        self._cb_message = on_message
        self._cb_delivered = on_delivered
        self._cb_peer = on_peer
        self._cb_state = on_state
        self._cb_raw = on_raw

        self.log = Logger(log_level, log_buffer, log_file, log_max_size, tag="espnow")
        wlog = getattr(wifi, "log", None)
        if share_log and isinstance(getattr(wlog, "_lines", None), list):
            # same RAM buffer as the WiFi log: ESP-NOW lines show up in the config portal's log too
            self.log._lines = wlog._lines
            self.log.buffer_size = wlog.buffer_size

        # peers: the constructor's ones first, then those learned earlier (peers_file)
        self._plist = []
        for item in peers or ():
            mac, pname, plmk = _parse_peer(item)
            if self._find(mac) is None:
                self._plist.append(_Peer(mac, pname, plmk, fixed_name=pname is not None))
        self._store = PeerStore(peers_file, self.log) if peers_file else None
        if self._store is not None:
            self._store.load()
            for d in self._store.peers():
                try:
                    mac = mac_bytes(d["mac"])
                    plmk = key_bytes(unhexlify(d["lmk"])) if d["lmk"] else None
                except ValueError as e:
                    self.log.error("peers: ignoring %r (%s)", d.get("mac"), e)  # never log the key
                    continue
                if self._find(mac) is None:
                    self._plist.append(_Peer(mac, d["name"], plmk, saved=True))

        # supervisor state
        self._sta = IDLE
        self._booted = False
        self._running = False
        self._async = False
        self._task = None
        self._epoch = 0
        self._kick = False
        self._wake = 0
        self._in_step = False
        self._first_done = False
        self._radio_ok = False
        self._wifi_up = False
        self._channel = None
        self._src = None            # where the channel comes from: "wifi", "fixed" or "auto"
        self._warned_fixed = False
        self._pause = None          # radio error: no sends before this
        self._last_error = ""
        self._scan = None           # [channels left, channel before the search, listening until]
        self._scan_fails = 0
        self._scan_next = None
        self._pair_until = None
        self._hello_at = None
        self._stats_at = None
        self._rx_dropped = 0
        # messages and subscriptions
        self._seq = random.getrandbits(16)
        self._subs = []
        self._outbox = []   # [mid, peer (None = broadcast), frame]
        self._inbox = []    # (peer, topic, payload, flags), or (None, mac, msg, 0) for on_raw
        self._mid = 0
        self._dropped = []
        self._n_dropped = 0

    # =====================================================================================
    # public: state
    # =====================================================================================
    @property
    def state(self):
        return self._sta

    def is_online(self):
        return self._sta == ONLINE

    def pending(self):
        """Number of queued messages (one per recipient) not delivered yet."""
        return len(self._outbox)

    def is_pending(self, mid):
        for m in self._outbox:
            if m[0] == mid:
                return True
        return False

    def subscriptions(self):
        return [s[_F] for s in self._subs]

    def peers(self):
        """Known peers: [{name, mac, online, rssi, last_seen (s ago), queued, saved}]."""
        return [self._info(p) for p in self._plist]

    def peer(self, peer):
        p = self._lookup(peer)
        return self._info(p) if p is not None else None

    def status(self):
        try:
            mac = mac_text(self.wlan.config("mac"))
        except Exception:
            mac = None
        return {"state": self._sta, "online": self._sta == ONLINE, "channel": self._channel,
                "channel_source": self._src, "mac": mac, "name": self.name, "group": self.group,
                "peers": len(self._plist), "peers_online": len([p for p in self._plist if p.online]),
                "queued": len(self._outbox), "dropped": self._n_dropped, "pairing": self._discovering(),
                "last_error": self._last_error, "stats": self._stats(), "version": __version__}

    # =====================================================================================
    # public: messages
    # =====================================================================================
    def publish(self, topic, payload, to=None):
        """Queue a message and send it now if possible. Returns its id (see is_pending()).

        to: a peer (name or MAC), None = every known peer (one unicast each), BROADCAST = one
        broadcast frame. payload: str, bytes, None (empty) or anything json.dumps() accepts.
        While a peer is unreachable its messages wait in RAM (at most `queue_size`, the oldest are dropped).
        """
        if not isinstance(topic, str) or not topic or "+" in topic or "#" in topic:
            raise ValueError("invalid topic for publish: %r" % (topic,))
        if to is None:
            targets = list(self._plist)
        elif _is_broadcast(to):
            targets = [None]
        else:
            p = self._lookup(to)
            if p is None:
                raise ValueError("unknown peer %r" % (to,))
            targets = [p]
        return self._publish(topic, payload, targets, 0)

    def subscribe(self, topic, handler=None, reply=None):
        """Handle incoming messages whose topic matches `topic` ('+' and '#' wildcards).

        handler(topic, payload, peer) may be a plain or an `async def` function; `peer` is the
        sender's name (or MAC). Messages without a handler go to on_message. With reply=True
        what the handler returns is sent back to the sender on topic + reply_suffix;
        reply="some/topic" sends it on that topic instead.
        """
        if not isinstance(topic, str) or not topic:
            raise ValueError("topic filter must be a non-empty string")
        for s in self._subs:
            if s[_F] == topic:
                s[_H], s[_R] = handler, reply
                return
        self._subs.append([topic, handler, reply])

    def on(self, topic, reply=None):
        """Decorator form of subscribe(): @en.on("cmd/led", reply=True)."""
        def deco(fn):
            self.subscribe(topic, fn, reply)
            return fn
        return deco

    def unsubscribe(self, topic):
        for i, s in enumerate(self._subs):
            if s[_F] == topic:
                del self._subs[i]
                return True
        return False

    # =====================================================================================
    # public: peers
    # =====================================================================================
    def add_peer(self, mac, name=None, lmk=None, save=True):
        """Add (or update) a peer. With save=True it is kept in peers_file. Returns its name or MAC."""
        mac = mac_bytes(mac)
        lmk = key_bytes(lmk) if lmk else None
        p = self._find(mac)
        if p is None:
            p = self._add(mac, name, lmk, save, fixed=bool(name))
            if p is None:
                raise OSError("peer table full")
            return p.label
        if name:
            p.name = name
            p.fixed_name = True
        if lmk != p.lmk:
            p.lmk = lmk
            if self._booted:
                self._register(p)
        if save:
            p.saved = True
        self._save_peer(p)
        return p.label

    def remove_peer(self, peer):
        """Forget a peer (and drop its queued messages). Peers given to the constructor come back at boot."""
        p = self._lookup(peer)
        if p is None:
            return False
        self._plist.remove(p)
        for m in [m for m in self._outbox if m[1] is p]:
            self._outbox.remove(m)
            self._forget(m[0])
        if self._booted:
            try:
                self.esp.del_peer(p.mac)
            except Exception:
                pass
        if self._store is not None and p.saved:
            self._store.remove(p.addr)
        self.log.info("peer %s removed", p.label)
        return True

    def pair(self, seconds=60):
        """Open pairing for `seconds`: HELLOs are broadcast and nodes of the same group become peers."""
        self._pair_until = ticks_add(C.ticks_ms(), int(seconds * 1000))
        self._hello_at = None
        self._scan_next = None
        self._kick = True
        self.log.info("pairing open for %ds (group %s)", seconds, self.group)

    # =====================================================================================
    # public: control
    # =====================================================================================
    def stop(self, bye=True):
        """Stop supervising and switch ESP-NOW off. With bye=True reachable peers are told first."""
        if self._booted:
            if bye and self._radio_ok:
                f = self._frame(BYE)
                for p in self._plist:
                    if p.online:
                        try:
                            self.esp.send(p.mac, f, True)
                        except Exception:
                            pass
            try:
                self.esp.irq(None)
                self.esp.active(False)
            except Exception as e:
                self.log.warning("cannot switch ESP-NOW off: %s", C.exc_text(e))
        self._running = False
        self._booted = False
        self._task = None
        self._epoch += 1
        self._kick = True
        self._scan = None
        self._radio_ok = False
        self._first_done = False
        self._wifi_up = False
        self._channel = self._src = None
        for p in self._plist:
            p.online = None
        self._set_sta(IDLE)
        self.log.info("stopped")

    # =====================================================================================
    # driver: asyncio
    # =====================================================================================
    async def start(self, wait=True):
        """Start background supervision (and the WiFiManager's, if not running yet).

        With wait=True returns after every peer was tried once: True if one answered.
        """
        if self._task is not None:
            return self.is_online()
        self._async = True
        self._boot()
        wifi_up = True
        start = getattr(self.wifi, "start", None)
        if start is not None:
            wifi_up = await start(wait=wait)
        self._task = C.create_task(self._run_async(self._epoch))
        await C.sleep_ms(0)  # let the task take its first step (state reflects reality on return)
        if wait and wifi_up:
            while self._running and not self._first_done:
                await C.sleep_ms(50)
        return self.is_online()

    async def wait_online(self, timeout=None):
        end = ticks_add(C.ticks_ms(), int(timeout * 1000)) if timeout else None
        while self._sta != ONLINE:
            if end is not None and ticks_diff(end, C.ticks_ms()) <= 0:
                return False
            await C.sleep_ms(100)
        return True

    async def wait_delivered(self, mid=None, timeout=None):
        """Wait until message `mid` (default: every queued message) is delivered.

        False on timeout or when the message was dropped (queue full, peer removed, no peer).
        """
        end = ticks_add(C.ticks_ms(), int(timeout * 1000)) if timeout else None
        while self.is_pending(mid) if mid is not None else self._outbox:
            if end is not None and ticks_diff(end, C.ticks_ms()) <= 0:
                return False
            await C.sleep_ms(50)
        return mid is None or mid not in self._dropped

    async def _run_async(self, epoch):
        while self._running and epoch == self._epoch:
            ms = self._step()
            end = ticks_add(C.ticks_ms(), ms)
            await C.sleep_ms(0)
            while self._running and not self._kick:
                rem = ticks_diff(end, C.ticks_ms())
                if rem <= 0:
                    break
                await C.sleep_ms(min(rem, 50))
            self._kick = False

    # =====================================================================================
    # driver: blocking / polling
    # =====================================================================================
    def connect(self, timeout=None):
        """Blocking: bring up WiFi (wm.connect(), if any) and ESP-NOW, wait until a peer answers.

        Returns True once online (at once for a node without peers), False after `timeout` seconds.
        """
        if self._async:
            return self.is_online()
        self._boot()
        end = ticks_add(C.ticks_ms(), int(timeout * 1000)) if timeout else None
        wconnect = getattr(self.wifi, "connect", None)
        if wconnect is not None and not self.wifi.is_connected():
            left = None if end is None else max(1, ticks_diff(end, C.ticks_ms())) / 1000
            if not wconnect(left):
                return False
        while self._running:
            self._wifi_check()
            ms = self._step()
            if self._sta == ONLINE or (self._sta == OFFLINE and not self._plist and not self._discovering()):
                self._wake = ticks_add(C.ticks_ms(), ms)
                return True
            if end is not None and ticks_diff(end, C.ticks_ms()) <= 0:
                self._wake = ticks_add(C.ticks_ms(), ms)
                return False
            self._sleep_sync(ms)
        return False

    def check(self):
        """One supervision step for your own loop (runs wm.check() too). Returns `en.state`.

        Cheap when nothing is due: call it often (every loop iteration).
        """
        if self._async:
            return self._sta
        self._boot()
        self._wifi_check()
        if self._kick or ticks_diff(C.ticks_ms(), self._wake) >= 0:
            self._kick = False
            ms = self._step()
            self._wake = ticks_add(C.ticks_ms(), ms)
        return self._sta

    def flush(self, timeout=None):
        """Blocking: run until every queued message is delivered. False after `timeout` seconds.

        (asyncio code uses `await en.wait_delivered()` instead.) Handy before machine.deepsleep().
        """
        end = ticks_add(C.ticks_ms(), int(timeout * 1000)) if timeout else None
        while self._outbox and not self._async:
            self.check()
            if not self._outbox:
                break
            if end is not None and ticks_diff(end, C.ticks_ms()) <= 0:
                return False
            C.sleep_ms_blocking(max(1, min(ticks_diff(self._wake, C.ticks_ms()), 100)))
        return not self._outbox

    def _sleep_sync(self, ms):
        end = ticks_add(C.ticks_ms(), ms)
        while not self._kick:
            rem = ticks_diff(end, C.ticks_ms())
            if rem <= 0:
                break
            C.sleep_ms_blocking(min(rem, 100))
            self._wifi_check()
        self._kick = False

    def _wifi_check(self):
        fn = getattr(self.wifi, "check", None)
        if fn is not None:
            fn()

    # =====================================================================================
    # helpers
    # =====================================================================================
    def _boot(self):
        if self._booted:
            return
        self._booted = True
        self._running = True
        w = self.wlan
        w.active(True)
        if self.wifi is None and not w.isconnected():
            try:
                w.disconnect()  # a STA still trying to join an AP hops channels: ESP-NOW needs it quiet
            except Exception:
                pass
        esp = self.esp
        try:
            esp.active(False)  # rxbuf is only applied when ESP-NOW is (re)initialised
            esp.config(rxbuf=self.rxbuf or max(esp.config("rxbuf"), 2048))
        except Exception as e:
            self.log.warning("cannot set rxbuf: %s", C.exc_text(e))
        esp.active(True)
        if self._pmk:
            esp.set_pmk(self._pmk)
        try:
            esp.add_peer(BROADCAST_MAC)
        except OSError as e:
            if "EXIST" not in _err_name(e):
                raise
        for p in list(self._plist):
            self._register(p)
        esp.irq(self._irq)
        try:
            mac = mac_text(w.config("mac"))
        except Exception:
            mac = "?"
        self.log.info("ESPNowManager %s started (mac %s, group %s, %d peer(s))",
                      __version__, mac, self.group, len(self._plist))

    def _irq(self, _esp):
        self._kick = True  # a frame arrived: run the supervisor now

    def _fire(self, cb, *args):
        if cb is None:
            return
        try:
            r = cb(*args)
            if C.is_coroutine(r):
                if self._async:
                    C.create_task(r)
                else:
                    C.run_until_complete(r)
        except Exception as e:
            self.log.error("callback error: %s", C.exc_text(e))

    def _set_sta(self, new):
        old = self._sta
        if new != old:
            self._sta = new
            self.log.debug("state %s -> %s", old, new)
            self._fire(self._cb_state, old, new)

    def _stats(self):
        try:
            return self.esp.stats() if self._booted else None
        except Exception:
            return None

    def _info(self, p):
        rssi = None
        table = getattr(self.esp, "peers_table", None)
        if table:
            e = table.get(p.mac)
            if e:
                rssi = e[0]
        seen = None if p.last_ok is None else ticks_diff(C.ticks_ms(), p.last_ok) // 1000
        return {"name": p.name, "mac": p.addr, "online": p.online, "rssi": rssi, "last_seen": seen,
                "queued": len([m for m in self._outbox if m[1] is p]), "saved": p.saved}

    def _find(self, mac):
        for p in self._plist:
            if p.mac == mac:
                return p
        return None

    def _lookup(self, peer):
        """A peer by name, MAC text or 6-byte MAC (None if unknown)."""
        if isinstance(peer, _Peer):
            return peer if peer in self._plist else None
        if isinstance(peer, str):
            for p in self._plist:
                if p.name == peer:
                    return p
        try:
            return self._find(mac_bytes(peer))
        except ValueError:
            return None

    def _add(self, mac, name, lmk, save, fixed=False):
        if len(self._plist) >= espnow.MAX_TOTAL_PEER_NUM - 1:  # the broadcast address uses one slot
            self.log.warning("peer table full (%d): %s not added", len(self._plist), mac_text(mac))
            return None
        p = _Peer(mac, name, lmk, saved=save, fixed_name=fixed)
        if self._booted and not self._register(p):
            return None
        self._plist.append(p)
        self._save_peer(p)
        self.log.info("peer %s added (%s)", p.label, p.addr)
        return p

    def _register(self, p):
        """Put `p` in the ESP-NOW peer table (channel 0: it follows the radio's channel)."""
        esp = self.esp
        for attempt in (0, 1):
            try:
                if p.lmk:
                    esp.add_peer(p.mac, p.lmk)
                else:
                    esp.add_peer(p.mac)
                return True
            except OSError as e:
                if attempt == 0 and "EXIST" in _err_name(e):
                    try:
                        esp.del_peer(p.mac)  # registered by somebody else: register it our way
                    except Exception:
                        pass
                    continue
                self.log.error("cannot register peer %s: %s", p.label, C.exc_text(e))
                return False
        return False

    def _save_peer(self, p):
        if self._store is not None and p.saved:
            self._store.upsert(p.addr, p.name, "".join("%02x" % b for b in p.lmk) if p.lmk else None)

    def _discovering(self):
        if self._pair_until is not None:
            if ticks_diff(self._pair_until, C.ticks_ms()) > 0:
                return True
            self._pair_until = None
            self.log.info("pairing closed")
        return bool(self.discovery)

    def _frame(self, kind, body=b"", flags=0):
        self._seq = (self._seq + 1) & 0xFFFF
        return encode(kind, flags, self._gid, self._seq, body)

    def _publish(self, topic, payload, targets, flags):
        frame = self._frame(DATA, data_body(topic, payload_bytes(payload)), flags)
        if len(frame) > self.max_len:
            raise ValueError("message too long: %d bytes (max_len %d)" % (len(frame), self.max_len))
        self._mid += 1
        mid = self._mid
        if not targets:
            self._forget(mid)
            self.log.warning("no peer to send message %d ('%s') to: dropped", mid, topic)
            return mid
        for p in targets:
            if len(self._outbox) >= self.queue_size:
                old = self._outbox.pop(0)
                self._forget(old[0])
                self.log.warning("queue full (%d): dropped message %d to %s",
                                 self.queue_size, old[0], old[1].label if old[1] else "broadcast")
            self._outbox.append([mid, p, frame])
        self._poke()
        return mid

    def _poke(self):
        """Send what is waiting right away when called from application code."""
        paused = self._pause is not None and ticks_diff(self._pause, C.ticks_ms()) > 0
        if self._running and self._radio_ok and self._scan is None and not self._in_step and not paused:
            self._in_step = True
            try:
                self._flush(C.ticks_ms())
                self._update_state()
            finally:
                self._in_step = False
        else:
            self._kick = True

    def _forget(self, mid):
        if mid not in self._dropped:
            self._n_dropped += 1
            self._dropped.append(mid)
            if len(self._dropped) > _DROPPED_KEEP:
                self._dropped.pop(0)

    def _backoff_ms(self, n):
        secs = min(self.backoff_max, self.backoff_base * (2 ** min(n - 1, 16)))
        return int(secs * 1000 * (80 + random.getrandbits(6) % 41) / 100)  # +-20 % jitter

    # ---- peer presence ---------------------------------------------------------------------
    def _alive(self, p):
        """`p` answered (ACK) or sent us something."""
        p.last_ok = C.ticks_ms()
        p.fails = 0
        p.next_try = None
        if p.online is not True:
            p.online = True
            self.log.info("peer %s reachable", p.label)
            self._fire(self._cb_peer, p.label, True)

    def _failed(self, p):
        """A unicast frame to `p` got no ACK: retry soon, then declare it unreachable and back off."""
        p.fails += 1
        now = C.ticks_ms()
        if p.fails < self.peer_retries:
            p.next_try = ticks_add(now, _RETRY_MS + random.getrandbits(7))
            return
        p.next_try = ticks_add(now, self._backoff_ms(p.fails - self.peer_retries + 1))
        self._unreachable(p, "no ACK")

    def _unreachable(self, p, why):
        if p.online is not False:
            was = p.online
            p.online = False
            self.log.warning("peer %s unreachable (%s)", p.label, why)
            if was:
                self._fire(self._cb_peer, p.label, False)

    # ---- radio -----------------------------------------------------------------------------
    def _home(self):
        if self.channel != "auto":
            return self.channel
        if self._store is not None and self._store.channel:
            return self._store.channel
        return 1

    def _tune(self, ch, src):
        try:
            self.wlan.config(channel=ch)
        except Exception as e:
            self._src = src  # logged once: _radio() does not retry it at every step
            self._last_error = "cannot set channel %d: %s" % (ch, C.exc_text(e))
            self.log.error("%s", self._last_error)
            return False
        if ch != self._channel:
            self.log.debug("channel %d (%s)", ch, src)
        self._channel = ch
        self._src = src
        return True

    def _radio(self):
        """Keep the radio on the right channel. False while it belongs to a WiFi that is not connected."""
        w = self.wlan
        wifi = self.wifi
        up = wifi.is_connected() if wifi is not None else w.isconnected()
        if up:
            if not self._wifi_up:
                self._wifi_up = True
                try:
                    w.config(pm=w.PM_NONE)  # with power saving the radio is deaf between beacons
                except Exception as e:
                    self.log.warning("cannot disable WiFi power saving: %s", C.exc_text(e))
            ch = w.config("channel")
            if ch != self._channel or self._src != "wifi":
                if self.channel != "auto" and ch != self.channel and not self._warned_fixed:
                    self._warned_fixed = True
                    self.log.warning("channel=%d ignored: the WiFi is on channel %d", self.channel, ch)
                moved = self._channel is not None and ch != self._channel
                self._channel = ch
                self._src = "wifi"
                self._scan = None
                self.log.info("channel %d (WiFi)", ch)
                if moved:
                    for p in self._plist:  # whoever stayed on the old channel is gone: find out now
                        p.probe = True
                        p.next_try = None
            return True
        self._wifi_up = False
        if wifi is not None:
            return False
        if self._src is None or self._src == "wifi":
            self._tune(self._home(), "fixed" if self.channel != "auto" else "auto")
        return True

    # =====================================================================================
    # the supervisor: one step, returns how many ms to wait before the next one
    # =====================================================================================
    def _step(self):
        if not self._running:
            return 1000
        if self._in_step:  # check() called from a handler or callback: the outer step carries on
            return self.poll_ms
        self._in_step = True
        try:
            self._radio_ok = self._radio()
            self._receive()
            self._dispatch()
            if not self._running:  # a handler called stop()
                return 1000
            now = C.ticks_ms()
            if not self._radio_ok:
                if self._sta != NO_WIFI:
                    self.log.info("waiting for WiFi")
                    self._set_sta(NO_WIFI)
                return self.poll_ms * 4  # frames that reach us meanwhile are still received
            if self._scan is not None:
                return self._scan_step(now)
            if self._pause is None or ticks_diff(now, self._pause) >= 0:
                self._pause = None
                self._hello(now)
                self._heartbeat(now)
                self._flush(now)
            self._watch_rx(now)
            if self._scan_due(now):
                self._start_scan()
                return 0
            self._update_state()
            return self.poll_ms
        except Exception as e:
            self.log.error("supervisor error: %s", C.exc_text(e))
            return 2000
        finally:
            self._in_step = False

    def _update_state(self):
        if self._scan is not None:
            new = SCANNING
        else:
            new = ONLINE if any(p.online for p in self._plist) else OFFLINE
        if not self._first_done and all(p.online is not None for p in self._plist):
            self._first_done = True
        self._set_sta(new)

    def _send_now(self, p, frame):
        """One frame now. True: acknowledged (broadcast: sent), False: no ACK, None: radio error."""
        mac = BROADCAST_MAC if p is None else p.mac
        for attempt in (0, 1):
            try:
                ok = self.esp.send(mac, frame, True)
                break
            except OSError as e:
                if attempt == 0 and p is not None and "NOT_FOUND" in _err_name(e):
                    self._register(p)  # the peer table was changed behind our back
                    continue
                self._last_error = "send failed: %s" % C.exc_text(e)
                self.log.warning("send to %s failed: %s", p.label if p else "broadcast", C.exc_text(e))
                self._pause = ticks_add(C.ticks_ms(), 1000)
                return None
        if p is not None:
            if ok:
                self._alive(p)
            else:
                self._failed(p)
        return ok

    def _hello(self, now):
        if not self._discovering():
            return
        if self._hello_at is not None and ticks_diff(self._hello_at, now) > 0:
            return
        pairing = self._pair_until is not None
        self._hello_at = ticks_add(now, _PAIR_HELLO_MS if pairing else self.hello_interval * 1000)
        self._send_now(None, self._frame(HELLO, self._name_b))

    def _queued_for(self, p):
        for m in self._outbox:
            if m[1] is p:
                return True
        return False

    def _heartbeat(self, now):
        hb = self.heartbeat * 1000
        for p in list(self._plist):
            if self._pause is not None:
                return
            if p.next_try is not None and ticks_diff(p.next_try, now) > 0:
                continue
            if p.online:
                if not p.probe and (not hb or ticks_diff(now, p.last_ok) < hb):
                    continue
            elif self._queued_for(p):
                continue  # the queued message itself is the probe
            elif not hb and not p.probe and p.online is False:
                continue  # heartbeat=0: unreachable peers are retried only when there is something to send
            p.probe = False
            self._send_now(p, self._frame(PING))

    def _flush(self, now):
        """Send the queued messages: in order per peer, an unreachable peer does not block the others."""
        i = 0
        while i < len(self._outbox):
            m = self._outbox[i]
            p = m[1]
            if p is not None and p.next_try is not None and ticks_diff(p.next_try, now) > 0:
                i += 1
                continue
            ok = self._send_now(p, m[2])
            if ok is None:
                return  # radio error: try again after the pause
            if not ok and p is not None:
                i += 1  # p is in its retry pause now: its later messages wait too
                continue
            self._outbox.pop(i)
            if not self.is_pending(m[0]):
                self.log.debug("message %d delivered", m[0])
                if m[0] not in self._dropped:
                    self._fire(self._cb_delivered, m[0])

    def _watch_rx(self, now):
        if self._stats_at is not None and ticks_diff(self._stats_at, now) > 0:
            return
        self._stats_at = ticks_add(now, _STATS_MS)
        st = self._stats()
        if st and st[4] > self._rx_dropped:
            self.log.warning("%d incoming frame(s) lost: receive buffer full (raise rxbuf or poll_ms lower)",
                             st[4] - self._rx_dropped)
            self._rx_dropped = st[4]

    # ---- channel search --------------------------------------------------------------------
    def _scan_due(self, now):
        if self.channel != "auto" or self._src == "wifi" or len(self.scan_channels) < 2:
            return False
        if self._scan_next is not None and ticks_diff(self._scan_next, now) > 0:
            return False
        if self._plist:
            return all(p.online is False for p in self._plist)
        return self._discovering()

    def _start_scan(self):
        chans = list(self.scan_channels)
        cur = self._channel
        if cur in chans:
            i = chans.index(cur)
            chans = chans[i + 1:] + chans[:i]
        self._scan = [chans, cur, None]
        self.log.info("looking for peers on the other channels")
        self._set_sta(SCANNING)

    def _scan_step(self, now):
        s = self._scan
        if any(p.online for p in self._plist):
            return self._scan_done(True)
        if s[2] is not None and ticks_diff(s[2], now) > 0:
            return min(ticks_diff(s[2], now), self.poll_ms)  # listening for answers on this channel
        if not s[0]:
            return self._scan_done(False)
        ch = s[0].pop(0)
        if not self._tune(ch, "auto"):
            return self.poll_ms
        ping = self._frame(PING)
        for p in self._plist:
            try:
                if not p.online and self.esp.send(p.mac, ping, True):
                    self._alive(p)
                    return self._scan_done(True)
            except OSError as e:
                self.log.debug("ping to %s on channel %d: %s", p.label, ch, C.exc_text(e))
        if self._discovering():
            self._send_now(None, self._frame(HELLO, self._name_b))
        s[2] = ticks_add(C.ticks_ms(), self.scan_dwell_ms)
        return self.poll_ms

    def _scan_done(self, ok):
        s = self._scan
        self._scan = None
        self._first_done = True
        if ok:
            self._scan_fails = 0
            self._scan_next = None
            self.log.info("peers found on channel %d", self._channel)
            if self._store is not None:
                self._store.set_channel(self._channel)
            for p in self._plist:
                if not p.online:
                    p.probe = True
                    p.next_try = None
        else:
            self._scan_fails += 1
            self._tune(s[1] or self._home(), "auto")
            delay = self._backoff_ms(self._scan_fails)
            self._scan_next = ticks_add(C.ticks_ms(), delay)
            self._last_error = "no peer answered on any channel"
            self.log.warning("%s (next search in %ds)", self._last_error, delay // 1000)
        self._update_state()
        return 0

    # ---- incoming frames -------------------------------------------------------------------
    def _receive(self):
        esp = self.esp
        for _ in range(_MAX_BURST):
            if not esp.any():
                break
            try:
                mac, msg = esp.recv(0)
            except ValueError as e:  # the firmware resets its buffer after this
                self.log.error("receive error: %s", C.exc_text(e))
                break
            if mac is None:
                break
            try:
                self._on_frame(mac, msg)
            except Exception as e:
                self.log.warning("bad frame from %s: %s", mac_text(mac), C.exc_text(e))

    def _on_frame(self, mac, msg):
        f = decode(msg)
        if f is None:
            if self._cb_raw is not None:
                self._inbox.append((None, mac, msg, 0))
            else:
                self.log.debug("ignored a frame from %s that is not ours", mac_text(mac))
            return
        kind, flags, gid, seq, body = f
        if gid != self._gid:
            self.log.debug("ignored a frame from %s of another group", mac_text(mac))
            return
        p = self._find(mac)
        if kind == HELLO:
            self._on_hello(mac, p, flags, body)
            return
        if p is None:
            if not self.accept_unknown:
                self.log.debug("dropped a frame from unknown %s", mac_text(mac))
                return
            p = self._add(mac, None, None, False)
            if p is None:
                return
        if kind == BYE:
            p.fails = self.peer_retries
            p.next_try = ticks_add(C.ticks_ms(), self._backoff_ms(1))
            self._unreachable(p, "it said bye")
            return
        self._alive(p)
        if kind == DATA:
            now = C.ticks_ms()
            p.seen = [e for e in p.seen if ticks_diff(now, e[1]) < _SEEN_MS]
            for e in p.seen:
                if e[0] == seq:
                    self.log.debug("duplicate message from %s dropped", p.label)
                    return
            p.seen.append((seq, now))
            if len(p.seen) > _SEEN_KEEP:
                p.seen.pop(0)
            topic, payload = split_data(body)
            self._inbox.append((p, topic, payload, flags))

    def _on_hello(self, mac, p, flags, body):
        try:
            name = bytes(body[:32]).decode() or None
        except Exception:
            name = None
        new = p is None
        if new:
            if not self._discovering():
                self.log.debug("HELLO from %s ignored (not pairing)", mac_text(mac))
                return
            # not encrypted yet: the other side cannot decrypt anything from us before it knows us
            p = self._add(mac, name, None, True)
            if p is None:
                return
            self.log.info("paired with %s", p.label)
        elif name and name != p.name and not p.fixed_name:
            p.name = name
            self._save_peer(p)
        p.seen = []  # it (re)started: its message ids start from scratch
        self._alive(p)
        if not flags & F_ANSWER:
            self._send_now(p, self._frame(HELLO, self._name_b, F_ANSWER))
        if new and self._lmk:
            p.lmk = self._lmk
            self._register(p)
            self._save_peer(p)

    def _dispatch(self):
        while self._inbox:
            p, topic, payload, flags = self._inbox.pop(0)
            if p is None:  # not one of our frames: (None, mac, msg, 0)
                self._fire(self._cb_raw, topic, payload)
                continue
            handled = False
            for s in list(self._subs):  # handlers may (un)subscribe
                if s[_H] is None or not topic_matches(s[_F], topic):
                    continue
                if s[_R] and flags & F_REPLY:
                    continue  # a reply is never answered by a replying handler: no loops
                handled = True
                self._call(s, topic, payload, p)
            if not handled:
                self._fire(self._cb_message, topic, payload, p.label)

    def _call(self, s, topic, payload, p):
        try:
            r = s[_H](topic, payload, p.label)
            if C.is_coroutine(r):
                if self._async:
                    C.create_task(self._await_handler(r, s, topic, p))
                    return
                r = C.run_until_complete(r)
        except Exception as e:
            r = self._handler_error(topic, e)
        self._reply(s, topic, r, p)

    async def _await_handler(self, coro, s, topic, p):
        try:
            r = await coro
        except Exception as e:
            r = self._handler_error(topic, e)
        self._reply(s, topic, r, p)

    def _handler_error(self, topic, e):
        self.log.error("handler for '%s' failed: %s", topic, C.exc_text(e))
        return {"error": C.exc_text(e)}

    def _reply(self, s, topic, result, p):
        to = s[_R]
        if not to or result is None:
            return
        if p not in self._plist:
            self.log.warning("reply to %s dropped: no longer a peer", p.label)
            return
        if to is True:
            to = topic + self.reply_suffix
        try:
            self._publish(to, result, [p], F_REPLY)
        except ValueError as e:
            self.log.error("reply on '%s' not sent: %s", to, e)
