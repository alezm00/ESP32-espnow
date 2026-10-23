# espnowmanager — plug-and-play ESP-NOW for MicroPython on ESP32

The ESP-NOW companion of [ESP32-wifi](https://github.com/alezm00/ESP32-wifi) (`wifimanager`) and
[ESP32-mqtt](https://github.com/alezm00/ESP32-mqtt) (`mqttmanager`). It uses the `espnow` module of the official firmware for the
radio and `wifimanager` for its clock/asyncio layer, its logger and (optionally) the WiFi. It gives your project:

* **Topics like MQTT**: `publish("sensors/temp", 21.5, to="hub")`, `@en.on("cmd/+")` handlers with `+`/`#` wildcards
* **Commands → replies**: what a handler returns is sent back to the node that asked (`<topic>/reply` or a topic you choose)
* **Sending** with an **offline queue**: messages for a peer that is away wait in RAM and go out, in order, when it is back
* **Delivery confirmation**: every `publish()` returns an id; a message is "delivered" when the peer's radio acknowledged it
  (`on_delivered`, `wait_delivered()`, `flush()`); duplicates caused by a lost ACK are dropped by the receiver
* **Presence**: peers are pinged when idle, marked online/offline (`on_peer`) and retried with exponential backoff (+ jitter)
* **Channel handling**: follows the WiFi's channel when the board is also on WiFi; otherwise it **searches the channels** to find
  its peers, and again when they vanish (e.g. the router moved to another channel)
* **Pairing**: nodes with the same group name find each other (`en.pair(30)`), like a WPS button
* **Peers and channel saved** in `/espnow_peers.json`, so a node that wakes from deep sleep talks at once
* **Logs** to the console and into the WiFi manager's log (so they show up in its config page too)

Works with `asyncio` (background supervision) **and** with plain blocking scripts, exactly like `wifimanager` and `mqttmanager`.

---

## 1. Install

With `mip`, which installs `espnowmanager` **and its dependency `wifimanager`** into `/lib`:

```bash
pip install mpremote
mpremote mip install github:alezm00/ESP32-espnow
```

Or by hand. Clone with the submodule, then copy both folders:

```bash
git clone --recurse-submodules https://github.com/alezm00/ESP32-espnow
cd ESP32-espnow
mpremote fs mkdir :lib
mpremote cp -r deps/ESP32-wifi/wifimanager :lib/
mpremote cp -r espnowmanager :lib/
```

`espnow` is part of the official ESP32 firmware, so there is nothing else to install.

Requirements: **MicroPython v1.29.0** (the latest release; older firmware is not supported) on ESP32, S2, S3 or C3, with
`wifimanager` 2.0. Flash use ≈ 54 kB of `.py`, ≈ 19 kB compiled with `mpy-cross`. A node that does not use WiFi still needs
`wifimanager`: only its `compat` and `log` modules do real work then, but importing the package also loads `WiFiManager`'s code,
so compile both libraries with `mpy-cross` (or freeze them) on boards short of RAM.

## 2. Quick start

Every ESP-NOW node needs to know who it talks to. Either write the other board's MAC address in the code (it is printed in the
log at start: `started (mac 24:0a:c4:12:ab:cd, ...)`), or let the nodes pair by themselves.

```python
# hub.py — connected to your router; nodes of the group "my-house" pair with it
import asyncio
from wifimanager import WiFiManager
from espnowmanager import ESPNowManager

wm = WiFiManager(networks=[("MyHome", "my-password")])
en = ESPNowManager(wifi=wm, name="hub", group="my-house", discovery=True)

@en.on("sensors/#")
def sensor(topic, payload, peer):        # peer = the sender's name, or its MAC
    print(peer, topic, payload)

async def main():
    await en.start()                     # starts the WiFi manager too; both are supervised in the background
    while True:
        await asyncio.sleep(60)

asyncio.run(main())
```

```python
# node.py — blocking style, no WiFi
from espnowmanager import ESPNowManager

en = ESPNowManager(name="kitchen", group="my-house")
if not en.peers():
    en.pair(30)                          # first boot: search the hub on every channel, then remember it
en.connect()                             # blocks until a peer answers

while True:
    en.check()                           # reads frames, pings, detects loss, searches the channel, resends
    ...                                  # your code: en.publish("sensors/kitchen", 21.5)
```

With a known address there is no pairing at all: `ESPNowManager([("hub", "24:0a:c4:12:ab:cd")])`.
See `examples/` for complete scripts (asyncio hub, deep-sleep sensor, commands with replies, blocking loop).

## 3. Sending and delivery confirmation

```python
mid = en.publish("sensors/kitchen", {"t": 21.5, "h": 40}, to="hub")
```

* `to` is a peer's **name** or **MAC** (`"24:0a:c4:12:ab:cd"` or 6 bytes). `to=None` (default) sends one copy to **every known
  peer**; `to=BROADCAST` (`"*"`) sends **one broadcast frame** to every node in range (no ACK, so "delivered" once transmitted).
* The **payload** can be `str` (UTF-8), `bytes`, `None` (empty), or anything `json.dumps()` accepts (dict, list, numbers,
  booleans), which is sent as JSON. The receiver always gets `bytes`.
* **Size**: a frame is at most `max_len` bytes (default 250, which every ESP-NOW device can receive). Our header takes 7 bytes
  plus the topic, so `payload ≤ 243 − len(topic)`. A longer message raises `ValueError` (there is no fragmentation). With every
  node on v1.29 you may raise `max_len` up to `espnow.MAX_DATA_LEN` (1470 on firmware built with ESP-IDF 5.4 or later).
* `publish()` **never fails because a peer is away**. When the peer is reachable it is sent right away. Otherwise the message joins a
  RAM queue (`queue_size`, default 20) that is sent, in order per peer, as soon as the peer answers again. A peer that is away
  never holds back the messages for the others. When the queue is full the **oldest** message is dropped (a warning in the log,
  `status()["dropped"]` counts them).
* "Delivered" means the peer's **radio acknowledged** the frame (ESP-NOW's MAC-layer ACK). When the ACK is lost the message is
  sent again: the receiver recognises the repeat (by its message id) and hands it to your code only once. A message re-sent after
  a long outage can still arrive twice (*at least once*).

| How to know it arrived | |
|---|---|
| `on_delivered(mid)` | callback, once per message (with `to=None`: when every copy was acknowledged) |
| `await en.wait_delivered(mid, timeout=None)` | asyncio. `True` once delivered, `False` on timeout or if it was dropped. Without `mid`: waits for the whole queue |
| `en.flush(timeout=None)` | blocking: runs the radio until the queue is empty (`False` after `timeout` s). Use it before `machine.deepsleep()` |
| `en.is_pending(mid)`, `en.pending()` | still queued? how many are queued (one per recipient)? |

## 4. Receiving, commands and replies

```python
en.subscribe("sensors/+/temp", handler)               # handler(topic: str, payload: bytes, peer: str)
en.subscribe("alerts/#")                              # no handler: goes to on_message(topic, payload, peer)

@en.on("cmd/led", reply=True)
def cmd_led(topic, payload, peer):
    led.value(payload == b"on")
    return {"led": led.value()}                       # sent back to `peer` on cmd/led/reply
```

* There is no broker: the sender chooses who gets a message (`to=`), the receiver picks it up with its subscriptions. `peer` is the
  sender's name (yours, or the one it announced when pairing) or its MAC text.
* Handlers can be plain functions or `async def`. A handler that raises an exception is logged and never stops the loop. Several
  matching subscriptions each get the message. Messages no handler wants go to `on_message`.
* `reply` controls what happens to the handler's return value:

| `reply=` | the return value is sent to the sender on |
|---|---|
| `None` / `False` (default) | nowhere (ignored) |
| `True` | the incoming topic + `reply_suffix` (default `"/reply"`) |
| `"some/topic"` | that fixed topic |

  A handler returning `None` sends no reply. A handler that raises replies `{"error": "ValueError: ..."}`, so the sender is not left
  waiting. Replies are marked as such and never answered by a replying handler (no loops between two nodes with `#` filters).
* Only frames from **known peers** are accepted; `accept_unknown=True` also takes frames from any node of your group (it becomes a
  peer for this session). Frames of other groups are ignored. Frames that are not ours (any other ESP-NOW program) go to
  `on_raw(mac, msg)` if you set it, so the library can live next to plain `espnow` devices.

## 5. Peers, presence and pairing

* Peers come from the constructor (`peers=[...]`: `"aa:bb:cc:dd:ee:ff"`, `(name, mac)`, `(name, mac, lmk)` or a dict), from
  `en.add_peer(mac, name=None, lmk=None, save=True)`, or from pairing. Added and paired peers are **saved** in `peers_file`;
  the constructor's ones are there at every boot anyway. `en.remove_peer(peer)` forgets one and drops its queued messages.
* **Presence.** A peer that sent nothing and acknowledged nothing for `heartbeat` s (default 30) gets a tiny `PING`. Every frame it
  sends and every ACK it gives proves it is there. `peer_retries` (3) unanswered frames, about 0.3 s apart, make it **unreachable**
  (`on_peer(peer, False)`); then it is pinged again after 2 → 4 → 8 … ≤ `backoff_max` s (±20 %), and the first answer brings it
  back (`on_peer(peer, True)`) and flushes its queue. `heartbeat=0` stops the pings (an unreachable peer is retried only when
  there is something to send). `stop()` tells the reachable peers with a `BYE`, so they mark it offline at once.
* **Pairing.** Nodes with the same `group` find each other while they are pairing: `en.pair(seconds)` opens a window (HELLO
  broadcasts every 2 s), `discovery=True` keeps it open for good (HELLO every `hello_interval` s), typically on the hub. A node that
  hears a HELLO adds the sender, answers, and both save each other. With `lmk=` both sides switch the new peer to ESP-NOW
  encryption with that key right after the handshake.
* `en.state` is `online` when at least one peer is reachable, `offline` when none is, `scanning` while the channel is searched,
  `no_wifi` while the WiFi it depends on is down, `idle` before `start()`/`connect()`/`check()` or after `stop()`.

**Security.** ESP-NOW frames are not authenticated by default: any device in range can send you frames claiming any MAC address,
and the group name travels in clear text. Accepting only known peers keeps casual traffic out, not an attacker. For commands that
matter, use ESP-NOW encryption: a `pmk=` (16 bytes, the same on every node) and an `lmk` per peer (16 bytes; `lmk=` for paired
peers). Broadcast frames (HELLO, `to=BROADCAST`) are never encrypted, and the firmware allows only `espnow.MAX_ENCRYPT_PEER_NUM`
encrypted peers. Keep `discovery=True` only while you add nodes: after pairing, peers are saved and pairing is no longer needed.
Keys are stored in plain text in `peers_file`, like the WiFi passwords in `wifi_config.json`.

## 6. The channel — how it works

ESP-NOW only reaches nodes listening on the **same WiFi channel**, and a board connected to a router must use the router's channel.
This is the most common reason why "ESP-NOW does not work"; `espnowmanager` handles it like this:

1. **With WiFi** (`wifi=` a `WiFiManager`, or WiFi you connected yourself): the channel is the router's. WiFi power saving is switched
   off (`pm=PM_NONE`: with it the radio is deaf between beacons and loses ESP-NOW frames). While the given WiFi manager is not
   connected the state is `no_wifi`: the WiFi is scanning and hopping channels, so messages are only queued (frames that do reach
   the board are still received). When the router moves to another channel, the board follows it and checks its peers at once.
2. **`channel=1..14`**: the radio stays on that channel. Use it for the hub when it has no WiFi.
3. **`channel="auto"`** (default) without WiFi: the saved channel (or 1). When **every** peer is unreachable, or while pairing with no
   peer yet, it **searches**: one channel at a time from `scan_channels` (1–13), it pings the peers (their radio's ACK proves they are
   there), sends a HELLO if pairing, and listens `scan_dwell_ms` for answers. Found → it stays there and saves the channel; not found
   → back to the previous channel and a new search after 2 → 4 → 8 … ≤ `backoff_max` s. A search takes about 2 s.

Two nodes that both search never meet reliably: give the hub a fixed channel or WiFi, and let the battery nodes use `"auto"`.

**Things that block for a moment** (the `espnow` module is blocking): every unicast `send()` waits for the ACK (about a millisecond
when the peer is there, a few ms when it is not), and the firmware retries for up to 2 s when its own buffers are full. In asyncio mode
other tasks wait during that time.

## 7. Constructor reference

`ESPNowManager(peers=None, *, ...)` — everything after `peers` is keyword-only.

**Peers / identity**

| Option | Default | Meaning |
|---|---|---|
| `peers` | `None` | list of `"aa:bb:cc:dd:ee:ff"`, 6-byte MACs, `(name, mac[, lmk])` or `{"mac":..,"name":..,"lmk":..}` |
| `name` | `None` | this node's name, announced when pairing (1–32 bytes) |
| `group` | `None` | group name: only nodes of the same group are talked to and paired with |
| `peers_file` | `"/espnow_peers.json"` | where learned peers and the found channel are saved (`None` = nothing saved) |
| `accept_unknown` | `False` | accept messages from senders that are not peers |
| `pmk` / `lmk` | `None` | ESP-NOW encryption: primary key, and the key given to peers learned by pairing (16 bytes each) |

**Radio / channel**

| Option | Default | Meaning |
|---|---|---|
| `wifi` | `None` | the `WiFiManager` whose channel to follow (it is started/checked with this manager). `None`: ESP-NOW only |
| `channel` | `"auto"` | `"auto"` (saved one, searched when peers vanish) or a fixed channel 1–14. With WiFi connected the router's channel wins |
| `scan_channels` / `scan_dwell_ms` | `range(1, 14)` / `150` | channels searched, and how long to listen on each |
| `max_len` | `250` | largest frame sent (up to `espnow.MAX_DATA_LEN`) |
| `rxbuf` | `None` | receive buffer in bytes (`None`: at least 2048, the firmware default is 2 full frames) |

**Messages / supervision**

| Option | Default | Meaning |
|---|---|---|
| `queue_size` | `20` | messages kept in RAM for peers that are away (the oldest are dropped) |
| `reply_suffix` | `"/reply"` | added to the topic for `reply=True` |
| `heartbeat` | `30` | seconds of silence before a peer is pinged (`0` = never) |
| `peer_retries` | `3` | unanswered frames before a peer is unreachable |
| `discovery` / `hello_interval` | `False` / `30` | pair with nodes of the group at any time, HELLO broadcast period (s) |
| `poll_ms` | `50` | how often incoming frames are read (a frame also wakes the supervisor at once) |
| `backoff_base` / `backoff_max` | `2` / `60` | wait between retries to an unreachable peer, and between channel searches: 2, 4, 8 … s, capped, ±20 % jitter |

**Logging**

| Option | Default | Meaning |
|---|---|---|
| `log_level` | `"INFO"` | `"DEBUG"`, `"INFO"`, `"WARNING"`, `"ERROR"` |
| `log_file` / `log_max_size` | `None` / `8192` | e.g. `"/espnow.log"`, rotating like the WiFi log |
| `log_buffer` | `60` | lines kept in RAM (`en.log.tail()`) |
| `share_log` | `True` | use the WiFi manager's RAM buffer: ESP-NOW lines appear in its config page |

`en.log.console = False` silences `print()`. Keys are never logged.

**Callbacks** (plain functions or `async def`): `on_message(topic, payload, peer)`, `on_delivered(mid)`, `on_peer(peer, online)`,
`on_state(old, new)`, `on_raw(mac, msg)`.

## 8. API

| Call | Description |
|---|---|
| `await en.start(wait=True)` | asyncio: start supervision (and the WiFi manager's); with `wait` returns after every peer was tried once (`True` if one answered) |
| `await en.wait_online(timeout=None)` | asyncio: wait until a peer is reachable |
| `await en.wait_delivered(mid=None, timeout=None)` | asyncio: wait for one message, or the whole queue |
| `en.connect(timeout=None)` | blocking: WiFi (if any) + ESP-NOW. `True` once a peer answers (at once for a node without peers that is not pairing), `False` after `timeout` s |
| `en.check()` | blocking style: one supervision step (also `wm.check()`), returns the state. Call it every loop |
| `en.flush(timeout=None)` | blocking: until the queue is empty |
| `en.publish(topic, payload, to=None)` | send or queue; returns the message id |
| `en.subscribe(filter, handler=None, reply=None)` / `@en.on(filter, reply=None)` | handle incoming topics |
| `en.unsubscribe(filter)`, `en.subscriptions()` | remove one, list them |
| `en.add_peer(mac, name=None, lmk=None, save=True)`, `en.remove_peer(peer)` | manage peers (by name or MAC) |
| `en.pair(seconds=60)` | open pairing for a while |
| `en.peers()`, `en.peer(peer)` | `{name, mac, online, rssi, last_seen, queued, saved}` |
| `en.stop(bye=True)` | stop supervising, tell the peers, switch ESP-NOW off |
| `en.state` | `"idle"`, `"no_wifi"`, `"scanning"`, `"online"`, `"offline"` |
| `en.is_online()`, `en.pending()`, `en.is_pending(mid)`, `en.status()` | live information (`status()`: channel and where it comes from, MAC, counters, `espnow` stats) |
| `en.esp`, `en.wlan`, `en.wifi`, `en.log` | the `ESPNow` object, the WLAN interface, the WiFi manager, the logger |
| `topic_matches(filter, topic)`, `BROADCAST` | the MQTT wildcard matcher used internally; `to=` value for a broadcast |

## 9. Design notes

* **Same shape as `wifimanager` and `mqttmanager`:** one class, keyword-only configuration, one supervisor step function returning
  "wait N ms" and three drivers (`start()`, `connect()`, `check()`), so blocking and asyncio users get identical behaviour.
* **`wifimanager` is a real dependency, not a copy:** `espnowmanager` uses its compatibility layer (`compat`), its logger, and, when
  given, its `is_connected()` / `start()` / `connect()` / `check()` and its WLAN.
* **A small frame format on top of ESP-NOW** (`frame.py`): a 6-byte header (magic, type, group id, message id), then the topic and
  the payload. The message id lets the receiver drop the copies a lost ACK causes; the group id keeps neighbouring projects apart.
* **The radio's ACK is the answer.** ESP-NOW acknowledges every unicast frame in hardware, so a ping needs no reply frame, a
  delivered message needs no application-level ACK, and a channel search just pings the peers on each channel.
* Handlers run **outside** the receive loop (frames are collected first, then dispatched), so a handler can publish, subscribe,
  remove peers or even call `stop()` safely. The receive callback (`esp.irq`) only wakes the supervisor.
* Every `recv()` uses a 0 ms timeout: the firmware's default is 5 minutes, which would freeze the board.

## 10. Troubleshooting

| Symptom | Hint |
|---|---|
| peers never answer, state stays `offline` | same channel? (`en.status()["channel"]` on both); a node on WiFi uses the router's channel: give the other side `channel="auto"` or the same number |
| works until the hub joins the WiFi | the hub moved to the router's channel: nodes with `"auto"` follow; nodes with a fixed channel need that channel |
| state stays `no_wifi` | the WiFi side is not connected: see the `wifi:` lines in the log / the WiFi config page |
| `dropped a frame from unknown ...` (DEBUG) | that node is not a peer: add it, pair, or set `accept_unknown=True` |
| `ignored a frame ... of another group` | the two nodes have different `group` names |
| pairing never finishes | the hub must be pairing too (`discovery=True` or `pair()`), with the same group, and on a fixed channel or WiFi |
| `peer table full` | ESP-NOW allows 20 peers (the broadcast address takes one), fewer when encrypted |
| `incoming frame(s) lost: receive buffer full` | raise `rxbuf`, lower `poll_ms`, or avoid long blocking code in your loop |
| `message too long` | shorten topic/payload (250-byte frames by default) or raise `max_len` if every node supports it |
| `queue full: dropped message ...` | a peer was away for long: raise `queue_size` (RAM!) or publish less often |
| works on the bench, not with WiFi on | WiFi power saving loses frames: it is switched off whenever the manager sees the STA connected; if your code sets `pm` later, set it back to `PM_NONE` |

Use `log_level="DEBUG"` to see every state change, channel change and delivered message.

## 11. Development and tests (no board needed)

The tests run on the **MicroPython v1.29.0 unix port**, the same interpreter as on the board (CPython is not supported).
Build it and install the test runner once, as described in [ESP32-wifi §9](https://github.com/alezm00/ESP32-wifi#9-development-and-tests-no-board-needed)
(`micropython -m mip install unittest-discover`), then:

```bash
git submodule update --init                     # deps/ESP32-wifi: the dependency, its fake radio and test helpers
micropython -m unittest discover -s tests       # supervisors on a virtual clock + fake air; asyncio on a real loop
```

`tests/fakes/espnow.py` is a fake `espnow` module (the v1.29 API, errors included) on a simulated "air": several nodes, each with its
own WLAN and channel, with knobs for range, sleeping nodes, lost ACKs, full receive buffers, `ESP_ERR_ESPNOW_NO_MEM` and keys.
Several managers (and WiFi managers, on the fake router of ESP32-wifi) talk to each other in one process.

### On-device checklist (needs two or three boards)

1. Copy both libraries (see §1); `examples/main_async.py` as `/main.py` on the hub (set your WiFi), `examples/main_sensor.py` on a node.
2. Boot the node: it searches the channel, pairs with the hub, and the hub prints its reading every minute.
3. Switch the hub off: the node's `flush()` gives up after 5 s and it sleeps again (that reading is lost); switch it on: the next readings arrive.
4. Change the router's channel: the hub follows it, the node finds the hub again on the next wake-up.
5. `examples/main_commands.py` on a third board: send it `cmd/led` from another node and get the `cmd/led/reply`.

## 12. License

MIT — use it freely in your projects.
