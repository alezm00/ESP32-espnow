"""The frames espnowmanager sends over ESP-NOW, MAC address helpers and the MQTT-style topic matcher.

Every frame starts with a 6-byte header::

    0     0xE7 (magic: frames without it are not ours)
    1     type << 4 | flags
    2-3   group id, big endian: 16-bit hash of the group name (0 = no group)
    4-5   message id, big endian: per sender, wraps at 65535 (duplicates are dropped by it)

    DATA   topic length (1 byte), topic (UTF-8), payload (the rest)
    PING   nothing: the MAC-layer ACK of the unicast frame is the answer
    HELLO  the sender's name (UTF-8, may be empty)
    BYE    nothing: the sender is going away
"""
import json
from binascii import hexlify, unhexlify

MAGIC = 0xE7
DATA = 1
PING = 2
HELLO = 3
BYE = 4

F_REPLY = 1   # DATA: a reply, never answered by a replying handler (no loops)
F_ANSWER = 1  # HELLO: the answer to a HELLO, not answered again

HEADER = 6
BROADCAST_MAC = b"\xff" * 6


def group_id(name):
    """16-bit FNV-1a hash of the group name; 0 when there is no group."""
    if not name:
        return 0
    h = 0x811C9DC5
    for b in name.encode():
        h = ((h ^ b) * 0x01000193) & 0xFFFFFFFF
    return ((h >> 16) ^ h) & 0xFFFF or 1


def encode(kind, flags, group, mid, body=b""):
    return bytes((MAGIC, kind << 4 | flags, group >> 8, group & 0xFF, mid >> 8, mid & 0xFF)) + body


def decode(msg):
    """(kind, flags, group, mid, body), or None when `msg` is not one of our frames."""
    if len(msg) < HEADER or msg[0] != MAGIC:
        return None
    return msg[1] >> 4, msg[1] & 0x0F, msg[2] << 8 | msg[3], msg[4] << 8 | msg[5], msg[HEADER:]


def data_body(topic, payload):
    t = topic.encode()
    if len(t) > 255:
        raise ValueError("topic longer than 255 bytes")
    return bytes((len(t),)) + t + payload


def split_data(body):
    """DATA body -> (topic, payload). ValueError when it is truncated or the topic is not UTF-8."""
    if not body or len(body) < 1 + body[0]:
        raise ValueError("truncated DATA frame")
    n = body[0]
    return bytes(body[1:1 + n]).decode(), bytes(body[1 + n:])


def topic_matches(flt, topic):
    """True when `topic` matches the subscription filter `flt` ('+' = one level, '#' = the rest)."""
    if flt == topic:
        return True
    if topic[:1] == "$" and flt[:1] in ("+", "#"):
        return False  # $SYS/... is not matched by wildcards at the first level
    f = flt.split("/")
    t = topic.split("/")
    for i, part in enumerate(f):
        if part == "#":
            return True
        if i >= len(t) or (part != "+" and part != t[i]):
            return False
    return len(f) == len(t)


def payload_bytes(v):
    """str -> UTF-8, bytes as they are, None -> empty, anything else (dict, list, numbers) -> JSON."""
    if v is None:
        return b""
    if isinstance(v, (bytes, bytearray)):
        return bytes(v)
    if isinstance(v, str):
        return v.encode()
    return json.dumps(v).encode()


def mac_bytes(v):
    """6 bytes, or "aa:bb:cc:dd:ee:ff" / "aa-bb-..." / "aabbccddeeff" -> 6 bytes. ValueError otherwise."""
    if isinstance(v, (bytes, bytearray)):
        if len(v) == 6:
            return bytes(v)
    elif isinstance(v, str):
        h = v.replace(":", "").replace("-", "")
        if len(h) == 12:
            try:
                return unhexlify(h)
            except ValueError:
                pass
    raise ValueError("invalid MAC address: %r" % (v,))


def mac_text(mac):
    h = hexlify(mac).decode()
    return ":".join(h[i:i + 2] for i in range(0, 12, 2))


def key_bytes(v):
    """An ESP-NOW key (PMK / LMK): 16 bytes, or a 16-character string."""
    if isinstance(v, str):
        v = v.encode()
    if not isinstance(v, (bytes, bytearray)) or len(v) != 16:
        raise ValueError("ESP-NOW keys must be exactly 16 bytes")
    return bytes(v)
