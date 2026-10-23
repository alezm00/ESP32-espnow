import _setup  # noqa: F401
import unittest

from espnowmanager import topic_matches
from espnowmanager import frame as F


class TopicTests(unittest.TestCase):
    def test_matches(self):
        yes = [("a/b", "a/b"), ("a/+", "a/b"), ("+/b", "a/b"), ("a/#", "a/b/c"), ("a/#", "a"), ("#", "a/b"),
               ("+/+", "a/b"), ("a/+/c", "a/x/c"), ("+", "a"), ("a//c", "a//c"), ("a/+/c", "a//c")]
        no = [("a/b", "a/c"), ("a/+", "a/b/c"), ("a/+", "a"), ("+/b", "a/c"), ("a/b", "a"), ("a", "a/b"),
              ("#", "$SYS/x"), ("+/x", "$SYS/x"), ("a/+/c", "a/x/d")]
        for flt, topic in yes:
            self.assertTrue(topic_matches(flt, topic), (flt, topic))
        for flt, topic in no:
            self.assertFalse(topic_matches(flt, topic), (flt, topic))


class FrameTests(unittest.TestCase):
    def test_round_trip(self):
        g = F.group_id("home")
        msg = F.encode(F.DATA, F.F_REPLY, g, 0xBEEF, F.data_body("cmd/led", b"on"))
        self.assertEqual(msg[:6], bytes((0xE7, 0x11, g >> 8, g & 0xFF, 0xBE, 0xEF)))
        kind, flags, gid, mid, body = F.decode(msg)
        self.assertEqual((kind, flags, gid, mid), (F.DATA, F.F_REPLY, g, 0xBEEF))
        self.assertEqual(F.split_data(body), ("cmd/led", b"on"))
        self.assertEqual(len(msg), 6 + 1 + 7 + 2)

    def test_foreign_and_broken_frames(self):
        self.assertIsNone(F.decode(b"hello world"))
        self.assertIsNone(F.decode(b"\xe7\x10"))
        self.assertRaises(ValueError, F.split_data, b"\x09abc")
        self.assertRaises(ValueError, F.split_data, b"")
        self.assertRaises(ValueError, F.data_body, "x" * 256, b"")

    def test_group_id(self):
        self.assertEqual(F.group_id(None), 0)
        self.assertEqual(F.group_id(""), 0)
        self.assertEqual(F.group_id("home"), F.group_id("home"))
        self.assertNotEqual(F.group_id("home"), F.group_id("office"))
        self.assertTrue(0 < F.group_id("home") <= 0xFFFF)

    def test_mac_and_keys(self):
        m = b"\x24\x0a\xc4\x12\xab\xcd"
        for v in (m, bytearray(m), "24:0a:c4:12:ab:cd", "24-0A-C4-12-AB-CD", "240ac412abcd"):
            self.assertEqual(F.mac_bytes(v), m)
        self.assertEqual(F.mac_text(m), "24:0a:c4:12:ab:cd")
        for bad in ("gateway", "24:0a:c4:12:ab", "zz:0a:c4:12:ab:cd", b"\x00" * 5, None, 42):
            self.assertRaises(ValueError, F.mac_bytes, bad)
        self.assertEqual(F.key_bytes("0123456789abcdef"), b"0123456789abcdef")
        self.assertRaises(ValueError, F.key_bytes, "short")

    def test_payload_bytes(self):
        self.assertEqual([F.payload_bytes(v) for v in ("x", b"\x00", bytearray(b"ba"), None, 21.5, {"v": 1}, [1, 2])],
                         [b"x", b"\x00", b"ba", b"", b"21.5", b'{"v": 1}', b"[1, 2]"])


if __name__ == "__main__":
    unittest.main()
