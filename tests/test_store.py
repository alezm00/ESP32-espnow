import _setup  # noqa: F401
import json
import unittest

from _util import exists, rmtree, tmpdir
from wifimanager.log import Logger
from espnowmanager.store import PeerStore


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.dir = tmpdir()
        self.path = self.dir + "/peers.json"
        self.log = Logger("DEBUG", 50, console=False)

    def tearDown(self):
        rmtree(self.dir)

    def store(self):
        s = PeerStore(self.path, self.log)
        s.load()
        return s

    def test_save_and_load(self):
        s = self.store()
        self.assertEqual((s.channel, s.peers()), (None, []))
        s.upsert("24:0a:c4:00:00:01", "gateway")
        s.upsert("24:0a:c4:00:00:02", None, "30313233343536373839616263646566")
        s.set_channel(6)
        with open(self.path) as f:
            data = json.load(f)
        self.assertEqual(data["version"], 1)
        s2 = self.store()
        self.assertEqual(s2.channel, 6)
        self.assertEqual(s2.peers(), [{"mac": "24:0a:c4:00:00:01", "name": "gateway", "lmk": None},
                                      {"mac": "24:0a:c4:00:00:02", "name": None,
                                       "lmk": "30313233343536373839616263646566"}])
        self.assertTrue(s2.remove("24:0a:c4:00:00:01"))
        self.assertFalse(s2.remove("24:0a:c4:00:00:01"))
        self.assertEqual([p["mac"] for p in self.store().peers()], ["24:0a:c4:00:00:02"])

    def test_writes_only_on_change(self):
        s = self.store()
        writes = []
        save = s.save
        s.save = lambda: (writes.append(1), save())
        s.upsert("24:0a:c4:00:00:01", "a")
        s.upsert("24:0a:c4:00:00:01", "a")
        s.set_channel(3)
        s.set_channel(3)
        s.upsert("24:0a:c4:00:00:01", "b")
        self.assertEqual(len(writes), 3)

    def test_tmp_is_recovered_and_corrupt_file_is_moved_away(self):
        with open(self.path + ".tmp", "w") as f:
            f.write('{"version": 1, "channel": 11, "peers": [{"mac": "24:0a:c4:00:00:09"}]}')
        s = self.store()
        self.assertEqual((s.channel, [p["mac"] for p in s.peers()]), (11, ["24:0a:c4:00:00:09"]))
        self.assertIn("recovering", "\n".join(self.log.tail()))
        with open(self.path, "w") as f:
            f.write("{not json")
        s = self.store()
        self.assertEqual((s.channel, s.peers()), (None, []))
        self.assertTrue(exists(self.path + ".bad"))
        self.assertIn("corrupt", "\n".join(self.log.tail()))

    def test_unwritable_file_never_raises(self):
        s = PeerStore(self.dir + "/no/such/dir/peers.json", self.log)
        s.load()
        s.upsert("24:0a:c4:00:00:01", "a")
        self.assertIn("cannot write", "\n".join(self.log.tail()))


if __name__ == "__main__":
    unittest.main()
