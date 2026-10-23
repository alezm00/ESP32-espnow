"""Plain-JSON storage of the peers learned at run time and of the channel they were found on.

File format (human readable, edit it with `mpremote edit` if you like)::

    {
     "version": 1,
     "channel": 6,
     "peers": [
      {"mac": "24:0a:c4:12:ab:cd", "name": "gateway", "lmk": null}
     ]
    }

Peers given to ESPNowManager(peers=[...]) are not stored: they are there at every boot anyway.
`lmk` is the peer's ESP-NOW key in hex (null: not encrypted).
"""
import json
import os

VERSION = 1


def _exists(path):
    try:
        os.stat(path)
        return True
    except OSError:
        return False


class PeerStore:
    def __init__(self, path, log):
        self.path = path
        self.log = log
        self.channel = None
        self._peers = []

    def load(self):
        path = self.path
        if not _exists(path):
            if _exists(path + ".tmp"):  # power was lost between "remove" and "rename"
                self.log.warning("peers: recovering from %s.tmp", path)
                path = path + ".tmp"
            else:
                return
        try:
            with open(path) as f:
                data = json.load(f)
            peers = []
            for p in data.get("peers", []):
                mac = p.get("mac")
                if isinstance(mac, str) and mac:
                    name = p.get("name")
                    lmk = p.get("lmk")
                    peers.append({"mac": mac, "name": name if isinstance(name, str) and name else None,
                                  "lmk": lmk if isinstance(lmk, str) and lmk else None})
            ch = data.get("channel")
            self.channel = ch if isinstance(ch, int) and 1 <= ch <= 14 else None
            self._peers = peers
        except Exception as e:
            self.log.error("peers: %s is corrupt (%s), starting empty", path, e)
            try:
                if _exists(self.path + ".bad"):
                    os.remove(self.path + ".bad")
                os.rename(path, self.path + ".bad")
            except OSError:
                pass
            self.channel = None
            self._peers = []

    def save(self):
        lines = [json.dumps(p) for p in self._peers]
        text = '{\n "version": %d,\n "channel": %s,\n "peers": [\n' % (VERSION, json.dumps(self.channel))
        text += ",\n".join("  " + l for l in lines)
        text += "\n ]\n}\n"
        tmp = self.path + ".tmp"
        try:
            with open(tmp, "w") as f:
                f.write(text)
            try:
                os.remove(self.path)
            except OSError:
                pass
            os.rename(tmp, self.path)
        except Exception as e:
            self.log.error("peers: cannot write %s: %s", self.path, e)  # never let the flash stop the radio

    def peers(self):
        return [dict(p) for p in self._peers]

    def set_channel(self, channel):
        if channel != self.channel:
            self.channel = channel
            self.save()

    def upsert(self, mac, name=None, lmk=None):
        """Add or update the peer `mac` (text form). Writes the file only when something changed."""
        entry = {"mac": mac, "name": name or None, "lmk": lmk or None}
        for i, p in enumerate(self._peers):
            if p["mac"] == mac:
                if p == entry:
                    return
                self._peers[i] = entry
                break
        else:
            self._peers.append(entry)
        self.save()

    def remove(self, mac):
        for i, p in enumerate(self._peers):
            if p["mac"] == mac:
                del self._peers[i]
                self.save()
                return True
        return False
