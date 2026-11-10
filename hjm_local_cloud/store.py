"""Persistent configuration and device registry (a single JSON file in the data dir)."""
import copy
import json
import os
import time

DEFAULTS = {
    "mqtt": {
        "host": "",
        "port": 1883,
        "username": "",
        "password": "",
        "base_topic": "hjm",
        "discovery_prefix": "homeassistant",
    },
    # When true, radiators that connect are published to MQTT immediately.
    # When false they show up as "pending" in the web UI until approved.
    "auto_approve": False,
    "devices": {},
}


def _merge(defaults, value):
    out = copy.deepcopy(defaults)
    for k, v in (value or {}).items():
        if isinstance(out.get(k), dict) and isinstance(v, dict) and k != "devices":
            out[k] = _merge(out[k], v)
        else:
            out[k] = v
    return out


class Store:
    def __init__(self, path):
        self.path = path
        try:
            with open(path) as f:
                raw = json.load(f)
        except (OSError, ValueError):
            raw = {}
        self.data = _merge(DEFAULTS, raw)

    def save(self):
        tmp = self.path + ".tmp"
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)  # holds MQTT password
        with os.fdopen(fd, "w") as f:
            json.dump(self.data, f, indent=2, sort_keys=True)
        os.replace(tmp, self.path)

    @property
    def mqtt(self):
        return self.data["mqtt"]

    def update_mqtt(self, values):
        m = self.data["mqtt"]
        for key in ("host", "username", "base_topic", "discovery_prefix"):
            if key in values:
                m[key] = str(values[key]).strip()
        if "port" in values:
            m["port"] = int(values["port"])
        # An absent/None password means "keep the current one"; "" clears it.
        if values.get("password") is not None:
            m["password"] = str(values["password"])
        self.save()

    @property
    def auto_approve(self):
        return bool(self.data.get("auto_approve"))

    def set_auto_approve(self, value):
        self.data["auto_approve"] = bool(value)
        self.save()

    @property
    def devices(self):
        return self.data["devices"]

    def device(self, devid, create=True):
        dev = self.devices.get(devid)
        if dev is None and create:
            now = int(time.time())
            dev = self.devices[devid] = {
                "approved": self.auto_approve,
                "name": "",
                "node_names": {},
                "first_seen": now,
                "last_seen": now,
                "last_ip": "",
                "fw": None,
            }
            self.save()
        return dev

    def remove_device(self, devid):
        self.devices.pop(devid, None)
        self.save()
