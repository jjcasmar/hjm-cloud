"""Helki device protocol (local cloud) and MQTT / Home Assistant bridge.

The radiator (ESP32) opens wss://<helki-host>/api/v2/ws_dev, authenticates with HTTP Basic
"<devid>:<secret>" and does NOT validate the server certificate. All messages are JSON text
frames shaped like REST calls:

  device -> server request : {"path": "htr/2/status", "method": "POST", "body": {...}}
  server -> device response: {"method": ..., "path": ..., "code": 201, "body": {}, "devid": ...}
  server -> device command : {"body": {"stemp": "22.0", "units": "C"}, "path": "/htr/2/status",
                              "method": "POST", "devid": ...}
  device -> server ack     : {"path": "/htr/2/status", "method": "POST", "code": 201}

After applying a command, and whenever a reading changes, the device pushes a full
htr/<addr>/status.

MQTT layout (base topic default "hjm"):
  hjm/bridge/status                         online/offline (retained, LWT)
  hjm/<devid>/availability                  online/offline (radiator connected)
  hjm/<devid>/<type><addr>/state            status JSON + derived fields (retained)
  hjm/<devid>/<type><addr>/setup|prog       raw JSON from the device (retained)
  hjm/<devid>/<type><addr>/set/mode         off | heat | auto
  hjm/<devid>/<type><addr>/set/temperature  e.g. 21.5
"""
import asyncio
import base64
import json
import logging
import time
from dataclasses import dataclass, field

import aiomqtt
from websockets.exceptions import ConnectionClosed

log = logging.getLogger("hjm")

WS_PATH = "/api/v2/ws_dev"

# rtc/tz answer captured from the real cloud for Europe/Madrid (Helki conventions: offsets in
# seconds with inverted sign, 0-based months, week 5 = last week of the month).
DEFAULT_TZ = {
    "tz_code": "Europe/Madrid",
    "gmt_diff": -3600,
    "dst_offset": -3600,
    "dst_start": {"month": 2, "week": 5, "week_day": 0, "hour": 2, "minute": 0},
    "dst_end": {"month": 9, "week": 5, "week_day": 0, "hour": 3, "minute": 0},
}

# Reported as "latest firmware" before we learn the device's own version. Afterwards we always
# echo the device's version back, so it never attempts an update.
DEFAULT_FW = {"major": 1, "minor": 8, "revision": 0}

HA_TO_DEV_MODE = {"off": "off", "heat": "manual", "auto": "auto"}
DEV_TO_HA_MODE = {"off": "off", "manual": "heat", "auto": "auto"}
ENTITY_KEYS = [
    ("climate", "climate"), ("sensor", "power"), ("sensor", "duty"), ("sensor", "temperature"),
    ("binary_sensor", "window_open"), ("sensor", "error_code"), ("sensor", "comfort_temp"),
    ("sensor", "eco_temp"),
]


@dataclass
class Node:
    devid: str
    type: str
    addr: int
    status: dict = field(default_factory=dict)
    setup: dict = field(default_factory=dict)
    prog: dict = field(default_factory=dict)
    version: dict = field(default_factory=dict)
    updated: float = 0.0

    @property
    def key(self):
        return f"{self.type}{self.addr}"

    @property
    def uid(self):
        return f"{self.devid}_{self.key}"


class Connection:
    def __init__(self, devid, ws, ip):
        self.devid = devid
        self.ws = ws
        self.ip = ip
        self.connected_at = time.time()
        self.pending: dict[str, list[asyncio.Future]] = {}

    async def send(self, obj):
        raw = json.dumps(obj, separators=(",", ":"))
        log.debug("-> %s %s", self.devid, raw)
        await self.ws.send(raw)


class Bridge:
    def __init__(self, store, users_online_interval=60, tz=None):
        self.store = store
        self.users_online_interval = users_online_interval
        self.tz = tz or DEFAULT_TZ
        self.nodes: dict[tuple[str, str], Node] = {}
        self.conns: dict[str, Connection] = {}
        self.mqtt: aiomqtt.Client | None = None
        self.mqtt_status = "not configured"
        self._mqtt_task: asyncio.Task | None = None

    # ------------------------------------------------------------------ helpers
    @property
    def base(self):
        return self.store.mqtt["base_topic"] or "hjm"

    @property
    def disc(self):
        return self.store.mqtt["discovery_prefix"] or "homeassistant"

    def node_topic(self, node):
        return f"{self.base}/{node.devid}/{node.key}"

    def node_name(self, node):
        dev = self.store.device(node.devid, create=False) or {}
        name = (dev.get("node_names") or {}).get(node.key) or dev.get("name")
        return name or f"HJM {node.key} {node.devid[-4:]}"

    def approved(self, devid):
        dev = self.store.device(devid, create=False)
        return bool(dev and dev.get("approved"))

    def _node(self, devid, ntype, addr):
        k = (devid, f"{ntype}{addr}")
        if k not in self.nodes:
            self.nodes[k] = Node(devid, ntype, addr)
            dev = self.store.device(devid)
            known = dev.setdefault("nodes", [])
            if k[1] not in known:
                known.append(k[1])
                self.store.save()
            log.info("node %s registered", self.nodes[k].uid)
        return self.nodes[k]

    # ------------------------------------------------------------------ websocket side
    @staticmethod
    def process_request(conn, request):
        if request.path.split("?")[0] != WS_PATH:
            log.info("rejecting request for %s", request.path)
            return conn.respond(404, "not found\n")
        return None

    @staticmethod
    def _devid_from_auth(headers):
        auth = headers.get("Authorization", "")
        if auth.lower().startswith("basic "):
            try:
                return base64.b64decode(auth[6:]).decode().split(":", 1)[0]
            except Exception:
                pass
        return None

    async def handle_ws(self, ws):
        ip = ws.remote_address[0] if ws.remote_address else "?"
        devid = self._devid_from_auth(ws.request.headers)
        if not devid:
            log.warning("connection from %s without device credentials, closing", ip)
            await ws.close(1008, "auth")
            return
        dev = self.store.device(devid)
        dev["last_ip"], dev["last_seen"] = ip, int(time.time())
        self.store.save()
        log.info("device %s connected from %s%s", devid, ip,
                 "" if dev["approved"] else " (pending approval)")
        old = self.conns.get(devid)
        conn = self.conns[devid] = Connection(devid, ws, ip)
        if old:
            await old.ws.close()
        if dev["approved"]:
            await self.publish(f"{self.base}/{devid}/availability", "online", retain=True)
        hint = asyncio.create_task(self._users_online_loop(conn))
        try:
            async for raw in ws:
                if isinstance(raw, bytes):
                    log.warning("binary frame from %s ignored", devid)
                    continue
                log.debug("<- %s %s", devid, raw)
                try:
                    msg = json.loads(raw)
                except ValueError:
                    log.warning("non-JSON frame from %s: %r", devid, raw[:200])
                    continue
                if "code" in msg:
                    self._on_reply(conn, msg)
                else:
                    await self._on_request(conn, msg)
        except ConnectionClosed:
            pass
        finally:
            hint.cancel()
            if self.conns.get(devid) is conn:
                del self.conns[devid]
                if self.approved(devid):
                    await self.publish(f"{self.base}/{devid}/availability", "offline",
                                       retain=True)
            dev = self.store.device(devid, create=False)
            if dev:
                dev["last_seen"] = int(time.time())
                self.store.save()
            log.info("device %s disconnected", devid)

    async def _users_online_loop(self, conn):
        # The real cloud sends this every ~90 s while the app is open; keep it up so the device
        # keeps pushing unsolicited status updates.
        if self.users_online_interval <= 0:
            return
        await asyncio.sleep(10)
        while True:
            try:
                await conn.send({"body": {"online": True}, "path": "/mgr/users_online",
                                 "method": "POST", "devid": conn.devid})
            except ConnectionClosed:
                return
            await asyncio.sleep(self.users_online_interval)

    def _on_reply(self, conn, msg):
        path = msg.get("path", "")
        futs = conn.pending.get(path)
        if futs:
            fut = futs.pop(0)
            if not fut.done():
                fut.set_result(msg)
        elif msg.get("code") not in (200, 201):
            log.info("device %s replied %s to %s: %s", conn.devid, msg.get("code"), path,
                     msg.get("body"))

    async def _on_request(self, conn, msg):
        devid = conn.devid
        path = msg.get("path", "")
        method = msg.get("method", "GET").upper()
        body = msg.get("body") or {}
        p = path.lstrip("/").split("?")[0]
        parts = p.split("/")
        code, resp = (200, {}) if method == "GET" else (201, {})
        dev = self.store.device(devid)

        if method == "GET":
            if p == "rtc/tz":
                resp = {**self.tz, "unixtime": str(int(time.time()))}
            elif p == "firmware/version/last":
                resp = dev.get("fw") or DEFAULT_FW
            elif p == "users_online":
                resp = {"online": True}
            else:
                log.warning("unhandled GET %s from %s", path, devid)
        elif p == "firmware/version":
            fw = {k: body[k] for k in ("major", "minor", "revision") if k in body}
            if fw and dev.get("fw") != fw:
                dev["fw"] = fw
                self.store.save()
        elif p == "mgr/node" and "type" in body and "addr" in body:
            node = self._node(devid, body["type"], int(body["addr"]))
            await self.publish_discovery(node)
        elif len(parts) == 3 and parts[1].isdigit():
            node = self._node(devid, parts[0], int(parts[1]))
            what = parts[2]
            node.updated = time.time()
            if what == "status":
                node.status.update(body)
                await self.publish_state(node)
            elif what in ("setup", "prog", "version"):
                setattr(node, what, body)
                if what == "version":
                    await self.publish_discovery(node)
                elif self.approved(devid):
                    await self.publish(f"{self.node_topic(node)}/{what}", json.dumps(body),
                                       retain=True)
            else:
                log.info("unhandled POST %s from %s: %s", path, devid, body)
        else:
            log.debug("unhandled %s %s from %s: %s", method, path, devid, body)

        await conn.send({"method": method, "path": path, "code": code, "body": resp,
                         "devid": devid})

    async def command(self, devid, node_key, body):
        """Send a partial status update (e.g. {"mode": "manual"}) to a node. Returns ok/err."""
        node = self.nodes.get((devid, node_key))
        conn = self.conns.get(devid)
        if not node or not conn:
            return False, "device offline or node unknown"
        path = f"/{node.type}/{node.addr}/status"
        fut = asyncio.get_running_loop().create_future()
        conn.pending.setdefault(path, []).append(fut)
        log.info("command %s %s", node.uid, body)
        try:
            await conn.send({"body": body, "path": path, "method": "POST", "devid": devid})
            reply = await asyncio.wait_for(fut, 10)
        except (TimeoutError, ConnectionClosed):
            if fut in conn.pending.get(path, []):
                conn.pending[path].remove(fut)
            return False, "no acknowledgement from device"
        if reply.get("code") not in (200, 201):
            return False, f"device replied {reply.get('code')}: {reply.get('body')}"
        return True, "ok"

    async def set_mode(self, devid, node_key, ha_mode):
        mode = HA_TO_DEV_MODE.get(str(ha_mode).lower())
        if not mode:
            return False, f"invalid mode {ha_mode!r}"
        return await self.command(devid, node_key, {"mode": mode})

    async def set_temperature(self, devid, node_key, value):
        try:
            t = float(value)
        except (TypeError, ValueError):
            return False, f"invalid temperature {value!r}"
        return await self.command(devid, node_key, {"stemp": f"{t:.1f}", "units": "C"})

    # ------------------------------------------------------------------ device management
    async def approve(self, devid, name=None):
        dev = self.store.device(devid)
        dev["approved"] = True
        if name is not None:
            dev["name"] = name.strip()
        self.store.save()
        log.info("device %s approved", devid)
        await self._publish_device(devid)

    async def rename_node(self, devid, node_key, name):
        dev = self.store.device(devid)
        dev.setdefault("node_names", {})[node_key] = name.strip()
        self.store.save()
        node = self.nodes.get((devid, node_key))
        if node:
            await self.publish_discovery(node)

    async def remove(self, devid):
        """Unpublish from Home Assistant and forget the device. If it is still connected it
        will reappear as pending."""
        dev = self.store.device(devid, create=False) or {}
        for key in set(dev.get("nodes", [])) | {k for d, k in self.nodes if d == devid}:
            uid = f"{devid}_{key}"
            for component, obj in ENTITY_KEYS:
                await self.publish(f"{self.disc}/{component}/hjm_{uid}_{obj}/config", "",
                                   retain=True)
            for sub in ("state", "setup", "prog"):
                await self.publish(f"{self.base}/{devid}/{key}/{sub}", "", retain=True)
        await self.publish(f"{self.base}/{devid}/availability", "", retain=True)
        for k in [k for k in self.nodes if k[0] == devid]:
            del self.nodes[k]
        self.store.remove_device(devid)
        log.info("device %s removed", devid)
        conn = self.conns.get(devid)
        if conn:
            await conn.ws.close()  # force a clean re-registration

    def snapshot(self):
        """Data for the web UI."""
        out = []
        for devid, dev in sorted(self.store.devices.items()):
            conn = self.conns.get(devid)
            nodes = []
            for (d, key), node in sorted(self.nodes.items()):
                if d != devid:
                    continue
                s = node.status
                nodes.append({
                    "key": key,
                    "name": (dev.get("node_names") or {}).get(key, ""),
                    "display_name": self.node_name(node),
                    "mode": DEV_TO_HA_MODE.get(s.get("mode"), s.get("mode")),
                    "stemp": s.get("stemp"),
                    "mtemp": s.get("mtemp"),
                    "active": s.get("active"),
                    "duty": s.get("act_duty"),
                    "window_open": s.get("window_open"),
                    "error_code": s.get("error_code"),
                    "updated": node.updated or None,
                })
            out.append({
                "devid": devid,
                "name": dev.get("name", ""),
                "approved": dev.get("approved", False),
                "connected": conn is not None,
                "ip": conn.ip if conn else dev.get("last_ip", ""),
                "connected_at": conn.connected_at if conn else None,
                "first_seen": dev.get("first_seen"),
                "last_seen": dev.get("last_seen"),
                "fw": "{major}.{minor}.{revision}".format(**dev["fw"]) if dev.get("fw") else "",
                "nodes": nodes,
            })
        return out

    # ------------------------------------------------------------------ MQTT side
    async def publish(self, topic, payload, retain=False):
        if self.mqtt is None:
            return
        try:
            await self.mqtt.publish(topic, payload, qos=1, retain=retain)
        except aiomqtt.MqttError as e:
            log.warning("publish %s failed: %s", topic, e)

    async def publish_state(self, node):
        if not self.approved(node.devid):
            return
        s = dict(node.status)
        mode = s.get("mode")
        s["ha_mode"] = DEV_TO_HA_MODE.get(mode, "off")
        s["hvac_action"] = "off" if mode == "off" else ("heating" if s.get("active") else "idle")
        try:
            s["power_w"] = round(float(s.get("power", 0)) * float(s.get("act_duty", 0)) / 100, 1)
        except (TypeError, ValueError):
            s["power_w"] = None
        await self.publish(f"{self.node_topic(node)}/state", json.dumps(s), retain=True)

    async def publish_discovery(self, node):
        if not self.approved(node.devid):
            return
        t = self.node_topic(node)
        st = f"{t}/state"
        dev = self.store.device(node.devid, create=False) or {}
        fw = dev.get("fw")
        device = {
            "identifiers": [f"hjm_{node.uid}"],
            "name": self.node_name(node),
            "manufacturer": "HJM (Helki)",
            "model": f"Wi-Fi radiator (pid {node.version.get('pid', '?')})",
            **({"sw_version": "{major}.{minor}.{revision}".format(**fw)} if fw else {}),
        }
        common = {
            "device": device,
            "availability": [{"topic": f"{self.base}/bridge/status"},
                             {"topic": f"{self.base}/{node.devid}/availability"}],
            "availability_mode": "all",
        }
        temp_sensor = {"device_class": "temperature", "unit_of_measurement": "°C"}
        configs = {
            ("climate", "climate"): {
                "name": None,
                "modes": ["off", "heat", "auto"],
                "mode_state_topic": st, "mode_state_template": "{{ value_json.ha_mode }}",
                "mode_command_topic": f"{t}/set/mode",
                "temperature_state_topic": st,
                "temperature_state_template": "{{ value_json.stemp }}",
                "temperature_command_topic": f"{t}/set/temperature",
                "current_temperature_topic": st,
                "current_temperature_template": "{{ value_json.mtemp }}",
                "action_topic": st, "action_template": "{{ value_json.hvac_action }}",
                "min_temp": 5, "max_temp": 30, "temp_step": 0.5, "precision": 0.1,
                "temperature_unit": "C",
            },
            ("sensor", "power"): {
                "name": "Power", "state_topic": st, "value_template": "{{ value_json.power_w }}",
                "device_class": "power", "unit_of_measurement": "W",
                "state_class": "measurement",
            },
            ("sensor", "duty"): {
                "name": "Duty", "state_topic": st, "value_template": "{{ value_json.act_duty }}",
                "unit_of_measurement": "%", "state_class": "measurement", "icon": "mdi:radiator",
            },
            ("sensor", "temperature"): {
                "name": "Temperature", "state_topic": st,
                "value_template": "{{ value_json.mtemp }}", "state_class": "measurement",
                **temp_sensor,
            },
            ("binary_sensor", "window_open"): {
                "name": "Window open", "state_topic": st, "device_class": "window",
                "value_template": "{{ 'ON' if value_json.window_open else 'OFF' }}",
            },
            ("sensor", "error_code"): {
                "name": "Error code", "state_topic": st, "entity_category": "diagnostic",
                "value_template": "{{ value_json.error_code }}",
            },
            ("sensor", "comfort_temp"): {
                "name": "Comfort temperature", "state_topic": st,
                "value_template": "{{ value_json.comf_temp }}", "entity_category": "diagnostic",
                **temp_sensor,
            },
            ("sensor", "eco_temp"): {
                "name": "Eco temperature", "state_topic": st,
                "value_template": "{{ value_json.eco_temp }}", "entity_category": "diagnostic",
                **temp_sensor,
            },
        }
        for (component, obj), cfg in configs.items():
            uid = f"hjm_{node.uid}_{obj}"
            await self.publish(f"{self.disc}/{component}/{uid}/config",
                               json.dumps({**common, **cfg, "unique_id": uid}), retain=True)

    async def _publish_device(self, devid):
        for (d, _), node in list(self.nodes.items()):
            if d == devid:
                await self.publish_discovery(node)
                if node.status:
                    await self.publish_state(node)
        if devid in self.conns and self.approved(devid):
            await self.publish(f"{self.base}/{devid}/availability", "online", retain=True)

    async def _republish_all(self):
        for devid in {d for d, _ in self.nodes} | set(self.conns):
            await self._publish_device(devid)

    async def _on_mqtt(self, msg):
        topic = msg.topic.value
        payload = msg.payload.decode(errors="replace").strip() if msg.payload else ""
        if topic == f"{self.disc}/status":
            if payload == "online":  # Home Assistant restarted
                await self._republish_all()
            return
        parts = topic.split("/")  # base/devid/nodekey/set/what
        if len(parts) != 5 or parts[3] != "set" or not self.approved(parts[1]):
            return
        if parts[4] == "mode":
            ok, err = await self.set_mode(parts[1], parts[2], payload)
        elif parts[4] == "temperature":
            ok, err = await self.set_temperature(parts[1], parts[2], payload)
        else:
            return
        if not ok:
            log.warning("MQTT command %s=%s failed: %s", topic, payload, err)

    def restart_mqtt(self):
        if self._mqtt_task:
            self._mqtt_task.cancel()
        self._mqtt_task = asyncio.create_task(self._mqtt_loop())

    async def _mqtt_loop(self):
        cfg = dict(self.store.mqtt)
        if not cfg["host"]:
            self.mqtt, self.mqtt_status = None, "not configured"
            log.info("MQTT not configured; set it up in the web UI")
            return
        bridge_topic = f"{self.base}/bridge/status"
        will = aiomqtt.Will(bridge_topic, "offline", qos=1, retain=True)
        try:
            while True:
                self.mqtt_status = f"connecting to {cfg['host']}:{cfg['port']}"
                try:
                    async with aiomqtt.Client(
                        cfg["host"], int(cfg["port"]), username=cfg["username"] or None,
                        password=cfg["password"] or None, will=will,
                        identifier="hjm-local-cloud",
                    ) as client:
                        self.mqtt = client
                        self.mqtt_status = "connected"
                        log.info("connected to MQTT %s:%s", cfg["host"], cfg["port"])
                        await client.publish(bridge_topic, "online", qos=1, retain=True)
                        await client.subscribe(f"{self.base}/+/+/set/+", qos=1)
                        await client.subscribe(f"{self.disc}/status", qos=1)
                        await self._republish_all()
                        try:
                            async for msg in client.messages:
                                try:
                                    await self._on_mqtt(msg)
                                except Exception:
                                    log.exception("error handling %s", msg.topic)
                        except asyncio.CancelledError:
                            # Reconfigured or shutting down: a clean disconnect doesn't fire
                            # the LWT, so mark ourselves offline explicitly.
                            try:
                                await asyncio.wait_for(client.publish(
                                    bridge_topic, "offline", qos=1, retain=True), 2)
                            except Exception:
                                pass
                            raise
                except aiomqtt.MqttError as e:
                    self.mqtt = None
                    self.mqtt_status = f"error: {e}"
                    log.warning("MQTT: %s; retrying in 10s", e)
                    await asyncio.sleep(10)
        finally:
            if self._mqtt_task is asyncio.current_task():  # don't clobber a newer connection
                self.mqtt = None
