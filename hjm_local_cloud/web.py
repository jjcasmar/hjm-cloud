"""Web UI: device registry, MQTT settings, onboarding help and basic control."""
import base64
import hmac
import logging
import os
from collections import deque

from aiohttp import web

log = logging.getLogger("hjm.web")
STATIC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")


class RingLogHandler(logging.Handler):
    def __init__(self, size=300):
        super().__init__()
        self.lines = deque(maxlen=size)
        self.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s",
                                            "%Y-%m-%d %H:%M:%S"))

    def emit(self, record):
        try:
            self.lines.append(self.format(record))
        except Exception:
            pass


def make_app(bridge, logs: RingLogHandler, *, password=None, device_port=8443):
    store = bridge.store

    @web.middleware
    async def guard(request, handler):
        if password:
            ok = False
            auth = request.headers.get("Authorization", "")
            if auth.lower().startswith("basic "):
                try:
                    _, pw = base64.b64decode(auth[6:]).decode().split(":", 1)
                    ok = hmac.compare_digest(pw, password)
                except Exception:
                    pass
            if not ok:
                return web.Response(status=401, text="authentication required",
                                    headers={"WWW-Authenticate": 'Basic realm="hjm-local-cloud"'})
        # Mutations must be JSON: blocks cross-site form posts (no CORS preflight allowed).
        if request.method == "POST" and request.content_type != "application/json":
            return web.json_response({"error": "expected application/json"}, status=415)
        return await handler(request)

    async def body(request):
        try:
            data = await request.json()
        except Exception:
            raise web.HTTPBadRequest(text="invalid JSON")
        if not isinstance(data, dict):
            raise web.HTTPBadRequest(text="expected a JSON object")
        return data

    def known_device(request):
        devid = request.match_info["devid"]
        if store.device(devid, create=False) is None:
            raise web.HTTPNotFound(text="unknown device")
        return devid

    async def index(_):
        return web.FileResponse(os.path.join(STATIC, "index.html"))

    async def state(_):
        m = store.mqtt
        return web.json_response({
            "mqtt": {
                "host": m["host"], "port": m["port"], "username": m["username"],
                "has_password": bool(m["password"]), "base_topic": m["base_topic"],
                "discovery_prefix": m["discovery_prefix"], "status": bridge.mqtt_status,
            },
            "auto_approve": store.auto_approve,
            "device_port": device_port,
            "devices": bridge.snapshot(),
        })

    async def save_mqtt(request):
        data = await body(request)
        try:
            port = int(data.get("port", store.mqtt["port"]))
            if not 0 < port < 65536:
                raise ValueError
        except (TypeError, ValueError):
            return web.json_response({"error": "invalid port"}, status=400)
        data["port"] = port
        store.update_mqtt(data)
        log.info("MQTT settings updated (host=%s user=%s)", store.mqtt["host"],
                 store.mqtt["username"] or "-")
        bridge.restart_mqtt()
        return web.json_response({"ok": True})

    async def save_settings(request):
        data = await body(request)
        if "auto_approve" in data:
            store.set_auto_approve(data["auto_approve"])
        return web.json_response({"ok": True})

    async def approve(request):
        devid = known_device(request)
        data = await body(request)
        await bridge.approve(devid, data.get("name"))
        return web.json_response({"ok": True})

    async def rename(request):
        devid = known_device(request)
        data = await body(request)
        dev = store.device(devid)
        dev["name"] = str(data.get("name", "")).strip()
        store.save()
        await bridge._publish_device(devid)
        return web.json_response({"ok": True})

    async def remove(request):
        devid = known_device(request)
        await bridge.remove(devid)
        return web.json_response({"ok": True})

    async def rename_node(request):
        devid = known_device(request)
        data = await body(request)
        await bridge.rename_node(devid, request.match_info["node"], str(data.get("name", "")))
        return web.json_response({"ok": True})

    async def command(request):
        devid = known_device(request)
        node = request.match_info["node"]
        data = await body(request)
        if "mode" in data:
            ok, msg = await bridge.set_mode(devid, node, data["mode"])
        elif "temperature" in data:
            ok, msg = await bridge.set_temperature(devid, node, data["temperature"])
        else:
            return web.json_response({"error": "expected mode or temperature"}, status=400)
        return web.json_response({"ok": ok, "message": msg}, status=200 if ok else 502)

    async def get_logs(_):
        return web.json_response({"lines": list(logs.lines)})

    app = web.Application(middlewares=[guard])
    app.add_routes([
        web.get("/", index),
        web.get("/api/state", state),
        web.get("/api/logs", get_logs),
        web.post("/api/mqtt", save_mqtt),
        web.post("/api/settings", save_settings),
        web.post("/api/devices/{devid}/approve", approve),
        web.post("/api/devices/{devid}/rename", rename),
        web.post("/api/devices/{devid}/remove", remove),
        web.post("/api/devices/{devid}/nodes/{node}/rename", rename_node),
        web.post("/api/devices/{devid}/nodes/{node}/command", command),
    ])
    return app
