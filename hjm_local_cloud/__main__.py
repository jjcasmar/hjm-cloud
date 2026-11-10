"""Entry point: python -m hjm_local_cloud

Environment:
  HJM_DATA_DIR               config.json, cert.pem, key.pem   (default ./data)
  HJM_DEVICE_PORT            TLS websocket port for radiators (default 8443)
  HJM_WEB_PORT               web UI port                      (default 8080)
  HJM_WEB_PASSWORD           enables HTTP basic auth on the web UI (any username)
  HJM_LOG_LEVEL              DEBUG logs every frame           (default INFO)
  HJM_USERS_ONLINE_INTERVAL  seconds between "app online" hints, 0 = off (default 60)
  HJM_TZ_JSON                override the rtc/tz answer (see bridge.DEFAULT_TZ)
"""
import asyncio
import json
import logging
import os
import signal
import ssl
import subprocess
from collections import deque

from aiohttp import web
from websockets.asyncio.server import serve

from .bridge import Bridge
from .store import Store
from .web import RingLogHandler, make_app

log = logging.getLogger("hjm")


class TLSFailureFilter(logging.Filter):
    """asyncio only reports failed TLS handshakes on incoming connections in debug mode, at
    DEBUG level with a traceback. Keep just those records and turn them into one-line warnings
    with the peer address; drop all other asyncio debug chatter."""

    def __init__(self):
        super().__init__()
        self._seen = deque(maxlen=64)  # protocol ids already reported (asyncio may report twice)

    def filter(self, record):
        try:
            text = record.getMessage()
        except Exception:
            text = str(record.msg)
        if "Error on transport creation" in text:
            return False  # debug-mode duplicate of the "SSL handshake failed" record
        if "SSL handshake failed" not in text:
            return record.levelno >= logging.WARNING
        proto = record.args[0] if record.args else None
        if proto is not None:
            if id(proto) in self._seen:
                return False
            self._seen.append(id(proto))
        peer, reason = "?", ""
        try:
            transport = getattr(proto, "_transport", None)
            if transport is not None:
                pn = transport.get_extra_info("peername")
                peer = pn[0] if pn else peer
        except Exception:
            pass
        if record.exc_info and record.exc_info[1] is not None:
            reason = f": {record.exc_info[1]!r}"
        record.levelno, record.levelname = logging.WARNING, "WARNING"
        record.name = "hjm.tls"
        record.msg, record.args, record.exc_info, record.exc_text = (
            f"TLS handshake with {peer} failed{reason}", (), None, None)
        return True


def setup_tls_failure_logging(loop):
    loop.set_debug(True)
    loop.slow_callback_duration = 3600  # silence debug-mode slow-callback warnings
    alog = logging.getLogger("asyncio")
    alog.setLevel(logging.DEBUG)
    alog.addFilter(TLSFailureFilter())


def ensure_cert(data_dir):
    cert, key = os.path.join(data_dir, "cert.pem"), os.path.join(data_dir, "key.pem")
    if not (os.path.exists(cert) and os.path.exists(key)):
        log.info("generating self-signed certificate in %s", data_dir)
        subprocess.run(
            ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", key,
             "-out", cert, "-days", "3650", "-subj", "/CN=*.helki.com",
             "-addext", "subjectAltName=DNS:*.helki.com,DNS:helki.com"],
            check=True, capture_output=True)
        os.chmod(key, 0o600)
    return cert, key


async def main():
    level = os.environ.get("HJM_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    ring = RingLogHandler()
    ring.setLevel(logging.INFO)
    logging.getLogger().addHandler(ring)
    # websockets logs failed HTTP/WebSocket upgrades ("opening handshake failed") at ERROR.
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
    setup_tls_failure_logging(asyncio.get_running_loop())

    data_dir = os.environ.get("HJM_DATA_DIR", "data")
    os.makedirs(data_dir, exist_ok=True)
    device_port = int(os.environ.get("HJM_DEVICE_PORT", "8443"))
    web_port = int(os.environ.get("HJM_WEB_PORT", "8080"))
    tz = json.loads(os.environ["HJM_TZ_JSON"]) if os.environ.get("HJM_TZ_JSON") else None

    store = Store(os.path.join(data_dir, "config.json"))
    bridge = Bridge(store, int(os.environ.get("HJM_USERS_ONLINE_INTERVAL", "60")), tz)

    cert, key = ensure_cert(data_dir)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.set_ciphers("ALL:@SECLEVEL=0")  # old ESP32 TLS stacks
    ctx.load_cert_chain(cert, key)

    app = make_app(bridge, ring, password=os.environ.get("HJM_WEB_PASSWORD") or None,
                   device_port=device_port)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", web_port).start()
    log.info("web UI on http://0.0.0.0:%s", web_port)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    async with serve(bridge.handle_ws, "0.0.0.0", device_port, ssl=ctx,
                     process_request=bridge.process_request, ping_interval=60,
                     ping_timeout=30, max_size=2**20):
        log.info("radiator endpoint on wss://0.0.0.0:%s/api/v2/ws_dev", device_port)
        bridge.restart_mqtt()
        energy = asyncio.create_task(bridge.energy_loop())
        await stop.wait()
        log.info("shutting down")
        # Must happen before leaving `serve`, which would otherwise send close frames.
        await bridge.drop_all()
        energy.cancel()
        for node in bridge.nodes.values():
            node.integrate()
        bridge.persist_energy()
        if bridge._mqtt_task:
            bridge._mqtt_task.cancel()
            try:
                await bridge._mqtt_task
            except (asyncio.CancelledError, Exception):
                pass
    await runner.cleanup()


def run():
    asyncio.run(main())


if __name__ == "__main__":
    run()
