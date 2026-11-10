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

from aiohttp import web
from websockets.asyncio.server import serve

from .bridge import Bridge
from .store import Store
from .web import RingLogHandler, make_app

log = logging.getLogger("hjm")


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
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)

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
        await stop.wait()
        log.info("shutting down")
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
