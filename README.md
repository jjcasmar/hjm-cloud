# hjm-local-cloud

A local stand-in for the Helki cloud that **HJM Wi-Fi radiators** connect to, bridged to MQTT with
Home Assistant discovery. With it running, the radiators work without internet access and without
the HJM / Smartbox app.

- Speaks the radiators' own cloud protocol, so **the original firmware stays untouched**: nothing to open or flash
- Each radiator appears in Home Assistant as a `climate` entity (off / heat / auto, target
  temperature, room temperature, heating or idle), plus power, energy, duty, temperature,
  window-open and diagnostic sensors
- **Power and energy are estimates.** The radiator doesn't meter its consumption, so power is
  calculated as reported element power × duty cycle, and energy (kWh, `total_increasing`, works in
  the Energy dashboard) is that power integrated over time by the bridge. The total is saved in
  `data/config.json`. Nothing is counted while a radiator is disconnected.
- A web UI to approve and name radiators, set up the MQTT broker, generate router rules and
  control radiators directly
- Runs as a single Docker container

> Not affiliated with HJM or Helki. Tested against a single radiator model (pid `0132`, firmware 1.8.0).
> Other Helki-based brands (Haverland, Ducasa, Climastar, …) probably use the same protocol but
> different hostnames. Reports are welcome.

## How it works

The radiator's ESP32 keeps a WebSocket open to `wss://api-hjm.helki.com/api/v2/ws_dev` and
**does not validate the server's TLS certificate**. So if you send that connection to this server
(with a NAT rule or a DNS rewrite), the radiator talks to it as if it were the real cloud.

```
radiator ──TLS/WebSocket──▶ hjm-local-cloud :8443 ──MQTT──▶ Mosquitto ◀──▶ Home Assistant
                                   │
                                   └── web UI :8080
```

## Quick start

```sh
git clone <this repo> && cd hjm-local-cloud
docker compose up -d --build
```

1. Open `http://<server>:8080` and enter your MQTT broker under **MQTT broker**.
2. Redirect a radiator to the server (see below), then power-cycle it.
3. It shows up as **pending**. Give it a name and press **Approve**. Home Assistant picks it up
   through MQTT discovery.

Everything persistent lives in `./data` (`config.json` with the MQTT password, plus a self-signed
certificate generated on first start). Set `HJM_WEB_PASSWORD` in `docker-compose.yml` to put
the web UI behind HTTP basic auth.

## Redirecting radiators

### Option A: NAT rule per radiator (recommended to start)

On an OpenWrt / GL.iNet router (the web UI generates these for you):

```sh
R=192.168.1.50      # radiator
S=192.168.1.10      # server running hjm-local-cloud
iptables -t nat -I PREROUTING  -s $R -p tcp --dport 443 -j DNAT --to-destination $S:8443
iptables -t nat -I POSTROUTING -s $R -d $S -p tcp --dport 8443 -j MASQUERADE
iptables -I FORWARD -s $R -d $S -p tcp --dport 8443 -j ACCEPT
```

The `MASQUERADE` rule is needed because the radiator and the server are on the same LAN. Without
it, replies would bypass the router. To keep the rules after a reboot, add them to
`/etc/firewall.user`.

Only the radiator is affected, so the official app keeps working for everything else while you try
it out.

### Option B: DNS rewrite for every radiator

Rewrite `*.helki.com` to the server. `docker-compose.yml` already publishes port 443 for this.
If 443 is taken on your server, remove that line and use option A. Every radiator then connects
locally, with no per-device rules or fixed IPs. The official app and the HACS `smartbox`
integration stop working, because they validate certificates.

GL.iNet gotcha: with AdGuard Home *and* VPN policy routing enabled, LAN DNS may be answered by a
separate dnsmasq instance (port 2153, conf-dir `/tmp/dnsmasq.d.wgclient1`) that never consults
AdGuard. The rewrite then has to be added there as well.

### Afterwards

Block the radiators' internet access on the router. That way a cloud-pushed firmware update can never
add certificate validation.

## Restarts and updates

Radiators that the server disconnects with a WebSocket close frame **stop reconnecting until they
are power-cycled**. A dropped connection, by contrast, makes them retry within about 15 s. So
the bridge never sends close frames to radiators: on shutdown (`docker compose up -d --build`,
`docker compose restart`, …) it resets their TCP connections, and they should come back within
about a minute without being touched.

Connections that fail before the WebSocket is established (TLS handshake, HTTP upgrade) are
logged as warnings with the radiator's IP, so they show up in the web UI's log view.

## MQTT topics

| Topic | Payload |
|---|---|
| `hjm/bridge/status` | `online` / `offline` (retained, LWT) |
| `hjm/<devid>/availability` | `online` / `offline`: radiator connected |
| `hjm/<devid>/<node>/state` | status JSON plus `ha_mode`, `hvac_action`, `power_w`, `energy_kwh` (retained) |
| `hjm/<devid>/<node>/setup`, `…/prog` | raw settings / weekly program from the device |
| `hjm/<devid>/<node>/set/mode` | `off` · `heat` · `auto` |
| `hjm/<devid>/<node>/set/temperature` | e.g. `21.5` |

`<node>` is the heater on the device, e.g. `htr2`. Discovery goes under `homeassistant/`; both
prefixes can be changed in the UI.

## Configuration

| Variable | Default | |
|---|---|---|
| `HJM_DATA_DIR` | `/data` (container) | config + certificate |
| `HJM_DEVICE_PORT` | `8443` | TLS WebSocket port for radiators |
| `HJM_WEB_PORT` | `8080` | web UI |
| `HJM_WEB_PASSWORD` | – | enables basic auth on the web UI |
| `HJM_LOG_LEVEL` | `INFO` | `DEBUG` logs every WebSocket frame |
| `HJM_USERS_ONLINE_INTERVAL` | `60` | seconds between "app open" hints (see below) |
| `HJM_TZ_JSON` | Europe/Madrid | override the `rtc/tz` answer |

## Protocol notes

The protocol was worked out by relaying a radiator's traffic to the real cloud. Every frame is
JSON text shaped like a REST call:

```jsonc
// device -> cloud request                        // cloud -> device response
{"path":"htr/2/status","method":"POST",          {"method":"POST","path":"htr/2/status",
 "body":{"mode":"manual","stemp":"22.0",          "code":201,"body":{},"devid":"<devid>"}
         "mtemp":"18.6","active":true, ...}}

// cloud -> device command                        // device -> cloud ack
{"path":"/htr/2/status","method":"POST",          {"path":"/htr/2/status","method":"POST",
 "body":{"stemp":"22.0","units":"C"},              "code":201}
 "devid":"<devid>"}
```

- **Auth:** HTTP Basic `<devid>:<secret>` on the WebSocket upgrade. This server accepts anything and
  uses the devid as the radiator's identity.
- **Startup:** `GET rtc/tz` (time zone + Unix time), `GET firmware/version/last` (we echo the
  device's own version so it never tries to update), then `POST`s of `firmware/version`,
  `mgr/away_status`, `htr/<n>/prog`, `mgr/node`, `mgr/setup`, `htr/<n>/status`,
  `htr/<n>/setup`, `htr/<n>/version`.
- **Commands:** partial updates to `/htr/<n>/status`: `{"mode": "off|manual|auto"}`,
  `{"stemp": "21.5", "units": "C"}`. After applying a command, and whenever a reading changes,
  the device pushes a full status.
- **Status fields:** `mode`, `stemp` (target), `mtemp` (measured), `active` (heating), `duty` /
  `act_duty` (%), `power` (W), `comf_temp` / `eco_temp` / `ice_temp`, `window_open`, `presence`,
  `boost`, `locked`, `error_code`.
- **Schedule:** `prog` holds 7 days × 24 hourly slots. In `auto`, `1` produced `stemp = eco_temp`.
- **Keep-alive:** WebSocket ping/pong. While the app is open, the real cloud also sends
  `POST /mgr/users_online {"online": true}` every ~90 s; this server keeps sending it.

Not implemented yet, because the messages haven't been captured: writing schedules, comfort/eco/frost
temperatures and boost.

## Development

```sh
uv run --with . python -m hjm_local_cloud   # data in ./data, web UI on :8080
```
