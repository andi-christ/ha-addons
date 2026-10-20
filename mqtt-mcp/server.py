"""
MQTT MCP — a small FastMCP server that lets Claude watch, publish and inspect
the Mosquitto broker that runs as a Home Assistant add-on.

Runs as a Home Assistant add-on. The broker host and login come from the
Supervisor's mqtt service (what the Mosquitto add-on publishes for other
add-ons) unless mqtt_host is set in the add-on options. The MCP endpoint is
Streamable HTTP at /<secret>/mcp, reached through the NGINX SSL proxy add-on
(https://<public_host>/<secret>/mcp) by way of a location snippet this add-on
writes to /share. The secret path is the only access control, so treat the
URL like a password.

Tools:
  watch            live traffic on a topic filter for a window
  retained         snapshot of retained messages under a filter
  topic_tree       which topics are active, with counts and last payloads
  publish          publish, optionally waiting for replies on other topics
  clear_retained   remove one retained message
  clear_retained_matching
                   remove retained messages under a filter (dry run by default)
  stats            Mosquitto $SYS snapshot
  broker_log       tail of the Mosquitto add-on log, with a connections view

Pinned to fastmcp==3.4.6, same as the other Faikin MCP servers. Do not float.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import secrets
import socket
import threading
import time
import urllib.error
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import anyio
import paho.mqtt.client as mqtt
from fastmcp import FastMCP

OPTIONS_FILE = Path("/data/options.json")
SECRET_FILE = Path("/data/secret.txt")
SUPERVISOR = "http://supervisor"
SUPERVISOR_TOKEN = os.environ.get("SUPERVISOR_TOKEN", "")
PORT = 8087

OPTIONS: dict[str, Any] = (
    json.loads(OPTIONS_FILE.read_text()) if OPTIONS_FILE.exists() else {}
)
MAX_WINDOW = int(OPTIONS.get("max_window_seconds") or 60)
MAX_MESSAGES = int(OPTIONS.get("max_messages") or 500)
PAYLOAD_LIMIT = int(OPTIONS.get("payload_limit") or 2000)


def log(msg: str) -> None:
    print(f"[mqtt-mcp] {msg}", flush=True)


# --------------------------------------------------------------------------
# Supervisor helpers
# --------------------------------------------------------------------------


def supervisor_get(path: str, headers: dict[str, str] | None = None, raw: bool = False):
    req = urllib.request.Request(
        SUPERVISOR + path,
        headers={"Authorization": f"Bearer {SUPERVISOR_TOKEN}", **(headers or {})},
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        body = resp.read()
    if raw:
        return body.decode("utf-8", "replace")
    return json.loads(body)


def broker_settings() -> dict[str, Any]:
    """Broker host and login: add-on options first, then the Supervisor mqtt service."""
    host = (OPTIONS.get("mqtt_host") or "").strip()
    if host:
        return {
            "host": host,
            "port": int(OPTIONS.get("mqtt_port") or 1883),
            "username": OPTIONS.get("mqtt_user") or None,
            "password": OPTIONS.get("mqtt_password") or None,
        }
    data = supervisor_get("/services/mqtt")["data"]
    return {
        "host": data["host"],
        "port": int(data["port"]),
        "username": data.get("username"),
        "password": data.get("password"),
    }


def load_secret() -> str:
    configured = (OPTIONS.get("secret") or "").strip()
    if configured:
        return configured
    if SECRET_FILE.exists():
        stored = SECRET_FILE.read_text().strip()
        if stored:
            return stored
    generated = secrets.token_urlsafe(24)
    SECRET_FILE.write_text(generated)
    return generated


# --------------------------------------------------------------------------
# MQTT plumbing
# --------------------------------------------------------------------------


def _decode(payload: bytes, limit: int) -> str:
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError:
        text = "<binary hex> " + payload.hex()
    if len(text) > limit:
        return text[:limit] + f"… [truncated, {len(payload)} bytes]"
    return text


def _now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="milliseconds")


class Session:
    """One short-lived broker connection with a message collector."""

    def __init__(self, purpose: str):
        cfg = broker_settings()
        self.host, self.port = cfg["host"], cfg["port"]
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"mqtt-mcp-{purpose}-{secrets.token_hex(3)}",
            protocol=mqtt.MQTTv311,
        )
        if cfg["username"]:
            self.client.username_pw_set(cfg["username"], cfg["password"])
        self.connected = threading.Event()
        self.subscribed = threading.Event()
        self.connect_rc: Any = None
        self.messages: list[dict[str, Any]] = []
        self.lock = threading.Lock()
        self.limit = PAYLOAD_LIMIT
        self.max_messages = MAX_MESSAGES
        self.keep_retained = True
        self.client.on_connect = self._on_connect
        self.client.on_subscribe = self._on_subscribe
        self.client.on_message = self._on_message

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        self.connect_rc = reason_code
        self.connected.set()

    def _on_subscribe(self, client, userdata, mid, reason_codes, properties=None):
        self.subscribed.set()

    def _on_message(self, client, userdata, msg):
        if msg.retain and not self.keep_retained:
            return
        with self.lock:
            if len(self.messages) >= self.max_messages:
                return
            self.messages.append(
                {
                    "time": _now(),
                    "topic": msg.topic,
                    "payload": _decode(msg.payload, self.limit),
                    "retained": bool(msg.retain),
                    "qos": msg.qos,
                }
            )

    def open(self, subscribe: list[str] | None = None, qos: int = 0) -> None:
        self.client.connect(self.host, self.port, keepalive=30)
        self.client.loop_start()
        if not self.connected.wait(10):
            self.close()
            raise RuntimeError(f"no CONNACK from {self.host}:{self.port} within 10 s")
        if self.connect_rc is not None and self.connect_rc.is_failure:
            self.close()
            raise RuntimeError(f"broker refused the connection: {self.connect_rc}")
        if subscribe:
            self.client.subscribe([(t, qos) for t in subscribe])
            self.subscribed.wait(5)

    def collect(self, seconds: float, stop_at: int | None = None) -> float:
        """Wait for the window to pass or the message cap to fill. Returns seconds waited."""
        started = time.monotonic()
        deadline = started + seconds
        cap = stop_at or self.max_messages
        while time.monotonic() < deadline:
            with self.lock:
                if len(self.messages) >= cap:
                    break
            time.sleep(0.1)
        return round(time.monotonic() - started, 2)

    def close(self) -> None:
        try:
            self.client.disconnect()
        finally:
            self.client.loop_stop()

    def snapshot(self) -> list[dict[str, Any]]:
        with self.lock:
            return list(self.messages)


def _clamp_window(seconds: float) -> float:
    return max(0.5, min(float(seconds), float(MAX_WINDOW)))


def _clamp_count(n: int) -> int:
    return max(1, min(int(n), MAX_MESSAGES))


# --------------------------------------------------------------------------
# Tool implementations (blocking; run in a worker thread by the async tools)
# --------------------------------------------------------------------------


def _watch(topic_filter: str, seconds: float, max_messages: int, include_retained: bool) -> dict:
    seconds = _clamp_window(seconds)
    s = Session("watch")
    s.max_messages = _clamp_count(max_messages)
    s.keep_retained = include_retained
    s.open([topic_filter])
    waited = s.collect(seconds)
    s.close()
    msgs = s.snapshot()
    return {
        "broker": f"{s.host}:{s.port}",
        "topic_filter": topic_filter,
        "seconds_watched": waited,
        "message_count": len(msgs),
        "capped": len(msgs) >= s.max_messages,
        "messages": msgs,
    }


def _retained(topic_filter: str, seconds: float, max_messages: int) -> dict:
    seconds = _clamp_window(seconds)
    s = Session("retained")
    s.max_messages = _clamp_count(max_messages)
    s.open([topic_filter])
    s.collect(seconds)
    s.close()
    latest: dict[str, dict[str, Any]] = {}
    for m in s.snapshot():
        if m["retained"]:
            latest[m["topic"]] = m
    items = [latest[t] for t in sorted(latest)]
    return {
        "broker": f"{s.host}:{s.port}",
        "topic_filter": topic_filter,
        "retained_count": len(items),
        "capped": len(s.snapshot()) >= s.max_messages,
        "messages": items,
    }


def _topic_tree(topic_filter: str, seconds: float, include_retained: bool) -> dict:
    seconds = _clamp_window(seconds)
    s = Session("tree")
    s.max_messages = MAX_MESSAGES * 4
    s.limit = 120
    s.keep_retained = include_retained
    s.open([topic_filter])
    waited = s.collect(seconds)
    s.close()
    msgs = s.snapshot()
    counts: Counter[str] = Counter(m["topic"] for m in msgs)
    live: Counter[str] = Counter(m["topic"] for m in msgs if not m["retained"])
    last: dict[str, dict[str, Any]] = {}
    for m in msgs:
        last[m["topic"]] = m
    prefixes: Counter[str] = Counter(t.split("/", 1)[0] for t in counts)
    topics = [
        {
            "topic": t,
            "count": counts[t],
            "live_count": live[t],
            "retained_seen": any(m["retained"] for m in msgs if m["topic"] == t),
            "last_payload": last[t]["payload"],
            "last_time": last[t]["time"],
        }
        for t in sorted(counts, key=lambda t: (-counts[t], t))
    ]
    return {
        "broker": f"{s.host}:{s.port}",
        "topic_filter": topic_filter,
        "seconds_watched": waited,
        "message_count": len(msgs),
        "topic_count": len(topics),
        "top_level_prefixes": dict(prefixes.most_common()),
        "topics": topics,
    }


def _publish(
    topic: str,
    payload: str,
    retain: bool,
    qos: int,
    reply_topics: list[str] | None,
    wait_seconds: float,
) -> dict:
    s = Session("publish")
    s.max_messages = MAX_MESSAGES
    s.open(reply_topics or None)
    info = s.client.publish(topic, payload.encode("utf-8"), qos=qos, retain=retain)
    info.wait_for_publish(5)
    result: dict[str, Any] = {
        "broker": f"{s.host}:{s.port}",
        "topic": topic,
        "bytes": len(payload.encode("utf-8")),
        "retain": retain,
        "qos": qos,
        "published": info.is_published(),
        "rc": int(info.rc),
    }
    if reply_topics:
        waited = s.collect(_clamp_window(wait_seconds or 5))
        result["reply_topics"] = reply_topics
        result["seconds_waited"] = waited
        result["replies"] = s.snapshot()
    s.close()
    return result


def _clear_retained(topics: list[str]) -> dict:
    s = Session("clear")
    s.open()
    cleared = []
    for t in topics:
        info = s.client.publish(t, b"", qos=0, retain=True)
        info.wait_for_publish(5)
        cleared.append({"topic": t, "published": info.is_published()})
    s.close()
    return {"broker": f"{s.host}:{s.port}", "cleared": cleared}


def _stats(seconds: float) -> dict:
    s = Session("stats")
    s.max_messages = 2000
    s.open(["$SYS/#"])
    s.collect(_clamp_window(seconds), stop_at=2000)
    s.close()
    values: dict[str, str] = {}
    for m in s.snapshot():
        values[m["topic"]] = m["payload"]
    return {"broker": f"{s.host}:{s.port}", "count": len(values), "sys": dict(sorted(values.items()))}


_CONNECT_RE = re.compile(
    r"^(?P<ts>\S+)?.*?New client connected from (?P<addr>\S+?)(?::\d+)? as (?P<id>\S+) "
    r"\((?P<flags>[^)]*)\)"
)
_DISCONNECT_RE = re.compile(r"^(?P<ts>\S+)?.*?Client (?P<id>\S+) (?P<what>closed its connection|disconnected|has exceeded timeout).*")


def _broker_log(lines: int, pattern: str | None, connections_only: bool) -> dict:
    lines = max(10, min(int(lines), 5000))
    text: str
    try:
        text = supervisor_get(
            "/addons/core_mosquitto/logs",
            headers={"Accept": "text/plain", "Range": f"entries=:-{lines}:"},
            raw=True,
        )
    except urllib.error.HTTPError as e:
        if e.code not in (400, 416):
            raise
        text = supervisor_get("/addons/core_mosquitto/logs", headers={"Accept": "text/plain"}, raw=True)
    raw_lines = text.splitlines()[-lines:]
    if connections_only:
        events = []
        for ln in raw_lines:
            m = _CONNECT_RE.search(ln)
            if m:
                events.append({"event": "connect", "client_id": m["id"], "from": m["addr"], "flags": m["flags"], "line": ln})
                continue
            m = _DISCONNECT_RE.search(ln)
            if m:
                events.append({"event": "disconnect", "client_id": m["id"], "detail": m["what"], "line": ln})
        return {"lines_scanned": len(raw_lines), "events": events}
    if pattern:
        rx = re.compile(pattern, re.IGNORECASE)
        raw_lines = [ln for ln in raw_lines if rx.search(ln)]
    return {"lines": raw_lines}


# --------------------------------------------------------------------------
# MCP surface
# --------------------------------------------------------------------------

mcp = FastMCP(
    "MQTT MCP",
    instructions=(
        "Tools for the Mosquitto broker on the Faikin debug Home Assistant "
        "(mqtt.faikin.au). watch and topic_tree are live windows of at most "
        f"{MAX_WINDOW} s and {MAX_MESSAGES} messages; run them back to back for "
        "longer captures. Payloads over the limit are truncated."
    ),
)


@mcp.tool
async def watch(
    topic_filter: str = "#",
    seconds: float = 30,
    max_messages: int = 200,
    include_retained: bool = False,
) -> dict:
    """Subscribe to topic_filter and return every message seen in the window.

    Retained messages are excluded unless include_retained is true. Ends early
    once max_messages have arrived. The window is capped by the add-on's
    max_window_seconds option (default 60 s).
    """
    return await anyio.to_thread.run_sync(_watch, topic_filter, seconds, max_messages, include_retained)


@mcp.tool
async def retained(topic_filter: str = "#", seconds: float = 3, max_messages: int = 500) -> dict:
    """Snapshot of the retained messages under topic_filter, one entry per topic."""
    return await anyio.to_thread.run_sync(_retained, topic_filter, seconds, max_messages)


@mcp.tool
async def topic_tree(topic_filter: str = "#", seconds: float = 10, include_retained: bool = True) -> dict:
    """Which topics are active under topic_filter: message counts, last payload (trimmed to 120 chars) and top-level prefixes."""
    return await anyio.to_thread.run_sync(_topic_tree, topic_filter, seconds, include_retained)


@mcp.tool
async def publish(
    topic: str,
    payload: str = "",
    retain: bool = False,
    qos: int = 0,
    reply_topics: list[str] | None = None,
    wait_seconds: float = 5,
) -> dict:
    """Publish payload to topic. If reply_topics are given, subscribe to them first and return what arrives within wait_seconds."""
    return await anyio.to_thread.run_sync(_publish, topic, payload, retain, max(0, min(qos, 2)), reply_topics, wait_seconds)


@mcp.tool
async def clear_retained(topic: str) -> dict:
    """Remove the retained message on one exact topic (publishes an empty retained payload)."""
    return await anyio.to_thread.run_sync(_clear_retained, [topic])


@mcp.tool
async def clear_retained_matching(topic_filter: str, confirm: bool = False, seconds: float = 3) -> dict:
    """List the retained topics under a wildcard filter and, only when confirm is true, clear them all. Capped at 200 topics."""
    snap = await anyio.to_thread.run_sync(_retained, topic_filter, seconds, 200)
    topics = [m["topic"] for m in snap["messages"]]
    if not confirm:
        return {"dry_run": True, "would_clear": topics, "hint": "call again with confirm=true to clear these"}
    if not topics:
        return {"dry_run": False, "cleared": []}
    return await anyio.to_thread.run_sync(_clear_retained, topics)


@mcp.tool
async def stats(seconds: float = 3) -> dict:
    """Mosquitto $SYS snapshot: clients connected, messages, bytes, uptime, version."""
    return await anyio.to_thread.run_sync(_stats, seconds)


@mcp.tool
async def broker_log(lines: int = 200, pattern: str | None = None, connections_only: bool = False) -> dict:
    """Tail of the Mosquitto add-on log. pattern filters lines (regex, case-insensitive); connections_only parses connect and disconnect events into a list."""
    return await anyio.to_thread.run_sync(_broker_log, lines, pattern, connections_only)


@mcp.tool
async def broker_info() -> dict:
    """Which broker this server talks to, and the limits in force."""
    cfg = await anyio.to_thread.run_sync(broker_settings)
    return {
        "host": cfg["host"],
        "port": cfg["port"],
        "username": cfg["username"],
        "max_window_seconds": MAX_WINDOW,
        "max_messages": MAX_MESSAGES,
        "payload_limit": PAYLOAD_LIMIT,
    }


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------


NGINX_SNIPPET = Path("/share/nginx_proxy_default_mqtt_mcp.conf")


def write_nginx_snippet(secret: str) -> None:
    """Drop a location block for the NGINX SSL proxy add-on into /share.

    The proxy add-on includes /share/nginx_proxy_default*.conf inside its HTTPS
    server block when customize.active is on. The upstream is resolved at
    request time (Docker's embedded DNS) so NGINX still starts if this add-on
    is down. The proxy has to be restarted after the file changes.
    """
    host = socket.gethostname()
    body = f"""# Written by the mqtt-mcp add-on at start. Do not edit; set the add-on options.
location /{secret}/ {{
    resolver 127.0.0.11 valid=30s ipv6=off;
    set $mqtt_mcp_upstream http://{host}:{PORT};
    proxy_pass $mqtt_mcp_upstream;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_buffering off;
    proxy_cache off;
    proxy_read_timeout 600s;
    proxy_send_timeout 600s;
}}
"""
    try:
        if NGINX_SNIPPET.exists() and NGINX_SNIPPET.read_text() == body:
            log(f"nginx snippet {NGINX_SNIPPET} unchanged")
            return
        NGINX_SNIPPET.write_text(body)
        log(f"wrote nginx snippet {NGINX_SNIPPET} (upstream {host}:{PORT}); restart the NGINX SSL proxy add-on to pick it up")
    except OSError as e:
        log(f"could not write {NGINX_SNIPPET}: {e}")


def main() -> None:
    secret = load_secret()
    path = f"/{secret}/mcp"
    public_host = (OPTIONS.get("public_host") or "").strip() or "<this host>"
    use_ssl = bool(OPTIONS.get("ssl", False))
    nginx_include = bool(OPTIONS.get("nginx_include", True))

    uvicorn_config: dict[str, Any] = {}
    scheme = "http"
    if use_ssl:
        cert = Path("/ssl") / (OPTIONS.get("certfile") or "fullchain.pem")
        key = Path("/ssl") / (OPTIONS.get("keyfile") or "privkey.pem")
        if not cert.exists() or not key.exists():
            raise SystemExit(f"ssl is on but {cert} or {key} is missing")
        uvicorn_config = {"ssl_certfile": str(cert), "ssl_keyfile": str(key)}
        scheme = "https"

    try:
        cfg = broker_settings()
        log(f"broker {cfg['host']}:{cfg['port']} as {cfg['username'] or '(anonymous)'}")
    except Exception as e:  # noqa: BLE001
        log(f"could not read broker settings yet: {e}")

    if nginx_include:
        write_nginx_snippet(secret)
        log(f"MCP endpoint via the NGINX proxy: https://{public_host}{path}")
    else:
        log(f"MCP endpoint: {scheme}://{public_host}:{PORT}{path}")
    log("Treat that URL as a password. Rotate it by setting a new secret in the add-on options.")

    asyncio.run(
        mcp.run_http_async(
            show_banner=False,
            transport="http",
            host="0.0.0.0",
            port=PORT,
            path=path,
            uvicorn_config=uvicorn_config or None,
        )
    )


if __name__ == "__main__":
    main()
