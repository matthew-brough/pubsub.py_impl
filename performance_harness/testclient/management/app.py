"""FastAPI analytics sidecar.

Subscribes to ``>`` (every subject) purely to *measure* the broker: counts
deliveries for throughput, reads ``pub_ts`` for latency, and reads the
controller's SQLite DB for DLQ + retained history. Serves a live dashboard over
SSE. It never modifies the broker and acks everything immediately so its own
bounded queue does not overflow.
"""

import asyncio
import contextlib
import json
import time
from collections.abc import AsyncIterator

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, StreamingResponse

from pubsub.server.durability.sqlite import SQLiteDurability

from testclient import config
from testclient.management import brokerotel
from testclient.management.stats import Stats
from testclient.management.dashboard import DASHBOARD_HTML

log = config.setup_logging().getChild("management")


_STATS_SELECTOR = config.STATS_PREFIX + ".>"


async def _pump_throughput(client, stats: Stats, stop: asyncio.Event) -> None:
    """Drain the ``>`` firehose for throughput. May saturate/evict under load."""
    while not stop.is_set():
        try:
            sub = await client.subscribe(config.ALL_SELECTOR)
        except (ConnectionError, OSError) as exc:
            log.warning("subscribe '>' failed: %s; retrying", exc)
            await asyncio.sleep(0.5)
            continue
        stats.mark_subscribed()
        log.info("sidecar subscribed to '%s'", config.ALL_SELECTOR)
        try:
            async for delivery in sub:
                m = delivery.message
                # Heartbeats are measured on the dedicated fleet sub, not here.
                if not m.topic.startswith(config.STATS_PREFIX + "."):
                    stats.record(m.topic, delivery.attempt, dict(m.extras), time.time())
                await sub.ack(delivery)
        except (ConnectionError, OSError) as exc:
            log.warning("throughput pump error: %s", exc)
        stats.mark_evicted()
        if not stop.is_set():
            await asyncio.sleep(0.5)


async def _pump_fleet(client, stats: Stats, stop: asyncio.Event) -> None:
    """Dedicated low-rate telemetry plane: `_stats.>` heartbeats only.

    Separate from the ``>`` firehose so sparse heartbeats are never dropped when
    the throughput sub saturates — the fleet view stays reliable under load.
    """
    while not stop.is_set():
        try:
            sub = await client.subscribe(_STATS_SELECTOR)
        except (ConnectionError, OSError) as exc:
            log.warning("subscribe '%s' failed: %s; retrying", _STATS_SELECTOR, exc)
            await asyncio.sleep(0.5)
            continue
        log.info("sidecar subscribed to '%s' (fleet)", _STATS_SELECTOR)
        try:
            async for delivery in sub:
                m = delivery.message
                payload = m.payload if isinstance(m.payload, dict) else {}
                stats.record_instance(m.topic, payload, time.time())
                await sub.ack(delivery)
        except (ConnectionError, OSError) as exc:
            log.warning("fleet pump error: %s", exc)
        if not stop.is_set():
            await asyncio.sleep(0.5)


async def _otel_loop(app: FastAPI, stop: asyncio.Event) -> None:
    """Scrape the controller's broker-side OTel counters, derive rates."""
    prev: dict | None = None
    prev_t = 0.0
    while not stop.is_set():
        await asyncio.sleep(max(config.STATS_INTERVAL * 2, 2.0))
        try:
            text = await asyncio.to_thread(brokerotel.fetch, config.CONTROLLER_METRICS_URL)
            agg = brokerotel.parse(text)
        except Exception as exc:  # noqa: BLE001 - metrics endpoint may be down
            log.debug("otel scrape failed: %s", exc)
            app.state.broker_otel = {"ok": False}
            continue
        now = time.time()
        rates: dict = {}
        if prev is not None:
            dt = max(now - prev_t, 1e-6)
            rates = {k: round((agg[k] - prev[k]) / dt, 1) for k in agg}
        app.state.broker_otel = {
            "ok": True,
            "totals": {k: int(v) for k, v in agg.items()},
            "rates": rates,
        }
        prev, prev_t = agg, now


async def _durable_snapshot(durability: SQLiteDurability) -> dict:
    """DLQ + retained-history view from the controller's SQLite DB."""
    out: dict = {"dlq": [], "dlq_count": 0, "history_count": 0, "topics": []}
    try:
        entries = await durability.read_dlq()
        out["dlq_count"] = len(entries)
        for e in entries[-50:]:
            out["dlq"].append(
                {
                    "topic": e.message.topic,
                    "message_id": e.message.message_id,
                    "subscription_id": e.subscription_id,
                    "attempts": e.attempts,
                    "created_at": e.message.created_at,
                    "payload_bytes": len(e.message.payload)
                    if isinstance(e.message.payload, (bytes, str))
                    else None,
                }
            )
    except Exception as exc:  # noqa: BLE001 - best-effort read of a live DB
        log.debug("read_dlq failed: %s", exc)
    # Cheap counts straight off the DB (avoid pulling all history each tick).
    with contextlib.suppress(Exception):
        # Only replayable traffic counts as retained history; unregistered
        # `_stats.*` heartbeats are replayable=0 and excluded.
        row = await durability._db.fetchone(
            "SELECT COUNT(*) AS n FROM messages WHERE replayable=1"
        )
        out["history_count"] = int(row["n"]) if row else 0
    with contextlib.suppress(Exception):
        rows = await durability._db.fetchall(
            "SELECT topic, replayable FROM topics ORDER BY topic"
        )
        out["topics"] = [
            {"topic": r["topic"], "replayable": bool(r["replayable"])} for r in rows
        ]
    return out


async def _build(app: FastAPI) -> dict:
    stats: Stats = app.state.stats
    snap = stats.snapshot()
    snap["durable"] = await _durable_snapshot(app.state.durability)
    snap["broker"] = {"host": config.PUBSUB_HOST, "port": config.PUBSUB_PORT}
    otel = getattr(app.state, "broker_otel", {"ok": False})
    snap["broker_otel"] = otel
    snap["health"] = _assess(otel)
    return snap


def _assess(otel: dict) -> dict:
    """Diagnose broker-event-loop saturation from broker-side OTel.

    Under saturation one producer monopolises the loop: deliveries keep flowing
    (broker-internal), but consumer->broker frames (acks, nacks, heartbeats) are
    starved. So `deliveries > 0` with `acks+nacks ~ 0` is the fingerprint — and
    it also explains why the client-side fleet view undercounts.
    """
    if not otel.get("ok"):
        return {"saturated": False, "msg": ""}
    r = otel.get("rates", {})
    deliv = r.get("deliveries", 0.0)
    processed = r.get("acks", 0.0) + r.get("nacks", 0.0)
    if deliv > 50 and processed < deliv * 0.15:
        pub = r.get("publishes_ok", 0.0)
        return {
            "saturated": True,
            "msg": (
                f"Broker saturated at ~{pub:.0f} publish/s (aggregate ceiling): one "
                f"producer is monopolising the event loop. Broker is delivering "
                f"{deliv:.0f}/s but processing only {processed:.0f} acks+nacks/s — "
                f"other producers and all consumer acks/heartbeats are starved, so "
                f"the fleet panel undercounts. Reduce producer count / per-producer "
                f"rate to run below the knee."
            ),
        }
    return {"saturated": False, "msg": ""}


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    stop = asyncio.Event()
    client = await config.connect_broker(log)
    log.info("connected to broker %s:%d", config.PUBSUB_HOST, config.PUBSUB_PORT)
    durability = await SQLiteDurability.connect(config.PUBSUB_DB)
    stats = Stats()
    app.state.stats = stats
    app.state.durability = durability
    app.state.broker_otel = {"ok": False}
    pumps = [
        asyncio.create_task(_pump_throughput(client, stats, stop)),
        asyncio.create_task(_pump_fleet(client, stats, stop)),
        asyncio.create_task(_otel_loop(app, stop)),
    ]
    try:
        yield
    finally:
        stop.set()
        for p in pumps:
            p.cancel()
        for p in pumps:
            with contextlib.suppress(asyncio.CancelledError):
                await p
        await client.close()
        await durability.close()


app = FastAPI(title="pubsub.py throughput sidecar", lifespan=lifespan)


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return DASHBOARD_HTML


@app.get("/api/stats")
async def api_stats() -> dict:
    return await _build(app)


@app.get("/api/dlq")
async def api_dlq() -> dict:
    return await _durable_snapshot(app.state.durability)


@app.get("/events")
async def events() -> StreamingResponse:
    async def gen() -> AsyncIterator[bytes]:
        while True:
            snap = await _build(app)
            yield f"data: {json.dumps(snap)}\n\n".encode()
            await asyncio.sleep(config.STATS_INTERVAL)

    return StreamingResponse(gen(), media_type="text/event-stream")
