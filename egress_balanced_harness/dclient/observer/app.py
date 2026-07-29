"""FastAPI observation platform for the distributed harness.

Aggregates the whole cluster from a single instance:
  * fans a ``>`` throughput sub and a ``_stats.>`` fleet sub *in* to every broker
    (one connection each per broker) so it measures the full stream and every
    producer/consumer heartbeat regardless of which broker they landed on;
  * scrapes every broker's OTel ``/metrics`` and sums the counters (also keeping
    the per-broker breakdown);
  * reads the shared Postgres durable layer for DLQ + retained-history.

It never modifies a broker and acks everything immediately so its own bounded
queues do not overflow.
"""

import asyncio
import contextlib
import json
import time
from collections.abc import AsyncIterator

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, StreamingResponse

from dclient import config
from dclient.observer import brokerotel
from dclient.observer.dashboard import DASHBOARD_HTML
from dclient.observer.stats import Stats
from dclient.postgres_durability import PostgresDurability

log = config.setup_logging().getChild("observer")

_STATS_SELECTOR = config.STATS_PREFIX + ".>"


async def _pump_throughput(client, tag: str, stats: Stats, stop: asyncio.Event) -> None:
    """Drain the ``>`` firehose on one broker for throughput."""
    while not stop.is_set():
        try:
            sub = await client.subscribe(config.ALL_SELECTOR)
        except (ConnectionError, OSError) as exc:
            log.warning("[%s] subscribe '>' failed: %s; retrying", tag, exc)
            await asyncio.sleep(0.5)
            continue
        stats.mark_subscribed()
        log.info("[%s] observer subscribed to '>'", tag)
        try:
            async for delivery in sub:
                m = delivery.message
                if not m.topic.startswith(config.STATS_PREFIX + "."):
                    stats.record(m.topic, delivery.attempt, dict(m.extras), time.time())
                await sub.ack(delivery)
        except (ConnectionError, OSError) as exc:
            log.warning("[%s] throughput pump error: %s", tag, exc)
        stats.mark_evicted()
        if not stop.is_set():
            await asyncio.sleep(0.5)


async def _pump_fleet(client, tag: str, stats: Stats, stop: asyncio.Event) -> None:
    """Dedicated low-rate telemetry plane per broker: `_stats.>` heartbeats."""
    while not stop.is_set():
        try:
            sub = await client.subscribe(_STATS_SELECTOR)
        except (ConnectionError, OSError) as exc:
            log.warning("[%s] subscribe '%s' failed: %s; retrying", tag, _STATS_SELECTOR, exc)
            await asyncio.sleep(0.5)
            continue
        log.info("[%s] observer subscribed to '%s' (fleet)", tag, _STATS_SELECTOR)
        try:
            async for delivery in sub:
                m = delivery.message
                payload = m.payload if isinstance(m.payload, dict) else {}
                stats.record_instance(m.topic, payload, time.time())
                await sub.ack(delivery)
        except (ConnectionError, OSError) as exc:
            log.warning("[%s] fleet pump error: %s", tag, exc)
        if not stop.is_set():
            await asyncio.sleep(0.5)


async def _otel_loop(app: FastAPI, stop: asyncio.Event) -> None:
    """Scrape every broker's OTel counters; sum them and keep per-broker rates."""
    prev: dict[str, dict] = {}
    prev_t: dict[str, float] = {}
    while not stop.is_set():
        await asyncio.sleep(max(config.STATS_INTERVAL * 2, 2.0))
        # Re-derive each tick so scraping follows --scale broker=N up and down.
        urls = config.broker_metrics_urls()
        now = time.time()
        agg_totals: dict[str, float] = {}
        agg_rates: dict[str, float] = {}
        per_broker: list[dict] = []
        any_ok = False
        for url in urls:
            name = url.split("//", 1)[-1].split(":", 1)[0]  # host portion
            try:
                text = await asyncio.to_thread(brokerotel.fetch, url)
                totals = brokerotel.parse(text)
            except Exception as exc:  # noqa: BLE001 - a broker may be down
                log.debug("[%s] otel scrape failed: %s", name, exc)
                per_broker.append({"name": name, "ok": False})
                continue
            any_ok = True
            rates: dict = {}
            if name in prev:
                dt = max(now - prev_t[name], 1e-6)
                rates = {k: round((totals[k] - prev[name][k]) / dt, 1) for k in totals}
            prev[name], prev_t[name] = totals, now
            for k, v in totals.items():
                agg_totals[k] = agg_totals.get(k, 0.0) + v
            for k, v in rates.items():
                agg_rates[k] = agg_rates.get(k, 0.0) + v
            per_broker.append({
                "name": name,
                "ok": True,
                "totals": {k: int(v) for k, v in totals.items()},
                "rates": rates,
            })
        app.state.brokers = per_broker
        app.state.broker_otel = {
            "ok": any_ok,
            "totals": {k: int(v) for k, v in agg_totals.items()},
            "rates": {k: round(v, 1) for k, v in agg_rates.items()},
        }


async def _durable_snapshot(durability: PostgresDurability) -> dict:
    """DLQ + retained-history view from the shared Postgres durable layer."""
    out: dict = {"dlq": [], "dlq_count": 0, "history_count": 0, "topics": []}
    try:
        entries = await durability.read_dlq()
        out["dlq_count"] = len(entries)
        for e in entries[-50:]:
            out["dlq"].append({
                "topic": e.message.topic,
                "message_id": e.message.message_id,
                "subscription_id": e.subscription_id,
                "attempts": e.attempts,
                "created_at": e.message.created_at,
                "payload_bytes": len(e.message.payload)
                if isinstance(e.message.payload, (bytes, str)) else None,
            })
    except Exception as exc:  # noqa: BLE001 - best-effort read of a live DB
        log.debug("read_dlq failed: %s", exc)
    counts = await durability.counts()
    out["history_count"] = counts.get("history_count", 0)
    out["topics"] = counts.get("topics", [])
    # dlq_count from the cheap COUNT(*) is authoritative if read_dlq was capped.
    out["dlq_count"] = max(out["dlq_count"], counts.get("dlq_count", 0))
    return out


async def _build(app: FastAPI) -> dict:
    stats: Stats = app.state.stats
    snap = stats.snapshot()
    snap["durable"] = await _durable_snapshot(app.state.durability)
    snap["audit"] = await app.state.durability.audit_by_broker()
    targets = config.broker_targets()
    snap["broker"] = {"host": "cluster", "port": len(targets)}
    snap["brokers"] = getattr(app.state, "brokers", [])
    otel = getattr(app.state, "broker_otel", {"ok": False})
    snap["broker_otel"] = otel
    snap["health"] = _assess(otel)
    return snap


def _assess(otel: dict) -> dict:
    """Diagnose cluster-wide event-loop saturation from summed broker OTel."""
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
                f"Cluster saturated at ~{pub:.0f} publish/s aggregate: brokers are "
                f"delivering {deliv:.0f}/s but processing only {processed:.0f} "
                f"acks+nacks/s — consumer acks/heartbeats are starved. Reduce "
                f"producer count / per-producer rate, or add brokers."
            ),
        }
    return {"saturated": False, "msg": ""}


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    stop = asyncio.Event()
    stats = Stats()
    app.state.stats = stats
    app.state.broker_otel = {"ok": False}
    app.state.brokers = []
    durability = await PostgresDurability.connect(config.PG_DSN)
    app.state.durability = durability

    # Fleet heartbeats are published per instance to whichever broker that
    # instance is pinned to, so the observer fans the `_stats.>` plane IN across
    # all brokers to see every producer/consumer (record_instance dedups by
    # instance id, so producer broadcast heartbeats don't double-count).
    async def attach_fleet(host: str, port: int, tag: str):
        fl = await config.connect_broker_at(log, host, port, deadline=5.0)
        return [fl], [asyncio.create_task(_pump_fleet(fl, tag, stats, stop))]

    managers = [
        asyncio.create_task(config.fan_in(log, attach_fleet, stop)),
        asyncio.create_task(_otel_loop(app, stop)),
    ]

    # Ingest is replicated: EVERY broker carries the full stream, so the
    # throughput firehose reads exactly ONE broker (via the egress LB) to avoid a
    # Y× count. Disabled at giant fanout (config.OBSERVER_FIREHOSE=0): it starves
    # the observer's HTTP loop and undercounts as one saturated subscriber —
    # throughput then comes from summed broker OTel (authoritative).
    tp = None
    if config.OBSERVER_FIREHOSE:
        tp = await config.connect_broker(log)
        managers.append(
            asyncio.create_task(_pump_throughput(tp, "egress-lb", stats, stop))
        )

    try:
        yield
    finally:
        stop.set()
        for p in managers:
            p.cancel()
        for p in managers:
            with contextlib.suppress(asyncio.CancelledError):
                await p
        if tp is not None:
            with contextlib.suppress(Exception):
                await tp.close()
        await durability.close()


app = FastAPI(title="pubsub.py egress observer", lifespan=lifespan)


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return DASHBOARD_HTML


@app.get("/api/stats")
async def api_stats() -> dict:
    return await _build(app)


@app.get("/api/otel")
async def api_otel() -> dict:
    """Cheap, firehose-independent throughput view for the bench driver.

    Returns only the cached broker-OTel scrape (summed + per-broker), refreshed
    by ``_otel_loop`` — no `>` firehose, no Postgres query — so it stays
    responsive even when the cluster is saturated at giant fanout.
    """
    return {
        "otel": getattr(app.state, "broker_otel", {"ok": False}),
        "brokers": getattr(app.state, "brokers", []),
    }


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
