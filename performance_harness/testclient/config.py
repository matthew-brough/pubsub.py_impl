"""Shared configuration + helpers for every harness service.

One module so producer, consumer, and the management sidecar agree on where the
broker is, which topics exist, and how hard to push. Everything is env-driven so
docker-compose (or a shell) can retune a run without code changes.
"""

import asyncio
import logging
import os
import socket
from collections.abc import Mapping

from pubsub.auth import Capability, Principal, StaticAuthenticator
from pubsub.shared.types import MessagePackValue
from pubsub.transport.client import BrokerClient
from pubsub.transport.wire import AuthError

# --- instance identity + fleet telemetry ----------------------------------
# Each producer/consumer heartbeats its own stats through the broker on
# `<STATS_PREFIX>.<kind>.<INSTANCE_ID>`; the sidecar's ">" sub collects them.
INSTANCE_ID = os.environ.get("INSTANCE_ID") or socket.gethostname()
STATS_PREFIX = os.environ.get("STATS_PREFIX", "_stats")
HEARTBEAT_EVERY = float(os.environ.get("HEARTBEAT_EVERY", "2.0"))
# Sidecar drops an instance from the fleet view after this many seconds silent.
STATS_TTL = float(os.environ.get("STATS_TTL", "8.0"))


def stats_topic(kind: str) -> str:
    return f"{STATS_PREFIX}.{kind}.{INSTANCE_ID}"


async def heartbeat_loop(client, kind: str, payload_fn) -> None:
    """Publish `payload_fn()` to the instance's stats topic every HEARTBEAT_EVERY.

    Each publish is bounded by a timeout so a slow/backpressured connection can
    never stall the loop — a skipped beat just ages the instance out of the
    fleet view. Callers give the heartbeat its own connection when the data
    connection is busy (e.g. a flooded consumer).
    """
    topic = stats_topic(kind)
    await client.register_topic(topic, replayable=False)
    while True:
        await asyncio.sleep(HEARTBEAT_EVERY)
        try:
            await asyncio.wait_for(
                client.publish(topic, payload_fn()), timeout=HEARTBEAT_EVERY
            )
        except (ConnectionError, OSError, asyncio.TimeoutError):
            pass

# --- broker location ------------------------------------------------------
PUBSUB_HOST = os.environ.get("PUBSUB_HOST", "controller")
PUBSUB_PORT = int(os.environ.get("PUBSUB_PORT", "8765"))
PUBSUB_DB = os.environ.get("PUBSUB_DB", "/data/pubsub.db")

# --- topic catalog --------------------------------------------------------
# register_topic works on *concrete* subjects, so the set the producer will
# publish is enumerated here (wildcards are only for subscription selectors).
ORDER_REGIONS = os.environ.get(
    "ORDER_REGIONS", "us-east,us-west,eu,apac"
).split(",")
SENSOR_IDS = list(range(int(os.environ.get("SENSOR_COUNT", "8"))))

ORDER_TOPICS = [f"orders.{r}" for r in ORDER_REGIONS if r]
SENSOR_TOPICS = [f"sensors.{i}.temp" for i in SENSOR_IDS]
ALL_TOPICS = ORDER_TOPICS + SENSOR_TOPICS

# Subscription selectors (NATS-style tail wildcard).
ORDER_SELECTOR = "orders.>"
SENSOR_SELECTOR = "sensors.>"
ALL_SELECTOR = ">"

# --- load knobs -----------------------------------------------------------
# PRODUCER_RATE: target messages/sec across all producer tasks, or "max" for
# unthrottled (find the ceiling).
PRODUCER_RATE = os.environ.get("PRODUCER_RATE", "2000")
PRODUCER_CONCURRENCY = int(os.environ.get("PRODUCER_CONCURRENCY", "4"))
PAYLOAD_BYTES = int(os.environ.get("PAYLOAD_BYTES", "256"))
# RAMP: optional comma list of rates held RAMP_HOLD seconds each, then the last
# value sticks. Empty => constant PRODUCER_RATE. Ignored in "max" mode.
RAMP = [r for r in os.environ.get("RAMP", "").split(",") if r]
RAMP_HOLD = float(os.environ.get("RAMP_HOLD", "20"))
# Burst: every BURST_EVERY seconds, publish BURST_SIZE extra messages back-to-back.
BURST_EVERY = float(os.environ.get("BURST_EVERY", "0"))  # 0 disables
BURST_SIZE = int(os.environ.get("BURST_SIZE", "500"))

# --- consumer knobs -------------------------------------------------------
# NACK_RATE: random *transient* nacks (message succeeds on a later attempt) —
#   shows retry/backoff as attempt>1 in the consumer log.
# POISON_EVERY: every Nth message (by seq) is a *poison* message the consumer
#   always nacks, so it exhausts the retry budget and lands in the DLQ. 0 = off.
NACK_RATE = float(os.environ.get("NACK_RATE", "0.02"))
POISON_EVERY = int(os.environ.get("POISON_EVERY", "500"))
CONSUMER_WORK_MS = float(os.environ.get("CONSUMER_WORK_MS", "0"))

# --- misc -----------------------------------------------------------------
STATS_INTERVAL = float(os.environ.get("STATS_INTERVAL", "1.0"))
CONNECT_RETRY_SECONDS = float(os.environ.get("CONNECT_RETRY_SECONDS", "30"))

# --- authentication + feature probe ---------------------------------------
# Shared identity intentionally preserves existing multi-producer semantics:
# every harness process may reclaim the same concrete topic without introducing
# authz partitioning as a new benchmark variable.
AUTH_TOKEN = os.environ.get("AUTH_TOKEN", "pubsub-harness")
AUTH_IDENTITY = os.environ.get("AUTH_IDENTITY", "pubsub-harness")
AUTH_CREDENTIALS: Mapping[str, MessagePackValue] = {"token": AUTH_TOKEN}


def _flag(name: str, default: str = "1") -> bool:
    return os.environ.get(name, default).lower() in ("1", "true", "yes", "on")


def make_authenticator() -> StaticAuthenticator:
    grants = {capability: (">",) for capability in Capability}
    return StaticAuthenticator(
        {AUTH_TOKEN: Principal(AUTH_IDENTITY, grants)}
    )


async def verify_upstream_features(host: str, port: int, topic: str) -> None:
    """Fail broker startup unless auth, claims, and packed delivery all work."""
    try:
        await BrokerClient.connect(
            host,
            port,
            reconnect=False,
            auth={"token": f"{AUTH_TOKEN}-invalid"},
        )
    except AuthError as exc:
        if exc.code != "auth_rejected":
            raise RuntimeError(f"unexpected auth probe code: {exc.code}") from exc
    else:
        raise RuntimeError("invalid auth token was accepted")

    client = await BrokerClient.connect(
        host,
        port,
        reconnect=False,
        auth=AUTH_CREDENTIALS,
    )
    try:
        await client.register_topic(topic, replayable=False)
        subscription = await client.subscribe(topic)
        result = await client.publish(topic, "packed-delivery-probe")
        if not result.accepted:
            raise RuntimeError(f"feature probe publish rejected: {result.error}")
        delivery = await asyncio.wait_for(anext(subscription), timeout=3.0)
        if delivery.message.payload != "packed-delivery-probe":
            raise RuntimeError("packed delivery probe payload mismatch")
        await subscription.ack(delivery)
        await subscription.unsubscribe()
        await client.unregister(topic)
    finally:
        await client.close()


# --- OpenTelemetry (broker-side metrics via pubsub.observability.otel) -----
# Controller wires an OTelObserver into the Broker and exposes a Prometheus
# /metrics endpoint; the sidecar scrapes it for broker-authoritative counters.
OTEL_ENABLED = _flag("OTEL_ENABLED", "1")
OTEL_SERVICE_NAME = os.environ.get("OTEL_SERVICE_NAME", "pubsub-controller")
OTEL_PROM_PORT = int(os.environ.get("OTEL_PROM_PORT", "9464"))
CONTROLLER_METRICS_URL = os.environ.get(
    "CONTROLLER_METRICS_URL", f"http://{PUBSUB_HOST}:{OTEL_PROM_PORT}/metrics"
)


def run_async(coro) -> None:
    """Run ``coro`` on uvloop when available, else stdlib asyncio.

    Mirrors the library runner's opt-in: the ``pubsub-py[fast]`` extra vendors
    uvloop, but our services build the broker/clients by hand (custom retry
    policy, observer) instead of going through ``pubsub-server``, so each
    entrypoint must select the fast loop itself. Falls back cleanly where uvloop
    is unavailable (e.g. Windows, or the extra not installed).
    """
    try:
        import uvloop
    except ImportError:
        asyncio.run(coro)
    else:
        uvloop.run(coro)


def setup_logging() -> logging.Logger:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    return logging.getLogger("testclient")


async def connect_broker(
    log: logging.Logger, *, deadline: float = CONNECT_RETRY_SECONDS
) -> BrokerClient:
    """Connect with a short retry loop.

    The compose healthcheck gates most races, but the very first ``connect``
    can still lose to the broker's bind; retry until ``deadline`` then give up.
    """
    loop = asyncio.get_running_loop()
    start = loop.time()
    attempt = 0
    while True:
        try:
            return await BrokerClient.connect(
                PUBSUB_HOST,
                PUBSUB_PORT,
                auth=AUTH_CREDENTIALS,
            )
        except (OSError, ConnectionError) as exc:
            attempt += 1
            if loop.time() - start > deadline:
                raise
            delay = min(0.25 * (2**attempt), 3.0)
            log.info("broker not ready (%s); retry in %.1fs", exc, delay)
            await asyncio.sleep(delay)
