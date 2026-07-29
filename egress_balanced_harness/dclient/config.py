"""Shared configuration + helpers for every distributed-harness service.

One module so producer, consumer, broker, and the observer agree on where the
brokers are, which topics exist, and how hard to push. Everything is env-driven
so docker-compose (or the topology generator) can retune a run without code
changes.

Distributed specifics vs the single-node performance_harness:
  * ``PUBSUB_HOST``/``PUBSUB_PORT`` point producers at the **nginx ingress**,
    which round-robins the TCP connection onto one broker.
  * ``BROKERS`` enumerates every broker ``host:port`` so consumers and the
    observer can **fan in** — open one subscription per broker — and thereby see
    every event exactly once regardless of which broker a publish landed on.
  * ``PG_DSN`` is the shared Postgres durable layer used by all brokers.
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
# `<STATS_PREFIX>.<kind>.<INSTANCE_ID>`; the observer's fleet sub collects them.
INSTANCE_ID = os.environ.get("INSTANCE_ID") or socket.gethostname()
STATS_PREFIX = os.environ.get("STATS_PREFIX", "_stats")
HEARTBEAT_EVERY = float(os.environ.get("HEARTBEAT_EVERY", "2.0"))
# Observer drops an instance from the fleet view after this many seconds silent.
STATS_TTL = float(os.environ.get("STATS_TTL", "8.0"))


def stats_topic(kind: str) -> str:
    return f"{STATS_PREFIX}.{kind}.{INSTANCE_ID}"


async def heartbeat_loop(client, kind: str, payload_fn) -> None:
    """Publish `payload_fn()` to the instance's stats topic every HEARTBEAT_EVERY.

    Each publish is bounded by a timeout so a slow/backpressured connection can
    never stall the loop — a skipped beat just ages the instance out of the
    fleet view.
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
# Producers connect through the nginx ingress (single virtual endpoint, LB'd).
PUBSUB_HOST = os.environ.get("PUBSUB_HOST", "nginx")
PUBSUB_PORT = int(os.environ.get("PUBSUB_PORT", "8765"))

# Fan-in discovery. Consumers and the observer open one connection per broker
# replica so no message is missed no matter which broker it landed on.
#
# Preferred: BROKER_SERVICE names a Docker Compose service (e.g. "broker") whose
# replicas are DNS-discovered — getaddrinfo returns every replica's IP, and a
# periodic re-resolve follows `--scale broker=N` up and down.
#
# Fallback: BROKERS is a static "host:port,host:port" list (used when there is no
# service DNS, e.g. running outside compose).
BROKER_SERVICE = os.environ.get("BROKER_SERVICE", "").strip()
BROKER_PORT = int(os.environ.get("BROKER_PORT", str(PUBSUB_PORT)))
RERESOLVE_SECONDS = float(os.environ.get("RERESOLVE_SECONDS", "10"))
_BROKERS_RAW = os.environ.get("BROKERS", "").strip()


def resolve_broker_ips() -> list[str]:
    """DNS-discover every replica IP of ``BROKER_SERVICE`` (sorted, de-duped)."""
    if not BROKER_SERVICE:
        return []
    try:
        # IPv4 only: Docker also returns an IPv6 record per replica, which would
        # double every fan-in subscription (each broker seen twice) and inflate
        # the OTel scrape list. nginx's resolver runs ipv6=off for the same reason.
        infos = socket.getaddrinfo(
            BROKER_SERVICE, BROKER_PORT, family=socket.AF_INET, type=socket.SOCK_STREAM
        )
    except OSError:
        return []
    return sorted({str(info[4][0]) for info in infos})


def broker_targets() -> list[tuple[str, int]]:
    """Return current ``[(host, port), ...]`` for fan-in subscribers.

    Uses DNS discovery when ``BROKER_SERVICE`` is set, else the static
    ``BROKERS`` list, else the ingress endpoint as a last resort.
    """
    if BROKER_SERVICE:
        return [(ip, BROKER_PORT) for ip in resolve_broker_ips()]
    if not _BROKERS_RAW:
        return [(PUBSUB_HOST, PUBSUB_PORT)]
    out: list[tuple[str, int]] = []
    for item in _BROKERS_RAW.split(","):
        item = item.strip()
        if not item:
            continue
        host, _, port = item.partition(":")
        out.append((host, int(port) if port else PUBSUB_PORT))
    return out


async def fan_in(log, attach, stop: asyncio.Event) -> None:
    """Keep one attachment per live broker replica, re-resolving on an interval.

    ``attach(host, port, tag)`` is awaited for each newly-appeared replica and
    returns ``(clients, tasks)`` — the broker connection(s) it opened and the
    pump task(s) running against them. This manager re-resolves the broker set
    every ``RERESOLVE_SECONDS``: it attaches to new replicas and cancels + closes
    departed ones, so a ``--scale broker=N`` change is followed live without a
    restart. ``attach`` owns its own connects (a replica may need several, e.g.
    a firehose plane plus a separate fleet plane).
    """
    active: dict[
        tuple[str, int], tuple[list[BrokerClient], list[asyncio.Task]]
    ] = {}

    async def _teardown(clients: list[BrokerClient], tasks: list[asyncio.Task]) -> None:
        for t in tasks:
            t.cancel()
        for c in clients:
            try:
                await c.close()
            except Exception as exc:  # noqa: BLE001 - best-effort teardown
                log.debug("fan-in client close failed: %s", exc)

    try:
        while not stop.is_set():
            targets = set(broker_targets())
            for key in list(active):
                if key not in targets:
                    clients, tasks = active.pop(key)
                    await _teardown(clients, tasks)
                    log.info("fan-in dropped departed broker %s:%d", *key)
            for host, port in targets:
                if (host, port) in active:
                    continue
                tag = f"{host}:{port}"
                try:
                    clients, tasks = await attach(host, port, tag)
                except (OSError, ConnectionError):
                    continue
                active[(host, port)] = (clients, tasks)
                log.info("fan-in attached to new broker %s", tag)
            await asyncio.sleep(max(RERESOLVE_SECONDS, 1.0))
    finally:
        for clients, tasks in active.values():
            await _teardown(clients, tasks)


# --- shared durable layer -------------------------------------------------
# Postgres DSN every broker (and the observer's DLQ reader) connects to.
PG_DSN = os.environ.get(
    "PG_DSN", "postgresql://pubsub:pubsub@postgres:5432/pubsub"
)

# --- topic catalog --------------------------------------------------------
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
PRODUCER_RATE = os.environ.get("PRODUCER_RATE", "2000")
PRODUCER_CONCURRENCY = int(os.environ.get("PRODUCER_CONCURRENCY", "4"))
PAYLOAD_BYTES = int(os.environ.get("PAYLOAD_BYTES", "256"))
RAMP = [r for r in os.environ.get("RAMP", "").split(",") if r]
RAMP_HOLD = float(os.environ.get("RAMP_HOLD", "20"))
BURST_EVERY = float(os.environ.get("BURST_EVERY", "0"))
BURST_SIZE = int(os.environ.get("BURST_SIZE", "500"))

# --- consumer knobs -------------------------------------------------------
NACK_RATE = float(os.environ.get("NACK_RATE", "0.02"))
POISON_EVERY = int(os.environ.get("POISON_EVERY", "500"))
CONSUMER_WORK_MS = float(os.environ.get("CONSUMER_WORK_MS", "0"))
# Spread the subscribe "join storm": each consumer sleeps random*(this) before
# its first subscribe. At giant Z a synchronized join can starve broker accept
# and the observer's startup; staggering smooths it. 0 = join immediately.
CONSUMER_JOIN_STAGGER = float(os.environ.get("CONSUMER_JOIN_STAGGER", "0"))

# --- misc -----------------------------------------------------------------
STATS_INTERVAL = float(os.environ.get("STATS_INTERVAL", "1.0"))
CONNECT_RETRY_SECONDS = float(os.environ.get("CONNECT_RETRY_SECONDS", "60"))

# --- authentication + feature probe ---------------------------------------
# Shared identity keeps replicated-ingest claims coherent across every broker
# while adding auth/grant/claim work without changing benchmark role topology.
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


# --- OpenTelemetry (broker-side metrics) ----------------------------------
# When off, the observer skips the `>` throughput firehose and reports
# throughput purely from summed broker OTel (authoritative and Y-safe). At giant
# fanout the firehose both undercounts (one starved subscriber) and starves the
# observer's own HTTP loop, so the bench disables it for large cells.
OBSERVER_FIREHOSE = _flag("OBSERVER_FIREHOSE", "1")

OTEL_ENABLED = _flag("OTEL_ENABLED", "1")
OTEL_SERVICE_NAME = os.environ.get("OTEL_SERVICE_NAME", "pubsub-broker")
OTEL_PROM_PORT = int(os.environ.get("OTEL_PROM_PORT", "9464"))


def broker_metrics_urls() -> list[str]:
    """Return every live broker's Prometheus /metrics URL for the observer.

    Re-derived from the current broker set each call so it follows scaling; an
    explicit ``BROKER_OTEL_URLS`` override wins (static deployments).
    """
    override = os.environ.get("BROKER_OTEL_URLS", "").strip()
    if override:
        return [u.strip() for u in override.split(",") if u.strip()]
    return [f"http://{host}:{OTEL_PROM_PORT}/metrics" for host, _ in broker_targets()]


def run_async(coro) -> None:
    """Run ``coro`` on uvloop when available, else stdlib asyncio."""
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
    return logging.getLogger("dclient")


async def connect_broker_at(
    log: logging.Logger,
    host: str,
    port: int,
    *,
    deadline: float = CONNECT_RETRY_SECONDS,
) -> BrokerClient:
    """Connect to a specific broker with a bounded retry loop.

    The compose healthchecks gate most races, but the very first ``connect`` can
    still lose to a broker's bind; retry until ``deadline`` then give up.
    """
    loop = asyncio.get_running_loop()
    start = loop.time()
    attempt = 0
    while True:
        try:
            return await BrokerClient.connect(
                host,
                port,
                auth=AUTH_CREDENTIALS,
            )
        except (OSError, ConnectionError) as exc:
            attempt += 1
            if loop.time() - start > deadline:
                raise
            delay = min(0.25 * (2**attempt), 3.0)
            log.info("broker %s:%d not ready (%s); retry in %.1fs", host, port, exc, delay)
            await asyncio.sleep(delay)


async def connect_broker(
    log: logging.Logger, *, deadline: float = CONNECT_RETRY_SECONDS
) -> BrokerClient:
    """Connect through the ingress endpoint (producers)."""
    return await connect_broker_at(log, PUBSUB_HOST, PUBSUB_PORT, deadline=deadline)
