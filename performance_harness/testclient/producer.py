"""Load generator: publishes at a target rate and self-reports achieved rate.

Ground-truth injected load. Each message carries ``extras={"seq", "pub_ts"}`` so
the analytics sidecar can measure end-to-end latency and detect gaps. Publish
throughput is reported from here (independent of any subscriber) so it stays
trustworthy even when the sidecar's own subscription is the bottleneck.
"""

import asyncio
import itertools
import os
import random
import time

from testclient import config

log = config.setup_logging().getChild("producer")

_seq = itertools.count(1)
_published = 0  # total successful publishes (single-threaded event loop => safe)
_rejected = 0
_errors = 0  # publishes that hit a transient disconnect


def _payload() -> bytes:
    return os.urandom(config.PAYLOAD_BYTES)


async def _publish_one(client) -> None:
    global _published, _rejected, _errors
    topic = random.choice(config.ALL_TOPICS)
    extras = {"seq": next(_seq), "pub_ts": time.time()}
    try:
        result = await client.publish(topic, _payload(), extras=extras)
    except (ConnectionError, OSError):
        # In-flight publishes fail on disconnect; the client auto-reconnects
        # underneath, so back off briefly and let the next attempt through.
        _errors += 1
        await asyncio.sleep(0.1)
        return
    if result.accepted:
        _published += 1
    else:
        _rejected += 1


async def _register_all(client) -> None:
    """Register every topic, retrying across transient disconnects."""
    while True:
        try:
            for topic in config.ALL_TOPICS:
                await client.register_topic(topic, replayable=True)
            return
        except (ConnectionError, OSError) as exc:
            log.warning("register failed (%s); client reconnecting, retrying", exc)
            await asyncio.sleep(0.5)


async def _paced_worker(client, rate_getter) -> None:
    """Publish paced to ``rate_getter()`` msgs/sec for this worker."""
    loop = asyncio.get_running_loop()
    next_at = loop.time()
    while True:
        rate = rate_getter()
        if rate <= 0:
            await asyncio.sleep(0.05)
            next_at = loop.time()
            continue
        await _publish_one(client)
        next_at += 1.0 / rate
        now = loop.time()
        if next_at > now:
            await asyncio.sleep(next_at - now)
        else:
            # Falling behind target; yield so we don't starve the loop.
            next_at = now
            await asyncio.sleep(0)


async def _max_worker(client) -> None:
    """Publish flat-out (no pacing) to find the ceiling."""
    while True:
        await _publish_one(client)


async def _burst_worker(client) -> None:
    if config.BURST_EVERY <= 0:
        return
    while True:
        await asyncio.sleep(config.BURST_EVERY)
        for _ in range(config.BURST_SIZE):
            await _publish_one(client)


def _payload_fn(workers: int):
    """Build the producer heartbeat payload with a self-tracked publish rate.

    Ground-truth publish rate (independent of any subscriber); the sidecar
    aggregates it into the fleet view.
    """
    loop = asyncio.get_running_loop()
    start = loop.time()
    state = {"last": _published, "t": loop.time()}

    def fn() -> dict:
        now = loop.time()
        rate = (_published - state["last"]) / max(now - state["t"], 1e-6)
        state["last"], state["t"] = _published, now
        return {
            "rate": round(rate, 1),
            "published": _published,
            "errors": _errors,
            "workers": workers,
            "uptime": round(now - start, 1),
        }

    return fn


async def _reporter() -> None:
    last, loop = _published, asyncio.get_running_loop()
    last_t = loop.time()
    while True:
        await asyncio.sleep(config.HEARTBEAT_EVERY)
        now = loop.time()
        rate = (_published - last) / (now - last_t) if now > last_t else 0.0
        log.info(
            "published=%d achieved=%.0f msg/s rejected=%d errors=%d",
            _published,
            rate,
            _rejected,
            _errors,
        )
        last, last_t = _published, now


def _make_rate_getter():
    """Return a callable giving the current per-worker target rate.

    Honours an optional stepped RAMP (total rate), split across workers.
    """
    loop = asyncio.get_running_loop()
    start = loop.time()
    workers = max(config.PRODUCER_CONCURRENCY, 1)

    if config.RAMP:
        steps = [float(r) for r in config.RAMP]

        def getter() -> float:
            idx = int((loop.time() - start) // config.RAMP_HOLD)
            total = steps[min(idx, len(steps) - 1)]
            return total / workers

        return getter

    total = float(config.PRODUCER_RATE)
    return lambda: total / workers


async def run() -> None:
    client = await config.connect_broker(log)
    workers = max(config.PRODUCER_CONCURRENCY, 1)
    is_max = str(config.PRODUCER_RATE).lower() == "max"
    log.info(
        "connected to %s:%d; producing mode=%s workers=%d payload=%dB",
        config.PUBSUB_HOST,
        config.PUBSUB_PORT,
        "max" if is_max else f"{config.PRODUCER_RATE} msg/s",
        workers,
        config.PAYLOAD_BYTES,
    )

    # Authenticated publish requires a durable claim. Do not start load workers
    # until every concrete data topic is owned by the harness principal.
    await _register_all(client)
    tasks: list[asyncio.Task[None]] = [
        asyncio.create_task(_reporter()),
        asyncio.create_task(config.heartbeat_loop(client, "producer", _payload_fn(workers))),
    ]
    if is_max:
        tasks += [asyncio.create_task(_max_worker(client)) for _ in range(workers)]
    else:
        rate_getter = _make_rate_getter()
        tasks += [
            asyncio.create_task(_paced_worker(client, rate_getter))
            for _ in range(workers)
        ]
    tasks.append(asyncio.create_task(_burst_worker(client)))

    try:
        await asyncio.gather(*tasks)
    finally:
        for t in tasks:
            t.cancel()
        await client.close()


def main() -> None:
    try:
        config.run_async(run())
    except KeyboardInterrupt:
        log.info("producer stopping")


if __name__ == "__main__":
    main()
