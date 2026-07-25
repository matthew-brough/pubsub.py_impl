"""Fast-acking sink that fans *in* across every broker.

One of Z consumer replicas. Because the library fans out live messages only to
subscribers on the same broker, a consumer must subscribe on **every** broker to
see the whole stream. Each published message lives on exactly one broker's
island, so fanning in delivers every event exactly once — no dedup needed, no
relay, library internals untouched.

Counters are process-global (single event loop) so ack/nack totals aggregate
across all broker connections for the fleet heartbeat.
"""

import asyncio
import contextlib
import random

from dclient import config

log = config.setup_logging().getChild("consumer")

_acked = 0
_nacked = 0
_max_attempt = 1
_evictions = 0


async def _pump(client, selector: str, tag: str, stop: asyncio.Event) -> None:
    """Drain a selector on one broker, surviving eviction by re-subscribing."""
    global _acked, _nacked, _max_attempt, _evictions
    while not stop.is_set():
        try:
            sub = await client.subscribe(selector)
        except (ConnectionError, OSError) as exc:
            log.warning("[%s] subscribe %s failed (%s); retrying", tag, selector, exc)
            await asyncio.sleep(0.5)
            continue
        log.info("[%s] subscribed %s", tag, selector)
        try:
            async for delivery in sub:
                if delivery.attempt > _max_attempt:
                    _max_attempt = delivery.attempt
                if config.CONSUMER_WORK_MS:
                    await asyncio.sleep(config.CONSUMER_WORK_MS / 1000.0)
                seq = delivery.message.extras.get("seq")
                poison = (
                    config.POISON_EVERY > 0
                    and isinstance(seq, int)
                    and seq % config.POISON_EVERY == 0
                )
                transient = config.NACK_RATE and random.random() < config.NACK_RATE
                if poison or transient:
                    await sub.nack(delivery)
                    _nacked += 1
                else:
                    await sub.ack(delivery)
                    _acked += 1
        except (ConnectionError, OSError) as exc:
            log.warning("[%s] %s stream error (%s); re-subscribing", tag, selector, exc)
        if not stop.is_set():
            _evictions += 1
            await asyncio.sleep(0.2)


def _payload_fn():
    """Consumer heartbeat payload with self-tracked ack/nack rates (all brokers)."""
    loop = asyncio.get_running_loop()
    start = loop.time()
    state = {"a": _acked, "n": _nacked, "t": loop.time()}

    def fn() -> dict:
        now = loop.time()
        dt = max(now - state["t"], 1e-6)
        ack_rate = (_acked - state["a"]) / dt
        nack_rate = (_nacked - state["n"]) / dt
        state["a"], state["n"], state["t"] = _acked, _nacked, now
        return {
            "ack_rate": round(ack_rate, 1),
            "nack_rate": round(nack_rate, 1),
            "acked": _acked,
            "nacked": _nacked,
            "max_attempt": _max_attempt,
            "evictions": _evictions,
            "uptime": round(now - start, 1),
        }

    return fn


async def _reporter() -> None:
    last, loop = _acked, asyncio.get_running_loop()
    last_t = loop.time()
    while True:
        await asyncio.sleep(config.HEARTBEAT_EVERY)
        now = loop.time()
        rate = (_acked - last) / (now - last_t) if now > last_t else 0.0
        log.info(
            "acked=%d (%.0f/s) nacked=%d max_attempt=%d evictions=%d brokers=%d",
            _acked,
            rate,
            _nacked,
            _max_attempt,
            _evictions,
            len(config.broker_targets()),
        )
        last, last_t = _acked, now


async def run() -> None:
    stop = asyncio.Event()

    # Stagger the join to smooth the subscribe storm at giant Z.
    if config.CONSUMER_JOIN_STAGGER > 0:
        delay = random.random() * config.CONSUMER_JOIN_STAGGER
        log.info("join stagger: sleeping %.1fs before subscribing", delay)
        await asyncio.sleep(delay)

    async def attach(host: str, port: int, tag: str):
        # One data connection per broker replica; drain orders + sensors from
        # that broker's island. Union across brokers == full stream, seen once.
        client = await config.connect_broker_at(log, host, port, deadline=5.0)
        tasks = [
            asyncio.create_task(_pump(client, config.ORDER_SELECTOR, tag, stop)),
            asyncio.create_task(_pump(client, config.SENSOR_SELECTOR, tag, stop)),
        ]
        return [client], tasks

    # Heartbeat rides the nginx ingress (any broker), so it does not depend on a
    # specific replica surviving a scale-down.
    hb_client = await config.connect_broker(log)

    tasks = [
        asyncio.create_task(config.fan_in(log, attach, stop)),
        asyncio.create_task(_reporter()),
        asyncio.create_task(config.heartbeat_loop(hb_client, "consumer", _payload_fn())),
    ]
    try:
        await asyncio.gather(*tasks)
    finally:
        stop.set()
        for t in tasks:
            t.cancel()
        with contextlib.suppress(Exception):
            await hb_client.close()


def main() -> None:
    try:
        config.run_async(run())
    except KeyboardInterrupt:
        log.info("consumer stopping")


if __name__ == "__main__":
    main()
