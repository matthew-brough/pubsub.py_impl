"""Fast-acking sink pinned to ONE broker via the egress load balancer.

The egress topology replicates ingest (every broker holds the full stream), so a
consumer only needs a single connection: the nginx egress LB (`least_conn`)
assigns it to one broker and it still sees every message. This is the fan-out
scaling model — Z consumers spread across Y brokers, each broker serving its own
shard of ~Z/Y consumers. On broker loss the LB re-homes the reconnect to a
surviving broker (which also has the full stream), so delivery continues.

Contrast the ingress harness, where each consumer must fan in to *every* broker.
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


async def _pump(client, selector: str, stop: asyncio.Event) -> None:
    """Drain a selector, surviving broker-side eviction by re-subscribing."""
    global _acked, _nacked, _max_attempt, _evictions
    while not stop.is_set():
        try:
            sub = await client.subscribe(selector)
        except (ConnectionError, OSError) as exc:
            log.warning("subscribe %s failed (%s); retrying", selector, exc)
            await asyncio.sleep(0.5)
            continue
        log.info("subscribed %s", selector)
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
            log.warning("%s stream error (%s); re-subscribing", selector, exc)
        if not stop.is_set():
            _evictions += 1
            await asyncio.sleep(0.2)


def _payload_fn():
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
            "acked=%d (%.0f/s) nacked=%d max_attempt=%d evictions=%d",
            _acked, rate, _nacked, _max_attempt, _evictions,
        )
        last, last_t = _acked, now


async def run() -> None:
    # Stagger the join to smooth the subscribe storm at giant Z.
    if config.CONSUMER_JOIN_STAGGER > 0:
        delay = random.random() * config.CONSUMER_JOIN_STAGGER
        log.info("join stagger: sleeping %.1fs before connecting", delay)
        await asyncio.sleep(delay)

    # Single connection through the egress LB (PUBSUB_HOST=nginx). One broker's
    # full stream == the whole stream, because ingest is replicated.
    client = await config.connect_broker(log)
    hb_client = await config.connect_broker(log)
    log.info("connected via egress LB %s:%d", config.PUBSUB_HOST, config.PUBSUB_PORT)
    stop = asyncio.Event()
    tasks = [
        asyncio.create_task(_pump(client, config.ORDER_SELECTOR, stop)),
        asyncio.create_task(_pump(client, config.SENSOR_SELECTOR, stop)),
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
            await client.close()
            await hb_client.close()


def main() -> None:
    try:
        config.run_async(run())
    except KeyboardInterrupt:
        log.info("consumer stopping")


if __name__ == "__main__":
    main()
