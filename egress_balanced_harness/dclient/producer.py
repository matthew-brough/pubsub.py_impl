"""Load generator with REPLICATED INGEST: broadcasts each message to every broker.

The egress topology scales fan-out to giant consumer counts by giving *every*
broker the full message stream and letting each broker serve a shard of
consumers. That requires the ingest side to replicate: this producer resolves the
`broker` service to every replica and publishes each message to all of them
(re-resolving so `--scale broker=N` is followed live). Producers are few, so
broadcasting to Y brokers is cheap — exactly the workload this topology targets.

Reported publish rate counts *logical* messages (one per generated message,
regardless of how many brokers it was mirrored to); broker-side OTel and the
Postgres audit trail show the Y× physical writes.
"""

import asyncio
import itertools
import os
import random
import time

from dclient import config

log = config.setup_logging().getChild("producer")

_seq = itertools.count(1)
_published = 0  # logical messages successfully mirrored to >=1 broker
_rejected = 0
_errors = 0


async def _register_all(client) -> None:
    while True:
        try:
            for topic in config.ALL_TOPICS:
                await client.register_topic(topic, replayable=True)
            return
        except (ConnectionError, OSError) as exc:
            log.warning("register failed (%s); retrying", exc)
            await asyncio.sleep(0.5)


class Broadcaster:
    """Maintains one publish connection per live broker replica, re-resolving."""

    def __init__(self) -> None:
        self._clients: dict[tuple[str, int], object] = {}

    def clients(self) -> list:
        return list(self._clients.values())

    async def maintain(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            targets = set(config.broker_targets())
            for key in list(self._clients):
                if key not in targets:
                    client = self._clients.pop(key)
                    try:
                        await client.close()
                    except Exception:  # noqa: BLE001
                        pass
                    log.info("broadcast dropped departed broker %s:%d", *key)
            for host, port in targets:
                if (host, port) in self._clients:
                    continue
                try:
                    client = await config.connect_broker_at(log, host, port, deadline=5.0)
                except (OSError, ConnectionError):
                    continue
                asyncio.create_task(_register_all(client))
                self._clients[(host, port)] = client
                log.info("broadcast attached to broker %s:%d", host, port)
            await asyncio.sleep(max(config.RERESOLVE_SECONDS, 1.0))

    async def close(self) -> None:
        for client in self._clients.values():
            try:
                await client.close()
            except Exception:  # noqa: BLE001
                pass


def _payload() -> bytes:
    return os.urandom(config.PAYLOAD_BYTES)


async def _publish_one(bc: Broadcaster) -> None:
    """Mirror one message to every current broker; one logical publish."""
    global _published, _rejected, _errors
    clients = bc.clients()
    if not clients:
        await asyncio.sleep(0.05)
        return
    topic = random.choice(config.ALL_TOPICS)
    extras = {"seq": next(_seq), "pub_ts": time.time()}
    payload = _payload()
    results = await asyncio.gather(
        *(c.publish(topic, payload, extras=extras) for c in clients),
        return_exceptions=True,
    )
    accepted = 0
    for r in results:
        if isinstance(r, BaseException):
            _errors += 1
        elif getattr(r, "accepted", False):
            accepted += 1
    if accepted:
        _published += 1
    else:
        _rejected += 1


async def _paced_worker(bc: Broadcaster, rate_getter) -> None:
    loop = asyncio.get_running_loop()
    next_at = loop.time()
    while True:
        rate = rate_getter()
        if rate <= 0:
            await asyncio.sleep(0.05)
            next_at = loop.time()
            continue
        await _publish_one(bc)
        next_at += 1.0 / rate
        now = loop.time()
        if next_at > now:
            await asyncio.sleep(next_at - now)
        else:
            next_at = now
            await asyncio.sleep(0)


async def _max_worker(bc: Broadcaster) -> None:
    while True:
        await _publish_one(bc)


async def _burst_worker(bc: Broadcaster) -> None:
    if config.BURST_EVERY <= 0:
        return
    while True:
        await asyncio.sleep(config.BURST_EVERY)
        for _ in range(config.BURST_SIZE):
            await _publish_one(bc)


def _payload_fn(workers: int):
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


async def _heartbeat(bc: Broadcaster, payload_fn) -> None:
    """Mirror the producer heartbeat to every broker (observer dedups by id)."""
    topic = config.stats_topic("producer")
    while True:
        await asyncio.sleep(config.HEARTBEAT_EVERY)
        data = payload_fn()
        for c in bc.clients():
            try:
                await asyncio.wait_for(c.publish(topic, data), timeout=config.HEARTBEAT_EVERY)
            except (ConnectionError, OSError, asyncio.TimeoutError):
                pass


async def _reporter() -> None:
    last, loop = _published, asyncio.get_running_loop()
    last_t = loop.time()
    while True:
        await asyncio.sleep(config.HEARTBEAT_EVERY)
        now = loop.time()
        rate = (_published - last) / (now - last_t) if now > last_t else 0.0
        log.info(
            "published=%d achieved=%.0f msg/s rejected=%d errors=%d brokers=%d",
            _published, rate, _rejected, _errors, len(config.broker_targets()),
        )
        last, last_t = _published, now


def _make_rate_getter():
    loop = asyncio.get_running_loop()
    start = loop.time()
    workers = max(config.PRODUCER_CONCURRENCY, 1)
    if config.RAMP:
        steps = [float(r) for r in config.RAMP]

        def getter() -> float:
            idx = int((loop.time() - start) // config.RAMP_HOLD)
            return steps[min(idx, len(steps) - 1)] / workers

        return getter
    total = float(config.PRODUCER_RATE)
    return lambda: total / workers


async def run() -> None:
    stop = asyncio.Event()
    bc = Broadcaster()
    workers = max(config.PRODUCER_CONCURRENCY, 1)
    is_max = str(config.PRODUCER_RATE).lower() == "max"
    log.info(
        "broadcast producer: mode=%s workers=%d payload=%dB (mirrors to all brokers)",
        "max" if is_max else f"{config.PRODUCER_RATE} msg/s", workers, config.PAYLOAD_BYTES,
    )

    tasks: list[asyncio.Task] = [
        asyncio.create_task(bc.maintain(stop)),
        asyncio.create_task(_reporter()),
        asyncio.create_task(_heartbeat(bc, _payload_fn(workers))),
    ]
    if is_max:
        tasks += [asyncio.create_task(_max_worker(bc)) for _ in range(workers)]
    else:
        rate_getter = _make_rate_getter()
        tasks += [asyncio.create_task(_paced_worker(bc, rate_getter)) for _ in range(workers)]
    tasks.append(asyncio.create_task(_burst_worker(bc)))

    try:
        await asyncio.gather(*tasks)
    finally:
        stop.set()
        for t in tasks:
            t.cancel()
        await bc.close()


def main() -> None:
    try:
        config.run_async(run())
    except KeyboardInterrupt:
        log.info("producer stopping")


if __name__ == "__main__":
    main()
