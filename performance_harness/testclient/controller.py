"""Broker host (controller) with an env-tunable retry policy.

Equivalent to ``python -m pubsub`` but constructs the ``Broker`` with a
``RetryPolicy`` we can tighten via env, so the harness can drive messages to the
DLQ in seconds instead of the ~8 minutes the library defaults imply (budget 10,
base 1s backoff). Binds 0.0.0.0 by default for cross-container reach.
"""

import os

from pubsub.server.broker import Broker
from pubsub.server.durability.memory import InMemoryDurability
from pubsub.server.durability.sqlite import SQLiteDurability
from pubsub.server.retry import RetryPolicy
from pubsub.transport.server import BrokerServer

from testclient import config

log = config.setup_logging().getChild("controller")

BIND_HOST = os.environ.get("BIND_HOST", "0.0.0.0")
DURABILITY = os.environ.get("DURABILITY", "sqlite")
RETRY_MAX_ATTEMPTS = int(os.environ.get("RETRY_MAX_ATTEMPTS", "5"))
RETRY_BASE = float(os.environ.get("RETRY_BASE", "0.2"))
RETRY_CAP_EXPONENT = int(os.environ.get("RETRY_CAP_EXPONENT", "4"))


async def run() -> None:
    if DURABILITY == "sqlite":
        backend = await SQLiteDurability.connect(config.PUBSUB_DB)
    elif DURABILITY in ("none", "null", "off"):
        from testclient.null_durability import NullDurability

        backend = NullDurability()
    else:
        backend = InMemoryDurability()

    policy = RetryPolicy(
        max_attempts=RETRY_MAX_ATTEMPTS,
        base=RETRY_BASE,
        cap_exponent=RETRY_CAP_EXPONENT,
    )

    observer = None
    if config.OTEL_ENABLED:
        from pubsub.observability.otel import OTelObserver
        from testclient.otel import setup_meter

        observer = OTelObserver(setup_meter())

    broker = Broker(backend, retry_policy=policy, observer=observer)
    server = BrokerServer(broker, host=BIND_HOST, port=config.PUBSUB_PORT)
    await server.start()
    import asyncio

    loop_impl = type(asyncio.get_running_loop()).__module__.split(".")[0]
    log.info(
        "broker on %s:%d durability=%s loop=%s retry(max=%d base=%.2gs cap_exp=%d)",
        BIND_HOST,
        server.port,
        DURABILITY,
        loop_impl,
        RETRY_MAX_ATTEMPTS,
        RETRY_BASE,
        RETRY_CAP_EXPONENT,
    )
    try:
        await server.serve_forever()
    finally:
        await server.close()
        await broker.close()


def main() -> None:
    try:
        config.run_async(run())
    except KeyboardInterrupt:
        log.info("controller stopping")


if __name__ == "__main__":
    main()
