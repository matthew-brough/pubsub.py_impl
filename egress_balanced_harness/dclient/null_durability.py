"""No-op durability backend — the "no durability" mode.

Distinct from :class:`InMemoryDurability`, which still retains replayable-topic
history in an unbounded list and tracks per-subscription acks. This backend
stores *nothing*: ``append`` drops every message, there is no replay, no ack
tracking, and no DLQ retention. It isolates the broker's pure transport + fanout
cost from all storage bookkeeping, and is the lightest-possible backend for
throughput benchmarking.

Semantics: publish still succeeds (``append`` returns immediately). Replay reads
return empty. ``last_acked`` is always ``None`` (every reconnect replays from the
subscription's requested start, since nothing is remembered). DLQ is a black hole.

In the distributed harnesses this backend is per-broker (no shared state), so
there is no cross-broker audit trail — use ``DURABILITY=postgres`` for that.
"""

from pubsub.server.durability.abc import DurabilityBackend
from pubsub.shared.types import DLQEntry, Message, MessagePackValue


class NullDurability(DurabilityBackend):
    """Drops everything. No history, no acks, no DLQ."""

    async def register_topic(self, topic: str, *, replayable: bool) -> None:
        return None

    async def append(self, message: Message[MessagePackValue]) -> None:
        return None

    async def read_from(self, timestamp: float) -> list[Message[MessagePackValue]]:
        return []

    async def record_ack(self, subscription_id: str, message_id: str) -> None:
        return None

    async def last_acked(self, subscription_id: str) -> str | None:
        return None

    async def to_dlq(self, entry: DLQEntry) -> None:
        return None

    async def read_dlq(self) -> list[DLQEntry]:
        return []

    async def close(self) -> None:
        return None
