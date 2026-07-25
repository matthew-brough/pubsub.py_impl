"""Postgres durability backend — the shared durable layer for every broker.

Implements the library's :class:`DurabilityBackend` ABC over ``asyncpg`` so all
brokers in the distributed harness persist messages, acks, and DLQ entries into
one Postgres instance. Payloads and extras are MessagePack-encoded ``bytea``
blobs (same wire shape as the SQLite backend). The ``replayable`` flag is
denormalised onto each stored message so ``read_from`` filters without a join.

Throughput model — deliberately NOT a copy of the SQLite backend's:
  * SQLite has a single writer, so its group-commit funnels every append through
    one connection. Postgres commits concurrently across backends and coalesces
    their fsyncs at the WAL layer (group commit), so we run a *bounded pool of
    concurrent commit workers* (``PG_MAX_WRITERS``) instead of one — appends
    still batch to amortise round-trips, but batches commit in parallel.
  * Reads (``read_from``/``last_acked``/``read_dlq``) use the pool directly and
    are never queued behind writes.
  * ``synchronous_commit`` is tunable (``PG_SYNCHRONOUS_COMMIT``): ``on`` keeps
    the strict publish-after-durable contract; ``off`` trades a small crash
    window for a much higher publish ceiling when finding limits.

Each waiter still resolves only after its batch commits, preserving the
publish-after-durable contract (at the configured ``synchronous_commit`` level).

Cross-broker note: ``message_id`` is a broker-assigned UUID and unique across
brokers; because each published message lives on exactly one broker's island,
there is no cross-broker duplicate insert. ``subscription_id`` is broker-local,
so the shared ``acks`` table keys reconnect-replay per subscription id as the ABC
specifies — adequate for the harness; ids are not namespaced by broker.
"""

import asyncio
import os
import socket
from typing import Self, cast

import asyncpg
import msgpack

from pubsub.server.durability.abc import DurabilityBackend
from pubsub.shared.types import DLQEntry, Message, MessagePackValue

# Tuning knobs (Postgres-specific; no SQLite equivalent).
_MAX_WRITERS = max(1, int(os.environ.get("PG_MAX_WRITERS", "4")))
_POOL_MAX = int(os.environ.get("PG_POOL_MAX", "24"))
_POOL_MIN = int(os.environ.get("PG_POOL_MIN", "4"))
_SYNC_COMMIT = os.environ.get("PG_SYNCHRONOUS_COMMIT", "on")

# Audit attribution: the broker identity stamped onto every row this backend
# writes. In an anonymous broker pool (no fixed names) the shared durable layer
# thus doubles as an audit trail — "which machine persisted / handled this
# message" and "which machine dead-lettered it" — answerable by broker_id even
# though brokers are otherwise fungible.
_BROKER_ID = os.environ.get("INSTANCE_ID") or socket.gethostname()

_SCHEMA = """
CREATE TABLE IF NOT EXISTS topics (
    topic      TEXT PRIMARY KEY,
    replayable BOOLEAN NOT NULL
);
CREATE TABLE IF NOT EXISTS messages (
    seq        BIGSERIAL PRIMARY KEY,
    message_id TEXT NOT NULL,
    topic      TEXT NOT NULL,
    payload    BYTEA NOT NULL,
    extras     BYTEA NOT NULL,
    created_at DOUBLE PRECISION NOT NULL,
    replayable BOOLEAN NOT NULL,
    broker_id  TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_messages_replay
    ON messages (replayable, created_at, seq);
CREATE INDEX IF NOT EXISTS idx_messages_broker ON messages (broker_id);
CREATE TABLE IF NOT EXISTS acks (
    subscription_id TEXT PRIMARY KEY,
    message_id      TEXT NOT NULL,
    broker_id       TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS dlq (
    seq             BIGSERIAL PRIMARY KEY,
    subscription_id TEXT NOT NULL,
    attempts        INTEGER NOT NULL,
    message         BYTEA NOT NULL,
    broker_id       TEXT NOT NULL DEFAULT ''
);
"""

_INSERT_MESSAGE = (
    "INSERT INTO messages "
    "(message_id, topic, payload, extras, created_at, replayable, broker_id) "
    "VALUES ($1, $2, $3, $4, $5, "
    "COALESCE((SELECT replayable FROM topics WHERE topic=$6), false), $7)"
)


def _pack(value: object) -> bytes:
    return cast(bytes, msgpack.packb(value, use_bin_type=True))


def _unpack(blob: bytes) -> MessagePackValue:
    return cast(MessagePackValue, msgpack.unpackb(bytes(blob), raw=False))


def _row_to_message(row: asyncpg.Record) -> Message[MessagePackValue]:
    return Message(
        message_id=row["message_id"],
        topic=row["topic"],
        payload=_unpack(row["payload"]),
        extras=cast("dict[str, MessagePackValue]", _unpack(row["extras"])),
        created_at=row["created_at"],
    )


class PostgresDurability(DurabilityBackend):
    """Durable backend over an ``asyncpg`` pool. Construct via ``connect``."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool
        self._append_queue: list[
            tuple[tuple[object, ...], asyncio.Future[None]]
        ] = []
        # Up to _MAX_WRITERS drain workers commit batches in parallel; Postgres
        # coalesces their fsyncs (WAL group commit) rather than serialising them.
        self._writers: set[asyncio.Task[None]] = set()
        self._closed = False

    @classmethod
    async def connect(cls, dsn: str) -> Self:
        # Retry: a broker can win the race against Postgres accepting connections
        # even with a compose healthcheck, and initial DNS may lag.
        last: Exception | None = None
        pool = None
        for _ in range(60):
            try:
                pool = await asyncpg.create_pool(
                    dsn,
                    min_size=_POOL_MIN,
                    max_size=_POOL_MAX,
                    # Applied to every pooled connection so writers and readers
                    # inherit the configured durability/latency trade-off.
                    server_settings={"synchronous_commit": _SYNC_COMMIT},
                )
                break
            except (OSError, asyncpg.PostgresError) as exc:  # noqa: PERF203
                last = exc
                await asyncio.sleep(1.0)
        if pool is None:  # pragma: no cover - only on a truly unreachable DB
            raise RuntimeError(f"postgres unreachable: {last}")
        async with pool.acquire() as conn:
            await conn.execute(_SCHEMA)
        return cls(pool)

    async def register_topic(self, topic: str, *, replayable: bool) -> None:
        await self._pool.execute(
            "INSERT INTO topics (topic, replayable) VALUES ($1, $2) "
            "ON CONFLICT (topic) DO UPDATE SET replayable=EXCLUDED.replayable",
            topic,
            replayable,
        )

    async def append(self, message: Message[MessagePackValue]) -> None:
        params = (
            message.message_id,
            message.topic,
            _pack(message.payload),
            _pack(dict(message.extras)),
            message.created_at,
            message.topic,
            _BROKER_ID,  # audit: which broker persisted this row
        )
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[None] = loop.create_future()
        self._append_queue.append((params, fut))
        # Spin up another concurrent committer while work is queued and we are
        # under the writer cap. Idle workers exit, so the pool self-sizes to the
        # arrival rate (one worker at low load, up to _MAX_WRITERS under flood).
        if not self._closed and self._append_queue and len(self._writers) < _MAX_WRITERS:
            task = loop.create_task(self._drain_appends())
            self._writers.add(task)
            task.add_done_callback(self._writers.discard)
        await fut

    async def _drain_appends(self) -> None:
        """Commit queued appends in batches; multiple of these run in parallel.

        Each iteration atomically claims whatever is queued (the swap has no
        ``await`` between read and reset, so two workers never claim the same
        rows on the single event loop) and commits it on its own pooled
        connection. While one worker's commit is in flight, newly-arrived appends
        pile up and a sibling worker claims them — so commits overlap instead of
        serialising behind a single writer.
        """
        while self._append_queue:
            batch = self._append_queue
            self._append_queue = []
            try:
                async with self._pool.acquire() as conn:
                    async with conn.transaction():
                        await conn.executemany(
                            _INSERT_MESSAGE, [p for p, _ in batch]
                        )
            except BaseException as exc:  # noqa: BLE001 - relay to every waiter
                for _, fut in batch:
                    if not fut.done():
                        fut.set_exception(exc)
                continue
            for _, fut in batch:
                if not fut.done():
                    fut.set_result(None)

    async def read_from(self, timestamp: float) -> list[Message[MessagePackValue]]:
        rows = await self._pool.fetch(
            "SELECT message_id, topic, payload, extras, created_at FROM messages "
            "WHERE replayable=true AND created_at>=$1 "
            "ORDER BY created_at ASC, seq ASC",
            timestamp,
        )
        return [_row_to_message(row) for row in rows]

    async def record_ack(self, subscription_id: str, message_id: str) -> None:
        await self._pool.execute(
            "INSERT INTO acks (subscription_id, message_id, broker_id) VALUES ($1, $2, $3) "
            "ON CONFLICT (subscription_id) DO UPDATE SET "
            "message_id=EXCLUDED.message_id, broker_id=EXCLUDED.broker_id",
            subscription_id,
            message_id,
            _BROKER_ID,  # audit: which broker owns this subscription's offset
        )

    async def last_acked(self, subscription_id: str) -> str | None:
        return await self._pool.fetchval(
            "SELECT message_id FROM acks WHERE subscription_id=$1", subscription_id
        )

    async def to_dlq(self, entry: DLQEntry) -> None:
        await self._pool.execute(
            "INSERT INTO dlq (subscription_id, attempts, message, broker_id) "
            "VALUES ($1, $2, $3, $4)",
            entry.subscription_id,
            entry.attempts,
            _pack(
                {
                    "message_id": entry.message.message_id,
                    "topic": entry.message.topic,
                    "payload": entry.message.payload,
                    "extras": dict(entry.message.extras),
                    "created_at": entry.message.created_at,
                }
            ),
            _BROKER_ID,  # audit: which broker dead-lettered this delivery
        )

    async def read_dlq(self) -> list[DLQEntry]:
        rows = await self._pool.fetch(
            "SELECT subscription_id, attempts, message FROM dlq ORDER BY seq ASC"
        )
        entries: list[DLQEntry] = []
        for row in rows:
            data = cast("dict[str, MessagePackValue]", _unpack(row["message"]))
            message: Message[MessagePackValue] = Message(
                message_id=cast(str, data["message_id"]),
                topic=cast(str, data["topic"]),
                payload=data["payload"],
                extras=cast("dict[str, MessagePackValue]", data["extras"]),
                created_at=cast(float, data["created_at"]),
            )
            entries.append(
                DLQEntry(
                    message=message,
                    subscription_id=row["subscription_id"],
                    attempts=row["attempts"],
                )
            )
        return entries

    # --- observer helpers (cheap counts straight off the DB) --------------
    async def counts(self) -> dict:
        """DLQ + retained-history counts for the observation dashboard."""
        out = {"dlq_count": 0, "history_count": 0, "topics": []}
        try:
            out["dlq_count"] = await self._pool.fetchval("SELECT COUNT(*) FROM dlq")
            out["history_count"] = await self._pool.fetchval(
                "SELECT COUNT(*) FROM messages WHERE replayable=true"
            )
            rows = await self._pool.fetch(
                "SELECT topic, replayable FROM topics ORDER BY topic"
            )
            out["topics"] = [
                {"topic": r["topic"], "replayable": bool(r["replayable"])}
                for r in rows
            ]
        except (OSError, asyncpg.PostgresError):
            pass
        return out

    async def audit_by_broker(self) -> list[dict]:
        """Audit trail: rows persisted + dead-lettered per broker_id.

        Answers "which machine handled this traffic" over an otherwise anonymous
        broker pool. In the egress (replicated-ingest) topology every broker
        persists every message it handles, so this shows each broker's real
        share of the work; in the ingress (sharded) topology it shows how the
        load balancer spread ingest across brokers.
        """
        out: list[dict] = []
        try:
            msg_rows = await self._pool.fetch(
                "SELECT COALESCE(NULLIF(broker_id,''),'?') AS broker_id, "
                "COUNT(*) AS n FROM messages GROUP BY 1 ORDER BY n DESC"
            )
            dlq_rows = await self._pool.fetch(
                "SELECT COALESCE(NULLIF(broker_id,''),'?') AS broker_id, "
                "COUNT(*) AS n FROM dlq GROUP BY 1"
            )
            dlq = {r["broker_id"]: int(r["n"]) for r in dlq_rows}
            for r in msg_rows:
                bid = r["broker_id"]
                out.append(
                    {"broker_id": bid, "persisted": int(r["n"]), "dlq": dlq.get(bid, 0)}
                )
        except (OSError, asyncpg.PostgresError):
            pass
        return out

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        # Let in-flight committers finish, then fail anything still queued so its
        # publishers don't hang on a future that will never drain.
        for task in list(self._writers):
            try:
                await task
            except BaseException:  # noqa: BLE001 - waiters already got the error
                pass
        pending, self._append_queue = self._append_queue, []
        for _, fut in pending:
            if not fut.done():
                fut.set_exception(RuntimeError("durability backend closed"))
        await self._pool.close()
