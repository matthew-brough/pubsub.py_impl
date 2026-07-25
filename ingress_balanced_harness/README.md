# pubsub.py — ingress-balanced harness

A multi-broker, horizontally-scaled deployment of the [`pubsub.py`](https://github.com/matthew-brough/pubsub.py)
broker where **nginx balances the producer ingress** and consumers **fan in** to
reconstruct the global stream: **X producers : Y brokers : Z consumers : 1
observation platform**, over a **shared Postgres durable layer + audit trail**.
All dockerized. **Every tier — including brokers — scales elastically with
`--scale`**; there are no hard-coded broker names.

> **Two harnesses, two questions.** This one *shards ingest* (each message lands
> on one broker via the ingress LB) and has subscribers *fan in* — the right
> test for "N brokers share a durable log while every consumer rebuilds the
> global stream." For the **few-producers / giant-consumers** fan-out-scaling
> workload, see the sibling **`../egress_balanced_harness/`**, which *replicates
> ingest* and *shards consumers* behind an egress LB.

Default topology is **3:2:3:1**.

```
                    ┌──────────── shared Postgres (history / acks / DLQ) ───────────┐
                    │                        ▲    ▲    ▲                             │
   producers ──▶ nginx (L4, discover+   ──▶  broker × N   (elastic; --scale broker=N)│
   (x3, LB'd)    least_conn over replicas)    :8765                                  │
                    ▲ re-resolves broker DNS    ▲ ▲ ▲                                │
   consumers ─── fan-in: DNS-resolve every replica, one sub each, re-resolve ────────┘
   (x3)
   observer ──── fan-in subs + scrape every replica's /metrics + read Postgres
   (x1, :8080 dashboard)
```

Both the nginx ingress and the fan-in subscribers **DNS-discover the `broker`
service and re-resolve on an interval**, so `--scale broker=N` is followed live.

## The delivery model (important)

`pubsub.py` brokers fan out **live** messages only to subscribers connected to
the *same* broker. The durability layer (here, Postgres) is persistence + replay
+ DLQ + acks — **not** a live message bus. So a naive "N brokers behind one
round-robin LB" does **not** give every consumer every message.

This harness solves that at the topology level, **without touching library
internals**:

- **Producers are load-balanced** through the nginx ingress. nginx uses OSS
  dynamic upstream resolution (`resolver` + `server broker:8765 resolve` +
  `zone`) to keep the upstream in sync with the live replica set, and
  `least_conn` for **true** balancing across it (not the dumb per-connection DNS
  round-robin a bare `proxy_pass` variable gives). Each producer holds one
  connection; every published message lands on **exactly one** broker's island.
- **Consumers and the observer fan *in***: each DNS-resolves the `broker` service
  to **every** replica IP and opens one subscription set per replica,
  re-resolving every `RERESOLVE_SECONDS` to attach to new replicas and drop
  departed ones. Because each message lives on exactly one broker, the union of
  all brokers is the whole stream — **every event delivered exactly once**, no
  dedup, no relay.

### Why brokers scale with `--scale` now (they used to be hard-named)

Brokers are *servers you must address individually* (fan-in must reach each one;
nginx must balance across each one), whereas producers/consumers are anonymous
clients that only dial out. The first cut hard-named `broker1`, `broker2`, … for
that reason. This version removes the naming by making **both** sides discover
the replica set dynamically: nginx via its `resolve` upstream, the fan-in
subscribers via `getaddrinfo` + periodic re-resolve. So brokers are now as
elastic as every other tier — `--scale broker=N`, no generator, no restart.

> Why not put an nginx in front of the consumers too? A round-robin **egress**
> LB would pin each consumer to a single broker, so it would miss every message
> published to the other brokers. Fan-in is the correct dual of ingress LB here.
> (If you truly need cross-broker live fanout on a *single* connection, that
> requires a Postgres `LISTEN/NOTIFY` relay that re-injects into each broker —
> a real cluster bus, out of scope for this harness.)

## Shared Postgres durable layer

Every broker uses `dclient.postgres_durability.PostgresDurability`, a from-scratch
implementation of the library's `DurabilityBackend` ABC over `asyncpg` (the
library ships only in-memory + SQLite). One Postgres instance is the single
source of truth for retained history, per-subscription acks, and the DLQ across
the whole cluster — which is also what makes the observer's DLQ/history view
cluster-wide.

It is **not** a copy of the SQLite backend's assumptions:

| Concern | SQLite backend | This Postgres backend |
|---|---|---|
| Writers | single writer, group-commit funnel | **pool of concurrent commit workers** (`PG_MAX_WRITERS`); Postgres coalesces their fsyncs (WAL group commit) |
| Durability/latency | fixed per-commit fsync | tunable `synchronous_commit` (`PG_SYNCHRONOUS_COMMIT=on|off`) |
| Reads | share the one connection | pooled, never queued behind writes |

Appends still batch (to amortise round-trips), but batches commit in parallel so
we don't serialise the whole cluster's writes behind one connection.

## Run

```bash
docker compose up --build          # then open http://localhost:8080
```

Scale **any** tier at runtime — brokers included:

```bash
docker compose up --build --scale broker=4 --scale producer=6 --scale consumer=6
```

nginx and the fan-in subscribers pick up the new/departed broker replicas within
`RERESOLVE_SECONDS` (default 10s) — no restart, no config regen.

Push toward the ceiling:

```bash
PRODUCER_RATE=max PG_SYNCHRONOUS_COMMIT=off docker compose up --build
```

## Services

| Service | Count | Role |
|---|---|---|
| `postgres` | 1 | Shared durable layer (history / acks / DLQ). |
| `broker` | Y (default 2, `--scale broker=N`) | Elastic broker replicas on shared Postgres; OTel `/metrics` on `:9464`. |
| `nginx` | 1 | L4 (`stream`) LB for **producer** → broker; dynamic `resolve` + `least_conn`. |
| `producer` | X (default 3) | Load generator; connects via nginx; self-reports achieved publish rate. |
| `consumer` | Z (default 3) | Fan-in sink; subscribes on every broker; small nack + poison fraction → DLQ. |
| `observer` | 1 | Observation platform: fan-in `>`/`_stats.>` + scrape every broker's OTel + read Postgres; live dashboard at `:8080`. |

## Key env knobs

| Var | Default | Effect |
|---|---|---|
| `PRODUCER_RATE` | `2000` | Total msg/s per producer, or `max`. |
| `PRODUCER_CONCURRENCY` | `4` | Publish tasks per producer. |
| `PAYLOAD_BYTES` | `256` | Message payload size. |
| `NACK_RATE` | `0.02` | Transient nack fraction (exercises retry). |
| `POISON_EVERY` | `500` | Every Nth seq → always-nack → DLQ. |
| `PG_SYNCHRONOUS_COMMIT` | `on` | `off` trades a crash window for a higher ceiling. |
| `PG_MAX_WRITERS` | `4` | Concurrent Postgres commit workers per broker. |
| `PG_POOL_MAX` | `24` | Max asyncpg pool size per broker. |
| `BROKER_SERVICE` | `broker` | Compose service name fan-in DNS-discovers (unset → static `BROKERS`). |
| `RERESOLVE_SECONDS` | `10` | Fan-in / metrics re-resolve interval (follows `--scale`). |

## Layout

```
distributed_harness/
├── docker-compose.yml          # 3:2:3:1 default; every tier --scale-able
├── nginx/nginx.conf            # TCP stream LB: dynamic resolve + least_conn
├── Dockerfile                  # shared image for broker/producer/consumer/observer
├── pyproject.toml
└── dclient/
    ├── config.py               # env config; resolve_broker_ips() + fan_in() manager
    ├── postgres_durability.py  # PostgresDurability(DurabilityBackend)
    ├── controller.py           # broker entrypoint (Postgres-backed)
    ├── producer.py             # ingress-LB'd load generator
    ├── consumer.py             # fan-in sink
    ├── otel.py                 # per-broker Prometheus exporter
    └── observer/
        ├── app.py              # FastAPI observation platform
        ├── stats.py            # in-memory throughput aggregator
        ├── brokerotel.py       # scrape + parse broker OTel
        └── dashboard.py        # single-file live dashboard
```

The single-node throughput harness this grew out of lives in
`../performance_harness/`.
```
