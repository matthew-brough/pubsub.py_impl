# pubsub.py — egress-balanced harness

A multi-broker deployment tuned for the **few-producers / giant-consumers**
fan-out workload: producers **broadcast** each message to every broker
(replicated ingest), and **nginx balances the consumer egress**, sharding a large
consumer fleet across the broker pool. Over a **shared Postgres durable layer +
audit trail**. All dockerized; every tier scales with `--scale`.

Default topology **2 producers : 2 brokers : 6 consumers : 1 observer** — the
consumer count is the axis you grow.

```
                 ┌──── shared Postgres (durable + audit: broker_id per row) ────┐
                 │            ▲            ▲            ▲                          │
   producers ──broadcast──▶  broker × N  (every broker holds the FULL stream)    │
   (few, x2)   to ALL brokers  :8765       each appends what it handles ─────────┘
                                  │  │  │
   consumers ── nginx egress LB (least_conn) shards Z consumers across Y brokers ─┐
   (giant, x6)     each consumer: ONE connection -> one broker (sees everything)  │
   observer ── firehose from ONE broker (via nginx) + fleet fan-in + scrape all ──┘
   (x1, :8080 dashboard)
```

## Why this topology (vs the ingress harness)

`pubsub.py` fans out live messages only to subscribers on the **same** broker.
Two ways to scale that across brokers:

| | `../ingress_balanced_harness` | **this (egress)** |
|---|---|---|
| Ingest | **sharded** — nginx LBs producers; each msg on one broker | **replicated** — producers broadcast to every broker |
| Consumers | **fan in** — one sub per broker, N connections each | **sharded** — one connection via egress LB |
| nginx fronts | producers (ingress) | consumers (egress) |
| Connections / consumer | Y | **1** |
| Connections / broker | all M consumers | **~M/Y** |
| Best for | N brokers sharing a durable log; every consumer needs the global stream (analytics) | **few producers, giant consumers** (fan-out scaling) |

For giant consumer counts M, connection load is the wall. Fan-in makes it **M×Y**;
egress sharding makes it **M/Y per broker, 1 per consumer** — so this is the valid
test for scaling fan-out. Replicated ingest is what makes an egress LB correct: a
consumer pinned to any one broker still sees everything.

**Cost:** every broker persists every message it handles → Y× writes in shared
Postgres. That is turned into a **feature** — the audit trail (below).

## Shared Postgres = durable audit trail

Every row the durability backend writes is stamped with `broker_id` (the handling
broker's identity). With replicated ingest, each broker independently persists
the messages it fanned out, so the `messages` table becomes a per-broker record
of **which (otherwise anonymous) machine handled which traffic** — surfaced on the
dashboard as *rows persisted / dead-lettered per broker*. Delivery still works
without shared Postgres here (each broker is self-sufficient); the shared DB earns
its keep as the cluster-wide **audit + observability** layer.

`broker_id` also lands on `acks` (which broker owns a subscription's offset) and
`dlq` (which broker dead-lettered a delivery).

## Run

```bash
docker compose up --build          # then open http://localhost:8080
```

Grow the consumer fleet and shard it over more brokers (the whole point):

```bash
docker compose up --build --scale broker=4 --scale consumer=24
```

nginx and the producer broadcaster pick up new/departed brokers within
`RERESOLVE_SECONDS` (10s) — no restart. Push the ceiling:

```bash
PRODUCER_RATE=max PG_SYNCHRONOUS_COMMIT=off docker compose up --build
```

Every service authenticates with `StaticAuthenticator`; `AUTH_TOKEN` and
`AUTH_IDENTITY` default to `pubsub-harness`. Each broker startup rejects an
invalid token, then verifies valid auth, durable topic claim/release, and a
delivery round trip through packed transport. Producers claim every replicated
data topic on each broker before entering load. Each broker also repeats the
probe on an ephemeral TLS 1.2+ listener using a trusted self-signed test
certificate; measured cluster traffic stays plaintext to isolate transport
changes from encryption cost.

## Services

| Service | Count | Role |
|---|---|---|
| `postgres` | 1 | Shared durable layer + `broker_id` audit trail. |
| `broker` | Y (default 2, `--scale broker=N`) | Elastic replicas; each holds the full stream. |
| `nginx` | 1 | L4 EGRESS LB for **consumers**; dynamic `resolve` + `least_conn`. |
| `producer` | X (default 2) | Broadcaster — mirrors every message to all brokers. |
| `consumer` | Z (default 6, `--scale consumer=N`) | Sharded sink — one connection via nginx. |
| `observer` | 1 | Firehose from one broker + fleet fan-in + scrape all + read Postgres; `:8080`. |

## Key env knobs

| Var | Default | Effect |
|---|---|---|
| `PRODUCER_RATE` | `2000` | Total *logical* msg/s per producer, or `max`. |
| `PRODUCER_CONCURRENCY` | `4` | Publish tasks per producer. |
| `PAYLOAD_BYTES` | `256` | Message payload size. |
| `NACK_RATE` | `0.02` | Transient nack fraction. |
| `POISON_EVERY` | `500` | Every Nth seq → always-nack → DLQ. |
| `AUTH_TOKEN` | `pubsub-harness` | Shared benchmark credential; override for a run. |
| `AUTH_IDENTITY` | `pubsub-harness` | Shared claim owner across replicated brokers. |
| `PG_SYNCHRONOUS_COMMIT` | `on` | `off` trades a crash window for a higher ceiling. |
| `PG_MAX_WRITERS` | `4` | Concurrent Postgres commit workers per broker. |
| `RERESOLVE_SECONDS` | `10` | Broadcast / fleet / metrics re-resolve interval. |

## Layout

```
egress_balanced_harness/
├── docker-compose.yml          # 2:2:6:1 default; every tier --scale-able
├── nginx/nginx.conf            # TCP EGRESS LB: dynamic resolve + least_conn
├── Dockerfile
├── pyproject.toml
└── dclient/
    ├── config.py               # env config; resolve_broker_ips() + fan_in()
    ├── postgres_durability.py  # PostgresDurability + broker_id audit trail
    ├── controller.py           # broker entrypoint (Postgres-backed)
    ├── producer.py             # BROADCASTER — replicated ingest to all brokers
    ├── consumer.py             # sharded sink — single connection via egress LB
    ├── otel.py
    └── observer/
        ├── app.py              # firehose(one) + fleet fan-in + scrape all + PG
        ├── stats.py
        ├── brokerotel.py
        └── dashboard.py
```

The complementary single-broker throughput harness is in `../performance_harness/`.
