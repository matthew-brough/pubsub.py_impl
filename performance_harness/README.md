# pubsub.py throughput harness

A multi-container test client that drives the [`pubsub.py`](https://github.com/matthew-brough/pubsub.py)
broker over its real TCP transport and **measures throughput**, with a rich
web analytics sidecar.

```
producer ──publish──▶ controller (broker, TCP :8765) ◀──subscribe── consumer
                          │  sqlite /data/pubsub.db (WAL)
 management ──subscribe('>')──┘  reads the same db (DLQ/history)
             serves dashboard ──▶ http://localhost:8080
```

| service | role |
|---|---|
| **controller** | hosts the `Broker` over TCP (`testclient/controller.py`); env-tunable retry policy; SQLite durability on a shared volume |
| **producer** | load generator; stamps each message with `seq` + `pub_ts`; self-reports achieved publish rate |
| **consumer** | fast-acking sink; small random nack + a poison fraction to exercise retry → DLQ; survives eviction by re-subscribing |
| **management** | FastAPI analytics sidecar; subscribes to `>` to measure delivery throughput + end-to-end latency, and reads the controller's SQLite DB for DLQ + retained history |

## Run

```bash
docker compose up --build
# open http://localhost:8080
```

The dashboard shows delivered/sec (current / sustained / peak), latency
percentiles (p50/p95/p99), a msg/s sparkline, per-topic throughput, the DLQ,
retained-history counts, and a **live fleet panel** (per-producer and
per-consumer rates), updating ~1/s over SSE.

### Fleet observability (multiple producers / consumers)
Each producer and consumer heartbeats its own stats through the broker on
`_stats.<kind>.<instance>`; the sidecar collects them on a **dedicated
`_stats.>` subscription** (separate from the `>` firehose, so sparse heartbeats
are never lost when the throughput sub is busy). Consumers publish heartbeats on
a **separate connection** so telemetry isn't starved by their delivery load.

The panel shows, per instance: publish rate / ack rate / nack rate, totals,
evictions, and age — plus fleet aggregates (Σ publish/s vs Σ ack/s). Scale with:

```bash
docker compose up -d --scale producer=4 --scale consumer=3
```

Because delivery fans out per subscription, **Σ ack/s ≈ (Σ publish/s) × n_consumers**
— scaling consumers measures fanout cost, not shared work (this broker has no
consumer groups).

### Broker-side OpenTelemetry
The controller wires the library's `OTelObserver` (`pubsub-py[otel]`) into the
`Broker` and configures an OTel SDK + **Prometheus exporter**, serving broker
counters at `http://localhost:9464/metrics`:
`pubsub_publishes_total{accepted,topic}`, `pubsub_deliveries_total`,
`pubsub_acks_total`, `pubsub_nacks_total`, `pubsub_retry_exhausted_total`.

These are **broker-authoritative** — they include what the client-side sidecar
can't see: rejected publishes and true dead-letter (retry-exhausted) counts. The
sidecar scrapes `/metrics` and shows them in a dedicated portal panel next to its
client-side numbers; e.g. `pubsub_retry_exhausted_total` cross-checks the SQLite
DLQ count. Scrape it directly (`curl localhost:9464/metrics`) or point a real
Prometheus/Grafana or OTel collector at it. Disable with `OTEL_ENABLED=0`.

> ⚠ **Saturation knee.** Aggregate publish is request/reply through a single
> controller event loop and plateaus around a few hundred msg/s. Past that knee,
> **one producer monopolises the loop**: it keeps publishing (deliveries flow,
> broker-internal), while every other connection's inbound processing is starved
> — the other producers publish ~0, and consumer acks/heartbeats, though *sent*,
> are never *processed* by the broker. So the client-side fleet panel undercounts
> (e.g. shows 1 producer / 0 consumers even with 8 + 4 running and acking).
>
> The portal detects this from **broker-side OTel** (the one reliable source
> under saturation): `deliveries/s > 0` while `acks+nacks/s ≈ 0` is the
> fingerprint, and it raises a **red "broker saturated" banner** explaining the
> undercount and naming the ceiling. Keep producer count / per-producer rate
> below the knee so instances coexist (the fleet panel is accurate there); push
> past it deliberately to watch the ceiling and the banner fire.

### Cross-check the throughput number
The sidecar measures *delivery* rate on its own `>` subscription. The producer
logs its *achieved publish* rate independently — the ground truth:

```bash
docker compose logs -f producer     # "achieved=NNN msg/s"
curl -s localhost:8080/api/stats     # scriptable snapshot
```

Equal rates ⇒ broker keeps up. If the sidecar reads lower **and** shows an
eviction warning, the measurement ceiling was hit — trust the producer number.

### Find the ceiling
```bash
PRODUCER_RATE=max PRODUCER_CONCURRENCY=16 docker compose up --build
# or scale producers:
docker compose up -d --scale producer=4
```
Publishing is request/reply, so a single client connection is RTT-bound (~hundreds
of msg/s locally); throughput scales with `PRODUCER_CONCURRENCY` and multiple
producer containers until the broker plateaus.

### Clean retry / DLQ demo
Dead-lettering is clearest at **low** load — under saturation the broker's bounded
delivery queue (128) and full-jitter retry dominate, and redeliveries become
sparse (itself a finding). To watch the DLQ fill quickly:

```bash
PRODUCER_RATE=100 PRODUCER_CONCURRENCY=1 POISON_EVERY=50 docker compose up --build
```

## Configuration (env)

| var | default | meaning |
|---|---|---|
| `PRODUCER_RATE` | `1200` | total target msg/s, or `max` (unthrottled) |
| `PRODUCER_CONCURRENCY` | `4` | concurrent publish tasks |
| `PAYLOAD_BYTES` | `256` | payload size |
| `NACK_RATE` | `0.05` | fraction of messages transiently nacked (→ retry) |
| `POISON_EVERY` | `200` | every Nth message is always nacked (→ DLQ); `0` disables |
| `RETRY_MAX_ATTEMPTS` / `RETRY_BASE` / `RETRY_CAP_EXPONENT` | `5` / `0.1` / `2` | controller retry policy (backoff to DLQ) |
| `DURABILITY` | `sqlite` | `sqlite` or `memory` |
| `OTEL_ENABLED` | `1` | controller emits broker-side OTel metrics on `OTEL_PROM_PORT` |
| `OTEL_PROM_PORT` | `9464` | Prometheus `/metrics` port on the controller |
| `INSTANCE_ID` | container hostname | fleet id shown in the portal (unique per replica) |
| `HEARTBEAT_EVERY` | `2.0` | seconds between fleet heartbeats |
| `STATS_TTL` | `8.0` | sidecar drops an instance from the fleet after this many seconds silent |
| `RAMP` / `RAMP_HOLD` | — | optional stepped rate ramp, e.g. `RAMP=500,1000,2000` |

## Run locally without docker

```bash
uv sync --group testclient
DB=/tmp/pubsub.db
BIND_HOST=127.0.0.1 PUBSUB_DB=$DB .venv/bin/python -m testclient.controller &
PUBSUB_HOST=127.0.0.1 PUBSUB_DB=$DB .venv/bin/python -m testclient.consumer &
PUBSUB_HOST=127.0.0.1 .venv/bin/python -m testclient.producer &
PUBSUB_HOST=127.0.0.1 PUBSUB_DB=$DB .venv/bin/uvicorn testclient.management.app:app --port 8080
```
