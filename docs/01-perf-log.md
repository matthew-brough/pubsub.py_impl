# pubsub.py — Performance Log

> Raw appendix — the run-by-run record behind the library README's Performance summary. Superseded by it
> for conclusions; kept for method and per-regime evidence. Absolute rates are
> host-bound; read them as shape, not spec.

**Date:** 2026-07-22
**Environment:** WSL2 (Linux 6.6), Docker Compose, single-host loopback.
**Broker config:** `DURABILITY=memory` **and** `DURABILITY=sqlite` (both swept — see §5),
single-threaded asyncio loop, TCP `:8765`,
`RETRY_MAX_ATTEMPTS=5 RETRY_BASE=0.1 RETRY_CAP_EXPONENT=2`.
**Fleet:** 1–8 producers (4 workers each), 4 consumers, 1 analytics sidecar (`>` firehose).
**Method:** producer scale sweep (`--scale producer=N`), 12 s settle + 16 s sample.
Rates are **broker-side OpenTelemetry** (authoritative) unless marked *self* (client heartbeat).

---

## 1. Key finding

The system ceiling is the **publish accept path, not delivery.**

- `BrokerClient.publish()` is a **synchronous request/reply** round-trip: it awaits a
  per-`rid` Future resolved by the broker's reply (`pubsub/transport/client.py:117,290`).
- `ack`/`nack` are **fire-and-forget** (`_raw_send`, no reply — `client.py:330`). This is
  why client-reported `ack_rate` is fiction under load: the client *emits* acks fast, but
  the broker can't *ingest* them, and the client never learns.
- Delivery/fanout is **not** the bottleneck: the firehose burst-peaked **2878 msg/s** to a
  single connection while publish-accept sat at ~60–88/s.

So: the broker delivers fast but *accepts* publishes slowly, because every publish reply is
serialized through the one broker event loop, and reply latency balloons under contention.

---

## 2. Sweep data (broker-side OTel)

| producers | offered/s | pub_accept/s | deliveries/s | ack_proc/s | ack_ratio | retry_exh/s | firehose_cv |
|----------:|----------:|-------------:|-------------:|-----------:|----------:|------------:|------------:|
| 1         | 100       | **73.7**     | 98.2         | **84.5**   | 0.86      | 5.9         | 1.8 |
| 2         | 200       | **87.5** ◄pk | 79.3         | 69.4       | 0.88 ◄ok  | 9.1         | 1.3 |
| 4         | 400       | 61.7         | 74.5         | 44.2       | 0.59 ◄bad | 14.2        | 1.9 |
| 8         | 800       | 60.6         | 39.8         | 17.9       | 0.45 ◄bad | 19.5        | 3.8 |

**Congestion collapse:** publish-accept *peaks at N=2 (~87/s) then decreases* under more
load. Offering 8× the load (800 vs 100) yields *less* accepted throughput (60 vs 74) and 3×
the DLQ churn. Classic overload — past the knee, adding producers makes everything worse.

Steady-state (8 producers, ~19 min settled) showed higher fanout (deliveries 367/s @ pub
88/s, fanout 4.2) because subscriptions were fully established; the sweep's rapid rescaling
kept consumers in eviction/re-subscribe churn (fanout <1.3), depressing delivery counts.
Publish-accept ~60–88/s is consistent across both — it is the stable ceiling.

---

## 3. Component maxima (this environment)

| component | metric | max observed | limited by |
|-----------|--------|-------------:|-----------|
| **Broker publish-accept** | aggregate accepted/s | **~87/s** (≈8 in-flight) | single-loop reply serialization; **congestion-collapses past knee** |
| **Broker delivery/fanout** | burst to 1 conn | **~2878/s** | not the bottleneck; consumer drain caps sustained |
| **Broker ack-processing** | acks/s | **~85/s** (unloaded) → 18/s (loaded) | same loop; ack ingest starves under publish contention |
| **Per-connection publish (serial `await`)** | msg/s | **~74/s** (4 workers) | RTT × in-flight |
| **Per-worker publish (1 in-flight)** | msg/s | **~18/s** | single-flight RTT ≈ **55 ms** |
| **Consumer ack (client emit, fire-&-forget)** | self/s | ~650/s each | client cheap; **broker ingest** is the real limit |
| **Firehose single-conn subscriber** | sustained | ~40–180/s, bursts 2878/s | one greedy `>` sub → gulp/stall (cv → 3.8) |

**Derived RTT / concurrency efficiency:**
- Single-flight publish RTT ≈ **55 ms** (18/s per worker).
- 4→8 concurrent in-flight publishes: 73.7 → 87.5/s = **+19% only** → diminishing returns
  already at ~4 in-flight; per-publish RTT *grows* 55 → 91 ms as the loop serializes replies.
- **Knee ≈ 4–8 concurrent in-flight publishes** (≈ 2 producers × 4 workers).

---

## 4. Measurement caveats

- **Client self-report is fiction under load.** `ack`/`nack` are fire-and-forget
  (`_raw_send`, no reply — `client.py:330`): the client emits acks fast but never learns the
  broker couldn't ingest them. Self-reported `ack_rate`/`pub_rate` diverged from broker OTel
  by 8–15×. Broker OTel (`:9464`) is ground truth.
- `ack_ratio` (broker acks ÷ deliveries) is the health signal: **≥0.85 healthy, <0.6 = ack
  starvation** → retries → DLQ eviction storm.
- **The `>` firehose sawtooth is a measurement artifact, not consumer failure.** The sidecar
  `_pump_throughput` is a single greedy subscriber; its one-connection gulp/stall drives cv up
  to 3.8. Consumer health reads off OTel, not the DELIVERED/SEC panel.

---

## 5. Secondary test — `DURABILITY=sqlite` (counter-intuitive: sqlite WINS)

Same sweep, controller recreated with `DURABILITY=sqlite` (WAL DB on shared volume).
Result: sqlite is **higher-throughput AND rock-stable** where memory collapsed.

| N | pub_accept/s (mem → sql) | deliveries/s (mem → sql) | ack_ratio (mem → sql) | retry_exh/s (mem → sql) | firehose_cv (mem → sql) |
|--:|:--:|:--:|:--:|:--:|:--:|
| 1 | 73.7 → **101.1** | 98 → **195** | 0.86 → **0.99** | 5.9 → **0.5** | 1.8 → **1.1** |
| 2 | 87.5 → **104.6** | 79 → **201** | 0.88 → **0.98** | 9.1 → **0.6** | 1.3 → **0.18** |
| 4 | 61.7 → **104.7** | 74 → **203** | 0.59 → **0.99** | 14.2 → **0.4** | 1.9 → **0.16** |
| 8 | 60.6 → **102.6** | 40 → **197** | 0.45 → **0.97** | 19.5 → **0.3** | 3.8 → **0.21** |

sqlite is **FLAT** across an 8× load range: pub ~103/s, deliveries ~200/s, ack_ratio ~0.98,
DLQ churn ~0, firehose smooth (cv ~0.2 — **the sawtooth disappears**). No congestion collapse,
no eviction churn (sidecar evictions constant vs memory's monotonic climb).

### Why sqlite is faster (mechanism)

Publish is round-trip and, under sqlite, the reply awaits the **WAL write**. That disk write
is a **natural per-connection backpressure / pacing brake**: a producer cannot flood the loop
faster than fsync cadence, so the loop is **never monopolised**. Consequences:
- Consumers stay subscribed (fanout stable **1.9** vs memory's churning 0.7–1.3).
- The ack-read path is serviced fairly → `ack_ratio` ~0.99 → no retries → no DLQ eviction storm.
- Delivery to the firehose is steady → sawtooth gone.

Memory has **no brake**: publishes are accepted instantly, one producer monopolises the loop,
the consumer ack-read path starves → retries → DLQ → slow-subscriber eviction → collapse spiral.
So memory's apparent "speed" is what *causes* its instability.

**This directly resolves the original report:** the intense eviction log + DELIVERED/SEC
sawtooth under `DURABILITY=memory` are fixed by switching to `sqlite` (or by adding explicit
publish pacing/backpressure to the memory path).

### Caveat — load was paced, not max

This sweep used **paced** producers (`PRODUCER_RATE=100`/instance). sqlite caps intake at
~103/s by **backpressuring the producer** (excess offered blocks in `await publish()`), rather
than collapsing internally. Under `PRODUCER_RATE=max` the sqlite loop may still saturate (a
prior run found publish starvation on both backends at max rate). Conclusion holds for
**bounded / paced** workloads: sqlite gives higher stable throughput and eliminates the
eviction/sawtooth pathology.
