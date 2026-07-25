# pubsub.py — Benchmark Rerun (post-backing-update)

> Raw appendix — the run-by-run record behind the library README's Performance summary. Superseded by it
> for conclusions; kept for method and per-regime evidence. Absolute rates are
> host-bound; read them as shape, not spec.

**Date:** 2026-07-22 (rerun)
**Environment:** WSL2 (Linux 6.6), Docker Compose, single-host loopback.
**Broker config:** single-threaded asyncio loop, TCP `:8765`,
`RETRY_MAX_ATTEMPTS=5 RETRY_BASE=0.1 RETRY_CAP_EXPONENT=2`.
**Fleet:** 1–8 producers (4 workers each), 4 consumers, 1 analytics sidecar (`>` firehose).
**Method:** identical scale sweep (`--scale producer=N`, N∈{1,2,4,8}), fresh image build,
clean volume (`down -v`) between backends, 12 s settle + 16 s OTel sample per step.
Rates are **broker-side OpenTelemetry** (authoritative); `*_self` = client heartbeat.
Offered load **paced** at 100 msg/s per producer instance.

---

## 1. Headline — the update fixed the collapse

**Memory durability no longer congestion-collapses. It now scales near-linearly and is the
throughput winner.** The previous run's memory pathology (publish-accept peaking ~87/s at
N=2 then collapsing to ~60/s, ack_ratio → 0.45, eviction storm, cv → 3.8) **is gone.**

- **Memory:** publish-accept **96 → 193 → 385 → 770/s** across N=1..8 — ~96/s per producer,
  clean linear scaling, **zero evictions**, ack_ratio ~0.99 throughout, firehose smooth (cv ~0.4).
- **sqlite:** stable and healthy but **fsync-bound at ~130–140/s** aggregate — does not scale
  with producers past N=2 (WAL write is the serialization point). ack_ratio ~0.92–1.0, zero evictions.

**Conclusion reversal:** in the prior run sqlite won because it back-pressured memory's collapse.
The broker fix removed that collapse, so the WAL brake that *helped* memory is now just a
**throughput cap on sqlite**. For raw throughput, **memory now beats sqlite ~5.4× at N=8** (770 vs 142).

---

## 2. New sweep data (broker-side OTel)

### memory
| N | offered/s | pub_accept/s | deliveries/s | ack_proc/s | ack_ratio | retry_exh/s | firehose_cv | evict |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 100 | 96.6  | 187.9  | 186.9  | 1.00 | 0.5 | 1.1 | 0 |
| 2 | 200 | 192.7 | 368.9  | 366.7  | 0.99 | 0.7 | 0.4 | 0 |
| 4 | 400 | 385.4 | 717.4  | 712.7  | 0.99 | 1.3 | 0.4 | 0 |
| 8 | 800 | **769.6** | **1418.8** | 1408.4 | 0.99 | 1.5 | 0.4 | 0 |

### sqlite
| N | offered/s | pub_accept/s | deliveries/s | ack_proc/s | ack_ratio | retry_exh/s | firehose_cv | evict |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 100 | 98.6  | 190.3 | 189.4 | 1.00 | 0.5 | 1.1 | 0 |
| 2 | 200 | 123.8 | 240.7 | 239.8 | 1.00 | 0.5 | 0.3 | 0 |
| 4 | 400 | 128.2 | 247.4 | 227.4 | 0.92 | 0.3 | 0.3 | 0 |
| 8 | 800 | **141.6** | 269.5 | 278.7 | 1.03* | 0.4 | 0.6 | 0 |

\* ack_ratio >1 = sample-window skew (acks from prior-step backlog draining), not an anomaly.

---

## 3. Before vs after (publish-accept/s, memory)

| N | prior (collapsed) | rerun | change |
|--:|--:|--:|--:|
| 1 | 73.7 | 96.6  | +31% |
| 2 | 87.5 | 192.7 | +120% |
| 4 | 61.7 | 385.4 | +525% |
| 8 | 60.6 | 769.6 | **+1170%** |

Prior memory: ack_ratio 0.86→0.45, retry_exh 5.9→19.5/s, cv 1.8→3.8, evictions climbing.
Rerun memory: ack_ratio flat ~0.99, retry_exh <1.5/s, cv ~0.4, **evictions 0**. The
divergence that used to widen with load is eliminated — the loop is no longer monopolised.

sqlite also improved modestly (N=8 pub 102.6 → 141.6) and now scales a little (N=1→2) before
hitting the fsync wall, vs the prior dead-flat ~103/s.

---

## 4. Component maxima (rerun, this env)

| component | memory | sqlite | limited by |
|-----------|--:|--:|-----------|
| Broker publish-accept (aggregate) | **~770/s** (linear, N=8, not yet saturated) | **~140/s** (plateau from N=2) | memory: none observed to N=8; sqlite: WAL fsync serialization |
| Broker delivery/fanout | **~1419/s** (fanout 1.85) | ~270/s | tracks accept × fanout; not the bottleneck |
| Broker ack-processing | **~1408/s** | ~279/s | tracks deliveries; healthy on both |
| Per-producer publish (4 workers, paced) | ~96/s | ~100/s (N=1), capped in aggregate | client RTT (memory) vs shared fsync (sqlite) |
| firehose single-conn subscriber | smooth cv ~0.4 | smooth cv ~0.3–0.6 | sawtooth gone on both |

**Memory ceiling not found** — at N=8 it is still scaling linearly and matching offered load
(769.6 accepted vs 797.4 self-offered). To locate the real memory knee, sweep further (N=16,
32) or run `PRODUCER_RATE=max`.

---

**Config note:** `docker-compose.yml` `DURABILITY` is now env-driven (`${DURABILITY:-sqlite}`),
so the sweep can select backend without editing the file. Default still resolves to `sqlite`.

---

## 5. Push to the ceiling — memory, `PRODUCER_RATE=max`, poison + nack OFF

Flat-out publish (`_max_worker`), transient nacks and poison disabled for **true rates**.
Two axes: scale producers at fixed 4 consumers, then scale consumers at fixed 16 producers.
`docker-compose.yml` poison typo fixed (`POISON_EdVERY` → `POISON_EVERY`) so it can be disabled.

### Producer axis (consumers = 4)
| P | pub_accept/s | (burst) | deliveries/s | ack_proc/s | fanout | ack_ratio | retry_exh/s |
|--:|--:|--:|--:|--:|--:|--:|--:|
| 4  | 6523 | 7334 | 3140 | 2042 | 0.48 | 0.65 | 1127 |
| 8  | 7145 | 8199 | 2575 | 1167 | 0.36 | 0.45 | 1427 |
| 16 | 7215 | 8452 | 2198 | 614  | 0.30 | 0.28 | 1565 |
| 32 | **8483** | **11688** | 1745 | 393 | 0.21 | **0.22** | 1439 |

### Consumer axis (producers = 16)
| C | pub_accept/s | deliveries/s | ack_proc/s | fanout | ack_ratio | retry_exh/s |
|--:|--:|--:|--:|--:|--:|--:|
| 4  | 7215 | 2198 | 614  | 0.30 | 0.28 | 1565 |
| 8  | 881  | **7451** | 7432 | 8.5  | **1.00** | 0.9 |
| 16 | 477  | **7796** | 7796 | 16.3 | **1.00** | 0.9 |

### What caps out

1. **Publish-accept ceiling ≈ 8500/s** (burst 11.7k). At max rate a *single* loop accepts
   publishes extremely fast — that is the problem, not the win.
2. **The bottleneck is consumer drain, not accept.** With 4 consumers, the 7–8k/s publish
   flood monopolises the loop; the consumer ack-read path starves → `ack_ratio` collapses
   0.65 → **0.22**, fanout falls below 1 (deliveries < publishes = consumers can't keep up
   and get evicted), retry-exhaust ~1.1–1.6k/s (evicted subs' in-flight requeues → DLQ, even
   with poison OFF), evictions climb monotonically. **Adding producers here makes it worse** —
   accept +30% (6523→8483) but delivery −44% (3140→1745).
3. **Adding consumers fixes health and reveals the delivery ceiling.** P=16: 4→8 consumers
   flips `ack_ratio` 0.28 → **1.00**, DLQ churn 1565 → ~1/s, delivery 2198 → **7451/s**.
4. **Delivery/ack ceiling ≈ 7800/s each.** With fanout, every publish = C deliveries + C acks.
   At C=16 delivery 7796 = pub 477 × 16.3. The single loop tops out at **~7800 deliveries/s +
   ~7800 acks/s ≈ 15.6k downstream ops/s** — so raising fanout (more consumers) *lowers* the
   sustainable publish rate (477 @ C=16). Publish and fanout share one loop budget.

### System limits (memory, this env)
| path | ceiling | notes |
|------|--:|-------|
| Publish-accept (publish-bound) | **~8500/s** (burst 11.7k) | reached only when delivery is starved — pathological |
| Delivery / fanout | **~7800/s** | true sustained, ack_ratio 1.0 |
| Ack-processing | **~7800/s** | matches delivery |
| Total loop ops (deliver + ack) | **~15.6k ops/s** | the real single-loop wall |
