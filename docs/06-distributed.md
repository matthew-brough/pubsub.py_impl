# pubsub.py — Distributed harness benchmark (ingress vs egress)

> Raw appendix — the run-by-run record behind `BENCHMARKS.md`. Superseded by it
> for conclusions; kept for method and per-regime evidence. Absolute rates are
> host-bound; read them as shape, not spec.

**Date:** 2026-07-24
**Host:** WSL2, 16 cores, 23 GB RAM (a constraint that itself set the top-end).
**Stacks:** two multi-broker topologies over a **shared Postgres** durable layer +
`broker_id` audit trail, elastic brokers (`--scale broker=N`, DNS discovery,
nginx OSS 1.27 `resolve`+`least_conn`). lib `pubsub-py` master (uvloop `[otel,fast]`).

- **ingress-balanced** — nginx LBs producer *ingress*; consumers **fan in** (one
  sub per broker). Sharded ingest, each msg on one broker, union = global stream.
- **egress-balanced** — producers **broadcast** to every broker (replicated
  ingest); nginx LBs consumer *egress* (shards Z consumers ~Z/Y per broker).

Method: fresh stack per cell, staggered consumer join, 18 s+ settle + 16 s delta
of **broker-authoritative OTel** (summed across replicas) via the observer's
firehose-free `/api/otel`; per-broker balance + Postgres audit alongside.
`DURABILITY=postgres` unless stated (T6 sweeps `none`/`memory`; sqlite excluded —
single-writer can't be shared across brokers). Ingress **26 cells** (host ceiling
capped the giant re-runs), egress **29/29**.

Throughput is reported as **aggregate broker work** (summed OTel deliveries/s) —
authoritative and, in egress, honestly Y-amplified. `repl_factor` =
agg-publish ÷ producer-self-reported logical publish ≈ **B** for egress (broadcast
tax), **≈1** for ingress.

---

## 1. Broker-scaling efficiency — both diminish, for *different* reasons

**Ingress** (fixed P4:C48):
| B | agg_deliv/s | per-broker | gain | balance |
|--:|--:|--:|--:|--:|
| 1 | 3 636 | 3 636 | — | 1.00 |
| 2 | 6 955 | 3 478 | ×1.91 | 0.96 |
| 4 | 9 596 | 2 399 | ×1.38 | 0.96 |
| 8 | 11 655 | 1 457 | ×1.21 | **0.00** |

**Egress** (fixed P2:C48):
| B | agg_deliv/s | per-broker | gain | balance |
|--:|--:|--:|--:|--:|
| 1 | 3 633 | 3 633 | — | 1.00 |
| 2 | 7 534 | 3 767 | ×2.07 | 0.96 |
| 4 | 8 450 | 2 112 | ×1.12 | 0.92 |
| 8 | 8 458 | 1 057 | ×1.00 | 0.71 |

Both scale near-linearly **1→2 brokers**, then flatten — but the wall differs:

- **Ingress** is bounded by **producer-connection count**. least_conn pins each of
  the 4 producer connections to one broker, so ingest can occupy at most 4
  brokers; at B=8 four brokers sit idle (**balance 0.00**) and per-broker halves.
  *Scaling brokers past your producer fan-out is wasted in ingress.*
- **Egress** saturates by **B=4 at fixed consumers** (×1.00 from 4→8). Replicated
  ingest means every broker already holds everything; adding brokers past the
  point where the C=48 consumer fleet is comfortably sharded only splits the same
  delivery work thinner. Growth comes from **more consumers**, not more brokers
  (capstone B8:C116 → 9 646/s).

ackr 1.00 / retry 0 across every scaling cell in both — no congestion collapse.

Those are the *topology-shape* reasons each curve flattens. Underneath both sits a
**deeper absolute wall: the single shared Postgres** — both topologies cap around
~10–12 k/s aggregate regardless of broker count. §8 isolates it with a `none`
control (no Postgres at all): broker gains **resume past B=2** (effective acked
throughput 8.8 k→13.6 k→18.4 k→30 k, B1→8) — proving the shared WAL/fsync, not the
brokers, set the ~12 k ceiling. So "no real gains past 2 brokers" is **primarily
the shared durable layer**, with a secondary backend-independent fan-in
connection wall (per-broker throughput still erodes on `none` — see §8).

## 2. Replication tax (egress) is real and ≈ B

`repl_factor` (agg publishes ÷ logical publishes):

| egress cell | B | repl_factor |
|--|--:|--:|
| 2:2:* (T2) | 2 | 1.5–1.8 |
| 4:4:120 | 4 | **3.72** |
| 8:4:4 (T3) | 4 | **4.12** |
| T6 postgres 4:4:120 | 4 | **3.93** |

Ingress `repl_factor` stays **≈1** everywhere (sharded — each publish lands once).
So egress buys its consumer-connection savings (1 conn/consumer vs B) with **B×
write amplification on the shared Postgres** — which is exactly why we turned that
amplification into the **audit trail**: each broker independently persists what it
handled, stamped with `broker_id`.

## 3. Delivery balance — egress shards evenly, ingress concentrates

| consumers | ingress balance | egress balance |
|--|--:|--:|
| moderate (C≤48) | 0.70–1.00 (producer-dependent) | 0.75 → 0.96 (improves with C) |
| **giant (C120)** | **0.01** | **0.86–0.97** |

Egress `least_conn` spreads a giant consumer fleet almost perfectly across brokers.
Ingress fan-in **concentrates delivery**: with few producers, ingest lands on 1–2
brokers, and those brokers alone do the ×fanout delivery work — the rest idle
(giant `4:4:120`: balance **0.01**). **For giant-consumer fan-out, egress is the
correct model** — which is what the split was built to show.

Consumer self-report backs this: at C120 egress saw **120/120** consumers
(1 light connection each) vs ingress **105/120** (B fan-in connections each,
heavier, some lag).

## 4. Postgres tuning — "not sqlite": single-writer is the bottleneck

`PG_MAX_WRITERS` / `synchronous_commit` at a mid cell:

| config | ingress deliv/s (p95) | egress deliv/s (p95) |
|--|--:|--:|
| sync=on **writers=1** | 3 257 (683 ms) | **255 (2 173 ms)** |
| sync=on writers=4 | 5 922 (68 ms) | 5 375 (8 ms) |
| sync=off writers=1 | 7 592 (58 ms) | 307 (2 371 ms) |
| sync=off writers=4 | 7 368 (64 ms) | 7 643 (35 ms) |

A single commit worker is catastrophic — **worst in egress** (replicated writes =
~B× the write load funneled through one writer → p95 **2.2 s**, throughput floor).
`writers=4` fixes it: **+82 % ingress / ×21 egress** throughput and 10–260× lower
latency. `writers=8` adds nothing. `sync=off` buys a further ~28 %. **This is the
concrete payoff of not inheriting SQLite's single-writer assumption** — the
concurrent-commit-worker design is what makes Postgres scale here. writers=4,
sync tuned to durability need, is the operating point.

## 5. Backend — durability is distributed admission control

Mid cell, `none` / `memory` / `postgres`:

| | ingress deliv/s · ackr · retry · p95 | egress deliv/s · ackr · retry · p95 |
|--|--|--|
| none | 20 502 · 0.78 · 502 · 285 ms | 24 871 · 0.84 · 496 · — |
| memory | 20 091 · 0.77 · 528 · 366 ms | 25 019 · 0.85 · 457 · — |
| postgres | 4 756 · **1.00 · 0** · 32 ms | 5 323 · **1.00 · 0** · — |

Identical thesis in both topologies: `none`≈`memory` let publish **run away**
(uncapped accept → over-deliver → redelivery churn, ackr 0.77–0.85, retry
hundreds, high latency); Postgres' fsync **paces** publish → ~4–5× less raw
delivery but **clean, bounded, low-latency** (ackr 1.0, retry 0). Even at giant
`4:4:120`, Postgres held ackr 0.997 / retry 0 while `none`/`memory` churned
(retry 764–1066). Durability isn't just persistence — it's free back-pressure.

## 6. Top-end viability (this host)

- **Viable:** single cold `4:4:120` (129 app-containers) — ~7.7 k (ingress) /
  9.0 k (egress) deliv/s, clean.
- **Egress reached 8-broker giant** `2:8:116` (9 646/s) — sharded consumers keep
  per-broker load ~C/8, so the observer stays sampleable.
- **Ingress could not** sample 8-broker giant `4:8:116` — fan-in puts full ×fanout
  on *every* broker and the observer must scrape all 8 while draining fleet from
  all 8; it saturates.
- **Back-to-back 129-container cells** fail on 23 GB: killed processes' memory /
  bridge networks don't recover before the next `up`. Mitigated (resource-wait
  teardown) but the host still degrades under sustained giant churn. Not a code
  defect — a host ceiling. Giant cells are reliable only as cold, spaced runs.

## 7. Egress top-end at 8P:8B — consumer sweep, no container cap

Fixed **8 producers broadcasting to 8 brokers** (repl ≈ 8 — the heaviest write
path), consumers swept 64 → +8 until a ceiling (ack<0.85 / throughput collapse /
keepalive fail). `dist_matrix_driver.py --sweep88`.

- **postgres:** throughput **plateaus ~10–13 k/s** (peak 13.4 k @ C144), `ack_ratio`
  pinned **1.00**, retry 0 — fsync admission control never loses fairness. Ran
  clean **past C160 / 177 app-containers** until host resources, not fairness,
  capped it. `PG_MAX_WRITERS` 4→8 gave only **~+10 %** (peak 13.4 k vs 12.2 k):
  the wall is Postgres **WAL/fsync throughput at 8× replication**, not
  commit-worker concurrency.
- **none:** **33–56 k/s** (≈3–4.5× the postgres raw, uncapped publish) but
  `ack_ratio` **~0.87–0.89 sustained** with retry churn **600–1 150/16 s** — the
  fairness tax of no admission control. Never fully collapsed; it self-limits.
- **Machine wall = CPU:** ~**1 450 / 1 600 %** (14.5 of 16 cores) saturated. That
  is the top-end on this host — a machine limit, not a pubsub one.

**Takeaway:** at 8×-replication egress, Postgres is a **bounded-fair ~12 k/s**
ceiling (WAL-bound, ack 1.0); `none` buys **~4× raw throughput** with a permanent
**~12 % ack loss + retry churn**; and absolute scale is gated by host CPU.

## 8. Why broker gains stop past B=2 — the `none` control

To separate "shared Postgres" from "broker/host/topology," re-ran the **ingress**
broker sweep on **`DURABILITY=none`** (Null backend — writes *nothing*, zero
Postgres involvement): fixed **4 producers : B brokers : 64 consumers**, B ∈
{1,2,3,4,8}. `dist_matrix_driver.py --broker-sweep`.

| B | agg_deliv/s | ack_ratio | **effective (acked)/s** | per-broker | retry | bal |
|--:|--:|--:|--:|--:|--:|--:|
| 1 | 15 253 | 0.57 | **8 755** | 15 253 | 855 | 1.00 |
| 2 | 23 329 | 0.59 | **13 648** | 11 665 | 1 319 | 0.99 |
| 3 | 28 732 | 0.62 | **17 670** | 9 577 | 1 707 | 0.00 |
| 4 | 26 095 | 0.71 | **18 397** | 6 524 | 1 692 | 0.94 |
| 8 | 41 413 | 0.72 | **29 983** | 5 177 | 2 660 | 0.00 |

(vs **postgres** ingress: aggregate capped ~12 k — B4 9.6 k, B8 11.6 k.)

1. **Shared Postgres WAS the absolute-throughput wall — confirmed.** `none` floods
   ~**4× higher** (B1 15 k vs 3.6 k). Effective *acked* throughput keeps climbing
   past B2 — **8.8 k→13.6 k→18.4 k→30 k** — where postgres flat-lined ~12 k. The
   single WAL/fsync set the ceiling; remove it and broker gains resume.
2. **`none` pays with fairness collapse.** ack_ratio **0.57–0.72**, retry
   **855→2 660**: no admission control → publish floods, 30–43 % of deliveries
   never ack → redelivered (raw `agg_deliv` is inflated by that churn; the
   *effective* column strips it). Postgres' cap *was* buying ackr 1.0.
3. **A second, backend-independent wall remains:** per-broker throughput still
   erodes on `none` (15 k→5 k). 64 consumers fan-in to *every* broker =
   **64×B connections**; more brokers = more total connections/coordination each
   must juggle → the "broker managing in×out connections" bottleneck, present
   regardless of durability, just at a 4× higher ceiling without PG.
4. `bal 0.00` at B3/B8 = the P4 sharded-ingest confound (4 producer connections
   can't evenly feed 3 or 8 brokers) — topology, not backend.

**So the ~2-broker ceiling is *primarily the shared durable layer.*** Sharding
Postgres / a per-broker durable layer unlocks continued scaling (`none` shows
~30 k acked at B8 of headroom), after which you hit the fan-in connection wall and
must re-introduce bounded admission control to keep fairness. It was never a raw
broker limit — it was the DB, then the connection model.

## 9. Higher-broker scenarios — egress wins on consumer *capacity*

At **fixed consumers**, neither topology gains throughput from more brokers:
ingress caps on shared-PG writes (~12 k) + producer-connection-limited ingest;
egress is **fanout-bound** (every broker holds the full stream, so more brokers
just shard the same C consumers thinner — the B4→8 ×1.00). Delivery ≈
logical_publish × C, independent of B.

The higher-broker win is about serving **more consumers**, and there egress is the
structural winner:

| at C consumers | ingress (fan-in) | egress (sharded) |
|--|--|--|
| total connections | **C × B** (every consumer → every broker) | **C** (1 each) |
| per-broker connections | C | **C / B** (shrinks with B) |
| adding a broker | *more* connection load everywhere | **offloads** — fewer each |

In ingress, more brokers make the connection wall (§8) **worse** (C×B grows); in
egress, more brokers make it **better** — broker count *is* the consumer-capacity
lever. Evidence in-hand: the 8:8 egress sweep served **C160+ / 177 containers
clean**, while ingress couldn't even *sample* its 8-broker giant.

**Caveat that flips it:** egress's **B× replication tax on the *shared* Postgres**
hammers one WAL harder at high B (8× writes at B8), so on a shared DB egress
saturates *writes* earlier — at fixed C its edge narrows to connection-handling,
not raw throughput. Egress fully wins the higher-broker regime only once the
durable layer is **sharded / per-broker**: then each broker independently holds the
full stream, serves its consumer shard, and writes its own DB → near-linear
horizontal scaling, which ingress's fan-in (C×B connections) structurally can't
match. Net: **for fan-out capacity at higher broker counts, egress is the primary
winner — provided the durable layer scales with it.**

---

## Verdict

The two-topology split is **empirically justified**:

- **egress-balanced is the right model for the stated pubsub workload** (few
  producers, giant consumers): 1 connection per consumer, near-perfect delivery
  balance, scales to the 8-broker giant, giant fleets fully observed. Its cost is
  the **B× Postgres replication tax**, repurposed as the cross-broker audit trail.
- **ingress-balanced fits analytics fan-in** (every consumer needs the whole
  stream) at moderate scale, but concentrates delivery at few producers, costs B
  connections per consumer, and hits an observer wall at 8-broker giant fanout.
- **Brokers stop paying off past ~2 primarily because of the shared Postgres**, not
  a broker limit — the `none` control (§8) resumes scaling past B2 (≈30 k acked at
  B8) once the DB is out of the path. Scaling *delivery* with brokers requires first
  sharding the durable layer (or going per-broker); the backend-independent **fan-in
  connection wall** (per-broker throughput erodes as 64×B connections pile up) and a
  bounded-admission-control fairness cost then re-enter behind it.

Driver: `dist_matrix_driver.py` (matrix + `--sweep88` + `--broker-sweep`). Raw
per-cell results (`*_results*.json`) and progress logs are run outputs —
regenerate them from the driver; they are not committed.
