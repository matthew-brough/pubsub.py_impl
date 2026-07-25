# pubsub.py — Durability × Topology Matrix (extended set)

> Raw appendix — the run-by-run record behind `BENCHMARKS.md`. Superseded by it
> for conclusions; kept for method and per-regime evidence. Absolute rates are
> host-bound; read them as shape, not spec.

**Date:** 2026-07-22 (third test set)
**Environment:** WSL2 (Linux 6.6), Docker Compose, single-host loopback, 16 CPU / 23 GB.
**Broker:** single-threaded asyncio loop, TCP `:8765`, `RETRY_MAX_ATTEMPTS=5 BASE=0.1 CAP_EXP=2`.
**Load:** `PRODUCER_RATE=max` (flat-out), `PRODUCER_CONCURRENCY=4`, **poison OFF, transient-nack OFF**
(`POISON_EVERY=0 NACK_RATE=0`) — true rates, no synthetic DLQ.
**Method:** per-step **fresh stack** (`down -v` between every step → clean subs, no eviction
carryover, bounded in-memory history), 16 s settle + 16 s OTel sample. Rates = broker OTel.
**Backends:** `none`, `memory`, `sqlite`. **Topologies:** matched P=C, producer-heavy, subscriber-heavy.

> **New backend added:** `NullDurability` (`testclient/null_durability.py`, `DURABILITY=none`) —
> a no-op store: `append` drops, no replay, no ack tracking, no DLQ. Distinct from `memory`
> (`InMemoryDurability`), which retains replayable-topic history in an **unbounded list**. `none`
> isolates pure transport+fanout cost and removes memory's long-run retention liability.

---

## 1. Headline findings

1. **Two independent ceilings, not one.** The broker has (a) a **publish-accept** ceiling and
   (b) a **delivery+ack loop-op** ceiling. They are hit in different topologies and trade against
   each other through one event loop.

   | backend | publish-accept peak | sustained delivery ceiling | loop-op wall (deliv+ack) |
   |--------|--:|--:|--:|
   | none   | **~15.6k/s** (burst 18.5k) | **~9.6k/s** | **~19k ops/s** |
   | memory | ~14.0k/s (burst 15.9k) | ~9.3k/s | ~18.6k ops/s |
   | sqlite | ~1.1k/s (burst 1.2k) | ~2.8k/s | ~5.7k ops/s |

2. **Congestion collapse is a producer-heavy phenomenon — and only on `none`/`memory`.** When
   consumers can't drain, publish-accept *runs away* (15k/s) while delivery **starves**: at
   P=128,C=1 `none` accepts 15619/s but delivers 489/s, `ack_ratio` **0.40**, retry-exhaust ~293/s
   (evicted subs requeue → DLQ, even with poison off). `memory` collapses **harder** (P=128,C=1:
   delivery **12/s** — near-total starvation; history-append competes for the loop).

3. **`sqlite` never collapses — in any topology.** `ack_ratio` is **1.0** and `retry_exh` is **0**
   across *all 23 cells*, including P=128,C=1. WAL fsync back-pressures producers so no producer can
   monopolise the loop. The cost is a **3–4× lower ceiling** (~2.8k deliv/s vs ~9.5k). Pure
   stability-for-throughput trade.

4. **`none` ≈ `memory` for throughput; `none` is strictly safer.** Nearly identical curves; `memory`
   is a touch slower at high publish rate (per-message history append) and carries the unbounded-
   history OOM risk on a long-lived controller. `none` = `memory` minus the liability.

5. **Fanout multiplies loop work: sustainable publish ≈ loop-op-wall ÷ (2 × fanout).** Every publish
   costs `C` deliveries + `C` acks. So more consumers *lower* the achievable publish rate — matched
   and subscriber-heavy runs are delivery-bound, publish throttles itself to keep `ack_ratio` 1.0.

6. **Fleet self-report is useless under load** (again): sidecar reported `n_consumers=0` while the
   broker delivered 9457/s to 16 live consumers. Trust OTel, never the dashboard fleet count.

---

## 2. Matched (P = C) — the balanced regime

Both sides scale together; always healthy (`ack_ratio` 1.0). Publish falls as fanout grows; delivery
climbs to the loop-op wall.

| P=C | none pub / deliv | memory pub / deliv | sqlite pub / deliv |
|--:|--:|--:|--:|
| 1  | 3350 / 6317 | 2190 / 4224 | 421 / 843 |
| 2  | 2386 / 6903 | 2214 / 6513 | 705 / 2114 |
| 4  | 1818 / 8189 | 1769 / 7820 | 488 / 2427 |
| 8  | 1026 / 8585 | 977 / 8117 | 322 / 2843 |
| 16 | 579 / **9457** | 537 / **8822** | 171 / **2714** |

`none`/`memory` delivery saturates ~8.5–9.5k/s at P=C≥4. `sqlite` saturates ~2.7–2.8k/s.

---

## 3. Producer-heavy (C ∈ {1,2,4}) — where collapse lives

| P:C | none pub / deliv / ackr / retry | memory pub / deliv / ackr / retry | sqlite pub / deliv / ackr / retry |
|--:|--:|--:|--:|
| 32:1  | 14988 / 1193 / **0.45** / 594 | 13958 / 1037 / **0.44** / 605 | 1127 / 2254 / **1.00** / 0 |
| 32:2  | 2117 / 5861 / 1.00 / 0.6      | 11420 / 1514 / **0.51** / 763 | 792 / 2359 / 1.00 / 0 |
| 32:4  | 1803 / 6941 / 1.00 / 3        | 2164 / 6247 / 1.00 / 11       | 558 / 2742 / 1.00 / 0 |
| 64:1  | 14837 / 719 / **0.48** / 298  | 11687 / 727 / **0.44** / 454  | 985 / 1971 / 1.00 / 0 |
| 64:2  | 13814 / 1149 / **0.56** / 465 | 6409 / 149 / **0.68** / 180   | 535 / 1579 / 1.00 / 0 |
| 64:4  | 1745 / 6571 / 1.00 / 3        | 5461 / 623 / **0.31** / 406   | 363 / 1738 / 1.00 / 0 |
| 128:1 | 15619 / 489 / **0.40** / 293  | 5923 / **12** / — / 89        | 547 / 1094 / 0.99 / 0 |
| 128:2 | 13579 / 498 / **0.40** / 351  | 6869 / 229 / **0.34** / 176   | 444 / 1296 / 1.00 / 0 |
| 128:4 | 8814 / 1061 / **0.56** / 461  | 6504 / 374 / **0.34** / 239   | 330 / 1541 / 1.00 / 0 |

- **`none`/`memory`: bimodal.** Either healthy (delivery ~6–7k, ackr 1.0) or collapsed (publish
  10–16k, delivery <1.2k, ackr 0.3–0.56, retry storm). The tipping point is consumer drain capacity:
  `none` P=32,C=2 sits healthy but P=64,C=2 flips to collapse; `memory` flips earlier and deeper.
- **Publish-accept ceiling exposed here:** the runaway `none` numbers (~15–16k/s, burst 18.5k) are
  the true accept ceiling — reachable *only* when delivery is abandoned. Not usable throughput.
- **`sqlite`: flat and healthy throughout.** Publish paced by fsync (330–1127/s), delivery 1.1–2.7k,
  ackr 1.0, zero retries — no collapse mode exists.

---

## 4. Subscriber-heavy (P ∈ {1,2,4}) — fanout-bound, always healthy

| P:C | none pub / deliv / fanout | memory pub / deliv / fanout | sqlite pub / deliv / fanout |
|--:|--:|--:|--:|
| 1:32  | 184 / 5596 / 30 | 184 / 5609 / 30 | 56 / 1360 / 24 |
| 2:32  | 295 / 9539 / 32 | 282 / 9107 / 32 | 94 / 2587 / 28 |
| 4:32  | 297 / **9591** / 32 | 295 / **9296** / 32 | 92 / 2530 / 28 |
| 1:64  | 109 / 6896 / 63 | 110 / 7084 / 64 | 65 / 2167 / 34 |
| 2:64  | 63 / 3997 / 64  | 59 / 3739 / 64  | 49 / 1325 / 27 |
| 4:64  | 54 / 3476 / 64  | 65 / 4172 / 64  | 45 / 1162 / 26 |
| 1:128 | 28 / 3366 / 122 | 24 / 2925 / 122 | 65 / 803 / 12 |
| 2:128 | 45 / 5406 / 120 | 35 / 4247 / 122 | 55 / 792 / 15 |
| 4:128 | 48 / 5910 / 124 | 39 / 4781 / 123 | 54 / 885 / 16 |

- **Never collapses** on any backend: `ack_ratio` 1.0, `retry_exh` ~0 everywhere. Fanout work is its
  own back-pressure — the loop spends its budget delivering, so publish self-throttles.
- **Publish is tiny** (tens/s) because each publish fans to 32–128 subscribers; delivery is the
  product `pub × fanout` and hits the same ~9.5k (none/mem) / ~2.8k (sqlite) wall.
- **Caveat — sqlite big-fanout undercount:** sqlite `C=128` shows fanout ~12–16, not ~128. Under
  fsync contention the 128 consumers don't all finish subscribing (WAL-read on subscribe) within the
  16 s window, so measured fanout/delivery understate steady state. `none`/`memory` reach ~122 fanout.

---

## 5. Component maxima (this env)

| path | none | memory | sqlite | limited by |
|------|--:|--:|--:|-----------|
| publish-accept (pathological, delivery-starved) | ~15.6k/s | ~14.0k/s | ~1.1k/s | single-loop accept; sqlite = WAL fsync |
| sustained delivery (ackr 1.0) | ~9.6k/s | ~9.3k/s | ~2.8k/s | loop-op wall ÷ 2 |
| loop-op wall (deliv+ack) | ~19k/s | ~18.6k/s | ~5.7k/s | single asyncio thread |
| per-publish fanout cost | 2×fanout ops | 2×fanout ops | 2×fanout ops | shared loop budget |

---

**Config/code changes this session:** added `testclient/null_durability.py` + wired
`DURABILITY in {none,null,off}` in `testclient/controller.py`; `docker-compose.yml` `DURABILITY`
now `${DURABILITY:-sqlite}` and poison env typo fixed (`POISON_EdVERY`→`POISON_EVERY`).
