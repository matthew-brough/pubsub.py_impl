# pubsub.py — Matrix Rerun on `[otel,fast]` (uvloop + `perf/sync-fanout`)

> Raw appendix — the run-by-run record behind `BENCHMARKS.md`. Superseded by it
> for conclusions; kept for method and per-regime evidence. Absolute rates are
> host-bound; read them as shape, not spec.

**Date:** 2026-07-23 (fifth set — sync-fanout branch)
**Change under test:** lib `v0.2.1` master (`2f2e876`) → branch **`perf/sync-fanout`** (`f799f70`).
**uvloop held constant** (`[otel,fast]`, uvloop 0.22.1) and everything else identical to
`04-matrix-uvloop.md`: same 3 backends × 23 topologies, `PRODUCER_RATE=max`,
poison+nack off, per-step fresh stack, 16 s settle + 16 s OTel sample. **This is a clean
single-variable A/B** — only the fanout implementation moved, so unlike the previous set there is
no uvloop/lib confound. Baseline for every before→after below is the uvloop v0.2.1 matrix.

---

## 1. Headline — sync fanout is a throughput unlock bought with ack-fairness

Two things move together, in opposite directions, exactly as the sync-fanout trade predicted:

- **Delivery ceiling jumps ~40%** on the loop-bound backends and the **publish throttle re-opens** —
  the v0.2.1 back-pressure cap (~3.5k accept) is loosened; publish-accept runs hot again (~8.5k).
- **ack_ratio degrades broadly** on `none`/`memory` (1.0 → 0.64–0.90) with retry churn in the
  hundreds. Delivery now outruns consumer ack capacity within the retry budget → redelivery churn.

This is **not** the stock-asyncio congestion collapse (that was `ack_ratio` 0.40 with delivery
*starved*). Here delivery is **up sharply** and `ack_ratio` only sags — throughput traded for
fairness, not thrown away.

### Publish-accept discipline across the three lib states (producer-heavy peak)
| lib state | publish-accept peak | delivery ceiling | producer-heavy ack_ratio |
|--|--|--|--|
| stock asyncio (baseline-0)   | ~15.6k (runaway) | ~9.6k  | **0.40** (collapse) |
| uvloop + v0.2.1 master       | ~3.5k (capped)   | ~10.1k | **1.00** (clean) |
| uvloop + **sync-fanout**     | **~8.5k**        | **~14.7k** | **~0.90** (mild churn) |

sync-fanout sits **between** stock and v0.2.1 on publish discipline, but **well above both** on
delivery. The v0.2.1 publish↔fanout coupling that *capped* accept is partly reverted; the sync
fast-path spends the reclaimed headroom on raw fanout throughput.

### Representative before → after (publish / delivery / ack_ratio / retry_exh), `none`
| cell | uvloop v0.2.1 | uvloop + sync-fanout |
|--|--|--|
| P16:C16 | 1013 / 9012 / **1.00** / 0   | 1733 / **13970** / **0.71** / 441 |
| P4:C32  | (n/a matched set) —          | 1311 / **14700** / **0.64** / 682 |
| P128:C1 | 1103 / 2206 / **1.00** / 0   | **8511** / **6197** / **0.90** / 45 |
| P4:C64  | 283 / 4593 / **0.81** / 717  | 828 / **12724** / 0.65 / 797 |
| P2:C32  | 659 / 10155 / **1.00** / 279 | 1017 / **13258** / **0.73** / 614 |

## …the churn that was *localized* in v0.2.1 is now *broad*

v0.2.1 confined ack-churn to subscriber-heavy `C≥64`. sync-fanout spreads sub-1.0 `ack_ratio` across
**matched, producer-heavy, and subscriber-heavy** on `none`/`memory` — every high-fanout corner.
`retry_exh` is nonzero in nearly all loop-bound cells (peaks ~800 at `none P4:C64`).

---

## 2. `sqlite` is untouched — clean in all 23 cells

`sqlite` holds `ack_ratio` **1.0** and `retry_exh` **0** in *every* cell, matched through
subscriber-heavy. Its fsync-paced publish (≤~850/s accept) never outruns ack, so the sync fast-path
has nothing to over-deliver. Delivery is roughly flat-to-slightly-lower vs v0.2.1 (disk-bound,
±15% run-to-run) — the sync path adds a little per-delivery cost that the fsync wall hides.

| backend | delivery ceiling (v0.2.1 → sync-fanout) | publish-accept peak (v0.2.1 → sync-fanout) | ack_ratio floor |
|--|--|--|--|
| none   | ~10.1k → **~14.7k** | ~3.5k → **~8.5k** | 1.00 → **0.64** |
| memory | ~10.9k → **~14.2k** | ~3.6k → **~8.5k** | 1.00 → **0.65** |
| sqlite | ~3.6k → ~3.1k        | ~1.3k → ~0.85k    | **1.00** (unchanged) |

---

## 3. Matched (P = C) — `publish / delivery / ack_ratio`

| P=C | none | memory | sqlite |
|--:|--|--|--|
| 1  | 3752 / 6701 / 1.00 | 4131 / 5443 / 0.96 | 844 / 1688 / 1.00 |
| 2  | 4619 / 7507 / 0.87 | 4808 / 7895 / 0.88 | 624 / 1872 / 1.00 |
| 4  | 3957 / 11280 / 0.82 | 3852 / 10984 / 0.82 | 492 / 2452 / 1.00 |
| 8  | 2316 / 11755 / 0.79 | 2286 / 11470 / 0.79 | 297 / 2643 / 1.00 |
| 16 | 1733 / 13970 / 0.71 | 1716 / 13665 / 0.71 | 191 / 3124 / 1.00 |

Delivery climbs monotonically with fanout width (6.7k → **14.0k** at P16) — the sync path's whole
point — while `ack_ratio` slides 1.00 → **0.71** as delivery outpaces acks. `sqlite` flat + clean.

## 4. Producer-heavy (C ∈ {1,2,4}) — publish uncapped, mild churn

| P:C | none pub/deliv/ackr | memory pub/deliv/ackr | sqlite pub/deliv/ackr |
|--:|--|--|--|
| 32:1 | 7449 / 5861 / 0.90 | 7690 / 5903 / 0.90 | 769 / 1537 / 1.00 |
| 32:4 | 4037 / 11351 / 0.83 | 3582 / 10117 / 0.82 | 456 / 2274 / 1.00 |
| 64:1 | 7872 / 6058 / 0.90 | 8326 / 6287 / 0.90 | 782 / 1564 / 1.00 |
| 64:4 | 3243 / 9326 / 0.82 | 3851 / 10981 / 0.83 | 454 / 2265 / 1.00 |
| 128:1 | 8511 / 6197 / 0.90 | 8516 / 5771 / 0.89 | 838 / 1676 / 1.00 |
| 128:4 | 3607 / 10335 / 0.83 | 3775 / 10839 / 0.83 | 495 / 2467 / 1.00 |

The v0.2.1 win here was "publish self-paces to ~3.5k, ack_ratio 1.0." sync-fanout **reopens accept
to ~8.5k** (7.7× the v0.2.1 P128:C1 figure) and trades it for `ack_ratio` ~0.90 + light retry churn
(`retry_exh` ~45). Not a collapse — delivery holds/rises — but the publish back-pressure guarantee is
weaker. `sqlite` immune (fsync paces accept below the over-delivery threshold).

## 5. Subscriber-heavy (P ∈ {1,2,4}) — biggest delivery gains, deepest ack dips

| P:C | none pub/deliv/ackr/retry/fanout | memory pub/deliv/ackr/retry/fanout | sqlite |
|--:|--|--|--|
| 1:32 | 486 / 12431 / 0.96 / 201 / 25.6× | 542 / 13711 / 0.94 / 232 / 25.3× | 102 / 2886 / 1.00 / 0 / 28.4× |
| 1:64 | 267 / 11005 / 0.95 / 277 / 41.3× | 306 / 11950 / 0.95 / 316 / 39.1× | 69 / 2625 / 1.00 / 0 / 37.9× |
| 2:32 | 1017 / 13258 / 0.73 / 614 / 13.0× | 647 / 10669 / 0.84 / 457 / 16.5× | 101 / 2853 / 1.00 / 0 / 28.4× |
| 2:64 | 536 / 12488 / 0.80 / 711 / 23.3× | 457 / 11001 / 0.81 / 685 / 24.1× | 75 / 2855 / 1.00 / 0 / 38.0× |
| 4:32 | 1311 / 14700 / 0.64 / 682 / 11.2× | 1265 / 14220 / 0.65 / 670 / 11.2× | 111 / 3142 / 1.00 / 0 / 28.4× |
| 4:64 | 828 / 12724 / 0.65 / 797 / 15.4× | 809 / 12186 / 0.67 / 741 / 15.1× | 75 / 2831 / 1.00 / 0 / 37.9× |

`none P4:C32` is the ceiling cell: **14.7k deliv/s** (vs v0.2.1's subscriber-heavy ~4.6–5.3k) — a
**~2.8× delivery gain** — at `ack_ratio` **0.64**, `retry_exh` 682. `P1:C*` (single producer) keeps
`ack_ratio` ≥0.93: one producer can't over-fill the fanout, so the sync path stays fair there. The
churn scales with **producer** count (P≥2), which is what feeds the sync fast-path faster than
consumers ack. `sqlite` clean at every fanout width, delivering full `28–38×` fanout with `re0`.

---

## 6. Interpretation — a fanout fast-path that removed a yield point

The signature (higher delivery + higher publish accept + lower ack_ratio + broad retry churn on
loop-bound backends only, `sqlite` immune) points to the sync-fanout change **removing or shortening
a yield/await in the delivery-enqueue path** relative to v0.2.1:

- Delivery per unit CPU rises (less per-message scheduling overhead) → higher ceiling.
- Publish accept is no longer gated as tightly by fanout progress → accept climbs to ~8.5k.
- Longer non-yielding fanout bursts starve the ack-processing path → deliveries outrun acks within
  the retry budget → redelivery + `retry_exh` churn. This matches the predicted "spends fairness"
  side of the sync-vs-worker trade.
- `sqlite`'s fsync wall paces publish below the over-delivery point, so none of it manifests.

**This does not raise the single-thread CPU wall via parallelism** — it reclaims per-delivery
overhead on the one core. Consistent with the matrix thesis that ~real parallelism (shard loop) is
the only thing that clears the extreme-consumer line; sync-fanout just moves *this* backend closer to
its own single-core wall and pushes the ack path past its fairness budget getting there.

---

**Code/deps this session:** `pyproject.toml` → `pubsub-py = { …, branch = "perf/sync-fanout" }`,
extras `[otel,fast]` kept; `uv.lock` re-locked to `f799f70`; docker image rebuilt (uvloop 0.22.1,
lib branch). Driver: `performance_harness/matrix_driver.py` (69 cells, 16 s settle + 16 s OTel delta
scrape at `:9464`). Baseline: `04-matrix-uvloop.md`.
