# pubsub.py — Matrix Rerun on `[otel,fast]` (uvloop + v0.2.1)

> Raw appendix — the run-by-run record behind the library README's Performance summary. Superseded by it
> for conclusions; kept for method and per-regime evidence. Absolute rates are
> host-bound; read them as shape, not spec.

**Date:** 2026-07-22 (fourth set — dependency upgrade)
**Change under test:** `pubsub-py[otel]` → `pubsub-py[otel,fast]` (declares uvloop) **and** lib bump to
**v0.2.1** (current master). Harness entrypoints now select uvloop via `config.run_async` — broker
startup log confirms `loop=uvloop`. **Both moved at once — see the confound note (§6).**
**Everything else identical** to `03-matrix.md`: same 3 backends × 23 topologies,
`PRODUCER_RATE=max`, poison+nack off, per-step fresh stack, 16 s settle + 16 s OTel sample.

---

## 1. Headline — the producer-heavy collapse is gone

The single biggest change from the stock-asyncio baseline: **congestion collapse under producer flood
is eliminated on every backend.** The runaway publish-accept (15k/s while delivery starved) no longer
happens — publish now **self-paces to delivery** and `ack_ratio` stays 1.0.

### Producer-heavy, before → after (publish / delivery / ack_ratio / retry_exh)
| cell | baseline (stock asyncio) | now (uvloop + v0.2.1) |
|--|--|--|
| none P128:C1   | 15619 / 489 / **0.40** / 293 | 1103 / **2206** / **1.00** / 0 |
| none P64:C1    | 14837 / 719 / **0.48** / 298 | 2405 / **4811** / **1.00** / 0 |
| none P32:C1    | 14988 / 1193 / **0.45** / 594 | 2953 / **5906** / **1.00** / 0 |
| memory P128:C1 | 5923 / **12** / — / 89        | 1216 / **2432** / **1.00** / 0 |
| memory P64:C4  | 5461 / 623 / **0.31** / 406   | 1149 / **5507** / **1.00** / 0 |
| memory P128:C4 | 6504 / 374 / **0.34** / 239   | 662 / **3201** / **1.00** / 0 |

Every producer-heavy cell (all 9 × none, all 9 × memory) is now `ack_ratio` 1.0, `retry_exh` 0, with
**no runaway publish** — max publish-accept dropped from ~15.6k/s to ~3.5k/s because accept is now
coupled to fanout progress (back-pressure), not fire-and-forget.

## …but the instability moved, it didn't vanish

The churn relocated to **subscriber-heavy** on the loop-bound backends (none/memory). Fast loop +
per-step reset makes 32–128 consumers subscribe in a storm; during the sample some subs are still
establishing → fanout undercounts and transient eviction/retry appears.

| cell | baseline | now |
|--|--|--|
| none P4:C64   | 54 / 3476 / **1.00** / 0 / 64× | 283 / 4593 / **0.81** / 717 / 16× |
| none P2:C64   | 63 / 3997 / **1.00** / 0 / 64× | 238 / 4965 / **0.81** / 600 / 21× |
| memory P4:C64 | 65 / 4172 / **1.00** / 0 / 64× | 319 / 5305 / **0.79** / 826 / 17× |

**`sqlite` shows no such churn** — `ack_ratio` 1.0, `retry_exh` 0 in *all 23 cells*, subscriber-heavy
included. Its fsync-paced publish is too slow to trigger the subscribe-storm. sqlite is now the only
backend clean in every corner.

---

## 2. Ceilings — modestly higher, collapse-free

| backend | delivery ceiling (base → now) | loop-op wall (base → now) | publish-accept peak (base → now) |
|--|--|--|--|
| none   | 9.6k → **~10.1k** | 19k → **~20k** | **15.6k → 3.5k** (runaway removed) |
| memory | 9.3k → **~10.9k** | 18.6k → **~21.7k** | 14.0k → 3.6k |
| sqlite | 2.8k → **~3.6k**  | 5.7k → **~7.3k**  | 1.1k → 1.3k |

uvloop lifts the sustained ceiling ~10–20%. The publish-accept "peak" *falls* — but that number was
always pathological (delivery-starved). Removing it is the point: real end-to-end throughput is up and
no longer bought with an `ack_ratio` 0.4 retry storm.

---

## 3. Matched (P = C) — `publish / delivery`, all ack_ratio 1.0

| P=C | none | memory | sqlite |
|--:|--:|--:|--:|
| 1  | 3479 / 6737 | 2825 / 5650 | 634 / 1268 |
| 2  | 2917 / 8481 | 2795 / 7522 | 766 / 2296 |
| 4  | 1773 / 8776 | 1934 / 9500 | 627 / 3123 |
| 8  | 1748 / 9386 | 1391 / 9869 | 362 / 3207 |
| 16 | 1013 / 9012 | 1142 / **10860** | 227 / **3633** |

## 4. Producer-heavy (C ∈ {1,2,4}) — now uniformly healthy

| P:C | none pub/deliv | memory pub/deliv | sqlite pub/deliv |
|--:|--:|--:|--:|
| 32:1 | 2953 / 5906 | 2967 / 5934 | 1328 / 2656 |
| 32:4 | 1840 / 8392 | 1688 / 7979 | 613 / 3016 |
| 64:1 | 2405 / 4811 | 2380 / 4760 | 1202 / 2404 |
| 64:4 | 889 / 4301  | 1149 / 5507 | 567 / 2751 |
| 128:1 | 1103 / 2206 | 1216 / 2432 | 753 / 1506 |
| 128:4 | 755 / 3537  | 662 / 3201  | 308 / 1438 |

All cells: `ack_ratio` 1.0, `retry_exh` 0. (Compare §1 — every one of these was a red collapse before.)

## 5. Subscriber-heavy (P ∈ {1,2,4}) — churn on none/memory, clean on sqlite

| P:C | none pub/deliv/ackr/retry | memory pub/deliv/ackr/retry | sqlite pub/deliv/ackr/retry |
|--:|--:|--:|--:|
| 2:32 | 659 / 10155 / 1.00 / 279 | 567 / 8836 / 1.00 / 407 | 118 / 3376 / 1.00 / 0 |
| 4:32 | 586 / 8670 / 0.89 / 986  | 598 / 8873 / 0.93 / 686 | 121 / 3434 / 1.00 / 0 |
| 2:64 | 238 / 4965 / 0.81 / 600  | 253 / 4893 / 0.83 / 794 | 62 / 2163 / 1.00 / 0 |
| 4:64 | 283 / 4593 / 0.81 / 717  | 319 / 5305 / 0.79 / 826 | 65 / 2256 / 1.00 / 0 |
| 2:128 | 131 / 1439 / 1.00 / 217 | 139 / 1495 / 1.00 / 233 | 61 / 889 / 1.02 / 0 |

`none`/`memory` at C=64 dip to `ack_ratio` ~0.8 with retry churn — a *subscribe-storm artifact*, not a
throughput collapse (delivery still 4.6–5.3k). Fanout undercounts (~16–21× vs 64×) because consumers
are still joining mid-sample. `sqlite` immune (fsync paces publish below the storm threshold).

---

## 6. Confound — uvloop AND lib v0.2.1 both changed

Two variables moved together; attribute with care:

- **Collapse elimination + publish self-pacing** → most likely **lib v0.2.1** (publish-accept now coupled
  to fanout/back-pressure). A faster loop alone would not *cap* publish at 3.5k — it would raise it. The
  disappearance of the 15k/s runaway is a semantics change, not a speed change.
- **~10–20% higher delivery ceiling** and the **faster subscribe-storm** (→ new subscriber-heavy churn)
  → most likely **uvloop** (raw loop throughput + faster connection/subscribe handling).
- **sqlite ceiling ~2.8k → ~3.6k** → could be either v0.2.1 sqlite path (e.g. WAL `synchronous=NORMAL`
  default in the new runner) or uvloop; disk-bound so uvloop effect should be small — lean v0.2.1.

To separate cleanly: an A/B holding the lib at v0.2.1 and toggling only `config.run_async`'s uvloop
branch (stdlib vs uvloop). Not run here.

---

**Code/deps this session:** `pyproject.toml` → `pubsub-py[otel,fast]`; `uv.lock` refreshed (uvloop
0.22.1, lib v0.2.1); `config.run_async` (uvloop-or-stdlib) wired into `controller/producer/consumer`;
broker logs `loop=`. Baseline for all before/after: `03-matrix.md`.
