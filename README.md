# pubsub.py — benchmark harnesses

Load-testing rigs that drive the real [`pubsub.py`](https://github.com/matthew-brough/pubsub.py)
TCP broker in Docker and scrape cluster-aggregated stats. This repo is the
**method + apparatus**; the findings it produced are synthesised in the library
README's [Performance](https://github.com/matthew-brough/pubsub.py#performance)
section — go there for results, ratios, and what they mean.

Every harness reports **relative behaviour** (ratios, percentages, quality
metrics like `ackr` / `balance` / `repl_factor`), never absolute throughput —
that is host-bound and deliberately omitted.

## The three rigs

| Harness | Topology | What it exercises |
|---|---|---|
| [`performance_harness`](performance_harness) | single broker, sqlite durability, `:8080` dashboard | raw one-node throughput, retry → DLQ churn, fanout cost. Scrape point: one broker `:9464`. |
| [`ingress_balanced_harness`](ingress_balanced_harness) | `B` brokers, nginx LBs **producer** ingress; consumers **fan in** (one sub per broker) | sharded ingest (each msg on one broker, union = global stream), shared durable log, `C × B` fan-in connection wall. |
| [`egress_balanced_harness`](egress_balanced_harness) | `B` brokers, producers **broadcast** to every broker; nginx LBs **consumer** egress | replicated ingest (`repl_factor ≈ B`), sharded egress (`C` connections), per-broker `broker_id` audit trail. |

Shared shape across the two distributed rigs: elastic brokers (`--scale
broker=N`, DNS discovery, nginx OSS `resolve` + `least_conn`), a **Postgres**
durable layer as scaffolding (any DB works — this is deployment plumbing, not a
library property), and an `observer` that aggregates the cluster firehose /
per-broker OTel. Per-harness env knobs (`DURABILITY`, `PG_MAX_WRITERS`,
`PRODUCER_RATE`, fanout width, …) are documented in each harness `README`.

Single-node and egress rigs pin upstream `pubsub.py` master at `b7f1651a`.
Both require authenticated connections and claimed publish subjects. Broker
startup probes rejected/valid auth, claim/release, and packed-delivery decoding;
an isolated TLS 1.2+ listener repeats that probe before closing. Fanout matrix
cells then exercise packed-message reuse under authenticated plaintext load.

## Drivers

- `performance_harness/matrix_driver.py` — single-node matrix. Scrapes one
  broker at `:9464`; breaks under `--scale broker=N` by design.
- `dist_matrix_driver.py` — distributed matrix for **both** topologies. Scrapes
  the observer's cluster-aggregated `/api/stats` (`:8080`), records logical vs
  aggregate throughput planes (the ratio is the replication tax), per-broker
  balance, ack/retry churn, and the Postgres audit trail. Runs one cell at a
  time (fresh stack, `down -v` between). Flags: `--harness {ingress,egress}`,
  `--stages`, `--sweep88`, `--broker-sweep`, `--dry-run`.

## Reproduce

```bash
# single node
cd performance_harness && docker compose up --build            # :8080 dashboard
python matrix_driver.py

# distributed (example: egress, 4 brokers, 24 consumers)
cd egress_balanced_harness && docker compose up --build --scale broker=4 --scale consumer=24
python ../dist_matrix_driver.py --harness egress --stages T1
```

Driver run outputs (`*_results*.json`, `*_progress*.log`, `*.out`) are
**gitignored** — reproduce them from the drivers, don't commit them.

## `docs/` — raw appendix

The unabridged per-run measurement writeups that predate and back the library
README's Performance summary, kept as raw source (not maintained as living
docs). Numbered in run order:

- `01-perf-log.md` — single-node publish-accept ceiling; sqlite-vs-memory.
- `02-rerun.md` — post-broker-fix rerun (collapse gone; memory scales).
- `03-matrix.md` — durability × topology matrix (none/memory/sqlite).
- `04-matrix-uvloop.md` — matrix on `[otel,fast]` (uvloop + lib v0.2.1).
- `05-matrix-sync-fanout.md` — matrix on the `perf/sync-fanout` branch.
- `06-distributed.md` — multi-broker ingress-vs-egress distributed run.
