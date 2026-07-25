#!/usr/bin/env python3
"""Distributed-harness bench driver (ingress + egress topologies).

Unlike performance_harness/matrix_driver.py — which scrapes ONE broker at
:9464 and therefore breaks under --scale broker=N — this driver scrapes the
observer's already-cluster-aggregated /api/stats (:8080). Per cell it records
two throughput planes that only a distributed run distinguishes:

  * LOGICAL throughput  — observer firehose `total`/`rate_sustained` (dedup):
    the real msgs/s the cluster moved. Ingress observer fans the firehose IN
    across every broker (sharded ingest, union = whole stream); egress reads
    ONE broker via the LB (replicated ingest, one broker = whole stream). So
    this number is apples-to-apples across both topologies.
  * AGGREGATE broker work — summed broker OTel (`broker_otel.totals`): equals
    logical for ingress (each msg lands once) but is Y× logical for egress
    (every broker persists every broadcast). The ratio IS the replication tax.

Plus per-broker balance (is nginx least_conn spreading?), ack_ratio / retry
churn (fairness), fleet self-report, and the Postgres audit trail (rows
persisted / dead-lettered per broker).

Container cap: every cell asserts producers + brokers + consumers + observer
<= MAX_CONTAINERS (129, per operator constraint). postgres + nginx are infra,
not counted. Cells run one at a time (fresh stack, `down -v` between), so peak
concurrent app-containers == the largest single cell.

Usage:
  python dist_matrix_driver.py --harness ingress --dry-run
  python dist_matrix_driver.py --harness egress --stages T1
  python dist_matrix_driver.py --harness ingress            # all stages

Emits <harness>_dist_results.json (incremental) and <harness>_dist_progress.log.
"""
import argparse
import json
import os
import subprocess
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
MAX_CONTAINERS = 129  # producers + brokers + consumers + observer ceiling
OBS_URL = "http://localhost:8080/api/stats"      # rich (firehose+PG); can stall at giant scale
OTEL_URL = "http://localhost:8080/api/otel"      # cheap cached broker-OTel; always responsive

SETTLE = 18.0
SAMPLE = 16.0
HEALTH_TIMEOUT = 300.0  # giant-fanout cells start slowly (staggered joins)

HARNESS_DIR = {
    "ingress": os.path.join(HERE, "ingress_balanced_harness"),
    "egress": os.path.join(HERE, "egress_balanced_harness"),
}

# Bench cells per harness. Each: (stage, P, B, C, note, env-overrides).
# P=producer replicas, B=broker replicas, C=consumer replicas. Capstone (giant,
# near the 129 cap) is the LAST cell of each stage, per the run constraint.
_MAX = {"PRODUCER_RATE": "max", "NACK_RATE": "0", "POISON_EVERY": "0"}


def _cells(harness: str) -> list[dict]:
    max_env = dict(_MAX)
    out: list[dict] = []

    def add(stage, p, b, c, note, env=None):
        e = dict(max_env)
        if env:
            e.update(env)
        # Giant-fanout hardening, derived from C (not per-cell overridable):
        #  * disable the observer firehose past 48 consumers (it starves the
        #    observer loop + undercounts) — throughput then comes from broker OTel;
        #  * stagger the consumer join storm for large fleets.
        e["OBSERVER_FIREHOSE"] = "0" if c > 48 else "1"
        if c >= 32:
            e["CONSUMER_JOIN_STAGGER"] = str(min(round(c * 0.2, 1), 30.0))
        out.append({"stage": stage, "P": p, "B": b, "C": c, "note": note, "env": e})

    if harness == "ingress":
        # T1 smoke — default topology, DLQ/audit exercised (poison+nack ON).
        add("T1", 3, 2, 3, "smoke default 3:2:3",
            {"PRODUCER_RATE": "2000", "NACK_RATE": "0.02", "POISON_EVERY": "500"})
        # T2 intended — analytics fan-in: matched + subscriber-heavy.
        for p in (2, 4, 8):
            add("T2", p, 2, p, f"matched {p}:2:{p}")
        for c in (8, 16, 32):
            add("T2", 3, 2, c, f"sub-heavy 3:2:{c}")
        add("T2", 4, 4, 120, "CAPSTONE giant fan-in 4:4:120")  # 129
        # T3 inverted — producer-heavy flood on sharded ingest + fan-in.
        for p in (16, 32):
            for c in (2, 4):
                add("T3", p, 2, c, f"prod-heavy {p}:2:{c}")
        add("T3", 120, 4, 4, "CAPSTONE producer flood 120:4:4")  # 129
        # T4 broker-scaling — fixed load P4:C48, sweep brokers.
        for b in (1, 2, 4, 8):
            add("T4", 4, b, 48, f"scale broker={b} (P4:C48)")
        # (8-broker giant-fanout capstone 4:8:116 CUT — non-viable: the observer
        #  can't be sampled while scraping 8 brokers + 8 fleet conns under a
        #  120-consumer flood. Viable top-end is the 4-broker *:4:120 class.)
        # T5 PG knobs — rep cell P4:C24 B2; sync_commit x max_writers.
        for sc in ("on", "off"):
            for w in ("1", "4", "8"):
                add("T5", 4, 2, 24, f"pg sync={sc} writers={w}",
                    {"PG_SYNCHRONOUS_COMMIT": sc, "PG_MAX_WRITERS": w})
        add("T5", 4, 4, 120, "CAPSTONE pg ceiling sync=off writers=8",
            {"PG_SYNCHRONOUS_COMMIT": "off", "PG_MAX_WRITERS": "8"})  # 129
        # T6 durability backend — none|memory|postgres at a mid cell + capstone.
        # none/memory are per-broker (no shared audit); isolates transport/fanout
        # from the shared DB. sqlite is out of scope (single-writer, distributed
        # brokers can't share it). Capstone goes last, per stage.
        for dur in ("none", "memory", "postgres"):
            add("T6", 4, 2, 8, f"backend={dur} (mid 4:2:8)", {"DURABILITY": dur})
        for dur in ("none", "memory", "postgres"):
            add("T6", 4, 4, 120, f"CAPSTONE backend={dur} 4:4:120", {"DURABILITY": dur})

    elif harness == "egress":
        # T1 smoke — default topology, DLQ/audit exercised.
        add("T1", 2, 2, 6, "smoke default 2:2:6",
            {"PRODUCER_RATE": "2000", "NACK_RATE": "0.02", "POISON_EVERY": "500"})
        # T2 intended — few producers, giant consumers (the fan-out headline).
        for c in (6, 12, 24, 48):
            add("T2", 2, 2, c, f"few-prod/giant-consumer 2:2:{c}")
        add("T2", 4, 4, 120, "CAPSTONE giant consumer fanout 4:4:120")  # 129
        # T3 inverted — many broadcasting producers = X*Y write amplification.
        for p in (8, 16):
            for b in (2, 4):
                add("T3", p, b, 4, f"broadcast storm {p}:{b}:4")
        add("T3", 120, 2, 4, "CAPSTONE broadcast storm 120:2:4")  # 127
        # T4 broker-scaling — fixed load P2:C48, sweep brokers.
        for b in (1, 2, 4, 8):
            add("T4", 2, b, 48, f"scale broker={b} (P2:C48)")
        # 8-broker giant capstone kept for egress: consumers pin to ONE broker
        # (sharded egress, no fan-in), so per-broker load is ~C/8 vs ingress's
        # full-fanout-on-every-broker — the observer may stay sampleable here
        # where the ingress 4:8:116 could not. Probes the top end.
        add("T4", 2, 8, 116, "CAPSTONE sharded consumers @8 brokers 2:8:116")  # 127
        # T5 PG knobs — rep cell P2:C24 B2.
        for sc in ("on", "off"):
            for w in ("1", "4", "8"):
                add("T5", 2, 2, 24, f"pg sync={sc} writers={w}",
                    {"PG_SYNCHRONOUS_COMMIT": sc, "PG_MAX_WRITERS": w})
        add("T5", 4, 4, 120, "CAPSTONE pg ceiling sync=off writers=8",
            {"PG_SYNCHRONOUS_COMMIT": "off", "PG_MAX_WRITERS": "8"})  # 129
        # T6 durability backend — none|memory|postgres at a mid cell + capstone.
        for dur in ("none", "memory", "postgres"):
            add("T6", 2, 2, 24, f"backend={dur} (mid 2:2:24)", {"DURABILITY": dur})
        for dur in ("none", "memory", "postgres"):
            add("T6", 4, 4, 120, f"CAPSTONE backend={dur} 4:4:120", {"DURABILITY": dur})
        # T7 egress top-end: 8 producers broadcast to 8 brokers (~8x replication
        # tax on shared PG), sweep consumers to the container cap. How far does
        # sharded egress scale the consumer fleet at max brokers? 8+8+C+1<=129.
        for c in (24, 48, 80, 112):
            add("T7", 8, 8, c, f"8:8 consumer sweep 8:8:{c}")
    else:
        raise SystemExit(f"unknown harness {harness!r}")

    for cell in out:
        n = cell["P"] + cell["B"] + cell["C"] + 1  # +observer
        cell["containers"] = n
        if n > MAX_CONTAINERS:
            raise SystemExit(
                f"cell {cell['note']} needs {n} app-containers > {MAX_CONTAINERS} cap")
    return out


def make_logger(path):
    def log(msg):
        line = f"{time.strftime('%H:%M:%S')} | {msg}"
        print(line, flush=True)
        with open(path, "a") as f:
            f.write(line + "\n")
    return log


def scrape(url=OBS_URL, timeout=4.0):
    """GET an observer JSON endpoint, or None if unreachable."""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except Exception:
        return None


def scrape_otel():
    """Cheap firehose-free view: {'otel': {...}, 'brokers': [...]} or None."""
    return scrape(OTEL_URL, timeout=10.0)


def scrape_otel_retry(tries=5, gap=3.0):
    """scrape_otel with retries — tolerate a transient observer stall at giant scale."""
    for _ in range(tries):
        s = scrape_otel()
        if s and s.get("otel", {}).get("ok"):
            return s
        time.sleep(gap)
    return None


def compose(cwd, args, env=None, check=True, capture=True):
    e = dict(os.environ)
    if env:
        e.update(env)
    return subprocess.run(
        ["docker", "compose"] + args,
        cwd=cwd, env=e, check=check,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.STDOUT if capture else None,
        text=True,
    )


def _free_mem_gb():
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) / (1024 * 1024)
    except Exception:
        pass
    return 0.0


def teardown(cwd, min_free_gb=15.0):
    """Fully tear down + WAIT until host RESOURCES recover before the next `up`.

    Back-to-back giant cells (129 containers) otherwise fail: `compose ps` reads
    empty within seconds, but the 120 just-killed processes' memory isn't
    reclaimed and the bridge network lingers, so the next stack's observer never
    binds/serves (readiness times out with an empty `last:`). Block on container
    records gone, network pruned, AND free memory back above a floor.
    """
    compose(cwd, ["down", "-v", "--remove-orphans", "-t", "5"], check=False)
    for _ in range(60):
        r = compose(cwd, ["ps", "-q"], check=False)
        if not (r.stdout or "").strip():
            break
        time.sleep(1.5)
    # Reap lingering bridge networks from prior giant stacks (best effort).
    try:
        subprocess.run(["docker", "network", "prune", "-f"],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
    except Exception:
        pass
    # Wait for the kernel to actually free the killed processes' memory.
    for _ in range(40):
        if _free_mem_gb() >= min_free_gb:
            break
        time.sleep(2.0)
    time.sleep(6.0)


def wait_ready(deadline, log):
    """Wait until brokers are up AND publishing has started (via cheap /api/otel)."""
    start = time.time()
    last = ""
    while time.time() - start < deadline:
        s = scrape_otel()
        if s is not None:
            otel = s.get("otel", {})
            pub = otel.get("totals", {}).get("publishes_ok", 0)
            if otel.get("ok") and pub > 0:
                return True
            last = f"otel_ok={otel.get('ok')} pub_ok={pub}"
        time.sleep(2.0)
    log(f"    !! not ready in {deadline:.0f}s (last: {last})")
    return False


def run_cell(cwd, cell, log):
    p, b, c = cell["P"], cell["B"], cell["C"]
    log(f"  [{cell['stage']}] {cell['note']}  ({cell['containers']} app-containers)")
    teardown(cwd)
    compose(cwd, ["up", "-d",
                  "--scale", f"broker={b}",
                  "--scale", f"producer={p}",
                  "--scale", f"consumer={c}"], env=cell["env"])
    if not wait_ready(HEALTH_TIMEOUT, log):
        compose(cwd, ["down", "-v", "--remove-orphans"], check=False)
        return None
    # Settle past the consumer join stagger before sampling.
    stagger = float(cell["env"].get("CONSUMER_JOIN_STAGGER", "0"))
    settle = max(SETTLE, stagger + 12.0)
    time.sleep(settle)

    # Throughput + per-broker balance from the cheap, firehose-free OTel endpoint
    # (authoritative and responsive even at giant fanout).
    a0 = scrape_otel_retry()
    t0 = time.time()
    time.sleep(SAMPLE)
    a1 = scrape_otel_retry()
    t1 = time.time()
    if not a0 or not a1:
        log("    !! otel scrape failed during sample (after retries)")
        compose(cwd, ["down", "-v", "--remove-orphans"], check=False)
        return None
    dt = t1 - t0
    o0 = a0["otel"]["totals"]
    o1 = a1["otel"]["totals"]

    def d(key):
        return o1.get(key, 0) - o0.get(key, 0)

    agg_pub = d("publishes_ok") / dt
    agg_deliv = d("deliveries") / dt
    ndeliv = d("deliveries")
    ack_ratio = round(d("acks") / ndeliv, 3) if ndeliv > 0 else None
    fanout = round(d("deliveries") / d("publishes_ok"), 1) if d("publishes_ok") > 0 else None

    # Per-broker delivery balance (nginx least_conn / broadcast spread quality).
    per = [(bk["name"], bk.get("rates", {}).get("deliveries", 0.0))
           for bk in a1.get("brokers", []) if bk.get("ok")]
    dv = sorted(v for _, v in per)
    balance = round(dv[0] / dv[-1], 2) if dv and dv[-1] > 0 else None

    # Best-effort rich snapshot for latency / fleet / audit / logical. This hits
    # the firehose+PG endpoint, which can be slow at giant fanout — tolerate None.
    s1 = scrape(timeout=8.0) or {}
    fleet = s1.get("fleet", {})
    logical_pub = fleet.get("pub_rate")  # producer self-report — Y-safe (counted once)
    # Replication factor: aggregate broker publishes / logical publishes.
    #   ingress (sharded ingest) ~ 1 ; egress (broadcast) ~ B (the tax).
    repl = round(agg_pub / logical_pub, 2) if logical_pub else None
    firehose_on = cell["env"].get("OBSERVER_FIREHOSE", "1") == "1"
    audit = s1.get("audit", []) or []

    res = {
        "stage": cell["stage"], "note": cell["note"],
        "P": p, "B": b, "C": c, "containers": cell["containers"],
        "backend": cell["env"].get("DURABILITY", "postgres"),
        "firehose": firehose_on,
        "env": {k: v for k, v in cell["env"].items()
                if k in ("PRODUCER_RATE", "DURABILITY", "PG_SYNCHRONOUS_COMMIT",
                         "PG_MAX_WRITERS", "NACK_RATE", "POISON_EVERY",
                         "CONSUMER_JOIN_STAGGER")},
        # aggregate broker work (authoritative; Y-amplified publishes in egress)
        "agg_publish": round(agg_pub, 1),
        "agg_deliv": round(agg_deliv, 1),
        "agg_reject": round(d("publishes_rej") / dt, 1),
        "ack_ratio": ack_ratio,
        "fanout": fanout,
        "retry_exh": int(round(d("retry_exhausted"))),
        "repl_factor": repl,           # agg_publish / logical_publish (~B egress, ~1 ingress)
        "logical_pub": logical_pub,    # producer self-reported logical publish/s
        # per-broker balance + broker count
        "brokers_seen": len(per),
        "balance_min_over_max": balance,
        # rich (best-effort; null at giant scale when firehose off / endpoint slow)
        "logical_deliv": s1.get("rate_sustained") if firehose_on else None,
        "lat_p50_ms": s1.get("latency_ms", {}).get("p50") if firehose_on else None,
        "lat_p95_ms": s1.get("latency_ms", {}).get("p95") if firehose_on else None,
        "lat_p99_ms": s1.get("latency_ms", {}).get("p99") if firehose_on else None,
        "fleet_producers": fleet.get("n_producers"),
        "fleet_consumers": fleet.get("n_consumers"),
        "audit": [{"broker_id": a.get("broker_id"),
                   "persisted": a.get("persisted"),
                   "dlq": a.get("dlq")} for a in audit],
        "dt": round(dt, 1),
    }
    log(f"    agg_deliv {res['agg_deliv']}/s  pub {res['agg_publish']}/s "
        f"repl {res['repl_factor']}x  ackr {res['ack_ratio']} fanout {res['fanout']} "
        f"retry_exh {res['retry_exh']} bal {res['balance_min_over_max']} "
        f"brokers {res['brokers_seen']}/{b} fleet_c {res['fleet_consumers']}/{c} "
        f"p95 {res['lat_p95_ms']} fh {int(firehose_on)}")
    compose(cwd, ["down", "-v", "--remove-orphans"], check=False)
    return res


def sweep_88(cwd, start, step, log, results_path, pg_writers=None, durability=None):
    """Egress 8:8 top-end probe: fixed 8 producers x 8 brokers, grow consumers by
    `step` from `start`. NO container cap. Stops on the FIRST ceiling hit:
      * keepalive   — cell fails to come ready / be sampled (host wall),
      * fairness    — ack_ratio < 0.85 (overload, acks can't keep up),
      * throughput  — agg_deliv < 60% of the running peak for 2 cells (collapse).
    Reports which ceiling and the last good C.

    ``pg_writers`` overrides PG_MAX_WRITERS; ``durability`` overrides DURABILITY
    (e.g. "none" — uncapped publish, where the fairness cliff actually appears).
    """
    log("BUILD ...")
    compose(cwd, ["build"])
    log("BUILD done")
    results = []
    peak_deliv = 0.0
    low_streak = 0
    c = start
    while True:
        env = dict(_MAX)  # PRODUCER_RATE=max, nack/poison off
        if pg_writers is not None:
            env["PG_MAX_WRITERS"] = str(pg_writers)
        if durability is not None:
            env["DURABILITY"] = durability
        env["OBSERVER_FIREHOSE"] = "0" if c > 48 else "1"
        env["CONSUMER_JOIN_STAGGER"] = str(min(round(c * 0.2, 1), 30.0))
        cell = {"stage": "SWEEP", "P": 8, "B": 8, "C": c,
                "note": f"8:8:{c}", "env": env, "containers": 8 + 8 + c + 1}
        log(f"[C={c}]  {cell['containers']} app-containers")
        res = run_cell(cwd, cell, log)
        if res is None:
            log(f"KEEPALIVE CEILING: 8:8:{c} failed to come ready/sample at "
                f"{cell['containers']} containers. Last good C={c - step}.")
            break
        results.append(res)
        with open(results_path, "w") as f:
            json.dump(results, f, indent=2)
        log(f"  ok C={c} (agg_deliv {res['agg_deliv']}/s repl {res['repl_factor']}x "
            f"ackr {res['ack_ratio']} bal {res['balance_min_over_max']} retry {res['retry_exh']})")
        ackr = res.get("ack_ratio")
        dv = res.get("agg_deliv") or 0.0
        peak_deliv = max(peak_deliv, dv)
        if ackr is not None and ackr < 0.85:
            log(f"FAIRNESS CEILING: 8:8:{c} ack_ratio {ackr} < 0.85 — stop.")
            break
        if dv < 0.6 * peak_deliv:
            low_streak += 1
            if low_streak >= 2:
                log(f"THROUGHPUT CEILING: 8:8:{c} agg_deliv {dv:.0f} < 60% of peak "
                    f"{peak_deliv:.0f} for {low_streak} cells — collapse.")
                break
        else:
            low_streak = 0
        c += step
    log(f"DONE {len(results)} good cells -> {results_path}")


def broker_sweep(cwd, p, c, blist, durability, log, results_path):
    """Fixed P producers : C consumers, sweep brokers over `blist`. Tests whether
    broker scaling continues past B=2 on a given backend (e.g. none) — isolating
    the shared-Postgres write wall from broker/host limits.
    """
    log("BUILD ...")
    compose(cwd, ["build"])
    log("BUILD done")
    results = []
    for b in blist:
        env = dict(_MAX)
        if durability is not None:
            env["DURABILITY"] = durability
        env["OBSERVER_FIREHOSE"] = "0" if c > 48 else "1"
        env["CONSUMER_JOIN_STAGGER"] = str(min(round(c * 0.2, 1), 30.0))
        cell = {"stage": "BSWEEP", "P": p, "B": b, "C": c,
                "note": f"{durability or 'postgres'} {p}:{b}:{c}",
                "env": env, "containers": p + b + c + 1}
        log(f"[B={b}]  {cell['containers']} app-containers")
        res = run_cell(cwd, cell, log)
        if res is None:
            log(f"  !! B={b} failed")
            continue
        results.append(res)
        with open(results_path, "w") as f:
            json.dump(results, f, indent=2)
        log(f"  ok B={b} agg_deliv {res['agg_deliv']}/s per-brk {res['agg_deliv']/b:.0f}/s "
            f"pub {res['agg_publish']}/s ackr {res['ack_ratio']} bal {res['balance_min_over_max']} "
            f"brk {res['brokers_seen']}/{b}")
    log(f"DONE {len(results)}/{len(blist)} -> {results_path}")


def main():
    global SETTLE, SAMPLE
    ap = argparse.ArgumentParser()
    ap.add_argument("--harness", choices=("ingress", "egress"), required=True)
    ap.add_argument("--stages", default="", help="comma list e.g. T1,T2 (default all)")
    ap.add_argument("--notes", default="", help="only cells whose note contains this substring")
    ap.add_argument("--out", default="", help="results filename suffix (default overwrites main)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--settle", type=float, default=SETTLE)
    ap.add_argument("--sample", type=float, default=SAMPLE)
    ap.add_argument("--sweep88", action="store_true",
                    help="egress 8:8, grow consumers from --sweep-start by --sweep-step until fail (no cap)")
    ap.add_argument("--sweep-start", type=int, default=32)
    ap.add_argument("--sweep-step", type=int, default=8)
    ap.add_argument("--pg-writers", type=int, default=None,
                    help="override PG_MAX_WRITERS in sweep cells")
    ap.add_argument("--durability", default=None, choices=("none", "memory", "postgres"),
                    help="override DURABILITY in sweep cells")
    ap.add_argument("--broker-sweep", default="", help="comma B list, e.g. 1,2,3,4,8")
    ap.add_argument("--sweep-p", type=int, default=4)
    ap.add_argument("--sweep-c", type=int, default=64)
    args = ap.parse_args()

    SETTLE, SAMPLE = args.settle, args.sample

    cwd = HARNESS_DIR[args.harness]

    if args.broker_sweep:
        blist = [int(x) for x in args.broker_sweep.split(",") if x.strip()]
        dur = args.durability or "postgres"
        rp = os.path.join(HERE, f"{args.harness}_bsweep_{dur}_results.json")
        pp = os.path.join(HERE, f"{args.harness}_bsweep_{dur}_progress.log")
        open(pp, "w").close()
        blog = make_logger(pp)
        blog(f"BROKER-SWEEP {args.harness}: {args.sweep_p}:B:{args.sweep_c} "
             f"durability={dur} B in {blist}")
        broker_sweep(cwd, args.sweep_p, args.sweep_c, blist, args.durability, blog, rp)
        return

    if args.sweep88:
        tag = ""
        if args.durability:
            tag += f"_{args.durability}"
        if args.pg_writers is not None:
            tag += f"_w{args.pg_writers}"
        rp = os.path.join(HERE, f"{args.harness}_sweep88{tag}_results.json")
        pp = os.path.join(HERE, f"{args.harness}_sweep88{tag}_progress.log")
        open(pp, "w").close()
        slog = make_logger(pp)
        slog(f"SWEEP88 {args.harness}: 8P:8B, C from {args.sweep_start} step "
             f"{args.sweep_step}, durability={args.durability or 'postgres'}, "
             f"pg_writers={args.pg_writers or 'default'}, cutoffs=[ack<0.85, "
             f"deliv<60%peak x2, keepalive-fail], no cap")
        sweep_88(cwd, args.sweep_start, args.sweep_step, slog, rp,
                 args.pg_writers, args.durability)
        return

    cells = _cells(args.harness)
    if args.stages:
        want = {s.strip() for s in args.stages.split(",")}
        cells = [c for c in cells if c["stage"] in want]
    if args.notes:
        cells = [c for c in cells if args.notes in c["note"]]

    suffix = f"_{args.out}" if args.out else ""
    results_path = os.path.join(HERE, f"{args.harness}_dist_results{suffix}.json")
    progress_path = os.path.join(HERE, f"{args.harness}_dist_progress{suffix}.log")
    open(progress_path, "w").close()
    log = make_logger(progress_path)

    log(f"HARNESS {args.harness}  cells={len(cells)}  "
        f"stages={sorted({c['stage'] for c in cells})}")
    peak = max(c["containers"] for c in cells)
    log(f"peak app-containers in any single cell = {peak} (cap {MAX_CONTAINERS})")
    for c in cells:
        log(f"  {c['stage']:3} {c['P']:>3}:{c['B']}:{c['C']:<3} "
            f"({c['containers']:>3} ctr)  {c['note']}")
    if args.dry_run:
        log("DRY RUN — no stack started")
        return

    log("BUILD ...")
    compose(cwd, ["build"])
    log("BUILD done")

    results = []
    for i, cell in enumerate(cells, 1):
        log(f"[{i}/{len(cells)}]")
        res = run_cell(cwd, cell, log)
        if res:
            results.append(res)
            with open(results_path, "w") as f:
                json.dump(results, f, indent=2)
    compose(cwd, ["down", "-v", "--remove-orphans"], check=False)
    log(f"DONE {len(results)}/{len(cells)} cells -> {results_path}")


if __name__ == "__main__":
    main()
