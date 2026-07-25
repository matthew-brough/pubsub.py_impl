#!/usr/bin/env python3
"""Matrix rerun driver: sync-fanout branch, uvloop [otel,fast].

Reproduces PERF_MATRIX_UVLOOP methodology:
  3 backends (none/memory/sqlite) x 23 topologies
  PRODUCER_RATE=max, poison+nack OFF, fresh stack per cell,
  16s settle + 16s OTel sample. Broker-authoritative counters scraped
  from controller Prometheus at localhost:9464/metrics.

Run from the repo root (needs docker-compose.yml alongside):
  python matrix_driver.py
Emits matrix_results.json (incremental) and matrix_progress.log next to this file.
"""
import json
import os
import re
import subprocess
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
PROJ = HERE  # repo root: docker-compose.yml lives beside this script
RESULTS = os.path.join(HERE, "matrix_results.json")
PROGRESS = os.path.join(HERE, "matrix_progress.log")
METRICS_URL = "http://localhost:9464/metrics"

SETTLE = 16.0
SAMPLE = 16.0
HEALTH_TIMEOUT = 90.0

BACKENDS = ["none", "memory", "sqlite"]
# 23 topologies: 5 matched + 9 producer-heavy + 9 subscriber-heavy
MATCHED = [(1, 1), (2, 2), (4, 4), (8, 8), (16, 16)]
PROD_HEAVY = [(p, c) for p in (32, 64, 128) for c in (1, 2, 4)]
SUB_HEAVY = [(p, c) for p in (1, 2, 4) for c in (32, 64, 128)]
TOPOS = MATCHED + PROD_HEAVY + SUB_HEAVY

_LINE = re.compile(r'^(pubsub_\w+_total)(\{[^}]*\})?\s+([0-9eE+.\-]+)\s*$')


def log(msg):
    line = f"{time.strftime('%H:%M:%S')} | {msg}"
    print(line, flush=True)
    with open(PROGRESS, "a") as f:
        f.write(line + "\n")


def scrape():
    """Return dict of aggregated broker counters, or None if unreachable."""
    try:
        with urllib.request.urlopen(METRICS_URL, timeout=3.0) as r:
            text = r.read().decode()
    except Exception:
        return None
    agg = {"publishes_ok": 0.0, "publishes_rej": 0.0, "deliveries": 0.0,
           "acks": 0.0, "nacks": 0.0, "retry_exhausted": 0.0}
    for raw in text.splitlines():
        m = _LINE.match(raw)
        if not m:
            continue
        name, labels, val = m.group(1), m.group(2) or "", float(m.group(3))
        if name == "pubsub_publishes_total":
            key = "publishes_ok" if 'accepted="true"' in labels else "publishes_rej"
            agg[key] += val
        elif name == "pubsub_deliveries_total":
            agg["deliveries"] += val
        elif name == "pubsub_acks_total":
            agg["acks"] += val
        elif name == "pubsub_nacks_total":
            agg["nacks"] += val
        elif name == "pubsub_retry_exhausted_total":
            agg["retry_exhausted"] += val
    return agg


def compose(args, env=None, check=True, capture=False):
    e = dict(os.environ)
    if env:
        e.update(env)
    return subprocess.run(
        ["docker", "compose"] + args,
        cwd=PROJ, env=e, check=check,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.STDOUT if capture else None,
        text=True,
    )


def wait_metrics(deadline):
    """Wait until controller /metrics is reachable AND publishing has started."""
    start = time.time()
    while time.time() - start < deadline:
        a = scrape()
        if a is not None and a["publishes_ok"] > 0:
            return True
        time.sleep(1.0)
    return False


def run_cell(backend, p, c):
    env = {
        "DURABILITY": backend,
        "PRODUCER_RATE": "max",
        "PRODUCER_CONCURRENCY": str(p),
        "NACK_RATE": "0",
        "POISON_EVERY": "0",
    }
    # fresh stack
    compose(["down", "-v", "--remove-orphans"], check=False, capture=True)
    compose(["up", "-d", "--scale", f"consumer={c}"], env=env, capture=True)
    if not wait_metrics(HEALTH_TIMEOUT):
        log(f"  !! {backend} P{p}:C{c} never started publishing")
        compose(["down", "-v", "--remove-orphans"], check=False, capture=True)
        return None
    time.sleep(SETTLE)
    t0 = scrape()
    ts0 = time.time()
    time.sleep(SAMPLE)
    t1 = scrape()
    ts1 = time.time()
    compose(["down", "-v", "--remove-orphans"], check=False, capture=True)
    if t0 is None or t1 is None:
        log(f"  !! {backend} P{p}:C{c} scrape failed")
        return None
    dt = ts1 - ts0
    d_pub = t1["publishes_ok"] - t0["publishes_ok"]
    d_rej = t1["publishes_rej"] - t0["publishes_rej"]
    d_del = t1["deliveries"] - t0["deliveries"]
    d_ack = t1["acks"] - t0["acks"]
    d_nack = t1["nacks"] - t0["nacks"]
    d_rex = t1["retry_exhausted"] - t0["retry_exhausted"]
    pub_rate = d_pub / dt
    del_rate = d_del / dt
    ack_ratio = (d_ack / d_del) if d_del > 0 else None
    fanout = (d_del / d_pub) if d_pub > 0 else None
    res = {
        "backend": backend, "P": p, "C": c,
        "publish": round(pub_rate, 1),
        "delivery": round(del_rate, 1),
        "ack_ratio": round(ack_ratio, 3) if ack_ratio is not None else None,
        "fanout": round(fanout, 1) if fanout is not None else None,
        "retry_exh": int(round(d_rex)),
        "rej_rate": round(d_rej / dt, 1),
        "nack_rate": round(d_nack / dt, 1),
        "dt": round(dt, 1),
    }
    log(f"  {backend} P{p}:C{c} -> pub {res['publish']} deliv {res['delivery']} "
        f"ackr {res['ack_ratio']} fanout {res['fanout']} retry_exh {res['retry_exh']}")
    return res


def main():
    open(PROGRESS, "w").close()
    log("BUILD image ...")
    compose(["build"], capture=True)
    log("BUILD done")
    results = []
    total = len(BACKENDS) * len(TOPOS)
    n = 0
    for backend in BACKENDS:
        for (p, c) in TOPOS:
            n += 1
            log(f"[{n}/{total}] {backend} P{p}:C{c}")
            res = run_cell(backend, p, c)
            if res:
                results.append(res)
                with open(RESULTS, "w") as f:
                    json.dump(results, f, indent=2)
    log(f"DONE {len(results)}/{total} cells")
    # ensure clean
    compose(["down", "-v", "--remove-orphans"], check=False, capture=True)


if __name__ == "__main__":
    main()
