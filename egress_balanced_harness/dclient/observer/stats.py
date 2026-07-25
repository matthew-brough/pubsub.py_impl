"""In-memory throughput aggregator fed by the ``>`` subscription.

Every ``record`` call is O(1): bump counters and per-second buckets, push one
latency sample into a bounded ring. Percentiles and rate windows are computed
only on ``snapshot`` (once per SSE tick), never per message, so the sidecar can
keep up with the delivery firehose.
"""

import time
from collections import deque

# How many trailing 1-second buckets to retain (sparkline + sustained window).
_BUCKETS = 300
_SUSTAINED_WINDOW = 30  # seconds averaged for "sustained" rate
_LATENCY_RING = 5000  # bounded latency reservoir
_STATS_TTL = 8.0  # drop a fleet instance after this many seconds silent


def _percentile(sorted_vals: list[float], pct: float) -> float:
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * pct
    lo = int(k)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = k - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


class Stats:
    def __init__(self) -> None:
        self._start = time.time()
        self.total = 0
        self.retries = 0  # deliveries with attempt > 1
        self.per_topic: dict[str, int] = {}
        self.highest_seq = 0
        self.latency = deque(maxlen=_LATENCY_RING)  # seconds
        self.buckets: deque[tuple[int, int]] = deque(maxlen=_BUCKETS)  # (sec, count)
        self.peak_rate = 0.0
        # sidecar health
        self.evicted_count = 0
        self.subscribed = False
        # fleet: instance_id -> {"kind","ts","data"} from heartbeat frames
        self.instances: dict[str, dict] = {}

    # --- ingest ----------------------------------------------------------
    def record_instance(self, topic: str, payload: dict, ts: float) -> None:
        """Ingest a `<prefix>.<kind>.<id>` heartbeat frame (not counted as traffic)."""
        parts = topic.split(".")
        if len(parts) < 3:
            return
        kind = parts[1]
        instance_id = ".".join(parts[2:])
        self.instances[instance_id] = {
            "kind": kind,
            "ts": ts,
            "data": payload if isinstance(payload, dict) else {},
        }

    def record(self, topic: str, attempt: int, extras: dict, recv_ts: float) -> None:
        self.total += 1
        if attempt > 1:
            self.retries += 1
        self.per_topic[topic] = self.per_topic.get(topic, 0) + 1

        seq = extras.get("seq")
        if isinstance(seq, int) and seq > self.highest_seq:
            self.highest_seq = seq
        pub_ts = extras.get("pub_ts")
        if isinstance(pub_ts, (int, float)):
            lat = recv_ts - pub_ts
            if lat >= 0:
                self.latency.append(lat)

        sec = int(recv_ts)
        if self.buckets and self.buckets[-1][0] == sec:
            s, c = self.buckets[-1]
            self.buckets[-1] = (s, c + 1)
        else:
            self.buckets.append((sec, 1))

    def mark_subscribed(self) -> None:
        self.subscribed = True

    def mark_evicted(self) -> None:
        self.subscribed = False
        self.evicted_count += 1

    # --- report ----------------------------------------------------------
    def _completed_buckets(self, now_sec: int) -> list[tuple[int, int]]:
        # Drop the current (still-filling) second so rates aren't understated.
        return [(s, c) for (s, c) in self.buckets if s < now_sec]

    def snapshot(self) -> dict:
        now = time.time()
        now_sec = int(now)
        completed = self._completed_buckets(now_sec)

        current = float(completed[-1][1]) if completed else 0.0
        recent = completed[-_SUSTAINED_WINDOW:]
        sustained = sum(c for _, c in recent) / len(recent) if recent else 0.0
        for _, c in completed:
            if c > self.peak_rate:
                self.peak_rate = float(c)

        # Dense sparkline: fill gaps (idle seconds) with 0 across the window.
        spark: list[int] = []
        if completed:
            last_sec = completed[-1][0]
            by_sec = {s: c for s, c in completed}
            for s in range(last_sec - 59, last_sec + 1):
                spark.append(by_sec.get(s, 0))

        lat = sorted(self.latency)
        elapsed = max(now - self._start, 1e-6)

        # --- fleet: expire stale instances, split by kind, aggregate ---
        live = {
            iid: rec
            for iid, rec in self.instances.items()
            if now - rec["ts"] <= _STATS_TTL
        }
        self.instances = live
        producers, consumers = [], []
        fleet_pub_rate = fleet_ack_rate = fleet_nack_rate = 0.0
        for iid, rec in sorted(live.items()):
            d = rec["data"]
            age = round(now - rec["ts"], 1)
            if rec["kind"] == "producer":
                fleet_pub_rate += float(d.get("rate", 0) or 0)
                producers.append({
                    "id": iid,
                    "rate": d.get("rate", 0),
                    "published": d.get("published", 0),
                    "errors": d.get("errors", 0),
                    "workers": d.get("workers", 0),
                    "uptime": d.get("uptime", 0),
                    "age": age,
                })
            elif rec["kind"] == "consumer":
                fleet_ack_rate += float(d.get("ack_rate", 0) or 0)
                fleet_nack_rate += float(d.get("nack_rate", 0) or 0)
                consumers.append({
                    "id": iid,
                    "ack_rate": d.get("ack_rate", 0),
                    "nack_rate": d.get("nack_rate", 0),
                    "acked": d.get("acked", 0),
                    "nacked": d.get("nacked", 0),
                    "max_attempt": d.get("max_attempt", 1),
                    "evictions": d.get("evictions", 0),
                    "uptime": d.get("uptime", 0),
                    "age": age,
                })

        return {
            "elapsed_s": round(elapsed, 1),
            "total": self.total,
            "rate_current": round(current, 1),
            "rate_sustained": round(sustained, 1),
            "rate_peak": round(self.peak_rate, 1),
            "rate_lifetime": round(self.total / elapsed, 1),
            "latency_ms": {
                "p50": round(_percentile(lat, 0.50) * 1000, 2),
                "p95": round(_percentile(lat, 0.95) * 1000, 2),
                "p99": round(_percentile(lat, 0.99) * 1000, 2),
                "max": round((lat[-1] if lat else 0.0) * 1000, 2),
                "samples": len(lat),
            },
            "retries": self.retries,
            "retry_pct": round(100 * self.retries / self.total, 2) if self.total else 0.0,
            "per_topic": dict(
                sorted(self.per_topic.items(), key=lambda kv: -kv[1])
            ),
            "highest_seq": self.highest_seq,
            "approx_missing": max(0, self.highest_seq - self.total),
            "sparkline": spark,
            "sidecar": {
                "subscribed": self.subscribed,
                "evictions": self.evicted_count,
            },
            "fleet": {
                "producers": producers,
                "consumers": consumers,
                "n_producers": len(producers),
                "n_consumers": len(consumers),
                "pub_rate": round(fleet_pub_rate, 1),
                "ack_rate": round(fleet_ack_rate, 1),
                "nack_rate": round(fleet_nack_rate, 1),
            },
        }
