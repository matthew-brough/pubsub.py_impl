"""Scrape + aggregate the controller's broker-side OTel metrics.

The controller exposes Prometheus counters emitted by the library's
`OTelObserver` (publishes/deliveries/acks/nacks/retry_exhausted). These are
*broker-authoritative* — they include things the client-side sidecar can't see
from its `>` subscription: rejected publishes and true dead-letter
(retry-exhausted) counts. We fetch the text endpoint and sum each family.
"""

import urllib.request

_KEYS = ("publishes_ok", "publishes_rej", "deliveries", "acks", "nacks", "retry_exhausted")


def fetch(url: str, timeout: float = 2.0) -> str:
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return resp.read().decode()


def parse(text: str) -> dict[str, float]:
    """Aggregate the pubsub_* counter families into scalar totals."""
    from prometheus_client.parser import text_string_to_metric_families

    agg = {k: 0.0 for k in _KEYS}
    for family in text_string_to_metric_families(text):
        for sample in family.samples:
            name = sample.name
            if name == "pubsub_publishes_total":
                key = "publishes_ok" if sample.labels.get("accepted") == "true" else "publishes_rej"
                agg[key] += sample.value
            elif name == "pubsub_deliveries_total":
                agg["deliveries"] += sample.value
            elif name == "pubsub_acks_total":
                agg["acks"] += sample.value
            elif name == "pubsub_nacks_total":
                agg["nacks"] += sample.value
            elif name == "pubsub_retry_exhausted_total":
                agg["retry_exhausted"] += sample.value
    return agg
