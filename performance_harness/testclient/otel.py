"""OpenTelemetry SDK wiring for the controller.

The pubsub library ships only the OTel metrics *API* (via the `otel` extra) and
the `OTelObserver` that maps broker hooks onto counters. Configuring an SDK +
exporter is the application's job — done here: a `MeterProvider` with a
Prometheus reader, exposed on a `/metrics` HTTP endpoint the sidecar scrapes.
"""

import logging

from prometheus_client import start_http_server

from testclient import config

log = logging.getLogger("testclient.otel")


def setup_meter():
    """Install a Prometheus-backed MeterProvider and serve `/metrics`.

    Returns the `pubsub` meter to hand to `OTelObserver`. The Prometheus reader
    registers itself with the default `prometheus_client` registry, so
    `start_http_server` exposes every broker counter.
    """
    from opentelemetry import metrics
    from opentelemetry.exporter.prometheus import PrometheusMetricReader
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.resources import Resource

    reader = PrometheusMetricReader()
    provider = MeterProvider(
        metric_readers=[reader],
        resource=Resource.create({"service.name": config.OTEL_SERVICE_NAME}),
    )
    metrics.set_meter_provider(provider)
    start_http_server(config.OTEL_PROM_PORT)
    log.info("OTel Prometheus metrics on :%d/metrics", config.OTEL_PROM_PORT)
    return metrics.get_meter("pubsub")
