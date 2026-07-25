"""OpenTelemetry SDK wiring for each broker.

The pubsub library ships only the OTel metrics *API* plus the ``OTelObserver``
that maps broker hooks onto counters. Configuring an SDK + exporter is the
application's job — done here: a ``MeterProvider`` with a Prometheus reader,
exposed on a ``/metrics`` endpoint the observer scrapes (once per broker).
"""

import logging

from prometheus_client import start_http_server

from dclient import config

log = logging.getLogger("dclient.otel")


def setup_meter():
    """Install a Prometheus-backed MeterProvider and serve ``/metrics``.

    Returns the ``pubsub`` meter to hand to ``OTelObserver``. Each broker tags
    its resource with its own ``OTEL_SERVICE_NAME`` so the observer can attribute
    counters per broker.
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
