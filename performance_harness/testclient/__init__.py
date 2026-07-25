"""Throughput test harness for pubsub.py.

Drives the real library over its TCP transport from separate processes
(producer, consumer, management sidecar) against a broker (controller).
See ``docker-compose.yml`` for the orchestrated topology.
"""
