"""Observation platform: one instance aggregating the whole broker cluster.

Fans a throughput (`>`) and a fleet (`_stats.>`) subscription *in* to every
broker, scrapes every broker's OTel `/metrics`, and reads the shared Postgres
durable layer for DLQ + retained-history counts. Serves a live SSE dashboard.
"""
