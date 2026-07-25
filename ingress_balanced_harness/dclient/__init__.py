"""Distributed pubsub.py harness client package.

Multi-broker topology: producers load-balanced across brokers through an nginx
TCP ingress; consumers and the observer fan *in* to every broker; all brokers
share one Postgres durability layer. See ../README.md for the delivery model.
"""
