"""Solace autoscale clients.

MessagingClient provides native SMF publish/subscribe with durable buffering and managed
handover. Resolver and KeyRouter remain available for lower-level protocol integrations.
Credentials come from an application-supplied provider; the controller returns locations.
"""
from .adapters import amqp_uri, mqtt_config, rest_target, smf_host
from .key_router import KeyRouter
from .messaging import Message, MessagingClient
from .resolver import Assignment, Resolver, ResolverError

# The smart shim (the per-message data path) is now the native Go shim under ``/shim``; it reads the
# same portable rule spec these helpers are documented alongside. This Python package keeps only the
# Tier-1 client helpers (resolver + per-protocol adapters), which never carry a message.

__all__ = [
    "Resolver",
    "MessagingClient",
    "Message",
    "KeyRouter",
    "Assignment",
    "ResolverError",
    "amqp_uri",
    "mqtt_config",
    "rest_target",
    "smf_host",
]
