"""Solace autoscale clients.

MessagingClient provides native SMF publish/subscribe with durable buffering and managed
handover. Resolver and KeyRouter remain available for lower-level protocol integrations.
Credentials come from an application-supplied provider; the controller returns locations.
"""
from .adapters import amqp_uri, mqtt_config, rest_target, smf_host
from .key_router import KeyRouter
from .messaging import Message, MessagingClient
from .resolver import Assignment, Resolver, ResolverError

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
