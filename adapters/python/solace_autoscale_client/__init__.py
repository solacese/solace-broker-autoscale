"""solace-autoscale client adapters (Python).

Three tiers (§9.2):
  - Tier 0 (DNS): no code here - connect to the shard DNS name with your normal client.
  - Tier 1 (resolver + thin protocol adapters): ``Resolver`` calls the assignment service and returns
    a per-protocol connection URI / factory config. Your app uses its OWN unmodified client library
    (Qpid JMS/Proton, Paho, any HTTP client). This is the default for AMQP and MQTT.
  - Tier 2 (SDK wrapper): only for protocols Solace owns (SMF, Solace JMS). Wraps connection
    creation, caches the assignment, re-looks-up on reconnect, honours a reassignment signal.

Never handles credentials - the resolver returns a location; auth stays with your existing
mechanism. When the assignment service is unreachable, the cached assignment is used (never fail
closed) - the service being down must not take an application down.
"""

from .adapters import amqp_uri, mqtt_config, rest_target, smf_host
from .resolver import Assignment, Resolver, ResolverError

__all__ = [
    "Resolver",
    "Assignment",
    "ResolverError",
    "amqp_uri",
    "mqtt_config",
    "rest_target",
    "smf_host",
]
