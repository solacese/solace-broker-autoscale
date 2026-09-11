"""Rule-based dispatch engine for the smart SHIM.

Pure decision core: ``(topic, payload) + rules -> Decision(broker, address, key)``. Combines a topic
pattern with JSON payload predicates (AND), first-match-wins, and derives a partition/group key. Not
round-robin — the same message always routes the same way, so per-key ordering and listener demux
hold. The SHIM (in ``adapters/``) turns a broker name into an endpoint and carries the message on the
AMQP path; nothing here touches the wire.
"""

from .payload import OPERATORS, Predicate, RuleError, eval_predicate, topic_matches
from .rules import (
    Decision,
    DispatchPlan,
    DispatchRule,
    Match,
    NoRouteError,
    Route,
    evaluate,
    render_template,
    rules_from_config,
    target_brokers,
)
from .spec import SPEC_VERSION, from_spec, rule_to_dict, to_spec

__all__ = [
    "OPERATORS",
    "Predicate",
    "RuleError",
    "eval_predicate",
    "topic_matches",
    "Decision",
    "DispatchPlan",
    "DispatchRule",
    "Match",
    "NoRouteError",
    "Route",
    "evaluate",
    "render_template",
    "rules_from_config",
    "target_brokers",
    "SPEC_VERSION",
    "from_spec",
    "rule_to_dict",
    "to_spec",
]
