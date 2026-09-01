"""Event Portal shard advisor (§8).

Goal: partition the topic space so publishers and subscribers of a branch land on the same shard,
and no payload crosses a broker boundary (ADR 0003).

Approach: build a bipartite graph of applications ↔ topics, find weakly connected components, treat
each component as a shard candidate. Report component sizes and any application spanning multiple
components (those force hybrid mode). Emit a ``topology.shards`` config fragment to paste.

Ingest (see docs — verify against a real export):
The Event Portal v2 API models a topic as ``EventVersion.deliveryDescriptor.address.addressLevels``
(each level ``{name, addressLevelType: literal|variable}``), and links applications to events via
``ApplicationVersion.declaredProducedEventVersionIds`` / ``declaredConsumedEventVersionIds`` and
``EventVersion.declaredProducing/ConsumingApplicationVersionIds``. Because the v2 API returns those
as separate entity lists rather than one export document, this module accepts either:

  1. a **normalised bundle**::

        {"applications": [{"id","name"}...],
         "events": [{"id","topic","producer_app_ids":[...],"consumer_app_ids":[...]}]}

  2. a **v2 entity bundle**::

        {"applicationVersions": [...], "eventVersions": [...], "applications": [...]}

     which this module folds down to the normalised form.

No real export sample was supplied; tests use a fictional fixture. If your export needs a different
platform endpoint, say which and it can be added.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class Component:
    index: int
    applications: set[str] = field(default_factory=set)
    topics: set[str] = field(default_factory=set)

    @property
    def suggested_match(self) -> str:
        """A topic-prefix match string covering the component's topics.

        Uses the longest common literal prefix of the component's topics, then a ``>`` wildcard.
        """
        return _common_prefix_match(sorted(self.topics))


@dataclass
class AdviceResult:
    components: list[Component]
    #: application name/id -> set of component indices it appears in (len>1 → spanning)
    application_components: dict[str, set[int]]
    app_names: dict[str, str]

    @property
    def spanning_applications(self) -> list[str]:
        return sorted(app for app, comps in self.application_components.items() if len(comps) > 1)


# ---- topic reconstruction from v2 addressLevels ---------------------------------------------

def topic_from_address_levels(levels: list[dict[str, Any]]) -> str:
    parts = []
    for lvl in levels:
        if lvl.get("addressLevelType") == "variable":
            parts.append("{" + str(lvl.get("name", "var")) + "}")
        else:
            parts.append(str(lvl.get("name", "")))
    return "/".join(parts)


# ---- ingest normalisation --------------------------------------------------------------------

def normalise_export(doc: dict[str, Any]) -> list[dict[str, Any]]:
    """Return normalised events: [{id, topic, producer_app_ids, consumer_app_ids}]."""
    if "events" in doc and doc["events"] and "topic" in doc["events"][0]:
        return doc["events"]
    if "eventVersions" in doc:
        return _fold_v2(doc)
    raise ValueError(
        "unrecognised Event Portal export: expected a normalised bundle with 'events' or a v2 "
        "bundle with 'eventVersions'"
    )


def _fold_v2(doc: dict[str, Any]) -> list[dict[str, Any]]:
    """Fold v2 applicationVersions + eventVersions into normalised events.

    Maps applicationVersion ids to their parent application id so the graph is over applications,
    not versions.
    """
    appver_to_app: dict[str, str] = {}
    for av in doc.get("applicationVersions", []):
        appver_to_app[av["id"]] = av.get("applicationId", av["id"])

    # producer/consumer links may live on either side; union both directions.
    produced: dict[str, set[str]] = {}  # eventVersionId -> {appId}
    consumed: dict[str, set[str]] = {}
    for av in doc.get("applicationVersions", []):
        app = appver_to_app.get(av["id"], av["id"])
        for ev in av.get("declaredProducedEventVersionIds", []) or []:
            produced.setdefault(ev, set()).add(app)
        for ev in av.get("declaredConsumedEventVersionIds", []) or []:
            consumed.setdefault(ev, set()).add(app)

    events = []
    for ev in doc.get("eventVersions", []):
        evid = ev["id"]
        levels = (ev.get("deliveryDescriptor") or {}).get("address", {}).get("addressLevels", [])
        topic = topic_from_address_levels(levels) if levels else ev.get("topic", evid)
        prod = set(produced.get(evid, set()))
        cons = set(consumed.get(evid, set()))
        for avid in ev.get("declaredProducingApplicationVersionIds", []) or []:
            prod.add(appver_to_app.get(avid, avid))
        for avid in ev.get("declaredConsumingApplicationVersionIds", []) or []:
            cons.add(appver_to_app.get(avid, avid))
        events.append({
            "id": evid,
            "topic": topic,
            "producer_app_ids": sorted(prod),
            "consumer_app_ids": sorted(cons),
        })
    return events


# ---- graph + connected components ------------------------------------------------------------

def advise(doc: dict[str, Any]) -> AdviceResult:
    events = normalise_export(doc)
    app_names = {a["id"]: a.get("name", a["id"]) for a in doc.get("applications", [])}

    # Union-Find over nodes tagged ("app", id) and ("topic", topic).
    parent: dict[tuple[str, str], tuple[str, str]] = {}

    Node = tuple[str, str]

    def find(x: Node) -> Node:
        parent.setdefault(x, x)
        root = x
        while parent[root] != root:
            root = parent[root]
        while parent[x] != root:
            parent[x], x = root, parent[x]
        return root

    def union(a: Node, b: Node) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    app_nodes: set[tuple[str, str]] = set()
    topic_nodes: set[tuple[str, str]] = set()
    for ev in events:
        topic = ev["topic"]
        tnode = ("topic", topic)
        topic_nodes.add(tnode)
        find(tnode)
        for app in list(ev.get("producer_app_ids", [])) + list(ev.get("consumer_app_ids", [])):
            anode = ("app", app)
            app_nodes.add(anode)
            union(anode, tnode)

    # group nodes by root
    groups: dict[tuple[str, str], list[tuple[str, str]]] = {}
    for node in app_nodes | topic_nodes:
        groups.setdefault(find(node), []).append(node)

    components: list[Component] = []
    application_components: dict[str, set[int]] = {}
    for idx, (_root, nodes) in enumerate(sorted(groups.items(), key=lambda kv: str(kv[0]))):
        comp = Component(index=idx)
        for kind, val in nodes:
            if kind == "app":
                comp.applications.add(val)
                application_components.setdefault(val, set()).add(idx)
            else:
                comp.topics.add(val)
        components.append(comp)

    return AdviceResult(
        components=components,
        application_components=application_components,
        app_names=app_names,
    )


def advise_from_file(path: str | Path) -> AdviceResult:
    return advise(json.loads(Path(path).read_text()))


# ---- config fragment output ------------------------------------------------------------------

def render_config_fragment(result: AdviceResult) -> str:
    lines = ["topology:", "  mode: %s" % ("hybrid" if result.spanning_applications else "sharded"),
             '  shard_key: "{domain}"', "  shards:"]
    for comp in result.components:
        if not comp.topics:
            continue
        name = f"shard-{comp.index}"
        lines.append(f"    - name: {name}")
        lines.append(f'      match: "{comp.suggested_match}"')
        app_labels = sorted(result.app_names.get(a, a) for a in comp.applications)
        lines.append(f"      # applications: {', '.join(app_labels) or '(none)'}")
        lines.append(f"      # topics: {len(comp.topics)}")
    if result.spanning_applications:
        labels = [result.app_names.get(a, a) for a in result.spanning_applications]
        lines.append("  # SPANNING applications force hybrid mode (they touch multiple shards):")
        for lbl in labels:
            lines.append(f"  #   - {lbl}")
    return "\n".join(lines) + "\n"


def _common_prefix_match(topics: list[str]) -> str:
    if not topics:
        return ">"
    split = [t.split("/") for t in topics]
    prefix: list[str] = []
    for level_group in zip(*split, strict=False):
        first = level_group[0]
        if all(lvl == first for lvl in level_group) and not (first.startswith("{") and first.endswith("}")):
            prefix.append(first)
        else:
            break
    if not prefix:
        return ">"
    return "/".join(prefix) + "/>"
