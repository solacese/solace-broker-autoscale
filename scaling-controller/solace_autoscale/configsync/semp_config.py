"""Thin SEMPv2 **config** client + reconcile orchestrator.

Mirrors ``metrics.semp.SempCollector`` in shape: an httpx client over ``/SEMP/v2/config/...``. It
snapshots a broker's replicable objects into a :class:`~solace_autoscale.configsync.model.BrokerConfig`
and applies :class:`~solace_autoscale.configsync.diff.ConfigOp`s. Writes are gated by ``dry_run``: in
dry-run it returns the plan without issuing anything (the default, matching the actuator's safety
posture). The pure diff decides WHAT to change; this decides only HOW to talk to the broker.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import httpx

from .diff import ConfigOp, diff, summarise
from .model import KIND_SPECS, BrokerConfig, normalise


class ConfigSyncError(Exception):
    pass


@dataclass
class ReconcilePlan:
    """The outcome of a reconcile: per-target ops and whether they were applied."""

    source_broker: str
    ops_by_target: dict[str, list[ConfigOp]] = field(default_factory=dict)
    applied: bool = False

    def summary(self) -> dict[str, dict[str, int]]:
        return {t: summarise(ops) for t, ops in self.ops_by_target.items()}

    def total_ops(self) -> int:
        return sum(len(ops) for ops in self.ops_by_target.values())


class SempConfigClient:
    """One broker's SEMPv2 config endpoint."""

    def __init__(self, broker_id: str, base_url: str, username: str, password: str, *,
                 verify: bool = True, timeout: float = 15.0) -> None:
        self.broker_id = broker_id
        self._base = base_url.rstrip("/")
        self._client = httpx.Client(auth=(username, password), verify=verify, timeout=timeout)

    def _get(self, path: str) -> dict[str, Any]:
        try:
            resp = self._client.get(f"{self._base}/SEMP/v2/config{path}")
            resp.raise_for_status()
        except httpx.HTTPError as e:
            raise ConfigSyncError(f"SEMP config GET failed: {path}: {e}") from e
        return resp.json()

    def _get_all(self, path: str) -> list[dict[str, Any]]:
        """GET a SEMPv2 collection and follow ``meta.paging.nextPageUri`` to the end.

        SEMPv2 caps a single response at the requested ``count`` and returns a cursor for the next
        page. Reading only the first page silently under-reports large brokers, so both snapshot
        reads page through every object. The page cap guards against a broker echoing a
        self-referential cursor; it is never expected to trip in practice.
        """
        rows: list[dict[str, Any]] = []
        # First request goes through the config-relative path; the cursor thereafter is absolute.
        body = self._get(path)
        for _ in range(10_000):
            rows.extend(body.get("data", []))
            next_uri = body.get("meta", {}).get("paging", {}).get("nextPageUri")
            if not next_uri:
                return rows
            try:
                resp = self._client.get(next_uri)
                resp.raise_for_status()
            except httpx.HTTPError as e:
                raise ConfigSyncError(f"SEMP config GET failed (paging): {next_uri}: {e}") from e
            body = resp.json()
        raise ConfigSyncError(f"SEMP config paging exceeded page cap for {path}")

    def _write(self, verb: str, path: str, body: dict[str, Any]) -> None:
        method = {"create": "POST", "update": "PUT", "delete": "DELETE"}[verb]
        try:
            if method == "DELETE":
                resp = self._client.request(method, f"{self._base}/SEMP/v2/config{path}")
            else:
                resp = self._client.request(method, f"{self._base}/SEMP/v2/config{path}", json=body)
        except httpx.HTTPError as e:  # transport-level (connect/timeout)
            raise ConfigSyncError(f"SEMP config {method} failed: {path}: {e}") from e
        # Idempotency: reconcile must be safely re-runnable. A create of something that already
        # exists, or a delete of something already gone, is a converged state, not an error.
        if resp.status_code >= 400:
            status = _semp_error_status(resp)
            if verb == "create" and status == "ALREADY_EXISTS":
                return
            if verb == "delete" and status == "NOT_FOUND":
                return
            raise ConfigSyncError(
                f"SEMP config {method} failed: {path}: {resp.status_code} {resp.text[:200]}"
            )

    def snapshot(self, msg_vpn: str, kinds: list[str]) -> BrokerConfig:
        """Fetch the selected object kinds for ``msg_vpn`` into a normalised BrokerConfig."""
        bc = BrokerConfig(broker_id=self.broker_id)
        want_subs = "queueSubscription" in kinds
        # Parents first. Queues are always needed when subscriptions are requested (to know the
        # parent queue names), even if the queue kind itself is not being diffed.
        queue_names: list[str] = []
        for kind in ("queue", "topicEndpoint", "clientProfile", "aclProfile"):
            if kind not in kinds and not (kind == "queue" and want_subs):
                continue
            coll = KIND_SPECS[kind]["collection"]
            data = self._get_all(f"/msgVpns/{msg_vpn}/{coll}?count=100")
            for raw in data:
                if kind == "queue":
                    queue_names.append(str(raw["queueName"]))
                if kind in kinds:  # only record kinds actually being diffed
                    bc.add(normalise(kind, raw))
        # children (queue subscriptions) keyed under their queue
        if want_subs:
            for qname in queue_names:
                data = self._get_all(
                    f"/msgVpns/{msg_vpn}/queues/{qname}/subscriptions?count=100"
                )
                for raw in data:
                    bc.add(normalise("queueSubscription", raw, parent=(qname,)))
        return bc

    def apply(self, msg_vpn: str, ops: list[ConfigOp]) -> None:
        """Issue the ops against this broker. Caller decides whether to call (dry-run gate)."""
        for op in ops:
            path = _op_path(msg_vpn, op)
            self._write(op.verb, path, {**op.attributes, **_identity_body(op)})

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> SempConfigClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def _semp_error_status(resp: httpx.Response) -> str:
    """Pull the SEMPv2 error ``status`` string (e.g. ALREADY_EXISTS, NOT_FOUND) from a 4xx body."""
    try:
        return str(resp.json().get("meta", {}).get("error", {}).get("status", ""))
    except (ValueError, AttributeError):
        return ""


def _identity_body(op: ConfigOp) -> dict[str, Any]:
    """The identity attribute(s) SEMP requires in a create body."""
    idattrs = KIND_SPECS[op.kind]["identity"]
    # parent identity segments precede the object's own; map only the object's own to attributes
    own = op.identity[-len(idattrs):]
    return dict(zip(idattrs, own, strict=True))


def _op_path(msg_vpn: str, op: ConfigOp) -> str:
    spec = KIND_SPECS[op.kind]
    if op.kind == "queueSubscription":
        qname, topic = op.identity
        base = f"/msgVpns/{msg_vpn}/queues/{qname}/subscriptions"
        return base if op.verb == "create" else f"{base}/{_enc(topic)}"
    coll = spec["collection"]
    name = op.identity[-1]
    base = f"/msgVpns/{msg_vpn}/{coll}"
    return base if op.verb == "create" else f"{base}/{_enc(name)}"


def _enc(seg: str) -> str:
    from urllib.parse import quote
    return quote(seg, safe="")


def reconcile(
    source: SempConfigClient,
    targets: list[SempConfigClient],
    msg_vpn: str,
    *,
    kinds: list[str],
    mode: str = "additive",
    dry_run: bool = True,
) -> ReconcilePlan:
    """Snapshot source + targets, diff each target against source, and (unless dry-run) apply.

    Re-runnable: an edit made on the source shows up as ops on the next reconcile until targets
    converge. Deterministic — the returned plan is the same for the same broker states.
    """
    src_cfg = source.snapshot(msg_vpn, kinds)
    plan = ReconcilePlan(source_broker=source.broker_id)
    for tgt in targets:
        tgt_cfg = tgt.snapshot(msg_vpn, kinds)
        ops = diff(src_cfg, tgt_cfg, mode=mode, kinds=kinds)  # type: ignore[arg-type]
        plan.ops_by_target[tgt.broker_id] = ops
    if not dry_run:
        for tgt in targets:
            tgt.apply(msg_vpn, plan.ops_by_target[tgt.broker_id])
        plan.applied = True
    return plan
