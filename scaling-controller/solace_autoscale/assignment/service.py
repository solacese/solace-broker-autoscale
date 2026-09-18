"""Assignment HTTP service (§9.1). Stateless over the store.

GET /assignment?shard=&client_id=&protocol=&mode=direct|guaranteed
  → per-protocol endpoint map (not a single host/port), broker_id, msg_vpn, state, lease_seconds.

GET /topology?shard=
  → the current topology snapshot for a shard, in the exact wire form the event spine publishes
    (ADR 0009). This is the shim's COLD-START fallback: on boot, or if the bus is unreachable, the
    shim fetches the snapshot once here, then live spine events take over and win by generation.
    Because the service is stateless over the store and does not own the generation counter, the
    cold snapshot carries gen 0 - a floor that any real spine event (gen >= 1) supersedes.

Never vends credentials - returns a location only. Health + readiness endpoints. The service is
stateless; all durable state is in the store, so it survives restart and horizontal replication
(with optimistic locking on placement writes).

``now`` is injected via a clock function so tests are deterministic; the running server uses the
real clock.
"""

from __future__ import annotations

import os
import secrets
import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime

from fastapi import FastAPI, Header, HTTPException, Query, Response

from ..config import AssignmentConfig, MessagingConfig, load_config
from ..controller.store import ControllerStore, queue_name
from .placement import NoBrokerAvailable, ProtocolUnavailable, assign
from .routing import partition_for
from .store import AssignmentStore, BrokerState
from .topics import TopicRegistry, group_queue, topic_prefix
from .topology import COLD_START_GEN, BrokerRef, ShardTopology

DEFAULT_LEASE_SECONDS = 300


def _now() -> float:
    return datetime.now(UTC).timestamp()


def create_app(
    store: AssignmentStore,
    *,
    clock: Callable[[], float] = _now,
    lease_seconds: int = DEFAULT_LEASE_SECONDS,
    api_key: str | None = None,
    policy: AssignmentConfig | None = None,
    fleet_id: str | None = None,
    messaging: MessagingConfig | None = None,
) -> FastAPI:
    policy = policy or AssignmentConfig(lease_seconds=lease_seconds)
    store.ensure_routing(policy.routing, policy.partitions)
    store.ensure_managed_namespace(fleet_id)
    migrations = ControllerStore(store)
    messaging = messaging or MessagingConfig()
    registry = TopicRegistry(store)
    registry.contract(messaging.routing_contract())
    if messaging.enabled:
        for group in messaging.subscriptions:
            registry.register(group.group, group.topics)
    if messaging.enabled and (not fleet_id or policy.routing != "partitioned"):
        raise ValueError("native messaging requires managed partitioned routing")
    app = FastAPI(title="solace-autoscale assignment service", version="1")

    # FastAPI's route decorators are typed to return the *decorated function unchanged* only on
    # recent releases; older FastAPI/mypy pairs treat @app.get(...) as an untyped decorator under
    # --strict. The `unused-ignore` code keeps this clean on toolchains where the primary code does
    # not fire, so the same line passes across versions without weakening the module config.
    @app.get("/healthz")  # type: ignore[untyped-decorator, unused-ignore]
    def healthz() -> dict:
        return {"status": "ok"}

    @app.get("/readyz")  # type: ignore[untyped-decorator, unused-ignore]
    def readyz() -> dict:
        # ready if the store is reachable
        try:
            store.brokers_for_shard("__probe__")
        except Exception as e:  # pragma: no cover
            raise HTTPException(status_code=503, detail=f"store not ready: {e}") from e
        return {"status": "ready"}

    @app.get("/assignment")  # type: ignore[untyped-decorator, unused-ignore]
    def assignment(
        response: Response,
        shard: str = Query(..., min_length=1, max_length=512),
        client_id: str = Query(..., min_length=1, max_length=512),
        protocol: str | None = Query(default=None),
        mode: str = Query(default="direct"),
        authorization: str | None = Header(default=None),
        routing_key: str | None = Query(default=None, min_length=1, max_length=1024),
        partition: int | None = Query(default=None, ge=0),
        role: str = Query(default="publisher"),
    ) -> dict:
        if api_key and not secrets.compare_digest(authorization or "", f"Bearer {api_key}"):
            raise HTTPException(status_code=401, detail="assignment authorization required")
        if mode not in ("direct", "guaranteed"):
            raise HTTPException(status_code=400, detail="mode must be direct or guaranteed")
        partition_id = None
        placement_key = client_id
        if policy.routing == "partitioned":
            if (routing_key is None) == (partition is None):
                raise HTTPException(status_code=400, detail="supply exactly one routing_key or partition")
            if partition is not None and partition >= policy.partitions:
                raise HTTPException(status_code=400, detail="partition exceeds configured partition count")
            partition_id = (
                partition
                if partition is not None
                else partition_for(shard, routing_key or "", policy.partitions)
            )
            placement_key = f"partition:{partition_id}"
        if role not in ("publisher", "consumer"):
            raise HTTPException(status_code=400, detail="role must be publisher or consumer")
        migration = migrations.for_partition(shard, partition_id) if partition_id is not None else None
        if role == "publisher" and migration and migration.phase not in ("preparing",):
            raise HTTPException(
                status_code=503,
                detail="partition handover in progress; retain and retry",
                headers={"Retry-After": "1"},
            )
        try:
            result = assign(
                store,
                shard,
                placement_key,
                mode,
                clock(),
                policy.lease_seconds,
                protocol=protocol,
                strategy=policy.strategy,
                broker_weights=policy.broker_weights,
            )
        except ProtocolUnavailable as e:
            raise HTTPException(status_code=404, detail=str(e)) from e
        except NoBrokerAvailable as e:
            raise HTTPException(status_code=503, detail=str(e), headers={"Retry-After": "5"}) from e
        except ValueError as e:
            raise HTTPException(status_code=409, detail=str(e)) from e
        except sqlite3.OperationalError as e:
            raise HTTPException(
                status_code=503,
                detail="assignment store temporarily unavailable",
                headers={"Retry-After": "5"},
            ) from e

        broker = result.broker
        endpoints = broker.endpoints
        if protocol is not None and protocol not in endpoints:
            raise HTTPException(
                status_code=404,
                detail=f"protocol {protocol!r} not offered by broker {broker.broker_id!r}; "
                f"available: {sorted(endpoints)}",
            )
        body = {
            "broker_id": broker.broker_id,
            "msg_vpn": broker.msg_vpn,
            "state": broker.state.value,
            "lease_seconds": result.lease_seconds,
            "reused_existing": result.reused_existing,
            # per-protocol endpoint map - never a single host/port, never a credential
            "endpoints": endpoints,
            "partition_id": partition_id,
            "partition_count": policy.partitions if policy.routing == "partitioned" else None,
            "placement_key": placement_key,
            "routing": policy.routing,
            "strategy": policy.strategy,
            "topic_prefix": (
                topic_prefix(fleet_id, broker.broker_id, shard, partition_id)
                if messaging.enabled and fleet_id and partition_id is not None
                else None
            ),
            "queue_name": (
                queue_name(fleet_id, shard, partition_id) if fleet_id and partition_id is not None else None
            ),
        }
        response.headers["Cache-Control"] = "no-store"
        return body

    @app.get("/topology")  # type: ignore[untyped-decorator, unused-ignore]
    def topology(shard: str = Query(...), authorization: str | None = Header(default=None)) -> dict:
        """Cold-start snapshot for a shard, in the spine's wire form (ADR 0009).

        Carries gen 0 (COLD_START_GEN): a floor the live event spine supersedes. Includes every
        broker on the shard with its state, so the shim can compute ownership (ACTIVE brokers only)
        exactly as it would from a spine event. No handoffs - the service does not track in-flight
        cutovers; those come only from the generation-owning spine.
        """
        if api_key and not secrets.compare_digest(authorization or "", f"Bearer {api_key}"):
            raise HTTPException(status_code=401, detail="assignment authorization required")
        if messaging.enabled:
            raise HTTPException(
                status_code=409,
                detail="managed topic ownership requires /assignment and /partitions; "
                       "topology hashing cannot bypass queue migration",
            )
        brokers = store.brokers_for_shard(shard)
        topo = ShardTopology(
            shard=shard,
            gen=COLD_START_GEN,
            brokers=tuple(BrokerRef.from_broker(b) for b in brokers),
        )
        return topo.to_event(emitted_at=datetime.fromtimestamp(clock(), UTC).isoformat())

    @app.get("/partitions")  # type: ignore[untyped-decorator, unused-ignore]
    def partitions(
        shard: str = Query(..., min_length=1),
        group: str | None = Query(default=None),
        authorization: str | None = Header(default=None),
    ) -> dict:
        """Consumer workers discover current owners and destination queues to pre-bind."""
        if api_key and not secrets.compare_digest(authorization or "", f"Bearer {api_key}"):
            raise HTTPException(status_code=401, detail="assignment authorization required")
        if not fleet_id or policy.routing != "partitioned":
            raise HTTPException(status_code=400, detail="managed partition discovery is not configured")
        if messaging.enabled and group not in registry.groups():
            raise HTTPException(status_code=400, detail="register a durable subscriber group first")
        if messaging.enabled and group not in registry.groups(shard):
            return {"partitions": []}
        records = []
        for partition_id in range(policy.partitions):
            placement = store.get_placement(shard, f"partition:{partition_id}")
            if placement is None:
                continue
            move = migrations.for_partition(shard, partition_id)
            broker_ids = {placement.broker_id}
            if move:
                broker_ids.update((move.source, move.target))
            locations = []
            for broker_id in sorted(broker_ids):
                broker = store.get_broker(broker_id)
                if broker:
                    locations.append(
                        {"broker_id": broker_id, "msg_vpn": broker.msg_vpn, "endpoints": broker.endpoints}
                    )
            records.append(
                {
                    "partition_id": partition_id,
                    "queue_name": (
                        group_queue(queue_name(fleet_id, shard, partition_id), group)
                        if messaging.enabled and group
                        else queue_name(fleet_id, shard, partition_id)
                    ),
                    "owner": placement.broker_id,
                    "phase": move.phase if move else "active",
                    "locations": locations,
                }
            )
        return {"partitions": records}

    def authorize_messaging(authorization: str | None) -> None:
        if api_key and not secrets.compare_digest(authorization or "", f"Bearer {api_key}"):
            raise HTTPException(status_code=401, detail="assignment authorization required")
        if not messaging.enabled:
            raise HTTPException(status_code=400, detail="native messaging is not enabled")

    @app.get("/messaging/config")  # type: ignore[untyped-decorator, unused-ignore]
    def messaging_config(authorization: str | None = Header(default=None)) -> dict:
        authorize_messaging(authorization)
        groups = registry.groups()
        return {
            "routes": [r.routing_contract() for r in messaging.routes],
            "partitions": policy.partitions,
            "groups": groups,
            "ready_groups": [g for g in groups if registry.ready([g])],
            "fleet_id": fleet_id,
        }

    @app.post("/messaging/subscriptions")  # type: ignore[untyped-decorator, unused-ignore]
    def subscribe(body: dict, authorization: str | None = Header(default=None)) -> dict:
        authorize_messaging(authorization)
        group, patterns = body.get("group"), body.get("topics")
        if (
            not isinstance(group, str)
            or not isinstance(patterns, list)
            or any(not isinstance(p, str) for p in patterns)
        ):
            raise HTTPException(status_code=400, detail="group and list of topic patterns are required")
        if not messaging.allow_dynamic_groups and group not in {g.group for g in messaging.subscriptions}:
            raise HTTPException(status_code=403, detail="subscription group must be declared in YAML")
        try:
            registry.register(group, patterns)
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {
            "group": group,
            "ready": registry.ready([group]),
            "shards": sorted({r.shard for r in messaging.routes if group in registry.groups(r.shard)}),
        }

    return app


def run_server(config_path: str, host: str = "127.0.0.1", port: int = 8099) -> None:  # pragma: no cover
    import uvicorn

    cfg = load_config(config_path)
    # store path: reuse the accuracy store dir convention; assignment store is separate.
    from pathlib import Path

    Path(cfg.assignment.store).parent.mkdir(parents=True, exist_ok=True)
    store = AssignmentStore(cfg.assignment.store)
    key = os.environ.get("SOLACE_ASSIGNMENT_API_KEY")
    if host not in ("127.0.0.1", "::1", "localhost") and not key:
        store.close()
        raise ValueError("set SOLACE_ASSIGNMENT_API_KEY before binding assignment service beyond loopback")
    app = create_app(
        store,
        api_key=key,
        policy=cfg.assignment,
        fleet_id=cfg.automation.fleet_id if cfg.automation.enabled else None,
        messaging=cfg.messaging,
    )
    try:
        uvicorn.run(app, host=host, port=port)
    finally:
        store.close()


# convenience for drain/lifecycle callers
def set_draining(store: AssignmentStore, broker_id: str) -> None:
    store.set_broker_state(broker_id, BrokerState.DRAINING)
