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

from collections.abc import Callable
from datetime import UTC, datetime

from fastapi import FastAPI, HTTPException, Query

from ..config import load_config
from .placement import NoBrokerAvailable, assign
from .store import AssignmentStore, BrokerState
from .topology import COLD_START_GEN, BrokerRef, ShardTopology

DEFAULT_LEASE_SECONDS = 300


def _now() -> float:
    return datetime.now(UTC).timestamp()


def create_app(store: AssignmentStore, *, clock: Callable[[], float] = _now,
               lease_seconds: int = DEFAULT_LEASE_SECONDS) -> FastAPI:
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
        shard: str = Query(...),
        client_id: str = Query(...),
        protocol: str | None = Query(default=None),
        mode: str = Query(default="direct"),
    ) -> dict:
        if mode not in ("direct", "guaranteed"):
            raise HTTPException(status_code=400, detail="mode must be direct or guaranteed")
        try:
            result = assign(store, shard, client_id, mode, clock(), lease_seconds)
        except NoBrokerAvailable as e:
            raise HTTPException(status_code=503, detail=str(e)) from e

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
        }
        return body

    @app.get("/topology")  # type: ignore[untyped-decorator, unused-ignore]
    def topology(shard: str = Query(...)) -> dict:
        """Cold-start snapshot for a shard, in the spine's wire form (ADR 0009).

        Carries gen 0 (COLD_START_GEN): a floor the live event spine supersedes. Includes every
        broker on the shard with its state, so the shim can compute ownership (ACTIVE brokers only)
        exactly as it would from a spine event. No handoffs - the service does not track in-flight
        cutovers; those come only from the generation-owning spine.
        """
        brokers = store.brokers_for_shard(shard)
        topo = ShardTopology(
            shard=shard,
            gen=COLD_START_GEN,
            brokers=tuple(BrokerRef.from_broker(b) for b in brokers),
        )
        return topo.to_event(emitted_at=datetime.fromtimestamp(clock(), UTC).isoformat())

    return app


def run_server(config_path: str, host: str = "127.0.0.1", port: int = 8099) -> None:  # pragma: no cover
    import uvicorn

    cfg = load_config(config_path)
    # store path: reuse the accuracy store dir convention; assignment store is separate.
    store = AssignmentStore("./assignment.db")
    _ = cfg  # config reserved for future per-shard protocol/port wiring
    app = create_app(store)
    uvicorn.run(app, host=host, port=port)


# convenience for drain/lifecycle callers
def set_draining(store: AssignmentStore, broker_id: str) -> None:
    store.set_broker_state(broker_id, BrokerState.DRAINING)
