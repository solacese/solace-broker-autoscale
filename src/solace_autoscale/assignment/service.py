"""Assignment HTTP service (§9.1). Stateless over the store.

GET /assignment?shard=&client_id=&protocol=&mode=direct|guaranteed
  → per-protocol endpoint map (not a single host/port), broker_id, msg_vpn, state, lease_seconds.

Never vends credentials — returns a location only. Health + readiness endpoints. The service is
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

DEFAULT_LEASE_SECONDS = 300


def _now() -> float:
    return datetime.now(UTC).timestamp()


def create_app(store: AssignmentStore, *, clock: Callable[[], float] = _now,
               lease_seconds: int = DEFAULT_LEASE_SECONDS) -> FastAPI:
    app = FastAPI(title="solace-autoscale assignment service", version="1")

    @app.get("/healthz")
    def healthz() -> dict:
        return {"status": "ok"}

    @app.get("/readyz")
    def readyz() -> dict:
        # ready if the store is reachable
        try:
            store.brokers_for_shard("__probe__")
        except Exception as e:  # pragma: no cover
            raise HTTPException(status_code=503, detail=f"store not ready: {e}") from e
        return {"status": "ready"}

    @app.get("/assignment")
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
            # per-protocol endpoint map — never a single host/port, never a credential
            "endpoints": endpoints,
        }
        return body

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
