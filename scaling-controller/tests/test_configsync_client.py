"""Unit tests for the SEMP config client + reconcile orchestrator, with a mock HTTP transport.

No broker: an httpx.MockTransport serves canned SEMPv2 config responses so the snapshot shaping,
idempotency handling, and the reconcile plan are all exercised deterministically.
"""

from __future__ import annotations

import httpx

from solace_autoscale.configsync.semp_config import SempConfigClient, reconcile


def _mk_client(broker_id: str, handler) -> SempConfigClient:
    c = SempConfigClient.__new__(SempConfigClient)
    c.broker_id = broker_id
    c._base = "http://broker"
    c._client = httpx.Client(transport=httpx.MockTransport(handler), auth=("u", "p"))
    return c


def _collection(items):
    return {"data": items, "meta": {"count": len(items)}}


def _handler_for(queues, subs_by_queue):
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/queues"):
            return httpx.Response(200, json=_collection([{"queueName": q, "accessType": "exclusive"}
                                                         for q in queues]))
        if "/subscriptions" in path and request.method == "GET":
            qname = path.split("/queues/")[1].split("/")[0]
            return httpx.Response(200, json=_collection(
                [{"subscriptionTopic": t} for t in subs_by_queue.get(qname, [])]))
        if path.endswith(("/clientProfiles", "/aclProfiles", "/topicEndpoints")):
            return httpx.Response(200, json=_collection([]))
        # writes
        if request.method in ("POST", "PUT", "DELETE"):
            return httpx.Response(200, json={})
        return httpx.Response(404, json={})
    return handler


def test_snapshot_pages_through_all_objects():
    """A collection larger than one page must be read in full via meta.paging.nextPageUri."""
    page1 = [{"queueName": f"q{i}", "accessType": "exclusive"} for i in range(100)]
    page2 = [{"queueName": f"q{i}", "accessType": "exclusive"} for i in range(100, 151)]

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/queues") and "cursor" not in request.url.query.decode():
            return httpx.Response(200, json={
                "data": page1,
                "meta": {"paging": {"nextPageUri": "http://broker/SEMP/v2/config"
                                    "/msgVpns/default/queues?count=100&cursor=PAGE2"}},
            })
        if path.endswith("/queues"):  # page 2 (cursor present), no further cursor
            return httpx.Response(200, json=_collection(page2))
        return httpx.Response(200, json=_collection([]))

    client = _mk_client("b", handler)
    snap = client.snapshot("default", ["queue"])
    names = {o.identity[-1] for o in snap.of_kind("queue")}
    assert len(names) == 151
    assert "q0" in names and "q150" in names


def test_snapshot_single_page_no_cursor():
    """No nextPageUri → exactly one request, existing behaviour intact."""
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/queues"):
            calls.append(str(request.url))
            return httpx.Response(200, json=_collection(
                [{"queueName": "only", "accessType": "exclusive"}]))
        return httpx.Response(200, json=_collection([]))

    client = _mk_client("b", handler)
    snap = client.snapshot("default", ["queue"])
    assert {o.identity[-1] for o in snap.of_kind("queue")} == {"only"}
    assert len(calls) == 1


def test_snapshot_fetches_queues_when_subscriptions_requested():
    client = _mk_client("b", _handler_for(["orders"], {"orders": ["a/>", "b/>"]}))
    snap = client.snapshot("default", ["queueSubscription"])
    subs = {o.identity for o in snap.of_kind("queueSubscription")}
    assert subs == {("orders", "a/>"), ("orders", "b/>")}
    # queue was fetched to find the parent but not recorded (not in the diffed kinds)
    assert snap.of_kind("queue") == []


def test_reconcile_dry_run_plans_but_does_not_write():
    writes: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method in ("POST", "PUT", "DELETE"):
            writes.append(request.method)
        # source has a queue; targets have none
        base = _handler_for(["orders"], {"orders": ["a/>"]}) if "source" in str(request.url) \
            else _handler_for([], {})
        return base(request)

    # source and target share one transport but branch on a marker in the base url
    src = _mk_client("source", handler)
    src._base = "http://broker/source"
    tgt = _mk_client("target", handler)
    tgt._base = "http://broker/target"

    plan = reconcile(src, tgt_list := [tgt], "default",
                     kinds=["queue", "queueSubscription"], mode="additive", dry_run=True)
    assert plan.applied is False
    assert writes == []  # dry-run issues nothing
    ops = plan.ops_by_target["target"]
    kinds = [o.kind for o in ops]
    assert "queue" in kinds and "queueSubscription" in kinds
    assert kinds.index("queue") < kinds.index("queueSubscription")  # parents first


def test_idempotent_create_and_delete_do_not_raise():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(400, json={"meta": {"error": {"status": "ALREADY_EXISTS"}}})
        if request.method == "DELETE":
            return httpx.Response(400, json={"meta": {"error": {"status": "NOT_FOUND"}}})
        return httpx.Response(200, json=_collection([]))

    from solace_autoscale.configsync.diff import ConfigOp
    client = _mk_client("b", handler)
    # neither raises — a converged broker is not an error
    client.apply("default", [ConfigOp("create", "queue", ("orders",), {"accessType": "exclusive"})])
    client.apply("default", [ConfigOp("delete", "queue", ("orders",), {})])


def test_real_error_still_raises():
    import pytest

    from solace_autoscale.configsync.diff import ConfigOp
    from solace_autoscale.configsync.semp_config import ConfigSyncError

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"meta": {"error": {"status": "INVALID_PARAMETER"}}})

    client = _mk_client("b", handler)
    with pytest.raises(ConfigSyncError):
        client.apply("default", [ConfigOp("create", "queue", ("orders",), {"accessType": "bad"})])
