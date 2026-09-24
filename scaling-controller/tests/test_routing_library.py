"""Customer routing evaluator contracts, durable acceptance and Python/Go vectors."""

import copy
import hashlib
import json
import sqlite3
import threading
from pathlib import Path

import pytest

from solace_autoscale.config import Config, MessagingConfig, TopicRoute
from solace_autoscale.controller.features import feature_contract
from solace_autoscale_client import (
    BusinessKey,
    Message,
    MessagingClient,
    RoutingEvaluationError,
    RoutingEvaluatorRegistry,
    SHA256Digest,
)


def library_config(*, result="key"):
    route = TopicRoute.model_validate(
        {
            "pattern": "orders/*/*",
            "shard": "orders",
            "key_evaluator": {"name": "order-entity", "version": "1.0.0", "result": result},
        }
    )
    return {
        "fleet_id": "orders",
        "partitions": 128,
        "routes": [route.routing_contract()],
        "groups": {"processor": ["orders/>"]},
        "ready_groups": ["processor"],
    }


def client(tmp_path, monkeypatch, registry, *, result="key"):
    config = library_config(result=result)
    monkeypatch.setattr(MessagingClient, "_request", lambda *args: copy.deepcopy(config))
    monkeypatch.setattr(MessagingClient, "_run", lambda self: None)
    return MessagingClient(
        "http://unused",
        state_dir=tmp_path,
        credentials=lambda _: ("u", "p"),
        routing_evaluators=registry,
    )


def order_key(message):
    return BusinessKey(f'{message.headers["tenant"]}:{message.payload["order_id"]}')


def test_payload_headers_key_and_message_identity_are_borrowed(tmp_path, monkeypatch):
    seen = []

    def evaluate(message):
        seen.append(message)
        return order_key(message)

    registry = RoutingEvaluatorRegistry().register("order-entity", "1.0.0", evaluate)
    message = Message(
        "orders/eu/created",
        {"order_id": "é-42", "amount": 20},
        "evt-1",
        {"tenant": "acme"},
    )
    with client(tmp_path, monkeypatch, registry) as opened:
        opened.publish_message(message)
        opened.publish("orders/eu/settled", message.payload, event_id="evt-2", headers=message.headers)
        assert seen[0] is message
        assert seen[1].payload is message.payload
        assert seen[1].headers is message.headers
        rows = opened.outboxes["orders"].db.execute(
            "SELECT event_id,partition,routing_kind,routing_value,evaluator_name,evaluator_version "
            "FROM outbox ORDER BY seq"
        ).fetchall()
        assert rows[0][1] == rows[1][1]
        assert rows[0][2:] == ("key", "acme:é-42", "order-entity", "1.0.0")


def test_sha256_result_maps_directly_without_heuristic_or_double_hash(tmp_path, monkeypatch):
    digest = hashlib.sha256("租户:注文-42".encode()).digest()
    registry = RoutingEvaluatorRegistry().register(
        "order-entity", "1.0.0", lambda message: SHA256Digest(digest)
    )
    with client(tmp_path, monkeypatch, registry, result="sha256") as opened:
        opened.publish(
            "orders/jp/created",
            {"order_id": "注文-42"},
            event_id="evt",
            headers={"tenant": "租户"},
        )
        row = opened.outboxes["orders"].db.execute(
            "SELECT partition,routing_kind,routing_value FROM outbox"
        ).fetchone()
        assert row == (int.from_bytes(digest, "big") % 128, "sha256", digest.hex())


@pytest.mark.parametrize(
    "result_format,value,error",
    [
        ("key", "looks" + "a" * 64, "BusinessKey"),
        ("key", BusinessKey(""), "1-1024"),
        ("key", BusinessKey("é" * 513), "1-1024"),
        ("sha256", BusinessKey("abc"), "SHA256Digest"),
        ("sha256", SHA256Digest(b"x" * 31), "exactly 32"),
    ],
)
def test_bad_typed_or_bounded_result_rejects_before_acceptance(
    tmp_path, monkeypatch, result_format, value, error
):
    registry = RoutingEvaluatorRegistry().register("order-entity", "1.0.0", lambda message: value)
    with client(tmp_path, monkeypatch, registry, result=result_format) as opened:
        with pytest.raises(RoutingEvaluationError, match=error):
            opened.publish("orders/eu/created", {}, event_id="evt", headers={"tenant": "acme"})
        assert opened.pending() == 0


def test_callback_runs_without_client_lock_and_failure_accepts_nothing(tmp_path, monkeypatch):
    entered, release = threading.Event(), threading.Event()

    def evaluate(message):
        entered.set()
        assert release.wait(2)
        if message.event_id == "bad":
            raise RuntimeError("deterministic customer rejection")
        return BusinessKey("entity")

    registry = RoutingEvaluatorRegistry().register("order-entity", "1.0.0", evaluate)
    with client(tmp_path, monkeypatch, registry) as opened:
        result = []
        thread = threading.Thread(
            target=lambda: result.append(
                opened.publish(
                    "orders/eu/created", {}, event_id="good", headers={"tenant": "acme"}
                )
            )
        )
        thread.start()
        assert entered.wait(1)
        assert opened.status()["pending"] == 0
        release.set()
        thread.join(2)
        assert not thread.is_alive() and result == [None]
        release.clear()
        entered.clear()
        failed = threading.Thread(
            target=lambda: pytest.raises(RoutingEvaluationError, opened.publish,
                "orders/eu/created", {}, event_id="bad", headers={"tenant": "acme"})
        )
        failed.start()
        assert entered.wait(1)
        release.set()
        failed.join(2)
        assert opened.pending() == 1


def test_close_during_callback_prevents_acceptance(tmp_path, monkeypatch):
    entered, release = threading.Event(), threading.Event()

    def evaluate(message):
        entered.set()
        release.wait(2)
        return BusinessKey("entity")

    registry = RoutingEvaluatorRegistry().register("order-entity", "1.0.0", evaluate)
    opened = client(tmp_path, monkeypatch, registry)
    error = []

    def publish():
        try:
            opened.publish("orders/eu/created", {}, event_id="evt", headers={"tenant": "acme"})
        except Exception as exc:
            error.append(exc)

    thread = threading.Thread(target=publish)
    thread.start()
    assert entered.wait(1)
    opened.close()
    release.set()
    thread.join(2)
    assert isinstance(error[0], RuntimeError)


def test_missing_or_wrong_version_rejected_at_startup(tmp_path, monkeypatch):
    config = library_config()
    monkeypatch.setattr(MessagingClient, "_request", lambda *args: copy.deepcopy(config))
    for registry in (
        RoutingEvaluatorRegistry(),
        RoutingEvaluatorRegistry().register("order-entity", "2.0.0", order_key),
    ):
        with pytest.raises(ValueError, match="not registered"):
            MessagingClient(
                "http://unused",
                state_dir=tmp_path / str(len(list(registry.identities()))),
                credentials=lambda _: ("u", "p"),
                routing_evaluators=registry,
            )


def test_subscriber_validation_mode_is_pinned_and_retry_is_idempotent(tmp_path, monkeypatch):
    import solace_autoscale_client.messaging as module

    registry = RoutingEvaluatorRegistry().register("order-entity", "1.0.0", order_key)
    config = library_config()
    requests = []

    def request(self, path, body=None):
        if body is None:
            return copy.deepcopy(config)
        requests.append(body)
        return {"shards": ["orders"]}

    class Worker:
        flows = {}
        def __init__(self, *args, **kwargs): pass
        def close(self): pass

    monkeypatch.setattr(MessagingClient, "_request", request)
    monkeypatch.setattr(MessagingClient, "_run", lambda self: None)
    monkeypatch.setattr(module, "ManagedConsumers", Worker)
    with MessagingClient(
        "http://unused", state_dir=tmp_path, credentials=lambda _: ("u", "p"),
        routing_evaluators=registry,
    ) as opened:
        opened.subscribe(group="processor", handler=lambda message: None, timeout=0, validate_routing=True)
        opened.subscribe(group="processor", handler=lambda message: None, timeout=0, validate_routing=True)
        with pytest.raises(ValueError, match="mode cannot change"):
            opened.subscribe(group="processor", handler=lambda message: None, timeout=0)
        assert len(requests) == 2


def test_restart_uses_persisted_partition_without_reevaluation(tmp_path, monkeypatch):
    calls = []
    registry = RoutingEvaluatorRegistry().register(
        "order-entity", "1.0.0", lambda message: calls.append(message) or BusinessKey("entity-7")
    )
    state = tmp_path / "state"
    opened = client(state, monkeypatch, registry)
    opened.publish("orders/eu/created", {"order_id": 7}, event_id="evt", headers={"tenant": "x"})
    row = opened.outboxes["orders"].heads()[0]
    opened.close()
    assert len(calls) == 1
    restarted = client(state, monkeypatch, registry)
    try:
        assert restarted.outboxes["orders"].heads()[0] == row
        assert len(calls) == 1
    finally:
        restarted.close()


def test_old_outbox_and_absent_route_defaults_remain_compatible(tmp_path):
    route = TopicRoute(pattern="orders/*/*", shard="orders", key_levels=[1])
    assert route.routing_contract() == {
        "pattern": "orders/*/*", "shard": "orders", "key_levels": [1]
    }
    path = tmp_path / "legacy.db"
    db = sqlite3.connect(path)
    db.execute(
        "CREATE TABLE outbox (seq INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT UNIQUE NOT NULL, "
        "partition INTEGER NOT NULL, payload BLOB NOT NULL)"
    )
    db.execute("CREATE TABLE routing_contract (value TEXT NOT NULL)")
    db.execute(
        "INSERT INTO routing_contract VALUES (?)",
        (json.dumps(["orders", 128, "guaranteed", "sha256-json-v1"]),),
    )
    db.execute("INSERT INTO outbox(event_id,partition,payload) VALUES ('old',4,x'7b7d')")
    db.commit()
    db.close()
    from solace_autoscale_client.key_router import KeyRouter
    from solace_autoscale_client.outbox import DurableOutbox
    from solace_autoscale_client.resolver import Resolver

    box = DurableOutbox(path, KeyRouter(Resolver("http://unused"), "orders", "test"))
    try:
        assert box.pending() == 1
        assert box.db.execute("SELECT routing_kind FROM outbox").fetchone() == (None,)
    finally:
        box.close()


def test_evaluator_identity_uses_utf8_byte_bounds_and_feature_contract_pins_version():
    with pytest.raises(ValueError, match="UTF-8 bytes"):
        TopicRoute.model_validate(
            {
                "pattern": "orders/*/*",
                "shard": "orders",
                "key_evaluator": {"name": "é" * 65, "version": "1", "result": "key"},
            }
        )
    cfg = Config.model_validate(
        {
            "messaging": {
                "enabled": True,
                "routes": [library_config()["routes"][0]],
            }
        }
    )
    contract = feature_contract(cfg, ["orders"], deployment_mode="HA")
    assert contract["routing_libraries"] == {
        "orders/*/*": {"name": "order-entity", "version": "1.0.0", "result": "key"}
    }
    changed = Config.model_validate(
        {
            "messaging": {
                "enabled": True,
                "routes": [
                    {
                        "pattern": "orders/*/*",
                        "shard": "orders",
                        "key_evaluator": {
                            "name": "order-entity", "version": "2.0.0", "result": "key"
                        },
                    }
                ],
            }
        }
    )
    assert feature_contract(changed, ["orders"], deployment_mode="HA") != contract


def test_python_reads_shared_key_and_digest_partition_vectors():
    vectors = json.loads(
        (Path(__file__).parents[2] / "shim/testdata/customer-routing.json").read_text()
    )
    from solace_autoscale_client.key_router import KeyRouter
    from solace_autoscale_client.resolver import Resolver

    router = KeyRouter(Resolver("http://unused"), "orders", "test")
    for vector in vectors:
        if vector["result"] == "key":
            partition = router.partition_for(vector["value"])
        else:
            tenant, order = vector["tenant"].encode(), vector["order_id"].encode()
            canonical = (
                len(tenant).to_bytes(4, "big") + tenant
                + len(order).to_bytes(4, "big") + order
            )
            assert hashlib.sha256(canonical).hexdigest() == vector["value"]
            partition = int.from_bytes(bytes.fromhex(vector["value"]), "big") % 128
        assert partition == vector["partition128"]


def test_sha256_hex_constructor_is_canonical_and_explicit():
    digest = hashlib.sha256(b"order").digest()
    assert SHA256Digest.from_hex(digest.hex()).value == digest
    for value in (digest.hex().upper(), digest.hex()[:-1], "g" * 64):
        with pytest.raises(RoutingEvaluationError):
            SHA256Digest.from_hex(value)


def test_messaging_config_rejects_ambiguous_library_and_key_levels():
    with pytest.raises(ValueError, match="replaces key_levels"):
        MessagingConfig.model_validate(
            {
                "enabled": True,
                "routes": [
                    {
                        "pattern": "orders/*/*",
                        "shard": "orders",
                        "key_levels": [1],
                        "key_evaluator": {"name": "x", "version": "1"},
                    }
                ],
            }
        )
