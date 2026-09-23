#!/usr/bin/env python3
"""Bounded synthetic workload measurements against one owned Cloud service."""

from __future__ import annotations

import argparse
import json
import os
import queue
import resource
import socket
import subprocess
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import httpx
import uvicorn
from solace_autoscale.actuator.solace_cloud import SempConnection
from solace_autoscale.assignment.placement import assign
from solace_autoscale.assignment.service import create_app
from solace_autoscale.assignment.store import AssignmentStore, Broker, BrokerState
from solace_autoscale.assignment.topics import TopicRegistry
from solace_autoscale.config import AssignmentConfig, MessagingConfig
from solace_autoscale.controller.semp import QueueManager
from solace_autoscale.controller.store import ControllerStore
from solace_autoscale.metrics.semp import SempCollector


@contextmanager
def api(app: Any) -> Iterator[str]:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    server = uvicorn.Server(uvicorn.Config(app, log_level="error"))
    thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        if time.monotonic() >= deadline:
            raise TimeoutError("assignment API startup timed out")
        time.sleep(.01)
    try:
        yield f"http://127.0.0.1:{sock.getsockname()[1]}"
    finally:
        server.should_exit = True
        thread.join(10)
        sock.close()


def client_command(
    binary: Path,
    controller: str,
    state: Path,
    groups: tuple[str, ...] = (),
    handler_delay_ms: int = 0,
) -> list[str]:
    args = [str(binary), "--controller", controller, "--state", str(state)]
    if groups:
        args.extend(("--subscribe", "--groups", ",".join(groups)))
        args.extend(("--handler-delay", f"{handler_delay_ms}ms"))
    return args


class Process:
    def __init__(
        self,
        binary: Path,
        controller: str,
        state: Path,
        credentials: dict[str, dict[str, str]],
        *,
        groups: tuple[str, ...] = (),
        handler_delay_ms: int = 0,
    ) -> None:
        self.lines: queue.Queue[dict[str, Any]] = queue.Queue()
        self.all: list[dict[str, Any]] = []
        args = client_command(binary, controller, state, groups, handler_delay_ms)
        self.process = subprocess.Popen(
            args,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env={**os.environ, "SOLACE_CREDENTIALS_JSON": json.dumps(credentials)},
        )
        assert self.process.stdout and self.process.stderr
        self.errors: list[str] = []

        def stdout() -> None:
            assert self.process.stdout
            for line in self.process.stdout:
                try:
                    item = json.loads(line)
                except json.JSONDecodeError as exc:
                    self.errors.append(f"invalid client output: {exc}")
                    continue
                item["observed_at"] = time.monotonic()
                self.all.append(item)
                self.lines.put(item)

        self.reader = threading.Thread(target=stdout, daemon=True)
        self.reader.start()
        self.error_reader = threading.Thread(
            target=lambda: self.errors.extend(self.process.stderr.read().splitlines()), daemon=True
        )
        self.error_reader.start()

    def next(self, timeout: float = 120) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        while True:
            if self.process.poll() is not None:
                raise RuntimeError(
                    f"client exited with code {self.process.returncode}: {self.errors}"
                )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"client response timed out: {self.errors}")
            try:
                return self.lines.get(timeout=min(0.1, remaining))
            except queue.Empty:
                continue

    def send(self, value: dict[str, Any]) -> None:
        if self.process.poll() is not None:
            raise RuntimeError(f"client exited: {self.errors}")
        assert self.process.stdin
        self.process.stdin.write(json.dumps(value) + "\n")
        self.process.stdin.flush()

    def request(self, value: dict[str, Any], timeout: float = 120) -> dict[str, Any]:
        self.send(value)
        result = self.next(timeout)
        if result.get("kind") in ("error", "rejected"):
            raise RuntimeError(f"client operation failed: {result}")
        return result

    def close(self) -> None:
        if self.process.poll() is None:
            self.process.terminate()
        try:
            self.process.wait(15)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(5)
        self.reader.join(2)
        self.error_reader.join(2)


def percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(len(ordered) * quantile + .999999) - 1))
    return ordered[index]


def reconcile_deliveries(
    accepted: set[str], groups: tuple[str, ...], processed: list[dict[str, Any]]
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, int], int]:
    by_group = {
        group: [item for item in processed if item.get("group") == group] for group in groups
    }
    loss = {
        group: len(accepted - {str(item["event_id"]) for item in items})
        for group, items in by_group.items()
    }
    duplicates = sum(
        len(items) - len({str(item["event_id"]) for item in items}) for items in by_group.values()
    )
    return by_group, loss, duplicates


def run_case(
    root: Path, binary: Path, connection: dict[str, Any], case: dict[str, Any]
) -> dict[str, Any]:
    state = root / case["name"]
    state.mkdir(parents=True, exist_ok=False)
    broker_id = connection["broker_id"]
    groups = tuple(f"{case['name']}-{index}" for index in range(case["fanout"]))
    messaging = MessagingConfig.model_validate({
        "enabled": True,
        "allow_dynamic_groups": False,
        "routes": [{"pattern": "qualify/*/*", "shard": "qualification", "key_levels": [1]}],
        "subscriptions": [{"group": group, "topics": ["qualify/>"]} for group in groups],
    })
    store = AssignmentStore(state / "controller.db")
    store.upsert_broker(Broker(
        broker_id, "qualification", connection["msg_vpn"], BrokerState.ACTIVE,
        {"smf": connection["smf"], "amqp": connection["amqp"]},
    ))
    app = create_app(
        store,
        policy=AssignmentConfig(
            routing="partitioned", partitions=case.get("partitions", 4), lease_seconds=1
        ),
        fleet_id="qualify-" + case["name"],
        messaging=messaging,
    )
    queues = QueueManager(
        "qualify-" + case["name"],
        {broker_id: SempConnection(
            connection["semp"], connection["admin_username"], connection["admin_password"]
        )},
        {broker_id: connection["msg_vpn"]},
        int(case.get("queue_spool_mb", 100)),
    )
    registry = TopicRegistry(store)
    queues.registry = registry
    queues.client_username = connection["client_username"]
    queues._native_configured.add(broker_id)
    controller_store = ControllerStore(store)
    for group in messaging.subscriptions:
        registry.register(group.group, group.topics)
    partitions = case.get("partitions", 4)
    for partition in range(partitions):
        assign(
            store, "qualification", f"partition:{partition}", "guaranteed", time.time(), 1,
            allowed_broker_ids=frozenset({broker_id}),
        )
    credentials = {broker_id: {"username": connection["client_username"],
                               "password": connection["client_password"]}}
    before_sample = after_sample = None
    subscriber = None
    publishers: list[Process] = []
    accepted_at: dict[str, float] = {}
    acceptance_ms: list[float] = []
    children_before = resource.getrusage(resource.RUSAGE_CHILDREN)
    started = time.monotonic()
    publish_started = publish_finished = started
    try:
        readiness_deadline = time.monotonic() + 60
        while True:
            try:
                queues.prepare(broker_id, "qualification", 0, enabled=True)
                break
            except httpx.HTTPStatusError as exc:
                if time.monotonic() >= readiness_deadline:
                    raise RuntimeError(
                        f"broker queue readiness failed: {exc.response.text}"
                    ) from exc
                time.sleep(.5)
        for partition in range(partitions):
            queues.prepare(broker_id, "qualification", partition, enabled=True)
            controller_store.mark_partition_ready("qualification", partition)
        registry.mark_ready(list(groups))
        with SempCollector(
            connection["semp"], connection["admin_username"], connection["admin_password"]
        ) as collector:
            before_sample = collector.collect(
                "qualification", connection["msg_vpn"], time.time(), 1
            )
        with api(app) as url:
            subscriber = Process(
                binary, url, state / "subscriber", credentials,
                groups=groups, handler_delay_ms=case["handler_delay_ms"],
            )
            subscribed = subscriber.next(timeout=120)
            if subscribed.get("kind") != "subscribed":
                raise RuntimeError(f"subscriber did not become ready: {subscribed}")
            concurrency = int(case.get("publisher_concurrency", 4))
            publishers = [
                Process(binary, url, state / f"publisher-{index}", credentials)
                for index in range(concurrency)
            ]
            padding = "x" * max(0, case["payload_bytes"] - 128)
            publish_started = time.monotonic()
            offered_rate = float(case["offered_messages_per_second"])
            next_send = publish_started
            pending_by_publisher = [0] * concurrency
            for index in range(case["messages"]):
                event_id = f"{case['name']}-{index}"
                if offered_rate > 0:
                    next_send = publish_started + index / offered_rate
                    delay = next_send - time.monotonic()
                    if delay > 0:
                        time.sleep(delay)
                begin = time.monotonic()
                accepted_at[event_id] = begin
                publisher_index = index % concurrency
                publishers[publisher_index].send({
                    "topic": f"qualify/account-{index % partitions}/created",
                    "event_id": event_id,
                    "payload": {
                        "account": f"account-{index % partitions}",
                        "sequence": index // partitions,
                        "sent": begin,
                        "padding": padding,
                    },
                })
                pending_by_publisher[publisher_index] += 1
            publish_finished = time.monotonic()
            for publisher_index, publisher in enumerate(publishers):
                for _ in range(pending_by_publisher[publisher_index]):
                    response = publisher.next()
                    if response.get("kind") != "accepted":
                        raise RuntimeError(f"publisher rejected offered event: {response}")
                    event = str(response["event_id"])
                    acceptance_ms.append((response["observed_at"] - accepted_at[event]) * 1000)
                publisher.request({"op": "flush"})
            deadline = time.monotonic() + 180
            expected_ids = set(accepted_at)
            while True:
                processed_now = [item for item in subscriber.all if item.get("kind") == "processed"]
                received = {
                    group: {str(item.get("event_id")) for item in processed_now if item.get("group") == group}
                    for group in groups
                }
                if all(ids == expected_ids for ids in received.values()):
                    break
                if subscriber.process.poll() is not None:
                    raise RuntimeError(
                        f"subscriber exited with code {subscriber.process.returncode}: "
                        f"{subscriber.errors}"
                    )
                if time.monotonic() >= deadline:
                    missing = {group: len(expected_ids - ids) for group, ids in received.items()}
                    raise TimeoutError(
                        f"subscriber processing did not reconcile IDs: missing={missing}; "
                        f"errors={subscriber.errors}"
                    )
                time.sleep(.05)
            time.sleep(float(case.get("quiet_seconds", .25)))
        with SempCollector(
            connection["semp"], connection["admin_username"], connection["admin_password"]
        ) as collector:
            after_sample = collector.collect(
                "qualification", connection["msg_vpn"], time.time(), 1
            )
    finally:
        for publisher in publishers:
            publisher.close()
        if subscriber:
            subscriber.close()
        queues.close()
        store.close()
    elapsed = time.monotonic() - started
    children_after = resource.getrusage(resource.RUSAGE_CHILDREN)
    processed = [item for item in subscriber.all if item.get("kind") == "processed"]
    accepted = set(accepted_at)
    by_group, loss, duplicates = reconcile_deliveries(accepted, groups, processed)
    latencies = [
        (item["observed_at"] - accepted_at[item["event_id"]]) * 1000
        for item in processed if item["event_id"] in accepted_at
    ]
    order_violations = 0
    for items in by_group.values():
        by_account: dict[str, list[int]] = {}
        for item in items:
            account = item["payload"].get("account")
            sequence = item["payload"].get("sequence")
            if isinstance(account, str) and isinstance(sequence, int) and not item.get("duplicate"):
                by_account.setdefault(account, []).append(sequence)
        order_violations += sum(
            values != sorted(values) for values in by_account.values()
        )
    if before_sample is None or after_sample is None:
        raise RuntimeError("broker telemetry collection did not complete")
    if any(loss.values()) or duplicates or order_violations:
        raise RuntimeError(
            f"workload invariants failed: loss={loss}, duplicates={duplicates}, "
            f"ordering_violations={order_violations}"
        )
    return {
        "name": case["name"], "payload_bytes": case["payload_bytes"],
        "fanout": case["fanout"], "messages": case["messages"],
        "handler_delay_ms": case["handler_delay_ms"],
        "configured_offered_messages_per_second": case["offered_messages_per_second"],
        "publisher_concurrency": concurrency, "partitions": partitions,
        "accepted": len(accepted),
        "processed": {group: len(items) for group, items in by_group.items()},
        "loss": loss,
        "duplicates": duplicates,
        "ordering_violations": order_violations,
        "elapsed_seconds": elapsed,
        "publish_seconds": publish_finished - publish_started,
        "achieved_offer_messages_per_second": case["messages"] / (publish_finished - publish_started),
        "offer_schedule_lag_ms": max(
            0,
            (publish_finished - publish_started)
            - (case["messages"] - 1) / case["offered_messages_per_second"],
        ) * 1000 if case["offered_messages_per_second"] else 0,
        "acceptance_latency_ms": {"p50": percentile(acceptance_ms, .50),
                                  "p95": percentile(acceptance_ms, .95),
                                  "p99": percentile(acceptance_ms, .99)},
        "processing_latency_ms": {"p50": percentile(latencies, .50),
                                  "p95": percentile(latencies, .95),
                                  "p99": percentile(latencies, .99)},
        "client_cpu_seconds": (
            children_after.ru_utime + children_after.ru_stime
            - children_before.ru_utime - children_before.ru_stime
        ),
        "broker_before": before_sample.__dict__, "broker_after": after_sample.__dict__,
        "bottleneck": "undetermined; bounded run did not establish saturation",
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--connections", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    connection_doc = json.loads(args.connections.read_text())
    cases = [
        {"name": "small-steady", "payload_bytes": 256, "fanout": 1,
         "messages": 200, "offered_messages_per_second": 200, "handler_delay_ms": 0},
        {"name": "medium-fanout", "payload_bytes": 4096, "fanout": 2,
         "messages": 200, "offered_messages_per_second": 500, "handler_delay_ms": 0},
        {"name": "large-burst", "payload_bytes": 65536, "fanout": 2,
         "messages": 100, "offered_messages_per_second": 0, "handler_delay_ms": 0},
        {"name": "slow-consumer", "payload_bytes": 1024, "fanout": 2,
         "messages": 100, "offered_messages_per_second": 500, "handler_delay_ms": 20},
    ]
    results = [run_case(args.output, args.binary, connection_doc["brokers"][0], case)
               for case in cases]
    report = {
        "schema_version": "cloud-workload-v1",
        "environment": connection_doc["service_class"],
        "broker_version": connection_doc["broker_version"],
        "topology": "one independent HA service",
        "protocol": "AMQP 1.0 over TLS",
        "measurement_scope": "bounded client-to-broker behavior; not saturation capacity",
        "cases": results,
    }
    (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
