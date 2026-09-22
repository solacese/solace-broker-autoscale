#!/usr/bin/env python3
"""Local-only manager demonstration: real Go clients, Solace brokers and migration.

The production planner consumes real SEMP counter deltas against a deliberately
small DEMO threshold, not a fabricated claim about broker capacity. No Cloud API.
"""
from __future__ import annotations

import argparse
import json
import os
import queue
import signal
import socket
import subprocess
import threading
import time
from contextlib import contextmanager
from pathlib import Path

import httpx
import uvicorn

from solace_autoscale.actuator.solace_cloud import SempConnection
from solace_autoscale.assignment.placement import assign
from solace_autoscale.assignment.service import create_app
from solace_autoscale.assignment.store import AssignmentStore, Broker, BrokerState
from solace_autoscale.assignment.topics import TopicRegistry
from solace_autoscale.config import AssignmentConfig, AutomationConfig, MessagingConfig
from solace_autoscale.controller.migration import MigrationEngine
from solace_autoscale.controller.planner import PartitionLoad, propose_move
from solace_autoscale.controller.semp import QueueManager
from solace_autoscale.controller.store import ControllerStore
from solace_autoscale_client.key_router import KeyRouter
from solace_autoscale_client.resolver import Resolver

ROOT = Path(__file__).resolve().parents[2]


def eventually(condition, timeout=30):
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() >= deadline:
            raise TimeoutError("demo condition timed out")
        time.sleep(0.05)


@contextmanager
def api(app):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    server = uvicorn.Server(uvicorn.Config(app, log_level="error"))
    thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
    thread.start()
    eventually(lambda: server.started)
    try:
        yield f"http://127.0.0.1:{sock.getsockname()[1]}"
    finally:
        server.should_exit = True
        thread.join(10)
        sock.close()


class Process:
    def __init__(self, binary, url, state, subscribe=False, credentials=None, handler_delay=0):
        self.lines = queue.Queue()
        self.all = []
        self.errors = []
        self.proc = subprocess.Popen(
            [str(binary), "--controller", url, "--state", str(state)]
            + (["--subscribe", "--handler-delay", str(handler_delay)] if subscribe else []),
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, bufsize=1,
            env={
                **os.environ,
                "SOLACE_USERNAME": "default",
                "SOLACE_PASSWORD": "default",
                **({"SOLACE_CREDENTIALS_JSON": json.dumps(credentials)} if credentials else {}),
            },
        )
        def read():
            for line in self.proc.stdout:
                result = json.loads(line)
                self.all.append(result)
                self.lines.put(result)
        self.thread = threading.Thread(target=read, daemon=True)
        self.thread.start()
        self.err_thread = threading.Thread(
            target=lambda: self.errors.extend(self.proc.stderr.read().splitlines()), daemon=True
        )
        self.err_thread.start()

    def request(self, value):
        if self.proc.poll() is not None:
            raise RuntimeError(f"Go process exited: {self.errors}")
        self.proc.stdin.write(json.dumps(value) + "\n")
        self.proc.stdin.flush()
        try:
            result = self.lines.get(timeout=35)
        except queue.Empty as exc:
            raise RuntimeError(f"Go response timed out: {self.errors}") from exc
        if result["kind"] in ("error", "rejected"):
            raise RuntimeError(result)
        return result

    def close(self, kill=False):
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL if kill else signal.SIGTERM)
        try:
            self.proc.wait(10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(5)
            raise
        self.thread.join(2)
        self.err_thread.join(2)
        self.proc.stdin.close()
        self.proc.stdout.close()
        self.proc.stderr.close()
        if not kill and self.proc.returncode != 0:
            raise RuntimeError(f"Go process failed ({self.proc.returncode}): {self.errors}")


class Lab:
    """Two explicit brokers; defaults are the disposable loopback demo."""
    def __init__(self, state: Path, partitions=4, connection_file: Path | None = None):
        self.state = state
        self.fleet = "demo-" + str(int(time.time() * 1000))
        self.db = AssignmentStore(state / "controller.db")
        self.partitions = partitions
        if connection_file is None:
            brokers = [
                {"broker_id": "a", "msg_vpn": "default", "semp": "http://127.0.0.1:18081",
                 "smf": "tcp://127.0.0.1:15556", "amqp": "amqp://127.0.0.1:15672",
                 "admin_username": "admin", "admin_password": "admin",
                 "client_username": "default", "client_password": "default"},
                {"broker_id": "b", "msg_vpn": "default", "semp": "http://127.0.0.1:18082",
                 "smf": "tcp://127.0.0.1:15557", "amqp": "amqp://127.0.0.1:15673",
                 "admin_username": "admin", "admin_password": "admin",
                 "client_username": "default", "client_password": "default"},
            ]
        else:
            brokers = json.loads(connection_file.read_text())["brokers"]
        self.source, self.target = (item["broker_id"] for item in brokers)
        self.credentials = (
            {
                item["broker_id"]: {
                    "username": item["client_username"],
                    "password": item["client_password"],
                }
                for item in brokers
            }
            if connection_file is not None
            else None
        )
        for index, item in enumerate(brokers):
            self.db.upsert_broker(Broker(
                item["broker_id"], "payments", item["msg_vpn"],
                BrokerState.ACTIVE if index == 0 else BrokerState.WARM,
                {"smf": item["smf"], "amqp": item["amqp"]},
            ))
        self.messaging = MessagingConfig.model_validate({
            "enabled": True, "allow_dynamic_groups": False,
            "routes": [{"pattern": "payments/*/*", "shard": "payments", "key_levels": [1]}],
            "subscriptions": [
                {"group": "ledger", "topics": ["payments/>"]},
                {"group": "audit", "topics": ["payments/*/created"]},
            ],
        })
        self.app = create_app(self.db, policy=AssignmentConfig(routing="partitioned", partitions=partitions,
                              lease_seconds=1), fleet_id=self.fleet, messaging=self.messaging)
        self.queues = QueueManager(
            self.fleet,
            {
                item["broker_id"]: SempConnection(
                    item["semp"], item["admin_username"], item["admin_password"]
                )
                for item in brokers
            },
            {item["broker_id"]: item["msg_vpn"] for item in brokers},
            100,
        )
        self.registry = TopicRegistry(self.db)
        self.queues.registry = self.registry
        self.queues.client_username = brokers[0]["client_username"]
        for group in self.messaging.subscriptions:
            self.registry.register(group.group, group.topics)
        if connection_file is None:
            for item in brokers:
                self.queues.configure_native(item["broker_id"])
        else:
            # Cloud-provided service credentials already reference a managed client profile.
            self.queues._native_configured.update(item["broker_id"] for item in brokers)
        for p in range(partitions):
            assign(
                self.db, "payments", f"partition:{p}", "guaranteed", time.time(), 1,
                allowed_broker_ids=frozenset({self.source}),
            )
            # SEMP can answer before the broker's message spool has finished starting.
            # Queue creation is idempotent, so retry readiness without weakening its checks.
            deadline = time.monotonic() + 60
            while True:
                try:
                    self.queues.prepare(self.source, "payments", p, enabled=True)
                    break
                except httpx.HTTPStatusError as exc:
                    if time.monotonic() >= deadline:
                        raise RuntimeError(f"Broker queue readiness failed: {exc.response.text}") from exc
                    time.sleep(0.5)
        self.registry.mark_ready(list(self.registry.groups()))
        self.store = ControllerStore(self.db)
        for p in range(partitions):
            self.store.mark_partition_ready("payments", p)
        self.engine = MigrationEngine(self.store, self.queues, AutomationConfig(
            poll_interval=1, migration_grace=2, empty_settle=1, migration_timeout=120,
        ))

    def owners(self):
        return [self.db.get_placement("payments", f"partition:{p}").broker_id
                for p in range(self.partitions)]

    def bound(self, broker, p):
        return self.queues.status(broker, "payments", p).consumers >= 1

    def close(self):
        self.queues.close()
        self.db.close()


def prepare_brokers():
    for port in (18081, 18082):
        base = f"http://127.0.0.1:{port}/SEMP/v2/config/msgVpns/default"
        with httpx.Client(auth=("admin", "admin"), timeout=5) as client:
            def ready(base=base):
                try:
                    return client.get(base).status_code == 200
                except httpx.HTTPError:
                    return False
            eventually(ready, 100)
            client.patch(base, json={"authenticationBasicEnabled": True,
                "authenticationBasicType": "internal", "maxMsgSpoolUsage": 1500,
                "serviceAmqpPlainTextEnabled": True}).raise_for_status()
            client.patch(base + "/clientUsernames/default",
                         json={"enabled": True, "password": "default"}).raise_for_status()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--connections", type=Path)
    parser.add_argument("--label", default="local disposable brokers")
    parser.add_argument("--migration-wait", type=float, default=45)
    parser.add_argument("--handler-delay", type=float, default=0)
    parser.add_argument("--payload-bytes", type=int, default=0)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    if args.connections is None:
        prepare_brokers()
    lab = Lab(args.output, connection_file=args.connections)
    start = time.monotonic()
    events, accepted, all_processed, sequences = [], [], [], {}
    publisher = subscriber = None
    def note(title, detail, **extra):
        event = {"seconds": round(time.monotonic() - start, 1), "title": title,
                 "detail": detail, "owners": lab.owners(), "accepted": len(accepted), **extra}
        events.append(event)
        print(f"{event['seconds']:5.1f}s  {title}: {detail}", flush=True)
    try:
        with api(lab.app) as url:
            subscriber = Process(
                args.binary, url, args.output / "subscriber", True, lab.credentials, args.handler_delay
            )
            publisher = Process(args.binary, url, args.output / "publisher", credentials=lab.credentials)
            eventually(lambda: all(lab.bound(lab.source, p) for p in range(4)))
            router = KeyRouter(Resolver(url), "payments", "demo", partitions=4)
            accounts = {}
            i = 0
            while len(accounts) < 4:
                name = f"account-{i}"
                accounts.setdefault(router.partition_for(json.dumps([name], separators=(",", ":"))), name)
                i += 1
            def publish(partition):
                account = accounts[partition]
                seq = sequences.get(account, 0)
                sequences[account] = seq + 1
                event_id = f"{account}-{seq}"
                result = publisher.request({"topic": f"payments/{account}/created",
                    "event_id": event_id, "payload": {"account": account, "sequence": seq,
                    "amount": 25, "padding": "x" * args.payload_bytes}})
                assert result["kind"] == "accepted"
                accepted.append(event_id)
            note(
                "One active broker",
                "Four ordered payment streams on the source. "
                "The destination is already provisioned and warm.",
            )
            for p in range(4):
                publish(p)
            publisher.request({"op": "flush"})
            eventually(lambda: len([x for x in subscriber.all if x.get("group") == "ledger"]) == 4)
            note("Payments arrive", "Ledger and audit receive independent native topic subscriptions.")
            before = [lab.queues.status(lab.source, "payments", p) for p in range(4)]
            measured_at = time.monotonic()
            for _ in range(20):
                for p in range(4):
                    publish(p)
            publisher.request({"op": "flush"})
            elapsed = time.monotonic() - measured_at
            after = [lab.queues.status(lab.source, "payments", p) for p in range(4)]
            rates = [
                (a.spooled_messages - b.spooled_messages) / elapsed
                for a, b in zip(after, before, strict=True)
            ]
            # Demonstration threshold: the observed burst is 120% of this small budget.
            # Real rates, deliberately reduced capacity; never used as a production model.
            demo_capacity = sum(rates) / 1.2
            loads = [
                PartitionLoad("payments", p, lab.source, rate / demo_capacity, 0)
                for p, rate in enumerate(rates)
            ]
            move = propose_move(loads, [lab.source, lab.target], trigger=0.8, target=0.65, excluded=set())
            assert move is not None
            chosen, target = move
            note("Burst detected — 120% of demo budget",
                 "The production planner selects a fitting partition from real broker counters. "
                 "This reduced budget is a demo trigger, not a broker benchmark.",
                 measured_copies_per_second=round(sum(rates), 1), demo_capacity=round(demo_capacity, 1))
            lab.db.set_broker_state(target, BrokerState.ACTIVE)
            lab.store.begin("payments", chosen.partition, lab.source, target, time.time())
            # Stop consumers to make the drain wait visible. Their durable queues survive.
            subscriber.close()
            all_processed.extend(subscriber.all)
            subscriber = Process(
                args.binary, url, args.output / "subscriber", True, lab.credentials, args.handler_delay
            )
            lab.engine.advance(lab.store.pending()[0], time.time())
            eventually(lambda: lab.bound(lab.target, chosen.partition))
            note(
                "Destination ready",
                f"The Go subscriber found the destination queue for partition {chosen.partition}.",
            )
            def fenced():
                lab.engine.advance(lab.store.pending()[0], time.time())
                return lab.store.pending()[0].phase == "draining"
            eventually(fenced)
            for _ in range(24):
                publish(chosen.partition)
            status = publisher.request({"op": "status"})["status"]
            assert status["pending"] == 24, status
            note(
                "Old ingress fenced",
                "24 new payments are safe on the publisher disk while the old queue drains.",
                pending=status["pending"],
            )
            publisher.close(kill=True)
            note(
                "Publisher killed — SIGKILL",
                "No graceful shutdown. Accepted publications must survive in the same outbox.",
                pending=24,
            )
            # Recover controller migration state too; ownership is never recomputed from membership.
            lab.engine = MigrationEngine(ControllerStore(lab.db), lab.queues, lab.engine.policy)
            publisher = Process(args.binary, url, args.output / "publisher", credentials=lab.credentials)
            assert publisher.request({"op": "status"})["status"]["pending"] == 24
            note(
                "Publisher restarted",
                "Same application, same outbox. No broker names in the publish call.",
                pending=24,
            )
            deadline = time.monotonic() + args.migration_wait
            while lab.store.pending():
                if time.monotonic() > deadline:
                    raise TimeoutError(f"migration blocked: {lab.store.pending()}")
                lab.engine.advance(lab.store.pending()[0], time.time())
                time.sleep(0.2)
            assert lab.owners()[chosen.partition] == lab.target
            note("Ownership committed", "Grace period and drain completed. New publications now use B.")
            publisher.request({"op": "flush"})
            for p in range(4):
                publish(p)
            publisher.request({"op": "flush"})
            def processed_ids(group):
                return {x["event_id"] for x in all_processed + subscriber.all if x.get("group") == group}
            eventually(
                lambda: processed_ids("ledger") == set(accepted)
                and processed_ids("audit") == set(accepted)
            )
            all_processed.extend(subscriber.all)
            for group in ("ledger", "audit"):
                for account in accounts.values():
                    ordered = [x["payload"]["sequence"] for x in all_processed
                               if x.get("group") == group and not x.get("duplicate")
                               and x["payload"]["account"] == account]
                    assert ordered == list(range(sequences[account])), (group, account, ordered)
            duplicates = sum(bool(x.get("duplicate")) for x in all_processed)
            note("Every accepted payment reconciled", "Both groups processed every ID, in account order. "
                 "Transport retries are deduplicated in each group's durable business transaction.",
                 pending=0, ledger=len(processed_ids("ledger")),
                 audit=len(processed_ids("audit")), duplicates=duplicates)
    finally:
        try:
            if publisher:
                publisher.close()
        finally:
            try:
                if subscriber:
                    subscriber.close()
            finally:
                lab.close()
    report = {"passed": True, "events": events, "accepted": len(accepted),
              "ledger": len({x["event_id"] for x in all_processed if x.get("group") == "ledger"}),
              "audit": len({x["event_id"] for x in all_processed if x.get("group") == "audit"}),
              "environment": args.label,
              "disclaimer": "Recorded functional run on two independent Solace services. Reduced "
                            "demo capacity; not production throughput or SLO qualification."}
    (args.output / "report.json").write_text(json.dumps(report, indent=2))
    template = (Path(__file__).parent / "presentation.html").read_text()
    document = template.replace("/*REPORT*/null", json.dumps(report).replace("<", "\\u003c"))
    (args.output / "index.html").write_text(document)
    print(f"\nVerified presentation: {args.output / 'index.html'}", flush=True)


if __name__ == "__main__":
    main()
