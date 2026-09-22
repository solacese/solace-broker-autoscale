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
    def __init__(self, binary, url, state, subscribe=False):
        self.lines = queue.Queue()
        self.all = []
        self.errors = []
        self.proc = subprocess.Popen(
            [str(binary), "--controller", url, "--state", str(state)]
            + (["--subscribe"] if subscribe else []),
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, bufsize=1,
            env={**os.environ, "SOLACE_USERNAME": "default", "SOLACE_PASSWORD": "default"},
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
    """Fixed loopback endpoints only. The launcher owns the two disposable containers."""
    def __init__(self, state: Path, partitions=4):
        self.state = state
        self.fleet = "demo-" + str(int(time.time() * 1000))
        self.db = AssignmentStore(state / "controller.db")
        self.partitions = partitions
        for broker, smf, amqp in [("a", 15556, 15672), ("b", 15557, 15673)]:
            self.db.upsert_broker(Broker(
                broker, "payments", "default", BrokerState.ACTIVE if broker == "a" else BrokerState.WARM,
                {"smf": f"tcp://127.0.0.1:{smf}", "amqp": f"amqp://127.0.0.1:{amqp}"},
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
        self.queues = QueueManager(self.fleet, {
            b: SempConnection(f"http://127.0.0.1:{p}", "admin", "admin")
            for b, p in [("a", 18081), ("b", 18082)]
        }, {"a": "default", "b": "default"}, 100)
        self.registry = TopicRegistry(self.db)
        self.queues.registry = self.registry
        self.queues.client_username = "default"
        for group in self.messaging.subscriptions:
            self.registry.register(group.group, group.topics)
        for b in ("a", "b"):
            self.queues.configure_native(b)
        for p in range(partitions):
            assign(self.db, "payments", f"partition:{p}", "guaranteed", time.time(), 1)
            # SEMP can answer before the broker's message spool has finished starting.
            # Queue creation is idempotent, so retry readiness without weakening its checks.
            deadline = time.monotonic() + 60
            while True:
                try:
                    self.queues.prepare("a", "payments", p, enabled=True)
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
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    prepare_brokers()
    lab = Lab(args.output)
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
            subscriber = Process(args.binary, url, args.output / "subscriber", True)
            publisher = Process(args.binary, url, args.output / "publisher")
            eventually(lambda: all(lab.bound("a", p) for p in range(4)))
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
                    "event_id": event_id, "payload": {"account": account, "sequence": seq, "amount": 25}})
                assert result["kind"] == "accepted"
                accepted.append(event_id)
            note("One active broker", "Four ordered payment streams on A. B is already provisioned and warm.")
            for p in range(4):
                publish(p)
            publisher.request({"op": "flush"})
            eventually(lambda: len([x for x in subscriber.all if x.get("group") == "ledger"]) == 4)
            note("Payments arrive", "Ledger and audit receive independent native topic subscriptions.")
            before = [lab.queues.status("a", "payments", p) for p in range(4)]
            measured_at = time.monotonic()
            for _ in range(20):
                for p in range(4):
                    publish(p)
            publisher.request({"op": "flush"})
            elapsed = time.monotonic() - measured_at
            after = [lab.queues.status("a", "payments", p) for p in range(4)]
            rates = [(a.spooled_messages - b.spooled_messages) / elapsed for a, b in zip(after, before)]
            # Demonstration threshold: the observed burst is 120% of this small budget.
            # Real rates, deliberately reduced capacity; never used as a production model.
            demo_capacity = sum(rates) / 1.2
            loads = [PartitionLoad("payments", p, "a", rate / demo_capacity, 0) for p, rate in enumerate(rates)]
            move = propose_move(loads, ["a", "b"], trigger=0.8, target=0.65, excluded=set())
            assert move is not None
            chosen, target = move
            note("Burst detected — 120% of demo budget",
                 "The production planner selects a fitting partition from real broker counters. "
                 "This reduced budget is a demo trigger, not a broker benchmark.",
                 measured_copies_per_second=round(sum(rates), 1), demo_capacity=round(demo_capacity, 1))
            lab.db.set_broker_state(target, BrokerState.ACTIVE)
            lab.store.begin("payments", chosen.partition, "a", target, time.time())
            # Stop consumers to make the drain wait visible. Their durable queues survive.
            subscriber.close()
            all_processed.extend(subscriber.all)
            subscriber = Process(args.binary, url, args.output / "subscriber", True)
            lab.engine.advance(lab.store.pending()[0], time.time())
            eventually(lambda: lab.bound("b", chosen.partition))
            note("Destination ready", f"The Go subscriber found B's preparing queue for partition {chosen.partition}.")
            def fenced():
                lab.engine.advance(lab.store.pending()[0], time.time())
                return lab.store.pending()[0].phase == "draining"
            eventually(fenced)
            for _ in range(24):
                publish(chosen.partition)
            status = publisher.request({"op": "status"})["status"]
            assert status["pending"] == 24, status
            note("Old ingress fenced", "24 new payments are safe on the publisher's disk while the old queue drains.",
                 pending=status["pending"])
            publisher.close(kill=True)
            note("Publisher killed — SIGKILL", "No graceful shutdown. Accepted publications must survive in the same outbox.", pending=24)
            # Recover controller migration state too; ownership is never recomputed from membership.
            lab.engine = MigrationEngine(ControllerStore(lab.db), lab.queues, lab.engine.policy)
            publisher = Process(args.binary, url, args.output / "publisher")
            assert publisher.request({"op": "status"})["status"]["pending"] == 24
            note("Publisher restarted", "Same application, same outbox. No broker names in the publish call.", pending=24)
            deadline = time.monotonic() + 45
            while lab.store.pending():
                if time.monotonic() > deadline:
                    raise TimeoutError(f"migration blocked: {lab.store.pending()}")
                lab.engine.advance(lab.store.pending()[0], time.time())
                time.sleep(0.2)
            assert lab.owners()[chosen.partition] == "b"
            note("Ownership committed", "Grace period and drain completed. New publications now use B.")
            publisher.request({"op": "flush"})
            for p in range(4):
                publish(p)
            publisher.request({"op": "flush"})
            def processed_ids(group):
                return {x["event_id"] for x in all_processed + subscriber.all if x.get("group") == group}
            eventually(lambda: processed_ids("ledger") == set(accepted) and processed_ids("audit") == set(accepted))
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
                 pending=0, ledger=len(processed_ids("ledger")), audit=len(processed_ids("audit")), duplicates=duplicates)
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
              "disclaimer": "Recorded local run on two real Solace containers. Reduced demo capacity. "
                            "No Cloud provisioning, production throughput or SLO qualification."}
    (args.output / "report.json").write_text(json.dumps(report, indent=2))
    template = (Path(__file__).parent / "presentation.html").read_text()
    (args.output / "index.html").write_text(template.replace("/*REPORT*/null", json.dumps(report).replace("<", "\\u003c")))
    print(f"\nVerified presentation: {args.output / 'index.html'}", flush=True)


if __name__ == "__main__":
    main()
