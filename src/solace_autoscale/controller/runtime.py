"""Unattended managed-queue controller over an explicitly scoped, pre-provisioned fleet.

Warm services are real provisioned capacity, not fictitious brokers. The loop measures queue
counter deltas, activates warm capacity, and automatically hands over fitting partitions.
"""

from __future__ import annotations

import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

from ..assignment.placement import assign
from ..assignment.store import AssignmentStore, Broker, BrokerState
from ..assignment.topics import TopicRegistry
from ..capacity.model import lookup
from ..capacity.schema import CapacityModel
from ..config import Config, ShardScalingPolicy
from ..metrics.fleet import FleetInventory
from .migration import MigrationEngine
from .planner import PartitionLoad, propose_move
from .semp import QueueManager, QueueStatus
from .store import ControllerStore


@dataclass(frozen=True)
class ControlResult:
    state: str
    detail: str
    migrations: int


@dataclass(frozen=True)
class EffectiveScalingPolicy:
    enabled: bool
    trigger_utilization: float
    target_utilization: float
    scale_up_window: float
    cooldown: float


class Controller:
    """Single writer: the CLI holds an OS file lock for its lifetime; assignment remains concurrent."""

    def __init__(
        self,
        config: Config,
        model: CapacityModel,
        inventory: FleetInventory,
        assignments: AssignmentStore,
        queues: QueueManager,
    ) -> None:
        inventory.validate_profile(model, config.fleet.service_class)
        if not config.automation.enabled or config.assignment.routing != "partitioned":
            raise ValueError("controller requires automation.enabled and assignment.routing: partitioned")
        if config.workload.delivery != "guaranteed":
            raise ValueError("automatic handover supports guaranteed managed queues only")
        if model.synthetic:
            raise ValueError("synthetic models cannot control brokers")
        if config.actuation.mode == "recommend" or config.actuation.dry_run:
            raise ValueError("use explicit scale-up-only/full mode and dry_run: false for controller")
        if config.actuation.require_confirmation:
            raise ValueError(
                "unattended controller requires require_confirmation: false in its explicit config"
            )
        if config.messaging.enabled and {r.shard for r in config.messaging.routes} != {
            b.shard for b in inventory.brokers
        }:
            raise ValueError("native messaging workloads must exactly match inventoried shards")
        self.config, self.model, self.inventory = config, model, inventory
        self.assignments, self.queues = assignments, queues
        assignments.ensure_routing(config.assignment.routing, config.assignment.partitions)
        assignments.ensure_managed_namespace(config.automation.fleet_id)
        self.store = ControllerStore(assignments)
        self.topics = TopicRegistry(assignments)
        self.topics.contract(config.messaging.routing_contract())
        if config.messaging.enabled:
            for group in config.messaging.subscriptions:
                self.topics.register(group.group, group.topics)
        if set(config.automation.shards) - {b.shard for b in inventory.brokers}:
            raise ValueError("scaling overrides must name inventoried shards")
        self._prepared_groups: tuple[str, ...] = ()
        if config.messaging.enabled:
            if {r.shard for r in config.messaging.routes} - {b.shard for b in inventory.brokers}:
                raise ValueError("each messaging route needs an inventoried shard")
            queues.registry = self.topics
            queues.client_username = config.provisioning.client_username
        self.migrations = MigrationEngine(self.store, queues, config.automation)
        self.previous: dict[tuple[str, int], tuple[float, QueueStatus, str]] = {}
        self.overloaded_since: dict[str, float] = {}
        self.last_migration_at: float | None = None
        for endpoint in inventory.brokers:
            if "smf" not in endpoint.endpoints:
                raise ValueError(
                    f"{endpoint.broker_id}: an SMF endpoint is required for automatic routing"
                )
            stored = assignments.get_broker(endpoint.broker_id)
            if stored is not None and (
                stored.shard != endpoint.shard or stored.msg_vpn != endpoint.msg_vpn
                or stored.endpoints != endpoint.endpoints
            ):
                raise ValueError(
                    "inventoried broker identity changed; existing queue ownership must be preserved"
                )
            if stored is None:
                assignments.upsert_broker(
                    Broker(
                        endpoint.broker_id,
                        endpoint.shard,
                        endpoint.msg_vpn,
                        BrokerState(endpoint.role),
                        endpoint.endpoints,
                    )
                )

    def scaling_policy(self, shard: str) -> EffectiveScalingPolicy:
        """Resolve YAML overrides; omitted settings inherit the global controller policy."""
        override = self.config.automation.shards.get(shard, ShardScalingPolicy())
        window = (
            float(self.config.policy.scale_up_window)
            if self.config.policy.scale_up_window != "auto"
            else max(3 * self.config.metrics.scrape_interval, 30)
        )
        return EffectiveScalingPolicy(
            override.enabled,
            override.trigger_utilization or self.config.automation.trigger_utilization,
            override.target_utilization or self.config.automation.target_utilization,
            override.scale_up_window if override.scale_up_window is not None else window,
            override.cooldown if override.cooldown is not None else self.config.policy.cooldown,
        )

    def bootstrap(self, now: float) -> None:
        """Provision managed partition queues on their durable initial owners; never touch other names."""
        if Path(self.config.actuation.kill_switch_file).exists():
            return
        if self.config.messaging.enabled:
            for broker in self.inventory.brokers:
                self.queues.configure_native(broker.broker_id)
        groups = tuple(self.topics.groups())
        pending = {(m.shard, m.partition) for m in self.store.pending()}
        for shard in sorted({b.shard for b in self.inventory.brokers}):
            for partition in range(self.config.assignment.partitions):
                if (shard, partition) in pending:
                    continue
                placement = assign(
                    self.assignments,
                    shard,
                    f"partition:{partition}",
                    "guaranteed",
                    now,
                    self.config.assignment.lease_seconds,
                    strategy=self.config.assignment.strategy,
                    broker_weights=self.config.assignment.broker_weights,
                )
                self.store.event(
                    now,
                    "bootstrap-queue-intent",
                    {"broker": placement.broker.broker_id, "shard": shard, "partition": partition},
                )
                self.queues.prepare(placement.broker.broker_id, shard, partition, enabled=True)

        if self.config.messaging.enabled and not pending:
            self.topics.mark_ready(list(groups))
            self._prepared_groups = groups

    def tick(self, now: float) -> ControlResult:
        """Advance durable work first; plan from complete fresh deltas only when no move is pending."""
        if Path(self.config.actuation.kill_switch_file).exists():
            return ControlResult("halted", "kill switch present", len(self.store.pending()))
        pending = self.store.pending()
        for migration in pending:
            self.migrations.advance(migration, now)
        if pending:
            # Re-measure after cutover; never plan using source load that has supposedly moved.
            self.previous.clear()
            self.overloaded_since.clear()
            return ControlResult("migrating", "advancing durable handovers", len(self.store.pending()))
        if self.config.messaging.enabled and tuple(self.topics.groups()) != self._prepared_groups:
            self.bootstrap(now)
            self.previous.clear()
            self.overloaded_since.clear()
        loads: list[PartitionLoad] = []
        scrape_started = time.monotonic()
        observations: dict[tuple[str, int], tuple[float, QueueStatus, str]] = {}
        try:
            owners = {}
            for shard in sorted({b.shard for b in self.inventory.brokers}):
                for partition in range(self.config.assignment.partitions):
                    owner = self.assignments.get_placement(shard, f"partition:{partition}")
                    if not owner:
                        raise ValueError("partition not initialized")
                    owners[shard, partition] = owner
            # Bound parallel SEMP reads so many partitions do not require a serial network roundtrip each.
            with ThreadPoolExecutor(max_workers=16) as pool:
                futures = {
                    key: pool.submit(self.queues.status, owner.broker_id, *key)
                    for key, owner in owners.items()
                }
                for key, future in futures.items():
                    shard, partition = key
                    owner = owners[key]
                    status = future.result()
                    if not status.ingress_enabled:
                        raise ValueError("owned queue unexpectedly fenced outside a migration")
                    observations[key] = (now, status, owner.broker_id)
                    previous = self.previous.get(key)
                    if previous is None:
                        continue
                    ts, prior, broker_id = previous
                    elapsed = now - ts
                    if (
                        broker_id != owner.broker_id
                        or not 0 < elapsed <= 2 * self.config.automation.poll_interval
                        or status.spooled_messages < prior.spooled_messages
                        or status.spooled_bytes < prior.spooled_bytes
                    ):
                        raise ValueError("counter reset, owner change or telemetry gap")
                    rate = (status.spooled_messages - prior.spooled_messages) / elapsed
                    byte_rate = (status.spooled_bytes - prior.spooled_bytes) / elapsed
                    # No ingress during this interval: use configured design size for spool-only pressure.
                    size = byte_rate / rate if rate else self.config.capacity.message_size_hint
                    if size is None:
                        if status.spool_bytes:
                            raise ValueError(
                                "idle partition with backlog requires capacity.message_size_hint"
                            )
                        loads.append(PartitionLoad(shard, partition, owner.broker_id, 0, 0))
                        continue
                    cap = lookup(
                        self.model,
                        self.config.fleet.service_class,
                        size,
                        "guaranteed",
                        fanout=self.config.capacity.fanout,
                        scenario=self.config.capacity.scenario,
                    )
                    assert cap.ingress_byte_rate is not None and cap.egress_byte_rate is not None
                    loads.append(
                        PartitionLoad(
                            shard,
                            partition,
                            owner.broker_id,
                            rate / cap.msg_rate,
                            max(
                                byte_rate / cap.ingress_byte_rate,
                                # Native counters already include each group's stored copy.
                                byte_rate * (1 if self.config.messaging.enabled else
                                             self.config.capacity.fanout) / cap.egress_byte_rate,
                            ),
                            spool=status.spool_bytes / cap.spool_bytes,
                        )
                    )
        except Exception as exc:
            self.previous.clear()
            self.overloaded_since.clear()
            self.store.event(now, "snapshot-refused", {"error_type": type(exc).__name__})
            return ControlResult("no-decision", "complete valid partition telemetry unavailable", 0)
        self.previous = observations
        if time.monotonic() - scrape_started > self.config.metrics.staleness_limit:
            self.previous.clear()
            self.overloaded_since.clear()
            return ControlResult("no-decision", "partition scrape exceeded staleness limit", 0)
        if len(loads) != len(observations):
            return ControlResult("observing", "collecting counter-delta history", 0)
        totals: dict[str, list[float]] = defaultdict(lambda: [0.0, 0.0, 0.0])
        for load in loads:
            for i, value in enumerate((load.messages, load.bytes, load.spool)):
                totals[load.broker_id][i] += value
        broker_shards = {load.broker_id: load.shard for load in loads}
        hot = {
            broker for broker, vector in totals.items()
            if self.scaling_policy(broker_shards[broker]).enabled
            and max(vector) > self.scaling_policy(broker_shards[broker]).trigger_utilization
        }
        self.overloaded_since = {b: self.overloaded_since.get(b, now) for b in hot}
        sustained = {
            b for b in hot
            if now - self.overloaded_since[b] >= self.scaling_policy(broker_shards[b]).scale_up_window
        }
        if not sustained:
            return ControlResult("observing", "no sustained broker overload", 0)
        if self.store.started_since(now - 3600) >= self.config.automation.max_migrations_per_hour:
            return ControlResult("limited", "hourly migration limit reached", 0)
        for shard in sorted({load.shard for load in loads}):
            scaling = self.scaling_policy(shard)
            if not scaling.enabled:
                continue
            candidates = [
                b
                for b in self.assignments.brokers_for_shard(shard)
                if b.state in (BrokerState.ACTIVE, BrokerState.WARM)
            ]
            active = [b for b in candidates if b.state == BrokerState.ACTIVE]
            warm = [b for b in candidates if b.state == BrokerState.WARM]
            # Warm capacity can be activated only within the active per-shard broker ceiling.
            allowed = active + warm[: max(0, self.config.fleet.max_brokers - len(active))]
            shard_loads = [load for load in loads if load.shard == shard]
            excluded = {
                (load.shard, load.partition)
                for load in shard_loads if load.broker_id not in sustained
            }
            excluded |= self.store.recently_moved(now - scaling.cooldown)
            move = propose_move(
                shard_loads,
                [b.broker_id for b in allowed],
                trigger=scaling.trigger_utilization,
                target=scaling.target_utilization,
                excluded=excluded,
            )
            if move:
                partition_load, destination = move
                target = self.assignments.get_broker(destination)
                assert target is not None
                if target.state == BrokerState.WARM:
                    self.store.event(now, "activate-warm-service", {"broker": destination})
                    self.assignments.set_broker_state(destination, BrokerState.ACTIVE)
                self.store.begin(shard, partition_load.partition, partition_load.broker_id, destination, now)
                return ControlResult("migration-planned", "moving measured load to spare capacity", 1)
        self.store.event(now, "capacity-shortfall", {"brokers": sorted(sustained)})
        return ControlResult(
            "capacity-shortfall", "no fitting destination or partition; replenish warm capacity", 0
        )
