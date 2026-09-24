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
from ..capacity.model import CapacityPoint, lookup
from ..capacity.schema import CapacityModel
from ..config import Config, ShardScalingPolicy
from ..decision.types import MetricSample
from ..metrics.fleet import FleetInventory
from .features import assess_migration, feature_contract, placement_contract, requirements_for
from .migration import MigrationEngine
from .planner import (
    BrokerCandidate,
    LoadVector,
    PlacementBundle,
    PlacementConstraints,
    PlacementProblem,
    plan_next_move,
)
from .semp import QueueManager, QueueStatus
from .store import ControllerStore


@dataclass(frozen=True)
class ControlResult:
    state: str
    detail: str
    migrations: int
    explanation: dict[str, object] | None = None


@dataclass(frozen=True)
class EffectiveScalingPolicy:
    enabled: bool
    trigger_utilization: float
    target_utilization: float
    scale_up_window: float
    cooldown: float


def residual_broker_load(
    observed: LoadVector, attributed_lower: LoadVector, configured: LoadVector
) -> LoadVector:
    """Retain live broker pressure not safely attributable to movable partitions."""
    return LoadVector(*(
        max(configured_value, observed_value - attributed_value, 0.0)
        for configured_value, observed_value, attributed_value in zip(
            configured.values, observed.values, attributed_lower.values, strict=True
        )
    ))


def queue_copy_bounds(
    copy_rate: float,
    copy_byte_rate: float,
    group_patterns: dict[str, list[str]],
    fallback_fanout: float,
) -> tuple[float, float, float, float, bool]:
    """Bound original publications from queue copies without assuming all filters match."""
    if not group_patterns:
        factor, homogeneous = max(1.0, fallback_fanout), True
    else:
        factor = float(len(group_patterns))
        homogeneous = len({tuple(patterns) for patterns in group_patterns.values()}) == 1
    lower_rate = copy_rate / factor
    lower_bytes = copy_byte_rate / factor
    return (
        lower_rate,
        lower_rate if homogeneous else copy_rate,
        lower_bytes,
        lower_bytes if homogeneous else copy_byte_rate,
        homogeneous,
    )


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
        for shard, constraints in inventory.placement.shards.items():
            invalid = sorted(
                partition
                for partition in constraints.pinned_partitions
                if partition >= config.assignment.partitions
            )
            if invalid:
                raise ValueError(
                    f"{shard}: pinned partitions outside configured range: "
                    + ", ".join(map(str, invalid))
                )
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
        scaling_shards = {b.shard for b in inventory.brokers if b.role in ("active", "warm")}
        if config.messaging.enabled and {r.shard for r in config.messaging.routes} != scaling_shards:
            raise ValueError("native messaging workloads must exactly match scaling-capacity shards")
        self.config, self.model, self.inventory = config, model, inventory
        self.assignments, self.queues = assignments, queues
        self.store = ControllerStore(assignments)
        shards = sorted(scaling_shards)
        self.scaling_brokers = {
            b.broker_id: b for b in inventory.brokers if b.role in ("active", "warm")
        }
        inventory_ids = set(self.scaling_brokers)
        with assignments.transaction():
            assignments.ensure_feature_contract(
                feature_contract(config, shards, deployment_mode=inventory.deployment_mode)
            )
            assignments.ensure_routing(config.assignment.routing, config.assignment.partitions)
            assignments.ensure_managed_namespace(config.automation.fleet_id)
            referenced = {
                row["broker_id"] for row in assignments._conn.execute("SELECT broker_id FROM placements")
            }
            referenced.update(
                value for row in assignments._conn.execute(
                    "SELECT source,target FROM migrations WHERE phase NOT IN ('complete','rolled-back')"
                ) for value in row
            )
            cloud_ids = set()
            if config.provisioning.enabled and assignments._conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='cloud_capacity'"
            ).fetchone():
                cloud_ids = {
                    row["service_id"] for row in assignments._conn.execute(
                        "SELECT service_id FROM cloud_capacity WHERE service_id IS NOT NULL"
                    )
                }
            missing = referenced - inventory_ids - cloud_ids
            if missing:
                raise ValueError(
                    "owned or migrating brokers are absent from scaling inventory: "
                    + ", ".join(sorted(missing))
                )
            assignments.ensure_placement_contract(placement_contract(inventory))
        self.topics = TopicRegistry(assignments)
        self.topics.contract(config.messaging.routing_contract())
        if config.messaging.enabled:
            for group in config.messaging.subscriptions:
                self.topics.register(group.group, group.topics)
        if set(config.automation.shards) - scaling_shards:
            raise ValueError("scaling overrides must name scaling-capacity shards")
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
            if endpoint.role == "dr":
                continue
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
            if stored is not None and endpoint.role == "active" and stored.state == BrokerState.WARM:
                assignments.set_broker_state(endpoint.broker_id, BrokerState.ACTIVE)
                stored = assignments.get_broker(endpoint.broker_id)
            if stored is not None and endpoint.role == "warm" and stored.state not in (
                BrokerState.WARM, BrokerState.ACTIVE
            ):
                raise ValueError("inventoried warm broker has incompatible durable runtime state")
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

    def _eligible_broker_ids(self, shard: str) -> frozenset[str]:
        """Filter initial owners through the same static capability/domain constraints as moves."""
        placement = self.inventory.placement.shards.get(shard)
        required = placement.required_capabilities if placement else frozenset()
        allowed = placement.allowed_failure_domains if placement else {}
        broker_limit = placement.max_partitions_per_broker if placement else None
        domain_limits = placement.max_partitions_per_domain if placement else {}
        endpoints = [
            broker for broker in self.inventory.brokers
            if broker.shard == shard and broker.role == "active"
        ]
        counts = {
            broker.broker_id: sum(
                placement.mode == "guaranteed" and placement.client_id.startswith("partition:")
                for placement in self.assignments.placements_on_broker(broker.broker_id)
            )
            for broker in endpoints
        }
        domain_counts: dict[str, dict[str, int]] = {
            key: defaultdict(int) for key in domain_limits
        }
        for broker in endpoints:
            for key in domain_limits:
                value = broker.failure_domains.get(key)
                if value is not None:
                    domain_counts[key][value] += counts[broker.broker_id]
        return frozenset(
            broker.broker_id
            for broker in endpoints
            if required <= broker.capabilities
            and all(broker.failure_domains.get(key) in values for key, values in allowed.items())
            and (broker_limit is None or counts[broker.broker_id] < broker_limit)
            and all(
                broker.failure_domains.get(key) is not None
                and domain_counts[key][broker.failure_domains[key]] < limit
                for key, limit in domain_limits.items()
            )
        )

    def bootstrap(self, now: float) -> None:
        """Provision managed partition queues on their durable initial owners; never touch other names."""
        if Path(self.config.actuation.kill_switch_file).exists():
            return
        if self.config.messaging.enabled:
            for broker in self.inventory.brokers:
                if broker.role in ("active", "warm"):
                    self.queues.configure_native(broker.broker_id)
        groups = tuple(self.topics.groups())
        pending = {(m.shard, m.partition) for m in self.store.pending()}
        for shard in sorted({b.shard for b in self.inventory.brokers if b.role in ("active", "warm")}):
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
                    allowed_broker_ids=self._eligible_broker_ids(shard),
                )
                self.store.event(
                    now,
                    "bootstrap-queue-intent",
                    {"broker": placement.broker.broker_id, "shard": shard, "partition": partition},
                )
                self.queues.prepare(placement.broker.broker_id, shard, partition, enabled=True)
                self.store.mark_partition_ready(shard, partition, now)

        if self.config.messaging.enabled and not pending:
            with self.assignments.transaction():
                if self.topics.mark_ready(list(groups)):
                    self.store.signal(now, "subscription-ready")
            self._prepared_groups = groups

    def _copy_factor(self, shard: str) -> tuple[int, bool]:
        """Return maximum queue copies and whether every group has the same filter contract."""
        if not self.config.messaging.enabled:
            return 1, True
        groups = self.topics.groups(shard)
        if not groups:
            return 1, True
        contracts = {tuple(patterns) for patterns in groups.values()}
        return len(groups), len(contracts) == 1

    @staticmethod
    def _broker_load(sample: MetricSample, cap: CapacityPoint) -> LoadVector:
        ingress_bytes = cap.ingress_byte_rate
        egress_bytes = cap.egress_byte_rate
        assert ingress_bytes is not None and egress_bytes is not None
        return LoadVector(
            sample.ingress_msg_rate / cap.msg_rate,
            max(sample.ingress_byte_rate / ingress_bytes, sample.egress_byte_rate / egress_bytes),
            sample.connection_count / cap.connections,
            sample.spool_used / cap.spool_bytes,
        )

    def tick(self, now: float) -> ControlResult:
        """Advance durable work first; plan from complete fresh deltas only when no move is pending."""
        refresh = getattr(self.queues, "refresh_requested", None)
        if refresh is not None:
            refresh.clear()
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
        loads: list[PlacementBundle] = []
        scrape_started = time.monotonic()
        observations: dict[tuple[str, int], tuple[float, QueueStatus, str]] = {}
        broker_samples: dict[str, MetricSample] = {}
        attribution: dict[str, object] = {"method": "queue-copy-bounds-plus-broker-totals"}
        try:
            owners = {}
            for shard in sorted({b.shard for b in self.inventory.brokers if b.role in ("active", "warm")}):
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
                broker_method = getattr(self.queues, "broker_metrics", None)
                broker_futures = {
                    broker_id: pool.submit(broker_method, broker_id, endpoint.shard, now)
                    for broker_id, endpoint in self.scaling_brokers.items()
                    if broker_method is not None
                }
                for broker_id, future in broker_futures.items():
                    broker_samples[broker_id] = future.result()
                for key, future in futures.items():
                    shard, partition = key
                    owner = owners[key]
                    status = future.result()
                    if not status.fully_enabled:
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
                    copy_rate = (status.spooled_messages - prior.spooled_messages) / elapsed
                    copy_byte_rate = (status.spooled_bytes - prior.spooled_bytes) / elapsed
                    groups = self.topics.groups(shard) if self.config.messaging.enabled else {}
                    (
                        lower_rate,
                        upper_rate,
                        lower_byte_rate,
                        upper_byte_rate,
                        homogeneous,
                    ) = queue_copy_bounds(
                        copy_rate, copy_byte_rate, groups, self.config.capacity.fanout
                    )
                    # No ingress during this interval: use configured design size for spool-only pressure.
                    size = (
                        upper_byte_rate / upper_rate
                        if upper_rate else self.config.capacity.message_size_hint
                    )
                    if size is None:
                        if status.spool_bytes:
                            raise ValueError(
                                "idle partition with backlog requires capacity.message_size_hint"
                            )
                        loads.append(
                            PlacementBundle(
                                f"{shard}/partition:{partition}",
                                shard,
                                partition,
                                owner.broker_id,
                                LoadVector(),
                                resident=LoadVector(spool=status.spool_bytes),
                                queue_ids=tuple(sorted(self.topics.groups(shard))) or ("default",),
                            )
                        )
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
                        PlacementBundle(
                            f"{shard}/partition:{partition}",
                            shard,
                            partition,
                            owner.broker_id,
                            LoadVector(
                                upper_rate / cap.msg_rate,
                                max(
                                    upper_byte_rate / cap.ingress_byte_rate,
                                    copy_byte_rate / cap.egress_byte_rate,
                                ),
                            ),
                            resident=LoadVector(spool=status.spool_bytes / cap.spool_bytes),
                            relief=LoadVector(
                                lower_rate / cap.msg_rate,
                                max(
                                    lower_byte_rate / cap.ingress_byte_rate,
                                    copy_byte_rate / cap.egress_byte_rate,
                                ),
                            ),
                            queue_ids=tuple(sorted(self.topics.groups(shard))) or ("default",),
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
        attributed_lower: dict[str, LoadVector] = defaultdict(LoadVector)
        for load in loads:
            attributed_lower[load.current_broker] = attributed_lower[load.current_broker].add(
                (load.relief or load.movable).add(load.resident)
            )
        runtime_fixed: dict[str, LoadVector] = {}
        try:
            for broker_id, endpoint in self.scaling_brokers.items():
                configured = LoadVector(**endpoint.fixed_load.model_dump())
                sample = broker_samples.get(broker_id)
                if sample is None:
                    runtime_fixed[broker_id] = configured
                    continue
                size = sample.avg_msg_size or self.config.capacity.message_size_hint
                if not size:
                    if any((sample.ingress_msg_rate, sample.egress_msg_rate, sample.spool_used)):
                        raise ValueError("broker traffic requires a measured or configured message size")
                    size = 1
                cap = lookup(
                    self.model,
                    self.config.fleet.service_class,
                    size,
                    "guaranteed",
                    fanout=self.config.capacity.fanout,
                    scenario=self.config.capacity.scenario,
                )
                observed = self._broker_load(sample, cap)
                runtime_fixed[broker_id] = residual_broker_load(
                    observed, attributed_lower[broker_id], configured
                )
        except Exception as exc:
            self.previous.clear()
            self.overloaded_since.clear()
            self.store.event(now, "snapshot-refused", {"error_type": type(exc).__name__})
            return ControlResult("no-decision", "complete valid broker telemetry unavailable", 0)
        attribution["broker_totals"] = bool(broker_samples)
        attribution["ambiguous_filter_shards"] = sorted({
            load.shard for load in loads if not self._copy_factor(load.shard)[1]
        })
        totals: dict[str, list[float]] = defaultdict(lambda: [0.0, 0.0, 0.0, 0.0])
        for broker_id in self.scaling_brokers:
            totals[broker_id] = list(runtime_fixed[broker_id].values)
        for load in loads:
            for i, value in enumerate(load.total.values):
                totals[load.current_broker][i] += value
        broker_shards = {
            broker_id: endpoint.shard for broker_id, endpoint in self.scaling_brokers.items()
        }
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
        pinned: dict[str, str] = {}
        proposals = []
        for shard in sorted({load.shard for load in loads}):
            scaling = self.scaling_policy(shard)
            shard_loads = [load for load in loads if load.shard == shard]
            if not scaling.enabled or not any(load.current_broker in sustained for load in shard_loads):
                continue
            requirements = requirements_for(self.config, shard)
            eligibility = assess_migration(
                requirements,
                deployment_mode=self.inventory.deployment_mode,
                target_role="active",
            )
            if not eligibility.allowed:
                pinned[shard] = eligibility.reason
                continue
            stored = {
                broker.broker_id: broker for broker in self.assignments.brokers_for_shard(shard)
            }
            candidates = []
            for broker_id in sorted(self.scaling_brokers):
                endpoint = self.scaling_brokers[broker_id]
                broker = stored.get(broker_id)
                if endpoint.shard != shard or endpoint.role not in ("active", "warm") or broker is None:
                    continue
                if not assess_migration(
                    requirements,
                    deployment_mode=self.inventory.deployment_mode,
                    target_role=endpoint.role,
                ).allowed:
                    continue
                candidates.append(
                    BrokerCandidate(
                        broker_id,
                        shard,
                        endpoint.role,
                        broker.state.value,
                        endpoint.capabilities,
                        tuple(sorted(endpoint.failure_domains.items())),
                        runtime_fixed[broker_id],
                    )
                )
            placement = self.inventory.placement.shards.get(shard)
            required = placement.required_capabilities if placement else frozenset()
            allowed_domains = tuple(
                sorted((key, tuple(sorted(values))) for key, values in (
                    placement.allowed_failure_domains.items() if placement else ()
                ))
            )
            domain_limits = tuple(sorted(
                placement.max_partitions_per_domain.items() if placement else ()
            ))
            pinned_partitions = placement.pinned_partitions if placement else frozenset()
            excluded = {
                load.bundle_id for load in shard_loads if load.current_broker not in sustained
            }
            excluded.update(
                f"{name}/partition:{partition}"
                for name, partition in self.store.recently_moved(now - scaling.cooldown)
            )
            excluded.update(f"{shard}/partition:{partition}" for partition in pinned_partitions)
            problem = PlacementProblem(
                shard,
                tuple(sorted(shard_loads, key=lambda load: load.bundle_id)),
                tuple(candidates),
                PlacementConstraints(
                    scaling.trigger_utilization,
                    scaling.target_utilization,
                    self.config.fleet.max_brokers,
                    required,
                    allowed_domains,
                    placement.max_partitions_per_broker if placement else None,
                    domain_limits,
                    placement.spread_by if placement else (),
                ),
                frozenset(excluded),
            )
            outcome = plan_next_move(problem)
            if outcome.proposal:
                severity = max(
                    max(totals[load.current_broker])
                    for load in shard_loads if load.current_broker in sustained
                )
                proposals.append((-severity, outcome.proposal, requirements))
        if proposals:
            _, proposal, requirements = min(
                proposals,
                key=lambda item: (
                    item[0], item[1].after, item[1].shard,
                    item[1].partition, item[1].destination,
                ),
            )
            target = self.assignments.get_broker(proposal.destination)
            assert target is not None
            endpoint = self.scaling_brokers[proposal.destination]
            decision = assess_migration(
                requirements,
                deployment_mode=self.inventory.deployment_mode,
                target_role=endpoint.role,
            )
            if not decision.allowed:
                raise ValueError(f"migration compatibility changed: {decision.reason}")
            explanation = {
                "bundle": proposal.bundle_id,
                "telemetry_attribution": attribution,
                "source": proposal.source,
                "destination": proposal.destination,
                "queue_count": proposal.after.moved_queue_count,
                "before": proposal.before.__dict__,
                "after": proposal.after.__dict__,
                "rejections": dict(proposal.rejection_counts),
            }
            self.store.begin(
                proposal.shard,
                proposal.partition,
                proposal.source,
                proposal.destination,
                now,
                activate_warm=target.state == BrokerState.WARM,
                explanation=explanation,
            )
            return ControlResult(
                "migration-planned", "moving measured load to spare capacity", 1, explanation
            )
        if pinned:
            self.store.event(now, "feature-pinned", {"shards": pinned})
            return ControlResult(
                "feature-pinned",
                "migration refused: " + ", ".join(
                    f"{shard} ({reason})" for shard, reason in sorted(pinned.items())
                ),
                0,
                {"pinned_shards": dict(sorted(pinned.items()))},
            )
        self.store.event(now, "capacity-shortfall", {"brokers": sorted(sustained)})
        return ControlResult(
            "capacity-shortfall", "no fitting destination or partition; replenish warm capacity", 0
        )
