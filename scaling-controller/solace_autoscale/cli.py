"""Command-line interface.

Subcommands:
  compile       xlsx -> versioned JSON capacity model (§6). Never run at decision time.
  recommend     run the decision engine over a config + metrics, emit markdown/JSON report (§11).
  simulate      run the simulator matrix and print validation (§13).
  accuracy      report predicted vs actual (§7, Phase 2).
  shard-advise  propose shard boundaries from an Event Portal export (§8).
  serve         run the assignment service (§9, Phase 3).

The decision engine is pure; the CLI is where I/O (loading model, collecting metrics, writing
files) lives.
"""

from __future__ import annotations

import json as _json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import click

from . import __version__
from .capacity.cli import plan, profiles
from .controller.cli import run_controller
from .metrics.fleet_cli import monitor_fleet

if TYPE_CHECKING:
    from .decision.types import ShardInput


@click.group()
@click.version_option(__version__, prog_name="solace-autoscale")
def main() -> None:
    """solace-autoscale - recommend and (optionally) scale a Solace Cloud broker fleet."""


main.add_command(profiles)
main.add_command(plan)
main.add_command(monitor_fleet)
main.add_command(run_controller)


@main.command()
@click.option("--workbook", required=True, type=click.Path(exists=True), help="performance.xlsx")
@click.option("--service-classes", required=True, type=click.Path(exists=True),
              help="service-classes.json with connections + spool per class")
@click.option("--out", required=True, type=click.Path(), help="output model JSON path")
@click.option("--platform", default=None, help="override platform label")
def compile(workbook: str, service_classes: str, out: str, platform: str | None) -> None:
    """Compile a performance workbook into a versioned capacity model."""
    from .capacity.compile import CompileError, compile_workbook

    compiled_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        res = compile_workbook(workbook, service_classes, compiled_at=compiled_at,
                               platform_hint=platform)
    except CompileError as e:
        click.echo(f"compile failed: {e}", err=True)
        sys.exit(2)
    Path(out).write_text(res.model.model_dump_json(indent=2, by_alias=True) + "\n")
    click.echo(f"wrote {out}  model_version={res.model.model_version}")
    if res.notes:
        click.echo(f"  {len(res.notes)} provenance note(s)")


def _load_static_shards(static_path: str) -> list[ShardInput]:
    """Build ShardInput per shard from a static metrics file, carrying subscribing_brokers and
    key_subdividable so mesh amplification (§5.3) and hot-shard (§5.6) actually fire from the CLI."""
    from .decision.types import ShardInput
    from .metrics.static import StaticCollector

    collector = StaticCollector(static_path)
    shards = []
    for name in collector.shard_names():
        shards.append(ShardInput(
            shard_name=name,
            samples=collector.window(name),
            subscribing_brokers=collector.subscribing_brokers(name),
            key_subdividable=collector.key_subdividable(name),
        ))
    return shards


@main.command()
@click.option("--config", "config_path", required=True, type=click.Path(exists=True))
@click.option("--metrics", "metrics_path", default=None, type=click.Path(exists=True),
              help="static metrics JSON (overrides config metrics.source for this run)")
@click.option("--format", "fmt", type=click.Choice(["markdown", "json"]), default="markdown")
@click.option("--now", type=float, default=None, help="epoch seconds to evaluate at (default: real clock)")
def recommend(config_path: str, metrics_path: str | None, fmt: str, now: float | None) -> None:
    """Run the decision engine and print a report."""
    from .capacity.model import load_model
    from .config import load_config
    from .decision.engine import DecisionRequest, decide
    from .report import json as jr
    from .report import markdown as mr

    cfg = load_config(config_path)
    model = load_model(cfg.capacity.model)
    eval_now = now if now is not None else datetime.now(UTC).timestamp()

    static_path = metrics_path or (cfg.metrics.static_path if cfg.metrics.source == "static" else None)
    if static_path is None:
        click.echo(
            "recommend reads a static metrics window here. Pass --metrics PATH or set "
            "metrics.source: static with metrics.static_path. For live collection use "
            "'solace-autoscale monitor' (SEMP).",
            err=True,
        )
        sys.exit(2)

    shards = _load_static_shards(static_path)
    decisions = [decide(DecisionRequest(config=cfg, model=model, shard=s, now=eval_now))
                 for s in shards]

    if fmt == "json":
        click.echo(_json.dumps(jr.build_report(cfg, model, decisions), indent=2))
    else:
        click.echo(mr.render(cfg, model, decisions))


@main.command()
@click.option("--config", "config_path", required=True, type=click.Path(exists=True))
@click.option("--metrics", "metrics_path", required=True, type=click.Path(exists=True))
@click.option("--multipliers", default="1,2,4", help="comma-separated load multipliers")
@click.option("--now", type=float, default=None)
def whatif(config_path: str, metrics_path: str, multipliers: str, now: float | None) -> None:
    """Project required brokers per shard under load multipliers (§ what-if)."""
    from .capacity.model import load_model
    from .config import load_config
    from .simulator.what_if import project

    cfg = load_config(config_path)
    model = load_model(cfg.capacity.model)
    eval_now = now if now is not None else datetime.now(UTC).timestamp()
    mults = tuple(float(x) for x in multipliers.split(","))

    shards = _load_static_shards(metrics_path)
    click.echo(f"# What-if projection (model {model.model_version})\n")
    for shard in shards:
        projs = project(cfg, model, shard, eval_now, multipliers=mults)
        click.echo(f"## Shard `{shard.shard_name}`")
        click.echo("| Load × | Rec. brokers | Binding | Action | Ceiling hit |")
        click.echo("|---|---|---|---|---|")
        for p in projs:
            ceil = "⚠️ yes" if p.hit_ceiling else "no"
            click.echo(f"| {p.multiplier:g}× | {p.recommended_brokers} | {p.binding_axis} | "
                       f"{p.action} | {ceil} |")
        click.echo("")


@main.command()
@click.option("--config", "config_path", required=True, type=click.Path(exists=True))
def simulate(config_path: str) -> None:
    """Run the simulator matrix and report model validation (§13)."""
    from .capacity.model import load_model
    from .config import load_config
    from .simulator.workload import validate_model

    cfg = load_config(config_path)
    model = load_model(cfg.capacity.model)
    report = validate_model(cfg, model)
    click.echo(f"validated {report.total_cells} matrix cells against model {model.model_version}")
    if report.ok:
        click.echo("OK: all invariants hold")
    else:
        click.echo(f"FAILURES ({len(report.failures)}):", err=True)
        for f in report.failures[:50]:
            click.echo(f"  - {f}", err=True)
        sys.exit(1)


@main.command()
@click.option("--config", "config_path", required=True, type=click.Path(exists=True))
@click.option("--by", type=click.Choice(["axis", "bucket"]), default="axis")
def accuracy(config_path: str, by: str) -> None:
    """Report predicted vs actual capacity error (§7)."""
    from .accuracy.recorder import AccuracyRecorder
    from .accuracy.report import format_accuracy_report
    from .config import load_config

    cfg = load_config(config_path)
    rec = AccuracyRecorder(cfg.accuracy.store)
    click.echo(format_accuracy_report(rec, group_by=by))


@main.command(name="shard-advise")
@click.option("--export", "export_path", required=True, type=click.Path(exists=True),
              help="Event Portal export JSON")
@click.option("--shard-key", default="{domain}")
def shard_advise(export_path: str, shard_key: str) -> None:
    """Propose shard boundaries from an Event Portal export (§8)."""
    from .portal.shard_advisor import advise_from_file, render_config_fragment

    result = advise_from_file(export_path)
    click.echo(render_config_fragment(result))
    click.echo("", err=True)
    click.echo(f"# {len(result.components)} component(s); "
               f"{len(result.spanning_applications)} spanning application(s) → "
               f"{'hybrid recommended' if result.spanning_applications else 'sharded OK'}", err=True)


@main.command()
@click.option("--config", "config_path", required=True, type=click.Path(exists=True))
@click.option("--host", default="127.0.0.1")
@click.option("--port", default=8099, type=int)
def serve(config_path: str, host: str, port: int) -> None:
    """Run the assignment service HTTP API (§9)."""
    from .assignment.service import run_server

    run_server(config_path, host=host, port=port)


@main.command()
@click.option("--config", "config_path", required=True, type=click.Path(exists=True))
@click.option("--broker-url", default=None, help="SEMP base URL (metrics.source: semp)")
@click.option("--user", default="admin")
@click.option("--password", default="admin")
@click.option("--vpn", default="default", help="Message VPN to monitor")
@click.option("--shard", "shard_name", default="default", help="shard name for this VPN")
@click.option("--current-brokers", default=1, type=int, help="brokers currently serving the shard")
@click.option("--once", is_flag=True, help="scrape once and exit (for testing / cron)")
@click.option("--iterations", default=0, type=int, help="stop after N ticks (0 = run forever)")
@click.option("--insecure", is_flag=True,
              help="disable TLS certificate verification for the SEMP connection (DANGEROUS; "
                   "only for a broker with a self-signed cert you trust)")
def monitor(config_path: str, broker_url: str | None, user: str, password: str, vpn: str,
            shard_name: str, current_brokers: int, once: bool, iterations: int,
            insecure: bool) -> None:
    """Continuously scrape SEMP, accumulate a rolling window, decide, and record accuracy (§7).

    Unlike one-shot `recommend`, this accrues real history over time so derived headroom (§5.7) and
    the evaluation window (§5.8) have data. Emits a one-line status each tick and an alert when the
    action changes.
    """
    import time as _time

    from .accuracy.join import record_observed_capacity
    from .accuracy.recorder import AccuracyRecorder
    from .capacity.model import load_model
    from .config import load_config
    from .decision.engine import DecisionRequest, decide
    from .decision.types import ShardInput
    from .metrics.history import RollingHistory
    from .metrics.semp import SempCollector

    cfg = load_config(config_path)
    model = load_model(cfg.capacity.model)
    if broker_url is None:
        broker_url = cfg.metrics.endpoint
    if broker_url is None:
        click.echo("monitor needs a SEMP base URL: pass --broker-url or set metrics.endpoint", err=True)
        sys.exit(2)

    # window retention: cover the scale-down window plus margin so hysteresis has history.
    retention = max(cfg.policy.scale_down_window, 3600.0) + cfg.metrics.scrape_interval
    history = RollingHistory(retention_seconds=retention)
    recorder = AccuracyRecorder(cfg.accuracy.store) if cfg.accuracy.record else None
    if insecure:
        host = urlparse(broker_url).hostname or broker_url
        click.echo(
            f"WARNING: TLS certificate verification DISABLED for SEMP connection to {host!r}; "
            "traffic is exposed to man-in-the-middle attacks. Use only with a broker whose "
            "self-signed certificate you trust.",
            err=True,
        )
    collector = SempCollector(broker_url, user, password, verify=not insecure)

    last_action: str | None = None
    tick = 0
    try:
        while True:
            tick += 1
            now = datetime.now(UTC).timestamp()
            try:
                sample = collector.collect(shard_name, vpn, now, current_brokers)
            except Exception as e:  # network hiccup: log and continue, never crash the loop
                click.echo(f"[tick {tick}] scrape failed: {e}", err=True)
            else:
                history.add(shard_name, sample)
                shard = ShardInput(shard_name=shard_name, samples=history.window(shard_name))
                d = decide(DecisionRequest(config=cfg, model=model, shard=shard, now=now))
                if recorder is not None:
                    recorder.record_recommendation(d, cfg.config_hash(), ts=now)
                    record_observed_capacity(recorder, model, cfg.fleet.service_class,
                                             cfg.workload.delivery, shard_name, sample, ts=now)
                axis = d.binding_axis.value if d.binding_axis else "-"
                click.echo(f"[tick {tick}] {shard_name}: {d.action.value} "
                           f"cur={d.current_brokers} rec={d.recommended_brokers} binding={axis} "
                           f"samples={len(shard.samples)}")
                if d.action.value != last_action and last_action is not None:
                    click.echo(f"  ALERT: action changed {last_action} → {d.action.value}")
                last_action = d.action.value
            if once or (iterations and tick >= iterations):
                break
            _time.sleep(cfg.metrics.scrape_interval)
    finally:
        collector.close()
        if recorder is not None:
            recorder.close()


@main.command(name="dispatch-test")
@click.option("--config", "config_path", required=True, type=click.Path(exists=True))
@click.option("--topic", required=True, help="message topic to route")
@click.option("--payload", default="", help="message payload (JSON or raw)")
@click.option("--payload-file", type=click.Path(exists=True), default=None,
              help="read payload from a file instead of --payload")
def dispatch_test(config_path: str, topic: str, payload: str, payload_file: str | None) -> None:
    """Show how the smart SHIM routes one message: which rule fires, broker, key, address.

    Pure and offline — no broker needed. Use it to author and verify dispatch rules before wiring
    them into the publisher/listener SHIM.
    """
    from .config import load_config
    from .dispatch import DispatchPlan, rules_from_config

    cfg = load_config(config_path)
    if not cfg.dispatch.enabled:
        click.echo("dispatch is disabled (set dispatch.enabled: true)", err=True)
    body = Path(payload_file).read_bytes() if payload_file else payload.encode()
    plan = DispatchPlan(
        rules=rules_from_config([r.model_dump() for r in cfg.dispatch.rules]),
        default_broker=cfg.dispatch.default_broker,
    )
    d = plan.decide(topic, body)
    click.echo(f"topic    : {topic}")
    click.echo(f"rule     : {d.rule}  ({'matched' if d.matched else 'default fallback'})")
    click.echo(f"broker   : {d.broker}")
    click.echo(f"address  : {d.address}")
    click.echo(f"key      : {d.key if d.key is not None else '(none)'}")
    click.echo(f"targets  : {', '.join(plan.targets)}  <- listener SHIM subscribes to all of these")


@main.command()
@click.option("--config", "config_path", required=True, type=click.Path(exists=True))
@click.option("--source", "source_url", required=True, help="SEMP base URL of the source-of-truth broker")
@click.option("--target", "target_urls", multiple=True, required=True,
              help="SEMP base URL of a target broker (repeatable)")
@click.option("--user", default="admin")
@click.option("--password", default="admin")
@click.option("--vpn", default="default")
@click.option("--apply", "do_apply", is_flag=True, help="actually apply (default: dry-run plan only)")
def configsync(config_path: str, source_url: str, target_urls: tuple[str, ...], user: str,
               password: str, vpn: str, do_apply: bool) -> None:
    """Replicate broker config from a source-of-truth broker to the fleet (dry-run by default).

    Re-runnable: an edit made on the source shows up as ops here until the targets converge. Honours
    ``configsync.mode`` (additive|mirror) and the object kinds in ``configsync.objects``.
    """
    from .config import load_config
    from .configsync.semp_config import SempConfigClient, reconcile

    cfg = load_config(config_path)
    kinds = cfg.configsync.objects
    src = SempConfigClient("source", source_url, user, password, verify=False)
    tgts = [SempConfigClient(f"target-{i}", u, user, password, verify=False)
            for i, u in enumerate(target_urls)]
    try:
        plan = reconcile(src, tgts, vpn, kinds=kinds, mode=cfg.configsync.mode, dry_run=not do_apply)
    finally:
        src.close()
        for t in tgts:
            t.close()

    verb = "APPLIED" if plan.applied else "PLAN (dry-run)"
    click.echo(f"# config replication {verb} — source={plan.source_broker} mode={cfg.configsync.mode}")
    for target, counts in plan.summary().items():
        ops = plan.ops_by_target[target]
        click.echo(f"\n## {target}: {counts['create']} create, {counts['update']} update, "
                   f"{counts['delete']} delete")
        for op in ops:
            click.echo(f"  {op.describe()}")
    if plan.total_ops() == 0:
        click.echo("\nfleet already converged — nothing to do")
    elif not plan.applied:
        click.echo("\nre-run with --apply to replicate (dry-run made no changes)")



@main.command("explain")
@click.option("--config", "config_path", required=True, type=click.Path(exists=True))
@click.option("--topic", default=None, help="Explain one concrete business topic without contacting brokers.")
@click.option("--json", "as_json", is_flag=True, help="Print the expanded machine-readable policy.")
def explain_policy(config_path: str, topic: str | None, as_json: bool) -> None:
    """Validate and explain the effective policy. Never connects to or mutates a broker."""
    from .assignment.routing import partition_for
    from .assignment.topics import matches, validate_topic
    from .config import load_config
    try:
        cfg = load_config(config_path)
        report: dict = {
            "config_hash": cfg.config_hash(),
            "broker_limit_per_shard": cfg.fleet.max_brokers,
            "warm_brokers_per_shard": cfg.policy.warm_pool,
            "cloud_creation": cfg.provisioning.enabled,
            "delivery": "guaranteed, asynchronous broker receipts, at least once",
            "partitions_per_shard": cfg.assignment.partitions,
            "migration": cfg.automation.model_dump(mode="json"),
            "messaging": cfg.messaging.model_dump(mode="json"),
            "note": "Current broker ownership requires the running assignment service.",
        }
        if topic is not None:
            validate_topic(topic)
            routes = [r for r in cfg.messaging.routes if matches(r.pattern, topic)]
            if len(routes) != 1:
                raise ValueError("topic must match exactly one route")
            route = routes[0]
            if route.dispatch == "by-key":
                key = _json.dumps([topic.split("/")[i] for i in route.key_levels], separators=(",", ":"))
            else:
                key = _json.dumps(["topic", topic] if route.dispatch == "by-topic"
                                  else ["route", route.pattern], separators=(",", ":"))
            report["topic"] = {"value": topic, "shard": route.shard, "key": key,
                               "partition": partition_for(route.shard, key, cfg.assignment.partitions)}
        if as_json:
            click.echo(_json.dumps(report, indent=2))
        else:
            click.echo("Publishing: save locally, deliver asynchronously, retry until broker acceptance.")
            click.echo("Subscribers process independently. Handlers must deduplicate event IDs.")
            click.echo(f"Capacity per workload: up to {cfg.fleet.max_brokers} brokers, "
                       f"including {cfg.policy.warm_pool} warm spare(s).")
            click.echo("Cloud service creation: " + ("enabled" if cfg.provisioning.enabled else "disabled"))
            for route in cfg.messaging.routes:
                keep = ("topic levels " + ", ".join(str(i + 1) for i in route.key_levels)
                        if route.dispatch == "by-key" else
                        "each complete topic" if route.dispatch == "by-topic" else "the whole workload")
                enabled = cfg.automation.shards.get(route.shard)
                mode = "automatic" if enabled is None or enabled.enabled else "paused"
                click.echo(f"{route.shard}: {route.pattern}; keep {keep} together; scaling {mode}.")
            for group in cfg.messaging.subscriptions:
                click.echo(f"Subscriber {group.group}: " + ", ".join(group.topics))
            if topic is not None:
                click.echo(f"This topic maps to partition {report['topic']['partition']} "
                           f"in {report['topic']['shard']}.")
            click.echo("Current broker ownership requires the running assignment service.")
            click.echo("Read-only check complete. Use --json for the expanded settings.")
    except (ValueError, OSError) as exc:
        raise click.ClickException(str(exc)) from exc


if __name__ == "__main__":
    main()
