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

if TYPE_CHECKING:
    from .decision.types import ShardInput


@click.group()
@click.version_option(__version__, prog_name="solace-autoscale")
def main() -> None:
    """solace-autoscale - recommend and (optionally) scale a Solace PubSub+ Cloud broker fleet."""


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


if __name__ == "__main__":
    main()
