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

import click

from . import __version__


@click.group()
@click.version_option(__version__, prog_name="solace-autoscale")
def main() -> None:
    """solace-autoscale — recommend and (optionally) scale a Solace PubSub+ Cloud broker fleet."""


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
    from .decision.types import ShardInput
    from .metrics.static import StaticCollector
    from .report import json as jr
    from .report import markdown as mr

    cfg = load_config(config_path)
    model = load_model(cfg.capacity.model)
    eval_now = now if now is not None else datetime.now(UTC).timestamp()

    static_path = metrics_path or (cfg.metrics.static_path if cfg.metrics.source == "static" else None)
    if static_path is None:
        click.echo(
            "recommend currently reads metrics from a static JSON window. Pass --metrics PATH or "
            "set metrics.source: static with metrics.static_path. Live SEMP collection is wired via "
            "the SempCollector but not yet exposed as a --metrics-source flag.",
            err=True,
        )
        sys.exit(2)

    collector = StaticCollector(static_path)
    decisions = []
    shard_names = list(collector._doc["shards"].keys())  # noqa: SLF001 (CLI plumbing)
    for name in shard_names:
        samples = collector.window(name)
        shard = ShardInput(shard_name=name, samples=samples)
        decisions.append(decide(DecisionRequest(config=cfg, model=model, shard=shard, now=eval_now)))

    if fmt == "json":
        click.echo(_json.dumps(jr.build_report(cfg, model, decisions), indent=2))
    else:
        click.echo(mr.render(cfg, model, decisions))


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


if __name__ == "__main__":
    main()
