"""Offline benchmark and sizing commands. No broker operations or credential access."""
from __future__ import annotations

import json
import math
from datetime import UTC, datetime
from pathlib import Path

import click

from .benchmarks import BenchmarkWorkbook, import_directory
from .model import load_model, lookup
from .profile_model import compile_profile


@click.group()
def profiles() -> None:
    """Import, inspect and compile measured Cloud performance profiles locally."""


@profiles.command("import")
@click.option("--directory", required=True, type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option("--out", default="resources/performance/catalog", type=click.Path(path_type=Path))
def import_profiles(directory: Path, out: Path) -> None:
    """Keep provider/version generations separate; index lab workbooks as reference-only."""
    try:
        index = import_directory(directory, out)
    except (ValueError, OSError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(json.dumps(index, indent=2))


@profiles.command("list")
@click.option("--directory", default="resources/performance/catalog", type=click.Path(path_type=Path))
def list_profiles(directory: Path) -> None:
    """Show available profile generations and their measured coverage."""
    if not (directory / "index.json").exists():
        raise click.ClickException("no local catalog; run profiles import --directory PATH first")
    index = json.loads((directory / "index.json").read_text())
    for item in index["profiles"]:
        click.echo(f"{item['provider']:5}  {item['broker_version']:12}  {item['tiers']} tiers  "
                   f"{item['observations']} paired points  {item['catalog']}")
    click.echo(f"{len(index['reference_only'])} reference-only files (see index.json)")


@profiles.command("inspect")
@click.argument("catalog", type=click.Path(exists=True, path_type=Path))
def inspect_profile(catalog: Path) -> None:
    """Report exact fanout/size coverage, including absent fanout rows."""
    try:
        book = BenchmarkWorkbook.model_validate_json(catalog.read_text())
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"{book.provider} / {book.broker_version} / {book.source_filename}")
    click.echo(f"SHA256 {book.source_sha256}")
    for sheet in book.sheets:
        click.echo(f"\n{sheet.service_class} ({sheet.metadata.get('instance type', 'instance unspecified')})")
        for scenario in sorted({p.scenario for p in sheet.observations}):
            points = [p for p in sheet.observations if p.scenario == scenario]
            click.echo(f"  {scenario}: fanouts={sorted({p.fanout for p in points})}, "
                       f"sizes={sorted({p.msg_size_bytes for p in points})}")
    for note in book.notes:
        click.echo(f"Note: {note}")


@profiles.command("compile")
@click.option("--catalog", required=True, type=click.Path(exists=True, path_type=Path))
@click.option("--service-classes", required=True, type=click.Path(exists=True, path_type=Path))
@click.option("--limits-source", required=True,
              help="Origin/date of HA VPN limits, or explicit planning assumption")
@click.option("--out", required=True, type=click.Path(path_type=Path))
def compile_command(catalog: Path, service_classes: Path, limits_source: str, out: Path) -> None:
    """Compile all measured dimensions with explicitly supplied HA service limits."""
    try:
        book = BenchmarkWorkbook.model_validate_json(catalog.read_text())
        limits = json.loads(service_classes.read_text())
        model = compile_profile(book, limits, compiled_at=datetime.now(UTC).isoformat(),
                                limits_source=limits_source)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(model.model_dump_json(indent=2, by_alias=True) + "\n")
    except (ValueError, KeyError, OSError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"wrote {out}  model_version={model.model_version}")


@click.command()
@click.option("--model", "model_path", required=True, type=click.Path(exists=True, path_type=Path))
@click.option("--service-class", required=True)
@click.option("--delivery", type=click.Choice(["direct", "guaranteed", "mixed"]), default="guaranteed")
@click.option("--scenario", type=click.Choice(["worst", "streaming", "unspooling", "replay", "tracing"]),
              default="worst")
@click.option("--message-size", type=click.FloatRange(min=1), required=True, help="Payload bytes")
@click.option("--fanout", type=click.FloatRange(min=1), default=1.0, show_default=True)
@click.option("--messages", type=click.FloatRange(min=0), required=True, help="Total ingress messages/second")
@click.option("--connections", type=click.IntRange(min=0), default=0)
@click.option("--spool-gib", type=click.FloatRange(min=0), default=0.0, help="Total retained bytes / 2^30")
@click.option("--utilization", type=click.FloatRange(min=0, max=1, min_open=True), default=0.75)
@click.option("--max-brokers", type=click.IntRange(min=1), default=8)
def plan(model_path: Path, service_class: str, delivery: str, scenario: str, message_size: float,
         fanout: float, messages: float, connections: int, spool_gib: float, utilization: float,
         max_brokers: int) -> None:
    """Size a hypothetical, evenly partitionable workload. Produces JSON; never actuates."""
    try:
        values = (message_size, fanout, messages, spool_gib, utilization)
        if not all(math.isfinite(v) for v in values):
            raise ValueError("workload values must be finite")
        model = load_model(model_path)
        cap = lookup(model, service_class, message_size, delivery, fanout=fanout, scenario=scenario)
        if model.schema_version != "2":
            raise ValueError("plan requires a measured schema-2 profile; use profiles compile first")
        assert cap.ingress_byte_rate is not None and cap.egress_byte_rate is not None
        ratios = {"messages": messages / cap.msg_rate,
                  "bytes": max(messages * message_size / cap.ingress_byte_rate,
                               messages * message_size * fanout / cap.egress_byte_rate),
                  "connections": connections / cap.connections,
                  "spool": spool_gib * 2**30 / cap.spool_bytes}
        by_axis = {a: max(1, math.ceil(r / utilization)) for a, r in ratios.items()}
    except (ValueError, KeyError, OSError) as exc:
        raise click.ClickException(str(exc)) from exc
    required = max(by_axis.values())
    click.echo(json.dumps({
        "kind": "capacity-plan", "model_version": model.model_version,
        "provider": model.provenance.platform, "broker_version": model.provenance.broker_version,
        "service_class": service_class, "scenario": scenario, "fanout": fanout,
        "message_size_bytes": message_size, "ingress_messages_per_second": messages,
        "target_utilization": utilization, "required_brokers": required,
        "within_configured_ceiling": required <= max_brokers, "max_brokers": max_brokers,
        "brokers_by_axis": by_axis, "estimated_between_points": cap.interpolated,
        "source_workbook": model.provenance.source_filename, "source_cells": cap.source_cells,
        "per_broker_ingress_messages_per_second": cap.msg_rate,
        "per_broker_ingress_bytes_per_second": cap.ingress_byte_rate,
        "per_broker_egress_bytes_per_second": cap.egress_byte_rate,
        "limits_source": model.provenance.limits_source,
        "assumptions": ["Even partitioning; existing guaranteed queues do not move automatically.",
                        "Measured SMF/CCSMP over TLS on HA; other protocols need workload validation.",
                        "Payload throughput, not network wire bandwidth. Planning estimate, not an SLA.",
                        "Replay plus tracing together is not measured. Standby nodes are not extra capacity.",
                        "Spool and connections use supplied limits; validate against deployed VPN settings."],
    }, indent=2))
