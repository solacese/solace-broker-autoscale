"""Command-line entry points for the controller and its local capacity tooling."""

from __future__ import annotations

import json
from pathlib import Path

import click

from . import __version__
from .capacity.cli import plan, profiles
from .controller.cli import adopt_feature_contract, run_controller


@click.group()
@click.version_option(__version__, prog_name="solace-autoscale")
def main() -> None:
    """Run the Solace managed-partition controller and local planning tools."""


main.add_command(profiles)
main.add_command(plan)
main.add_command(run_controller)
main.add_command(adopt_feature_contract)


@main.command()
@click.option("--config", "config_path", required=True, type=click.Path(exists=True, path_type=Path))
@click.option("--host", default="127.0.0.1", show_default=True)
@click.option("--port", default=8099, show_default=True, type=int)
def serve(config_path: Path, host: str, port: int) -> None:
    """Serve assignment and managed-client discovery APIs."""
    from .assignment.service import run_server

    run_server(str(config_path), host=host, port=port)


@main.command("explain")
@click.option("--config", "config_path", required=True, type=click.Path(exists=True, path_type=Path))
@click.option("--topic", default=None, help="Explain one concrete business topic without broker I/O.")
@click.option("--json", "as_json", is_flag=True, help="Print the effective machine-readable policy.")
def explain_policy(config_path: Path, topic: str | None, as_json: bool) -> None:
    """Validate and explain policy without connecting to or mutating a broker."""
    from .assignment.routing import partition_for
    from .assignment.topics import matches, validate_topic
    from .config import load_config
    from .controller.features import requirements_for

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
            "feature_requirements": {
                shard: requirements_for(cfg, shard).__dict__
                for shard in sorted(
                    {*cfg.automation.shards, *(route.shard for route in cfg.messaging.routes)}
                )
            },
            "messaging": cfg.messaging.model_dump(mode="json"),
            "note": "Current broker ownership requires the running assignment service.",
        }
        if topic is not None:
            validate_topic(topic)
            routes = [route for route in cfg.messaging.routes if matches(route.pattern, topic)]
            if len(routes) != 1:
                raise ValueError("topic must match exactly one route")
            route = routes[0]
            if route.key_evaluator is not None:
                report["topic"] = {
                    "value": topic,
                    "shard": route.shard,
                    "evaluator": route.key_evaluator.model_dump(mode="json"),
                    "partition": "computed by the registered customer evaluator",
                }
            else:
                levels = topic.split("/")
                if route.dispatch == "by-key":
                    key = json.dumps([levels[index] for index in route.key_levels], separators=(",", ":"))
                elif route.dispatch == "by-topic":
                    key = json.dumps(["topic", topic], separators=(",", ":"))
                else:
                    key = json.dumps(["route", route.pattern], separators=(",", ":"))
                report["topic"] = {
                    "value": topic,
                    "shard": route.shard,
                    "key": key,
                    "partition": partition_for(route.shard, key, cfg.assignment.partitions),
                }
        if as_json:
            click.echo(json.dumps(report, indent=2))
            return
        click.echo("Publishing is durably buffered locally and retried until broker acceptance.")
        click.echo("Subscribers must commit event-ID deduplication before their handler returns.")
        click.echo(f"Cloud creation: {'enabled' if cfg.provisioning.enabled else 'disabled'}")
        for route in cfg.messaging.routes:
            click.echo(f"{route.shard}: {route.pattern} ({route.dispatch})")
        if topic is not None:
            click.echo(f"Topic result: {report['topic']}")
    except (ValueError, OSError) as exc:
        raise click.ClickException(str(exc)) from exc


if __name__ == "__main__":
    main()
