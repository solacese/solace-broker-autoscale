"""Markdown report (§11): for putting in front of an architect.

Leads with the recommendation, then the evidence. Per shard: current vs recommended brokers,
binding axis, all four ratios against effective thresholds, derived vs configured headroom with the
inputs that produced it, warnings, model version + provenance, warm-pool cost. Derived headroom is
prominent, not a footnote (§5.7).
"""

from __future__ import annotations

from ..capacity.schema import CapacityModel
from ..config import Config
from ..decision.types import ShardDecision
from .cost import fleet_cost_summary

_AXIS_ORDER = ("messages", "bytes", "connections", "spool")


def _render_cost(config: Config, decisions: list[ShardDecision]) -> list[str]:
    summary = fleet_cost_summary(config, decisions)
    out: list[str] = ["## Cost"]
    cur = config.billing.currency
    if not summary["priced"]:
        out.append(
            "- No price table configured (`billing.per_broker_monthly`), so only broker counts are "
            "shown. Add your own rates to see monthly cost and deltas."
        )
        total_cur = sum(d.current_brokers for d in decisions)
        total_rec = sum(d.recommended_brokers for d in decisions)
        out.append(f"- Fleet brokers: **{total_cur} → {total_rec}** across {len(decisions)} shard(s).")
        out.append("")
        return out

    out.append(f"| Shard | Brokers (cur→rec) | Monthly {cur} (cur→rec) | Δ / month |")
    out.append("|---|---|---|---|")
    for c in summary["per_shard"]:
        out.append(
            f"| {c['shard']} | {c['current_brokers']}→{c['recommended_brokers']} | "
            f"{c['current_monthly']:.0f}→{c['recommended_monthly']:.0f} | "
            f"{c['delta_monthly']:+.0f} |"
        )
    out.append(
        f"| **fleet** | | **{summary['total_current_monthly']:.0f}→"
        f"{summary['total_recommended_monthly']:.0f}** | "
        f"**{summary['total_delta_monthly']:+.0f}** |"
    )
    if summary["warm_pool_monthly"] is not None:
        out.append("")
        note = summary["warm_pool_note"] or "billed idle capacity"
        out.append(f"- **Warm pool:** {summary['warm_pool_monthly']:.0f} {cur}/month — {note}.")
    out.append("")
    return out


def render(config: Config, model: CapacityModel, decisions: list[ShardDecision]) -> str:
    lines: list[str] = []
    lines.append("# solace-autoscale recommendation")
    lines.append("")

    if model.synthetic:
        lines.append("> ⚠️ **SYNTHETIC CAPACITY MODEL** — "
                     f"{model.warning}")
        lines.append("> Recommendations below are illustrative only. Actuation is hard-blocked.")
        lines.append("")

    lines.append(f"- **Model version:** `{model.model_version}`")
    lines.append(f"- **Config hash:** `{config.config_hash()}`")
    lines.append(f"- **Billing model:** {config.billing.model}")
    lines.append(f"- **Topology:** {config.topology.mode}")
    if config.billing.model == "committed":
        lines.append("- **Note:** committed billing — scale-down recommendations are suppressed; "
                     "a warm pool is billed idle capacity with no offsetting saving.")
    wp = config.policy.warm_pool
    if wp > 0:
        lines.append(f"- **Warm pool:** {wp} pre-provisioned broker(s), billed as idle capacity.")
    lines.append("")

    for d in decisions:
        lines.extend(_render_shard(d))
        lines.append("")

    lines.extend(_render_cost(config, decisions))

    lines.append("---")
    prov = model.provenance
    lines.append("## Provenance")
    lines.append(f"- Source: `{prov.source_filename}` (sha256 `{prov.source_sha256[:16]}…`)")
    lines.append(f"- Compiled at: {prov.compiled_at}, compiler v{prov.compiler_version}, "
                 f"{prov.row_count} rows")
    lines.append(f"- Measured message-size range: {prov.measured_range.msg_size_bytes} bytes")
    lines.append("")
    lines.append("_Community project. Not a supported Solace product. No warranty._")
    return "\n".join(lines) + "\n"


def _render_shard(d: ShardDecision) -> list[str]:
    out: list[str] = []
    out.append(f"## Shard `{d.shard_name}`")
    # Lead with the recommendation.
    verb = {
        "scale-up": f"**Scale up** to **{d.recommended_brokers}** brokers "
                    f"(from {d.current_brokers}).",
        "scale-down": f"**Scale down** to **{d.recommended_brokers}** brokers "
                      f"(from {d.current_brokers}).",
        "hold": f"**Hold** at **{d.current_brokers}** brokers.",
        "no-decision": "**No decision** — refusing to recommend on this data.",
    }[d.action.value]
    out.append(f"> {verb}")
    if d.reason:
        out.append(f"> _Reason: {d.reason}_")
    if d.binding_axis:
        out.append(f"> Binding axis: **{d.binding_axis.value}**"
                   f" (fanout {d.fanout_ratio:.2f}, avg msg {d.avg_msg_size:.0f}B).")
    out.append("")

    if d.axes:
        out.append("| Axis | Demand ratio | Effective threshold | Configured | Derived (safe) | Pressure |")
        out.append("|---|---|---|---|---|---|")
        for name in _AXIS_ORDER:
            ar = d.axes.get(name)
            if ar is None:
                continue
            derived = "—" if ar.derived_threshold is None else f"{ar.derived_threshold:.2f}"
            marker = " ⟵ binding" if d.binding_axis and d.binding_axis.value == name else ""
            out.append(
                f"| {name}{marker} | {ar.demand_ratio:.2f} | {ar.effective_threshold:.2f} | "
                f"{ar.configured_threshold:.2f} | {derived} | {ar.pressure:.2f} |"
            )
        out.append("")

        # Derived headroom, prominent (§5.7)
        binding = d.axes.get(d.binding_axis.value) if d.binding_axis else None
        if binding and binding.derived_threshold is not None and binding.derived_inputs:
            di = binding.derived_inputs
            if binding.derived_threshold < binding.configured_threshold:
                out.append(
                    f"**Derived headroom (binding axis):** the configured "
                    f"{binding.configured_threshold:.2f} is **less safe** than the derived "
                    f"{binding.derived_threshold:.2f}. Using {binding.effective_threshold:.2f}."
                )
            else:
                out.append(
                    f"**Derived headroom (binding axis):** derived {binding.derived_threshold:.2f} "
                    f"≥ configured {binding.configured_threshold:.2f}; using configured "
                    f"{binding.effective_threshold:.2f}."
                )
            out.append(
                f"- peak growth: {di.get('peak_growth_rate_per_min', 0):.4f}/min · "
                f"minutes-to-capacity: {di.get('minutes_to_capacity', 0):.1f} · "
                f"safety factor: {di.get('safety_factor', 0):.1f}"
            )
            out.append("")

    if d.warnings:
        out.append("**Warnings:**")
        for w in d.warnings:
            out.append(f"- ⚠️ _{w.code.value}_: {w.message}")
        out.append("")

    return out
