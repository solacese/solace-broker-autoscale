"""Accuracy report (§7): predicted vs actual, per axis and per size bucket.

Highlights buckets where the model is consistently optimistic - those are the dangerous ones.
"""

from __future__ import annotations

from .recorder import AccuracyRecorder


def format_accuracy_report(recorder: AccuracyRecorder, group_by: str = "axis") -> str:
    stats = recorder.stats(group_by=group_by)
    if not stats:
        return ("No observations recorded yet. Run recommendations, let later samples arrive, and "
                "join observed capacity back with record_observation().")

    lines = ["# Prediction accuracy (predicted vs actual per-broker capacity)", ""]
    if group_by == "bucket":
        lines.append("| Axis | Size bucket (B) | N | MAPE % | Mean signed % | Optimistic frac |")
        lines.append("|---|---|---|---|---|---|")
        for s in stats:
            flag = "  ⚠️ OPTIMISTIC" if s.mean_signed_pct > 5 and s.optimistic_fraction >= 0.5 else ""
            lines.append(
                f"| {s.axis} | {s.bucket} | {s.count} | {s.mape:.1f} | "
                f"{s.mean_signed_pct:+.1f} | {s.optimistic_fraction:.2f}{flag} |"
            )
    else:
        lines.append("| Axis | N | MAPE % | Mean signed % | Optimistic frac |")
        lines.append("|---|---|---|---|---|")
        for s in stats:
            flag = "  ⚠️ OPTIMISTIC" if s.mean_signed_pct > 5 and s.optimistic_fraction >= 0.5 else ""
            lines.append(
                f"| {s.axis} | {s.count} | {s.mape:.1f} | {s.mean_signed_pct:+.1f} | "
                f"{s.optimistic_fraction:.2f}{flag} |"
            )
    lines.append("")
    lines.append("_Positive signed error = the model predicted MORE capacity than the broker "
                 "delivered (optimistic). Optimistic error is the dangerous kind: it recommends too "
                 "few brokers._")
    return "\n".join(lines) + "\n"
