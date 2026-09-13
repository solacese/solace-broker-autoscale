"""Prediction accuracy recorder (§7).

Records every recommendation, and joins later observed samples back to compute observed capacity at
observed load. SQLite by default - this data never leaves the operator's machine and is gitignored.

The capacity model's credibility is the whole tool: if the first recommendation an architect checks
is off by 40%, the project is finished. So we make error measurable, especially the OPTIMISTIC kind
(model predicted more capacity than the broker actually delivered), which is the dangerous one.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from ..decision.types import AXES, ShardDecision

_SCHEMA = """
CREATE TABLE IF NOT EXISTS recommendations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    shard TEXT NOT NULL,
    model_version TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    binding_axis TEXT,
    action TEXT NOT NULL,
    current_brokers INTEGER NOT NULL,
    recommended_brokers INTEGER NOT NULL,
    avg_msg_size REAL NOT NULL,
    ratio_messages REAL, ratio_bytes REAL, ratio_connections REAL, ratio_spool REAL,
    -- predicted per-broker capacity per axis (raw units), from the model at record time
    pred_cap_messages REAL, pred_cap_bytes REAL, pred_cap_connections REAL, pred_cap_spool REAL
);
CREATE TABLE IF NOT EXISTS observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    shard TEXT NOT NULL,
    msg_size_bucket INTEGER NOT NULL,
    axis TEXT NOT NULL,
    -- observed per-broker capacity at observed load (raw units)
    observed_capacity REAL NOT NULL,
    -- the predicted per-broker capacity the model gave for the same axis+bucket
    predicted_capacity REAL NOT NULL,
    model_version TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rec_shard ON recommendations(shard);
CREATE INDEX IF NOT EXISTS idx_obs_bucket ON observations(msg_size_bucket, axis);
"""


@dataclass(frozen=True)
class AccuracyStat:
    axis: str
    bucket: int | None
    count: int
    mape: float  # mean absolute percentage error
    mean_signed_pct: float  # + = model optimistic (predicted > observed), the dangerous direction
    optimistic_fraction: float


class AccuracyRecorder:
    def __init__(self, store: str | Path) -> None:
        self._path = str(store)
        self._conn = sqlite3.connect(self._path)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    # ---- recording ---------------------------------------------------------------------------

    def record_recommendation(self, decision: ShardDecision, config_hash: str, ts: float) -> int:
        axes = decision.axes
        # predicted per-broker capacity per axis: demand_ratio = demand / cap → cap = demand/ratio,
        # but we store the ratios and let observations carry predicted capacity directly. Here we
        # persist the ratios and avg size; predicted capacity is joined at observation time.
        cur = self._conn.execute(
            """INSERT INTO recommendations
               (ts, shard, model_version, config_hash, binding_axis, action, current_brokers,
                recommended_brokers, avg_msg_size, ratio_messages, ratio_bytes, ratio_connections,
                ratio_spool, pred_cap_messages, pred_cap_bytes, pred_cap_connections, pred_cap_spool)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                ts, decision.shard_name, decision.model_version, config_hash,
                decision.binding_axis.value if decision.binding_axis else None,
                decision.action.value, decision.current_brokers, decision.recommended_brokers,
                decision.avg_msg_size,
                *[axes[a].demand_ratio if a in axes else None for a in AXES],
                *[None for _ in AXES],  # pred_cap filled by callers that have the model handy
            ),
        )
        self._conn.commit()
        return int(cur.lastrowid or 0)

    def record_observation(
        self,
        ts: float,
        shard: str,
        msg_size_bucket: int,
        axis: str,
        observed_capacity: float,
        predicted_capacity: float,
        model_version: str,
    ) -> None:
        """Record observed vs predicted per-broker capacity for one axis at one size bucket.

        Observed capacity = observed load on the axis / brokers actually serving it (the caller
        computes this from a later metrics join). Predicted = what the model claimed.
        """
        self._conn.execute(
            """INSERT INTO observations
               (ts, shard, msg_size_bucket, axis, observed_capacity, predicted_capacity, model_version)
               VALUES (?,?,?,?,?,?,?)""",
            (ts, shard, msg_size_bucket, axis, observed_capacity, predicted_capacity, model_version),
        )
        self._conn.commit()

    # ---- reporting ---------------------------------------------------------------------------

    def stats(self, group_by: str = "axis") -> list[AccuracyStat]:
        """MAPE and signed error per axis, or per (axis, bucket).

        Signed error is (predicted - observed) / observed: positive means the model was OPTIMISTIC
        (claimed more capacity than delivered) - flagged prominently because it is the dangerous one.
        """
        rows = self._conn.execute(
            "SELECT axis, msg_size_bucket, observed_capacity, predicted_capacity FROM observations"
        ).fetchall()
        buckets: dict[tuple[str, int | None], list[tuple[float, float]]] = {}
        for r in rows:
            key = (r["axis"], r["msg_size_bucket"] if group_by == "bucket" else None)
            buckets.setdefault(key, []).append((r["observed_capacity"], r["predicted_capacity"]))

        out: list[AccuracyStat] = []
        for (axis, bucket), pairs in sorted(buckets.items(), key=lambda kv: (kv[0][0], kv[0][1] or 0)):
            abs_errs = []
            signed = []
            optimistic = 0
            for observed, predicted in pairs:
                if observed <= 0:
                    continue
                err = (predicted - observed) / observed
                abs_errs.append(abs(err))
                signed.append(err)
                if err > 0:
                    optimistic += 1
            n = len(abs_errs)
            if n == 0:
                continue
            out.append(AccuracyStat(
                axis=axis,
                bucket=bucket,
                count=n,
                mape=100.0 * sum(abs_errs) / n,
                mean_signed_pct=100.0 * sum(signed) / n,
                optimistic_fraction=optimistic / n,
            ))
        return out

    def close(self) -> None:
        self._conn.close()
