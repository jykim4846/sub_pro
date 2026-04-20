"""Path C — online EMA calibration of strategy_tables.win_rate_estimate.

Path B (``strategy_mc.build_strategy_tables_v2``) seeds quantile positions and
their monte-carlo estimated win rate. As auto-bid portfolios are actually
evaluated against real results (``mock_bid_evaluations``), the observed
(scope, n_customers) win rate may drift away from the MC estimate.

This module blends the two with a conservative EMA:

    new_estimate = α * observed + (1 - α) * old_estimate

and flips ``source`` to ``'online_v2'`` so readers can see which rows have
been calibrated online. Quantile positions themselves are not adjusted here
— that belongs to a future phase (re-run Path B MC seeded with observed
win-rate targets).

Run weekly via cron or as an optional daily-batch step once evaluations
accumulate.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from .db import connect, summarize_evaluations_by_scope_n


DEFAULT_ALPHA = 0.1
DEFAULT_MIN_DECIDED = 20


def ema_update_strategy_tables(
    db_path: str | Path,
    alpha: float = DEFAULT_ALPHA,
    min_decided: int = DEFAULT_MIN_DECIDED,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Blend observed win rates into strategy_tables.win_rate_estimate.

    Only baseline rows (agency_name='', region='') are updated — agency- or
    region-scoped rows would need their own evaluation aggregation.

    Returns a summary dict with per-row diffs useful for CLI logging and
    dashboard surfacing of 'calibration drift'.
    """
    if not (0.0 < alpha <= 1.0):
        raise ValueError(f"alpha must be in (0, 1], got {alpha}")
    if min_decided < 1:
        raise ValueError(f"min_decided must be >= 1, got {min_decided}")

    observed = summarize_evaluations_by_scope_n(db_path)
    by_key: dict[tuple[str, str, int], dict] = {
        (row["category"], row["contract_method"], int(row["n_customers"])): row
        for row in observed
    }

    diffs: list[dict[str, Any]] = []
    evaluated = 0
    updated = 0
    skipped_low_evidence = 0
    skipped_no_evidence = 0

    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT category, contract_method, n_customers,
                   win_rate_estimate, sample_size, source
            FROM strategy_tables
            WHERE agency_name = '' AND region = ''
            ORDER BY category, contract_method, n_customers
            """
        ).fetchall()

        for row in rows:
            evaluated += 1
            key = (row["category"], row["contract_method"], int(row["n_customers"]))
            obs = by_key.get(key)
            if obs is None:
                skipped_no_evidence += 1
                continue
            decided = int(obs["decided"] or 0)
            if decided < min_decided:
                skipped_low_evidence += 1
                continue

            old_estimate = float(row["win_rate_estimate"] or 0.0)
            observed_rate = float(obs["observed_win_rate"])
            new_estimate = alpha * observed_rate + (1.0 - alpha) * old_estimate

            diffs.append(
                {
                    "category": key[0],
                    "contract_method": key[1],
                    "n_customers": key[2],
                    "old": round(old_estimate, 4),
                    "observed": round(observed_rate, 4),
                    "new": round(new_estimate, 4),
                    "decided": decided,
                    "wins": int(obs["wins"] or 0),
                    "prior_source": row["source"],
                }
            )
            updated += 1

            if not dry_run:
                conn.execute(
                    """
                    UPDATE strategy_tables
                    SET win_rate_estimate = ?,
                        sample_size = ?,
                        source = 'online_v2',
                        updated_at = CURRENT_TIMESTAMP
                    WHERE category = ? AND contract_method = ?
                      AND agency_name = '' AND region = ''
                      AND n_customers = ?
                    """,
                    (new_estimate, decided, key[0], key[1], key[2]),
                )

    return {
        "alpha": alpha,
        "min_decided": min_decided,
        "dry_run": bool(dry_run),
        "rows_evaluated": evaluated,
        "rows_updated": updated,
        "rows_skipped_low_evidence": skipped_low_evidence,
        "rows_skipped_no_evidence": skipped_no_evidence,
        "diffs": diffs,
    }
