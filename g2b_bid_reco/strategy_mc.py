"""Monte-carlo initialization for strategy_tables (MODES.md Path B).

For each scope (category, contract_method) with enough historical bid_results,
find the quantile-position distribution that maximizes our estimated win rate
for each customer count N = 1..Nmax.

The winning rule mirrors ``simulation.run_simulation``: customer rates are
sorted ascending, and for a given competitor rate, the winner is the customer
with the highest rate still strictly below the competitor. We win iff at
least one of our customers satisfies this relation.

Search strategy: coordinate descent on quantile positions, seeded with even
spacing [1/(N+1), 2/(N+1), ..., N/(N+1)]. Cheap (~120 evaluations per scope×N)
so it scales to dozens of scopes × N=1..10 in seconds.
"""

from __future__ import annotations

import bisect
import json
import random
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from .db import connect


# Minimum historical rates required per scope before we attempt MC. Below this
# the estimate is too noisy to beat the heuristic seed.
MIN_SCOPE_SAMPLES = 30

# Acceptable rate range (percent). Drops obvious errors (NULL, 0, > 110 etc.).
RATE_MIN = 50.0
RATE_MAX = 110.0


@dataclass
class ScopeKey:
    category: str
    contract_method: str
    agency_name: str = ""
    region: str = ""


def _quantile(sorted_values: list[float], q: float) -> float:
    if not sorted_values:
        return 0.0
    if q <= 0:
        return sorted_values[0]
    if q >= 1:
        return sorted_values[-1]
    idx = q * (len(sorted_values) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = idx - lo
    return sorted_values[lo] * (1 - frac) + sorted_values[hi] * frac


def estimate_win_rate(
    sorted_rates: list[float],
    quantiles: list[float],
    n_trials: int,
    rng: random.Random | None = None,
) -> float:
    """Win rate under the bisect game given candidate quantile positions.

    ``sorted_rates`` is the historical rate distribution for this scope. For
    each trial we sample one competitor rate and check whether our highest
    customer rate strictly below it exists.
    """
    if not sorted_rates or not quantiles:
        return 0.0
    our_rates = sorted(_quantile(sorted_rates, q) for q in quantiles)
    rng = rng or random
    wins = 0
    for _ in range(n_trials):
        comp = rng.choice(sorted_rates)
        # bisect_left gives first index >= comp, so our rates strictly below
        # comp exist iff that index is > 0.
        if bisect.bisect_left(our_rates, comp) > 0:
            wins += 1
    return wins / n_trials


def optimize_quantiles(
    sorted_rates: list[float],
    n_customers: int,
    n_trials: int = 2000,
    rng: random.Random | None = None,
) -> tuple[list[float], float]:
    """Coordinate descent over quantile positions.

    Seeds with even spacing then perturbs each coordinate by ±step (halving
    step each outer pass) to find a local max of the estimated win rate.
    """
    if n_customers <= 0:
        return [], 0.0
    rng = rng or random.Random(42)
    quantiles = [(i + 1) / (n_customers + 1) for i in range(n_customers)]
    best_wr = estimate_win_rate(sorted_rates, quantiles, n_trials, rng)

    step = 0.10
    for _pass in range(3):
        improved = False
        for i in range(n_customers):
            for delta in (-step, step, -step / 2, step / 2):
                candidate = quantiles.copy()
                candidate[i] = max(0.05, min(0.95, candidate[i] + delta))
                candidate.sort()
                wr = estimate_win_rate(sorted_rates, candidate, n_trials, rng)
                if wr > best_wr + 1e-4:
                    best_wr = wr
                    quantiles = candidate
                    improved = True
        if not improved:
            break
        step *= 0.5

    return [round(q, 4) for q in quantiles], round(best_wr, 4)


def fetch_scope_rates(
    conn: sqlite3.Connection, scope: ScopeKey
) -> list[float]:
    """Historical bid_rate values for this scope, filtered to sane range."""
    sql = (
        "SELECT r.bid_rate FROM bid_results r "
        "JOIN bid_notices n ON r.notice_id = n.notice_id "
        "WHERE n.category = ? AND n.contract_method = ? "
        "  AND r.bid_rate BETWEEN ? AND ?"
    )
    params: list[object] = [scope.category, scope.contract_method, RATE_MIN, RATE_MAX]
    if scope.agency_name:
        sql += " AND n.agency_name = ?"
        params.append(scope.agency_name)
    if scope.region:
        sql += " AND n.region = ?"
        params.append(scope.region)
    return [row["bid_rate"] for row in conn.execute(sql, params)]


def fetch_scopes(
    conn: sqlite3.Connection, min_samples: int = MIN_SCOPE_SAMPLES
) -> list[ScopeKey]:
    """Distinct (category, contract_method) scopes with >= min_samples rows."""
    rows = conn.execute(
        """
        SELECT n.category, n.contract_method, COUNT(*) AS cnt
        FROM bid_results r JOIN bid_notices n ON r.notice_id = n.notice_id
        WHERE n.category <> '' AND n.contract_method <> ''
          AND r.bid_rate BETWEEN ? AND ?
        GROUP BY n.category, n.contract_method
        HAVING cnt >= ?
        ORDER BY cnt DESC
        """,
        (RATE_MIN, RATE_MAX, min_samples),
    ).fetchall()
    return [ScopeKey(row["category"], row["contract_method"]) for row in rows]


def upsert_strategy_row(
    conn: sqlite3.Connection,
    scope: ScopeKey,
    n_customers: int,
    quantiles: list[float],
    win_rate_estimate: float,
    sample_size: int,
) -> None:
    conn.execute(
        """
        INSERT INTO strategy_tables
            (agency_name, category, contract_method, region,
             n_customers, quantiles_json, source,
             sample_size, win_rate_estimate, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, 'montecarlo', ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT (agency_name, category, contract_method, region, n_customers)
        DO UPDATE SET
            quantiles_json = excluded.quantiles_json,
            source = excluded.source,
            sample_size = excluded.sample_size,
            win_rate_estimate = excluded.win_rate_estimate,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            scope.agency_name,
            scope.category,
            scope.contract_method,
            scope.region,
            n_customers,
            json.dumps(quantiles),
            sample_size,
            win_rate_estimate,
        ),
    )


def build_strategy_tables(
    db_path: str | Path,
    n_range: range = range(1, 11),
    n_trials: int = 2000,
    min_samples: int = MIN_SCOPE_SAMPLES,
    seed: int = 42,
) -> dict[str, int]:
    """Populate strategy_tables via monte carlo over all qualifying scopes.

    Returns summary counts for logging.
    """
    rng = random.Random(seed)
    summary = {"scopes": 0, "rows_written": 0, "skipped_small": 0}
    with connect(db_path) as conn:
        scopes = fetch_scopes(conn, min_samples=min_samples)
        summary["scopes"] = len(scopes)
        for scope in scopes:
            rates = fetch_scope_rates(conn, scope)
            if len(rates) < min_samples:
                summary["skipped_small"] += 1
                continue
            sorted_rates = sorted(rates)
            for n in n_range:
                quantiles, wr = optimize_quantiles(
                    sorted_rates, n, n_trials=n_trials, rng=rng
                )
                upsert_strategy_row(
                    conn, scope, n, quantiles, wr, len(sorted_rates)
                )
                summary["rows_written"] += 1
        conn.commit()
    return summary
