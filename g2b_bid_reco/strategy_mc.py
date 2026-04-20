"""Monte-carlo initialization for strategy_tables (MODES.md Path B).

Two models live here — see MODES.md §9 for the full rationale.

**v1 — coverage model** (``estimate_win_rate``, ``optimize_quantiles``,
``build_strategy_tables``). Mirrors ``simulation.run_simulation`` 's bisect
rule: we count "wins" as trials where at least one of our customer rates is
strictly below a sampled historical winning rate. Not a real auction — just
a coverage check. Left in place for historical comparison.

**v2 — within-notice 평균가 model** (``simulate_win_rate_v2``,
``optimize_quantiles_v2``, ``build_strategy_tables_v2``). Models one actual
auction per trial: our N customers + M sampled competitors all bid into the
same notice with a hidden target rate T (proxied by historical winning rates).
Valid bids are those with rate ≤ T; winner is the highest valid bid. Used by
default for all contract methods except 수의계약 (direct-award, no competition).

Both search via coordinate descent on quantile positions seeded with even
spacing. Cheap enough (~120 evaluations per scope×N) to run the whole table
in seconds on a laptop.
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

# Contract methods that don't follow a competitive-auction rule. 수의계약 is
# direct award with a single pre-selected vendor — there is no "winning
# position" to optimize. Skipped entirely in v2 scope selection.
EXCLUDED_CONTRACT_METHODS = {"수의계약"}

# Cap on competitor count per trial. Real bidder_count in Korean public
# procurement can reach thousands — simulating that many draws adds cost
# without changing the answer much (our strategy converges once M >> N).
# Also serves as a circuit breaker for pathological scope data.
MAX_BIDDERS_PER_TRIAL = 50


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
    """Populate strategy_tables via v1 (coverage) monte carlo.

    Left in place for historical comparison — v2 is the default for new runs
    via the CLI. Returns summary counts for logging.
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


# ---------------------------------------------------------------------------
# v2 — within-notice 평균가 based auction MC
# ---------------------------------------------------------------------------


def simulate_win_rate_v2(
    historical_rates: list[float],
    bidder_counts: list[int],
    our_quantiles: list[float],
    n_trials: int,
    max_bidders: int = MAX_BIDDERS_PER_TRIAL,
    rng: random.Random | None = None,
) -> float:
    """One-notice 평균가 MC: simulate N of ours + M competitors per trial.

    For each trial we sample
      - T ∈ historical_rates (this notice's hidden 기준가격 %)
      - M ∈ bidder_counts (how many other bidders showed up)
      - M competitor rates bootstrapped from historical_rates
    then determine the winner as the highest rate ≤ T across everyone. We
    win iff that winner is one of our N customers.
    """
    if not historical_rates or not bidder_counts or not our_quantiles:
        return 0.0
    rng = rng or random
    sorted_hist = sorted(historical_rates)
    our_rates = [_quantile(sorted_hist, q) for q in our_quantiles]
    capped_counts = [min(b, max_bidders) for b in bidder_counts if b > 0]
    if not capped_counts:
        return 0.0

    wins = 0
    for _ in range(n_trials):
        t_target = rng.choice(historical_rates)
        m = rng.choice(capped_counts)
        # Sample M competitor rates (with replacement)
        best_them = -1.0
        for _c in range(m):
            cr = rng.choice(historical_rates)
            if cr <= t_target and cr > best_them:
                best_them = cr
        best_ours = -1.0
        for our in our_rates:
            if our <= t_target and our > best_ours:
                best_ours = our
        if best_ours > best_them:
            wins += 1
    return wins / n_trials


def optimize_quantiles_v2(
    sorted_rates: list[float],
    bidder_counts: list[int],
    n_customers: int,
    n_trials: int = 2000,
    max_bidders: int = MAX_BIDDERS_PER_TRIAL,
    rng: random.Random | None = None,
) -> tuple[list[float], float]:
    """Coordinate descent over quantile positions for the v2 auction model."""
    if n_customers <= 0 or not sorted_rates or not bidder_counts:
        return [], 0.0
    rng = rng or random.Random(42)
    quantiles = [(i + 1) / (n_customers + 1) for i in range(n_customers)]
    best_wr = simulate_win_rate_v2(
        sorted_rates, bidder_counts, quantiles, n_trials, max_bidders, rng
    )

    step = 0.10
    for _pass in range(3):
        improved = False
        for i in range(n_customers):
            for delta in (-step, step, -step / 2, step / 2):
                candidate = quantiles.copy()
                candidate[i] = max(0.05, min(0.95, candidate[i] + delta))
                candidate.sort()
                wr = simulate_win_rate_v2(
                    sorted_rates, bidder_counts, candidate,
                    n_trials, max_bidders, rng,
                )
                if wr > best_wr + 1e-4:
                    best_wr = wr
                    quantiles = candidate
                    improved = True
        if not improved:
            break
        step *= 0.5
    return [round(q, 4) for q in quantiles], round(best_wr, 4)


def fetch_scopes_v2(
    conn: sqlite3.Connection, min_samples: int = MIN_SCOPE_SAMPLES
) -> list[ScopeKey]:
    """Like fetch_scopes but excludes non-competitive contract methods."""
    placeholders = ",".join("?" for _ in EXCLUDED_CONTRACT_METHODS)
    excluded_clause = (
        f"AND n.contract_method NOT IN ({placeholders})"
        if EXCLUDED_CONTRACT_METHODS
        else ""
    )
    sql = f"""
        SELECT n.category, n.contract_method, COUNT(*) AS cnt
        FROM bid_results r JOIN bid_notices n ON r.notice_id = n.notice_id
        WHERE n.category <> '' AND n.contract_method <> ''
          {excluded_clause}
          AND r.bid_rate BETWEEN ? AND ?
        GROUP BY n.category, n.contract_method
        HAVING cnt >= ?
        ORDER BY cnt DESC
    """
    params: list[object] = list(EXCLUDED_CONTRACT_METHODS)
    params.extend([RATE_MIN, RATE_MAX, min_samples])
    rows = conn.execute(sql, params).fetchall()
    return [ScopeKey(row["category"], row["contract_method"]) for row in rows]


def fetch_scope_bidder_counts(
    conn: sqlite3.Connection, scope: ScopeKey
) -> list[int]:
    """Historical bidder_count values for this scope, filtered to positives."""
    sql = (
        "SELECT r.bidder_count FROM bid_results r "
        "JOIN bid_notices n ON r.notice_id = n.notice_id "
        "WHERE n.category = ? AND n.contract_method = ? "
        "  AND r.bidder_count > 0"
    )
    params: list[object] = [scope.category, scope.contract_method]
    if scope.agency_name:
        sql += " AND n.agency_name = ?"
        params.append(scope.agency_name)
    if scope.region:
        sql += " AND n.region = ?"
        params.append(scope.region)
    return [row["bidder_count"] for row in conn.execute(sql, params)]


def _upsert_strategy_row_v2(
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
        VALUES (?, ?, ?, ?, ?, ?, 'montecarlo_v2', ?, ?, CURRENT_TIMESTAMP)
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


def build_strategy_tables_v2(
    db_path: str | Path,
    n_range: range = range(1, 11),
    n_trials: int = 2000,
    min_samples: int = MIN_SCOPE_SAMPLES,
    max_bidders: int = MAX_BIDDERS_PER_TRIAL,
    seed: int = 42,
) -> dict[str, int]:
    """Populate strategy_tables via v2 (within-notice 평균가) monte carlo.

    Skips 수의계약 (direct-award, no competition) and any scope with fewer
    than ``min_samples`` historical rates.
    """
    rng = random.Random(seed)
    summary = {
        "scopes": 0,
        "rows_written": 0,
        "skipped_small": 0,
        "skipped_no_bidder_count": 0,
    }
    with connect(db_path) as conn:
        scopes = fetch_scopes_v2(conn, min_samples=min_samples)
        summary["scopes"] = len(scopes)
        for scope in scopes:
            rates = fetch_scope_rates(conn, scope)
            if len(rates) < min_samples:
                summary["skipped_small"] += 1
                continue
            bidder_counts = fetch_scope_bidder_counts(conn, scope)
            if not bidder_counts:
                summary["skipped_no_bidder_count"] += 1
                continue
            sorted_rates = sorted(rates)
            for n in n_range:
                quantiles, wr = optimize_quantiles_v2(
                    sorted_rates,
                    bidder_counts,
                    n,
                    n_trials=n_trials,
                    max_bidders=max_bidders,
                    rng=rng,
                )
                _upsert_strategy_row_v2(
                    conn, scope, n, quantiles, wr, len(sorted_rates)
                )
                summary["rows_written"] += 1
        conn.commit()
    return summary
