"""Tests for MODES.md Path B monte-carlo seeding of strategy_tables."""

from __future__ import annotations

import json
import random
from pathlib import Path

import pytest

from g2b_bid_reco.db import connect, init_db
from g2b_bid_reco.strategy_mc import (
    ScopeKey,
    build_strategy_tables,
    estimate_win_rate,
    fetch_scope_rates,
    optimize_quantiles,
)


def _seed_test_scope(
    db_path: Path, category: str, method: str, rates: list[float]
) -> None:
    init_db(db_path)
    with connect(db_path) as conn:
        for i, rate in enumerate(rates):
            nid = f"{category[:3]}-{method[:3]}-{i:04d}"
            conn.execute(
                "INSERT INTO bid_notices "
                "(notice_id, agency_name, category, contract_method, "
                " region, base_amount, opened_at) "
                "VALUES (?, 'test-agency', ?, ?, '서울', 1000000, "
                "'2026-01-01 00:00:00')",
                (nid, category, method),
            )
            conn.execute(
                "INSERT INTO bid_results "
                "(notice_id, award_amount, bid_rate, bidder_count) "
                "VALUES (?, 1000000, ?, 5)",
                (nid, rate),
            )
        conn.commit()


def test_estimate_win_rate_ground_truth() -> None:
    """Low quantile → more competitors strictly above us → more wins.

    (Winning rule from simulation.run_simulation: ``bisect_left(our_rates, comp)
    > 0`` — we need a rate strictly below the competitor.)
    """
    rates = list(range(80, 95))  # [80..94], uniform
    sorted_rates = sorted(float(r) for r in rates)
    rng = random.Random(0)
    q_low = estimate_win_rate(sorted_rates, [0.1], 2000, rng)
    q_high = estimate_win_rate(sorted_rates, [0.9], 2000, rng)
    # Our one customer at the 10th pctile wins whenever competitor > that rate,
    # which is almost always. At 90th pctile the reverse is true.
    assert q_low > q_high
    assert q_low > 0.7
    assert q_high < 0.3


def test_optimize_quantiles_improves_over_seed() -> None:
    """Coordinate descent should not make the estimate worse than seed."""
    rates = sorted(float(r) for r in range(60, 100))  # wide distribution
    rng = random.Random(42)
    # Seed win rate: even spacing for N=3
    seed_q = [0.25, 0.5, 0.75]
    seed_wr = estimate_win_rate(rates, seed_q, 2000, rng)
    rng = random.Random(42)  # reset so optimize starts from same seed
    quantiles, best_wr = optimize_quantiles(rates, 3, n_trials=2000, rng=rng)
    assert best_wr >= seed_wr - 1e-3
    assert len(quantiles) == 3
    assert all(0.05 <= q <= 0.95 for q in quantiles)
    assert quantiles == sorted(quantiles)


def test_optimize_quantiles_n1() -> None:
    """Single-customer case should return one quantile."""
    rates = sorted(float(r) for r in range(70, 100))
    rng = random.Random(1)
    qs, wr = optimize_quantiles(rates, 1, n_trials=500, rng=rng)
    assert len(qs) == 1
    assert 0.0 < wr <= 1.0


def test_fetch_scope_rates_filters_range(tmp_path: Path) -> None:
    db = tmp_path / "fetch.db"
    # Mix: valid (80, 85), below floor (10), above ceiling (120), zero
    _seed_test_scope(
        db, "goods", "전자입찰", [80.0, 85.0, 10.0, 120.0, 0.0]
    )
    with connect(db) as conn:
        rates = fetch_scope_rates(
            conn, ScopeKey(category="goods", contract_method="전자입찰")
        )
    assert sorted(rates) == [80.0, 85.0]


def test_build_strategy_tables_end_to_end(tmp_path: Path) -> None:
    db = tmp_path / "build.db"
    # 60 rows spread across [75, 95] so this scope qualifies for MC.
    rates = [75.0 + (i % 21) for i in range(60)]
    _seed_test_scope(db, "goods", "전자입찰", rates)

    summary = build_strategy_tables(
        db, n_range=range(1, 4), n_trials=500, min_samples=30
    )
    assert summary["scopes"] == 1
    assert summary["rows_written"] == 3  # N=1,2,3

    with connect(db) as conn:
        rows = conn.execute(
            "SELECT n_customers, quantiles_json, win_rate_estimate, "
            "       source, sample_size "
            "FROM strategy_tables WHERE category='goods' "
            "ORDER BY n_customers"
        ).fetchall()
    assert [r["n_customers"] for r in rows] == [1, 2, 3]
    for row in rows:
        assert row["source"] == "montecarlo"
        assert row["sample_size"] == 60
        quantiles = json.loads(row["quantiles_json"])
        assert len(quantiles) == row["n_customers"]
        assert 0.0 <= row["win_rate_estimate"] <= 1.0


def test_build_strategy_tables_is_idempotent_upsert(tmp_path: Path) -> None:
    db = tmp_path / "upsert.db"
    rates = [80.0 + (i % 10) for i in range(50)]
    _seed_test_scope(db, "goods", "전자입찰", rates)

    first = build_strategy_tables(db, n_range=range(1, 3), n_trials=200)
    second = build_strategy_tables(db, n_range=range(1, 3), n_trials=200)
    assert first["rows_written"] == second["rows_written"] == 2

    with connect(db) as conn:
        count = conn.execute(
            "SELECT COUNT(*) AS c FROM strategy_tables"
        ).fetchone()["c"]
    # UPSERT — second run must not duplicate
    assert count == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
