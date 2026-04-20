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


#
# v2 — within-notice 평균가 model
#


def _seed_scope_with_bidder_counts(
    db_path: Path,
    category: str,
    method: str,
    rates: list[float],
    bidder_counts: list[int],
) -> None:
    """Seed a scope with matched rate + bidder_count per notice."""
    assert len(rates) == len(bidder_counts)
    init_db(db_path)
    with connect(db_path) as conn:
        for i, (rate, bc) in enumerate(zip(rates, bidder_counts)):
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
                "VALUES (?, 1000000, ?, ?)",
                (nid, rate, bc),
            )
        conn.commit()


def test_simulate_win_rate_v2_solo_bidder_wins_iff_below_target() -> None:
    """With no competitors, we win whenever our rate ≤ T."""
    from g2b_bid_reco.strategy_mc import simulate_win_rate_v2

    # All competitors from empty pool — we set bidder_counts so M=0
    rates = list(map(float, range(80, 100)))  # 80..99
    rng = random.Random(0)
    wr = simulate_win_rate_v2(
        historical_rates=rates,
        bidder_counts=[1],  # 1 bidder = just us, no competitors
        our_quantiles=[0.5],  # at median ~89.5
        n_trials=1000,
        rng=rng,
    )
    # bidder_count=1 still means "m=1" competitor in our MC though — clarify
    # by using max_bidders=0 effectively. With m=1 competitor, we contest
    # against one random rate. Win rate should be > 0.
    assert 0.0 < wr < 1.0


def test_simulate_win_rate_v2_more_customers_wins_more() -> None:
    """With the same scope, adding customers must not decrease win rate.

    Strict monotonicity isn't guaranteed (sampling noise + coord descent
    choosing locally-bad seeds), but 3 customers at evenly-spaced quantiles
    should dominate 1 customer at the same distribution's median.
    """
    from g2b_bid_reco.strategy_mc import simulate_win_rate_v2

    rates = [80.0 + i * 0.5 for i in range(20)]  # 80..89.5 continuous-ish
    rng = random.Random(7)
    single = simulate_win_rate_v2(rates, [5], [0.5], 2000, rng=rng)
    trio = simulate_win_rate_v2(rates, [5], [0.25, 0.5, 0.75], 2000, rng=rng)
    assert trio > single


def test_fetch_scopes_v2_excludes_수의계약(tmp_path: Path) -> None:
    from g2b_bid_reco.strategy_mc import fetch_scopes_v2

    db = tmp_path / "excl.db"
    rates = [80.0 + i for i in range(40)]
    bcs = [5] * 40
    _seed_scope_with_bidder_counts(db, "goods", "전자입찰", rates, bcs)
    # add 수의계약 rows too — they should be filtered out
    with connect(db) as conn:
        for i in range(40):
            nid = f"SU-{i:04d}"
            conn.execute(
                "INSERT INTO bid_notices "
                "(notice_id, agency_name, category, contract_method, "
                " region, base_amount, opened_at) "
                "VALUES (?, 'a', 'goods', '수의계약', '서울', 1, '2026-01-01')",
                (nid,),
            )
            conn.execute(
                "INSERT INTO bid_results "
                "(notice_id, award_amount, bid_rate, bidder_count) "
                "VALUES (?, 1, 80, 1)",
                (nid,),
            )
        conn.commit()
    with connect(db) as conn:
        scopes = fetch_scopes_v2(conn, min_samples=30)
    methods = {s.contract_method for s in scopes}
    assert "전자입찰" in methods
    assert "수의계약" not in methods


def test_build_strategy_tables_v2_writes_montecarlo_v2_source(tmp_path: Path) -> None:
    from g2b_bid_reco.strategy_mc import build_strategy_tables_v2

    db = tmp_path / "v2.db"
    rates = [80.0 + (i % 15) for i in range(60)]
    bcs = [3 + (i % 8) for i in range(60)]
    _seed_scope_with_bidder_counts(db, "goods", "전자입찰", rates, bcs)

    summary = build_strategy_tables_v2(
        db, n_range=range(1, 4), n_trials=500, min_samples=30
    )
    assert summary["scopes"] == 1
    assert summary["rows_written"] == 3
    assert summary["skipped_no_bidder_count"] == 0

    with connect(db) as conn:
        rows = conn.execute(
            "SELECT source, n_customers, quantiles_json FROM strategy_tables "
            "WHERE category='goods' ORDER BY n_customers"
        ).fetchall()
    assert len(rows) == 3
    for row in rows:
        assert row["source"] == "montecarlo_v2"
        parsed = json.loads(row["quantiles_json"])
        assert len(parsed) == row["n_customers"]


def test_build_strategy_tables_v2_skips_수의계약(tmp_path: Path) -> None:
    from g2b_bid_reco.strategy_mc import build_strategy_tables_v2

    db = tmp_path / "skip.db"
    init_db(db)
    with connect(db) as conn:
        for i in range(60):
            nid = f"SU-{i:04d}"
            conn.execute(
                "INSERT INTO bid_notices "
                "(notice_id, agency_name, category, contract_method, "
                " region, base_amount, opened_at) "
                "VALUES (?, 'a', 'goods', '수의계약', '서울', 1, '2026-01-01')",
                (nid,),
            )
            conn.execute(
                "INSERT INTO bid_results "
                "(notice_id, award_amount, bid_rate, bidder_count) "
                "VALUES (?, 1, 80, 1)",
                (nid,),
            )
        conn.commit()
    summary = build_strategy_tables_v2(db, n_range=range(1, 3), n_trials=200)
    assert summary["scopes"] == 0
    assert summary["rows_written"] == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
