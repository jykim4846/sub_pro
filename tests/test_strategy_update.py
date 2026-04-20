"""Tests for Path C — EMA calibration of strategy_tables.win_rate_estimate."""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from g2b_bid_reco.db import connect, init_db
from g2b_bid_reco.strategy_update import (
    DEFAULT_ALPHA,
    DEFAULT_MIN_DECIDED,
    ema_update_strategy_tables,
)


class StrategyUpdateEMATest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "bids.db"
        init_db(self.db_path)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _seed_strategy(
        self,
        category: str,
        method: str,
        n: int,
        quantiles: list[float],
        win_rate: float,
        sample_size: int = 100,
    ) -> None:
        with connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO strategy_tables
                    (agency_name, category, contract_method, region,
                     n_customers, quantiles_json, source,
                     sample_size, win_rate_estimate)
                VALUES ('', ?, ?, '', ?, ?, 'montecarlo_v2', ?, ?)
                """,
                (category, method, n, json.dumps(quantiles), sample_size, win_rate),
            )

    def _seed_notice(self, notice_id: str, category: str, method: str) -> None:
        with connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO bid_notices (notice_id, agency_name, category,
                    contract_method, region, base_amount, opened_at)
                VALUES (?, '기관X', ?, ?, 'seoul', 100000000, '2025-03-01')
                """,
                (notice_id, category, method),
            )

    def _seed_evaluations(
        self,
        notice_id: str,
        n: int,
        verdicts: list[str],
    ) -> None:
        with connect(self.db_path) as conn:
            for idx, verdict in enumerate(verdicts, start=1):
                conn.execute(
                    """
                    INSERT INTO mock_bids (notice_id, bid_amount, bid_rate,
                        note, simulation_id, customer_idx, n_customers)
                    VALUES (?, ?, ?, 'auto:strategy_v2', 'sim-T', ?, ?)
                    """,
                    (notice_id, 90_000_000 + idx, 90.0 + idx * 0.01, idx, n),
                )
                mock_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                conn.execute(
                    """
                    INSERT INTO mock_bid_evaluations (mock_id, notice_id,
                        simulation_id, customer_idx, bid_amount, bid_rate,
                        verdict, n_customers)
                    VALUES (?, ?, 'sim-T', ?, 0, 0, ?, ?)
                    """,
                    (mock_id, notice_id, idx, verdict, n),
                )

    def test_ema_updates_win_rate_with_observed_data(self) -> None:
        self._seed_strategy("service", "전자입찰", n=5, quantiles=[0.1, 0.3, 0.5, 0.7, 0.9],
                             win_rate=0.4)
        self._seed_notice("EMA-N-1", "service", "전자입찰")
        # 30 evaluations, 21 won → observed 0.70
        self._seed_evaluations("EMA-N-1", n=5, verdicts=(["won"] * 21 + ["lost"] * 9))

        result = ema_update_strategy_tables(self.db_path, alpha=0.1, min_decided=20)

        self.assertEqual(result["rows_updated"], 1)
        diff = result["diffs"][0]
        self.assertAlmostEqual(diff["old"], 0.4)
        self.assertAlmostEqual(diff["observed"], 0.7)
        # new = 0.1*0.7 + 0.9*0.4 = 0.43
        self.assertAlmostEqual(diff["new"], 0.43, places=4)
        self.assertEqual(diff["decided"], 30)

        with connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT win_rate_estimate, source, sample_size FROM strategy_tables "
                "WHERE category='service' AND contract_method='전자입찰' AND n_customers=5"
            ).fetchone()
        self.assertAlmostEqual(float(row["win_rate_estimate"]), 0.43, places=4)
        self.assertEqual(row["source"], "online_v2")
        self.assertEqual(int(row["sample_size"]), 30)

    def test_ema_dry_run_computes_without_writing(self) -> None:
        self._seed_strategy("service", "전자입찰", n=3, quantiles=[0.2, 0.5, 0.8],
                             win_rate=0.5)
        self._seed_notice("EMA-DRY-1", "service", "전자입찰")
        self._seed_evaluations("EMA-DRY-1", n=3,
                                verdicts=(["won"] * 20 + ["lost"] * 5))

        result = ema_update_strategy_tables(self.db_path, alpha=0.2, min_decided=20,
                                             dry_run=True)
        self.assertEqual(result["rows_updated"], 1)
        with connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT win_rate_estimate, source FROM strategy_tables "
                "WHERE n_customers=3"
            ).fetchone()
        # unchanged
        self.assertAlmostEqual(float(row["win_rate_estimate"]), 0.5)
        self.assertEqual(row["source"], "montecarlo_v2")

    def test_ema_skips_low_evidence_scopes(self) -> None:
        # enough rows in strategy_tables but only 5 decided evaluations
        self._seed_strategy("goods", "전자입찰", n=2, quantiles=[0.3, 0.7], win_rate=0.6)
        self._seed_notice("EMA-LOW-1", "goods", "전자입찰")
        self._seed_evaluations("EMA-LOW-1", n=2,
                                verdicts=["won", "won", "lost", "won", "lost"])
        result = ema_update_strategy_tables(self.db_path, min_decided=20)
        self.assertEqual(result["rows_updated"], 0)
        self.assertEqual(result["rows_skipped_low_evidence"], 1)

    def test_ema_skips_rows_without_any_evidence(self) -> None:
        self._seed_strategy("construction", "전자입찰", n=4, quantiles=[0.1, 0.4, 0.6, 0.9],
                             win_rate=0.2)
        # no evaluations at all
        result = ema_update_strategy_tables(self.db_path)
        self.assertEqual(result["rows_updated"], 0)
        self.assertEqual(result["rows_skipped_no_evidence"], 1)

    def test_ema_rejects_bad_params(self) -> None:
        with self.assertRaises(ValueError):
            ema_update_strategy_tables(self.db_path, alpha=0.0)
        with self.assertRaises(ValueError):
            ema_update_strategy_tables(self.db_path, alpha=1.5)
        with self.assertRaises(ValueError):
            ema_update_strategy_tables(self.db_path, min_decided=0)

    def test_defaults_are_conservative(self) -> None:
        # Sanity: default alpha should match MODES.md recommendation (0.1).
        self.assertAlmostEqual(DEFAULT_ALPHA, 0.1)
        self.assertGreaterEqual(DEFAULT_MIN_DECIDED, 10)


if __name__ == "__main__":
    unittest.main()
