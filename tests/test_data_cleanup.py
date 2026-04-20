"""Tests for floor_rate normalisation."""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from g2b_bid_reco.data_cleanup import cleanup_floor_rates
from g2b_bid_reco.db import connect, init_db


def _insert_notice(
    conn,
    notice_id: str,
    category: str,
    method: str,
    floor_rate: float | None,
) -> None:
    conn.execute(
        """
        INSERT INTO bid_notices (notice_id, agency_name, category,
            contract_method, region, base_amount, floor_rate, opened_at)
        VALUES (?, '기관A', ?, ?, 'seoul', 100000000, ?, '2025-03-01')
        """,
        (notice_id, category, method, floor_rate),
    )


class CleanupFloorRatesTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "bids.db"
        init_db(self.db_path)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_fills_null_zero_and_outliers_with_scope_modal(self) -> None:
        with connect(self.db_path) as conn:
            # 40 valid construction/전자입찰 notices with modal 87.745
            for i in range(40):
                _insert_notice(conn, f"C-V-{i}", "construction", "전자입찰", 87.745)
            # 10 anomalies that must all be normalised: NULL, zero, low/high outlier
            _insert_notice(conn, "C-N-1", "construction", "전자입찰", None)
            _insert_notice(conn, "C-N-2", "construction", "전자입찰", 0.0)
            _insert_notice(conn, "C-N-3", "construction", "전자입찰", 1.0)
            _insert_notice(conn, "C-N-4", "construction", "전자입찰", 50.0 - 0.01)
            _insert_notice(conn, "C-N-5", "construction", "전자입찰", 99.0 + 0.01)
            _insert_notice(conn, "C-N-6", "construction", "전자입찰", 100.0)

        summary = cleanup_floor_rates(self.db_path)
        self.assertEqual(summary["rows_updated"], 6)
        scope_report = [s for s in summary["scopes"] if s["category"] == "construction"][0]
        self.assertAlmostEqual(scope_report["modal"], 87.745)
        self.assertEqual(scope_report["modal_n"], 40)
        self.assertEqual(scope_report["updated"], 6)

        with connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT floor_rate FROM bid_notices "
                "WHERE category='construction' AND contract_method='전자입찰'"
            ).fetchall()
        self.assertEqual(len(rows), 46)
        self.assertTrue(all(abs(float(r["floor_rate"]) - 87.745) < 1e-9 for r in rows))

    def test_per_scope_modal_differs(self) -> None:
        with connect(self.db_path) as conn:
            for i in range(40):
                _insert_notice(conn, f"C-{i}", "construction", "전자입찰", 87.745)
            for i in range(40):
                _insert_notice(conn, f"G-{i}", "goods", "전자입찰", 88.0)
            _insert_notice(conn, "C-N", "construction", "전자입찰", None)
            _insert_notice(conn, "G-N", "goods", "전자입찰", None)

        cleanup_floor_rates(self.db_path)

        with connect(self.db_path) as conn:
            cn = conn.execute(
                "SELECT floor_rate FROM bid_notices WHERE notice_id='C-N'"
            ).fetchone()
            gn = conn.execute(
                "SELECT floor_rate FROM bid_notices WHERE notice_id='G-N'"
            ).fetchone()
        self.assertAlmostEqual(float(cn["floor_rate"]), 87.745)
        self.assertAlmostEqual(float(gn["floor_rate"]), 88.0)

    def test_skips_scope_without_enough_evidence(self) -> None:
        with connect(self.db_path) as conn:
            # Only 10 valid rows — below default min_modal_n=30
            for i in range(10):
                _insert_notice(conn, f"S-V-{i}", "service", "전자입찰", 88.0)
            _insert_notice(conn, "S-N-1", "service", "전자입찰", None)

        summary = cleanup_floor_rates(self.db_path)
        self.assertEqual(summary["rows_updated"], 0)
        self.assertEqual(summary["rows_skipped_no_modal"], 1)
        scope_report = summary["scopes"][0]
        self.assertIsNone(scope_report["modal"])
        self.assertTrue(scope_report["skipped"])

        with connect(self.db_path) as conn:
            null_row = conn.execute(
                "SELECT floor_rate FROM bid_notices WHERE notice_id='S-N-1'"
            ).fetchone()
        self.assertIsNone(null_row["floor_rate"])

    def test_dry_run_reports_without_writing(self) -> None:
        with connect(self.db_path) as conn:
            for i in range(40):
                _insert_notice(conn, f"C-V-{i}", "construction", "전자입찰", 87.745)
            _insert_notice(conn, "C-N", "construction", "전자입찰", None)

        summary = cleanup_floor_rates(self.db_path, dry_run=True)
        self.assertEqual(summary["rows_updated"], 1)

        with connect(self.db_path) as conn:
            null_row = conn.execute(
                "SELECT floor_rate FROM bid_notices WHERE notice_id='C-N'"
            ).fetchone()
        self.assertIsNone(null_row["floor_rate"])

    def test_is_idempotent(self) -> None:
        with connect(self.db_path) as conn:
            for i in range(40):
                _insert_notice(conn, f"C-V-{i}", "construction", "전자입찰", 87.745)
            _insert_notice(conn, "C-N", "construction", "전자입찰", None)

        first = cleanup_floor_rates(self.db_path)
        second = cleanup_floor_rates(self.db_path)
        self.assertEqual(first["rows_updated"], 1)
        self.assertEqual(second["rows_updated"], 0)

    def test_does_not_touch_untargeted_contract_methods(self) -> None:
        with connect(self.db_path) as conn:
            for i in range(40):
                _insert_notice(conn, f"C-V-{i}", "construction", "전자입찰", 87.745)
            _insert_notice(conn, "SU-N", "construction", "수의계약", None)

        cleanup_floor_rates(self.db_path)

        with connect(self.db_path) as conn:
            su = conn.execute(
                "SELECT floor_rate FROM bid_notices WHERE notice_id='SU-N'"
            ).fetchone()
        self.assertIsNone(su["floor_rate"])

    def test_rejects_bad_params(self) -> None:
        with self.assertRaises(ValueError):
            cleanup_floor_rates(self.db_path, contract_methods=[])
        with self.assertRaises(ValueError):
            cleanup_floor_rates(self.db_path, min_modal_n=0)


if __name__ == "__main__":
    unittest.main()
