import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from g2b_bid_reco.agency_analysis import AgencyRangeAnalyzer
from g2b_bid_reco.backtest import build_backtest_report
from g2b_bid_reco.db import (
    connect,
    get_actual_award,
    get_notice_snapshot,
    init_db,
    insert_case,
    load_historical_cases_for_notice,
)
from g2b_bid_reco.models import HistoricalBidCase
from g2b_bid_reco.notice_prediction import NoticePredictor


class BacktestReportTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "bids.db"
        init_db(self.db_path)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _seed_history(self, notice_date: datetime) -> None:
        bid_rates = [88.1, 87.7, 88.5, 88.0, 87.9, 88.4, 87.6, 88.2]
        with connect(self.db_path) as conn:
            for index, rate in enumerate(bid_rates):
                opened_at = (notice_date - timedelta(days=30 + index * 10)).strftime("%Y-%m-%d %H:%M:%S")
                case = HistoricalBidCase(
                    notice_id=f"PAST-{index:03d}",
                    agency_name="테스트청",
                    category="service",
                    contract_method="적격심사",
                    region="seoul",
                    base_amount=1_000_000_000.0,
                    award_amount=1_000_000_000.0 * (rate / 100.0),
                    bid_rate=rate,
                    bidder_count=20,
                    opened_at=opened_at,
                    winning_company=f"COMP-{index}",
                )
                insert_case(conn, case)

    def _seed_target_notice(self, notice_date: datetime) -> None:
        opened_at = notice_date.strftime("%Y-%m-%d %H:%M:%S")
        target_rate = 88.3
        target_base = 1_500_000_000.0
        with connect(self.db_path) as conn:
            case = HistoricalBidCase(
                notice_id="TARGET-001",
                agency_name="테스트청",
                category="service",
                contract_method="적격심사",
                region="seoul",
                base_amount=target_base,
                award_amount=target_base * (target_rate / 100.0),
                bid_rate=target_rate,
                bidder_count=15,
                opened_at=opened_at,
                winning_company="WINNER",
            )
            insert_case(conn, case)

    def test_actual_falls_within_predicted_range(self) -> None:
        notice_date = datetime(2025, 6, 1, 10, 0, 0)
        self._seed_history(notice_date)
        self._seed_target_notice(notice_date)

        notice = get_notice_snapshot(self.db_path, "TARGET-001")
        assert notice is not None
        cases = load_historical_cases_for_notice(self.db_path, "TARGET-001", notice.opened_at)
        analyzer = AgencyRangeAnalyzer(cases, target_win_probability=0.5)
        prediction = NoticePredictor(analyzer).predict(notice)

        actual = get_actual_award(self.db_path, "TARGET-001")
        assert actual is not None
        report = build_backtest_report(prediction, actual)

        self.assertEqual(report.notice_id, "TARGET-001")
        self.assertEqual(report.actual_rate, 88.3)
        self.assertAlmostEqual(report.actual_amount, 1_500_000_000.0 * 0.883, places=0)
        self.assertLess(abs(report.rate_gap_pp), 1.5)
        self.assertIsNotNone(report.predicted_amount)
        self.assertIsNotNone(report.amount_gap)
        self.assertIsNotNone(report.amount_gap_ratio)
        self.assertGreater(report.peer_case_count, 0)

    def test_amount_gap_none_when_base_amount_missing(self) -> None:
        notice_date = datetime(2025, 6, 1, 10, 0, 0)
        self._seed_history(notice_date)

        opened_at = notice_date.strftime("%Y-%m-%d %H:%M:%S")
        with connect(self.db_path) as conn:
            insert_case(
                conn,
                HistoricalBidCase(
                    notice_id="TARGET-002",
                    agency_name="테스트청",
                    category="service",
                    contract_method="적격심사",
                    region="seoul",
                    base_amount=0.0,
                    award_amount=500_000_000.0,
                    bid_rate=90.0,
                    bidder_count=10,
                    opened_at=opened_at,
                ),
            )

        notice = get_notice_snapshot(self.db_path, "TARGET-002")
        assert notice is not None

        # predict-notice would refuse if base_amount<=0, but build_backtest_report
        # still has to be robust when an analysis returns no recommended amount.
        # Force base_amount to zero on the snapshot to mimic that branch.
        notice.base_amount = 0.0
        cases = load_historical_cases_for_notice(self.db_path, "TARGET-002", notice.opened_at)
        analyzer = AgencyRangeAnalyzer(cases)
        prediction = NoticePredictor(analyzer).predict(notice)

        actual = get_actual_award(self.db_path, "TARGET-002")
        assert actual is not None
        report = build_backtest_report(prediction, actual)

        self.assertEqual(report.predicted_amount or 0.0, 0.0)
        self.assertIsNone(report.amount_gap)
        self.assertIsNone(report.amount_gap_ratio)


if __name__ == "__main__":
    unittest.main()
