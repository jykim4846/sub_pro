import tempfile
import unittest
from pathlib import Path

from g2b_bid_reco.agency_analysis import AgencyRangeAnalyzer
from g2b_bid_reco.db import connect, init_db, insert_case, load_historical_cases
from g2b_bid_reco.models import AgencyRangeRequest, HistoricalBidCase
from g2b_bid_reco.sample_data import SAMPLE_CASES


class AgencyRangeAnalyzerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "bids.db"
        init_db(self.db_path)
        with connect(self.db_path) as conn:
            for case in SAMPLE_CASES:
                insert_case(conn, case)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_agency_range_uses_agency_history_when_available(self) -> None:
        cases = load_historical_cases(self.db_path)
        analyzer = AgencyRangeAnalyzer(cases)
        request = AgencyRangeRequest(
            agency_name="한국출판문화산업진흥원",
            category="service",
            contract_method="적격심사",
            region="seoul",
            base_amount=245_000_000,
            floor_rate=87.745,
        )

        report = analyzer.analyze(request)

        self.assertEqual(report.confidence, "high")
        self.assertEqual(report.lookback_years_used, 3)
        self.assertGreaterEqual(report.agency_case_count, 5)
        self.assertGreaterEqual(report.blended_rate, 87.93)
        self.assertLessEqual(report.blended_rate, 88.02)
        self.assertTrue(report.evidence)

    def test_agency_range_falls_back_to_peers_for_sparse_agency(self) -> None:
        cases = load_historical_cases(self.db_path)
        analyzer = AgencyRangeAnalyzer(cases)
        request = AgencyRangeRequest(
            agency_name="축산물품질평가원",
            category="service",
            contract_method="적격심사",
            base_amount=185_000_000,
            floor_rate=87.745,
        )

        report = analyzer.analyze(request)

        self.assertEqual(report.confidence, "medium")
        self.assertEqual(report.lookback_years_used, 7)
        self.assertEqual(report.agency_case_count, 2)
        self.assertGreater(report.peer_case_count, report.agency_case_count)
        self.assertIn("peer 그룹", " ".join(report.notes))

    def test_agency_range_returns_low_confidence_without_peer_cases(self) -> None:
        cases = load_historical_cases(self.db_path)
        analyzer = AgencyRangeAnalyzer(cases)
        request = AgencyRangeRequest(
            agency_name="없는기관",
            category="construction",
            contract_method="종합심사",
            base_amount=500_000_000,
            floor_rate=86.745,
        )

        report = analyzer.analyze(request)

        self.assertEqual(report.peer_case_count, 0)
        self.assertEqual(report.confidence, "low")
        self.assertIsNone(report.lookback_years_used)
        self.assertIsNotNone(report.recommended_amount)

    def test_parent_pool_narrows_peer_to_sibling_group(self) -> None:
        def _case(notice_id: str, agency: str, rate: float) -> HistoricalBidCase:
            return HistoricalBidCase(
                notice_id=notice_id,
                agency_name=agency,
                category="service",
                contract_method="일반경쟁",
                region="seoul",
                base_amount=100_000_000,
                award_amount=100_000_000 * (rate / 100.0),
                bid_rate=rate,
                bidder_count=10,
                opened_at="2025-05-01",
            )

        cases = (
            [_case(f"SUB-{i}", "하위기관", 85.0) for i in range(3)]
            + [_case(f"SIB-{i}", "형제기관", 95.0) for i in range(10)]
            + [_case(f"OUT-{i}", "무관기관", 70.0) for i in range(40)]
        )
        request = AgencyRangeRequest(
            agency_name="하위기관",
            category="service",
            contract_method="일반경쟁",
            region="seoul",
            base_amount=100_000_000,
        )

        wide = AgencyRangeAnalyzer(cases).analyze(request)
        narrow = AgencyRangeAnalyzer(
            cases,
            parent_pool_agencies=frozenset({"형제기관", "상위기관"}),
            parent_name="상위기관",
        ).analyze(request)

        self.assertEqual(wide.peer_case_count, len(cases))
        # Peer restricted to sub (3) + siblings (10).
        self.assertEqual(narrow.peer_case_count, 13)
        # Without pool: mean dragged down by the 40 unrelated cases at 70%.
        self.assertLess(wide.peer_mean_rate, narrow.peer_mean_rate)

    def test_parent_pool_strengthens_shrinkage_toward_sibling_trend(self) -> None:
        def _case(notice_id: str, agency: str, rate: float) -> HistoricalBidCase:
            return HistoricalBidCase(
                notice_id=notice_id,
                agency_name=agency,
                category="service",
                contract_method="일반경쟁",
                region="seoul",
                base_amount=100_000_000,
                award_amount=100_000_000 * (rate / 100.0),
                bid_rate=rate,
                bidder_count=10,
                opened_at="2025-05-01",
            )

        # Sub has only 2 cases at 85% -- hard to trust alone.
        # Sibling has 20 cases at 95% under same parent.
        cases = (
            [_case(f"SUB-{i}", "하위기관", 85.0) for i in range(2)]
            + [_case(f"SIB-{i}", "형제기관", 95.0) for i in range(20)]
        )
        request = AgencyRangeRequest(
            agency_name="하위기관",
            category="service",
            contract_method="일반경쟁",
            region="seoul",
            base_amount=100_000_000,
        )
        report = AgencyRangeAnalyzer(
            cases,
            prior_strength=10.0,
            parent_pool_agencies=frozenset({"형제기관"}),
            parent_name="상위기관",
        ).analyze(request)
        # Blended should sit between sub (85) and sibling (95): shrinkage pulls the
        # 2-case sub signal toward the parent pool rather than keeping it at 85.
        self.assertGreater(report.blended_rate, 85.0)
        self.assertLess(report.blended_rate, 95.0)

    def test_agency_range_expands_from_3y_to_5y_when_recent_history_is_too_small(self) -> None:
        cases = [
            HistoricalBidCase(
                notice_id="A-001",
                agency_name="한국수자원공사 시화사업본부",
                category="goods",
                contract_method="전자입찰",
                region="gyeonggi",
                base_amount=100_000_000,
                award_amount=88_100_000,
                bid_rate=88.1,
                bidder_count=10,
                opened_at="2024-05-01",
            ),
            HistoricalBidCase(
                notice_id="A-002",
                agency_name="한국수자원공사 시화사업본부",
                category="goods",
                contract_method="전자입찰",
                region="gyeonggi",
                base_amount=105_000_000,
                award_amount=92_190_000,
                bid_rate=87.8,
                bidder_count=11,
                opened_at="2022-07-01",
            ),
            HistoricalBidCase(
                notice_id="A-003",
                agency_name="한국수자원공사 시화사업본부",
                category="goods",
                contract_method="전자입찰",
                region="gyeonggi",
                base_amount=98_000_000,
                award_amount=86_828_000,
                bid_rate=88.6,
                bidder_count=9,
                opened_at="2021-09-01",
            ),
            HistoricalBidCase(
                notice_id="P-001",
                agency_name="다른기관",
                category="goods",
                contract_method="전자입찰",
                region="gyeonggi",
                base_amount=101_000_000,
                award_amount=88_375_000,
                bid_rate=87.5,
                bidder_count=12,
                opened_at="2024-02-01",
            ),
        ]
        analyzer = AgencyRangeAnalyzer(cases)
        request = AgencyRangeRequest(
            agency_name="한국수자원공사 시화사업본부",
            category="goods",
            contract_method="전자입찰",
            region="gyeonggi",
            base_amount=102_000_000,
            reference_date="2026-04-17",
        )

        report = analyzer.analyze(request)

        self.assertEqual(report.lookback_years_used, 5)
        self.assertEqual(report.agency_case_count, 3)
        self.assertIn("최근 5년", " ".join(report.notes))
