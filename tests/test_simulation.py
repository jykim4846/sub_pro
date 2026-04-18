import json
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path

from g2b_bid_reco.cli import _auto_bid_pending
from g2b_bid_reco.api import PPSCollector, PublicDataPortalClient
from g2b_bid_reco.db import (
    fail_stale_automation_runs,
    finish_automation_run,
    connect,
    get_cached_notice_prediction,
    get_demand_agency,
    get_latest_automation_run,
    init_db,
    insert_case,
    list_pending_notice_prediction_rows,
    list_mock_bids,
    replace_auto_mock_bid_batch,
    start_automation_run,
    upsert_notice_prediction_cache,
    update_automation_run,
)
from g2b_bid_reco.models import BidNoticeSnapshot, HistoricalBidCase
from g2b_bid_reco.simulation import run_simulation


class SimulationStrategyTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "bids.db"
        init_db(self.db_path)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_run_simulation_selects_highest_eligible_customer(self) -> None:
        cases = [
            HistoricalBidCase(
                notice_id=f"H-{idx}",
                agency_name="테스트청",
                category="service",
                contract_method="적격심사",
                region="seoul",
                base_amount=100_000_000.0,
                award_amount=100_000_000.0 * ((90.0 + idx * 0.1) / 100.0),
                bid_rate=90.0 + idx * 0.1,
                bidder_count=10,
                opened_at=f"2025-01-{idx + 1:02d}",
            )
            for idx in range(6)
        ]
        report = run_simulation(
            notice_id="N-1",
            base_amount=100_000_000.0,
            floor_rate=None,
            predicted_rate=89.85,
            lower_rate=89.70,
            upper_rate=90.05,
            predicted_amount=89_850_000.0,
            competitors=[],
            historical_cases=cases,
            n_customers=4,
        )
        self.assertEqual(len(report.customers), 4)
        self.assertEqual(report.best_customer_idx, 4)
        self.assertAlmostEqual(
            report.mean_winning_rate_when_we_win or 0.0,
            report.customers[-1].rate,
            places=4,
        )

    def test_replace_auto_mock_bid_batch_replaces_only_pending_auto_rows(self) -> None:
        with connect(self.db_path) as conn:
            insert_case(
                conn,
                HistoricalBidCase(
                    notice_id="PENDING-1",
                    agency_name="기관A",
                    category="service",
                    contract_method="적격심사",
                    region="seoul",
                    base_amount=100_000_000.0,
                    award_amount=0.0,
                    bid_rate=0.0,
                    bidder_count=0,
                    opened_at="2025-01-01",
                ),
            )
            insert_case(
                conn,
                HistoricalBidCase(
                    notice_id="AWARDED-1",
                    agency_name="기관B",
                    category="service",
                    contract_method="적격심사",
                    region="seoul",
                    base_amount=100_000_000.0,
                    award_amount=88_100_000.0,
                    bid_rate=88.1,
                    bidder_count=10,
                    opened_at="2025-01-01",
                ),
            )
            conn.execute(
                """
                INSERT INTO mock_bids (notice_id, bid_amount, bid_rate, note, simulation_id, customer_idx)
                VALUES
                ('PENDING-1', 87000000, 87.0, 'auto:old', 'auto-old', 1),
                ('AWARDED-1', 87000000, 87.0, 'auto:old', 'auto-old', 1)
                """
            )

        replace_auto_mock_bid_batch(
            self.db_path,
            "auto-new",
            [
                {
                    "notice_id": "PENDING-1",
                    "bid_amount": 87100000,
                    "bid_rate": 87.1,
                    "predicted_amount": None,
                    "predicted_rate": None,
                    "note": "auto:new",
                    "customer_idx": 1,
                }
            ],
        )

        rows = list_mock_bids(self.db_path)
        pending = [row for row in rows if row["notice_id"] == "PENDING-1"]
        awarded = [row for row in rows if row["notice_id"] == "AWARDED-1"]
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["note"], "auto:new")
        self.assertEqual(len(awarded), 1)
        self.assertEqual(awarded[0]["note"], "auto:old")

    def test_sync_demand_agencies_ingests_master_data(self) -> None:
        payload = {
            "response": {
                "header": {"resultCode": "00", "resultMsg": "OK"},
                "body": {
                    "items": {
                        "item": [
                            {
                                "dminsttCd": "1234567",
                                "dminsttNm": "테스트 수요기관",
                                "topInsttCd": "1000000",
                                "topInsttNm": "최상위기관",
                                "jurirnoDivNm": "공공기관",
                                "insttAddr": "세종시 어딘가",
                                "zipNo": "30112",
                            }
                        ]
                    },
                    "totalCount": 1,
                },
            }
        }
        client = PublicDataPortalClient(
            service_key="dummy",
            opener=lambda _url: json.dumps(payload, ensure_ascii=False),
        )
        collector = PPSCollector(client=client, db_path=str(self.db_path))
        result = collector.sync_demand_agencies(
            endpoint="http://example.com/user-info",
            query={"pageNo": 1, "numOfRows": 10, "type": "json", "inqryDiv": "1"},
            max_pages=1,
        )
        self.assertEqual(result.items_upserted, 1)
        agency = get_demand_agency(self.db_path, "1234567")
        assert agency is not None
        self.assertEqual(agency["agency_name"], "테스트 수요기관")
        self.assertEqual(agency["top_agency_name"], "최상위기관")
        self.assertEqual(agency["postal_code"], "30112")

    def test_automation_run_progress_tracking(self) -> None:
        start_automation_run(
            self.db_path,
            run_id="auto-1",
            kind="auto_bid_pending",
            total_items=25,
            message="starting",
        )
        update_automation_run(
            self.db_path,
            run_id="auto-1",
            processed_items=10,
            success_items=9,
            failed_items=1,
            message="processing 10/25",
        )
        finish_automation_run(
            self.db_path,
            run_id="auto-1",
            status="completed",
            processed_items=25,
            success_items=24,
            failed_items=1,
            message="done",
        )
        row = get_latest_automation_run(self.db_path, "auto_bid_pending")
        assert row is not None
        self.assertEqual(row["status"], "completed")
        self.assertEqual(row["processed_items"], 25)
        self.assertEqual(row["success_items"], 24)
        self.assertEqual(row["failed_items"], 1)

    def test_fail_stale_automation_runs_closes_old_running_rows(self) -> None:
        start_automation_run(
            self.db_path,
            run_id="auto-stale",
            kind="auto_bid_pending",
            total_items=100,
            message="starting",
        )
        with connect(self.db_path) as conn:
            conn.execute(
                """
                UPDATE automation_runs
                SET updated_at = datetime('now', '-20 minutes')
                WHERE run_id = 'auto-stale'
                """
            )
        closed = fail_stale_automation_runs(
            self.db_path,
            kind="auto_bid_pending",
            stale_after_minutes=10,
        )
        self.assertEqual(closed, 1)
        row = get_latest_automation_run(self.db_path, "auto_bid_pending")
        assert row is not None
        self.assertEqual(row["status"], "failed")

    def test_notice_prediction_cache_invalidates_after_new_result_ingest(self) -> None:
        with connect(self.db_path) as conn:
            insert_case(
                conn,
                HistoricalBidCase(
                    notice_id="PENDING-CACHE-1",
                    agency_name="기관C",
                    category="service",
                    contract_method="적격심사",
                    region="seoul",
                    base_amount=120_000_000.0,
                    award_amount=0.0,
                    bid_rate=0.0,
                    bidder_count=0,
                    opened_at="2025-02-01",
                ),
            )

        notice = BidNoticeSnapshot(
            notice_id="PENDING-CACHE-1",
            agency_name="기관C",
            category="service",
            contract_method="적격심사",
            region="seoul",
            base_amount=120_000_000.0,
            floor_rate=None,
            opened_at="2025-02-01",
        )
        upsert_notice_prediction_cache(
            self.db_path,
            notice,
            0.3,
            predicted_amount=105_000_000.0,
            predicted_rate=87.5,
            lower_rate=87.2,
            upper_rate=87.8,
            estimated_win_probability=0.31,
            confidence="medium",
            agency_cases=8,
            peer_cases=22,
            lookback_years_used=2,
            analysis_notes="cached",
        )
        cached = get_cached_notice_prediction(self.db_path, notice, 0.3)
        self.assertIsNotNone(cached)

        with connect(self.db_path) as conn:
            insert_case(
                conn,
                HistoricalBidCase(
                    notice_id="AWARDED-CACHE-2",
                    agency_name="기관D",
                    category="service",
                    contract_method="적격심사",
                    region="seoul",
                    base_amount=100_000_000.0,
                    award_amount=88_100_000.0,
                    bid_rate=88.1,
                    bidder_count=12,
                    opened_at="2025-02-02",
                ),
            )

        invalidated = get_cached_notice_prediction(self.db_path, notice, 0.3)
        self.assertIsNone(invalidated)
        rows = list_pending_notice_prediction_rows(
            self.db_path,
            target_win_probability=0.3,
            category="service",
            since_days=None,
            limit=10,
        )
        self.assertEqual(rows[0]["cache_status"], "stale")

    def test_resolve_demand_agency_endpoint_uses_first_non_404_candidate(self) -> None:
        calls: list[str] = []

        def opener(url: str) -> str:
            calls.append(url)
            if "getDminsttInfo02" in url:
                raise urllib.error.HTTPError(url, 404, "not found", hdrs=None, fp=None)
            return json.dumps(
                {
                    "response": {
                        "header": {"resultCode": "00", "resultMsg": "OK"},
                        "body": {"items": {"item": []}, "totalCount": 0},
                    }
                },
                ensure_ascii=False,
            )

        import urllib.error

        client = PublicDataPortalClient(service_key="dummy", opener=opener)
        collector = PPSCollector(client=client, db_path=str(self.db_path))
        endpoint = collector.resolve_demand_agency_endpoint()
        self.assertTrue(endpoint.endswith("getDmndInsttInfo02"))
        self.assertGreaterEqual(len(calls), 2)

    def test_auto_bid_pending_tracks_timing_without_changing_strategy(self) -> None:
        with connect(self.db_path) as conn:
            for idx in range(12):
                insert_case(
                    conn,
                    HistoricalBidCase(
                        notice_id=f"HIST-{idx}",
                        agency_name="기관A",
                        category="service",
                        contract_method="적격심사",
                        region="seoul",
                        base_amount=100_000_000.0,
                        award_amount=88_000_000.0 + idx,
                        bid_rate=88.0 + (idx * 0.01),
                        bidder_count=10,
                        opened_at=f"2025-01-{idx + 1:02d}",
                    ),
                )
            insert_case(
                conn,
                HistoricalBidCase(
                    notice_id="PENDING-1",
                    agency_name="기관A",
                    category="service",
                    contract_method="적격심사",
                    region="seoul",
                    base_amount=100_000_000.0,
                    award_amount=0.0,
                    bid_rate=0.0,
                    bidder_count=0,
                    opened_at="2025-02-01",
                ),
            )

        payload = _auto_bid_pending(
            Namespace(
                db_path=str(self.db_path),
                category=None,
                agency=None,
                since_days=None,
                limit=10,
                top_k=5,
                num_customers=3,
                target_win_probability=0.75,
                dry_run=True,
            )
        )
        self.assertEqual(payload["notices_seen"], 1)
        row = get_latest_automation_run(self.db_path, "auto_bid_pending")
        assert row is not None
        self.assertIn("predict=", row["message"])
        self.assertIn("simulate=", row["message"])

    def test_auto_bid_pending_preserves_existing_non_auto_rows(self) -> None:
        with connect(self.db_path) as conn:
            for idx in range(12):
                insert_case(
                    conn,
                    HistoricalBidCase(
                        notice_id=f"HIST-R-{idx}",
                        agency_name="기관A",
                        category="service",
                        contract_method="적격심사",
                        region="seoul",
                        base_amount=100_000_000.0,
                        award_amount=88_000_000.0 + idx,
                        bid_rate=88.0 + (idx * 0.01),
                        bidder_count=10,
                        opened_at=f"2025-01-{idx + 1:02d}",
                    ),
                )
            insert_case(
                conn,
                HistoricalBidCase(
                    notice_id="PENDING-REPLACE-1",
                    agency_name="기관A",
                    category="service",
                    contract_method="적격심사",
                    region="seoul",
                    base_amount=100_000_000.0,
                    award_amount=0.0,
                    bid_rate=0.0,
                    bidder_count=0,
                    opened_at="2025-02-01",
                ),
            )
            conn.execute(
                """
                INSERT INTO mock_bids (notice_id, bid_amount, bid_rate, note, simulation_id, customer_idx)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("PENDING-REPLACE-1", 87_000_000, 87.0, "manual:old", "manual-old", 1),
            )

        _auto_bid_pending(
            Namespace(
                db_path=str(self.db_path),
                category=None,
                agency=None,
                since_days=None,
                limit=10,
                top_k=5,
                num_customers=3,
                target_win_probability=0.75,
                dry_run=False,
            )
        )
        rows = [row for row in list_mock_bids(self.db_path) if row["notice_id"] == "PENDING-REPLACE-1"]
        self.assertEqual(len(rows), 4)
        self.assertEqual(sum(1 for row in rows if str(row["note"]).startswith("manual:")), 1)
        self.assertEqual(sum(1 for row in rows if str(row["note"]).startswith("auto:trend-aware-quantile")), 3)

    def test_auto_bid_pending_resumes_existing_pending_auto_notices(self) -> None:
        with connect(self.db_path) as conn:
            for idx in range(12):
                insert_case(
                    conn,
                    HistoricalBidCase(
                        notice_id=f"HIST-S-{idx}",
                        agency_name="기관A",
                        category="service",
                        contract_method="적격심사",
                        region="seoul",
                        base_amount=100_000_000.0,
                        award_amount=88_000_000.0 + idx,
                        bid_rate=88.0 + (idx * 0.01),
                        bidder_count=10,
                        opened_at=f"2025-01-{idx + 1:02d}",
                    ),
                )
            insert_case(
                conn,
                HistoricalBidCase(
                    notice_id="PENDING-RESUME-1",
                    agency_name="기관A",
                    category="service",
                    contract_method="적격심사",
                    region="seoul",
                    base_amount=100_000_000.0,
                    award_amount=0.0,
                    bid_rate=0.0,
                    bidder_count=0,
                    opened_at="2025-02-01",
                ),
            )
            insert_case(
                conn,
                HistoricalBidCase(
                    notice_id="PENDING-RESUME-2",
                    agency_name="기관A",
                    category="service",
                    contract_method="적격심사",
                    region="seoul",
                    base_amount=100_000_000.0,
                    award_amount=0.0,
                    bid_rate=0.0,
                    bidder_count=0,
                    opened_at="2025-02-02",
                ),
            )
            conn.execute(
                """
                INSERT INTO mock_bids (notice_id, bid_amount, bid_rate, note, simulation_id, customer_idx)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("PENDING-RESUME-1", 87_000_000, 87.0, "auto:old", "auto-old", 1),
            )

        payload = _auto_bid_pending(
            Namespace(
                db_path=str(self.db_path),
                category=None,
                agency=None,
                since_days=None,
                limit=10,
                top_k=5,
                num_customers=3,
                target_win_probability=0.75,
                dry_run=True,
            )
        )
        self.assertEqual(payload["notices_seen"], 2)
        self.assertEqual(payload["notices_resumed"], 1)
        self.assertEqual(payload["notices_computed"], 1)
        row = get_latest_automation_run(self.db_path, "auto_bid_pending")
        assert row is not None
        self.assertEqual(row["processed_items"], 2)
        self.assertEqual(row["success_items"], 2)


if __name__ == "__main__":
    unittest.main()
