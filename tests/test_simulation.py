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
    get_agency_parent_pool,
    list_strategy_table_rows,
    load_cases_with_shrinkage,
    load_strategy_table_for_scope,
    refresh_mock_bid_evaluations,
    summarize_evaluations_by_scope_n,
    replace_auto_mock_bid_batch,
    seed_agency_parent_mapping,
    start_automation_run,
    upsert_bid_result,
    upsert_demand_agency,
    upsert_notice_prediction_cache,
    update_automation_run,
)
from g2b_bid_reco.models import BidNoticeSnapshot, HistoricalBidCase
from g2b_bid_reco.simulation import generate_customer_bids, run_simulation


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

    def test_seed_agency_parent_mapping_prefers_user_api_top_agency(self) -> None:
        with connect(self.db_path) as conn:
            for idx, agency_name in enumerate(("강릉산학협력단", "삼척산학협력단"), start=1):
                agency_code = f"C-00{idx}"
                upsert_demand_agency(
                    conn,
                    agency_code=agency_code,
                    agency_name=agency_name,
                    top_agency_code="1000000",
                    top_agency_name="강원대학교",
                    source="user-api",
                )
                for notice_idx in range(30):
                    notice_id = f"API-{idx}-{notice_idx}"
                    conn.execute(
                        """
                        INSERT INTO bid_notices (
                            notice_id, agency_name, agency_code, category, contract_method,
                            region, base_amount, estimated_amount, opened_at
                        ) VALUES (?, ?, ?, 'service', '일반경쟁', 'seoul', 100000000, 100000000, ?)
                        """,
                        (notice_id, agency_name, agency_code, f"2025-01-{notice_idx + 1:02d}"),
                    )
                    upsert_bid_result(
                        conn,
                        notice_id=notice_id,
                        award_amount=88_000_000.0,
                        bid_rate=88.0,
                        bidder_count=10,
                        winning_company="낙찰사",
                        result_status="awarded",
                        category="service",
                    )

        result = seed_agency_parent_mapping(self.db_path, min_subunits=2, min_parent_cases=50)
        self.assertEqual(result["skipped_unsafe"], 0)
        with connect(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT agency_name, parent_name, status, source
                FROM agency_parent_mapping
                ORDER BY agency_name
                """
            ).fetchall()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["parent_name"], "강원대학교")
        self.assertEqual(rows[1]["parent_name"], "강원대학교")
        self.assertTrue(all(row["source"] == "auto" for row in rows))

    def test_seed_agency_parent_mapping_falls_back_to_name_token(self) -> None:
        with connect(self.db_path) as conn:
            for idx, agency_name in enumerate(
                ("서울특별시 강남구", "서울특별시 서초구"),
                start=1,
            ):
                agency_code = f"1{idx:06d}"
                for notice_idx in range(30):
                    notice_id = f"TOKEN-{idx}-{notice_idx}"
                    conn.execute(
                        """
                        INSERT INTO bid_notices (
                            notice_id, agency_name, agency_code, category, contract_method,
                            region, base_amount, estimated_amount, opened_at
                        ) VALUES (?, ?, ?, 'service', '일반경쟁', 'seoul', 100000000, 100000000, ?)
                        """,
                        (notice_id, agency_name, agency_code, f"2025-02-{notice_idx + 1:02d}"),
                    )
                    upsert_bid_result(
                        conn,
                        notice_id=notice_id,
                        award_amount=89_000_000.0,
                        bid_rate=89.0,
                        bidder_count=12,
                        winning_company="낙찰사",
                        result_status="awarded",
                        category="service",
                    )

        result = seed_agency_parent_mapping(self.db_path, min_subunits=2, min_parent_cases=50)
        self.assertEqual(result["skipped_unsafe"], 0)
        with connect(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT agency_name, parent_name
                FROM agency_parent_mapping
                ORDER BY agency_name
                """
            ).fetchall()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["parent_name"], "서울특별시")
        self.assertEqual(rows[1]["parent_name"], "서울특별시")

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

    def test_refresh_mock_bid_evaluations_materializes_verdicts(self) -> None:
        with connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO bid_notices (
                    notice_id, agency_name, category, contract_method, region,
                    base_amount, estimated_amount, floor_rate, opened_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("EVAL-1", "기관A", "service", "적격심사", "seoul", 100_000_000.0, 100_000_000.0, 87.5, "2025-01-02"),
            )
            conn.execute(
                """
                INSERT INTO mock_bids (
                    notice_id, bid_amount, bid_rate, predicted_amount,
                    predicted_rate, note, simulation_id, customer_idx
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("EVAL-1", 87_600_000.0, 87.6, 87_700_000.0, 87.7, "auto:test", "auto-eval", 1),
            )
            upsert_bid_result(
                conn,
                notice_id="EVAL-1",
                award_amount=88_000_000.0,
                bid_rate=88.0,
                bidder_count=10,
                winning_company="낙찰사",
                result_status="awarded",
                category="service",
            )

        payload = refresh_mock_bid_evaluations(self.db_path)
        self.assertEqual(payload["evaluated_mock_bids"], 1)
        self.assertEqual(payload["won"], 1)
        with connect(self.db_path) as conn:
            row = conn.execute(
                """
                SELECT notice_id, verdict, actual_amount, actual_rate, simulation_id
                FROM mock_bid_evaluations
                WHERE notice_id = 'EVAL-1'
                """
            ).fetchone()
        assert row is not None
        self.assertEqual(row["verdict"], "won")
        self.assertEqual(row["simulation_id"], "auto-eval")
        self.assertAlmostEqual(row["actual_amount"], 88_000_000.0)

    def test_refresh_mock_bid_evaluations_today_results_only(self) -> None:
        with connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO bid_notices (
                    notice_id, agency_name, category, contract_method, region,
                    base_amount, estimated_amount, floor_rate, opened_at
                ) VALUES
                ('TODAY-1', '기관A', 'service', '적격심사', 'seoul', 100000000, 100000000, 87.5, '2025-01-02'),
                ('OLD-1', '기관B', 'service', '적격심사', 'seoul', 100000000, 100000000, 87.5, '2025-01-03')
                """
            )
            conn.execute(
                """
                INSERT INTO mock_bids (
                    notice_id, bid_amount, bid_rate, note, simulation_id, customer_idx
                ) VALUES
                ('TODAY-1', 87000000, 87.0, 'auto:test', 'auto-today', 1),
                ('OLD-1', 87000000, 87.0, 'auto:test', 'auto-old', 1)
                """
            )
            upsert_bid_result(
                conn,
                notice_id="TODAY-1",
                award_amount=88_000_000.0,
                bid_rate=88.0,
                bidder_count=10,
                winning_company="낙찰사",
                result_status="awarded",
                category="service",
            )
            upsert_bid_result(
                conn,
                notice_id="OLD-1",
                award_amount=88_000_000.0,
                bid_rate=88.0,
                bidder_count=10,
                winning_company="낙찰사",
                result_status="awarded",
                category="service",
            )
            conn.execute(
                "UPDATE bid_results SET created_at = date('now', '-2 days') WHERE notice_id = 'OLD-1'"
            )

        payload = refresh_mock_bid_evaluations(self.db_path, today_results_only=True)
        self.assertEqual(payload["evaluated_mock_bids"], 1)
        with connect(self.db_path) as conn:
            count = conn.execute("SELECT COUNT(*) FROM mock_bid_evaluations").fetchone()[0]
            notice_ids = {
                row[0] for row in conn.execute("SELECT notice_id FROM mock_bid_evaluations").fetchall()
            }
        self.assertEqual(count, 1)
        self.assertEqual(notice_ids, {"TODAY-1"})

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


    def _insert_awarded_case(
        self,
        conn,
        *,
        notice_id: str,
        agency_name: str,
        opened_at: str,
        bid_rate: float = 88.0,
        category: str = "service",
        contract_method: str = "일반경쟁",
    ) -> None:
        conn.execute(
            """
            INSERT INTO bid_notices (
                notice_id, agency_name, agency_code, category, contract_method,
                region, base_amount, estimated_amount, opened_at
            ) VALUES (?, ?, '', ?, ?, 'seoul', 100000000, 100000000, ?)
            """,
            (notice_id, agency_name, category, contract_method, opened_at),
        )
        upsert_bid_result(
            conn,
            notice_id=notice_id,
            award_amount=100_000_000.0 * (bid_rate / 100.0),
            bid_rate=bid_rate,
            bidder_count=10,
            winning_company="winner",
            result_status="awarded",
            category=category,
        )

    def _map_parent(self, conn, agency_name: str, parent_name: str) -> None:
        conn.execute(
            "INSERT OR REPLACE INTO agency_parent_mapping "
            "(agency_name, parent_name, status, source) VALUES (?, ?, 'approved', 'auto')",
            (agency_name, parent_name),
        )

    def test_shrinkage_no_parent_returns_own_only(self) -> None:
        with connect(self.db_path) as conn:
            for i in range(3):
                self._insert_awarded_case(
                    conn,
                    notice_id=f"SOLO-{i}",
                    agency_name="혼자기관",
                    opened_at=f"2025-05-{i + 1:02d}",
                    bid_rate=90.0,
                )
        cases, meta = load_cases_with_shrinkage(
            self.db_path, "혼자기관",
            category="service", contract_method="일반경쟁",
        )
        self.assertEqual(len(cases), 3)
        self.assertEqual(meta["n_sub"], 3)
        self.assertEqual(meta["n_parent_anchor"], 0)
        self.assertIsNone(meta["parent_name"])
        self.assertAlmostEqual(meta["w_sub"], 1.0)

    def test_shrinkage_blends_sub_with_parent_pool(self) -> None:
        with connect(self.db_path) as conn:
            for i in range(5):
                self._insert_awarded_case(
                    conn,
                    notice_id=f"SUB-{i}",
                    agency_name="하위기관",
                    opened_at=f"2025-06-{i + 1:02d}",
                    bid_rate=85.0,
                )
            for i in range(20):
                self._insert_awarded_case(
                    conn,
                    notice_id=f"SIB-{i}",
                    agency_name="형제기관",
                    opened_at=f"2025-06-{i + 1:02d}",
                    bid_rate=95.0,
                )
            self._map_parent(conn, "하위기관", "상위기관")
            self._map_parent(conn, "형제기관", "상위기관")

        cases, meta = load_cases_with_shrinkage(
            self.db_path, "하위기관",
            category="service", contract_method="일반경쟁",
            k=10,
        )
        self.assertEqual(meta["n_sub"], 5)
        self.assertEqual(meta["n_parent_anchor"], 10)
        self.assertEqual(meta["n_parent_available"], 20)
        self.assertEqual(meta["parent_name"], "상위기관")
        self.assertAlmostEqual(meta["w_sub"], 5 / 15)
        self.assertEqual(len(cases), 15)
        mean_rate = sum(c.bid_rate for c in cases) / len(cases)
        # Expected blend mean = (5*85 + 10*95) / 15
        self.assertAlmostEqual(mean_rate, (5 * 85 + 10 * 95) / 15, places=6)

    def test_shrinkage_pure_parent_when_sub_empty(self) -> None:
        with connect(self.db_path) as conn:
            for i in range(15):
                self._insert_awarded_case(
                    conn,
                    notice_id=f"ONLY-PARENT-{i}",
                    agency_name="형제만",
                    opened_at=f"2025-07-{i + 1:02d}",
                    bid_rate=88.0,
                )
            self._map_parent(conn, "신생기관", "상위기관")
            self._map_parent(conn, "형제만", "상위기관")

        cases, meta = load_cases_with_shrinkage(
            self.db_path, "신생기관",
            category="service", contract_method="일반경쟁",
            k=10,
        )
        self.assertEqual(meta["n_sub"], 0)
        self.assertEqual(meta["n_parent_anchor"], 10)
        self.assertEqual(meta["parent_name"], "상위기관")
        self.assertAlmostEqual(meta["w_sub"], 0.0)
        self.assertEqual(len(cases), 10)

    def test_parent_pool_lookup_returns_siblings_and_parent(self) -> None:
        with connect(self.db_path) as conn:
            self._map_parent(conn, "하위기관A", "상위")
            self._map_parent(conn, "하위기관B", "상위")
            self._map_parent(conn, "하위기관C", "상위")
            # Unrelated agency under a different parent.
            self._map_parent(conn, "외부기관", "다른상위")
        parent, pool = get_agency_parent_pool(self.db_path, "하위기관A")
        self.assertEqual(parent, "상위")
        self.assertIn("하위기관B", pool)
        self.assertIn("하위기관C", pool)
        self.assertIn("상위", pool)
        self.assertNotIn("하위기관A", pool)
        self.assertNotIn("외부기관", pool)

    def test_parent_pool_lookup_empty_when_unmapped(self) -> None:
        parent, pool = get_agency_parent_pool(self.db_path, "미매핑기관")
        self.assertIsNone(parent)
        self.assertEqual(pool, frozenset())

    def test_shrinkage_large_sub_dominates(self) -> None:
        with connect(self.db_path) as conn:
            for i in range(40):
                self._insert_awarded_case(
                    conn,
                    notice_id=f"BIG-SUB-{i}",
                    agency_name="독립기관",
                    opened_at=f"2025-08-{(i % 28) + 1:02d}",
                    bid_rate=82.0,
                )
            for i in range(12):
                self._insert_awarded_case(
                    conn,
                    notice_id=f"BIG-SIB-{i}",
                    agency_name="형제2",
                    opened_at=f"2025-08-{(i % 28) + 1:02d}",
                    bid_rate=92.0,
                )
            self._map_parent(conn, "독립기관", "상위2")
            self._map_parent(conn, "형제2", "상위2")

        cases, meta = load_cases_with_shrinkage(
            self.db_path, "독립기관",
            category="service", contract_method="일반경쟁",
            k=10,
        )
        self.assertEqual(meta["n_sub"], 40)
        self.assertEqual(meta["n_parent_anchor"], 10)
        self.assertAlmostEqual(meta["w_sub"], 40 / 50)
        self.assertEqual(len(cases), 50)


    def test_load_strategy_table_for_scope_returns_per_n_quantiles(self) -> None:
        with connect(self.db_path) as conn:
            for n, qs in [(1, [0.5]), (2, [0.3, 0.7]), (3, [0.2, 0.5, 0.8])]:
                conn.execute(
                    """
                    INSERT INTO strategy_tables
                        (agency_name, category, contract_method, region,
                         n_customers, quantiles_json, source,
                         sample_size, win_rate_estimate)
                    VALUES (?, ?, ?, ?, ?, ?, 'montecarlo_v2', 100, 0.5)
                    """,
                    ("", "service", "전자입찰", "", n, json.dumps(qs)),
                )
            # Row for a different scope that must not leak in.
            conn.execute(
                """
                INSERT INTO strategy_tables
                    (agency_name, category, contract_method, region,
                     n_customers, quantiles_json, source,
                     sample_size, win_rate_estimate)
                VALUES ('', 'goods', '전자입찰', '', 2, ?, 'montecarlo_v2', 100, 0.5)
                """,
                (json.dumps([0.1, 0.9]),),
            )
        with connect(self.db_path) as conn:
            out = load_strategy_table_for_scope(conn, "service", "전자입찰")
        self.assertEqual(sorted(out.keys()), [1, 2, 3])
        self.assertEqual(out[2], [0.3, 0.7])
        self.assertEqual(out[3], [0.2, 0.5, 0.8])

    def test_generate_customer_bids_honors_override_quantiles(self) -> None:
        cases = [
            HistoricalBidCase(
                notice_id=f"H-OV-{idx}",
                agency_name="기관Z",
                category="service",
                contract_method="전자입찰",
                region="seoul",
                base_amount=100_000_000.0,
                award_amount=100_000_000.0 * ((85.0 + idx * 0.2) / 100.0),
                bid_rate=85.0 + idx * 0.2,
                bidder_count=10,
                opened_at=f"2025-01-{idx + 1:02d}",
            )
            for idx in range(20)
        ]
        bids_h, *_ = generate_customer_bids(
            predicted_rate=88.0, lower_rate=86.0, upper_rate=90.0,
            floor_rate=None, base_amount=100_000_000.0, n_customers=3,
            historical_cases=cases,
        )
        bids_o, *_ = generate_customer_bids(
            predicted_rate=88.0, lower_rate=86.0, upper_rate=90.0,
            floor_rate=None, base_amount=100_000_000.0, n_customers=3,
            historical_cases=cases,
            override_quantiles=[0.1, 0.5, 0.9],
        )
        self.assertEqual(len(bids_o), 3)
        # Role labelling mirrors _quantile_plan so downstream stays consistent.
        self.assertEqual([b.role for b in bids_o], ["attack", "core", "explore"])
        self.assertEqual([b.target_quantile for b in bids_o], [0.1, 0.5, 0.9])
        # Override should yield a wider spread than the default heuristic (0.18 / 0.78).
        o_spread = bids_o[-1].rate - bids_o[0].rate
        h_spread = bids_h[-1].rate - bids_h[0].rate
        self.assertGreater(o_spread, h_spread)

    def test_auto_bid_pending_uses_strategy_tables_multi_n(self) -> None:
        scope_category = "service"
        scope_method = "전자입찰"
        with connect(self.db_path) as conn:
            for idx in range(12):
                insert_case(
                    conn,
                    HistoricalBidCase(
                        notice_id=f"HIST-STX-{idx}",
                        agency_name="기관A",
                        category=scope_category,
                        contract_method=scope_method,
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
                    notice_id="PENDING-STX-1",
                    agency_name="기관A",
                    category=scope_category,
                    contract_method=scope_method,
                    region="seoul",
                    base_amount=100_000_000.0,
                    award_amount=0.0,
                    bid_rate=0.0,
                    bidder_count=0,
                    opened_at="2025-02-01",
                ),
            )
            for n, qs in [(1, [0.5]), (2, [0.3, 0.7]), (3, [0.2, 0.5, 0.8])]:
                conn.execute(
                    """
                    INSERT INTO strategy_tables
                        (agency_name, category, contract_method, region,
                         n_customers, quantiles_json, source,
                         sample_size, win_rate_estimate)
                    VALUES ('', ?, ?, '', ?, ?, 'montecarlo_v2', 100, 0.5)
                    """,
                    (scope_category, scope_method, n, json.dumps(qs)),
                )

        _auto_bid_pending(
            Namespace(
                db_path=str(self.db_path),
                category=None,
                agency=None,
                since_days=None,
                limit=10,
                top_k=5,
                num_customers=3,  # ignored when strategy_tables populated
                target_win_probability=0.75,
                dry_run=False,
            )
        )
        with connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT note, n_customers FROM mock_bids WHERE notice_id = ?",
                ("PENDING-STX-1",),
            ).fetchall()
        # 1 + 2 + 3 = 6 bids across N=1,2,3 portfolios.
        self.assertEqual(len(rows), 6)
        self.assertTrue(all(str(r["note"]).startswith("auto:strategy_v2") for r in rows))
        ns = sorted(int(r["n_customers"]) for r in rows)
        self.assertEqual(ns, [1, 2, 2, 3, 3, 3])

    def test_list_strategy_table_rows_parses_quantiles(self) -> None:
        with connect(self.db_path) as conn:
            for n, qs in [(1, [0.5]), (3, [0.2, 0.5, 0.8])]:
                conn.execute(
                    """
                    INSERT INTO strategy_tables
                        (agency_name, category, contract_method, region,
                         n_customers, quantiles_json, source,
                         sample_size, win_rate_estimate)
                    VALUES ('', 'goods', '전자입찰', '', ?, ?, 'montecarlo_v2', 42, 0.6)
                    """,
                    (n, json.dumps(qs)),
                )
        rows = list_strategy_table_rows(self.db_path)
        self.assertEqual(len(rows), 2)
        rows_by_n = {r["n_customers"]: r for r in rows}
        self.assertEqual(rows_by_n[1]["quantiles"], [0.5])
        self.assertEqual(rows_by_n[3]["quantiles"], [0.2, 0.5, 0.8])
        self.assertEqual(rows_by_n[3]["source"], "montecarlo_v2")
        self.assertNotIn("quantiles_json", rows_by_n[1])

    def test_summarize_evaluations_by_scope_n_computes_win_rate(self) -> None:
        # Seed a notice so JOIN resolves, plus 5 mock_bids with verdicts.
        with connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO bid_notices (notice_id, agency_name, category,
                    contract_method, region, base_amount, opened_at)
                VALUES ('EVAL-N-1', '기관X', 'service', '전자입찰', 'seoul',
                        100000000, '2025-03-01')
                """
            )
            for i in range(5):
                conn.execute(
                    """
                    INSERT INTO mock_bids (notice_id, bid_amount, bid_rate,
                        note, simulation_id, customer_idx, n_customers)
                    VALUES ('EVAL-N-1', ?, ?, 'auto:strategy_v2', 'sim-1', ?, 5)
                    """,
                    (90_000_000 + i, 90.0 + i * 0.1, i + 1),
                )
            # 3 won, 2 lost for (service/전자입찰, N=5)
            for mock_id, verdict in ((1, "won"), (2, "won"), (3, "won"), (4, "lost"), (5, "lost")):
                conn.execute(
                    """
                    INSERT INTO mock_bid_evaluations (mock_id, notice_id,
                        simulation_id, customer_idx, bid_amount, bid_rate,
                        verdict, n_customers)
                    VALUES (?, 'EVAL-N-1', 'sim-1', ?, 0, 0, ?, 5)
                    """,
                    (mock_id, mock_id, verdict),
                )
        out = summarize_evaluations_by_scope_n(self.db_path)
        self.assertEqual(len(out), 1)
        row = out[0]
        self.assertEqual(row["category"], "service")
        self.assertEqual(row["contract_method"], "전자입찰")
        self.assertEqual(row["n_customers"], 5)
        self.assertEqual(row["wins"], 3)
        self.assertEqual(row["decided"], 5)
        self.assertAlmostEqual(row["observed_win_rate"], 0.6)


if __name__ == "__main__":
    unittest.main()
