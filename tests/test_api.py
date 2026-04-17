import io
import json
import tempfile
import unittest
import urllib.error
from pathlib import Path

from g2b_bid_reco.api import PPSCollector, PublicDataPortalClient, build_collect_query
from g2b_bid_reco.db import connect, init_db


class _FlakyOpener:
    def __init__(self, http_errors: int, final_body: str) -> None:
        self.http_errors = http_errors
        self.final_body = final_body
        self.calls = 0

    def __call__(self, url: str) -> str:
        self.calls += 1
        if self.http_errors > 0:
            self.http_errors -= 1
            raise urllib.error.HTTPError(
                url=url, code=429, msg="Too Many Requests", hdrs=None, fp=io.BytesIO(b"")
            )
        return self.final_body


class ApiCollectorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "bids.db"
        init_db(self.db_path)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_collect_notices_normalizes_and_upserts(self) -> None:
        payload = {
            "response": {
                "header": {"resultCode": "00", "resultMsg": "NORMAL SERVICE"},
                "body": {
                    "items": {
                        "item": [
                            {
                                "bidNtceNo": "20260000123",
                                "bidNtceOrd": "0",
                                "dminsttNm": "테스트기관",
                                "cntrctMthdNm": "적격심사",
                                "prtcptPsblRgnNm": "seoul",
                                "presmptPrce": "120000000",
                                "asignBdgtAmt": "130000000",
                                "sucsfbidLwltRate": "87.745",
                                "bidNtceDt": "202601021000",
                            }
                        ]
                    },
                    "totalCount": 1,
                },
            }
        }

        client = PublicDataPortalClient("dummy", opener=lambda _: json.dumps(payload))
        collector = PPSCollector(client=client, db_path=str(self.db_path))
        result = collector.collect(
            source="notices",
            category="service",
            query=build_collect_query("notices", "202601010000", "202601312359", 100),
            max_pages=1,
            endpoint_override="http://example.com",
        )

        self.assertEqual(result.items_upserted, 1)
        with connect(self.db_path) as conn:
            row = conn.execute("SELECT * FROM bid_notices WHERE notice_id = ?", ("20260000123-000",)).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["agency_name"], "테스트기관")
        self.assertEqual(row["category"], "service")
        self.assertEqual(row["contract_method"], "적격심사")
        self.assertEqual(row["base_amount"], 120000000.0)

    def test_collect_results_creates_stub_notice_when_missing(self) -> None:
        payload = {
            "response": {
                "header": {"resultCode": "00"},
                "body": {
                    "items": {
                        "item": [
                            {
                                "bidNtceNo": "20260000999",
                                "bidNtceOrd": "1",
                                "sucsfbidAmt": "99887766",
                                "sucsfbidRate": "87.951",
                                "prtcptCnum": "42",
                                "sucsfbidprsnCmpyNm": "낙찰기업",
                                "opengRsltDivNm": "awarded",
                            }
                        ]
                    },
                    "totalCount": 1,
                },
            }
        }

        client = PublicDataPortalClient("dummy", opener=lambda _: json.dumps(payload))
        collector = PPSCollector(client=client, db_path=str(self.db_path))
        result = collector.collect(
            source="results",
            category="goods",
            query=build_collect_query("results", "202601010000", "202601312359", 100),
            max_pages=1,
            endpoint_override="http://example.com",
        )

        self.assertEqual(result.items_upserted, 1)
        with connect(self.db_path) as conn:
            notice = conn.execute("SELECT * FROM bid_notices WHERE notice_id = ?", ("20260000999-001",)).fetchone()
            row = conn.execute("SELECT * FROM bid_results WHERE notice_id = ?", ("20260000999-001",)).fetchone()
        self.assertIsNotNone(notice)
        self.assertIsNotNone(row)
        self.assertEqual(row["winning_company"], "낙찰기업")
        self.assertEqual(row["award_amount"], 99887766.0)

    def test_ingest_result_enriches_stub_notice_with_agency_and_derived_base(self) -> None:
        payload = {
            "response": {
                "header": {"resultCode": "00"},
                "body": {
                    "items": {
                        "item": [
                            {
                                "bidNtceNo": "R25ABC00001",
                                "bidNtceOrd": "000",
                                "dminsttNm": "수자원공사",
                                "sucsfbidAmt": "90000000",
                                "sucsfbidRate": "90",
                                "rlOpengDt": "2025-02-01 10:00:00",
                                "bidwinnrNm": "낙찰사",
                            }
                        ]
                    },
                    "totalCount": 1,
                },
            }
        }

        client = PublicDataPortalClient("dummy", opener=lambda _: json.dumps(payload))
        collector = PPSCollector(client=client, db_path=str(self.db_path))
        collector.collect(
            source="results",
            category="service",
            query=build_collect_query("results", "202502010000", "202502282359", 100),
            max_pages=1,
            endpoint_override="http://example.com",
        )

        with connect(self.db_path) as conn:
            notice = conn.execute(
                "SELECT * FROM bid_notices WHERE notice_id = ?", ("R25ABC00001-000",)
            ).fetchone()
        self.assertIsNotNone(notice)
        self.assertEqual(notice["agency_name"], "수자원공사")
        self.assertEqual(notice["category"], "service")
        self.assertAlmostEqual(notice["base_amount"], 100000000.0, places=0)
        self.assertEqual(notice["opened_at"], "2025-02-01 10:00:00")

    def test_ingest_result_does_not_overwrite_authoritative_notice_data(self) -> None:
        notice_payload = {
            "response": {
                "header": {"resultCode": "00"},
                "body": {
                    "items": {
                        "item": [
                            {
                                "bidNtceNo": "R25ABC00002",
                                "bidNtceOrd": "000",
                                "dminsttNm": "원래기관명",
                                "cntrctMthdNm": "적격심사",
                                "prtcptPsblRgnNm": "seoul",
                                "presmptPrce": "50000000",
                                "bidNtceDt": "2025-01-10 09:00:00",
                            }
                        ]
                    },
                    "totalCount": 1,
                },
            }
        }
        result_payload = {
            "response": {
                "header": {"resultCode": "00"},
                "body": {
                    "items": {
                        "item": [
                            {
                                "bidNtceNo": "R25ABC00002",
                                "bidNtceOrd": "000",
                                "dminsttNm": "다른기관명",
                                "sucsfbidAmt": "44000000",
                                "sucsfbidRate": "88",
                            }
                        ]
                    },
                    "totalCount": 1,
                },
            }
        }
        responses = iter([json.dumps(notice_payload), json.dumps(result_payload)])
        client = PublicDataPortalClient("dummy", opener=lambda _: next(responses))
        collector = PPSCollector(client=client, db_path=str(self.db_path))
        collector.collect(
            source="notices",
            category="service",
            query=build_collect_query("notices", "202501010000", "202501312359", 100),
            max_pages=1,
            endpoint_override="http://example.com",
        )
        collector.collect(
            source="results",
            category="service",
            query=build_collect_query("results", "202501010000", "202501312359", 100),
            max_pages=1,
            endpoint_override="http://example.com",
        )

        with connect(self.db_path) as conn:
            notice = conn.execute(
                "SELECT * FROM bid_notices WHERE notice_id = ?", ("R25ABC00002-000",)
            ).fetchone()
        self.assertEqual(notice["agency_name"], "원래기관명")
        self.assertEqual(notice["base_amount"], 50000000.0)
        self.assertEqual(notice["contract_method"], "적격심사")

    def test_enrich_stub_notices_fills_contract_method_from_detail_api(self) -> None:
        result_payload = {
            "response": {
                "header": {"resultCode": "00"},
                "body": {
                    "items": {
                        "item": [
                            {
                                "bidNtceNo": "R25ABC00003",
                                "bidNtceOrd": "000",
                                "dminsttNm": "",
                                "sucsfbidAmt": "80000000",
                                "sucsfbidRate": "80",
                            }
                        ]
                    },
                    "totalCount": 1,
                },
            }
        }
        detail_payload = {
            "response": {
                "header": {"resultCode": "00"},
                "body": {
                    "items": {
                        "item": [
                            {
                                "bidNtceNo": "R25ABC00003",
                                "bidNtceOrd": "000",
                                "dminsttNm": "상세기관",
                                "cntrctCnclsMthdNm": "제한경쟁",
                                "prtcptPsblRgnNm": "busan",
                                "presmptPrce": "100000000",
                                "sucsfbidLwltRate": "87.7",
                                "bidNtceDt": "2025-03-01 10:00:00",
                            }
                        ]
                    },
                    "totalCount": 1,
                },
            }
        }
        responses = iter([json.dumps(result_payload), json.dumps(detail_payload)])
        client = PublicDataPortalClient("dummy", opener=lambda _: next(responses))
        collector = PPSCollector(client=client, db_path=str(self.db_path))
        collector.collect(
            source="results",
            category="service",
            query=build_collect_query("results", "202503010000", "202503312359", 100),
            max_pages=1,
            endpoint_override="http://example.com",
        )

        enrich_result = collector.enrich_stub_notices(category="service")

        self.assertEqual(enrich_result.attempted, 1)
        self.assertEqual(enrich_result.matched, 1)
        self.assertEqual(enrich_result.enriched, 1)
        with connect(self.db_path) as conn:
            notice = conn.execute(
                "SELECT * FROM bid_notices WHERE notice_id = ?", ("R25ABC00003-000",)
            ).fetchone()
        self.assertEqual(notice["agency_name"], "상세기관")
        self.assertEqual(notice["contract_method"], "제한경쟁")
        self.assertEqual(notice["region"], "busan")
        self.assertEqual(notice["base_amount"], 100000000.0)
        self.assertEqual(notice["floor_rate"], 87.7)

    def test_client_retries_on_429_http_errors(self) -> None:
        payload = json.dumps(
            {
                "response": {
                    "header": {"resultCode": "00"},
                    "body": {"items": {"item": []}, "totalCount": 0},
                }
            }
        )
        flaky = _FlakyOpener(http_errors=2, final_body=payload)
        sleep_calls: list[float] = []

        client = PublicDataPortalClient(
            "dummy",
            opener=flaky,
            max_retries=5,
            base_backoff_sec=0.0,
            sleep=sleep_calls.append,
        )
        items, pages = client.fetch_items(
            "http://example.com",
            {"pageNo": 1, "numOfRows": 10, "inqryDiv": "1", "type": "json"},
            max_pages=1,
        )

        self.assertEqual(items, [])
        self.assertEqual(pages, 1)
        self.assertEqual(flaky.calls, 3)
        self.assertEqual(len(sleep_calls), 2)

    def test_build_collect_query_adds_order_months_for_plans(self) -> None:
        query = build_collect_query("plans", "202601010000", "202603312359", 50)

        self.assertEqual(query["orderBgnYm"], "202601")
        self.assertEqual(query["orderEndYm"], "202603")
        self.assertEqual(query["numOfRows"], 50)


if __name__ == "__main__":
    unittest.main()
