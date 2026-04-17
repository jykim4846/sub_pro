import json
import tempfile
import unittest
from pathlib import Path

from g2b_bid_reco.api import PPSCollector, PublicDataPortalClient, build_collect_query
from g2b_bid_reco.db import connect, init_db


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

    def test_build_collect_query_adds_order_months_for_plans(self) -> None:
        query = build_collect_query("plans", "202601010000", "202603312359", 50)

        self.assertEqual(query["orderBgnYm"], "202601")
        self.assertEqual(query["orderEndYm"], "202603")
        self.assertEqual(query["numOfRows"], 50)


if __name__ == "__main__":
    unittest.main()
