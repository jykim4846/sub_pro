import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from g2b_bid_reco.api import PPSCollector, PublicDataPortalClient, month_windows, months_between
from g2b_bid_reco.db import init_db


class BackfillTest(unittest.TestCase):
    def test_month_windows_builds_recent_month_ranges(self) -> None:
        windows = month_windows(3, end=datetime(2026, 4, 17, 13, 30))

        self.assertEqual(
            windows,
            [
                ("202602010000", "202602282359"),
                ("202603010000", "202603312359"),
                ("202604010000", "202604171330"),
            ],
        )

    def test_months_between_spans_start_to_end_inclusive(self) -> None:
        windows = months_between(
            start=datetime(2026, 2, 15, 9, 0),
            end=datetime(2026, 4, 17, 13, 30),
        )
        self.assertEqual(
            windows,
            [
                ("202602150900", "202602282359"),
                ("202603010000", "202603312359"),
                ("202604010000", "202604171330"),
            ],
        )

    def test_months_between_empty_when_start_after_end(self) -> None:
        self.assertEqual(
            months_between(
                start=datetime(2026, 5, 1),
                end=datetime(2026, 4, 30),
            ),
            [],
        )

    def test_collect_between_runs_per_month(self) -> None:
        payload = {
            "response": {
                "header": {"resultCode": "00"},
                "body": {"items": {"item": []}, "totalCount": 0},
            }
        }
        with tempfile.TemporaryDirectory() as tempdir:
            db_path = Path(tempdir) / "bids.db"
            init_db(db_path)
            client = PublicDataPortalClient("dummy", opener=lambda _: json.dumps(payload))
            collector = PPSCollector(client=client, db_path=str(db_path))

            result = collector.collect_between(
                category="service",
                sources=["notices", "results"],
                start=datetime(2026, 2, 15, 9, 0),
                end=datetime(2026, 4, 17, 13, 30),
                page_size=50,
                max_pages_per_window=2,
            )

        self.assertEqual(result.months, 3)
        self.assertEqual(len(result.windows), 6)
        self.assertEqual(result.total_items_seen, 0)

    def test_backfill_aggregates_window_results(self) -> None:
        payload = {
            "response": {
                "header": {"resultCode": "00"},
                "body": {
                    "items": {"item": []},
                    "totalCount": 0,
                },
            }
        }

        with tempfile.TemporaryDirectory() as tempdir:
            db_path = Path(tempdir) / "bids.db"
            init_db(db_path)
            client = PublicDataPortalClient("dummy", opener=lambda _: json.dumps(payload))
            collector = PPSCollector(client=client, db_path=str(db_path))

            result = collector.backfill_recent_months(
                category="service",
                sources=["notices", "results"],
                months=2,
                page_size=50,
                max_pages_per_window=3,
                end=datetime(2026, 4, 17, 13, 30),
            )

        self.assertEqual(result.months, 2)
        self.assertEqual(len(result.windows), 4)
        self.assertEqual(result.total_items_seen, 0)
        self.assertEqual(result.total_items_upserted, 0)


if __name__ == "__main__":
    unittest.main()
