import tempfile
import unittest
from pathlib import Path

from g2b_bid_reco.db import connect, get_notice_snapshot, init_db, insert_case, load_historical_cases_for_notice
from g2b_bid_reco.notice_prediction import NoticePredictor
from g2b_bid_reco.sample_data import SAMPLE_CASES
from g2b_bid_reco.agency_analysis import AgencyRangeAnalyzer


class NoticePredictionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "bids.db"
        init_db(self.db_path)
        with connect(self.db_path) as conn:
            for case in SAMPLE_CASES:
                insert_case(conn, case)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_predict_notice_excludes_current_notice_from_history(self) -> None:
        notice = get_notice_snapshot(self.db_path, "R25BK000029-000")
        self.assertIsNotNone(notice)
        cases = load_historical_cases_for_notice(self.db_path, notice.notice_id, notice.opened_at)
        self.assertTrue(all(case.notice_id != notice.notice_id for case in cases))

        predictor = NoticePredictor(AgencyRangeAnalyzer(cases))
        report = predictor.predict(notice)

        self.assertEqual(report.notice.notice_id, "R25BK000029-000")
        self.assertGreaterEqual(report.analysis.agency_case_count, 5)
        self.assertGreater(report.analysis.blended_rate, 87.9)
        self.assertIn("현재 공고 자체", report.analysis.notes[0])


if __name__ == "__main__":
    unittest.main()
