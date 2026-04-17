import tempfile
import unittest
from pathlib import Path

from g2b_bid_reco.db import connect, init_db, insert_case, load_historical_cases
from g2b_bid_reco.models import BidRecommendationRequest
from g2b_bid_reco.recommender import BidRecommender
from g2b_bid_reco.sample_data import SAMPLE_CASES


class BidRecommenderTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "bids.db"
        init_db(self.db_path)
        with connect(self.db_path) as conn:
            for case in SAMPLE_CASES:
                insert_case(conn, case)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_recommendation_uses_matching_agency_cases(self) -> None:
        cases = load_historical_cases(self.db_path)
        recommender = BidRecommender(cases)
        request = BidRecommendationRequest(
            agency_name="한국출판문화산업진흥원",
            category="service",
            contract_method="적격심사",
            region="seoul",
            base_amount=240_000_000,
            floor_rate=87.745,
            bidder_count=33,
        )

        result = recommender.recommend(request)

        self.assertGreaterEqual(result.recommended_rate, 87.90)
        self.assertLessEqual(result.recommended_rate, 88.05)
        self.assertEqual(result.confidence, "high")
        self.assertTrue(result.evidence)

    def test_floor_rate_is_respected_when_data_is_lower(self) -> None:
        cases = load_historical_cases(self.db_path)
        recommender = BidRecommender(cases)
        request = BidRecommendationRequest(
            agency_name="축산물품질평가원",
            category="service",
            contract_method="적격심사",
            region="daejeon",
            base_amount=180_000_000,
            floor_rate=88.10,
            bidder_count=20,
        )

        result = recommender.recommend(request)

        self.assertGreaterEqual(result.recommended_rate, 88.18)
        self.assertIn("상향 보정", " ".join(result.notes))

    def test_fallback_is_returned_when_no_cases_match(self) -> None:
        cases = load_historical_cases(self.db_path)
        recommender = BidRecommender(cases)
        request = BidRecommendationRequest(
            agency_name="없는기관",
            category="construction",
            contract_method="종합심사",
            region="busan",
            base_amount=500_000_000,
            floor_rate=86.745,
        )

        result = recommender.recommend(request)

        self.assertEqual(result.confidence, "low")
        self.assertFalse(result.evidence)
        self.assertGreater(result.recommended_rate, 86.745)


if __name__ == "__main__":
    unittest.main()
