from __future__ import annotations

from math import sqrt
from statistics import mean

from .models import BidRecommendation, BidRecommendationRequest, EvidenceItem, HistoricalBidCase


class BidRecommender:
    def __init__(self, cases: list[HistoricalBidCase]) -> None:
        self.cases = cases

    def recommend(self, request: BidRecommendationRequest) -> BidRecommendation:
        weighted_cases = self._score_cases(request)
        if not weighted_cases:
            return self._fallback(request)

        rates = [case.bid_rate for case, _ in weighted_cases]
        weights = [weight for _, weight in weighted_cases]
        weighted_average = self._weighted_average(rates, weights)
        spread = self._weighted_spread(rates, weights)

        target_rate = weighted_average
        notes: list[str] = []

        if request.floor_rate is not None:
            floor_adjusted = max(target_rate, request.floor_rate + 0.08)
            if floor_adjusted != target_rate:
                notes.append("낙찰하한율 기준으로 추천치를 상향 보정했습니다.")
            target_rate = floor_adjusted

        if request.bidder_count is not None and request.bidder_count >= 35:
            target_rate -= 0.015
            notes.append("경쟁 강도가 높다고 가정해 추천 투찰률을 소폭 낮췄습니다.")

        lower_rate = max(target_rate - max(spread, 0.03), 0)
        upper_rate = target_rate + max(spread, 0.03)

        strong_matches = sum(1 for _, weight in weighted_cases if weight >= 4.0)
        confidence = self._confidence_label(len(weighted_cases), strong_matches)

        evidence = [
            EvidenceItem(
                notice_id=case.notice_id,
                agency_name=case.agency_name,
                bid_rate=case.bid_rate,
                award_amount=case.award_amount,
                bidder_count=case.bidder_count,
                similarity_score=round(weight, 3),
            )
            for case, weight in weighted_cases[:5]
        ]

        if strong_matches < 3:
            notes.append("동일 기관 또는 동일 조건 사례가 충분하지 않아 유사 사례를 함께 사용했습니다.")

        return BidRecommendation(
            recommended_rate=round(target_rate, 3),
            recommended_amount=round(request.base_amount * (target_rate / 100), 2),
            lower_rate=round(lower_rate, 3),
            upper_rate=round(upper_rate, 3),
            confidence=confidence,
            notes=notes,
            evidence=evidence,
        )

    def _score_cases(self, request: BidRecommendationRequest) -> list[tuple[HistoricalBidCase, float]]:
        scored: list[tuple[HistoricalBidCase, float]] = []
        for case in self.cases:
            if case.category != request.category:
                continue
            score = 0.0

            if case.contract_method == request.contract_method:
                score += 3.0
            else:
                continue

            if case.agency_name == request.agency_name:
                score += 4.0
            if case.region == request.region:
                score += 1.5

            amount_gap_ratio = abs(case.base_amount - request.base_amount) / max(request.base_amount, 1)
            score += max(0.0, 2.0 - amount_gap_ratio * 4.0)

            if score >= 3.5:
                scored.append((case, score))

        scored.sort(key=lambda item: item[1], reverse=True)
        return scored

    @staticmethod
    def _weighted_average(values: list[float], weights: list[float]) -> float:
        total_weight = sum(weights)
        return sum(value * weight for value, weight in zip(values, weights)) / total_weight

    @staticmethod
    def _weighted_spread(values: list[float], weights: list[float]) -> float:
        avg = BidRecommender._weighted_average(values, weights)
        total_weight = sum(weights)
        variance = sum(weight * ((value - avg) ** 2) for value, weight in zip(values, weights)) / total_weight
        return sqrt(variance)

    @staticmethod
    def _confidence_label(case_count: int, strong_matches: int) -> str:
        if strong_matches >= 4 and case_count >= 5:
            return "high"
        if strong_matches >= 2 and case_count >= 3:
            return "medium"
        return "low"

    @staticmethod
    def _fallback(request: BidRecommendationRequest) -> BidRecommendation:
        base_rate = request.floor_rate + 0.1 if request.floor_rate is not None else 88.0
        return BidRecommendation(
            recommended_rate=round(base_rate, 3),
            recommended_amount=round(request.base_amount * (base_rate / 100), 2),
            lower_rate=round(base_rate - 0.05, 3),
            upper_rate=round(base_rate + 0.05, 3),
            confidence="low",
            notes=["활용 가능한 과거 사례가 없어 보수적 기본값을 사용했습니다."],
            evidence=[],
        )
