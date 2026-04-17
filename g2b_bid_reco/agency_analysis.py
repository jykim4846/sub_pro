from __future__ import annotations

from math import sqrt

from .models import AgencyRangeReport, AgencyRangeRequest, EvidenceItem, HistoricalBidCase


class AgencyRangeAnalyzer:
    def __init__(self, cases: list[HistoricalBidCase], prior_strength: float = 6.0) -> None:
        self.cases = cases
        self.prior_strength = prior_strength

    def analyze(self, request: AgencyRangeRequest) -> AgencyRangeReport:
        peer_cases = self._peer_cases(request)
        agency_cases = [case for case in peer_cases if case.agency_name == request.agency_name]

        if not peer_cases:
            return self._empty_report(request)

        agency_weighted = self._weight_cases(agency_cases, request)
        peer_weighted = self._weight_cases(peer_cases, request)

        agency_mean = self._weighted_mean(agency_weighted) if agency_weighted else None
        peer_mean = self._weighted_mean(peer_weighted)
        agency_spread = self._weighted_spread(agency_weighted) if agency_weighted else None
        peer_spread = self._weighted_spread(peer_weighted)

        agency_strength = sum(weight for _, weight in agency_weighted)
        blend_weight = agency_strength / (agency_strength + self.prior_strength) if agency_strength > 0 else 0.0
        blended_rate = ((agency_mean or peer_mean) * blend_weight) + (peer_mean * (1.0 - blend_weight))

        blended_spread = self._blended_spread(agency_spread, peer_spread, blend_weight)
        min_half_width = 0.035 if len(agency_cases) >= 5 else 0.06 if len(agency_cases) >= 3 else 0.09
        half_width = max(blended_spread, min_half_width)

        notes: list[str] = []
        if len(agency_cases) < 3:
            notes.append("기관 자체 표본이 적어 동일 계약방법 peer 그룹 분포를 강하게 반영했습니다.")
        if request.floor_rate is not None and blended_rate < request.floor_rate:
            blended_rate = request.floor_rate + 0.03
            notes.append("낙찰하한율 아래로 내려가지 않도록 중심값을 상향 보정했습니다.")
        if request.base_amount is not None:
            notes.append("입력 금액과 가까운 사례에 더 높은 가중치를 부여했습니다.")
        if request.region:
            notes.append("동일 지역 사례를 우선 반영했습니다.")

        confidence = self._confidence(len(agency_cases), len(peer_cases))
        evidence = self._build_evidence(agency_weighted, peer_weighted, request.agency_name)

        return AgencyRangeReport(
            agency_name=request.agency_name,
            category=request.category,
            contract_method=request.contract_method,
            region=request.region,
            agency_case_count=len(agency_cases),
            peer_case_count=len(peer_cases),
            blended_rate=round(blended_rate, 3),
            lower_rate=round(max(blended_rate - half_width, 0), 3),
            upper_rate=round(blended_rate + half_width, 3),
            recommended_amount=round(request.base_amount * (blended_rate / 100), 2) if request.base_amount is not None else None,
            confidence=confidence,
            agency_mean_rate=round(agency_mean, 3) if agency_mean is not None else None,
            peer_mean_rate=round(peer_mean, 3),
            notes=notes,
            evidence=evidence,
        )

    def _peer_cases(self, request: AgencyRangeRequest) -> list[HistoricalBidCase]:
        peers: list[HistoricalBidCase] = []
        for case in self.cases:
            if case.category != request.category:
                continue
            if case.contract_method != request.contract_method:
                continue
            if request.region and case.region != request.region:
                continue
            peers.append(case)
        return peers

    @staticmethod
    def _weight_cases(cases: list[HistoricalBidCase], request: AgencyRangeRequest) -> list[tuple[HistoricalBidCase, float]]:
        weighted: list[tuple[HistoricalBidCase, float]] = []
        for case in cases:
            weight = 1.0
            if request.region and case.region == request.region:
                weight += 0.5
            if request.base_amount is not None and request.base_amount > 0:
                gap_ratio = abs(case.base_amount - request.base_amount) / request.base_amount
                weight += max(0.0, 2.0 - gap_ratio * 4.0)
            weighted.append((case, weight))
        weighted.sort(key=lambda item: item[1], reverse=True)
        return weighted

    @staticmethod
    def _weighted_mean(weighted_cases: list[tuple[HistoricalBidCase, float]]) -> float:
        total_weight = sum(weight for _, weight in weighted_cases)
        return sum(case.bid_rate * weight for case, weight in weighted_cases) / total_weight

    @staticmethod
    def _weighted_spread(weighted_cases: list[tuple[HistoricalBidCase, float]]) -> float:
        avg = AgencyRangeAnalyzer._weighted_mean(weighted_cases)
        total_weight = sum(weight for _, weight in weighted_cases)
        variance = sum(weight * ((case.bid_rate - avg) ** 2) for case, weight in weighted_cases) / total_weight
        return sqrt(variance)

    @staticmethod
    def _blended_spread(agency_spread: float | None, peer_spread: float, blend_weight: float) -> float:
        if agency_spread is None:
            return peer_spread
        return (agency_spread * blend_weight) + (peer_spread * (1.0 - blend_weight))

    @staticmethod
    def _confidence(agency_case_count: int, peer_case_count: int) -> str:
        if agency_case_count >= 5:
            return "high"
        if agency_case_count >= 3:
            return "medium"
        if peer_case_count >= 8:
            return "medium"
        return "low"

    @staticmethod
    def _build_evidence(
        agency_weighted: list[tuple[HistoricalBidCase, float]],
        peer_weighted: list[tuple[HistoricalBidCase, float]],
        agency_name: str,
    ) -> list[EvidenceItem]:
        evidence: list[EvidenceItem] = []
        seen_notice_ids: set[str] = set()
        for case, weight in agency_weighted[:3] + peer_weighted[:5]:
            if case.notice_id in seen_notice_ids:
                continue
            evidence.append(
                EvidenceItem(
                    notice_id=case.notice_id,
                    agency_name=case.agency_name,
                    bid_rate=case.bid_rate,
                    award_amount=case.award_amount,
                    bidder_count=case.bidder_count,
                    similarity_score=round(weight + (1.0 if case.agency_name == agency_name else 0.0), 3),
                )
            )
            seen_notice_ids.add(case.notice_id)
            if len(evidence) >= 5:
                break
        return evidence

    @staticmethod
    def _empty_report(request: AgencyRangeRequest) -> AgencyRangeReport:
        notes = ["동일 업무구분/계약방법에 해당하는 과거 사례가 아직 없습니다."]
        fallback_rate = request.floor_rate + 0.05 if request.floor_rate is not None else 88.0
        return AgencyRangeReport(
            agency_name=request.agency_name,
            category=request.category,
            contract_method=request.contract_method,
            region=request.region,
            agency_case_count=0,
            peer_case_count=0,
            blended_rate=round(fallback_rate, 3),
            lower_rate=round(max(fallback_rate - 0.08, 0), 3),
            upper_rate=round(fallback_rate + 0.08, 3),
            recommended_amount=round(request.base_amount * (fallback_rate / 100), 2) if request.base_amount is not None else None,
            confidence="low",
            agency_mean_rate=None,
            peer_mean_rate=None,
            notes=notes,
            evidence=[],
        )
