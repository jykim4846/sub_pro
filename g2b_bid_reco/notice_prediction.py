from __future__ import annotations

from .agency_analysis import AgencyRangeAnalyzer
from .models import AgencyRangeRequest, BidNoticeSnapshot, NoticePredictionReport


class NoticePredictor:
    def __init__(self, analyzer: AgencyRangeAnalyzer) -> None:
        self.analyzer = analyzer

    def predict(self, notice: BidNoticeSnapshot) -> NoticePredictionReport:
        analysis = self.analyzer.analyze(
            AgencyRangeRequest(
                agency_name=notice.agency_name,
                category=notice.category,
                contract_method=notice.contract_method,
                region=notice.region,
                base_amount=notice.base_amount,
                floor_rate=notice.floor_rate,
            )
        )
        analysis.notes.insert(0, "현재 공고 자체는 제외하고 이전 유사 사례만 반영했습니다.")
        return NoticePredictionReport(notice=notice, analysis=analysis)
