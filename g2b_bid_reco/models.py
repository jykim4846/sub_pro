from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class HistoricalBidCase:
    notice_id: str
    agency_name: str
    category: str
    contract_method: str
    region: str
    base_amount: float
    award_amount: float
    bid_rate: float
    bidder_count: int
    opened_at: str
    winning_company: str = ""


@dataclass
class BidRecommendationRequest:
    agency_name: str
    category: str
    contract_method: str
    region: str
    base_amount: float
    floor_rate: float | None = None
    bidder_count: int | None = None


@dataclass
class EvidenceItem:
    notice_id: str
    agency_name: str
    bid_rate: float
    award_amount: float
    bidder_count: int
    similarity_score: float


@dataclass
class BidRecommendation:
    recommended_rate: float
    recommended_amount: float
    lower_rate: float
    upper_rate: float
    confidence: str
    notes: list[str] = field(default_factory=list)
    evidence: list[EvidenceItem] = field(default_factory=list)


@dataclass
class AgencyRangeRequest:
    agency_name: str
    category: str
    contract_method: str
    region: str | None = None
    base_amount: float | None = None
    floor_rate: float | None = None


@dataclass
class AgencyRangeReport:
    agency_name: str
    category: str
    contract_method: str
    region: str | None
    agency_case_count: int
    peer_case_count: int
    blended_rate: float
    lower_rate: float
    upper_rate: float
    recommended_amount: float | None
    confidence: str
    agency_mean_rate: float | None
    peer_mean_rate: float | None
    notes: list[str] = field(default_factory=list)
    evidence: list[EvidenceItem] = field(default_factory=list)


@dataclass
class BidNoticeSnapshot:
    notice_id: str
    agency_name: str
    category: str
    contract_method: str
    region: str
    base_amount: float
    floor_rate: float | None
    opened_at: str | None


@dataclass
class NoticePredictionReport:
    notice: BidNoticeSnapshot
    analysis: AgencyRangeReport
