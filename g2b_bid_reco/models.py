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
    reference_date: str | None = None


@dataclass
class AgencyRangeReport:
    agency_name: str
    category: str
    contract_method: str
    region: str | None
    lookback_years_used: int | None
    agency_case_count: int
    peer_case_count: int
    blended_rate: float
    lower_rate: float
    upper_rate: float
    recommended_amount: float | None
    confidence: str
    agency_mean_rate: float | None
    peer_mean_rate: float | None
    target_win_probability: float = 0.0
    estimated_win_probability: float = 0.0
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


@dataclass
class ActualAwardOutcome:
    notice_id: str
    award_amount: float
    bid_rate: float
    bidder_count: int
    winning_company: str
    result_status: str


@dataclass
class BatchBacktestSummary:
    category: str
    sample_size: int
    successful: int
    skipped_no_peer: int
    hit_count: int
    hit_rate: float
    mean_rate_gap_pp: float | None
    median_rate_gap_pp: float | None
    mean_abs_rate_gap_pp: float | None
    median_abs_rate_gap_pp: float | None
    mean_abs_amount_gap_ratio: float | None
    median_abs_amount_gap_ratio: float | None
    confidence_breakdown: dict[str, int] = field(default_factory=dict)
    worst_cases: list["BacktestReport"] = field(default_factory=list)


@dataclass
class BacktestReport:
    notice_id: str
    agency_name: str
    category: str
    contract_method: str
    region: str
    base_amount: float
    predicted_rate: float
    predicted_lower_rate: float
    predicted_upper_rate: float
    predicted_amount: float | None
    actual_rate: float
    actual_amount: float
    rate_gap_pp: float
    amount_gap: float | None
    amount_gap_ratio: float | None
    actual_within_range: bool
    analysis_confidence: str
    agency_case_count: int
    peer_case_count: int
    lookback_years_used: int | None
    analysis_notes: list[str] = field(default_factory=list)
