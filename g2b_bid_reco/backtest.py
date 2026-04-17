from __future__ import annotations

import statistics
from typing import Callable

from .agency_analysis import AgencyRangeAnalyzer
from .db import (
    get_actual_award,
    get_notice_snapshot,
    load_historical_cases_for_notice,
    sample_awarded_notice_ids,
)
from .models import (
    ActualAwardOutcome,
    BacktestReport,
    BatchBacktestSummary,
    NoticePredictionReport,
)
from .notice_prediction import NoticePredictor


def run_batch_backtest(
    db_path: str,
    category: str,
    sample_size: int,
    seed: int | None = None,
    worst_case_keep: int = 5,
    log: Callable[[str], None] | None = None,
) -> BatchBacktestSummary:
    notice_ids = sample_awarded_notice_ids(db_path, category, sample_size, seed=seed)

    reports: list[BacktestReport] = []
    skipped_no_peer = 0
    for notice_id in notice_ids:
        notice = get_notice_snapshot(db_path, notice_id)
        if notice is None:
            continue
        if not notice.agency_name or not notice.contract_method or notice.base_amount <= 0:
            continue

        actual = get_actual_award(db_path, notice_id)
        if actual is None or actual.award_amount <= 0 or actual.bid_rate <= 0:
            continue

        cases = load_historical_cases_for_notice(db_path, notice_id, notice.opened_at)
        analyzer = AgencyRangeAnalyzer(cases)
        prediction = NoticePredictor(analyzer).predict(notice)

        if prediction.analysis.peer_case_count == 0:
            skipped_no_peer += 1
            continue

        reports.append(build_backtest_report(prediction, actual))
        if log is not None and len(reports) % 25 == 0:
            log(f"[batch-backtest] progressed {len(reports)}/{len(notice_ids)}")

    successful = len(reports)
    hit_count = sum(1 for report in reports if report.actual_within_range)
    hit_rate = round(hit_count / successful, 4) if successful else 0.0
    rate_gaps = [report.rate_gap_pp for report in reports]
    abs_rate_gaps = [abs(value) for value in rate_gaps]
    abs_amount_gap_ratios = [
        abs(report.amount_gap_ratio)
        for report in reports
        if report.amount_gap_ratio is not None
    ]

    confidence_breakdown: dict[str, int] = {}
    for report in reports:
        confidence_breakdown[report.analysis_confidence] = (
            confidence_breakdown.get(report.analysis_confidence, 0) + 1
        )

    worst_cases = sorted(
        reports,
        key=lambda r: abs(r.rate_gap_pp),
        reverse=True,
    )[:worst_case_keep]

    return BatchBacktestSummary(
        category=category,
        sample_size=len(notice_ids),
        successful=successful,
        skipped_no_peer=skipped_no_peer,
        hit_count=hit_count,
        hit_rate=hit_rate,
        mean_rate_gap_pp=_mean(rate_gaps),
        median_rate_gap_pp=_median(rate_gaps),
        mean_abs_rate_gap_pp=_mean(abs_rate_gaps),
        median_abs_rate_gap_pp=_median(abs_rate_gaps),
        mean_abs_amount_gap_ratio=_mean(abs_amount_gap_ratios),
        median_abs_amount_gap_ratio=_median(abs_amount_gap_ratios),
        confidence_breakdown=confidence_breakdown,
        worst_cases=worst_cases,
    )


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return round(statistics.mean(values), 4)


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    return round(statistics.median(values), 4)


def build_backtest_report(
    prediction: NoticePredictionReport,
    actual: ActualAwardOutcome,
) -> BacktestReport:
    notice = prediction.notice
    analysis = prediction.analysis

    predicted_rate = analysis.blended_rate
    predicted_amount = analysis.recommended_amount
    rate_gap_pp = round(actual.bid_rate - predicted_rate, 3)
    actual_within_range = analysis.lower_rate <= actual.bid_rate <= analysis.upper_rate

    # Goods results sometimes store a per-unit `sucsfbidAmt` instead of the
    # contract-level award, while `bid_rate` comes through reliably. Recompute
    # the effective award amount from the base × rate so everything downstream
    # (gap, charts) shows the apples-to-apples total.
    if notice.base_amount and notice.base_amount > 0 and actual.bid_rate > 0:
        effective_actual_amount = round(notice.base_amount * actual.bid_rate / 100.0, 2)
    else:
        effective_actual_amount = actual.award_amount

    if predicted_amount is not None and predicted_amount > 0:
        amount_gap = round(effective_actual_amount - predicted_amount, 2)
        amount_gap_ratio = round(
            (effective_actual_amount - predicted_amount) / predicted_amount, 4
        )
    else:
        amount_gap = None
        amount_gap_ratio = None

    return BacktestReport(
        notice_id=notice.notice_id,
        agency_name=notice.agency_name,
        category=notice.category,
        contract_method=notice.contract_method,
        region=notice.region,
        base_amount=notice.base_amount,
        predicted_rate=predicted_rate,
        predicted_lower_rate=analysis.lower_rate,
        predicted_upper_rate=analysis.upper_rate,
        predicted_amount=predicted_amount,
        actual_rate=actual.bid_rate,
        actual_amount=effective_actual_amount,
        rate_gap_pp=rate_gap_pp,
        amount_gap=amount_gap,
        amount_gap_ratio=amount_gap_ratio,
        actual_within_range=actual_within_range,
        analysis_confidence=analysis.confidence,
        agency_case_count=analysis.agency_case_count,
        peer_case_count=analysis.peer_case_count,
        lookback_years_used=analysis.lookback_years_used,
        analysis_notes=list(analysis.notes),
    )
