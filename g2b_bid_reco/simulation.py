from __future__ import annotations

import bisect
import math
import statistics
from dataclasses import dataclass
from datetime import datetime

from .models import HistoricalBidCase


@dataclass
class CompetitorSpec:
    biz_no: str
    company_name: str
    historical_rates: list[float]
    wins: int = 0


@dataclass
class CustomerBid:
    idx: int
    rate: float
    amount: float
    role: str = "core"
    target_quantile: float | None = None


@dataclass
class SimulationReport:
    notice_id: str
    base_amount: float
    floor_rate: float | None
    predicted_rate: float | None
    predicted_amount: float | None
    lower_rate: float | None
    upper_rate: float | None
    customers: list[CustomerBid]
    competitors: list[CompetitorSpec]
    num_runs: int
    our_wins: int
    our_win_rate: float
    mean_winning_rate_when_we_win: float | None
    mean_winning_amount_when_we_win: float | None
    best_customer_idx: int | None
    best_customer_win_rate: float | None
    market_center: float | None = None
    market_spread: float | None = None
    market_drift: float | None = None
    uncertainty_score: float = 0.0
    strategy_name: str = "trend-aware-quantile"


def _clip(rate: float, floor: float | None) -> float:
    lower = floor if floor is not None and floor > 0 else 0.0
    return max(lower, min(110.0, rate))


def _safe_mean(values: list[float]) -> float | None:
    # 10x faster than statistics.mean for float inputs. For typical bid-rate
    # magnitudes (0..110) the two agree to float precision (~1e-13).
    if not values:
        return None
    return sum(values) / len(values)


def _safe_spread(values: list[float]) -> float:
    n = len(values)
    if n <= 1:
        return 0.0
    # Population stdev with a single pass; avoids statistics.pstdev's
    # as_integer_ratio path which dominates the profiler for big scopes.
    mean_v = sum(values) / n
    acc = 0.0
    for v in values:
        diff = v - mean_v
        acc += diff * diff
    return math.sqrt(acc / n)


def _parse_opened_at(value: str | None) -> datetime:
    text = (value or "").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y%m%d%H%M", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return datetime.min


def _quantile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    q = max(0.0, min(1.0, q))
    pos = q * (len(ordered) - 1)
    lo = int(pos)
    hi = min(len(ordered) - 1, lo + 1)
    if lo == hi:
        return ordered[lo]
    frac = pos - lo
    return ordered[lo] + (ordered[hi] - ordered[lo]) * frac


def _linspace(start: float, stop: float, count: int) -> list[float]:
    if count <= 0:
        return []
    if count == 1:
        return [round((start + stop) / 2.0, 4)]
    step = (stop - start) / (count - 1)
    return [round(start + step * i, 4) for i in range(count)]


def _trend_adjusted_market_rates(
    historical_cases: list[HistoricalBidCase],
    floor_rate: float | None,
    historical_rates_opened_asc: list[float] | None = None,
) -> tuple[list[float], float | None, float | None, float]:
    if historical_rates_opened_asc is not None:
        rates = [rate for rate in reversed(historical_rates_opened_asc) if 0 < rate <= 110]
        if not rates:
            return ([], None, None, 0.0)
        recent_count = min(max(6, len(rates) // 3), len(rates))
        recent_rates = rates[:recent_count]
        long_center = _safe_mean(rates)
        recent_center = _safe_mean(recent_rates)
        if long_center is None or recent_center is None:
            return (rates, long_center, _safe_spread(rates), 0.0)

        drift = max(-0.35, min(0.35, recent_center - long_center))
        total = max(1, len(rates) - 1)
        lower_bound = floor_rate if floor_rate is not None and floor_rate > 0 else 0.0
        # Inlined clip + age weighting. Removes ~1M function-call frames
        # compared to the per-element `_clip(...)` version observed in profiling.
        step = drift / total if total else 0.0
        adjusted: list[float] = [0.0] * len(rates)
        for idx in range(len(rates)):
            v = rates[idx] + step * idx
            if v < lower_bound:
                v = lower_bound
            elif v > 110.0:
                v = 110.0
            adjusted[idx] = v
        return (adjusted, recent_center, _safe_spread(recent_rates), drift)

    if not historical_cases:
        return ([], None, None, 0.0)
    ordered_cases = sorted(
        [case for case in historical_cases if 0 < case.bid_rate <= 110],
        key=lambda case: _parse_opened_at(case.opened_at),
        reverse=True,
    )
    if not ordered_cases:
        return ([], None, None, 0.0)

    rates = [case.bid_rate for case in ordered_cases]
    recent_count = min(max(6, len(rates) // 3), len(rates))
    recent_rates = rates[:recent_count]
    long_center = _safe_mean(rates)
    recent_center = _safe_mean(recent_rates)
    if long_center is None or recent_center is None:
        return (rates, long_center, _safe_spread(rates), 0.0)

    drift = max(-0.35, min(0.35, recent_center - long_center))
    total = max(1, len(ordered_cases) - 1)
    step = drift / total if total else 0.0
    lower_bound = floor_rate if floor_rate is not None and floor_rate > 0 else 0.0
    adjusted: list[float] = [0.0] * len(ordered_cases)
    for idx in range(len(ordered_cases)):
        v = ordered_cases[idx].bid_rate + step * idx
        if v < lower_bound:
            v = lower_bound
        elif v > 110.0:
            v = 110.0
        adjusted[idx] = v
    return (adjusted, recent_center, _safe_spread(recent_rates), drift)


def _uncertainty_score(
    historical_cases: list[HistoricalBidCase],
    market_spread: float | None,
    market_drift: float,
) -> float:
    sample_penalty = 1.0 - min(1.0, len(historical_cases) / 24.0)
    spread_penalty = min(1.0, (market_spread or 0.0) / 0.35)
    drift_penalty = min(1.0, abs(market_drift) / 0.25)
    return round((sample_penalty * 0.45) + (spread_penalty * 0.25) + (drift_penalty * 0.30), 3)


def _quantile_plan(n_customers: int, uncertainty: float) -> list[tuple[str, float]]:
    if n_customers <= 0:
        return []
    if n_customers == 1:
        return [("core", 0.45)]

    attack_q = max(0.06, 0.18 - (uncertainty * 0.10))
    explore_q = min(0.94, 0.78 + (uncertainty * 0.10))
    if n_customers == 2:
        return [("attack", round(attack_q, 4)), ("explore", round(explore_q, 4))]

    core_count = n_customers - 2
    core_start = max(attack_q + 0.10, 0.34 - (uncertainty * 0.10))
    core_end = min(explore_q - 0.10, 0.64 + (uncertainty * 0.08))
    plan = [("attack", round(attack_q, 4))]
    plan.extend(("core", q) for q in _linspace(core_start, core_end, core_count))
    plan.append(("explore", round(explore_q, 4)))
    return plan


def generate_customer_bids(
    predicted_rate: float,
    lower_rate: float,
    upper_rate: float,
    floor_rate: float | None,
    base_amount: float,
    n_customers: int,
    historical_cases: list[HistoricalBidCase],
    historical_rates_opened_asc: list[float] | None = None,
) -> tuple[list[CustomerBid], float | None, float | None, float, list[float]]:
    market_rates, market_center, market_spread, market_drift = _trend_adjusted_market_rates(
        historical_cases, floor_rate, historical_rates_opened_asc
    )
    if not market_rates:
        market_rates = [predicted_rate]
        market_center = predicted_rate
        market_spread = max(0.05, (upper_rate - lower_rate) / 2.0)
        market_drift = 0.0

    uncertainty = _uncertainty_score(historical_cases, market_spread, market_drift)
    epsilon = 0.018 + (0.022 * uncertainty)
    upper_guard = max(predicted_rate + 0.08, upper_rate + 0.03)
    floor_guard = (floor_rate + 0.005) if floor_rate is not None and floor_rate > 0 else 0.0

    bids: list[CustomerBid] = []
    previous_rate = floor_guard - 0.005
    for idx, (role, target_q) in enumerate(_quantile_plan(n_customers, uncertainty), start=1):
        anchor = _quantile(market_rates, target_q)
        rate = _clip(min(anchor - epsilon, upper_guard), floor_rate)
        if rate <= previous_rate:
            rate = _clip(previous_rate + 0.005, floor_rate)
        previous_rate = rate
        amount = round(base_amount * rate / 100.0, 0)
        bids.append(
            CustomerBid(
                idx=idx,
                rate=round(rate, 4),
                amount=amount,
                role=role,
                target_quantile=round(target_q, 4),
            )
        )
    return bids, market_center, market_spread, uncertainty, market_rates


def run_simulation(
    notice_id: str,
    base_amount: float,
    floor_rate: float | None,
    predicted_rate: float,
    lower_rate: float,
    upper_rate: float,
    predicted_amount: float | None,
    competitors: list[CompetitorSpec],
    historical_cases: list[HistoricalBidCase],
    n_customers: int,
    historical_rates_opened_asc: list[float] | None = None,
) -> SimulationReport:
    customers, market_center, market_spread, uncertainty, market_rates = generate_customer_bids(
        predicted_rate=predicted_rate,
        lower_rate=lower_rate,
        upper_rate=upper_rate,
        floor_rate=floor_rate,
        base_amount=base_amount,
        n_customers=n_customers,
        historical_cases=historical_cases,
        historical_rates_opened_asc=historical_rates_opened_asc,
    )
    if not customers or base_amount <= 0:
        return SimulationReport(
            notice_id=notice_id,
            base_amount=base_amount,
            floor_rate=floor_rate,
            predicted_rate=predicted_rate,
            predicted_amount=predicted_amount,
            lower_rate=lower_rate,
            upper_rate=upper_rate,
            customers=customers,
            competitors=competitors,
            num_runs=0,
            our_wins=0,
            our_win_rate=0.0,
            mean_winning_rate_when_we_win=None,
            mean_winning_amount_when_we_win=None,
            best_customer_idx=None,
            best_customer_win_rate=None,
            market_center=market_center,
            market_spread=market_spread,
            market_drift=None,
            uncertainty_score=uncertainty,
        )

    per_customer_wins = [0] * len(customers)
    winning_rates: list[float] = []
    winning_amounts: list[float] = []
    customer_rates = [customer.rate for customer in customers]

    for competitor_rate in market_rates:
        winner_idx = bisect.bisect_left(customer_rates, competitor_rate) - 1
        if winner_idx < 0:
            continue
        winner = customers[winner_idx]
        per_customer_wins[winner.idx - 1] += 1
        winning_rates.append(winner.rate)
        winning_amounts.append(winner.amount)

    total_scenarios = len(market_rates)
    our_wins = sum(per_customer_wins)
    best_idx = None
    best_rate = None
    if per_customer_wins:
        max_idx = max(range(len(per_customer_wins)), key=lambda i: per_customer_wins[i])
        if per_customer_wins[max_idx] > 0:
            best_idx = max_idx + 1
            best_rate = per_customer_wins[max_idx] / total_scenarios if total_scenarios else 0.0

    drift = None
    if market_center is not None:
        if historical_rates_opened_asc is not None:
            long_center = _safe_mean([rate for rate in historical_rates_opened_asc if 0 < rate <= 110])
        else:
            long_center = _safe_mean([case.bid_rate for case in historical_cases if 0 < case.bid_rate <= 110])
        if long_center is not None:
            drift = round(market_center - long_center, 4)

    return SimulationReport(
        notice_id=notice_id,
        base_amount=base_amount,
        floor_rate=floor_rate,
        predicted_rate=predicted_rate,
        predicted_amount=predicted_amount,
        lower_rate=lower_rate,
        upper_rate=upper_rate,
        customers=customers,
        competitors=competitors,
        num_runs=total_scenarios,
        our_wins=our_wins,
        our_win_rate=(our_wins / total_scenarios) if total_scenarios else 0.0,
        mean_winning_rate_when_we_win=_safe_mean(winning_rates),
        mean_winning_amount_when_we_win=_safe_mean(winning_amounts),
        best_customer_idx=best_idx,
        best_customer_win_rate=best_rate,
        market_center=market_center,
        market_spread=market_spread,
        market_drift=drift,
        uncertainty_score=uncertainty,
    )
