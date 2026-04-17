from __future__ import annotations

import random
import statistics
from dataclasses import dataclass, field


@dataclass
class CompetitorSpec:
    biz_no: str
    company_name: str
    historical_rates: list[float]
    wins: int = 0

    def sample_rate(self, rng: random.Random) -> float:
        if not self.historical_rates:
            return 100.0
        if len(self.historical_rates) <= 2:
            base = rng.choice(self.historical_rates)
            return max(50.0, min(110.0, base + rng.normalvariate(0, 0.3)))
        m = statistics.mean(self.historical_rates)
        s = max(0.2, statistics.pstdev(self.historical_rates))
        return max(50.0, min(110.0, rng.normalvariate(m, s)))


@dataclass
class CustomerBid:
    idx: int
    rate: float
    amount: float


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


def _clip(rate: float, floor: float | None) -> float:
    if floor and floor > 0 and rate < floor:
        return floor
    return max(0.0, rate)


def generate_customer_bids(
    predicted_rate: float,
    lower_rate: float,
    upper_rate: float,
    floor_rate: float | None,
    base_amount: float,
    n_customers: int,
    seed: int,
) -> list[CustomerBid]:
    rng = random.Random(seed)
    spread = max(0.6, (upper_rate - lower_rate))
    out: list[CustomerBid] = []
    for i in range(n_customers):
        t = (i + 0.5) / max(1, n_customers)
        z = (t - 0.5) * 2.0  # [-1, +1]
        rate = predicted_rate + z * (spread / 2.0)
        rate += rng.normalvariate(0, 0.15)
        rate = _clip(rate, floor_rate)
        amount = round(base_amount * rate / 100.0, 0)
        out.append(CustomerBid(idx=i + 1, rate=round(rate, 4), amount=amount))
    return out


def run_simulation(
    notice_id: str,
    base_amount: float,
    floor_rate: float | None,
    predicted_rate: float,
    lower_rate: float,
    upper_rate: float,
    predicted_amount: float | None,
    competitors: list[CompetitorSpec],
    n_customers: int,
    num_runs: int,
    seed: int = 42,
) -> SimulationReport:
    customers = generate_customer_bids(
        predicted_rate, lower_rate, upper_rate, floor_rate,
        base_amount, n_customers, seed,
    )
    if not customers or base_amount <= 0:
        return SimulationReport(
            notice_id=notice_id, base_amount=base_amount, floor_rate=floor_rate,
            predicted_rate=predicted_rate, predicted_amount=predicted_amount,
            lower_rate=lower_rate, upper_rate=upper_rate,
            customers=customers, competitors=competitors, num_runs=0,
            our_wins=0, our_win_rate=0.0,
            mean_winning_rate_when_we_win=None,
            mean_winning_amount_when_we_win=None,
            best_customer_idx=None, best_customer_win_rate=None,
        )
    per_customer_wins = [0] * len(customers)
    our_winning_rates: list[float] = []
    our_winning_amounts: list[float] = []
    our_wins = 0
    rng = random.Random(seed + 97)
    for _ in range(num_runs):
        comp_amounts: list[float] = []
        for comp in competitors:
            r = comp.sample_rate(rng)
            if floor_rate and floor_rate > 0 and r < floor_rate:
                continue
            comp_amounts.append(base_amount * r / 100.0)
        comp_min = min(comp_amounts) if comp_amounts else None
        # Lowest customer bid is our representative
        our_winner = min(customers, key=lambda x: x.amount)
        if comp_min is None or our_winner.amount < comp_min:
            our_wins += 1
            per_customer_wins[our_winner.idx - 1] += 1
            our_winning_rates.append(our_winner.rate)
            our_winning_amounts.append(our_winner.amount)
    best_idx = None
    best_rate = None
    if per_customer_wins:
        max_idx = max(range(len(per_customer_wins)), key=lambda i: per_customer_wins[i])
        if per_customer_wins[max_idx] > 0:
            best_idx = max_idx + 1
            best_rate = per_customer_wins[max_idx] / num_runs if num_runs else 0.0
    return SimulationReport(
        notice_id=notice_id, base_amount=base_amount, floor_rate=floor_rate,
        predicted_rate=predicted_rate, predicted_amount=predicted_amount,
        lower_rate=lower_rate, upper_rate=upper_rate,
        customers=customers, competitors=competitors,
        num_runs=num_runs, our_wins=our_wins,
        our_win_rate=our_wins / num_runs if num_runs else 0.0,
        mean_winning_rate_when_we_win=(
            statistics.mean(our_winning_rates) if our_winning_rates else None
        ),
        mean_winning_amount_when_we_win=(
            statistics.mean(our_winning_amounts) if our_winning_amounts else None
        ),
        best_customer_idx=best_idx,
        best_customer_win_rate=best_rate,
    )
