"""Streamlit dashboard: predict-vs-actual for one agency's past awards.

Run:
    streamlit run dashboard.py

Install the extras once:
    pip install -e ".[dashboard]"
"""
from __future__ import annotations

import os
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from g2b_bid_reco.agency_analysis import AgencyRangeAnalyzer
from g2b_bid_reco.backtest import build_backtest_report
from g2b_bid_reco.db import (
    add_suggestion,
    auto_generate_suggestions,
    compute_weekly_metrics,
    connect,
    delete_mock_bid,
    get_actual_award,
    get_notice_snapshot,
    list_agencies_with_backtestable_notices,
    list_agency_parent_mappings,
    list_metrics_snapshots,
    list_mock_bids,
    list_simulation_ids,
    list_suggestions,
    load_backtestable_notices_for_agency,
    load_cases_for_agencies,
    load_historical_cases_for_notice,
    load_pending_notices_for_prediction,
    resolve_adaptive_agencies,
    revenue_summary,
    save_mock_bid,
    save_mock_bid_batch,
    seed_agency_parent_mapping,
    take_weekly_snapshot,
    top_winners_for_scope,
    update_agency_parent_status,
    update_suggestion,
)
from g2b_bid_reco.simulation import CompetitorSpec, run_simulation
from g2b_bid_reco.models import ActualAwardOutcome, BidNoticeSnapshot
from g2b_bid_reco.notice_prediction import NoticePredictor

G2B_TASK_CL_CD = {"goods": "5", "service": "20", "construction": "3"}


def _build_g2b_detail_url(notice_id: str, category: str) -> str:
    parts = notice_id.rsplit("-", 1)
    bid_no = parts[0]
    bid_ord = parts[1] if len(parts) == 2 else "000"
    task = G2B_TASK_CL_CD.get(category, "")
    return (
        "https://www.g2b.go.kr:8101/ep/invitation/publish/bidInfoDtl.do"
        f"?bidno={bid_no}&bidseq={bid_ord}&releaseYn=Y&taskClCd={task}"
    )


def _build_search_fallback_url(notice_id: str) -> str:
    return f"https://www.google.com/search?q={notice_id}+site:g2b.go.kr"


def _format_amount(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "–"
    try:
        return f"{float(value):,.0f}"
    except (TypeError, ValueError):
        return "–"


def _format_rate(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "–"
    try:
        return f"{float(value):,.3f}"
    except (TypeError, ValueError):
        return "–"


def _format_pct(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "–"
    try:
        return f"{float(value) * 100:.0f}%"
    except (TypeError, ValueError):
        return "–"

DEFAULT_DB_PATH = os.environ.get("G2B_DB_PATH", "data/bids.db")
CATEGORY_KEY_TO_LABEL = {
    "service": "용역",
    "goods": "물품",
    "construction": "공사",
}
CATEGORY_DROPDOWN = [
    ("(전체)", None),
    ("용역 (service)", "service"),
    ("물품 (goods)", "goods"),
    ("공사 (construction)", "construction"),
]


def _floor_rate_meaningful(floor_rate: float | None) -> bool:
    return floor_rate is not None and floor_rate > 0


def _humanize_category(value: str | None) -> str:
    if not value:
        return ""
    return CATEGORY_KEY_TO_LABEL.get(value, value)


def _run_prediction(
    db_path: str,
    notice: BidNoticeSnapshot,
    actual: ActualAwardOutcome,
    target_win_probability: float,
):
    cases, parent_used = _load_cases_adaptive(
        db_path, notice.notice_id, notice.agency_name, notice.category,
        notice.contract_method, notice.opened_at,
    )
    analyzer = AgencyRangeAnalyzer(cases, target_win_probability=target_win_probability)
    prediction = NoticePredictor(analyzer).predict(notice)
    if parent_used:
        prediction.analysis.notes.append(
            f"sparse-agency fallback: 부모 '{parent_used}'로 확장된 풀로 예측했습니다."
        )
    return prediction, build_backtest_report(prediction, actual)


def _load_cases_adaptive(
    db_path: str,
    notice_id: str,
    agency_name: str,
    category: str,
    contract_method: str,
    opened_at,
):
    agency_list, parent_used = resolve_adaptive_agencies(
        db_path, agency_name, category=category, contract_method=contract_method,
    )
    if parent_used:
        cases = load_cases_for_agencies(
            db_path, agency_list,
            category=category, contract_method=contract_method,
            cutoff_opened_at=opened_at, exclude_notice_id=notice_id,
        )
        # Fallback: expand to full category+method peer pool for non-agency peers.
        peer = load_historical_cases_for_notice(
            db_path, notice_id, opened_at, category=category, contract_method=contract_method,
        )
        # Merge: include both (dedupe by notice_id)
        seen = {c.notice_id for c in cases}
        for c in peer:
            if c.notice_id not in seen:
                cases.append(c)
        return cases, parent_used
    cases = load_historical_cases_for_notice(
        db_path, notice_id, opened_at,
        category=category, contract_method=contract_method,
    )
    return cases, None


def _win_possible(report, floor_rate: float | None) -> bool:
    if report.predicted_amount is None or report.predicted_amount <= 0:
        return False
    if report.predicted_amount > report.actual_amount:
        return False
    if _floor_rate_meaningful(floor_rate) and report.predicted_rate < floor_rate:
        return False
    return True


@st.cache_data(show_spinner="과거 데이터 기반 예측을 계산 중...")
def _load_rows_for_agency(
    db_path: str,
    agency_name: str,
    category: str | None,
    target_win_probability: float,
) -> pd.DataFrame:
    pairs = load_backtestable_notices_for_agency(db_path, agency_name, category=category, limit=100)
    rows: list[dict] = []
    for notice, actual in pairs:
        try:
            _prediction, report = _run_prediction(db_path, notice, actual, target_win_probability)
        except Exception as exc:  # noqa: BLE001 — dashboard must stay up
            rows.append({
                "notice_id": notice.notice_id,
                "opened_at": notice.opened_at,
                "category": notice.category,
                "contract_method": notice.contract_method,
                "base_amount": notice.base_amount,
                "actual_amount": actual.award_amount,
                "actual_rate": actual.bid_rate,
                "predicted_amount": None,
                "predicted_rate": None,
                "predicted_lower_rate": None,
                "predicted_upper_rate": None,
                "floor_rate": notice.floor_rate,
                "win_possible": False,
                "actual_within_range": False,
                "confidence": "error",
                "agency_cases": 0,
                "peer_cases": 0,
                "target_win_probability": target_win_probability,
                "estimated_win_probability": 0.0,
                "error": str(exc),
            })
            continue

        rows.append({
            "notice_id": notice.notice_id,
            "opened_at": notice.opened_at,
            "category": notice.category,
            "contract_method": notice.contract_method,
            "base_amount": notice.base_amount,
            "actual_amount": report.actual_amount,
            "actual_rate": report.actual_rate,
            "predicted_amount": report.predicted_amount,
            "predicted_rate": report.predicted_rate,
            "predicted_lower_rate": report.predicted_lower_rate,
            "predicted_upper_rate": report.predicted_upper_rate,
            "floor_rate": notice.floor_rate,
            "win_possible": _win_possible(report, notice.floor_rate),
            "actual_within_range": report.actual_within_range,
            "confidence": report.analysis_confidence,
            "agency_cases": report.agency_case_count,
            "peer_cases": report.peer_case_count,
            "target_win_probability": _prediction.analysis.target_win_probability,
            "estimated_win_probability": _prediction.analysis.estimated_win_probability,
            "error": "",
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["opened_at"] = pd.to_datetime(df["opened_at"], errors="coerce")
        df = df.sort_values("opened_at").reset_index(drop=True)
    return df


def _render_summary(df: pd.DataFrame) -> None:
    total = len(df)
    if total == 0:
        return
    predictable = df[df["predicted_amount"].notna()]
    wins = int(predictable["win_possible"].sum())
    predictable_n = len(predictable)
    mean_win = (
        float(predictable["estimated_win_probability"].mean())
        if predictable_n and "estimated_win_probability" in predictable
        else 0.0
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("공고 수", total)
    c2.metric("예측 가능", predictable_n)
    c3.metric(
        "낙찰 가능했을 비율",
        f"{(wins / predictable_n * 100):.1f}%" if predictable_n else "–",
        help="predicted_amount ≤ actual_amount AND predicted_rate ≥ floor_rate",
    )
    c4.metric(
        "평균 추정 낙찰확률",
        f"{mean_win * 100:.1f}%" if predictable_n else "–",
        help="예측 투찰가로 썼을 때 과거 유사 사례 기준 낙찰 확률 추정치의 평균",
    )


def _render_chart(df: pd.DataFrame) -> None:
    if df.empty:
        return

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df["opened_at"],
            y=df["base_amount"],
            name="예산(base_amount)",
            mode="lines+markers",
            line=dict(color="#6c757d", dash="dot"),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df["opened_at"],
            y=df["actual_amount"],
            name="실제 낙찰가",
            mode="lines+markers",
            line=dict(color="#1f77b4"),
        )
    )
    predictable = df[df["predicted_amount"].notna()]
    if not predictable.empty:
        fig.add_trace(
            go.Scatter(
                x=predictable["opened_at"],
                y=predictable["predicted_amount"],
                name="예측 투찰가",
                mode="lines+markers",
                line=dict(color="#ff7f0e", dash="dash"),
            )
        )

    win = predictable[predictable["win_possible"]]
    if not win.empty:
        fig.add_trace(
            go.Scatter(
                x=win["opened_at"],
                y=win["predicted_amount"],
                name="낙찰 가능",
                mode="markers",
                marker=dict(symbol="star", size=12, color="#2ca02c"),
            )
        )
    lose = predictable[~predictable["win_possible"]]
    if not lose.empty:
        fig.add_trace(
            go.Scatter(
                x=lose["opened_at"],
                y=lose["predicted_amount"],
                name="낙찰 불가",
                mode="markers",
                marker=dict(symbol="x", size=11, color="#d62728"),
            )
        )

    fig.update_layout(
        hovermode="x unified",
        xaxis_title="공고 개찰일",
        yaxis_title="금액 (원)",
        height=520,
        margin=dict(l=30, r=20, t=30, b=30),
    )
    fig.update_yaxes(tickformat=",")
    fig.update_traces(hovertemplate="%{x|%Y-%m-%d} · %{y:,.0f}원<extra>%{fullData.name}</extra>")
    st.plotly_chart(fig, use_container_width=True)


def _render_table(df: pd.DataFrame) -> None:
    if df.empty:
        st.info("표시할 공고가 없습니다.")
        return

    def _format_ratio(row):
        if row["predicted_amount"] is None or row["predicted_amount"] == 0:
            return "–"
        ratio = (row["actual_amount"] - row["predicted_amount"]) / row["predicted_amount"]
        return f"{ratio * 100:+.2f}%"

    sort_options = {
        "개찰일 최신순": ("opened_at", False),
        "개찰일 오래된순": ("opened_at", True),
        "예산 큰 순": ("base_amount", False),
        "실제낙찰가 큰 순": ("actual_amount", False),
        "예측-실제 gap 큰 순": ("abs_amount_gap", False),
    }
    sort_label = st.selectbox("테이블 정렬", list(sort_options.keys()), index=0)
    sort_col, ascending = sort_options[sort_label]

    working = df.copy()
    working["abs_amount_gap"] = (
        working["actual_amount"].fillna(0) - working["predicted_amount"].fillna(0)
    ).abs()

    working = working.sort_values(sort_col, ascending=ascending, na_position="last").reset_index(drop=True)

    def _format_amount(value) -> str:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return "–"
        try:
            return f"{float(value):,.0f}"
        except (TypeError, ValueError):
            return "–"

    def _format_rate(value) -> str:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return "–"
        try:
            return f"{float(value):,.3f}"
        except (TypeError, ValueError):
            return "–"

    def _format_pct(value) -> str:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return "–"
        try:
            return f"{float(value) * 100:.0f}%"
        except (TypeError, ValueError):
            return "–"

    display = working.assign(
        win_badge=working["win_possible"].map({True: "✅ 가능", False: "❌ 불가"}),
        range_badge=working["actual_within_range"].map({True: "✅", False: "❌"}),
        amount_gap_pct=working.apply(_format_ratio, axis=1),
        category=working["category"].map(_humanize_category),
        base_amount=working["base_amount"].map(_format_amount),
        actual_amount=working["actual_amount"].map(_format_amount),
        predicted_amount=working["predicted_amount"].map(_format_amount),
        predicted_rate=working["predicted_rate"].map(_format_rate),
        actual_rate=working["actual_rate"].map(_format_rate),
        floor_rate=working["floor_rate"].map(_format_rate),
        est_win=working.get("estimated_win_probability", pd.Series(dtype=float)).map(_format_pct),
    )[
        [
            "opened_at",
            "notice_id",
            "category",
            "contract_method",
            "base_amount",
            "actual_amount",
            "predicted_amount",
            "predicted_rate",
            "actual_rate",
            "floor_rate",
            "amount_gap_pct",
            "est_win",
            "win_badge",
            "range_badge",
            "confidence",
            "agency_cases",
            "peer_cases",
        ]
    ]
    display.columns = [
        "개찰일", "공고번호", "구분", "계약방법",
        "예산", "실제낙찰가", "예측투찰가",
        "예측률(%)", "실제률(%)", "하한율(%)",
        "실제-예측 gap", "추정 낙찰확률",
        "낙찰가능", "범위적중",
        "신뢰도", "기관사례", "peer사례",
    ]
    st.dataframe(
        display,
        hide_index=True,
        use_container_width=True,
        height=min(600, 52 + 35 * max(1, len(display))),
    )


@st.cache_data(show_spinner="진행 중 공고들의 예측 투찰가를 계산 중...")
def _load_pending_rows(
    db_path: str,
    category: str | None,
    agency_name: str | None,
    since_days: int,
    limit: int,
    target_win_probability: float,
) -> pd.DataFrame:
    notices = load_pending_notices_for_prediction(
        db_path=db_path,
        category=category,
        agency_name=agency_name,
        since_days=since_days,
        limit=limit,
    )
    rows: list[dict] = []
    for notice in notices:
        try:
            cases, parent_used = _load_cases_adaptive(
                db_path, notice.notice_id, notice.agency_name, notice.category,
                notice.contract_method, notice.opened_at,
            )
            analyzer = AgencyRangeAnalyzer(cases, target_win_probability=target_win_probability)
            prediction = NoticePredictor(analyzer).predict(notice)
            if parent_used:
                prediction.analysis.notes.append(
                    f"sparse-agency fallback: 부모 '{parent_used}'로 확장된 풀로 예측했습니다."
                )
            analysis = prediction.analysis
            predicted_amount = analysis.recommended_amount
            predicted_rate = analysis.blended_rate
            est_win = analysis.estimated_win_probability
            lookback = analysis.lookback_years_used
            agency_cases = analysis.agency_case_count
            peer_cases = analysis.peer_case_count
            confidence = analysis.confidence
        except Exception:
            predicted_amount = None
            predicted_rate = None
            est_win = 0.0
            lookback = None
            agency_cases = 0
            peer_cases = 0
            confidence = "error"

        rows.append({
            "opened_at": notice.opened_at,
            "notice_id": notice.notice_id,
            "category": notice.category,
            "agency_name": notice.agency_name,
            "contract_method": notice.contract_method,
            "region": notice.region,
            "base_amount": notice.base_amount,
            "floor_rate": notice.floor_rate,
            "predicted_rate": predicted_rate,
            "predicted_amount": predicted_amount,
            "estimated_win_probability": est_win,
            "agency_cases": agency_cases,
            "peer_cases": peer_cases,
            "lookback_years_used": lookback,
            "confidence": confidence,
            "detail_url": _build_g2b_detail_url(notice.notice_id, notice.category),
            "search_url": _build_search_fallback_url(notice.notice_id),
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df["opened_at"] = pd.to_datetime(df["opened_at"], errors="coerce")
        df = df.sort_values("opened_at", ascending=False).reset_index(drop=True)
    return df


def _render_live_view(db_path: str, target_win_probability: float) -> None:
    st.subheader("진행 중 / 낙찰 미확정 공고")
    st.caption(
        "아직 낙찰 결과가 연결되지 않은 공고들에 대해 즉시 예측 투찰가를 계산합니다. "
        "상세 링크는 나라장터 공고 페이지로 연결됩니다(로그인 필요할 수 있음)."
    )

    with st.container(border=True):
        c1, c2, c3 = st.columns([1, 1, 1])
        with c1:
            category_label = st.selectbox(
                "구분",
                [label for label, _ in CATEGORY_DROPDOWN],
                index=0,
                key="live_category",
            )
        with c2:
            since_days = st.number_input(
                "최근 N일 공고",
                min_value=7,
                max_value=365,
                value=60,
                step=7,
                key="live_since_days",
            )
        with c3:
            limit = st.number_input(
                "최대 표시 건수",
                min_value=10,
                max_value=1000,
                value=30,
                step=10,
                key="live_limit",
                help="너무 큰 값은 예측 계산 시간이 오래 걸립니다. 처음엔 30~50 권장.",
            )

        agency_filter = st.text_input(
            "기관명 (선택, 정확히 일치해야 필터 적용)",
            key="live_agency",
            help="특정 기관만 보고 싶을 때 정확한 이름을 입력하세요. 부분 일치는 지원하지 않습니다.",
        )

    category = dict(CATEGORY_DROPDOWN)[category_label]

    run_key = (category_label, int(since_days), int(limit), agency_filter.strip(), target_win_probability)
    if st.session_state.get("live_last_run_key") != run_key:
        if not st.button("🔮 예측 계산 실행", key="live_run_btn"):
            st.info(
                "Live 예측은 공고마다 과거 이력을 스캔해서 시간이 걸립니다. "
                "위 조건을 확인한 뒤 버튼을 눌러 계산을 시작하세요."
            )
            return
        st.session_state["live_last_run_key"] = run_key

    df = _load_pending_rows(
        db_path=db_path,
        category=category,
        agency_name=agency_filter.strip() or None,
        since_days=int(since_days),
        limit=int(limit),
        target_win_probability=target_win_probability,
    )

    if df.empty:
        st.info("조건에 맞는 진행 중 공고가 없습니다. 기간/구분/기관명을 조정해 보세요.")
        return

    def _format_amount(value) -> str:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return "–"
        try:
            return f"{float(value):,.0f}"
        except (TypeError, ValueError):
            return "–"

    def _format_rate(value) -> str:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return "–"
        try:
            return f"{float(value):,.3f}"
        except (TypeError, ValueError):
            return "–"

    def _format_pct(value) -> str:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return "–"
        try:
            return f"{float(value) * 100:.0f}%"
        except (TypeError, ValueError):
            return "–"

    display = df.assign(
        category=df["category"].map(_humanize_category),
        base_amount=df["base_amount"].map(_format_amount),
        predicted_amount=df["predicted_amount"].map(_format_amount),
        predicted_rate=df["predicted_rate"].map(_format_rate),
        floor_rate=df["floor_rate"].map(_format_rate),
        est_win=df["estimated_win_probability"].map(_format_pct),
    )[
        [
            "opened_at",
            "notice_id",
            "category",
            "agency_name",
            "contract_method",
            "base_amount",
            "predicted_amount",
            "predicted_rate",
            "floor_rate",
            "est_win",
            "confidence",
            "agency_cases",
            "peer_cases",
            "detail_url",
        ]
    ]
    display.columns = [
        "개찰일", "공고번호", "구분", "기관", "계약방법",
        "예산", "예측투찰가", "예측률(%)", "하한율(%)",
        "추정 낙찰확률", "신뢰도", "기관사례", "peer사례",
        "나라장터 링크",
    ]

    selection_event = st.dataframe(
        display,
        hide_index=True,
        use_container_width=True,
        height=min(700, 52 + 35 * max(1, len(display))),
        on_select="rerun",
        selection_mode="single-row",
        key="live_notice_table",
        column_config={
            "나라장터 링크": st.column_config.LinkColumn(
                "나라장터 공고",
                display_text="🔗 공고 열기",
                help="나라장터 공고 상세 URL (taskClCd가 맞지 않으면 열리지 않을 수 있음)",
            ),
        },
    )

    selected_indices = []
    try:
        selected_indices = selection_event.selection.rows  # type: ignore[attr-defined]
    except Exception:
        selected_indices = []

    if not selected_indices:
        st.info("위 표에서 공고 행을 클릭하면 해당 기관의 과거 낙찰 추이와 분포가 아래에 펼쳐집니다.")
        return

    selected_row = df.iloc[int(selected_indices[0])].to_dict()
    _render_notice_detail(db_path, selected_row)


def _render_notice_detail(db_path: str, row: dict) -> None:
    st.markdown("---")
    opened = row.get("opened_at")
    opened_s = opened.strftime("%Y-%m-%d") if hasattr(opened, "strftime") else str(opened)
    predicted_amount = row.get("predicted_amount") or 0
    predicted_rate = row.get("predicted_rate")
    est_win = row.get("estimated_win_probability") or 0

    st.subheader(f"📂 {row['agency_name']} 과거 발주·낙찰")
    st.caption(
        f"선택 공고: {row['notice_id']} · 개찰일 {opened_s} · 계약방법 {row['contract_method']} · "
        f"예산 {row['base_amount']:,.0f}원 · 예측 투찰가 {predicted_amount:,.0f}원 · "
        f"추정 낙찰확률 {est_win * 100:.0f}%"
    )

    cutoff = opened.strftime("%Y-%m-%d %H:%M:%S") if hasattr(opened, "strftime") else opened
    cases = load_historical_cases_for_notice(
        db_path, row["notice_id"], cutoff, agency_name=row["agency_name"]
    )
    same_agency_all = [c for c in cases if c.agency_name == row["agency_name"]]
    same_agency = [
        c for c in same_agency_all if c.contract_method == row["contract_method"]
    ]

    if not same_agency_all:
        st.info(
            "이 기관의 과거 낙찰 사례가 아직 DB에 없습니다. "
            "그래서 아래 예측은 동일 계약방법의 다른 기관(peer) 분포에서 역산됐습니다."
        )
        return

    only_same_method = st.checkbox(
        f"이 공고와 같은 계약방법(`{row['contract_method']}`)만 보기",
        value=True,
        key=f"live_detail_same_method_{row['notice_id']}",
        help=(
            "체크 해제하면 이 기관의 모든 계약방법 과거 공고가 포함됩니다. "
            "계약방법이 다르면 낙찰률 패턴이 달라질 수 있으니 참고용으로만 보세요."
        ),
    )
    shown_cases = same_agency if only_same_method else same_agency_all

    if not shown_cases:
        st.info("선택한 조건에 해당하는 이 기관의 과거 공고가 없습니다.")
        return

    avg_rate = sum(c.bid_rate for c in shown_cases) / len(shown_cases)
    total_award = sum(c.award_amount for c in shown_cases)
    total_base = sum(c.base_amount for c in shown_cases if c.base_amount)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("과거 공고 수", f"{len(shown_cases)}건")
    c2.metric("평균 낙찰률", f"{avg_rate:.2f}%")
    c3.metric("낙찰 총액", f"{total_award:,.0f}원")
    c4.metric("예가 총액", f"{total_base:,.0f}원")

    st.markdown("#### 📈 이 기관의 과거 발주 · 낙찰 흐름")
    st.caption(
        "회색 점선이 **예가(발주 금액)**, 파란 선이 **실제 낙찰가**, 주황 수평선이 이번 공고에서 우리가 추천한 "
        "**예측 투찰가**입니다. 실제 낙찰가가 예가 아래에 붙는 각도/폭으로 이 기관의 입찰 관행을 볼 수 있습니다."
    )

    past_df = pd.DataFrame(
        [
            {
                "opened_at": _parse_datetime(c.opened_at),
                "notice_id": c.notice_id,
                "contract_method": c.contract_method,
                "base_amount": c.base_amount,
                "award_amount": c.award_amount,
                "bid_rate": c.bid_rate,
                "bidder_count": c.bidder_count,
                "winning_company": c.winning_company,
                "region": c.region,
            }
            for c in shown_cases
        ]
    ).dropna(subset=["opened_at"]).sort_values("opened_at")

    if past_df.empty:
        st.info("시간 정보가 있는 과거 공고가 없어 차트를 그릴 수 없습니다.")
    else:
        flow_fig = go.Figure()
        flow_fig.add_trace(
            go.Scatter(
                x=past_df["opened_at"],
                y=past_df["base_amount"],
                name="예가(발주 금액)",
                mode="lines+markers",
                line=dict(color="#9aa0a6", dash="dot"),
                hovertemplate="%{x|%Y-%m-%d}<br>예가 %{y:,.0f}원<br>%{customdata}",
                customdata=past_df["notice_id"],
            )
        )
        flow_fig.add_trace(
            go.Scatter(
                x=past_df["opened_at"],
                y=past_df["award_amount"],
                name="실제 낙찰가",
                mode="lines+markers",
                line=dict(color="#1f77b4"),
                hovertemplate="%{x|%Y-%m-%d}<br>낙찰가 %{y:,.0f}원<br>%{customdata}",
                customdata=past_df["notice_id"],
            )
        )
        if predicted_amount:
            flow_fig.add_hline(
                y=float(predicted_amount),
                line_dash="dash",
                line_color="#ff7f0e",
                annotation_text=(
                    f"🎯 이 공고 예측 투찰가 {float(predicted_amount):,.0f}원 "
                    f"({float(predicted_rate):.2f}%)" if predicted_rate else f"🎯 예측 {float(predicted_amount):,.0f}원"
                ),
                annotation_position="top left",
                annotation_font_color="#ff7f0e",
            )
        flow_fig.update_layout(
            xaxis_title="개찰일",
            yaxis_title="금액 (원)",
            hovermode="x unified",
            height=420,
            margin=dict(l=20, r=10, t=20, b=30),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        flow_fig.update_yaxes(tickformat=",")
        st.plotly_chart(flow_fig, use_container_width=True)

    st.markdown("#### 📋 공고 상세 목록 (최신순)")
    table_df = past_df.sort_values("opened_at", ascending=False).copy()
    table_df["개찰일"] = table_df["opened_at"].dt.strftime("%Y-%m-%d")
    table_df["예가"] = table_df["base_amount"].map(_format_amount)
    table_df["낙찰가"] = table_df["award_amount"].map(_format_amount)
    table_df["낙찰률(%)"] = table_df["bid_rate"].map(_format_rate)
    table_df["참가자"] = table_df["bidder_count"]
    table_df = table_df[
        [
            "개찰일", "notice_id", "contract_method", "region",
            "예가", "낙찰가", "낙찰률(%)", "참가자", "winning_company",
        ]
    ]
    table_df.columns = [
        "개찰일", "공고번호", "계약방법", "지역",
        "예가", "낙찰가", "낙찰률(%)", "참가자", "낙찰 기업",
    ]
    st.dataframe(
        table_df,
        hide_index=True,
        use_container_width=True,
        height=min(500, 52 + 35 * max(1, len(table_df))),
    )


def _parse_datetime(value):
    try:
        return pd.to_datetime(value, errors="coerce")
    except Exception:
        return None


def _render_review_tab(db_path: str, fee_rate: float) -> None:
    st.subheader("주간 리뷰 & 개선 제안 슬랏")
    st.caption(
        "주간 KPI 스냅샷으로 측정-기반 진화를 유지합니다. 버튼으로 스냅샷을 찍으면 "
        "자동 룰 엔진이 감지한 개선 포인트가 바로 '개선 제안'에 올라오고, 사람이 검토/승인/구현 상태를 관리합니다."
    )

    with st.container(border=True):
        c1, c2, c3 = st.columns([2, 2, 3])
        with c1:
            if st.button("📸 이번 주 스냅샷 생성", type="primary"):
                take_weekly_snapshot(db_path, fee_rate=fee_rate)
                created = auto_generate_suggestions(db_path)
                st.success(
                    f"스냅샷 저장 완료. 자동 제안 {len(created)}건 추가."
                )
                st.rerun()
        with c2:
            if st.button("🧠 지금 자동 제안만 재점검"):
                created = auto_generate_suggestions(db_path)
                st.info(f"자동 제안 {len(created)}건 추가")
                st.rerun()
        with c3:
            st.caption(
                "수수료율은 사이드바의 '목표 낙찰 확률'과 별개로 '모의 입찰' 탭에서 설정한 값이 "
                "스냅샷에 함께 기록됩니다. 현재 리뷰에서 사용 중인 수수료: "
                f"**{fee_rate * 100:.3f}%**"
            )

    snaps = list_metrics_snapshots(db_path, limit=12)
    if not snaps:
        st.info("아직 스냅샷이 없습니다. 위 버튼으로 첫 스냅샷을 만드세요.")
    else:
        cur = snaps[0]
        prev = snaps[1] if len(snaps) >= 2 else None

        def fmt_num(v):
            return f"{int(v):,}" if v is not None else "–"

        def fmt_money(v):
            return f"{float(v):,.0f}" if v is not None else "–"

        def fmt_pct(v):
            return f"{(v or 0) * 100:.1f}%"

        def delta(v, pv):
            if pv is None or v is None:
                return None
            if isinstance(v, (int, float)) and isinstance(pv, (int, float)):
                return v - pv
            return None

        def indicator(v, pv, good_is_up: bool = True):
            d = delta(v, pv)
            if d is None:
                return ""
            if d == 0:
                return "—"
            up = d > 0
            good = up if good_is_up else (not up)
            return ("🟢" if good else "🔴") + (" ▲" if up else " ▼") + f" {d:+,.2f}"

        st.markdown(f"##### 🗓 최근 스냅샷: **{cur['snapshot_date']}**"
                     + (f"  ·  직전: {prev['snapshot_date']}" if prev else ""))
        r1 = st.columns(4)
        r1[0].metric("Notices(total)", fmt_num(cur["notices_total"]),
                       indicator(cur["notices_total"], prev["notices_total"] if prev else None))
        r1[1].metric("Notices(new 7d)", fmt_num(cur["notices_new_7d"]),
                       indicator(cur["notices_new_7d"], prev["notices_new_7d"] if prev else None))
        r1[2].metric("Approved mappings", fmt_num(cur["approved_mappings"]),
                       indicator(cur["approved_mappings"], prev["approved_mappings"] if prev else None))
        r1[3].metric("Pending mappings", fmt_num(cur["pending_mappings"]),
                       indicator(cur["pending_mappings"], prev["pending_mappings"] if prev else None,
                                 good_is_up=False))
        r2 = st.columns(4)
        r2[0].metric("Mock wins", fmt_num(cur["mock_wins"]),
                       indicator(cur["mock_wins"], prev["mock_wins"] if prev else None))
        r2[1].metric("Win rate", fmt_pct(cur["win_rate"]),
                       indicator(cur["win_rate"], prev["win_rate"] if prev else None))
        r2[2].metric("Revenue 7d", fmt_money(cur["revenue_7d"]),
                       indicator(cur["revenue_7d"], prev["revenue_7d"] if prev else None))
        r2[3].metric("Revenue total", fmt_money(cur["revenue_total"]),
                       indicator(cur["revenue_total"], prev["revenue_total"] if prev else None))

        with st.expander("📜 과거 스냅샷 전체"):
            df = pd.DataFrame(snaps)
            st.dataframe(df, hide_index=True, use_container_width=True)

    st.markdown("### 💡 다음 개선 제안 슬랏")
    impact_order = {"high": 0, "medium": 1, "low": 2}
    pending = sorted(
        list_suggestions(db_path, status="proposed"),
        key=lambda s: (impact_order.get(s["impact"], 3), s["updated_at"]),
    )
    top3 = pending[:3]
    if top3:
        for s in top3:
            with st.container(border=True):
                st.markdown(f"**[{s['impact'].upper()}] {s['title']}**   <small>#{s['suggestion_id']} · {s['source']}</small>",
                              unsafe_allow_html=True)
                if s["description"]:
                    st.write(s["description"])
                if s["rationale"]:
                    st.caption(f"근거: {s['rationale']}")
                b1, b2, b3 = st.columns([1, 1, 6])
                with b1:
                    if st.button("✅ 승인", key=f"ap_{s['suggestion_id']}"):
                        update_suggestion(db_path, s["suggestion_id"], status="approved")
                        st.rerun()
                with b2:
                    if st.button("🚫 보류", key=f"rj_{s['suggestion_id']}"):
                        update_suggestion(db_path, s["suggestion_id"], status="rejected")
                        st.rerun()
    else:
        st.info("현재 올라와 있는 제안이 없습니다. 스냅샷을 찍거나 아래에서 수동으로 추가하세요.")

    with st.expander("➕ 새 제안 수동 추가"):
        t = st.text_input("제목", key="sg_title")
        d = st.text_area("설명", key="sg_desc")
        r = st.text_area("근거 (지표/관측)", key="sg_rationale")
        imp = st.selectbox("예상 임팩트", ["high", "medium", "low"], index=1, key="sg_impact")
        if st.button("등록", key="sg_add"):
            if not t.strip():
                st.error("제목은 필수입니다.")
            else:
                sid = add_suggestion(db_path, t.strip(), d.strip(), r.strip(), imp, source="manual")
                st.success(f"제안 #{sid} 등록")
                st.rerun()

    st.markdown("### 📋 모든 제안 (status 별)")
    all_suggestions = list_suggestions(db_path)
    if not all_suggestions:
        st.info("등록된 제안이 없습니다.")
        return
    df = pd.DataFrame(all_suggestions)
    st.dataframe(
        df[["suggestion_id", "status", "impact", "title", "rationale",
             "source", "updated_at", "note"]],
        hide_index=True, use_container_width=True,
        height=min(500, 52 + 35 * max(1, len(df))),
    )
    with st.expander("✏️ 상태/메모 업데이트"):
        ids = [int(x) for x in df["suggestion_id"].tolist()]
        target = st.selectbox("대상 suggestion_id", ["(선택)"] + ids, key="sg_update_pick")
        new_status = st.selectbox(
            "새 상태", ["(변경 없음)", "proposed", "approved", "implemented", "rejected"],
            index=0, key="sg_update_status",
        )
        new_note = st.text_input("메모", key="sg_update_note")
        if st.button("적용", key="sg_update_apply") and target != "(선택)":
            update_suggestion(
                db_path, int(target),
                status=None if new_status == "(변경 없음)" else new_status,
                note=new_note or None,
            )
            st.success("업데이트 적용")
            st.rerun()


def _render_mock_tab(db_path: str, target_win_probability: float) -> None:
    st.subheader("자동 모의 입찰 (Monte Carlo 배치)")
    st.caption(
        "진행 중 공고에 대해 1) 내 고객 1~10명이 분산 전략으로 투찰, 2) 해당 scope의 과거 반복 낙찰 top-10 업체가 "
        "각자 과거 투찰률 분포에서 샘플링해 경쟁한다고 가정. N회 시뮬레이션으로 승률을 측정하고, "
        "고객 투찰 내역을 DB에 저장해 이후 실제 결과 도착 시 수익(수수료) 합계까지 자동 집계합니다."
    )

    with st.container(border=True):
        st.markdown("### ▶️ 시뮬레이션 실행")
        r1 = st.columns(4)
        with r1[0]:
            category_label = st.selectbox(
                "구분", [label for label, _ in CATEGORY_DROPDOWN], index=0, key="sim_category",
            )
        with r1[1]:
            since_days = st.number_input("최근 N일", min_value=1, max_value=365,
                                          value=14, step=1, key="sim_since_days")
        with r1[2]:
            max_notices = st.number_input("최대 공고 수", min_value=5, max_value=200,
                                            value=30, step=5, key="sim_max_notices")
        with r1[3]:
            agency_filter = st.text_input("기관 (선택, 정확 일치)", key="sim_agency")

        r2 = st.columns(4)
        with r2[0]:
            num_customers = st.slider("내 고객 수", 1, 10, 5, key="sim_num_customers")
        with r2[1]:
            num_competitors = st.slider("경쟁사 수 (top-K)", 1, 15, 10, key="sim_num_competitors")
        with r2[2]:
            num_runs = st.slider("시뮬레이션 횟수", 100, 2000, 300, step=100, key="sim_num_runs")
        with r2[3]:
            fee_pct = st.number_input("수수료 %", min_value=0.0, max_value=5.0,
                                       value=0.05, step=0.01, format="%.2f", key="sim_fee_pct",
                                       help="낙찰가 대비 내 수수료. 기본 0.05%.")

        persist = st.checkbox("결과를 DB에 저장(실제 결과 반영 시 수익 집계 가능)",
                                value=True, key="sim_persist")
        run_btn = st.button("🎲 배치 시뮬레이션 실행", key="sim_run_btn", type="primary")

    category = dict(CATEGORY_DROPDOWN)[category_label]
    fee_rate = float(fee_pct) / 100.0

    if run_btn:
        with st.spinner("공고 수집 중..."):
            notices = load_pending_notices_for_prediction(
                db_path=db_path, category=category,
                agency_name=agency_filter.strip() or None,
                since_days=int(since_days), limit=int(max_notices),
            )
        if not notices:
            st.warning("조건에 맞는 진행 중 공고가 없습니다.")
        else:
            reports = []
            batch_rows: list[dict] = []
            simulation_id = f"sim-{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}"
            progress = st.progress(0.0, text=f"0/{len(notices)} 처리 중...")
            for i, notice in enumerate(notices):
                try:
                    cases, parent_used = _load_cases_adaptive(
                        db_path, notice.notice_id, notice.agency_name,
                        notice.category, notice.contract_method, notice.opened_at,
                    )
                    analyzer = AgencyRangeAnalyzer(cases, target_win_probability=target_win_probability)
                    prediction = NoticePredictor(analyzer).predict(notice)
                    analysis = prediction.analysis
                    winners = top_winners_for_scope(
                        db_path, notice.agency_name, notice.category, notice.contract_method,
                        limit=int(num_competitors), base_amount=notice.base_amount,
                    )
                    comps = [
                        CompetitorSpec(biz_no=w["biz_no"],
                                       company_name=w["company_name"],
                                       historical_rates=w["rates"],
                                       wins=w["wins"])
                        for w in winners
                    ]
                    report = run_simulation(
                        notice_id=notice.notice_id,
                        base_amount=notice.base_amount,
                        floor_rate=notice.floor_rate,
                        predicted_rate=analysis.blended_rate,
                        lower_rate=analysis.lower_rate,
                        upper_rate=analysis.upper_rate,
                        predicted_amount=analysis.recommended_amount,
                        competitors=comps,
                        n_customers=int(num_customers),
                        num_runs=int(num_runs),
                        seed=42 + i,
                    )
                    reports.append({
                        "notice": notice,
                        "report": report,
                        "parent_used": parent_used,
                        "confidence": analysis.confidence,
                    })
                    if persist:
                        for cb in report.customers:
                            batch_rows.append({
                                "notice_id": notice.notice_id,
                                "bid_amount": cb.amount,
                                "bid_rate": cb.rate,
                                "predicted_amount": analysis.recommended_amount,
                                "predicted_rate": analysis.blended_rate,
                                "note": f"sim_win_rate={report.our_win_rate:.3f}",
                                "customer_idx": cb.idx,
                            })
                except Exception as exc:  # noqa: BLE001
                    reports.append({"notice": notice, "error": str(exc)})
                progress.progress((i + 1) / len(notices),
                                    text=f"{i + 1}/{len(notices)} 처리 중...")
            progress.empty()
            if persist and batch_rows:
                save_mock_bid_batch(db_path, simulation_id, batch_rows)
                st.success(f"완료. {len(reports)}개 공고 시뮬레이션 · 고객 입찰 {len(batch_rows)}건 저장 "
                             f"(simulation_id={simulation_id}).")
            else:
                st.success(f"완료. {len(reports)}개 공고 시뮬레이션 (DB 저장 안 함).")
            st.session_state["sim_last_reports"] = reports
            st.session_state["sim_last_id"] = simulation_id if persist else None

    reports = st.session_state.get("sim_last_reports") or []
    if reports:
        _render_sim_reports(reports, fee_rate)

    st.markdown("### 💰 실현 수익 (실제 결과가 들어온 건만)")
    summary = revenue_summary(db_path, fee_rate=fee_rate,
                                simulation_id=st.session_state.get("sim_last_id"))
    r1, r2, r3 = st.columns(3)
    r1.metric("실현 낙찰 건수", summary["total_wins"])
    r2.metric("낙찰 총액", f"{summary['total_won_amount']:,.0f} 원")
    r3.metric(f"누적 수익 (@ {fee_pct:.2f}%)", f"{summary['total_revenue']:,.0f} 원")
    if summary["daily"]:
        daily_df = pd.DataFrame(summary["daily"])
        daily_df["누적수익"] = daily_df[::-1]["revenue"].cumsum()[::-1]
        st.markdown("##### 일별 수익")
        st.dataframe(
            daily_df.assign(
                won_amount=daily_df["won_amount"].map(lambda v: f"{v:,.0f}"),
                revenue=daily_df["revenue"].map(lambda v: f"{v:,.0f}"),
                누적수익=daily_df["누적수익"].map(lambda v: f"{v:,.0f}"),
            ).rename(columns={
                "day": "개찰일", "wins": "낙찰건수",
                "won_amount": "낙찰총액", "revenue": "일수익",
            })[["개찰일", "낙찰건수", "낙찰총액", "일수익", "누적수익"]],
            hide_index=True, use_container_width=True,
        )
    else:
        st.info("아직 실제 결과가 매핑된 낙찰 건이 없습니다. 추후 새로운 CSV 임포트 시 자동 반영.")

    runs = list_simulation_ids(db_path)
    if runs:
        with st.expander("🗂 이전 시뮬레이션 배치"):
            st.dataframe(pd.DataFrame(runs), hide_index=True, use_container_width=True)


def _render_sim_reports(reports: list[dict], fee_rate: float) -> None:
    st.markdown("### 📊 이번 배치 결과")
    ok = [r for r in reports if "report" in r]
    if not ok:
        st.info("처리된 리포트가 없습니다.")
        return
    rows_summary = []
    for r in ok:
        notice = r["notice"]
        rep = r["report"]
        best = None
        if rep.best_customer_idx:
            best = next((c for c in rep.customers if c.idx == rep.best_customer_idx), None)
        rows_summary.append({
            "공고번호": notice.notice_id,
            "기관": notice.agency_name,
            "구분": notice.category,
            "방법": notice.contract_method,
            "예산": notice.base_amount,
            "예측투찰가": rep.predicted_amount or 0,
            "예측률": rep.predicted_rate or 0,
            "고객수": len(rep.customers),
            "고객_최저투찰가": min(c.amount for c in rep.customers) if rep.customers else 0,
            "고객_최고투찰가": max(c.amount for c in rep.customers) if rep.customers else 0,
            "경쟁사수": len(rep.competitors),
            "시뮬승률": rep.our_win_rate,
            "최고고객#": rep.best_customer_idx or 0,
            "최고고객승률": rep.best_customer_win_rate or 0,
            "예상수수료(승률기반)": (rep.our_win_rate or 0) * (rep.mean_winning_amount_when_we_win or 0) * fee_rate,
            "부모통합": r.get("parent_used") or "",
        })
    df = pd.DataFrame(rows_summary)
    disp = df.assign(
        예산=df["예산"].map(lambda v: f"{v:,.0f}"),
        예측투찰가=df["예측투찰가"].map(lambda v: f"{v:,.0f}"),
        예측률=df["예측률"].map(lambda v: f"{v:.2f}%"),
        고객_최저투찰가=df["고객_최저투찰가"].map(lambda v: f"{v:,.0f}"),
        고객_최고투찰가=df["고객_최고투찰가"].map(lambda v: f"{v:,.0f}"),
        시뮬승률=df["시뮬승률"].map(lambda v: f"{v * 100:.1f}%"),
        최고고객승률=df["최고고객승률"].map(lambda v: f"{v * 100:.1f}%"),
        **{"예상수수료(승률기반)": df["예상수수료(승률기반)"].map(lambda v: f"{v:,.0f}")},
    )
    st.dataframe(disp, hide_index=True, use_container_width=True,
                   height=min(520, 52 + 35 * max(1, len(disp))))

    # 기대 수수료 합계
    expected_fee = df["예상수수료(승률기반)"].sum()
    st.caption(f"📈 이번 배치 기대 수수료(승률 × 평균 낙찰금액 × fee) 합계: **{expected_fee:,.0f}원**")

    # per-notice drill-down
    nid_list = df["공고번호"].tolist()
    pick = st.selectbox("공고 상세 선택 (고객/경쟁사 분포 보기)", ["(선택)"] + nid_list,
                         key="sim_drill_pick")
    if pick != "(선택)":
        entry = next((r for r in ok if r["notice"].notice_id == pick), None)
        if entry:
            _render_sim_detail(entry)


def _render_sim_detail(entry: dict) -> None:
    notice = entry["notice"]
    rep = entry["report"]
    st.markdown(f"#### 🔍 {notice.notice_id} · {notice.agency_name}")
    cust_df = pd.DataFrame([
        {"고객#": c.idx, "투찰률": f"{c.rate:.3f}%",
         "투찰금액": f"{c.amount:,.0f}원"}
        for c in rep.customers
    ])
    st.markdown("##### 내 고객 투찰 내역")
    st.dataframe(cust_df, hide_index=True, use_container_width=True)

    comp_df = pd.DataFrame([
        {"업체": c.company_name, "사업자번호": c.biz_no,
         "과거 낙찰 수": c.wins,
         "평균 투찰률": (
             f"{(sum(c.historical_rates)/len(c.historical_rates)):.2f}%"
             if c.historical_rates else "–"
         ),
         "분포(min~max)": (
             f"{min(c.historical_rates):.2f} ~ {max(c.historical_rates):.2f}"
             if c.historical_rates else "–"
         )}
        for c in rep.competitors
    ])
    st.markdown("##### 경쟁사 top-K 참조")
    st.dataframe(comp_df, hide_index=True, use_container_width=True)


def _stat_block(cases):
    rates = [c.bid_rate for c in cases if 0 < c.bid_rate <= 105]
    if not rates:
        return {"n": 0, "mean": None, "median": None, "stdev": None, "min": None, "max": None}
    import statistics
    return {
        "n": len(rates),
        "mean": round(sum(rates) / len(rates), 3),
        "median": round(statistics.median(rates), 3),
        "stdev": round(statistics.pstdev(rates), 3) if len(rates) > 1 else 0.0,
        "min": round(min(rates), 3),
        "max": round(max(rates), 3),
    }


def _render_mapping_tab(db_path: str) -> None:
    st.subheader("기관 통합 관리 (back office)")
    st.caption(
        "세부 기관의 표본이 부족할 때 부모 기관(단일 법인)으로 확장해 예측하는 규칙. "
        "`approved` 만 실제 예측에 반영됩니다. `pending` 은 사람이 검토 후 승인해야 합니다."
    )

    with st.container(border=True):
        col_a, col_b, col_c = st.columns([1, 1, 2])
        with col_a:
            if st.button("🌱 자동 시더 실행 (pending 채움)"):
                with st.spinner("시드 생성 중..."):
                    result = seed_agency_parent_mapping(db_path)
                st.success(
                    f"신규 {result['inserted']}건 추가 · unsafe {result['skipped_unsafe']} 그룹 건너뜀 · "
                    f"총 {result['total_in_table']}건 보관"
                )
                _mapping_cache.clear()
                st.rerun()
        with col_b:
            status_filter = st.selectbox(
                "상태",
                ["(전체)", "pending", "approved", "blacklisted"],
                index=1,
            )
        with col_c:
            search = st.text_input("기관명/부모 검색 (부분일치)", key="mapping_search")

    status_param = None if status_filter == "(전체)" else status_filter
    rows = _mapping_cache(db_path, status_param, search.strip() or None)
    if not rows:
        st.info("조건에 맞는 매핑이 없습니다. 먼저 시더를 돌리거나 필터를 조정하세요.")
        return

    df = pd.DataFrame([dict(r) for r in rows])
    st.caption(f"총 {len(df)}건 · 행을 클릭하면 아래에 통합 전/후 비교가 표시됩니다.")
    selection = st.dataframe(
        df.rename(columns={
            "agency_name": "기관",
            "parent_name": "부모",
            "subunit_count": "부모 subunits",
            "agency_case_count": "기관 낙찰수",
            "parent_case_count": "부모 총 낙찰수",
            "status": "상태",
            "source": "출처",
            "note": "메모",
            "updated_at": "갱신",
        }),
        hide_index=True,
        use_container_width=True,
        height=min(400, 52 + 35 * max(1, len(df))),
        on_select="rerun",
        selection_mode="single-row",
        key="mapping_table",
    )

    try:
        idx = selection.selection.rows[0]  # type: ignore[attr-defined]
    except Exception:
        idx = None
    if idx is None:
        st.info("행을 선택하면 통합 전/후 비교가 열립니다.")
        return

    row = df.iloc[int(idx)].to_dict()
    _render_mapping_detail(db_path, row)


@st.cache_data(ttl=60)
def _mapping_cache(db_path: str, status, search):
    return [dict(r) for r in list_agency_parent_mappings(db_path, status=status, search=search)]


def _render_mapping_detail(db_path: str, row: dict) -> None:
    st.markdown("---")
    agency = row["agency_name"]
    parent = row["parent_name"] or ""
    st.subheader(f"🔍 {agency} → 부모 `{parent or '(미지정)'}` 비교")

    c1, c2, c3 = st.columns(3)
    with c1:
        category_label = st.selectbox(
            "구분",
            [label for label, _ in CATEGORY_DROPDOWN],
            index=0,
            key=f"mapdetail_cat_{agency}",
        )
    with c2:
        method = st.text_input(
            "계약방법 (선택, 정확 일치)",
            key=f"mapdetail_method_{agency}",
            placeholder="예: 제한경쟁 / 일반경쟁 / 협상에 의한 계약",
        )
    with c3:
        st.write("")
        st.write("")
        st.caption(f"기관 낙찰 {row['agency_case_count']} · 부모 계 {row['parent_case_count']}")

    category = dict(CATEGORY_DROPDOWN)[category_label]
    method_param = method.strip() or None

    # Panel A: agency alone
    solo_cases = load_cases_for_agencies(
        db_path, [agency], category=category, contract_method=method_param
    )
    # Panel B: agency + parent siblings (other approved + pending under same parent)
    sibling_rows = [
        r for r in _mapping_cache(db_path, None, None)
        if r["parent_name"] == parent and r["agency_name"] != agency
    ]
    expanded_names = [agency, parent] + [r["agency_name"] for r in sibling_rows]
    expanded_cases = load_cases_for_agencies(
        db_path, expanded_names, category=category, contract_method=method_param
    )

    solo_stats = _stat_block(solo_cases)
    exp_stats = _stat_block(expanded_cases)

    left, right = st.columns(2)
    with left:
        st.markdown(f"### 🎯 단독 (세부 기관)\n**{agency}**")
        st.metric("낙찰 사례 수", solo_stats["n"])
        st.write(
            f"평균 {solo_stats['mean']} · 중앙값 {solo_stats['median']} · stdev {solo_stats['stdev']}  \n"
            f"범위 {solo_stats['min']} ~ {solo_stats['max']}"
        )
    with right:
        st.markdown(f"### 🧩 부모 통합\n**{parent}** + subunit {len(expanded_names)-1}개")
        st.metric("낙찰 사례 수", exp_stats["n"])
        st.write(
            f"평균 {exp_stats['mean']} · 중앙값 {exp_stats['median']} · stdev {exp_stats['stdev']}  \n"
            f"범위 {exp_stats['min']} ~ {exp_stats['max']}"
        )

    if solo_stats["n"] and exp_stats["n"]:
        try:
            fig = go.Figure()
            fig.add_trace(go.Histogram(
                x=[c.bid_rate for c in solo_cases if 0 < c.bid_rate <= 105],
                name=f"단독 (n={solo_stats['n']})", opacity=0.6, nbinsx=40,
            ))
            fig.add_trace(go.Histogram(
                x=[c.bid_rate for c in expanded_cases if 0 < c.bid_rate <= 105],
                name=f"통합 (n={exp_stats['n']})", opacity=0.55, nbinsx=40,
            ))
            fig.update_layout(
                barmode="overlay",
                xaxis_title="투찰률 (%)",
                yaxis_title="건수",
                height=320,
                margin=dict(l=30, r=20, t=30, b=30),
            )
            st.plotly_chart(fig, use_container_width=True)
        except Exception as exc:  # noqa: BLE001
            st.warning(f"히스토그램 렌더 실패: {exc}")

    with st.expander(f"단독 사례 {solo_stats['n']}건 (최근 20)"):
        if solo_cases:
            st.dataframe(
                pd.DataFrame([
                    {"공고": c.notice_id, "기관": c.agency_name, "구분": c.category,
                     "방법": c.contract_method, "예산": c.base_amount, "낙찰가": c.award_amount,
                     "투찰률": c.bid_rate, "개찰일": c.opened_at}
                    for c in solo_cases[:20]
                ]),
                hide_index=True, use_container_width=True,
            )
    with st.expander(f"통합 추가 사례 미리보기 (부모+형제 중 최근 30)"):
        extra = [c for c in expanded_cases if c.agency_name != agency][:30]
        if extra:
            st.dataframe(
                pd.DataFrame([
                    {"공고": c.notice_id, "기관": c.agency_name, "구분": c.category,
                     "방법": c.contract_method, "투찰률": c.bid_rate, "개찰일": c.opened_at}
                    for c in extra
                ]),
                hide_index=True, use_container_width=True,
            )

    st.markdown("### ✏️ 액션")
    action_cols = st.columns([2, 2, 2, 3])
    current_status = row["status"]
    with action_cols[0]:
        if st.button("✅ 승인 (approved)", key=f"approve_{agency}", disabled=current_status == "approved"):
            update_agency_parent_status(db_path, agency, "approved",
                                         note=row.get("note", "") or "")
            _mapping_cache.clear()
            st.success("승인됨. 다음 예측부터 반영됩니다.")
            st.rerun()
    with action_cols[1]:
        if st.button("🚫 차단 (blacklisted)", key=f"block_{agency}", disabled=current_status == "blacklisted"):
            update_agency_parent_status(db_path, agency, "blacklisted",
                                         note=row.get("note", "") or "")
            _mapping_cache.clear()
            st.warning("차단됨. 이 기관은 부모 통합이 되지 않습니다.")
            st.rerun()
    with action_cols[2]:
        if st.button("↩️ 대기로 되돌리기", key=f"pending_{agency}", disabled=current_status == "pending"):
            update_agency_parent_status(db_path, agency, "pending",
                                         note=row.get("note", "") or "")
            _mapping_cache.clear()
            st.info("pending 으로 되돌렸습니다.")
            st.rerun()
    with action_cols[3]:
        new_note = st.text_input("메모", value=row.get("note", "") or "", key=f"note_{agency}")
        if st.button("💾 메모 저장", key=f"save_note_{agency}"):
            update_agency_parent_status(db_path, agency, current_status, note=new_note)
            _mapping_cache.clear()
            st.success("메모 저장됨.")
            st.rerun()


def main() -> None:
    st.set_page_config(page_title="G2B 입찰 예측 대시보드", layout="wide")
    st.title("G2B 입찰 예측 대시보드")
    st.caption(
        "진행 중인 공고를 예측하고, 선택한 공고의 기관 과거 실적을 즉시 확인합니다."
    )

    with st.sidebar:
        st.header("설정")
        db_path = st.text_input("DB 경로", value=DEFAULT_DB_PATH)
        fee_pct_global = st.number_input(
            "기본 수수료 %", min_value=0.0, max_value=5.0, value=0.05,
            step=0.01, format="%.2f",
            help="리뷰·스냅샷 기본 수수료율. 모의 입찰 탭에서 개별 지정도 가능.",
        )

        st.markdown("---")
        st.header("전략")
        target_win_probability = st.slider(
            "목표 낙찰 확률",
            min_value=0.5,
            max_value=0.95,
            value=0.75,
            step=0.05,
            help=(
                "값이 높을수록 보수적으로 낮게 써서 확실히 이기려 하고 (격차↑), "
                "낮을수록 과감하게 높게 써서 이익을 극대화하지만 낙찰 확률은 떨어집니다."
            ),
        )

    if not Path(db_path).exists():
        st.error(f"DB 파일을 찾을 수 없습니다: {db_path}")
        st.stop()

    tab_live, tab_mock, tab_map, tab_review = st.tabs([
        "📝 진행 중 공고", "🎯 모의 입찰", "🧩 기관 통합 관리", "📈 주간 리뷰",
    ])

    with tab_live:
        _render_live_view(db_path, target_win_probability)

    with tab_mock:
        _render_mock_tab(db_path, target_win_probability)

    with tab_map:
        _render_mapping_tab(db_path)

    with tab_review:
        _render_review_tab(db_path, fee_rate=float(fee_pct_global) / 100.0)

    with st.expander("데이터 스키마 / 컬럼 설명"):
        st.markdown(
            """
            - **예측투찰가(predicted_amount)**: 같은 기관의 과거 공고만 참고해서 계산한 추천 투찰 금액
            - **낙찰가능**: `예측투찰가 ≤ 실제낙찰가` 이고 (하한율이 있다면) `예측률 ≥ 하한율` 이면 `✅`
            - **범위적중**: 실제 낙찰률이 예측 구간 `[lower, upper]` 안에 들어왔는가
            - **신뢰도**: `agency_case_count`, `peer_case_count`를 종합한 self-rated confidence
            """
        )


if __name__ == "__main__":
    main()
