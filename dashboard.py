"""Streamlit dashboard: predict-vs-actual for one agency's past awards.

Run:
    streamlit run dashboard.py

Install the extras once:
    pip install -e ".[dashboard]"
"""
from __future__ import annotations

import os
from dataclasses import asdict
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from g2b_bid_reco.agency_analysis import AgencyRangeAnalyzer
from g2b_bid_reco.backtest import build_backtest_report
from g2b_bid_reco.db import (
    list_agencies_with_backtestable_notices,
    load_backtestable_notices_for_agency,
    load_historical_cases_for_notice,
    load_pending_notices_for_prediction,
)
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
    cases = load_historical_cases_for_notice(db_path, notice.notice_id, notice.opened_at)
    analyzer = AgencyRangeAnalyzer(cases, target_win_probability=target_win_probability)
    prediction = NoticePredictor(analyzer).predict(notice)
    return prediction, build_backtest_report(prediction, actual)


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
    pairs = load_backtestable_notices_for_agency(db_path, agency_name, category=category)
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
            cases = load_historical_cases_for_notice(db_path, notice.notice_id, notice.opened_at)
            analyzer = AgencyRangeAnalyzer(cases, target_win_probability=target_win_probability)
            prediction = NoticePredictor(analyzer).predict(notice)
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
                min_value=20,
                max_value=1000,
                value=200,
                step=20,
                key="live_limit",
            )

        agency_filter = st.text_input(
            "기관명 (선택, 정확히 일치해야 필터 적용)",
            key="live_agency",
            help="특정 기관만 보고 싶을 때 정확한 이름을 입력하세요. 부분 일치는 지원하지 않습니다.",
        )

    category = dict(CATEGORY_DROPDOWN)[category_label]

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
    cases = load_historical_cases_for_notice(db_path, row["notice_id"], cutoff)
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


def main() -> None:
    st.set_page_config(page_title="G2B 입찰 예측 대시보드", layout="wide")
    st.title("G2B 입찰 예측 대시보드")
    st.caption(
        "특정 기관의 과거 공고마다 `predict-notice`를 다시 돌려 예측 투찰가를 계산하고 "
        "실제 낙찰가와 비교합니다."
    )

    with st.sidebar:
        st.header("필터")
        db_path = st.text_input("DB 경로", value=DEFAULT_DB_PATH)
        category_labels = [label for label, _ in CATEGORY_DROPDOWN]
        category_label = st.selectbox("구분", category_labels, index=0)
        min_notices = st.number_input("최소 공고 수", min_value=1, max_value=50, value=3, step=1)

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

    category = dict(CATEGORY_DROPDOWN)[category_label]

    if not Path(db_path).exists():
        st.error(f"DB 파일을 찾을 수 없습니다: {db_path}")
        st.stop()

    agencies_all = list_agencies_with_backtestable_notices(db_path, category=category, min_notices=1)
    if not agencies_all:
        st.warning("조건을 만족하는 기관이 없습니다. DB 적재 상태를 확인하세요.")
        st.stop()
    agencies = [(name, count) for name, count in agencies_all if count >= int(min_notices)]

    agency_sort_label = st.sidebar.radio(
        "기관 정렬",
        ["공고 수 많은 순", "가나다 순"],
        index=0,
    )
    if agency_sort_label == "가나다 순":
        agencies_sorted = sorted(agencies, key=lambda item: item[0])
    else:
        agencies_sorted = agencies  # already notice_count desc

    search_query = st.text_input(
        "기관 검색",
        key="agency_search",
        placeholder="예: 수자원공사, 교육청, 출판문화",
        help=(
            "기관명 일부만 입력하면 아래 드롭다운이 좁혀집니다. "
            "입력 후 Enter 또는 입력창 바깥을 클릭하면 즉시 반영됩니다."
        ),
    )
    needle = (search_query or "").strip().lower().replace(" ", "")
    if needle:
        # When searching, ignore the min_notices filter so rare agencies are still findable.
        search_pool = agencies_all if agency_sort_label != "가나다 순" else sorted(agencies_all, key=lambda item: item[0])
        filtered = [
            (name, count)
            for name, count in search_pool
            if needle in name.lower().replace(" ", "")
        ]
    else:
        filtered = agencies_sorted

    if not filtered:
        st.warning(
            f"`{search_query}` 에 해당하는 기관이 없습니다. "
            "검색어를 바꾸거나 구분(카테고리)을 다시 확인해 보세요."
        )
        st.stop()

    st.caption(
        f"🔎 매칭 기관 {len(filtered)}개 · 드롭다운에서도 타이핑하면 실시간 필터링됩니다."
    )
    label_for = {f"{name}  (n={count})": name for name, count in filtered}
    select_key = f"agency_select_{needle}_{category or 'all'}_{len(filtered)}"
    selected_label = st.selectbox("기관 선택", list(label_for.keys()), key=select_key)
    agency = label_for[selected_label]

    tab_back, tab_live = st.tabs(["📊 과거 백테스트", "📝 진행 중 공고"])

    with tab_back:
        df = _load_rows_for_agency(db_path, agency, category, target_win_probability)
        _render_summary(df)
        _render_chart(df)
        _render_table(df)

    with tab_live:
        _render_live_view(db_path, target_win_probability)

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
