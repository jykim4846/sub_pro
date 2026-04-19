"""Streamlit dashboard: predict-vs-actual for one agency's past awards.

Run:
    streamlit run dashboard.py

Install the extras once:
    pip install -e ".[dashboard]"
"""
from __future__ import annotations

import os
import statistics
from collections import Counter
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
    get_agency_parent,
    get_monitoring_alerts,
    get_monitoring_overview,
    get_operations_summary,
    get_cached_notice_prediction,
    connect,
    delete_mock_bid,
    get_actual_award,
    get_notice_snapshot,
    search_notices,
    init_db,
    list_agencies_with_backtestable_notices,
    list_agency_parent_mappings,
    list_metrics_snapshots,
    list_pending_notice_prediction_rows,
    list_run_tasks,
    list_simulation_ids,
    list_suggestions,
    load_backtestable_notices_for_agency,
    list_mock_bids_for_notice,
    load_cases_for_agencies,
    load_cases_with_shrinkage,
    load_historical_cases_for_notice,
    load_pending_notices_for_prediction,
    get_agency_parent_pool,
    revenue_summary,
    AGENCY_SHRINKAGE_K,
    replace_auto_mock_bid_batch,
    seed_agency_parent_mapping,
    take_weekly_snapshot,
    top_winners_for_scope,
    upsert_notice_prediction_cache,
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


def _format_count(value, suffix: str = "") -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return f"–{suffix}"
    try:
        return f"{int(value):,}{suffix}"
    except (TypeError, ValueError):
        return f"–{suffix}"


def _kind_label(kind: str) -> str:
    return {
        "collect_recent:service": "용역 수집",
        "collect_recent:goods": "물품 수집",
        "collect_recent:construction": "공사 수집",
        "sync_demand_agencies": "수요기관 동기화",
        "auto_bid_pending": "자동 입찰 생성",
    }.get(kind, kind)


def _inject_dashboard_styles() -> None:
    st.markdown(
        """
        <style>
        :root {
          --sub-card-border: rgba(15, 23, 42, 0.08);
          --sub-card-shadow: 0 12px 28px rgba(15, 23, 42, 0.08);
          --sub-card-text: #0f172a;
          --sub-card-muted: #64748b;
          --sub-card-bg: linear-gradient(180deg, rgba(255,255,255,0.98), rgba(248,250,252,0.98));
        }
        .subpro-card-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
          gap: 10px;
          margin: 0.35rem 0 0.7rem 0;
        }
        .subpro-stat-card {
          position: relative;
          overflow: hidden;
          border-radius: 14px;
          padding: 12px 13px 11px 13px;
          background: var(--sub-card-bg);
          border: 1px solid var(--sub-card-border);
          box-shadow: var(--sub-card-shadow);
        }
        .subpro-stat-card::before {
          content: "";
          position: absolute;
          inset: 0 auto 0 0;
          width: 5px;
          background: var(--accent, #1d4ed8);
        }
        .subpro-stat-label {
          font-size: 0.76rem;
          font-weight: 700;
          color: var(--sub-card-muted);
          letter-spacing: -0.01em;
          margin-bottom: 6px;
        }
        .subpro-stat-value {
          font-size: 1.5rem;
          line-height: 1;
          font-weight: 800;
          color: var(--sub-card-text);
          letter-spacing: -0.04em;
          margin-bottom: 6px;
        }
        .subpro-stat-meta {
          font-size: 0.75rem;
          color: var(--sub-card-muted);
          line-height: 1.25;
        }
        .subpro-summary-band {
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 10px;
          border-radius: 14px;
          padding: 10px 13px;
          margin: 0.35rem 0 0.8rem 0;
          background: linear-gradient(135deg, #eff6ff, #f8fafc 55%, #ecfeff);
          border: 1px solid rgba(59, 130, 246, 0.14);
        }
        .subpro-summary-title {
          font-size: 0.84rem;
          font-weight: 700;
          color: #0f172a;
          margin-bottom: 2px;
        }
        .subpro-summary-detail {
          font-size: 0.75rem;
          color: #475569;
          line-height: 1.25;
        }
        .subpro-badge {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          border-radius: 999px;
          padding: 4px 10px;
          font-size: 0.72rem;
          font-weight: 700;
          white-space: nowrap;
          color: var(--badge-text, #1e293b);
          background: var(--badge-bg, rgba(148, 163, 184, 0.16));
          border: 1px solid var(--badge-border, rgba(148, 163, 184, 0.22));
        }
        .subpro-alert-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
          gap: 8px;
          margin: 0.25rem 0 0.7rem 0;
        }
        .subpro-alert-chip {
          border-radius: 12px;
          border: 1px solid;
          padding: 9px 11px;
        }
        .subpro-alert-title {
          font-size: 0.75rem;
          font-weight: 800;
          margin-bottom: 3px;
        }
        .subpro-alert-detail {
          font-size: 0.73rem;
          line-height: 1.3;
        }
        .subpro-run-card {
          border-radius: 14px;
          border: 1px solid rgba(15, 23, 42, 0.08);
          background: linear-gradient(180deg, rgba(255,255,255,0.98), rgba(248,250,252,0.98));
          box-shadow: 0 10px 24px rgba(15, 23, 42, 0.06);
          padding: 11px 13px;
          margin-bottom: 10px;
        }
        .subpro-run-head {
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 10px;
          margin-bottom: 7px;
        }
        .subpro-run-title {
          font-size: 0.85rem;
          font-weight: 800;
          color: #0f172a;
        }
        .subpro-run-meta {
          font-size: 0.73rem;
          color: #64748b;
          line-height: 1.35;
          margin-top: 5px;
        }
        .subpro-progress-track {
          width: 100%;
          height: 8px;
          border-radius: 999px;
          background: rgba(148, 163, 184, 0.16);
          overflow: hidden;
          margin: 6px 0 4px 0;
        }
        .subpro-progress-bar {
          height: 100%;
          border-radius: 999px;
          background: linear-gradient(90deg, #2563eb, #06b6d4);
        }
        .subpro-search-hero {
          border-radius: 18px;
          padding: 20px 22px 18px 22px;
          margin: 0.25rem 0 1rem 0;
          background: linear-gradient(135deg, #0f172a 0%, #1e3a8a 60%, #0891b2 120%);
          color: #f8fafc;
          box-shadow: 0 14px 36px rgba(15, 23, 42, 0.18);
        }
        .subpro-search-hero h2 {
          margin: 0 0 4px 0;
          font-size: 1.3rem;
          font-weight: 800;
          letter-spacing: -0.02em;
          color: #ffffff;
        }
        .subpro-search-hero p {
          margin: 0;
          font-size: 0.82rem;
          color: rgba(226, 232, 240, 0.88);
          line-height: 1.45;
        }
        .subpro-section-title {
          display: flex;
          align-items: center;
          gap: 8px;
          font-size: 0.95rem;
          font-weight: 800;
          color: #0f172a;
          margin: 1.1rem 0 0.55rem 0;
          padding-left: 10px;
          border-left: 4px solid #2563eb;
          line-height: 1.2;
        }
        .subpro-section-title .subpro-section-meta {
          font-size: 0.74rem;
          font-weight: 600;
          color: #64748b;
          margin-left: 4px;
        }
        .subpro-notice-hero {
          border-radius: 18px;
          padding: 18px 20px;
          background: linear-gradient(135deg, #eef2ff 0%, #ecfeff 55%, #f0fdf4 110%);
          border: 1px solid rgba(99, 102, 241, 0.18);
          box-shadow: 0 10px 24px rgba(15, 23, 42, 0.06);
          margin: 0.25rem 0 0.9rem 0;
        }
        .subpro-notice-hero-top {
          display: flex;
          align-items: baseline;
          justify-content: space-between;
          gap: 14px;
          flex-wrap: wrap;
        }
        .subpro-notice-agency {
          font-size: 1.12rem;
          font-weight: 800;
          color: #0f172a;
          letter-spacing: -0.02em;
        }
        .subpro-notice-id {
          font-size: 0.78rem;
          font-family: "SF Mono", ui-monospace, monospace;
          color: #334155;
          background: rgba(255,255,255,0.7);
          padding: 3px 9px;
          border-radius: 8px;
          border: 1px solid rgba(15, 23, 42, 0.08);
        }
        .subpro-notice-sub {
          margin-top: 6px;
          display: flex;
          gap: 16px;
          flex-wrap: wrap;
          font-size: 0.8rem;
          color: #475569;
        }
        .subpro-notice-sub b { color: #0f172a; font-weight: 700; }
        .subpro-result-highlight {
          display: inline-flex;
          gap: 8px;
          align-items: center;
          padding: 6px 12px;
          border-radius: 10px;
          font-size: 0.82rem;
          font-weight: 700;
          margin-top: 10px;
        }
        .subpro-result-won {
          background: rgba(34, 197, 94, 0.14);
          color: #166534;
          border: 1px solid rgba(34, 197, 94, 0.26);
        }
        .subpro-result-pending {
          background: rgba(251, 191, 36, 0.18);
          color: #92400e;
          border: 1px solid rgba(251, 191, 36, 0.32);
        }
        .subpro-quiet {
          color: #64748b;
          font-size: 0.82rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_stat_card(
    label: str,
    value: str,
    *,
    meta: str = "",
    accent: str = "#2563eb",
) -> str:
    return (
        f"<div class='subpro-stat-card' style='--accent:{accent};'>"
        f"<div class='subpro-stat-label'>{label}</div>"
        f"<div class='subpro-stat-value'>{value}</div>"
        f"<div class='subpro-stat-meta'>{meta or '&nbsp;'}</div>"
        f"</div>"
    )


def _status_badge_html(label: str, *, tone: str = "neutral") -> str:
    tone_map = {
        "running": ("#d97706", "rgba(245, 158, 11, 0.14)", "rgba(245, 158, 11, 0.24)"),
        "completed": ("#166534", "rgba(34, 197, 94, 0.14)", "rgba(34, 197, 94, 0.24)"),
        "failed": ("#b91c1c", "rgba(248, 113, 113, 0.14)", "rgba(248, 113, 113, 0.24)"),
        "neutral": ("#334155", "rgba(148, 163, 184, 0.14)", "rgba(148, 163, 184, 0.24)"),
    }
    text, bg, border = tone_map.get(tone, tone_map["neutral"])
    return (
        "<span class='subpro-badge' "
        f"style='--badge-text:{text};--badge-bg:{bg};--badge-border:{border};'>{label}</span>"
    )


def _alert_badge_html(title: str, detail: str, *, severity: str) -> str:
    tone_map = {
        "high": ("#991b1b", "rgba(248, 113, 113, 0.14)", "rgba(248, 113, 113, 0.24)"),
        "medium": ("#9a3412", "rgba(251, 191, 36, 0.16)", "rgba(251, 191, 36, 0.26)"),
        "low": ("#166534", "rgba(34, 197, 94, 0.14)", "rgba(34, 197, 94, 0.24)"),
    }
    text, bg, border = tone_map.get(severity, tone_map["medium"])
    return (
        "<div class='subpro-alert-chip' "
        f"style='color:{text};background:{bg};border-color:{border};'>"
        f"<div class='subpro-alert-title'>{title}</div>"
        f"<div class='subpro-alert-detail'>{detail}</div>"
        "</div>"
    )


def _run_elapsed_minutes(run: dict) -> float:
    started = pd.to_datetime(run.get("started_at"), errors="coerce")
    ended = pd.to_datetime(run.get("finished_at") or run.get("updated_at"), errors="coerce")
    if pd.isna(started) or pd.isna(ended):
        return 0.0
    return max(0.0, round((ended - started).total_seconds() / 60.0, 1))


def _run_progress_ratio(run: dict) -> float | None:
    total = int(run.get("total_items") or 0)
    processed = int(run.get("processed_items") or 0)
    if total <= 0:
        return None
    return min(1.0, max(0.0, processed / total))


def _run_status_snapshot(run: dict, stalled_after_minutes: float = 3.0) -> dict:
    status = str(run.get("status") or "")
    updated_at = pd.to_datetime(run.get("updated_at"), errors="coerce")
    now = pd.Timestamp.utcnow().tz_localize(None)
    minutes_since_update = 0.0
    if not pd.isna(updated_at):
        minutes_since_update = max(
            0.0, round((now - updated_at).total_seconds() / 60.0, 1)
        )
    if status == "running":
        if minutes_since_update >= stalled_after_minutes:
            return {
                "label": "멈춤 의심",
                "active": False,
                "tone": "warning",
                "minutes_since_update": minutes_since_update,
            }
        return {
            "label": "실행 중",
            "active": True,
            "tone": "info",
            "minutes_since_update": minutes_since_update,
        }
    if status == "completed":
        return {
            "label": "완료",
            "active": False,
            "tone": "success",
            "minutes_since_update": minutes_since_update,
        }
    if status == "partial":
        return {
            "label": "부분 완료",
            "active": False,
            "tone": "warning",
            "minutes_since_update": minutes_since_update,
        }
    if status == "failed":
        return {
            "label": "실패",
            "active": False,
            "tone": "error",
            "minutes_since_update": minutes_since_update,
        }
    if status == "cancelled":
        return {
            "label": "취소됨",
            "active": False,
            "tone": "warning",
            "minutes_since_update": minutes_since_update,
        }
    return {
        "label": "대기",
        "active": False,
        "tone": "info",
        "minutes_since_update": minutes_since_update,
    }


def _track_run_history(run: dict) -> dict:
    run_id = str(run.get("run_id") or "")
    status = str(run.get("status") or "")
    processed = int(run.get("processed_items") or 0)
    success = int(run.get("success_items") or 0)
    resumed = int(run.get("resumed_items") or 0)
    now = pd.Timestamp.utcnow().tz_localize(None)
    state_key = "subpro_run_history"
    history_state = st.session_state.setdefault(state_key, {})
    # Reset samples when switching run_ids (plan 5절 rule).
    if run_id and set(history_state.keys()) - {run_id}:
        history_state.clear()
    # If the run isn't actively running, drop throughput samples entirely.
    zero_stats = {
        "delta_processed_5m": 0,
        "delta_success_5m": 0,
        "rate_per_min_5m": 0.0,
        "avg_rate_per_sec": 0.0,
    }
    snapshot = _run_status_snapshot(run)
    if status != "running" or not snapshot.get("active"):
        history_state.pop(run_id, None)
        return zero_stats
    history = list(history_state.get(run_id, []))
    history.append({
        "ts": now.isoformat(),
        "processed": processed,
        "success": success,
    })
    cutoff = now - pd.Timedelta(minutes=5)
    trimmed = []
    for item in history:
        ts = pd.to_datetime(item.get("ts"), errors="coerce")
        if pd.isna(ts) or ts < cutoff:
            continue
        trimmed.append(item)
    history_state[run_id] = trimmed
    if not trimmed:
        return zero_stats

    first = trimmed[0]
    last = trimmed[-1]
    first_ts = pd.to_datetime(first.get("ts"), errors="coerce")
    last_ts = pd.to_datetime(last.get("ts"), errors="coerce")
    delta_processed = max(0, int(last.get("processed", 0)) - int(first.get("processed", 0)))
    delta_success = max(0, int(last.get("success", 0)) - int(first.get("success", 0)))
    elapsed_seconds = max(1.0, (last_ts - first_ts).total_seconds()) if not (pd.isna(first_ts) or pd.isna(last_ts)) else 1.0
    started_at = pd.to_datetime(run.get("started_at"), errors="coerce")
    avg_elapsed_seconds = max(1.0, (now - started_at).total_seconds()) if not pd.isna(started_at) else elapsed_seconds
    new_computed = max(0, processed - resumed)
    return {
        "delta_processed_5m": delta_processed,
        "delta_success_5m": delta_success,
        "rate_per_min_5m": (delta_processed / elapsed_seconds) * 60.0,
        "avg_rate_per_sec": new_computed / avg_elapsed_seconds,
    }


def _resolve_default_db_path() -> str:
    env_path = os.environ.get("G2B_DB_PATH")
    if env_path:
        return env_path
    local_path = Path("data/bids.db")
    runner_path = Path.home() / "Library/Application Support/sub_pro-runner/data/bids.db"

    def _latest_run_meta(path: Path) -> tuple[bool, pd.Timestamp]:
        if not path.exists():
            return False, pd.NaT
        try:
            with connect(path) as conn:
                row = conn.execute(
                    """
                    SELECT status, COALESCE(updated_at, started_at) AS heartbeat
                    FROM automation_runs
                    ORDER BY
                        CASE
                            WHEN status = 'running'
                                 AND COALESCE(updated_at, started_at) >= datetime('now', '-3 minutes')
                            THEN 0
                            ELSE 1
                        END,
                        COALESCE(updated_at, started_at) DESC
                    LIMIT 1
                    """
                ).fetchone()
            if not row:
                return False, pd.NaT
            heartbeat = pd.to_datetime(row["heartbeat"], errors="coerce")
            is_fresh_running = (
                str(row["status"] or "") == "running"
                and not pd.isna(heartbeat)
                and (pd.Timestamp.now(tz=LOCAL_TZ).tz_localize(None) - heartbeat).total_seconds() <= 180
            )
            return is_fresh_running, heartbeat
        except Exception:
            return False, pd.NaT

    local_active, local_heartbeat = _latest_run_meta(local_path)
    runner_active, runner_heartbeat = _latest_run_meta(runner_path)

    # Prefer the DB that has any fresh running automation run. The dashboard
    # should follow collect/sync/auto-bid activity, not only auto_bid_pending.
    if local_active and not runner_active:
        return str(local_path)
    if runner_active and not local_active:
        return str(runner_path)
    if local_active and runner_active:
        return str(local_path if local_heartbeat >= runner_heartbeat else runner_path)

    # If nothing is actively running, follow the DB with the most recent
    # automation heartbeat so today's collection results are visible by default.
    if not pd.isna(local_heartbeat) and not pd.isna(runner_heartbeat):
        return str(local_path if local_heartbeat >= runner_heartbeat else runner_path)
    if runner_path.exists():
        return str(runner_path)
    return str(local_path)


DEFAULT_DB_PATH = _resolve_default_db_path()
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
    cases, meta = _load_cases_adaptive(
        db_path, notice.notice_id, notice.agency_name, notice.category,
        notice.contract_method, notice.opened_at,
    )
    analyzer = _build_analyzer(cases, meta, target_win_probability)
    prediction = NoticePredictor(analyzer).predict(notice)
    _annotate_shrinkage_note(prediction.analysis.notes, meta)
    return prediction, build_backtest_report(prediction, actual)


def _annotate_shrinkage_note(notes: list[str], meta: dict) -> None:
    parent = meta.get("parent_name")
    if not parent:
        return
    n_sub = int(meta.get("n_sub") or 0)
    n_pool = int(meta.get("n_parent_pool") or 0)
    k = int(meta.get("k") or AGENCY_SHRINKAGE_K)
    w_sub = float(meta.get("w_sub") or 0.0)
    notes.append(
        f"shrinkage blend: 하위 {n_sub}건 + 상위 '{parent}' 그룹 {n_pool}건 "
        f"(K={k}, 자체 비중 {w_sub:.0%})."
    )


def _build_analyzer(
    cases,
    meta: dict,
    target_win_probability: float,
):
    """Construct an AgencyRangeAnalyzer wired with the parent-pool shrinkage."""
    parent_name = meta.get("parent_name") if meta else None
    pool = meta.get("parent_pool_agencies") if meta else None
    prior_strength = int(meta.get("k") or AGENCY_SHRINKAGE_K) if parent_name else 4.0
    return AgencyRangeAnalyzer(
        cases,
        target_win_probability=target_win_probability,
        prior_strength=prior_strength,
        parent_pool_agencies=pool,
        parent_name=parent_name,
    )


def _load_cases_adaptive(
    db_path: str,
    notice_id: str,
    agency_name: str,
    category: str,
    contract_method: str,
    opened_at,
):
    """Return (cases, meta) where meta describes the sub↔parent shrinkage blend.

    `cases` is the scope-wide peer pool; the analyzer filters it down to
    sibling-group peers when meta["parent_pool_agencies"] is present.
    """
    cases = load_historical_cases_for_notice(
        db_path, notice_id, opened_at,
        category=category, contract_method=contract_method,
    )
    parent_name, pool = get_agency_parent_pool(db_path, agency_name)
    n_sub = sum(1 for c in cases if c.agency_name == agency_name)
    n_pool = sum(1 for c in cases if c.agency_name in pool) if pool else 0
    denom = n_sub + AGENCY_SHRINKAGE_K
    meta = {
        "parent_name": parent_name,
        "parent_pool_agencies": pool if parent_name else None,
        "n_sub": n_sub,
        "n_parent_pool": n_pool,
        "k": AGENCY_SHRINKAGE_K,
        "w_sub": (n_sub / denom) if (parent_name and denom > 0) else 1.0,
    }
    return cases, meta


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


@st.cache_data(show_spinner="진행 중 공고 목록을 불러오는 중...")
def _load_pending_notice_rows(
    db_path: str,
    category: str | None,
    agency_name: str | None,
    since_days: int,
    limit: int,
    target_win_probability: float,
) -> pd.DataFrame:
    rows = list_pending_notice_prediction_rows(
        db_path,
        target_win_probability=target_win_probability,
        category=category,
        agency_name=agency_name,
        since_days=since_days,
        limit=limit,
    )
    df = pd.DataFrame(rows)
    if not df.empty:
        df["opened_at"] = pd.to_datetime(df["opened_at"], errors="coerce")
        df = df.sort_values("opened_at", ascending=False).reset_index(drop=True)
    return df


def _compute_and_store_notice_prediction(
    db_path: str,
    notice: BidNoticeSnapshot,
    target_win_probability: float,
) -> dict:
    cases, meta = _load_cases_adaptive(
        db_path, notice.notice_id, notice.agency_name, notice.category,
        notice.contract_method, notice.opened_at,
    )
    analyzer = _build_analyzer(cases, meta, target_win_probability)
    prediction = NoticePredictor(analyzer).predict(notice)
    _annotate_shrinkage_note(prediction.analysis.notes, meta)
    analysis = prediction.analysis
    notes_text = "\n".join(analysis.notes)
    upsert_notice_prediction_cache(
        db_path,
        notice,
        target_win_probability,
        predicted_amount=analysis.recommended_amount,
        predicted_rate=analysis.blended_rate,
        lower_rate=analysis.lower_rate,
        upper_rate=analysis.upper_rate,
        estimated_win_probability=analysis.estimated_win_probability,
        confidence=analysis.confidence,
        agency_cases=analysis.agency_case_count,
        peer_cases=analysis.peer_case_count,
        lookback_years_used=analysis.lookback_years_used,
        parent_used=meta.get("parent_name"),
        analysis_notes=notes_text,
    )
    cached = get_cached_notice_prediction(db_path, notice, target_win_probability)
    if cached is None:
        raise RuntimeError("prediction cache write failed")
    return cached


def _cache_status_label(value: str) -> str:
    return {
        "ready": "저장됨",
        "stale": "갱신 필요",
        "missing": "미계산",
    }.get(str(value or ""), "미계산")


_VERDICT_LABEL = {
    "won": "🟢 낙찰",
    "lost": "🔴 패찰",
    "disqualified": "⚫ 실격",
    "pending": "⏳ 대기",
}


@st.cache_data(ttl=600, show_spinner=False)
def _notice_detail_bundle(
    db_path: str,
    notice_id: str,
    target_win_probability: float,
) -> dict | None:
    """Cached bundle for the inline detail panel.

    All four reads (notice snapshot, prediction cache, latest mock bids,
    top winners) stay fresh within the daily batch cadence, so caching keeps
    click responses near-instant without a DB roundtrip.
    """
    notice = get_notice_snapshot(db_path, notice_id)
    if notice is None:
        return None
    prediction = get_cached_notice_prediction(db_path, notice, target_win_probability)
    mock_rows = list_mock_bids_for_notice(db_path, notice_id)
    winners = top_winners_for_scope(
        db_path,
        notice.agency_name,
        notice.category,
        notice.contract_method,
        limit=10,
        base_amount=notice.base_amount,
    )
    return {
        "notice": notice,
        "prediction": prediction,
        "mock_rows": mock_rows,
        "winners": winners,
    }


def _render_notice_inline_detail(
    db_path: str,
    notice_id: str,
    row: dict,
    target_win_probability: float,
) -> None:
    """Expanded panel shown above the notice table when a row is selected."""
    bundle = _notice_detail_bundle(db_path, notice_id, target_win_probability)
    if bundle is None:
        st.warning("공고 정보를 불러오지 못했습니다.")
        return
    notice = bundle["notice"]
    cached = bundle["prediction"]
    mock_rows = bundle["mock_rows"]
    winners = bundle["winners"]
    st.caption(
        f"계약방법 {notice.contract_method} · 구분 {_humanize_category(notice.category)} · "
        f"예산 {_format_amount(notice.base_amount)}원 · "
        f"[나라장터 공고 열기]({_build_g2b_detail_url(notice.notice_id, notice.category)})"
    )

    button_label = "예측 다시 계산" if cached else "이 공고 예측 계산"
    if st.button(button_label, key=f"compute_notice_prediction_{notice_id}", type="primary"):
        with st.spinner("선택한 공고의 예측을 계산하고 저장하는 중..."):
            cached = _compute_and_store_notice_prediction(db_path, notice, target_win_probability)
        _load_pending_notice_rows.clear()
        _notice_detail_bundle.clear()
        st.success("예측 결과를 저장했습니다.")
        st.rerun()

    if cached is None:
        st.info("저장된 예측 결과가 없습니다. 위 버튼을 눌러 이 공고만 계산하세요.")
    else:
        info1, info2, info3, info4 = st.columns(4)
        info1.metric("예측 투찰가", _format_amount(cached.get("predicted_amount")))
        info2.metric("예측률", f"{_format_rate(cached.get('predicted_rate'))}%")
        info3.metric("추정 낙찰확률", _format_pct(cached.get("estimated_win_probability")))
        info4.metric("계산시각", str(cached.get("computed_at") or "–"))
        sub1, sub2, sub3, sub4 = st.columns(4)
        sub1.metric("하한율", f"{_format_rate(notice.floor_rate)}%")
        sub2.metric("신뢰도", str(cached.get("confidence") or "–"))
        sub3.metric("기관사례", f"{int(cached.get('agency_cases') or 0):,}건")
        sub4.metric("peer사례", f"{int(cached.get('peer_cases') or 0):,}건")
        if cached.get("analysis_notes"):
            st.caption(str(cached["analysis_notes"]).replace("\n", "  \n"))

    st.markdown("**🤖 자동 모의입찰**")
    if not mock_rows:
        st.info("이 공고에 대한 자동 모의입찰이 아직 생성되지 않았습니다.")
    else:
        sim_id = str(mock_rows[0].get("simulation_id") or "")
        submitted = str(mock_rows[0].get("submitted_at") or "")
        verdict_counts = Counter(r.get("verdict") or "pending" for r in mock_rows)
        verdict_summary = " · ".join(
            f"{_VERDICT_LABEL.get(v, v)} {n}명" for v, n in verdict_counts.most_common()
        )
        st.caption(
            f"simulation_id=`{sim_id}` · 생성시각 {submitted} · "
            f"고객 {len(mock_rows)}명 · {verdict_summary}"
        )
        mock_df = pd.DataFrame(
            [
                {
                    "고객": int(r.get("customer_idx") or 0),
                    "투찰가": _format_amount(r.get("bid_amount")),
                    "투찰률(%)": _format_rate(r.get("bid_rate")),
                    "예측가": _format_amount(r.get("predicted_amount")),
                    "예측률(%)": _format_rate(r.get("predicted_rate")),
                    "결과": _VERDICT_LABEL.get(r.get("verdict") or "pending", "–"),
                    "메모": str(r.get("note") or ""),
                }
                for r in mock_rows
            ]
        )
        st.dataframe(mock_df, hide_index=True, use_container_width=True)

    st.markdown("**🏢 동일 스코프 상위 낙찰사**")
    if not winners:
        st.info("동일 스코프(구분·계약방법·예산 범위)에서 낙찰 이력이 있는 업체가 없습니다.")
    else:
        winners_df = pd.DataFrame(
            [
                {
                    "업체": w.get("company_name") or "–",
                    "사업자번호": w.get("biz_no") or "–",
                    "낙찰 건수": int(w.get("wins") or 0),
                    "낙찰률 중앙값(%)": (
                        f"{statistics.median(w['rates']):.2f}" if w.get("rates") else "–"
                    ),
                    "최저(%)": (
                        f"{min(w['rates']):.2f}" if w.get("rates") else "–"
                    ),
                    "최고(%)": (
                        f"{max(w['rates']):.2f}" if w.get("rates") else "–"
                    ),
                }
                for w in winners
            ]
        )
        st.dataframe(winners_df, hide_index=True, use_container_width=True)

        all_rates = [rate for w in winners for rate in (w.get("rates") or [])]
        our_rates = (
            [float(r.get("bid_rate") or 0) for r in mock_rows if r.get("bid_rate")]
            if mock_rows
            else []
        )
        if all_rates:
            fig = go.Figure()
            fig.add_trace(
                go.Histogram(
                    x=all_rates,
                    nbinsx=30,
                    name="상위 업체 낙찰률 분포",
                    marker_color="#4C78A8",
                    opacity=0.75,
                )
            )
            for rate in our_rates:
                fig.add_vline(
                    x=rate,
                    line_width=2,
                    line_dash="dash",
                    line_color="#E45756",
                    annotation_text=f"{rate:.2f}",
                    annotation_position="top",
                )
            fig.update_layout(
                height=260,
                margin=dict(l=10, r=10, t=30, b=10),
                xaxis_title="낙찰률(%)",
                yaxis_title="빈도",
                showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True)


def _render_live_view(db_path: str, target_win_probability: float) -> None:
    st.subheader("진행 중 / 낙찰 미확정 공고")
    st.caption(
        "공고 행을 클릭하면 표 위에 상세·자동 모의입찰 내역이 펼쳐집니다. "
        "저장된 결과는 새 데이터가 적재되기 전까지 재사용됩니다."
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
                value=200,
                step=10,
                key="live_limit",
                help="표는 빠르게 불러오지만, 가독성을 위해 처음엔 100~200 정도를 권장합니다.",
            )

        agency_filter = st.text_input(
            "기관명 (선택, 정확히 일치해야 필터 적용)",
            key="live_agency",
            help="특정 기관만 보고 싶을 때 정확한 이름을 입력하세요. 부분 일치는 지원하지 않습니다.",
        )

    category = dict(CATEGORY_DROPDOWN)[category_label]
    df = _load_pending_notice_rows(
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

    display = df.assign(
        category=df["category"].map(_humanize_category),
        base_amount=df["base_amount"].map(_format_amount),
        predicted_amount=df["predicted_amount"].map(_format_amount),
        predicted_rate=df["predicted_rate"].map(_format_rate),
        floor_rate=df["floor_rate"].map(_format_rate),
        est_win=df["estimated_win_probability"].map(_format_pct),
        cache_status=df["cache_status"].map(_cache_status_label),
    )[
        [
            "opened_at",
            "notice_id",
            "category",
            "agency_name",
            "contract_method",
            "base_amount",
            "cache_status",
            "cached_at",
            "predicted_amount",
            "predicted_rate",
            "floor_rate",
            "est_win",
            "confidence",
            "agency_cases",
            "peer_cases",
        ]
    ]
    display.columns = [
        "개찰일", "공고번호", "구분", "기관", "계약방법",
        "예산", "상태", "계산시각", "예측투찰가", "예측률(%)", "하한율(%)",
        "추정 낙찰확률", "신뢰도", "기관사례", "peer사례",
    ]

    # Render selected-row detail ABOVE the table so the click and the expanded
    # panel stay close in the viewport. Streamlit can't insert the detail
    # inline between rows, so we keep it pinned at the top when a row is
    # selected. The table height is bounded so scrolling stays local.
    detail_slot = st.container()
    selection_event = st.dataframe(
        display,
        hide_index=True,
        use_container_width=True,
        height=min(500, 52 + 35 * max(1, len(display))),
        on_select="rerun",
        selection_mode="single-row",
        key="live_notice_table",
    )

    selected_indices = []
    try:
        selected_indices = selection_event.selection.rows  # type: ignore[attr-defined]
    except Exception:
        selected_indices = []

    with detail_slot:
        if not selected_indices:
            st.info("위 표에서 공고 행을 클릭하면 상세와 자동 모의입찰 내역이 여기 펼쳐집니다.")
            return
        selected_row = df.iloc[int(selected_indices[0])].to_dict()
        notice_id = str(selected_row["notice_id"])
        with st.container(border=True):
            st.markdown(
                f"**{selected_row.get('opened_at') or '–'} · `{notice_id}` · "
                f"{selected_row.get('agency_name') or '–'}**"
            )
            _render_notice_inline_detail(
                db_path, notice_id, selected_row, target_win_probability
            )


@st.cache_data(ttl=5, show_spinner=False)
def _ops_summary_cached(db_path: str) -> dict:
    return get_operations_summary(db_path)


@st.cache_data(ttl=5, show_spinner=False)
def _monitoring_overview_cached(db_path: str) -> dict:
    return get_monitoring_overview(db_path)


@st.cache_data(ttl=5, show_spinner=False)
def _monitoring_alerts_cached(db_path: str) -> list:
    return get_monitoring_alerts(db_path)


def _is_auto_bid_running(db_path: str) -> bool:
    """True iff the latest auto_bid_pending run is actively running and fresh.
    Used to gate the 5s auto-refresh so idle sessions stay still.
    """
    summary = _ops_summary_cached(db_path)
    run = summary.get("latest_auto_bid_run") or {}
    if str(run.get("status") or "") != "running":
        return False
    # Require recent heartbeat; a stale 'running' is treated as idle for
    # auto-refresh purposes (the realtime panel still shows a 멈춤 의심 card).
    snap = _run_status_snapshot(run)
    return bool(snap.get("active"))


def _render_operations_summary(db_path: str) -> None:
    summary = _ops_summary_cached(db_path)
    st.subheader("운영 요약")
    st.caption("오늘 기준 진행 상태와 자동 모의 입찰 커버리지를 실시간으로 보여줍니다.")
    coverage_pct = (
        (summary["auto_covered_pending"] / summary["pending_total"]) * 100.0
        if summary["pending_total"] else 0.0
    )
    eval_pct = (
        (summary["evaluated_today"] / summary["completed_today"]) * 100.0
        if summary["completed_today"] else 0.0
    )
    top_cards = [
        _render_stat_card("진행 중 발주", _format_count(summary["pending_total"], "건"), meta="현재 결과 미확정 공고", accent="#2563eb"),
        _render_stat_card("신규 입수", _format_count(summary["new_today"], "건"), meta="오늘 새로 적재된 공고", accent="#0891b2"),
        _render_stat_card("오늘 완료", _format_count(summary["completed_today"], "건"), meta="오늘 결과가 들어온 공고", accent="#7c3aed"),
        _render_stat_card("평가 완료", _format_count(summary["evaluated_today"], "건"), meta=f"오늘 완료 대비 {eval_pct:.1f}%", accent="#16a34a"),
        _render_stat_card("자동입찰 커버", _format_count(summary["auto_covered_pending"], "건"), meta=f"진행 중 대비 {coverage_pct:.1f}%", accent="#ea580c"),
    ]
    api_cards = [
        _render_stat_card("오늘 API 호출", _format_count(summary["total_api_calls_today"], "회"), meta="수집 + 기관 API 합계", accent="#0f766e"),
        _render_stat_card("수집 API", _format_count(summary["collect_api_calls_today"], "회"), meta="입찰/결과/계약 수집", accent="#0284c7"),
        _render_stat_card("기관 API", _format_count(summary["agency_api_calls_today"], "회"), meta="수요기관 사용자정보", accent="#9333ea"),
        _render_stat_card(
            "오늘 자동입찰",
            _format_count(summary["auto_bid_notices_today"], "건"),
            meta=(
                f"현재 run 반영 {_format_count(summary['latest_auto_bid_saved_notices'], '건')}"
                if summary.get("latest_auto_bid_saved_notices")
                else "오늘 생성된 자동 포트폴리오"
            ),
            accent="#ca8a04",
        ),
    ]
    st.markdown(f"<div class='subpro-card-grid'>{''.join(top_cards)}</div>", unsafe_allow_html=True)
    st.markdown(f"<div class='subpro-card-grid'>{''.join(api_cards)}</div>", unsafe_allow_html=True)
    latest_run = summary.get("latest_auto_bid_run")
    # Show the "최근 자동 입찰 배치" band only when an auto-bid is actively
    # running (status=='running' with a fresh heartbeat). Stale `running`
    # still renders in the 운영 모니터링 패널 as a 멈춤 의심 card.
    is_active = bool(
        latest_run and _run_status_snapshot(latest_run).get("active")
    )
    if latest_run and is_active:
        processed = int(latest_run.get("processed_items") or 0)
        total = int(latest_run.get("total_items") or 0)
        success = int(latest_run.get("success_items") or 0)
        failed = int(latest_run.get("failed_items") or 0)
        resumed = int(latest_run.get("resumed_items") or 0)
        new_computed = int(summary.get("latest_auto_bid_new_computed") or 0)
        status = str(latest_run.get("status") or "")
        status_label_map = {
            "running": ("실행 중", "running"),
            "completed": ("완료", "completed"),
            "partial": ("부분 완료", "running"),
            "failed": ("실패", "failed"),
            "cancelled": ("취소됨", "failed"),
        }
        label, tone = status_label_map.get(status, (status or "–", "neutral"))
        badge = _status_badge_html(label, tone=tone)
        active_task = summary.get("latest_auto_bid_active_task")
        active_line = ""
        if active_task:
            at_processed = int(active_task.get("processed_items") or 0)
            at_total = int(active_task.get("total_items") or 0)
            active_line = (
                f"<div class='subpro-summary-detail' style='margin-top:4px;'>"
                f"🧩 active task · {active_task.get('category') or '–'}/{active_task.get('contract_method') or '–'} "
                f"#{active_task.get('task_seq')} · {_format_count(at_processed)}/{_format_count(at_total)}"
                f"</div>"
            )
        task_summary = summary.get("latest_auto_bid_task_summary") or {}
        task_line = ""
        if task_summary and task_summary.get("task_count"):
            task_line = (
                f"<div class='subpro-summary-detail' style='margin-top:4px;'>"
                f"🗂 tasks · queued {_format_count(task_summary.get('queued_tasks') or 0)} · "
                f"running {_format_count(task_summary.get('running_tasks') or 0)} · "
                f"completed {_format_count(task_summary.get('completed_tasks') or 0)} · "
                f"partial {_format_count(task_summary.get('partial_tasks') or 0)} · "
                f"failed {_format_count(task_summary.get('failed_tasks') or 0)}"
                f"</div>"
            )
        st.markdown(
            (
                "<div class='subpro-summary-band'>"
                "<div>"
                "<div class='subpro-summary-title'>최근 자동 입찰 배치</div>"
                f"<div class='subpro-summary-detail'>`{latest_run.get('run_id')}` · "
                f"{_format_count(processed)}/{_format_count(total)} 공고 처리 ({summary['latest_auto_bid_progress_pct']:.1f}%) · "
                f"resumed {_format_count(resumed)} · 신규 계산 {_format_count(new_computed)} · "
                f"성공 {_format_count(success)} · 실패 {_format_count(failed)}</div>"
                f"{active_line}{task_line}"
                "</div>"
                f"{badge}"
                "</div>"
            ),
            unsafe_allow_html=True,
        )
        if status == "running" and total > 0:
            st.progress(min(1.0, processed / total))
    st.markdown("---")


@st.fragment(run_every="5s")
def _render_operations_summary_fragment(db_path: str) -> None:
    _render_operations_summary(db_path)


def _render_run_card(db_path: str, run: dict) -> None:
    progress_ratio = _run_progress_ratio(run)
    elapsed_minutes = _run_elapsed_minutes(run)
    processed = int(run.get("processed_items") or 0)
    total = int(run.get("total_items") or 0)
    success = int(run.get("success_items") or 0)
    failed = int(run.get("failed_items") or 0)
    resumed = int(run.get("resumed_items") or 0)
    new_computed = max(0, processed - resumed)
    status_snapshot = _run_status_snapshot(run)
    speed_stats = _track_run_history(run)
    tone_for_label = {
        "실행 중": "running",
        "완료": "completed",
        "부분 완료": "running",
        "실패": "failed",
        "멈춤 의심": "failed",
        "취소됨": "failed",
        "대기": "neutral",
    }
    badge = _status_badge_html(
        status_snapshot["label"],
        tone=tone_for_label.get(status_snapshot["label"], "neutral"),
    )
    progress_pct = progress_ratio * 100.0 if progress_ratio is not None else 0.0
    progress_bar = (
        "<div class='subpro-progress-track'>"
        f"<div class='subpro-progress-bar' style='width:{progress_pct:.1f}%'></div>"
        "</div>"
    )
    active_task = None
    if str(run.get("kind") or "") == "auto_bid_pending" and status_snapshot.get("active"):
        try:
            from g2b_bid_reco.db import _latest_active_task as _get_active_task
            active_task = _get_active_task(db_path, str(run.get("run_id") or ""))
        except Exception:
            active_task = None
    task_meta = ""
    if active_task:
        at_proc = int(active_task.get("processed_items") or 0)
        at_total = int(active_task.get("total_items") or 0)
        task_meta = (
            "<div class='subpro-run-meta'>🧩 active task · "
            f"{active_task.get('category') or '–'}/{active_task.get('contract_method') or '–'} "
            f"#{active_task.get('task_seq')} · {_format_count(at_proc)}/{_format_count(at_total)}</div>"
        )
    resumed_meta = (
        f"<div class='subpro-run-meta'>resumed {_format_count(resumed)} · "
        f"신규 계산 {_format_count(new_computed)}</div>"
        if str(run.get("kind") or "") == "auto_bid_pending" else ""
    )
    st.markdown(
        (
            "<div class='subpro-run-card'>"
            "<div class='subpro-run-head'>"
            f"<div class='subpro-run-title'>{_kind_label(str(run.get('kind') or ''))}</div>"
            f"{badge}"
            "</div>"
            f"{progress_bar}"
            f"<div class='subpro-run-meta'>{_format_count(processed)}/{_format_count(total)} 처리 · "
            f"성공 {_format_count(success)} · 실패 {_format_count(failed)} · 경과 {elapsed_minutes:.1f}분</div>"
            f"{resumed_meta}"
            f"<div class='subpro-run-meta'>최근 5분 {_format_count(speed_stats['delta_processed_5m'], '건')} · "
            f"{speed_stats['rate_per_min_5m']:.1f}건/분 · 평균 {speed_stats['avg_rate_per_sec']:.1f}건/초</div>"
            f"{task_meta}"
            + (
                f"<div class='subpro-run-meta'>현재 단계: {run.get('message')}</div>"
                if run.get("message") else ""
            )
            + "</div>"
        ),
        unsafe_allow_html=True,
    )


def _render_realtime_status_content(db_path: str) -> None:
    overview = _monitoring_overview_cached(db_path)
    alerts = _monitoring_alerts_cached(db_path)
    fresh_active_runs: list[dict] = []
    stalled_runs: list[dict] = []
    for run in overview["latest_runs"]:
        snap = _run_status_snapshot(run)
        if snap.get("active"):
            fresh_active_runs.append(run)
        elif str(run.get("status") or "") == "running":
            stalled_runs.append(run)

    st.subheader("운영 모니터링")
    if fresh_active_runs or stalled_runs:
        st.caption(
            "실행 중/멈춤 의심 배치가 있어 이 영역만 15초마다 자동 갱신합니다."
        )
        if stalled_runs and not fresh_active_runs:
            st.warning(
                f"🚨 running 상태이지만 heartbeat가 멈춘 배치 {len(stalled_runs)}건이 있습니다. "
                "최근 기록된 진행률만 보이며 실제로는 정체 중일 수 있습니다."
            )
        st.markdown("#### 실시간 실행 현황")
        for run in fresh_active_runs:
            _render_run_card(db_path, run)
        for run in stalled_runs:
            _render_run_card(db_path, run)
    else:
        st.caption("실행 중인 배치가 없습니다. 자동 갱신은 이 영역에서만 최소 비용으로 동작합니다.")

    if alerts:
        st.markdown(
            "<div class='subpro-alert-grid'>"
            + "".join(
                _alert_badge_html(
                    str(alert["title"]),
                    str(alert["detail"]),
                    severity=str(alert["severity"]),
                )
                for alert in alerts
            )
            + "</div>",
            unsafe_allow_html=True,
        )
    else:
        st.success("현재 감지된 운영 이상징후가 없습니다.")

    with st.expander("배치 상태 / 데이터 신선도"):
        runs = pd.DataFrame(overview["latest_runs"])
        if not runs.empty:
            runs["소요분"] = runs.apply(
                lambda row: (
                    round((pd.to_datetime(row["finished_at"] or row["updated_at"]) - pd.to_datetime(row["started_at"])).total_seconds() / 60.0, 1)
                    if row.get("started_at") else None
                ),
                axis=1,
            )
            runs = runs.rename(columns={
                "kind": "배치종류",
                "status": "상태",
                "total_items": "전체",
                "processed_items": "처리",
                "success_items": "성공",
                "failed_items": "실패",
                "started_at": "시작시각",
                "finished_at": "종료시각",
                "message": "메시지",
            })
            st.dataframe(runs, hide_index=True, use_container_width=True)

        freshness = pd.DataFrame(overview["freshness"])
        if not freshness.empty:
            freshness_cards = []
            for row in freshness.to_dict("records"):
                freshness_cards.append(
                    _render_stat_card(
                        _humanize_category(str(row["category"])),
                        "신선",
                        meta=(
                            f"공고 {row.get('latest_opened_at') or '–'}<br>"
                            f"적재 {row.get('latest_ingested_at') or '–'}"
                        ),
                        accent="#0ea5e9",
                    )
                )
            st.markdown(
                "<div class='subpro-card-grid'>" + "".join(freshness_cards) + "</div>",
                unsafe_allow_html=True,
            )


@st.fragment(run_every="5s")
def _render_realtime_status_fragment(db_path: str) -> None:
    _render_realtime_status_content(db_path)


def _render_monitoring_panel(db_path: str) -> None:
    _render_realtime_status_fragment(db_path)


def _render_mock_realtime_status_content(db_path: str) -> None:
    auto_bid_run = get_latest_automation_run(db_path, "auto_bid_pending")
    if auto_bid_run is None:
        st.caption("최근 자동 입찰 배치 이력이 아직 없습니다.")
        return

    run = dict(auto_bid_run)
    status_snapshot = _run_status_snapshot(run)
    processed = int(run.get("processed_items") or 0)
    total = int(run.get("total_items") or 0)
    success = int(run.get("success_items") or 0)
    failed = int(run.get("failed_items") or 0)
    resumed = int(run.get("resumed_items") or 0)
    new_computed = max(0, processed - resumed)
    elapsed_minutes = _run_elapsed_minutes(run)
    progress_ratio = _run_progress_ratio(run)
    speed_stats = _track_run_history(run)
    prev_processed_key = "mock_tab_prev_auto_bid_processed"
    prev_processed = int(st.session_state.get(prev_processed_key, processed))
    delta_processed = processed - prev_processed
    st.session_state[prev_processed_key] = processed
    try:
        from g2b_bid_reco.db import _latest_active_task as _get_active_task
        active_task = _get_active_task(db_path, str(run.get("run_id") or ""))
    except Exception:
        active_task = None

    with st.container(border=True):
        st.markdown("### 실시간 자동 입찰 상태")
        s1, s2, s3, s4, s5 = st.columns(5)
        s1.metric("상태", status_snapshot["label"])
        s2.metric("처리", f"{_format_count(processed)} / {_format_count(total)}" if total else _format_count(processed, "건"))
        s3.metric("성공", _format_count(success, "건"))
        s4.metric("실패", _format_count(failed, "건"))
        s5.metric("경과", f"{elapsed_minutes:.1f}분")
        r1, r2, r3 = st.columns(3)
        r1.metric("resumed", _format_count(resumed, "건"))
        r2.metric("신규 계산", _format_count(new_computed, "건"))
        r3.metric(
            "active task",
            (
                f"#{active_task['task_seq']} {active_task.get('category') or '–'}/{active_task.get('contract_method') or '–'}"
                if active_task else "–"
            ),
            help=(
                f"처리 {_format_count(int(active_task.get('processed_items') or 0))}/"
                f"{_format_count(int(active_task.get('total_items') or 0))}"
                if active_task else "실행 중 task 없음"
            ),
        )
        m1, m2, m3 = st.columns(3)
        m1.metric("최근 5분 처리", _format_count(speed_stats["delta_processed_5m"], "건"))
        m2.metric("최근 5분 속도", f"{speed_stats['rate_per_min_5m']:.1f}건/분")
        m3.metric("평균 속도", f"{speed_stats['avg_rate_per_sec']:.1f}건/초")
        if progress_ratio is not None:
            st.progress(progress_ratio)
            st.caption(
                f"전체 {_format_count(total, '건')} 중 {_format_count(processed, '건')} 처리 ({progress_ratio * 100:.1f}%)"
                + (
                    f" · 마지막 새로고침 이후 +{_format_count(delta_processed, '건')}"
                    if delta_processed > 0 else ""
                )
            )
        else:
            st.progress(0)
            st.caption(
                "전체 대상 수를 아직 집계 중입니다."
                + (
                    f" 마지막 새로고침 이후 +{_format_count(delta_processed, '건')}"
                    if delta_processed > 0 else ""
                )
            )
        if run.get("message"):
            st.write(f"현재 단계: {run['message']}")
        st.caption(f"마지막 상태 갱신 {status_snapshot['minutes_since_update']:.1f}분 전")
        if status_snapshot["label"] == "완료":
            st.success("최근 자동 입찰 배치가 정상 완료됐습니다.")
        elif status_snapshot["label"] == "부분 완료":
            st.warning(
                "일부 공고는 처리됐지만 전체 완료 전에 중단됐습니다. "
                "다시 실행하면 이어서 계산합니다."
            )
        elif status_snapshot["label"] == "실패":
            st.error("최근 자동 입찰 배치가 실패 상태로 종료됐습니다.")
        elif status_snapshot["label"] == "멈춤 의심":
            st.warning(
                "최근 자동 입찰 배치가 running으로 남아 있지만 상태 갱신이 멈췄습니다. "
                "프로세스 중단 여부를 점검해야 합니다."
            )
        elif run.get("status") == "running" and delta_processed <= 0 and processed > 0:
            st.info(
                "숫자가 바로 안 올라가더라도 큰 공고 묶음을 계산 중일 수 있습니다. "
                "같은 값이 계속 유지되면 멈춤 여부를 추가 점검하면 됩니다."
            )

    # Recent tasks for this run
    try:
        tasks = list_run_tasks(db_path, str(run.get("run_id") or ""))
    except Exception:
        tasks = []
    if tasks:
        with st.expander(f"🗂 최근 task {len(tasks)}건 (run_id={run.get('run_id')})"):
            tdf = pd.DataFrame(tasks)
            show = tdf[[
                "task_seq", "category", "contract_method", "status",
                "processed_items", "total_items", "success_items",
                "failed_items", "started_at", "finished_at", "message",
            ]].rename(columns={
                "task_seq": "seq", "contract_method": "방법", "category": "구분",
                "status": "상태", "processed_items": "처리", "total_items": "전체",
                "success_items": "성공", "failed_items": "실패",
                "started_at": "시작", "finished_at": "종료", "message": "메시지",
            })
            st.dataframe(show, hide_index=True, use_container_width=True,
                           height=min(360, 52 + 35 * max(1, len(show))))


@st.fragment(run_every="5s")
def _render_mock_realtime_status_fragment(db_path: str) -> None:
    _render_mock_realtime_status_content(db_path)


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
    st.subheader("자동 모의 입찰 포트폴리오")
    st.caption(
        "진행 중 공고에 대해 예측값과 최근 낙찰 추세를 결합해 고객별 분산 투찰 포트폴리오를 자동 생성합니다. "
        "저장된 모의 입찰은 실제 결과 수집 시 자동 평가되어 낙찰 건수와 수익 집계에 반영됩니다."
    )
    _render_mock_realtime_status_fragment(db_path)

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

        r2 = st.columns(3)
        with r2[0]:
            num_customers = st.slider("내 고객 수", 1, 10, 5, key="sim_num_customers")
        with r2[1]:
            num_competitors = st.slider("경쟁사 수 (top-K)", 1, 15, 10, key="sim_num_competitors")
        with r2[2]:
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
                    cases, meta = _load_cases_adaptive(
                        db_path, notice.notice_id, notice.agency_name,
                        notice.category, notice.contract_method, notice.opened_at,
                    )
                    analyzer = _build_analyzer(cases, meta, target_win_probability)
                    prediction = NoticePredictor(analyzer).predict(notice)
                    _annotate_shrinkage_note(prediction.analysis.notes, meta)
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
                        historical_cases=cases,
                        n_customers=int(num_customers),
                    )
                    reports.append({
                        "notice": notice,
                        "report": report,
                        "parent_used": meta.get("parent_name"),
                        "shrinkage": meta,
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
                                "note": (
                                    "auto:trend-aware-quantile"
                                    f";portfolio_win_rate={report.our_win_rate:.3f}"
                                    f";uncertainty={report.uncertainty_score:.3f}"
                                ),
                                "customer_idx": cb.idx,
                            })
                except Exception as exc:  # noqa: BLE001
                    reports.append({"notice": notice, "error": str(exc)})
                progress.progress((i + 1) / len(notices),
                                    text=f"{i + 1}/{len(notices)} 처리 중...")
            progress.empty()
            if persist and batch_rows:
                replace_auto_mock_bid_batch(db_path, simulation_id, batch_rows)
                st.success(f"완료. {len(reports)}개 공고 포트폴리오 · 고객 입찰 {len(batch_rows)}건 저장 "
                             f"(simulation_id={simulation_id}).")
            else:
                st.success(f"완료. {len(reports)}개 공고 포트폴리오 생성 (DB 저장 안 함).")
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
            "추정 포트폴리오 승률": rep.our_win_rate,
            "최고고객#": rep.best_customer_idx or 0,
            "최고고객승률": rep.best_customer_win_rate or 0,
            "추정 낙찰가": rep.mean_winning_amount_when_we_win or 0,
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
        **{"추정 포트폴리오 승률": df["추정 포트폴리오 승률"].map(lambda v: f"{v * 100:.1f}%")},
        최고고객승률=df["최고고객승률"].map(lambda v: f"{v * 100:.1f}%"),
        추정_낙찰가=df["추정 낙찰가"].map(lambda v: f"{v:,.0f}"),
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
        {"고객#": c.idx, "역할": c.role, "목표 분위수": f"{(c.target_quantile or 0) * 100:.1f}%",
         "투찰률": f"{c.rate:.3f}%",
         "투찰금액": f"{c.amount:,.0f}원"}
        for c in rep.customers
    ])
    st.markdown("##### 내 고객 투찰 내역")
    st.dataframe(cust_df, hide_index=True, use_container_width=True)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("시장 중심", f"{(rep.market_center or 0):.3f}%")
    c2.metric("최근 드리프트", f"{(rep.market_drift or 0):+.3f}%p")
    c3.metric("불확실성", f"{rep.uncertainty_score:.3f}")
    c4.metric("추정 포트폴리오 승률", f"{rep.our_win_rate * 100:.1f}%")

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


def _render_search_tab(db_path: str, target_win_probability: float) -> None:
    st.markdown(
        """
        <div class='subpro-search-hero'>
          <h2>🔍 공고 검색</h2>
          <p>수요기관명 또는 공고번호로 전체 공고를 찾아봅니다.
          결과 행을 클릭하면 공고 상세·낙찰 결과가 뜨고, <b>approved</b> 매핑 기관이라면
          백테스트 꺾은선 차트까지 자동으로 나옵니다.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    c1, c2, c3 = st.columns([5, 2, 1.2])
    with c1:
        query = st.text_input(
            "검색어",
            key="search_query",
            placeholder="🔎 기관명 일부 또는 공고번호  (예: 한국수자원공사 / R25BK01145814)",
            label_visibility="collapsed",
        )
    with c2:
        category_label = st.selectbox(
            "구분",
            [label for label, _ in CATEGORY_DROPDOWN],
            index=0,
            key="search_category",
            label_visibility="collapsed",
        )
    with c3:
        limit = st.number_input(
            "최대 표시",
            min_value=20, max_value=500, value=100, step=20,
            key="search_limit",
            label_visibility="collapsed",
        )
    category = dict(CATEGORY_DROPDOWN)[category_label]

    needle = (query or "").strip()
    if not needle:
        st.markdown(
            "<div class='subpro-quiet'>검색어를 입력하세요. 공고번호는 부분 일치도 됩니다.</div>",
            unsafe_allow_html=True,
        )
        return

    with st.spinner(f"‘{needle}’ 검색 중..."):
        rows = search_notices(db_path, needle, category=category, limit=int(limit))
    if not rows:
        st.warning("결과가 없습니다. 검색어를 바꾸거나 구분을 다시 확인해 보세요.")
        return

    # Summary banner
    resolved = sum(1 for r in rows if r.get("has_result") and (r.get("bid_rate") or 0) > 0)
    pending = len(rows) - resolved
    st.markdown(
        "<div class='subpro-card-grid'>"
        + _render_stat_card("매칭 공고", _format_count(len(rows), "건"),
                             meta=f"검색어: {needle}", accent="#2563eb")
        + _render_stat_card("결과 있음", _format_count(resolved, "건"),
                             meta="낙찰가·낙찰업체 확인 가능", accent="#16a34a")
        + _render_stat_card("결과 대기", _format_count(pending, "건"),
                             meta="아직 낙찰 결과 없음", accent="#f59e0b")
        + "</div>",
        unsafe_allow_html=True,
    )

    df = pd.DataFrame(rows)
    display = df.assign(
        category_label=df["category"].map(_humanize_category),
        base_amount_fmt=df["base_amount"].map(lambda v: f"{v:,.0f}" if v else "–"),
        award_amount_fmt=df["award_amount"].map(lambda v: f"{v:,.0f}" if pd.notna(v) else "–"),
        bid_rate_fmt=df["bid_rate"].map(lambda v: f"{v:.2f}%" if pd.notna(v) and v > 0 else "–"),
        result_badge=df.apply(
            lambda r: "✅ 낙찰" if (r.get("has_result") and (r.get("bid_rate") or 0) > 0)
            else "⏳ 대기",
            axis=1,
        ),
    )[
        [
            "opened_at", "result_badge", "notice_id", "agency_name",
            "category_label", "contract_method",
            "base_amount_fmt", "award_amount_fmt", "bid_rate_fmt", "winning_company",
        ]
    ]
    display.columns = [
        "개찰일", "결과", "공고번호", "수요기관",
        "구분", "방법",
        "예산", "낙찰가", "낙찰률", "낙찰업체",
    ]

    selection = st.dataframe(
        display,
        hide_index=True,
        use_container_width=True,
        height=min(480, 52 + 35 * max(1, len(display))),
        on_select="rerun",
        selection_mode="single-row",
        key="search_results_table",
    )
    try:
        idx = selection.selection.rows[0]  # type: ignore[attr-defined]
    except Exception:
        idx = None
    if idx is None:
        st.markdown(
            "<div class='subpro-quiet'>위 표에서 공고 행을 클릭하면 아래에 상세 패널이 열립니다.</div>",
            unsafe_allow_html=True,
        )
        return

    picked = df.iloc[int(idx)].to_dict()
    _render_search_detail(db_path, picked, target_win_probability)


def _render_search_detail(db_path: str, row: dict, target_win_probability: float) -> None:
    notice_id = row["notice_id"]
    agency_name = row.get("agency_name") or ""
    category = row.get("category") or ""
    method = row.get("contract_method") or ""
    base = float(row.get("base_amount") or 0)
    opened = row.get("opened_at") or "–"
    region = row.get("region") or "–"
    floor = row.get("floor_rate")

    has_result = bool(row.get("has_result") and (row.get("bid_rate") or 0) > 0)
    result_html = (
        "<div class='subpro-result-highlight subpro-result-won'>"
        f"✅ 낙찰 완료 · {row.get('winning_company') or '(업체명 없음)'} · "
        f"{(row.get('award_amount') or 0):,.0f}원 · {row.get('bid_rate'):.2f}%"
        "</div>"
    ) if has_result else (
        "<div class='subpro-result-highlight subpro-result-pending'>"
        "⏳ 낙찰 결과 아직 없음 (진행 중 또는 DB 미반영)"
        "</div>"
    )

    st.markdown(
        "<div class='subpro-notice-hero'>"
        "<div class='subpro-notice-hero-top'>"
        f"<div class='subpro-notice-agency'>{agency_name or '(기관명 없음)'}</div>"
        f"<div class='subpro-notice-id'>{notice_id}</div>"
        "</div>"
        "<div class='subpro-notice-sub'>"
        f"<span>📅 <b>{opened}</b></span>"
        f"<span>📂 <b>{_humanize_category(category) or '–'}</b></span>"
        f"<span>⚖️ <b>{method or '–'}</b></span>"
        f"<span>📍 <b>{region}</b></span>"
        "</div>"
        f"{result_html}"
        "</div>",
        unsafe_allow_html=True,
    )

    # Notice / Result key figures
    st.markdown(
        "<div class='subpro-section-title'>공고 기본 정보</div>",
        unsafe_allow_html=True,
    )
    info_cards = [
        _render_stat_card("예산(추정가)", f"{base:,.0f}", meta="원",
                           accent="#2563eb"),
        _render_stat_card("하한율", f"{float(floor):.3f}%" if floor else "미지정",
                           meta="낙찰 최저율", accent="#7c3aed"),
        _render_stat_card("기관코드", row.get("agency_code") or "–",
                           meta="수요기관 식별자", accent="#0891b2"),
        _render_stat_card("나라장터", "🔗 공고 열기",
                           meta=f"{_build_g2b_detail_url(notice_id, category)}",
                           accent="#0f766e"),
    ]
    st.markdown(
        "<div class='subpro-card-grid'>" + "".join(info_cards) + "</div>",
        unsafe_allow_html=True,
    )
    st.markdown(
        f"[🔗 나라장터에서 열기]({_build_g2b_detail_url(notice_id, category)})"
    )

    st.markdown(
        "<div class='subpro-section-title'>🏆 낙찰 결과</div>",
        unsafe_allow_html=True,
    )
    if has_result:
        spread = None
        if base > 0 and (row.get("award_amount") or 0) > 0:
            spread = base - float(row["award_amount"])
        result_cards = [
            _render_stat_card("낙찰업체", row.get("winning_company") or "–",
                               meta=f"사업자번호 {row.get('winner_biz_no') or '–'}",
                               accent="#16a34a"),
            _render_stat_card("낙찰가", f"{(row.get('award_amount') or 0):,.0f}",
                               meta="원", accent="#0f766e"),
            _render_stat_card("낙찰률", f"{row['bid_rate']:.2f}%",
                               meta=("낙찰가/추정가 × 100"), accent="#7c3aed"),
            _render_stat_card("예산 대비 절감", f"{spread:,.0f}" if spread is not None else "–",
                               meta="원 (추정가 − 낙찰가)", accent="#ea580c"),
        ]
        st.markdown(
            "<div class='subpro-card-grid'>" + "".join(result_cards) + "</div>",
            unsafe_allow_html=True,
        )
        if row.get("bidder_count"):
            st.markdown(
                f"<div class='subpro-quiet'>참여업체 수 (API 반영 시): <b>{row['bidder_count']}</b></div>",
                unsafe_allow_html=True,
            )
    else:
        st.markdown(
            "<div class='subpro-quiet'>아직 낙찰 결과가 DB에 들어오지 않았습니다. "
            "다음 일일 수집 또는 CSV 임포트 시 자동으로 반영됩니다.</div>",
            unsafe_allow_html=True,
        )

    # Demand-agency backtest block
    st.markdown(
        "<div class='subpro-section-title'>📈 수요기관 백테스트 <span class='subpro-section-meta'>approved 매핑 기관만</span></div>",
        unsafe_allow_html=True,
    )
    if not agency_name:
        st.markdown(
            "<div class='subpro-quiet'>기관명이 없는 공고라 건너뜀.</div>",
            unsafe_allow_html=True,
        )
        return
    mapping = get_agency_parent(db_path, agency_name)
    approved = bool(mapping and mapping["status"] == "approved")
    if not approved:
        status_label = (mapping["status"] if mapping else "미등록")
        parent_label = (mapping["parent_name"] if mapping else "–")
        st.markdown(
            "<div class='subpro-notice-hero' style='background:linear-gradient(135deg,#fef3c7,#fff7ed);border-color:rgba(217,119,6,0.25);'>"
            f"<div class='subpro-notice-agency'>🧩 부모 통합 매핑이 `{status_label}` 상태입니다</div>"
            f"<div class='subpro-notice-sub'>"
            f"<span>대상: <b>{agency_name}</b></span>"
            f"<span>부모 후보: <b>{parent_label}</b></span>"
            "</div>"
            "<div class='subpro-quiet' style='margin-top:6px;'>🧩 기관 통합 관리 탭에서 승인하면 여기서 "
            "꺾은선 차트가 표시됩니다.</div>"
            "</div>",
            unsafe_allow_html=True,
        )
        return

    with st.spinner(f"`{agency_name}` 백테스트 계산 중..."):
        df_back = _load_rows_for_agency(db_path, agency_name, category or None, target_win_probability)
    if df_back.empty:
        st.warning("백테스트 대상 과거 공고가 없습니다.")
        return
    _render_summary(df_back)
    _render_chart(df_back)
    _render_table(df_back)


def main() -> None:
    st.set_page_config(page_title="G2B 입찰 예측 대시보드", layout="wide")
    _inject_dashboard_styles()
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

    init_db(db_path)
    db_label = "로컬 수동 DB" if str(Path(db_path)) == "data/bids.db" else "운영 runner DB"
    st.caption(f"현재 연결 DB: `{db_path}` · {db_label}")

    # Only auto-refresh the summary/monitoring panels when ingestion is
    # actively running — idle sessions render once and stay still.
    if _is_auto_bid_running(db_path):
        _render_operations_summary_fragment(db_path)
        _render_monitoring_panel(db_path)
    else:
        _render_operations_summary(db_path)
        _render_realtime_status_content(db_path)

    view = st.segmented_control(
        "화면",
        options=[
            "📝 진행 중 공고",
            "🔍 공고 검색",
            "🎯 모의 입찰",
            "🧩 기관 통합 관리",
            "📈 주간 리뷰",
        ],
        default="📝 진행 중 공고",
        key="main_view",
    )

    if view == "📝 진행 중 공고":
        _render_live_view(db_path, target_win_probability)
    elif view == "🔍 공고 검색":
        _render_search_tab(db_path, target_win_probability)
    elif view == "🎯 모의 입찰":
        _render_mock_tab(db_path, target_win_probability)
    elif view == "🧩 기관 통합 관리":
        _render_mapping_tab(db_path)
    elif view == "📈 주간 리뷰":
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
