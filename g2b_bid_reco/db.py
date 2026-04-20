from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

from .models import ActualAwardOutcome, BidNoticeSnapshot, HistoricalBidCase


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS procurement_plans (
    plan_id TEXT PRIMARY KEY,
    agency_name TEXT NOT NULL,
    agency_code TEXT DEFAULT '',
    category TEXT NOT NULL,
    budget_amount REAL NOT NULL,
    planned_quarter TEXT,
    contract_method TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS demand_agencies (
    agency_code TEXT PRIMARY KEY,
    agency_name TEXT NOT NULL DEFAULT '',
    top_agency_code TEXT NOT NULL DEFAULT '',
    top_agency_name TEXT NOT NULL DEFAULT '',
    jurisdiction_type TEXT NOT NULL DEFAULT '',
    address TEXT NOT NULL DEFAULT '',
    road_address TEXT NOT NULL DEFAULT '',
    postal_code TEXT NOT NULL DEFAULT '',
    source TEXT NOT NULL DEFAULT 'observed',
    raw_json TEXT NOT NULL DEFAULT '',
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bid_notices (
    notice_id TEXT PRIMARY KEY,
    agency_name TEXT NOT NULL,
    agency_code TEXT DEFAULT '',
    category TEXT NOT NULL,
    contract_method TEXT NOT NULL,
    region TEXT NOT NULL,
    base_amount REAL NOT NULL,
    estimated_amount REAL,
    floor_rate REAL,
    opened_at TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bid_results (
    notice_id TEXT PRIMARY KEY REFERENCES bid_notices(notice_id),
    winning_company TEXT,
    winner_biz_no TEXT DEFAULT '',
    award_amount REAL NOT NULL,
    bid_rate REAL NOT NULL,
    bidder_count INTEGER NOT NULL,
    result_status TEXT NOT NULL DEFAULT 'awarded',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS contracts (
    contract_id TEXT PRIMARY KEY,
    notice_id TEXT NOT NULL REFERENCES bid_notices(notice_id),
    contract_amount REAL NOT NULL,
    contract_date TEXT,
    changed_amount REAL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS feature_snapshots (
    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
    notice_id TEXT NOT NULL REFERENCES bid_notices(notice_id),
    feature_key TEXT NOT NULL,
    feature_value TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS agency_parent_mapping (
    agency_name TEXT PRIMARY KEY,
    parent_name TEXT NOT NULL DEFAULT '',
    subunit_count INTEGER NOT NULL DEFAULT 0,
    agency_case_count INTEGER NOT NULL DEFAULT 0,
    parent_case_count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending',
    source TEXT NOT NULL DEFAULT 'auto',
    note TEXT NOT NULL DEFAULT '',
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS mock_bids (
    mock_id INTEGER PRIMARY KEY AUTOINCREMENT,
    notice_id TEXT NOT NULL,
    bid_amount REAL NOT NULL,
    bid_rate REAL NOT NULL,
    predicted_amount REAL,
    predicted_rate REAL,
    note TEXT NOT NULL DEFAULT '',
    submitted_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS metrics_snapshots (
    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date TEXT NOT NULL UNIQUE,
    notices_total INTEGER DEFAULT 0,
    notices_new_7d INTEGER DEFAULT 0,
    results_total INTEGER DEFAULT 0,
    approved_mappings INTEGER DEFAULT 0,
    pending_mappings INTEGER DEFAULT 0,
    sim_batches INTEGER DEFAULT 0,
    mock_bids_total INTEGER DEFAULT 0,
    mock_wins INTEGER DEFAULT 0,
    mock_lost INTEGER DEFAULT 0,
    mock_pending INTEGER DEFAULT 0,
    mock_disqualified INTEGER DEFAULT 0,
    win_rate REAL DEFAULT 0,
    revenue_total REAL DEFAULT 0,
    revenue_7d REAL DEFAULT 0,
    fee_rate REAL DEFAULT 0,
    note TEXT DEFAULT '',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS improvement_suggestions (
    suggestion_id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    rationale TEXT NOT NULL DEFAULT '',
    impact TEXT NOT NULL DEFAULT 'medium',
    status TEXT NOT NULL DEFAULT 'proposed',
    source TEXT NOT NULL DEFAULT 'manual',
    metric_snapshot_id INTEGER,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    note TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS automation_daily_stats (
    stat_date TEXT PRIMARY KEY,
    collect_api_calls INTEGER NOT NULL DEFAULT 0,
    agency_api_calls INTEGER NOT NULL DEFAULT 0,
    auto_bid_runs INTEGER NOT NULL DEFAULT 0,
    auto_bid_notices INTEGER NOT NULL DEFAULT 0,
    auto_bid_customer_bids INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS automation_runs (
    run_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    total_items INTEGER NOT NULL DEFAULT 0,
    processed_items INTEGER NOT NULL DEFAULT 0,
    success_items INTEGER NOT NULL DEFAULT 0,
    failed_items INTEGER NOT NULL DEFAULT 0,
    resumed_items INTEGER NOT NULL DEFAULT 0,
    message TEXT NOT NULL DEFAULT '',
    started_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    finished_at TEXT
);

CREATE TABLE IF NOT EXISTS automation_run_tasks (
    task_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    category TEXT NOT NULL DEFAULT '',
    contract_method TEXT NOT NULL DEFAULT '',
    task_seq INTEGER NOT NULL DEFAULT 0,
    total_items INTEGER NOT NULL DEFAULT 0,
    processed_items INTEGER NOT NULL DEFAULT 0,
    success_items INTEGER NOT NULL DEFAULT 0,
    failed_items INTEGER NOT NULL DEFAULT 0,
    resumed_items INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'queued',
    message TEXT NOT NULL DEFAULT '',
    started_at TEXT,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    finished_at TEXT
);

CREATE TABLE IF NOT EXISTS notice_prediction_cache (
    notice_id TEXT PRIMARY KEY REFERENCES bid_notices(notice_id),
    cache_key TEXT NOT NULL DEFAULT '',
    target_win_probability REAL NOT NULL DEFAULT 0,
    predicted_amount REAL,
    predicted_rate REAL,
    lower_rate REAL,
    upper_rate REAL,
    estimated_win_probability REAL NOT NULL DEFAULT 0,
    confidence TEXT NOT NULL DEFAULT '',
    agency_cases INTEGER NOT NULL DEFAULT 0,
    peer_cases INTEGER NOT NULL DEFAULT 0,
    lookback_years_used INTEGER,
    parent_used TEXT NOT NULL DEFAULT '',
    analysis_notes TEXT NOT NULL DEFAULT '',
    computed_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS mock_bid_evaluations (
    mock_id INTEGER PRIMARY KEY REFERENCES mock_bids(mock_id) ON DELETE CASCADE,
    notice_id TEXT NOT NULL,
    simulation_id TEXT NOT NULL DEFAULT '',
    customer_idx INTEGER NOT NULL DEFAULT 0,
    bid_amount REAL NOT NULL DEFAULT 0,
    bid_rate REAL NOT NULL DEFAULT 0,
    verdict TEXT NOT NULL DEFAULT 'pending',
    actual_amount REAL NOT NULL DEFAULT 0,
    actual_rate REAL NOT NULL DEFAULT 0,
    winning_company TEXT NOT NULL DEFAULT '',
    result_status TEXT NOT NULL DEFAULT '',
    result_created_at TEXT,
    evaluated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS strategy_tables (
    agency_name TEXT NOT NULL DEFAULT '',
    category TEXT NOT NULL DEFAULT '',
    contract_method TEXT NOT NULL DEFAULT '',
    region TEXT NOT NULL DEFAULT '',
    n_customers INTEGER NOT NULL,
    quantiles_json TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'montecarlo',
    sample_size INTEGER NOT NULL DEFAULT 0,
    win_rate_estimate REAL,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (agency_name, category, contract_method, region, n_customers)
);
"""


def connect(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def _run_write_with_retry(
    db_path: str | Path,
    action,
    *,
    attempts: int = 5,
    initial_sleep_sec: float = 0.2,
) -> None:
    last_exc: sqlite3.OperationalError | None = None
    for attempt in range(attempts):
        try:
            with connect(db_path) as conn:
                action(conn)
            return
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower():
                raise
            last_exc = exc
            if attempt >= attempts - 1:
                break
            time.sleep(initial_sleep_sec * (attempt + 1))
    if last_exc is not None:
        raise last_exc


def fail_stale_automation_runs(
    db_path: str | Path,
    *,
    kind: str,
    stale_after_minutes: int = 10,
    note: str = "stale run closed before new start",
) -> int:
    updated = 0

    def _action(conn: sqlite3.Connection) -> None:
        nonlocal updated
        cur = conn.execute(
            """
            UPDATE automation_runs
            -- Preserve partial progress for resume/monitoring instead of
            -- collapsing every stale run into a hard failure.
            SET status = CASE WHEN success_items > 0 THEN 'partial' ELSE 'failed' END,
                finished_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP,
                message = CASE
                    WHEN message = '' THEN ?
                    ELSE message || ' | ' || ?
                END
            WHERE kind = ?
              AND status = 'running'
              AND updated_at <= datetime('now', ?)
            """,
            (note, note, kind, f"-{max(1, stale_after_minutes)} minutes"),
        )
        updated = int(cur.rowcount or 0)

    _run_write_with_retry(db_path, _action)
    return updated


def init_db(db_path: str | Path) -> None:
    with connect(db_path) as conn:
        conn.executescript(SCHEMA_SQL)
        _ensure_column(conn, "demand_agencies", "top_agency_code", "TEXT DEFAULT ''")
        _ensure_column(conn, "demand_agencies", "top_agency_name", "TEXT DEFAULT ''")
        _ensure_column(conn, "demand_agencies", "jurisdiction_type", "TEXT DEFAULT ''")
        _ensure_column(conn, "demand_agencies", "address", "TEXT DEFAULT ''")
        _ensure_column(conn, "demand_agencies", "road_address", "TEXT DEFAULT ''")
        _ensure_column(conn, "demand_agencies", "postal_code", "TEXT DEFAULT ''")
        _ensure_column(conn, "demand_agencies", "source", "TEXT DEFAULT 'observed'")
        _ensure_column(conn, "demand_agencies", "raw_json", "TEXT DEFAULT ''")
        _ensure_column(conn, "bid_notices", "agency_code", "TEXT DEFAULT ''")
        _ensure_column(conn, "procurement_plans", "agency_code", "TEXT DEFAULT ''")
        _ensure_column(conn, "bid_results", "winner_biz_no", "TEXT DEFAULT ''")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_demand_agencies_name ON demand_agencies(agency_name)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_demand_agencies_top ON demand_agencies(top_agency_code)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_notices_cat_method ON bid_notices(category, contract_method)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_notices_agency ON bid_notices(agency_name)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_notices_opened_at ON bid_notices(opened_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_results_winner_biz ON bid_results(winner_biz_no)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_parent_mapping_parent ON agency_parent_mapping(parent_name)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_parent_mapping_status ON agency_parent_mapping(status)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_mock_bids_notice ON mock_bids(notice_id)"
        )
        _ensure_column(conn, "mock_bids", "simulation_id", "TEXT DEFAULT ''")
        _ensure_column(conn, "mock_bids", "customer_idx", "INTEGER DEFAULT 0")
        _ensure_column(conn, "mock_bids", "n_customers", "INTEGER DEFAULT 0")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_mock_bids_simulation ON mock_bids(simulation_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_mock_bids_sim_n ON mock_bids(simulation_id, n_customers)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_automation_daily_stats_updated_at ON automation_daily_stats(updated_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_automation_runs_kind_started ON automation_runs(kind, started_at DESC)"
        )
        _ensure_column(conn, "automation_runs", "resumed_items", "INTEGER NOT NULL DEFAULT 0")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_automation_run_tasks_run ON automation_run_tasks(run_id, task_seq)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_automation_run_tasks_status ON automation_run_tasks(status, updated_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_notice_prediction_cache_computed_at ON notice_prediction_cache(computed_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_mock_bid_evaluations_notice ON mock_bid_evaluations(notice_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_mock_bid_evaluations_eval_at ON mock_bid_evaluations(evaluated_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_mock_bid_evaluations_verdict ON mock_bid_evaluations(verdict, evaluated_at DESC)"
        )
        _ensure_column(conn, "mock_bid_evaluations", "n_customers", "INTEGER DEFAULT 0")
        _backfill_n_customers(conn)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_strategy_tables_scope "
            "ON strategy_tables(category, contract_method, n_customers)"
        )


def _backfill_n_customers(conn: sqlite3.Connection) -> None:
    """Populate n_customers from max(customer_idx) per simulation_id.

    Idempotent and bounded: only runs UPDATE if at least one candidate row
    exists (probe via indexed lookup on simulation_id). Skips legacy rows
    with empty simulation_id since their customer_idx is also 0.
    """
    probe = conn.execute(
        "SELECT 1 FROM mock_bids "
        "WHERE simulation_id != '' AND n_customers = 0 LIMIT 1"
    ).fetchone()
    if probe is not None:
        conn.execute(
            """
            UPDATE mock_bids
            SET n_customers = (
                SELECT MAX(m2.customer_idx)
                FROM mock_bids m2
                WHERE m2.simulation_id = mock_bids.simulation_id
            )
            WHERE simulation_id != '' AND n_customers = 0
            """
        )
    probe = conn.execute(
        "SELECT 1 FROM mock_bid_evaluations "
        "WHERE simulation_id != '' AND n_customers = 0 LIMIT 1"
    ).fetchone()
    if probe is not None:
        conn.execute(
            """
            UPDATE mock_bid_evaluations
            SET n_customers = (
                SELECT MAX(e2.customer_idx)
                FROM mock_bid_evaluations e2
                WHERE e2.simulation_id = mock_bid_evaluations.simulation_id
            )
            WHERE simulation_id != '' AND n_customers = 0
            """
        )


def _ensure_column(
    conn: sqlite3.Connection, table: str, column: str, type_decl: str
) -> None:
    existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
    if column in existing:
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {type_decl}")


def bump_automation_daily_stats(
    db_path: str | Path,
    *,
    collect_api_calls: int = 0,
    agency_api_calls: int = 0,
    auto_bid_runs: int = 0,
    auto_bid_notices: int = 0,
    auto_bid_customer_bids: int = 0,
    stat_date: str | None = None,
) -> None:
    def _action(conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            INSERT INTO automation_daily_stats (
                stat_date, collect_api_calls, agency_api_calls,
                auto_bid_runs, auto_bid_notices, auto_bid_customer_bids,
                updated_at
            ) VALUES (
                COALESCE(?, date('now', 'localtime')), ?, ?, ?, ?, ?, CURRENT_TIMESTAMP
            )
            ON CONFLICT(stat_date) DO UPDATE SET
                collect_api_calls = automation_daily_stats.collect_api_calls + excluded.collect_api_calls,
                agency_api_calls = automation_daily_stats.agency_api_calls + excluded.agency_api_calls,
                auto_bid_runs = automation_daily_stats.auto_bid_runs + excluded.auto_bid_runs,
                auto_bid_notices = automation_daily_stats.auto_bid_notices + excluded.auto_bid_notices,
                auto_bid_customer_bids = automation_daily_stats.auto_bid_customer_bids + excluded.auto_bid_customer_bids,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                stat_date,
                max(0, int(collect_api_calls)),
                max(0, int(agency_api_calls)),
                max(0, int(auto_bid_runs)),
                max(0, int(auto_bid_notices)),
                max(0, int(auto_bid_customer_bids)),
            ),
        )
    _run_write_with_retry(db_path, _action)


def start_automation_run(
    db_path: str | Path,
    *,
    run_id: str,
    kind: str,
    total_items: int = 0,
    resumed_items: int = 0,
    message: str = "",
) -> None:
    fail_stale_automation_runs(db_path, kind=kind)
    fail_stale_run_tasks(db_path, kind=kind)

    def _action(conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            INSERT OR REPLACE INTO automation_runs (
                run_id, kind, status, total_items, processed_items,
                success_items, failed_items, resumed_items, message,
                started_at, updated_at, finished_at
            ) VALUES (?, ?, 'running', ?, ?, ?, 0, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL)
            """,
            (
                run_id, kind, max(0, int(total_items)),
                max(0, int(resumed_items)), max(0, int(resumed_items)),
                max(0, int(resumed_items)), message,
            ),
        )
    _run_write_with_retry(db_path, _action)


def update_automation_run(
    db_path: str | Path,
    *,
    run_id: str,
    processed_items: int | None = None,
    success_items: int | None = None,
    failed_items: int | None = None,
    total_items: int | None = None,
    resumed_items: int | None = None,
    message: str | None = None,
) -> None:
    sets: list[str] = ["updated_at = CURRENT_TIMESTAMP"]
    params: list = []
    if processed_items is not None:
        sets.append("processed_items = ?")
        params.append(max(0, int(processed_items)))
    if success_items is not None:
        sets.append("success_items = ?")
        params.append(max(0, int(success_items)))
    if resumed_items is not None:
        sets.append("resumed_items = ?")
        params.append(max(0, int(resumed_items)))
    if failed_items is not None:
        sets.append("failed_items = ?")
        params.append(max(0, int(failed_items)))
    if total_items is not None:
        sets.append("total_items = ?")
        params.append(max(0, int(total_items)))
    if message is not None:
        sets.append("message = ?")
        params.append(message)
    if len(sets) == 1:
        return
    params.append(run_id)

    def _action(conn: sqlite3.Connection) -> None:
        conn.execute(
            f"UPDATE automation_runs SET {', '.join(sets)} WHERE run_id = ?",
            params,
        )
    _run_write_with_retry(db_path, _action)


def finish_automation_run(
    db_path: str | Path,
    *,
    run_id: str,
    status: str,
    processed_items: int | None = None,
    success_items: int | None = None,
    failed_items: int | None = None,
    message: str = "",
) -> None:
    # Run/task state model uses partial/cancelled to distinguish incomplete but
    # usable results from hard failures.
    if status not in {"completed", "partial", "failed", "cancelled"}:
        raise ValueError(f"invalid automation run status: {status}")
    sets = [
        "status = ?",
        "updated_at = CURRENT_TIMESTAMP",
        "finished_at = CURRENT_TIMESTAMP",
        "message = ?",
    ]
    params: list = [status, message]
    if processed_items is not None:
        sets.append("processed_items = ?")
        params.append(max(0, int(processed_items)))
    if success_items is not None:
        sets.append("success_items = ?")
        params.append(max(0, int(success_items)))
    if failed_items is not None:
        sets.append("failed_items = ?")
        params.append(max(0, int(failed_items)))
    params.append(run_id)

    def _action(conn: sqlite3.Connection) -> None:
        conn.execute(
            f"UPDATE automation_runs SET {', '.join(sets)} WHERE run_id = ?",
            params,
        )
    _run_write_with_retry(db_path, _action)


def get_latest_automation_run(
    db_path: str | Path,
    kind: str,
) -> sqlite3.Row | None:
    with connect(db_path) as conn:
        return conn.execute(
            """
            SELECT run_id, kind, status, total_items, processed_items,
                   success_items, failed_items, resumed_items, message,
                   started_at, updated_at, finished_at
            FROM automation_runs
            WHERE kind = ?
            -- Fresh running rows should win. Once a run is stale, prefer a
            -- newer completed result, then partial, before falling back to
            -- stale running/failed rows in the dashboard.
            ORDER BY CASE
                         WHEN status = 'running'
                              AND COALESCE(updated_at, started_at) >= datetime('now', '-3 minutes')
                         THEN 0
                         WHEN status = 'completed' THEN 1
                         WHEN status = 'partial' THEN 2
                         WHEN status = 'cancelled' THEN 3
                         WHEN status = 'failed' THEN 4
                         ELSE 5
                     END,
                     COALESCE(updated_at, started_at) DESC,
                     started_at DESC
            LIMIT 1
            """,
            (kind,),
        ).fetchone()


def list_latest_automation_runs(
    db_path: str | Path,
    kinds: list[str],
) -> list[dict]:
    if not kinds:
        return []
    placeholders = ",".join("?" for _ in kinds)
    with connect(db_path) as conn:
        rows = conn.execute(
            f"""
            WITH ranked AS (
                SELECT run_id, kind, status, total_items, processed_items,
                       success_items, failed_items, message,
                       started_at, updated_at, finished_at,
                       ROW_NUMBER() OVER (PARTITION BY kind ORDER BY started_at DESC) AS rn
                FROM automation_runs
                WHERE kind IN ({placeholders})
            )
            SELECT run_id, kind, status, total_items, processed_items,
                   success_items, failed_items, message,
                   started_at, updated_at, finished_at
            FROM ranked
            WHERE rn = 1
            ORDER BY kind ASC
            """,
            kinds,
        ).fetchall()
    return [dict(row) for row in rows]


TASK_STATUSES_VALID = {"queued", "running", "completed", "partial", "failed", "cancelled"}


def create_run_task(
    db_path: str | Path,
    *,
    task_id: str,
    run_id: str,
    kind: str,
    category: str,
    contract_method: str,
    task_seq: int,
    total_items: int,
    resumed_items: int = 0,
) -> None:
    def _action(conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            INSERT OR REPLACE INTO automation_run_tasks (
                task_id, run_id, kind, category, contract_method, task_seq,
                total_items, processed_items, success_items, failed_items,
                resumed_items, status, message, started_at, updated_at, finished_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, 0, ?, 'queued', '', NULL, CURRENT_TIMESTAMP, NULL)
            """,
            (
                task_id, run_id, kind, category, contract_method,
                int(task_seq), int(total_items), int(resumed_items),
            ),
        )
    _run_write_with_retry(db_path, _action)


def start_run_task(db_path: str | Path, *, task_id: str, message: str = "") -> None:
    def _action(conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            UPDATE automation_run_tasks
            SET status='running',
                started_at = COALESCE(started_at, CURRENT_TIMESTAMP),
                updated_at = CURRENT_TIMESTAMP,
                message = CASE WHEN ?='' THEN message ELSE ? END
            WHERE task_id = ?
            """,
            (message, message, task_id),
        )
    _run_write_with_retry(db_path, _action)


def heartbeat_run_task(
    db_path: str | Path,
    *,
    task_id: str,
    processed_items: int,
    success_items: int,
    failed_items: int,
    message: str = "",
) -> None:
    def _action(conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            UPDATE automation_run_tasks
            SET processed_items = ?,
                success_items = ?,
                failed_items = ?,
                message = CASE WHEN ?='' THEN message ELSE ? END,
                updated_at = CURRENT_TIMESTAMP
            WHERE task_id = ?
            """,
            (int(processed_items), int(success_items), int(failed_items),
             message, message, task_id),
        )
    _run_write_with_retry(db_path, _action)


def finish_run_task(
    db_path: str | Path,
    *,
    task_id: str,
    status: str,
    message: str = "",
    processed_items: int | None = None,
    success_items: int | None = None,
    failed_items: int | None = None,
) -> None:
    if status not in TASK_STATUSES_VALID:
        raise ValueError(f"invalid task status: {status}")
    sets = [
        "status = ?",
        "message = CASE WHEN ?='' THEN message ELSE ? END",
        "finished_at = CURRENT_TIMESTAMP",
        "updated_at = CURRENT_TIMESTAMP",
    ]
    params: list = [status, message, message]
    if processed_items is not None:
        sets.append("processed_items = ?")
        params.append(int(processed_items))
    if success_items is not None:
        sets.append("success_items = ?")
        params.append(int(success_items))
    if failed_items is not None:
        sets.append("failed_items = ?")
        params.append(int(failed_items))
    params.append(task_id)

    def _action(conn: sqlite3.Connection) -> None:
        conn.execute(
            f"UPDATE automation_run_tasks SET {', '.join(sets)} WHERE task_id = ?",
            params,
        )
    _run_write_with_retry(db_path, _action)


def list_run_tasks(db_path: str | Path, run_id: str) -> list[dict]:
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT task_id, run_id, kind, category, contract_method, task_seq,
                   total_items, processed_items, success_items, failed_items,
                   resumed_items, status, message,
                   started_at, updated_at, finished_at
            FROM automation_run_tasks
            WHERE run_id = ?
            ORDER BY task_seq ASC
            """,
            (run_id,),
        ).fetchall()
    return [dict(row) for row in rows]


def summarize_run_tasks(db_path: str | Path, run_id: str) -> dict:
    """Aggregated rollup of tasks for the dashboard source-of-truth."""
    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) AS task_count,
                   SUM(CASE WHEN status='running' THEN 1 ELSE 0 END) AS running_tasks,
                   SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed_tasks,
                   SUM(CASE WHEN status='partial' THEN 1 ELSE 0 END) AS partial_tasks,
                   SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed_tasks,
                   SUM(CASE WHEN status='queued' THEN 1 ELSE 0 END) AS queued_tasks,
                   COALESCE(SUM(total_items), 0) AS total_items,
                   COALESCE(SUM(processed_items), 0) AS processed_items,
                   COALESCE(SUM(success_items), 0) AS success_items,
                   COALESCE(SUM(failed_items), 0) AS failed_items,
                   COALESCE(SUM(resumed_items), 0) AS resumed_items,
                   MAX(updated_at) AS last_update
            FROM automation_run_tasks
            WHERE run_id = ?
            """,
            (run_id,),
        ).fetchone()
    if row is None:
        return {}
    return dict(row)


def fail_stale_run_tasks(
    db_path: str | Path,
    *,
    kind: str,
    stale_after_minutes: int = 10,
    note: str = "stale task closed",
) -> int:
    updated = 0

    def _action(conn: sqlite3.Connection) -> None:
        nonlocal updated
        cur = conn.execute(
            """
            UPDATE automation_run_tasks
            SET status = CASE WHEN success_items > 0 THEN 'partial' ELSE 'failed' END,
                finished_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP,
                message = CASE WHEN message='' THEN ? ELSE message || ' | ' || ? END
            WHERE kind = ?
              AND status IN ('running', 'queued')
              AND updated_at <= datetime('now', ?)
            """,
            (note, note, kind, f"-{max(1, stale_after_minutes)} minutes"),
        )
        updated = int(cur.rowcount or 0)
    _run_write_with_retry(db_path, _action)
    return updated


def _run_duration_minutes(run: dict) -> float | None:
    started = run.get("started_at")
    ended = run.get("finished_at") or run.get("updated_at")
    if not started or not ended:
        return None
    with connect(":memory:") as conn:
        row = conn.execute(
            "SELECT (julianday(?) - julianday(?)) * 24.0 * 60.0",
            (ended, started),
        ).fetchone()
    return float(row[0]) if row and row[0] is not None else None


def _daily_notice_baseline(db_path: str | Path, lookback_days: int = 21) -> float | None:
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT date(created_at, 'localtime') AS day, COUNT(*) AS c
            FROM bid_notices
            WHERE date(created_at, 'localtime') >= date('now', 'localtime', ?)
              AND date(created_at, 'localtime') < date('now', 'localtime')
            GROUP BY day
            ORDER BY day DESC
            """,
            (f"-{max(1, lookback_days)} days",),
        ).fetchall()
    counts = [int(row["c"]) for row in rows if row["c"] is not None]
    if not counts:
        return None
    counts.sort()
    mid = len(counts) // 2
    if len(counts) % 2:
        return float(counts[mid])
    return (counts[mid - 1] + counts[mid]) / 2.0


def upsert_demand_agency(
    conn: sqlite3.Connection,
    agency_code: str,
    agency_name: str = "",
    top_agency_code: str = "",
    top_agency_name: str = "",
    jurisdiction_type: str = "",
    address: str = "",
    road_address: str = "",
    postal_code: str = "",
    source: str = "observed",
    raw_json: str = "",
) -> None:
    code = (agency_code or "").strip()
    if not code:
        return
    conn.execute(
        """
        INSERT INTO demand_agencies (
            agency_code, agency_name, top_agency_code, top_agency_name,
            jurisdiction_type, address, road_address, postal_code,
            source, raw_json, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(agency_code) DO UPDATE SET
            agency_name = CASE WHEN excluded.agency_name != '' THEN excluded.agency_name ELSE demand_agencies.agency_name END,
            top_agency_code = CASE WHEN excluded.top_agency_code != '' THEN excluded.top_agency_code ELSE demand_agencies.top_agency_code END,
            top_agency_name = CASE WHEN excluded.top_agency_name != '' THEN excluded.top_agency_name ELSE demand_agencies.top_agency_name END,
            jurisdiction_type = CASE WHEN excluded.jurisdiction_type != '' THEN excluded.jurisdiction_type ELSE demand_agencies.jurisdiction_type END,
            address = CASE WHEN excluded.address != '' THEN excluded.address ELSE demand_agencies.address END,
            road_address = CASE WHEN excluded.road_address != '' THEN excluded.road_address ELSE demand_agencies.road_address END,
            postal_code = CASE WHEN excluded.postal_code != '' THEN excluded.postal_code ELSE demand_agencies.postal_code END,
            source = CASE WHEN excluded.source != '' THEN excluded.source ELSE demand_agencies.source END,
            raw_json = CASE WHEN excluded.raw_json != '' THEN excluded.raw_json ELSE demand_agencies.raw_json END,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            code,
            agency_name,
            top_agency_code,
            top_agency_name,
            jurisdiction_type,
            address,
            road_address,
            postal_code,
            source,
            raw_json,
        ),
    )


def seed_demand_agencies_from_notices(conn: sqlite3.Connection) -> int:
    before = conn.total_changes
    conn.execute(
        """
        INSERT INTO demand_agencies (agency_code, agency_name, source, updated_at)
        SELECT agency_code, MAX(agency_name), 'observed', CURRENT_TIMESTAMP
        FROM bid_notices
        WHERE agency_code != ''
        GROUP BY agency_code
        ON CONFLICT(agency_code) DO UPDATE SET
            agency_name = CASE WHEN excluded.agency_name != '' THEN excluded.agency_name ELSE demand_agencies.agency_name END,
            updated_at = CURRENT_TIMESTAMP
        """
    )
    return conn.total_changes - before


def insert_case(conn: sqlite3.Connection, case: HistoricalBidCase) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO bid_notices (
            notice_id, agency_name, category, contract_method, region, base_amount, estimated_amount, opened_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            case.notice_id,
            case.agency_name,
            case.category,
            case.contract_method,
            case.region,
            case.base_amount,
            case.base_amount,
            case.opened_at,
        ),
    )
    conn.execute(
        """
        INSERT OR REPLACE INTO bid_results (
            notice_id, winning_company, award_amount, bid_rate, bidder_count, result_status
        ) VALUES (?, ?, ?, ?, ?, 'awarded')
        """,
        (
            case.notice_id,
            case.winning_company,
            case.award_amount,
            case.bid_rate,
            case.bidder_count,
        ),
    )
    conn.execute(
        """
        INSERT OR REPLACE INTO contracts (
            contract_id, notice_id, contract_amount, contract_date, changed_amount
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (
            f"CT-{case.notice_id}",
            case.notice_id,
            case.award_amount,
            case.opened_at,
            case.award_amount,
        ),
    )


def upsert_notice(
    conn: sqlite3.Connection,
    notice_id: str,
    agency_name: str,
    category: str,
    contract_method: str,
    region: str,
    base_amount: float,
    estimated_amount: float | None,
    floor_rate: float | None,
    opened_at: str | None,
    agency_code: str = "",
) -> None:
    if agency_code:
        upsert_demand_agency(
            conn=conn,
            agency_code=agency_code,
            agency_name=agency_name,
            source="notice",
        )
    conn.execute(
        """
        INSERT INTO bid_notices (
            notice_id, agency_name, agency_code, category, contract_method, region,
            base_amount, estimated_amount, floor_rate, opened_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(notice_id) DO UPDATE SET
            agency_name = CASE WHEN excluded.agency_name != '' THEN excluded.agency_name ELSE bid_notices.agency_name END,
            agency_code = CASE WHEN excluded.agency_code != '' THEN excluded.agency_code ELSE bid_notices.agency_code END,
            category = CASE WHEN excluded.category != '' THEN excluded.category ELSE bid_notices.category END,
            contract_method = CASE WHEN excluded.contract_method != '' THEN excluded.contract_method ELSE bid_notices.contract_method END,
            region = CASE WHEN excluded.region != '' THEN excluded.region ELSE bid_notices.region END,
            base_amount = CASE WHEN excluded.base_amount > 0 THEN excluded.base_amount ELSE bid_notices.base_amount END,
            estimated_amount = COALESCE(excluded.estimated_amount, bid_notices.estimated_amount),
            floor_rate = COALESCE(excluded.floor_rate, bid_notices.floor_rate),
            opened_at = COALESCE(excluded.opened_at, bid_notices.opened_at)
        """,
        (
            notice_id,
            agency_name,
            agency_code,
            category,
            contract_method,
            region,
            base_amount,
            estimated_amount,
            floor_rate,
            opened_at,
        ),
    )


def ensure_notice_stub(conn: sqlite3.Connection, notice_id: str, category: str = "") -> None:
    upsert_notice(
        conn=conn,
        notice_id=notice_id,
        agency_name="",
        category=category,
        contract_method="",
        region="",
        base_amount=0.0,
        estimated_amount=None,
        floor_rate=None,
        opened_at=None,
    )


def enrich_notice_from_result(
    conn: sqlite3.Connection,
    notice_id: str,
    category: str,
    agency_name: str,
    base_amount: float | None,
    opened_at: str | None,
    agency_code: str = "",
) -> None:
    """Fill stub notice fields with data pulled from the results endpoint.

    Non-destructive: only fills fields that are currently empty / zero, so
    authoritative data coming from the notices endpoint is never overwritten.
    """
    ensure_notice_stub(conn, notice_id, category=category)
    if agency_code:
        upsert_demand_agency(
            conn=conn,
            agency_code=agency_code,
            agency_name=agency_name,
            source="result",
        )
    base_value = float(base_amount) if base_amount is not None else 0.0
    conn.execute(
        """
        UPDATE bid_notices
        SET
            agency_name = COALESCE(NULLIF(agency_name, ''), NULLIF(?, ''), ''),
            agency_code = COALESCE(NULLIF(agency_code, ''), NULLIF(?, ''), ''),
            category = COALESCE(NULLIF(category, ''), NULLIF(?, ''), ''),
            base_amount = CASE WHEN base_amount > 0 THEN base_amount ELSE ? END,
            estimated_amount = COALESCE(estimated_amount, NULLIF(?, 0)),
            opened_at = COALESCE(opened_at, NULLIF(?, ''))
        WHERE notice_id = ?
        """,
        (
            agency_name,
            agency_code,
            category,
            base_value,
            base_value,
            opened_at,
            notice_id,
        ),
    )


def enrich_notice_from_detail(
    conn: sqlite3.Connection,
    notice_id: str,
    agency_name: str,
    category: str,
    contract_method: str,
    region: str,
    base_amount: float | None,
    estimated_amount: float | None,
    floor_rate: float | None,
    opened_at: str | None,
    agency_code: str = "",
) -> None:
    """Fill stub notice fields with data pulled from a notices detail lookup.

    Also non-destructive: a field is only updated when the current value is
    empty / zero / null.
    """
    ensure_notice_stub(conn, notice_id, category=category)
    if agency_code:
        upsert_demand_agency(
            conn=conn,
            agency_code=agency_code,
            agency_name=agency_name,
            source="notice-detail",
        )
    base_value = float(base_amount) if base_amount is not None else 0.0
    conn.execute(
        """
        UPDATE bid_notices
        SET
            agency_name = COALESCE(NULLIF(agency_name, ''), NULLIF(?, ''), ''),
            agency_code = COALESCE(NULLIF(agency_code, ''), NULLIF(?, ''), ''),
            category = COALESCE(NULLIF(category, ''), NULLIF(?, ''), ''),
            contract_method = COALESCE(NULLIF(contract_method, ''), NULLIF(?, ''), ''),
            region = COALESCE(NULLIF(region, ''), NULLIF(?, ''), ''),
            base_amount = CASE WHEN base_amount > 0 THEN base_amount ELSE ? END,
            estimated_amount = COALESCE(estimated_amount, ?),
            floor_rate = COALESCE(floor_rate, ?),
            opened_at = COALESCE(opened_at, NULLIF(?, ''))
        WHERE notice_id = ?
        """,
        (
            agency_name,
            agency_code,
            category,
            contract_method,
            region,
            base_value,
            estimated_amount,
            floor_rate,
            opened_at,
            notice_id,
        ),
    )


def stub_notice_ids(
    conn: sqlite3.Connection,
    category: str | None = None,
    limit: int | None = None,
) -> list[str]:
    """Return notice_ids whose notice row is missing agency/method/base data but has a linked result."""
    sql = [
        "SELECT n.notice_id",
        "FROM bid_notices n",
        "JOIN bid_results r ON r.notice_id = n.notice_id",
        "WHERE (n.agency_name = '' OR n.contract_method = '' OR n.base_amount <= 0)",
    ]
    params: list = []
    if category:
        sql.append("AND (n.category = ? OR n.category = '')")
        params.append(category)
    sql.append("ORDER BY n.notice_id")
    if limit is not None:
        sql.append("LIMIT ?")
        params.append(limit)
    rows = conn.execute("\n".join(sql), params).fetchall()
    return [row["notice_id"] for row in rows]


def upsert_bid_result(
    conn: sqlite3.Connection,
    notice_id: str,
    award_amount: float,
    bid_rate: float,
    bidder_count: int,
    winning_company: str,
    result_status: str,
    category: str = "",
    winner_biz_no: str = "",
) -> None:
    ensure_notice_stub(conn, notice_id, category=category)
    conn.execute(
        """
        INSERT INTO bid_results (
            notice_id, winning_company, winner_biz_no, award_amount, bid_rate,
            bidder_count, result_status
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(notice_id) DO UPDATE SET
            winning_company = CASE WHEN excluded.winning_company != '' THEN excluded.winning_company ELSE bid_results.winning_company END,
            winner_biz_no = CASE WHEN excluded.winner_biz_no != '' THEN excluded.winner_biz_no ELSE bid_results.winner_biz_no END,
            award_amount = CASE WHEN excluded.award_amount > 0 THEN excluded.award_amount ELSE bid_results.award_amount END,
            bid_rate = CASE WHEN excluded.bid_rate > 0 THEN excluded.bid_rate ELSE bid_results.bid_rate END,
            bidder_count = CASE WHEN excluded.bidder_count > 0 THEN excluded.bidder_count ELSE bid_results.bidder_count END,
            result_status = CASE WHEN excluded.result_status != '' THEN excluded.result_status ELSE bid_results.result_status END
        """,
        (
            notice_id,
            winning_company,
            winner_biz_no,
            award_amount,
            bid_rate,
            bidder_count,
            result_status,
        ),
    )


def upsert_contract(
    conn: sqlite3.Connection,
    contract_id: str,
    notice_id: str,
    contract_amount: float,
    contract_date: str | None,
    changed_amount: float | None,
    category: str = "",
) -> None:
    ensure_notice_stub(conn, notice_id, category=category)
    conn.execute(
        """
        INSERT INTO contracts (
            contract_id, notice_id, contract_amount, contract_date, changed_amount
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(contract_id) DO UPDATE SET
            notice_id = excluded.notice_id,
            contract_amount = CASE WHEN excluded.contract_amount > 0 THEN excluded.contract_amount ELSE contracts.contract_amount END,
            contract_date = COALESCE(excluded.contract_date, contracts.contract_date),
            changed_amount = COALESCE(excluded.changed_amount, contracts.changed_amount)
        """,
        (
            contract_id,
            notice_id,
            contract_amount,
            contract_date,
            changed_amount,
        ),
    )


def upsert_procurement_plan(
    conn: sqlite3.Connection,
    plan_id: str,
    agency_name: str,
    category: str,
    budget_amount: float,
    planned_quarter: str,
    contract_method: str,
    agency_code: str = "",
) -> None:
    if agency_code:
        upsert_demand_agency(
            conn=conn,
            agency_code=agency_code,
            agency_name=agency_name,
            source="plan",
        )
    conn.execute(
        """
        INSERT INTO procurement_plans (
            plan_id, agency_name, category, budget_amount, planned_quarter, contract_method
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(plan_id) DO UPDATE SET
            agency_name = CASE WHEN excluded.agency_name != '' THEN excluded.agency_name ELSE procurement_plans.agency_name END,
            category = CASE WHEN excluded.category != '' THEN excluded.category ELSE procurement_plans.category END,
            budget_amount = CASE WHEN excluded.budget_amount > 0 THEN excluded.budget_amount ELSE procurement_plans.budget_amount END,
            planned_quarter = CASE WHEN excluded.planned_quarter != '' THEN excluded.planned_quarter ELSE procurement_plans.planned_quarter END,
            contract_method = CASE WHEN excluded.contract_method != '' THEN excluded.contract_method ELSE procurement_plans.contract_method END
        """,
        (
            plan_id,
            agency_name,
            category,
            budget_amount,
            planned_quarter,
            contract_method,
        ),
    )


def load_historical_cases(db_path: str | Path) -> list[HistoricalBidCase]:
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT
                n.notice_id,
                n.agency_name,
                n.category,
                n.contract_method,
                n.region,
                n.base_amount,
                r.award_amount,
                r.bid_rate,
                r.bidder_count,
                n.opened_at,
                COALESCE(r.winning_company, '') AS winning_company
            FROM bid_notices n
            JOIN bid_results r ON r.notice_id = n.notice_id
            WHERE r.result_status = 'awarded'
              AND n.base_amount > 0
              AND n.agency_name != ''
              AND n.contract_method != ''
              AND r.bid_rate > 0
              AND r.award_amount > 0
            ORDER BY n.opened_at DESC, n.notice_id DESC
            """
        ).fetchall()

    return [
        HistoricalBidCase(
            notice_id=row["notice_id"],
            agency_name=row["agency_name"],
            category=row["category"],
            contract_method=row["contract_method"],
            region=row["region"],
            base_amount=row["base_amount"],
            award_amount=row["award_amount"],
            bid_rate=row["bid_rate"],
            bidder_count=row["bidder_count"],
            opened_at=row["opened_at"],
            winning_company=row["winning_company"],
        )
        for row in rows
    ]


def get_demand_agency(db_path: str | Path, agency_code: str) -> sqlite3.Row | None:
    with connect(db_path) as conn:
        return conn.execute(
            """
            SELECT agency_code, agency_name, top_agency_code, top_agency_name,
                   jurisdiction_type, address, road_address, postal_code,
                   source, updated_at
            FROM demand_agencies
            WHERE agency_code = ?
            """,
            (agency_code,),
        ).fetchone()


def list_demand_agencies(
    db_path: str | Path,
    search: str | None = None,
    limit: int = 200,
) -> list[sqlite3.Row]:
    filters: list[str] = []
    params: list = []
    if search:
        filters.append("(agency_code LIKE ? OR agency_name LIKE ? OR top_agency_name LIKE ?)")
        needle = f"%{search}%"
        params.extend([needle, needle, needle])
    where_sql = ("WHERE " + " AND ".join(filters)) if filters else ""
    with connect(db_path) as conn:
        return conn.execute(
            f"""
            SELECT agency_code, agency_name, top_agency_code, top_agency_name,
                   jurisdiction_type, address, road_address, postal_code,
                   source, updated_at
            FROM demand_agencies
            {where_sql}
            ORDER BY agency_name ASC, agency_code ASC
            LIMIT ?
            """,
            params + [max(1, limit)],
        ).fetchall()


def load_historical_cases_for_notice(
    db_path: str | Path,
    notice_id: str,
    cutoff_opened_at: str | None,
    category: str | None = None,
    contract_method: str | None = None,
    agency_name: str | None = None,
) -> list[HistoricalBidCase]:
    filters = [
        "r.result_status = 'awarded'",
        "n.base_amount > 0",
        "n.agency_name != ''",
        "n.contract_method != ''",
        "r.bid_rate > 0",
        "r.award_amount > 0",
        "n.notice_id != ?",
    ]
    params: list = [notice_id]
    if cutoff_opened_at:
        filters.append("COALESCE(n.opened_at, '') < ?")
        params.append(cutoff_opened_at)
    if category:
        filters.append("n.category = ?")
        params.append(category)
    if contract_method:
        filters.append("n.contract_method = ?")
        params.append(contract_method)
    if agency_name:
        filters.append("n.agency_name = ?")
        params.append(agency_name)
    where_sql = " AND ".join(filters)

    with connect(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT
                n.notice_id,
                n.agency_name,
                n.category,
                n.contract_method,
                n.region,
                n.base_amount,
                r.award_amount,
                r.bid_rate,
                r.bidder_count,
                n.opened_at,
                COALESCE(r.winning_company, '') AS winning_company
            FROM bid_notices n
            JOIN bid_results r ON r.notice_id = n.notice_id
            WHERE {where_sql}
            ORDER BY n.opened_at DESC, n.notice_id DESC
            """,
            params,
        ).fetchall()

    return [
        HistoricalBidCase(
            notice_id=row["notice_id"],
            agency_name=row["agency_name"],
            category=row["category"],
            contract_method=row["contract_method"],
            region=row["region"],
            base_amount=row["base_amount"],
            award_amount=row["award_amount"],
            bid_rate=row["bid_rate"],
            bidder_count=row["bidder_count"],
            opened_at=row["opened_at"],
            winning_company=row["winning_company"],
        )
        for row in rows
    ]


# Code prefixes considered safe for auto-seeded parent aggregation.
# Only single-legal-entity hierarchies (central ministries, public corporations).
# Excludes local governments (3-6, Z0 social welfare), education offices (7-9).
_SAFE_PARENT_PREFIXES = ("1", "A", "B", "D")


def _parent_token(agency_name: str) -> str:
    agency_name = (agency_name or "").strip()
    if not agency_name or " " not in agency_name:
        return ""
    return agency_name.split(" ", 1)[0]


def _normalized_parent_name(
    agency_name: str,
    top_agency_code: str,
    top_agency_name: str,
) -> tuple[str, str]:
    agency_name = (agency_name or "").strip()
    top_agency_code = (top_agency_code or "").strip()
    top_agency_name = (top_agency_name or "").strip()

    if top_agency_code and top_agency_name:
        return top_agency_name, "api"
    if top_agency_name and top_agency_name != agency_name:
        return top_agency_name, "api"

    token = _parent_token(agency_name)
    if token:
        return token, "token"
    return "", ""


def seed_agency_parent_mapping(
    db_path: str | Path,
    min_subunits: int = 10,
    min_parent_cases: int = 50,
    refresh: bool = False,
) -> dict:
    """Populate agency_parent_mapping with demand-agency-aware parent suggestions.

    Parent grouping prefers `demand_agencies.top_agency_*` from the user API.
    When that metadata is missing, it falls back to the legacy first-token rule.
    Only inserts rows for parent buckets that (a) have at least `min_subunits`
    distinct sub-units and (b) have at least `min_parent_cases` awarded cases.
    The legacy name-token fallback also keeps the existing safe-prefix filter.
    Existing rows are preserved unless `refresh=True`.
    """
    with connect(db_path) as conn:
        if refresh:
            conn.execute("DELETE FROM agency_parent_mapping WHERE source='auto'")

        agency_stats = conn.execute(
            """
            SELECT n.agency_name,
                   n.agency_code,
                   SUBSTR(n.agency_code,1,1) AS code1,
                   COALESCE(d.top_agency_code, '') AS top_agency_code,
                   COALESCE(d.top_agency_name, '') AS top_agency_name,
                   COUNT(DISTINCT n.notice_id) AS notices,
                   SUM(CASE WHEN r.bid_rate > 0 THEN 1 ELSE 0 END) AS awarded
            FROM bid_notices n
            LEFT JOIN bid_results r ON r.notice_id = n.notice_id
            LEFT JOIN demand_agencies d ON d.agency_code = n.agency_code
            WHERE n.agency_name != ''
            GROUP BY n.agency_name, n.agency_code, d.top_agency_code, d.top_agency_name
            """
        ).fetchall()

        parent_buckets: dict[tuple[str, str], list[sqlite3.Row]] = {}
        for row in agency_stats:
            parent_name, bucket_kind = _normalized_parent_name(
                row["agency_name"],
                row["top_agency_code"],
                row["top_agency_name"],
            )
            if not parent_name or not bucket_kind:
                continue
            parent_buckets.setdefault((bucket_kind, parent_name), []).append(row)

        inserted = 0
        skipped_unsafe = 0
        skipped_small = 0
        for (bucket_kind, parent), rows in parent_buckets.items():
            if len(rows) < min_subunits:
                skipped_small += 1
                continue
            parent_awarded_total = sum(row["awarded"] or 0 for row in rows)
            if parent_awarded_total < min_parent_cases:
                skipped_small += 1
                continue
            if bucket_kind == "token":
                prefixes = {row["code1"] for row in rows if row["code1"]}
                if not prefixes or not prefixes.issubset(set(_SAFE_PARENT_PREFIXES)):
                    skipped_unsafe += 1
                    continue
            for row in rows:
                if row["agency_name"] == parent:
                    continue
                conn.execute(
                    """
                    INSERT OR IGNORE INTO agency_parent_mapping (
                        agency_name, parent_name, subunit_count,
                        agency_case_count, parent_case_count,
                        status, source, note, updated_at
                    ) VALUES (?, ?, ?, ?, ?, 'pending', 'auto', '', CURRENT_TIMESTAMP)
                    """,
                    (
                        row["agency_name"],
                        parent,
                        len(rows),
                        row["awarded"] or 0,
                        parent_awarded_total,
                    ),
                )
                if conn.total_changes:
                    inserted += 1
        total = conn.execute("SELECT COUNT(*) FROM agency_parent_mapping").fetchone()[0]
    return {
        "inserted": inserted,
        "skipped_unsafe": skipped_unsafe,
        "skipped_small": skipped_small,
        "total_in_table": total,
    }


def get_agency_parent(db_path: str | Path, agency_name: str) -> sqlite3.Row | None:
    with connect(db_path) as conn:
        return conn.execute(
            "SELECT * FROM agency_parent_mapping WHERE agency_name = ?",
            (agency_name,),
        ).fetchone()


def list_agency_parent_mappings(
    db_path: str | Path,
    status: str | None = None,
    parent_name: str | None = None,
    search: str | None = None,
) -> list[sqlite3.Row]:
    filters: list[str] = []
    params: list = []
    if status:
        filters.append("status = ?")
        params.append(status)
    if parent_name:
        filters.append("parent_name = ?")
        params.append(parent_name)
    if search:
        filters.append("(agency_name LIKE ? OR parent_name LIKE ?)")
        params.extend([f"%{search}%", f"%{search}%"])
    where_sql = (" WHERE " + " AND ".join(filters)) if filters else ""
    with connect(db_path) as conn:
        return conn.execute(
            f"""
            SELECT agency_name, parent_name, subunit_count,
                   agency_case_count, parent_case_count,
                   status, source, note, updated_at
            FROM agency_parent_mapping
            {where_sql}
            ORDER BY status ASC, agency_case_count ASC, agency_name ASC
            """,
            params,
        ).fetchall()


def update_agency_parent_status(
    db_path: str | Path,
    agency_name: str,
    status: str,
    note: str = "",
    parent_name: str | None = None,
) -> None:
    if status not in ("pending", "approved", "blacklisted"):
        raise ValueError(f"invalid status: {status}")
    with connect(db_path) as conn:
        if parent_name is not None:
            conn.execute(
                """
                UPDATE agency_parent_mapping
                SET status=?, note=?, parent_name=?,
                    source=CASE WHEN source='auto' AND ?='' THEN 'auto' ELSE 'manual' END,
                    updated_at=CURRENT_TIMESTAMP
                WHERE agency_name=?
                """,
                (status, note, parent_name, note, agency_name),
            )
        else:
            conn.execute(
                """
                UPDATE agency_parent_mapping
                SET status=?, note=?, source=CASE WHEN source='auto' AND ?='' THEN 'auto' ELSE 'manual' END,
                    updated_at=CURRENT_TIMESTAMP
                WHERE agency_name=?
                """,
                (status, note, note, agency_name),
            )


def save_mock_bid(
    db_path: str | Path,
    notice_id: str,
    bid_amount: float,
    bid_rate: float,
    predicted_amount: float | None = None,
    predicted_rate: float | None = None,
    note: str = "",
    simulation_id: str = "",
    customer_idx: int = 0,
) -> int:
    with connect(db_path) as conn:
        cur = conn.execute(
            """
            INSERT INTO mock_bids (notice_id, bid_amount, bid_rate,
                predicted_amount, predicted_rate, note, simulation_id,
                customer_idx, submitted_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (notice_id, bid_amount, bid_rate, predicted_amount, predicted_rate,
             note, simulation_id, customer_idx),
        )
        return int(cur.lastrowid)


def save_mock_bid_batch(
    db_path: str | Path,
    simulation_id: str,
    rows: list[dict],
) -> int:
    """Insert many mock bids in one transaction. Each row must have keys:
    notice_id, bid_amount, bid_rate, predicted_amount, predicted_rate,
    note, customer_idx. Optional: n_customers."""
    if not rows:
        return 0
    with connect(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO mock_bids (notice_id, bid_amount, bid_rate,
                predicted_amount, predicted_rate, note, simulation_id,
                customer_idx, n_customers, submitted_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                (r["notice_id"], r["bid_amount"], r["bid_rate"],
                 r.get("predicted_amount"), r.get("predicted_rate"),
                 r.get("note", ""), simulation_id, r.get("customer_idx", 0),
                 int(r.get("n_customers", 0) or 0))
                for r in rows
            ],
        )
    return len(rows)


def load_strategy_table_for_scope(
    conn: sqlite3.Connection,
    category: str,
    contract_method: str,
    *,
    agency_name: str = "",
    region: str = "",
) -> dict[int, list[float]]:
    """Return {n_customers: quantiles_list} for a scope's strategy_tables rows.

    Empty agency/region scopes are the cat/method baseline populated by
    build_strategy_tables_v2. Callers can pass specific agency/region to look
    up tighter scopes (not yet populated as of 2026-04-20).
    """
    rows = conn.execute(
        """
        SELECT n_customers, quantiles_json
        FROM strategy_tables
        WHERE category = ? AND contract_method = ?
          AND agency_name = ? AND region = ?
        ORDER BY n_customers
        """,
        (category, contract_method, agency_name, region),
    ).fetchall()
    out: dict[int, list[float]] = {}
    for row in rows:
        try:
            qs = json.loads(row["quantiles_json"])
        except (TypeError, ValueError):
            continue
        if isinstance(qs, list) and qs:
            out[int(row["n_customers"])] = [float(q) for q in qs]
    return out


def replace_auto_mock_bid_batch(
    db_path: str | Path,
    simulation_id: str,
    rows: list[dict],
) -> int:
    """Replace pending auto-generated mock bids for the same notices.

    Rows are considered auto-generated when note starts with ``auto:``.
    Awarded notices are preserved so historical evaluation remains available.
    Optional row key ``n_customers`` tags the portfolio size for per-N
    aggregation (MODES.md §2-표기).
    """
    if not rows:
        return 0
    notice_ids = sorted({str(row["notice_id"]) for row in rows if row.get("notice_id")})
    placeholders = ",".join("?" for _ in notice_ids)
    with connect(db_path) as conn:
        if notice_ids:
            conn.execute(
                f"""
                DELETE FROM mock_bids
                WHERE note LIKE 'auto:%'
                  AND notice_id IN ({placeholders})
                  AND NOT EXISTS (
                      SELECT 1
                      FROM bid_results r
                      WHERE r.notice_id = mock_bids.notice_id
                        AND r.award_amount > 0
                        AND r.bid_rate > 0
                  )
                """,
                notice_ids,
            )
        conn.executemany(
            """
            INSERT INTO mock_bids (notice_id, bid_amount, bid_rate,
                predicted_amount, predicted_rate, note, simulation_id,
                customer_idx, n_customers, submitted_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                (r["notice_id"], r["bid_amount"], r["bid_rate"],
                 r.get("predicted_amount"), r.get("predicted_rate"),
                 r.get("note", ""), simulation_id, r.get("customer_idx", 0),
                 int(r.get("n_customers", 0) or 0))
                for r in rows
            ],
        )
    return len(rows)


def list_simulation_ids(db_path: str | Path) -> list[dict]:
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT simulation_id,
                   COUNT(*) AS customer_bids,
                   COUNT(DISTINCT notice_id) AS notices,
                   MIN(submitted_at) AS started_at
            FROM mock_bids
            WHERE simulation_id != ''
            GROUP BY simulation_id
            ORDER BY started_at DESC
            """
        ).fetchall()
    return [dict(r) for r in rows]


def revenue_summary(
    db_path: str | Path,
    fee_rate: float,
    simulation_id: str | None = None,
) -> dict:
    """Compute realized revenue = SUM(bid_amount * fee_rate) for mock bids
    whose actual notice result indicates a win (our amount < actual),
    grouped by notice opened_at date. Ignores pending and disqualified rows.
    """
    filters = [
        "r.notice_id IS NOT NULL",
        "r.award_amount > 0",
        "r.bid_rate > 0",
        "m.bid_amount < r.award_amount",
        "(n.floor_rate IS NULL OR n.floor_rate <= 0 OR m.bid_rate >= n.floor_rate)",
    ]
    params: list = []
    if simulation_id:
        filters.append("m.simulation_id = ?")
        params.append(simulation_id)
    where_sql = " AND ".join(filters)
    with connect(db_path) as conn:
        total_row = conn.execute(
            f"""
            SELECT COUNT(*) AS wins, COALESCE(SUM(m.bid_amount),0) AS won_total
            FROM mock_bids m
            LEFT JOIN bid_notices n ON n.notice_id = m.notice_id
            LEFT JOIN bid_results r ON r.notice_id = m.notice_id
            WHERE {where_sql}
            """,
            params,
        ).fetchone()
        daily_rows = conn.execute(
            f"""
            SELECT substr(COALESCE(n.opened_at,''),1,10) AS day,
                   COUNT(*) AS wins,
                   SUM(m.bid_amount) AS won_total
            FROM mock_bids m
            LEFT JOIN bid_notices n ON n.notice_id = m.notice_id
            LEFT JOIN bid_results r ON r.notice_id = m.notice_id
            WHERE {where_sql}
            GROUP BY day
            ORDER BY day DESC
            """,
            params,
        ).fetchall()
    total_won = total_row["won_total"] or 0.0
    total_wins = total_row["wins"] or 0
    return {
        "fee_rate": fee_rate,
        "total_wins": total_wins,
        "total_won_amount": total_won,
        "total_revenue": round(total_won * fee_rate, 2),
        "daily": [
            {
                "day": row["day"] or "(미상)",
                "wins": row["wins"],
                "won_amount": row["won_total"] or 0.0,
                "revenue": round((row["won_total"] or 0.0) * fee_rate, 2),
            }
            for row in daily_rows
        ],
    }


def compute_weekly_metrics(db_path: str | Path, fee_rate: float = 0.0005) -> dict:
    """Compute current KPIs for weekly review. Does NOT write to DB."""
    with connect(db_path) as conn:
        notices_total = conn.execute("SELECT COUNT(*) FROM bid_notices").fetchone()[0]
        notices_new_7d = conn.execute(
            "SELECT COUNT(*) FROM bid_notices WHERE opened_at >= date('now','-7 days')"
        ).fetchone()[0]
        results_total = conn.execute("SELECT COUNT(*) FROM bid_results").fetchone()[0]
        approved_mappings = conn.execute(
            "SELECT COUNT(*) FROM agency_parent_mapping WHERE status='approved'"
        ).fetchone()[0]
        pending_mappings = conn.execute(
            "SELECT COUNT(*) FROM agency_parent_mapping WHERE status='pending'"
        ).fetchone()[0]
        sim_batches = conn.execute(
            "SELECT COUNT(DISTINCT simulation_id) FROM mock_bids WHERE simulation_id != ''"
        ).fetchone()[0]
        mock_bids_total = conn.execute("SELECT COUNT(*) FROM mock_bids").fetchone()[0]
    mocks = list_mock_bids(db_path)
    counts = {"won": 0, "lost": 0, "pending": 0, "disqualified": 0}
    for m in mocks:
        counts[m["verdict"]] = counts.get(m["verdict"], 0) + 1
    resolved = counts["won"] + counts["lost"] + counts["disqualified"]
    win_rate = (counts["won"] / resolved) if resolved else 0.0
    rev_all = revenue_summary(db_path, fee_rate=fee_rate)
    with connect(db_path) as conn:
        rev_7d_row = conn.execute(
            """
            SELECT COALESCE(SUM(m.bid_amount),0)
            FROM mock_bids m
            LEFT JOIN bid_notices n ON n.notice_id=m.notice_id
            LEFT JOIN bid_results r ON r.notice_id=m.notice_id
            WHERE r.award_amount > 0 AND r.bid_rate > 0
              AND m.bid_amount < r.award_amount
              AND (n.floor_rate IS NULL OR n.floor_rate <= 0 OR m.bid_rate >= n.floor_rate)
              AND substr(COALESCE(n.opened_at,''),1,10) >= date('now','-7 days')
            """
        ).fetchone()
    rev_7d = float(rev_7d_row[0] or 0) * fee_rate
    return {
        "snapshot_date": None,  # caller sets
        "notices_total": notices_total,
        "notices_new_7d": notices_new_7d,
        "results_total": results_total,
        "approved_mappings": approved_mappings,
        "pending_mappings": pending_mappings,
        "sim_batches": sim_batches,
        "mock_bids_total": mock_bids_total,
        "mock_wins": counts["won"],
        "mock_lost": counts["lost"],
        "mock_pending": counts["pending"],
        "mock_disqualified": counts["disqualified"],
        "win_rate": round(win_rate, 4),
        "revenue_total": rev_all["total_revenue"],
        "revenue_7d": round(rev_7d, 2),
        "fee_rate": fee_rate,
    }


def take_weekly_snapshot(
    db_path: str | Path, fee_rate: float = 0.0005, note: str = "",
) -> int:
    from datetime import date
    m = compute_weekly_metrics(db_path, fee_rate=fee_rate)
    today = date.today().isoformat()
    with connect(db_path) as conn:
        cur = conn.execute(
            """
            INSERT INTO metrics_snapshots (
                snapshot_date, notices_total, notices_new_7d, results_total,
                approved_mappings, pending_mappings, sim_batches,
                mock_bids_total, mock_wins, mock_lost, mock_pending, mock_disqualified,
                win_rate, revenue_total, revenue_7d, fee_rate, note
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(snapshot_date) DO UPDATE SET
                notices_total=excluded.notices_total,
                notices_new_7d=excluded.notices_new_7d,
                results_total=excluded.results_total,
                approved_mappings=excluded.approved_mappings,
                pending_mappings=excluded.pending_mappings,
                sim_batches=excluded.sim_batches,
                mock_bids_total=excluded.mock_bids_total,
                mock_wins=excluded.mock_wins,
                mock_lost=excluded.mock_lost,
                mock_pending=excluded.mock_pending,
                mock_disqualified=excluded.mock_disqualified,
                win_rate=excluded.win_rate,
                revenue_total=excluded.revenue_total,
                revenue_7d=excluded.revenue_7d,
                fee_rate=excluded.fee_rate,
                note=excluded.note
            """,
            (today, m["notices_total"], m["notices_new_7d"], m["results_total"],
             m["approved_mappings"], m["pending_mappings"], m["sim_batches"],
             m["mock_bids_total"], m["mock_wins"], m["mock_lost"],
             m["mock_pending"], m["mock_disqualified"],
             m["win_rate"], m["revenue_total"], m["revenue_7d"],
             m["fee_rate"], note),
        )
        row_id = cur.lastrowid
    return int(row_id or 0)


def list_metrics_snapshots(db_path: str | Path, limit: int = 12) -> list[dict]:
    with connect(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM metrics_snapshots ORDER BY snapshot_date DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def add_suggestion(
    db_path: str | Path,
    title: str,
    description: str = "",
    rationale: str = "",
    impact: str = "medium",
    source: str = "manual",
    metric_snapshot_id: int | None = None,
) -> int:
    with connect(db_path) as conn:
        cur = conn.execute(
            """
            INSERT INTO improvement_suggestions (
                title, description, rationale, impact, source, metric_snapshot_id,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            (title, description, rationale, impact, source, metric_snapshot_id),
        )
    return int(cur.lastrowid or 0)


def list_suggestions(
    db_path: str | Path, status: str | None = None, limit: int = 100,
) -> list[dict]:
    with connect(db_path) as conn:
        if status:
            rows = conn.execute(
                """
                SELECT * FROM improvement_suggestions WHERE status=?
                ORDER BY CASE impact WHEN 'high' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END,
                         updated_at DESC LIMIT ?
                """,
                (status, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT * FROM improvement_suggestions
                ORDER BY CASE status
                    WHEN 'proposed' THEN 0 WHEN 'approved' THEN 1
                    WHEN 'implemented' THEN 2 WHEN 'rejected' THEN 3 ELSE 4 END,
                    CASE impact WHEN 'high' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END,
                    updated_at DESC LIMIT ?
                """,
                (limit,),
            ).fetchall()
    return [dict(r) for r in rows]


def update_suggestion(
    db_path: str | Path,
    suggestion_id: int,
    status: str | None = None,
    note: str | None = None,
) -> None:
    sets: list[str] = []
    params: list = []
    if status is not None:
        if status not in ("proposed", "approved", "implemented", "rejected"):
            raise ValueError(f"invalid suggestion status: {status}")
        sets.append("status=?")
        params.append(status)
    if note is not None:
        sets.append("note=?")
        params.append(note)
    if not sets:
        return
    sets.append("updated_at=CURRENT_TIMESTAMP")
    params.append(suggestion_id)
    with connect(db_path) as conn:
        conn.execute(
            f"UPDATE improvement_suggestions SET {', '.join(sets)} WHERE suggestion_id=?",
            params,
        )


def auto_generate_suggestions(db_path: str | Path) -> list[int]:
    """Lightweight rule-based suggestion generator. Called after a snapshot.

    Compares the latest two snapshots (if available) and inserts suggestions
    for clear regressions/opportunities. Idempotent per-day via title.
    """
    snaps = list_metrics_snapshots(db_path, limit=2)
    if not snaps:
        return []
    cur = snaps[0]
    prev = snaps[1] if len(snaps) >= 2 else None

    created: list[int] = []

    def _exists(title: str) -> bool:
        with connect(db_path) as conn:
            row = conn.execute(
                "SELECT 1 FROM improvement_suggestions WHERE title=? AND status IN ('proposed','approved') LIMIT 1",
                (title,),
            ).fetchone()
        return bool(row)

    if prev is not None:
        rev_delta = (cur["revenue_7d"] or 0) - (prev["revenue_7d"] or 0)
        if prev["revenue_7d"] and rev_delta < 0:
            title = "7일 수익 감소: 전략/데이터 신선도 점검"
            if not _exists(title):
                created.append(add_suggestion(
                    db_path, title,
                    description="최근 7일 수익이 직전 스냅샷 대비 감소. target_win_probability, 경쟁사 top-K, 부모 통합 범위를 재점검.",
                    rationale=f"revenue_7d: {prev['revenue_7d']:,.0f} → {cur['revenue_7d']:,.0f} (Δ {rev_delta:+,.0f})",
                    impact="high", source="auto",
                ))
        if prev["win_rate"] and cur["win_rate"] + 1e-6 < prev["win_rate"]:
            title = "모의 승률 하락"
            if not _exists(title):
                created.append(add_suggestion(
                    db_path, title,
                    description="전주 대비 모의 승률 하락. 경쟁사 분포 변화 또는 예측 모델 드리프트 의심.",
                    rationale=f"win_rate: {prev['win_rate']:.3f} → {cur['win_rate']:.3f}",
                    impact="medium", source="auto",
                ))

    if cur["approved_mappings"] == 0 and cur["pending_mappings"] >= 50:
        title = "부모 통합 승인이 아직 0건"
        if not _exists(title):
            created.append(add_suggestion(
                db_path, title,
                description="자동 시더가 pending 매핑을 쌓았지만 approved가 0. 기관 통합 관리 탭에서 소규모 공공기관 그룹을 승인해 효과 측정.",
                rationale=f"pending={cur['pending_mappings']}, approved=0",
                impact="medium", source="auto",
            ))

    if cur["mock_pending"] >= 50 and cur["mock_wins"] + cur["mock_lost"] == 0:
        title = "시뮬 모의 입찰이 전부 pending — 실제 결과 보강 필요"
        if not _exists(title):
            created.append(add_suggestion(
                db_path, title,
                description="최근 모의 입찰 전부 pending. 새 CSV 임포트 또는 API 수집으로 실제 낙찰 결과를 붙여 판정 파이프라인을 검증.",
                rationale=f"pending={cur['mock_pending']}, resolved=0",
                impact="high", source="auto",
            ))

    return [c for c in created if c]


def top_winners_for_scope(
    db_path: str | Path,
    agency_name: str,
    category: str,
    contract_method: str,
    limit: int = 10,
    base_amount: float | None = None,
    base_amount_ratio: tuple[float, float] = (0.25, 4.0),
) -> list[dict]:
    """Return top-K winning bidders (by win count) for a notice scope,
    each with their historical bid_rate list for sampling.

    Scope: same category + contract_method. If base_amount given, restrict to
    notices with base_amount in the ratio window around it.
    """
    filters = [
        "n.category = ?",
        "n.contract_method = ?",
        "r.result_status = 'awarded'",
        "r.bid_rate > 0",
        "r.bid_rate <= 110",
        "r.winner_biz_no != ''",
    ]
    params: list = [category, contract_method]
    if base_amount and base_amount > 0:
        lo, hi = base_amount_ratio
        filters.append("n.base_amount BETWEEN ? AND ?")
        params.extend([base_amount * lo, base_amount * hi])
    where_sql = " AND ".join(filters)
    with connect(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT r.winner_biz_no AS biz_no,
                   MAX(COALESCE(r.winning_company,'')) AS company_name,
                   COUNT(*) AS wins,
                   GROUP_CONCAT(r.bid_rate) AS rates_csv
            FROM bid_notices n
            JOIN bid_results r ON r.notice_id = n.notice_id
            WHERE {where_sql}
            GROUP BY r.winner_biz_no
            ORDER BY wins DESC
            LIMIT ?
            """,
            params + [limit],
        ).fetchall()
    out: list[dict] = []
    for row in rows:
        rates = []
        for chunk in (row["rates_csv"] or "").split(","):
            try:
                rates.append(float(chunk))
            except ValueError:
                continue
        out.append({
            "biz_no": row["biz_no"],
            "company_name": row["company_name"] or row["biz_no"],
            "wins": row["wins"],
            "rates": rates,
        })
    return out


def delete_mock_bid(db_path: str | Path, mock_id: int) -> None:
    with connect(db_path) as conn:
        conn.execute("DELETE FROM mock_bids WHERE mock_id = ?", (mock_id,))


def list_mock_bids_for_notice(db_path: str | Path, notice_id: str) -> list[dict]:
    """Return the latest simulation's mock bids for a single notice.

    Rows carry notice metadata, predicted values, the actual result (when known)
    and a verdict string so the UI can show won/lost/disqualified/pending.
    """
    if not notice_id:
        return []
    with connect(db_path) as conn:
        latest = conn.execute(
            "SELECT simulation_id, MAX(submitted_at) AS submitted_at "
            "FROM mock_bids WHERE notice_id=? "
            "GROUP BY simulation_id ORDER BY submitted_at DESC LIMIT 1",
            (notice_id,),
        ).fetchone()
        if latest is None:
            return []
        simulation_id = latest["simulation_id"] or ""
        rows = conn.execute(
            """
            SELECT m.mock_id, m.notice_id, m.simulation_id, m.customer_idx,
                   m.bid_amount, m.bid_rate,
                   m.predicted_amount, m.predicted_rate, m.note, m.submitted_at,
                   n.agency_name, n.category, n.contract_method, n.region,
                   n.base_amount, n.floor_rate, n.opened_at,
                   r.award_amount AS actual_amount, r.bid_rate AS actual_rate,
                   COALESCE(r.winning_company,'') AS winning_company,
                   COALESCE(r.result_status,'') AS result_status
            FROM mock_bids m
            LEFT JOIN bid_notices n ON n.notice_id = m.notice_id
            LEFT JOIN bid_results r ON r.notice_id = m.notice_id
            WHERE m.notice_id=? AND m.simulation_id=?
            ORDER BY m.customer_idx ASC, m.mock_id ASC
            """,
            (notice_id, simulation_id),
        ).fetchall()
    out: list[dict] = []
    for row in rows:
        d = dict(row)
        d["verdict"] = _evaluate_mock_bid(d)
        out.append(d)
    return out


def list_mock_bids(db_path: str | Path) -> list[dict]:
    """Return mock bids joined with notice + live result to enable verdict."""
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT m.mock_id, m.notice_id, m.bid_amount, m.bid_rate,
                   m.predicted_amount, m.predicted_rate, m.note, m.submitted_at,
                   n.agency_name, n.category, n.contract_method, n.region,
                   n.base_amount, n.floor_rate, n.opened_at,
                   r.award_amount AS actual_amount, r.bid_rate AS actual_rate,
                   COALESCE(r.winning_company,'') AS winning_company,
                   COALESCE(r.result_status,'') AS result_status
            FROM mock_bids m
            LEFT JOIN bid_notices n ON n.notice_id = m.notice_id
            LEFT JOIN bid_results r ON r.notice_id = m.notice_id
            ORDER BY m.submitted_at DESC
            """
        ).fetchall()
    out: list[dict] = []
    for row in rows:
        d = dict(row)
        d["verdict"] = _evaluate_mock_bid(d)
        out.append(d)
    return out


def _evaluate_mock_bid(row: dict) -> str:
    actual_rate = row.get("actual_rate")
    actual_amount = row.get("actual_amount")
    if actual_rate is None or actual_amount is None or actual_amount <= 0 or actual_rate <= 0:
        return "pending"  # 아직 결과 미확정 (또는 DB에 결과 미적재)
    floor = row.get("floor_rate")
    my_rate = row.get("bid_rate") or 0
    my_amount = row.get("bid_amount") or 0
    if floor is not None and floor > 0 and my_rate < floor:
        return "disqualified"  # 낙찰하한율 미달
    # Lowest-price-wins assumption (적격심사·제한최저가). Equal = tie → loss.
    if my_amount < actual_amount:
        return "won"
    return "lost"


def refresh_mock_bid_evaluations(
    db_path: str | Path,
    *,
    today_results_only: bool = False,
    simulation_id: str | None = None,
) -> dict:
    """Materialize current mock-bid verdicts into a dedicated table.

    This keeps the evaluation logic identical to `_evaluate_mock_bid()` while
    making daily analysis cheap and stable. Only mock bids linked to notices
    with an actual recorded result are persisted here; pending rows stay in
    `mock_bids` and become materialized once results arrive.
    """
    with connect(db_path) as conn:
        filters = [
            "r.award_amount > 0",
            "r.bid_rate > 0",
        ]
        params: list = []
        if today_results_only:
            filters.append("date(r.created_at, 'localtime') = date('now', 'localtime')")
        if simulation_id:
            filters.append("m.simulation_id = ?")
            params.append(simulation_id)
        where_sql = " AND ".join(filters)
        rows = conn.execute(
            f"""
            SELECT m.mock_id, m.notice_id, m.simulation_id, m.customer_idx,
                   m.bid_amount, m.bid_rate,
                   n.floor_rate,
                   r.award_amount AS actual_amount, r.bid_rate AS actual_rate,
                   COALESCE(r.winning_company,'') AS winning_company,
                   COALESCE(r.result_status,'') AS result_status,
                   r.created_at AS result_created_at
            FROM mock_bids m
            JOIN bid_results r ON r.notice_id = m.notice_id
            LEFT JOIN bid_notices n ON n.notice_id = m.notice_id
            WHERE {where_sql}
            """,
            params,
        ).fetchall()

    evaluations: list[tuple] = []
    verdict_counts = {"won": 0, "lost": 0, "pending": 0, "disqualified": 0}
    notice_ids: set[str] = set()
    for row in rows:
        record = dict(row)
        verdict = _evaluate_mock_bid(record)
        verdict_counts[verdict] = verdict_counts.get(verdict, 0) + 1
        notice_ids.add(str(record["notice_id"]))
        evaluations.append(
            (
                int(record["mock_id"]),
                str(record["notice_id"]),
                str(record["simulation_id"] or ""),
                int(record["customer_idx"] or 0),
                float(record["bid_amount"] or 0),
                float(record["bid_rate"] or 0),
                verdict,
                float(record["actual_amount"] or 0),
                float(record["actual_rate"] or 0),
                str(record["winning_company"] or ""),
                str(record["result_status"] or ""),
                record["result_created_at"],
            )
        )

    def _action(conn: sqlite3.Connection) -> None:
        if today_results_only and not simulation_id:
            conn.execute(
                """
                DELETE FROM mock_bid_evaluations
                WHERE date(result_created_at, 'localtime') = date('now', 'localtime')
                """
            )
        elif simulation_id:
            conn.execute(
                "DELETE FROM mock_bid_evaluations WHERE simulation_id = ?",
                (simulation_id,),
            )
        if evaluations:
            conn.executemany(
                """
                INSERT INTO mock_bid_evaluations (
                    mock_id, notice_id, simulation_id, customer_idx,
                    bid_amount, bid_rate, verdict,
                    actual_amount, actual_rate, winning_company, result_status,
                    result_created_at, evaluated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(mock_id) DO UPDATE SET
                    notice_id = excluded.notice_id,
                    simulation_id = excluded.simulation_id,
                    customer_idx = excluded.customer_idx,
                    bid_amount = excluded.bid_amount,
                    bid_rate = excluded.bid_rate,
                    verdict = excluded.verdict,
                    actual_amount = excluded.actual_amount,
                    actual_rate = excluded.actual_rate,
                    winning_company = excluded.winning_company,
                    result_status = excluded.result_status,
                    result_created_at = excluded.result_created_at,
                    evaluated_at = CURRENT_TIMESTAMP
                """,
                evaluations,
            )

    _run_write_with_retry(db_path, _action)
    return {
        "evaluated_mock_bids": len(evaluations),
        "evaluated_notices": len(notice_ids),
        "won": verdict_counts.get("won", 0),
        "lost": verdict_counts.get("lost", 0),
        "pending": verdict_counts.get("pending", 0),
        "disqualified": verdict_counts.get("disqualified", 0),
        "today_results_only": bool(today_results_only),
        "simulation_id": simulation_id or "",
    }


def resolve_adaptive_agencies(
    db_path: str | Path,
    agency_name: str,
    category: str | None,
    contract_method: str | None,
    min_agency_cases: int = 10,
) -> tuple[list[str], str | None]:
    """Return (agency_names_to_include, parent_used_or_None).

    If agency has an approved parent mapping AND its scoped case count is below
    `min_agency_cases`, expand the pool to include all siblings under the
    parent. Otherwise return just [agency_name].
    """
    if not agency_name:
        return ([], None)
    with connect(db_path) as conn:
        mapping = conn.execute(
            "SELECT parent_name, status FROM agency_parent_mapping WHERE agency_name=?",
            (agency_name,),
        ).fetchone()
        if not mapping or mapping["status"] != "approved" or not mapping["parent_name"]:
            return ([agency_name], None)

        count_params: list = [agency_name]
        filters = [
            "n.agency_name = ?",
            "r.result_status='awarded'",
            "n.base_amount > 0",
            "n.contract_method != ''",
            "r.bid_rate > 0",
            "r.award_amount > 0",
        ]
        if category:
            filters.append("n.category = ?")
            count_params.append(category)
        if contract_method:
            filters.append("n.contract_method = ?")
            count_params.append(contract_method)
        own_count = conn.execute(
            f"SELECT COUNT(*) FROM bid_notices n JOIN bid_results r ON r.notice_id=n.notice_id WHERE {' AND '.join(filters)}",
            count_params,
        ).fetchone()[0]
        if own_count >= min_agency_cases:
            return ([agency_name], None)

        siblings = conn.execute(
            "SELECT agency_name FROM agency_parent_mapping WHERE parent_name=? AND status='approved'",
            (mapping["parent_name"],),
        ).fetchall()
        names = {row["agency_name"] for row in siblings}
        names.add(mapping["parent_name"])
        names.add(agency_name)
    return (sorted(names), mapping["parent_name"])


AGENCY_SHRINKAGE_K = 10


def get_agency_parent_pool(
    db_path: str | Path,
    agency_name: str,
) -> tuple[str | None, frozenset[str]]:
    """Return (parent_name, sibling_pool) for an approved parent mapping.

    The sibling pool excludes the agency itself; feed it to
    `AgencyRangeAnalyzer(parent_pool_agencies=...)` to restrict peer cases to
    the parent group.
    """
    if not agency_name:
        return (None, frozenset())
    with connect(db_path) as conn:
        mapping = conn.execute(
            "SELECT parent_name FROM agency_parent_mapping "
            "WHERE agency_name=? AND status='approved'",
            (agency_name,),
        ).fetchone()
        parent_name = (
            mapping["parent_name"] if mapping and mapping["parent_name"] else None
        )
        if not parent_name:
            return (None, frozenset())
        sibling_rows = conn.execute(
            "SELECT agency_name FROM agency_parent_mapping "
            "WHERE parent_name=? AND status='approved'",
            (parent_name,),
        ).fetchall()
    pool = {row["agency_name"] for row in sibling_rows}
    pool.add(parent_name)
    pool.discard(agency_name)
    return (parent_name, frozenset(pool))


def load_cases_with_shrinkage(
    db_path: str | Path,
    agency_name: str,
    *,
    category: str | None = None,
    contract_method: str | None = None,
    cutoff_opened_at: str | None = None,
    exclude_notice_id: str | None = None,
    k: int = AGENCY_SHRINKAGE_K,
) -> tuple[list, dict]:
    """Blend sub-agency history with its parent pool via empirical-Bayes shrinkage.

    The returned case list contains every own case plus up to `k` most recent
    parent-pool cases. Because the downstream analyzer averages cases uniformly,
    the resulting mean ≈ `(n_sub*mean_sub + k_eff*mean_parent) / (n_sub + k_eff)`,
    where `k_eff = min(k, n_parent_available)`.
    """
    meta = {
        "n_sub": 0,
        "n_parent_anchor": 0,
        "n_parent_available": 0,
        "parent_name": None,
        "k": int(k),
        "w_sub": 1.0,
    }
    if not agency_name:
        return ([], meta)

    own = load_cases_for_agencies(
        db_path,
        [agency_name],
        category=category,
        contract_method=contract_method,
        cutoff_opened_at=cutoff_opened_at,
        exclude_notice_id=exclude_notice_id,
    )
    meta["n_sub"] = len(own)

    with connect(db_path) as conn:
        mapping = conn.execute(
            "SELECT parent_name FROM agency_parent_mapping "
            "WHERE agency_name=? AND status='approved'",
            (agency_name,),
        ).fetchone()
    parent_name = mapping["parent_name"] if mapping and mapping["parent_name"] else None
    if not parent_name or k <= 0:
        return (own, meta)

    with connect(db_path) as conn:
        sibling_rows = conn.execute(
            "SELECT agency_name FROM agency_parent_mapping "
            "WHERE parent_name=? AND status='approved'",
            (parent_name,),
        ).fetchall()
    pool_names = {row["agency_name"] for row in sibling_rows}
    pool_names.add(parent_name)
    pool_names.discard(agency_name)
    meta["parent_name"] = parent_name
    if not pool_names:
        return (own, meta)

    parent_pool = load_cases_for_agencies(
        db_path,
        sorted(pool_names),
        category=category,
        contract_method=contract_method,
        cutoff_opened_at=cutoff_opened_at,
        exclude_notice_id=exclude_notice_id,
    )
    own_ids = {c.notice_id for c in own}
    parent_pool = [c for c in parent_pool if c.notice_id not in own_ids]
    anchor = parent_pool[: int(k)]
    meta["n_parent_anchor"] = len(anchor)
    meta["n_parent_available"] = len(parent_pool)
    denom = len(own) + len(anchor)
    meta["w_sub"] = (len(own) / denom) if denom > 0 else 0.0
    return (own + anchor, meta)


def load_cases_for_agencies(
    db_path: str | Path,
    agency_names: list[str],
    category: str | None = None,
    contract_method: str | None = None,
    cutoff_opened_at: str | None = None,
    exclude_notice_id: str | None = None,
) -> list[HistoricalBidCase]:
    if not agency_names:
        return []
    placeholders = ",".join(["?"] * len(agency_names))
    filters = [
        f"n.agency_name IN ({placeholders})",
        "r.result_status='awarded'",
        "n.base_amount > 0",
        "n.contract_method != ''",
        "r.bid_rate > 0",
        "r.award_amount > 0",
    ]
    params: list = list(agency_names)
    if category:
        filters.append("n.category = ?")
        params.append(category)
    if contract_method:
        filters.append("n.contract_method = ?")
        params.append(contract_method)
    if cutoff_opened_at:
        filters.append("COALESCE(n.opened_at, '') < ?")
        params.append(cutoff_opened_at)
    if exclude_notice_id:
        filters.append("n.notice_id != ?")
        params.append(exclude_notice_id)
    with connect(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT n.notice_id, n.agency_name, n.category, n.contract_method, n.region,
                   n.base_amount, r.award_amount, r.bid_rate, r.bidder_count, n.opened_at,
                   COALESCE(r.winning_company,'') AS winning_company
            FROM bid_notices n JOIN bid_results r ON r.notice_id=n.notice_id
            WHERE {' AND '.join(filters)}
            ORDER BY n.opened_at DESC, n.notice_id DESC
            """,
            params,
        ).fetchall()
    return [
        HistoricalBidCase(
            notice_id=row["notice_id"],
            agency_name=row["agency_name"],
            category=row["category"],
            contract_method=row["contract_method"],
            region=row["region"],
            base_amount=row["base_amount"],
            award_amount=row["award_amount"],
            bid_rate=row["bid_rate"],
            bidder_count=row["bidder_count"],
            opened_at=row["opened_at"],
            winning_company=row["winning_company"],
        )
        for row in rows
    ]


def get_latest_opened_at(db_path: str | Path, category: str | None = None) -> str | None:
    """Return the most recent `bid_notices.opened_at` string, optionally filtered by category."""
    with connect(db_path) as conn:
        if category:
            row = conn.execute(
                """
                SELECT MAX(opened_at) AS m FROM bid_notices
                WHERE opened_at IS NOT NULL AND opened_at != ''
                  AND category = ?
                """,
                (category,),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT MAX(opened_at) AS m FROM bid_notices
                WHERE opened_at IS NOT NULL AND opened_at != ''
                """
            ).fetchone()
    return row["m"] if row else None


def _get_prediction_cache_versions(conn: sqlite3.Connection) -> tuple[str, str]:
    notice_row = conn.execute(
        """
        SELECT COALESCE(MAX(created_at), '') AS max_created_at,
               COUNT(*) AS total_count
        FROM bid_notices
        """
    ).fetchone()
    result_row = conn.execute(
        """
        SELECT COALESCE(MAX(created_at), '') AS max_created_at,
               COUNT(*) AS total_count
        FROM bid_results
        """
    ).fetchone()
    notice_version = f"{notice_row['max_created_at']}|{notice_row['total_count']}"
    result_version = f"{result_row['max_created_at']}|{result_row['total_count']}"
    return str(notice_version), str(result_version)


def _build_notice_prediction_cache_key(
    notice: BidNoticeSnapshot,
    target_win_probability: float,
    notice_version: str,
    result_version: str,
) -> str:
    parts = [
        notice.notice_id,
        notice.agency_name or "",
        notice.category or "",
        notice.contract_method or "",
        notice.region or "",
        f"{float(notice.base_amount or 0):.3f}",
        "" if notice.floor_rate is None else f"{float(notice.floor_rate):.6f}",
        notice.opened_at or "",
        f"{float(target_win_probability):.6f}",
        notice_version,
        result_version,
    ]
    return "|".join(parts)


def get_cached_notice_prediction(
    db_path: str | Path,
    notice: BidNoticeSnapshot,
    target_win_probability: float,
) -> dict | None:
    with connect(db_path) as conn:
        notice_version, result_version = _get_prediction_cache_versions(conn)
        expected_key = _build_notice_prediction_cache_key(
            notice, target_win_probability, notice_version, result_version,
        )
        row = conn.execute(
            "SELECT * FROM notice_prediction_cache WHERE notice_id = ?",
            (notice.notice_id,),
        ).fetchone()
    if row is None or row["cache_key"] != expected_key:
        return None
    return dict(row)


def upsert_notice_prediction_cache(
    db_path: str | Path,
    notice: BidNoticeSnapshot,
    target_win_probability: float,
    *,
    predicted_amount: float | None,
    predicted_rate: float | None,
    lower_rate: float | None,
    upper_rate: float | None,
    estimated_win_probability: float,
    confidence: str,
    agency_cases: int,
    peer_cases: int,
    lookback_years_used: int | None,
    parent_used: str | None = None,
    analysis_notes: str = "",
) -> None:
    with connect(db_path) as conn:
        notice_version, result_version = _get_prediction_cache_versions(conn)
        cache_key = _build_notice_prediction_cache_key(
            notice, target_win_probability, notice_version, result_version,
        )
        conn.execute(
            """
            INSERT INTO notice_prediction_cache (
                notice_id, cache_key, target_win_probability,
                predicted_amount, predicted_rate, lower_rate, upper_rate,
                estimated_win_probability, confidence,
                agency_cases, peer_cases, lookback_years_used,
                parent_used, analysis_notes, computed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(notice_id) DO UPDATE SET
                cache_key = excluded.cache_key,
                target_win_probability = excluded.target_win_probability,
                predicted_amount = excluded.predicted_amount,
                predicted_rate = excluded.predicted_rate,
                lower_rate = excluded.lower_rate,
                upper_rate = excluded.upper_rate,
                estimated_win_probability = excluded.estimated_win_probability,
                confidence = excluded.confidence,
                agency_cases = excluded.agency_cases,
                peer_cases = excluded.peer_cases,
                lookback_years_used = excluded.lookback_years_used,
                parent_used = excluded.parent_used,
                analysis_notes = excluded.analysis_notes,
                computed_at = CURRENT_TIMESTAMP
            """,
            (
                notice.notice_id,
                cache_key,
                target_win_probability,
                predicted_amount,
                predicted_rate,
                lower_rate,
                upper_rate,
                estimated_win_probability,
                confidence,
                agency_cases,
                peer_cases,
                lookback_years_used,
                parent_used or "",
                analysis_notes,
            ),
        )


def list_pending_notice_prediction_rows(
    db_path: str | Path,
    *,
    target_win_probability: float,
    category: str | None = None,
    agency_name: str | None = None,
    since_days: int | None = None,
    limit: int | None = None,
) -> list[dict]:
    notices = load_pending_notices_for_prediction(
        db_path=db_path,
        category=category,
        agency_name=agency_name,
        since_days=since_days,
        limit=limit,
    )
    if not notices:
        return []

    cache_map: dict[str, sqlite3.Row] = {}
    with connect(db_path) as conn:
        notice_version, result_version = _get_prediction_cache_versions(conn)
        placeholders = ",".join("?" for _ in notices)
        rows = conn.execute(
            f"""
            SELECT * FROM notice_prediction_cache
            WHERE notice_id IN ({placeholders})
            """,
            [notice.notice_id for notice in notices],
        ).fetchall()
        cache_map = {str(row["notice_id"]): row for row in rows}

    pending_rows: list[dict] = []
    for notice in notices:
        cached = cache_map.get(notice.notice_id)
        expected_key = _build_notice_prediction_cache_key(
            notice, target_win_probability, notice_version, result_version,
        )
        cache_ready = cached is not None and cached["cache_key"] == expected_key
        cache_status = "ready" if cache_ready else ("stale" if cached else "missing")
        pending_rows.append(
            {
                "opened_at": notice.opened_at,
                "notice_id": notice.notice_id,
                "category": notice.category,
                "agency_name": notice.agency_name,
                "contract_method": notice.contract_method,
                "region": notice.region,
                "base_amount": notice.base_amount,
                "floor_rate": notice.floor_rate,
                "cache_status": cache_status,
                "cached_at": cached["computed_at"] if cache_ready else None,
                "predicted_amount": cached["predicted_amount"] if cache_ready else None,
                "predicted_rate": cached["predicted_rate"] if cache_ready else None,
                "estimated_win_probability": (
                    cached["estimated_win_probability"] if cache_ready else None
                ),
                "confidence": cached["confidence"] if cache_ready else "",
                "agency_cases": int(cached["agency_cases"]) if cache_ready else 0,
                "peer_cases": int(cached["peer_cases"]) if cache_ready else 0,
            }
        )
    return pending_rows


def load_pending_notices_for_prediction(
    db_path: str | Path,
    category: str | None = None,
    agency_name: str | None = None,
    since_days: int | None = None,
    limit: int | None = None,
) -> list[BidNoticeSnapshot]:
    """Notices that have predictor-ready metadata but no linked award yet."""
    filters = [
        "n.agency_name != ''",
        "n.contract_method != ''",
        "n.base_amount > 0",
        "COALESCE(n.opened_at, '') != ''",
        "(r.notice_id IS NULL OR r.bid_rate <= 0)",
    ]
    params: list = []
    if since_days is not None and since_days > 0:
        filters.append("n.opened_at >= date('now', ?)")
        params.append(f"-{since_days} days")
    if category:
        filters.append("n.category = ?")
        params.append(category)
    if agency_name:
        filters.append("n.agency_name = ?")
        params.append(agency_name)
    where_sql = " AND ".join(filters)

    with connect(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT n.notice_id, n.agency_name, n.category, n.contract_method,
                   n.region, n.base_amount, n.floor_rate, n.opened_at
            FROM bid_notices n
            LEFT JOIN bid_results r ON r.notice_id = n.notice_id
            WHERE {where_sql}
            ORDER BY n.opened_at DESC, n.notice_id DESC
            """,
            params,
        ).fetchall()
    if limit is not None and limit > 0:
        rows = rows[:limit]

    return [
        BidNoticeSnapshot(
            notice_id=row["notice_id"],
            agency_name=row["agency_name"],
            category=row["category"],
            contract_method=row["contract_method"],
            region=row["region"],
            base_amount=row["base_amount"],
            floor_rate=row["floor_rate"],
            opened_at=row["opened_at"],
        )
        for row in rows
    ]


def get_operations_summary(
    db_path: str | Path,
    category: str | None = None,
) -> dict:
    """Realtime operating summary for dashboard monitoring."""
    filters = [
        "n.agency_name != ''",
        "n.contract_method != ''",
        "n.base_amount > 0",
        "COALESCE(n.opened_at, '') != ''",
    ]
    params: list = []
    if category:
        filters.append("n.category = ?")
        params.append(category)
    where_sql = " AND ".join(filters)

    with connect(db_path) as conn:
        pending_total = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM bid_notices n
            LEFT JOIN bid_results r ON r.notice_id = n.notice_id
            WHERE {where_sql}
              AND (r.notice_id IS NULL OR r.bid_rate <= 0)
            """,
            params,
        ).fetchone()[0]
        new_today = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM bid_notices n
            WHERE {where_sql}
              AND date(n.created_at, 'localtime') = date('now', 'localtime')
            """,
            params,
        ).fetchone()[0]
        completed_today = conn.execute(
            f"""
            SELECT COUNT(DISTINCT n.notice_id)
            FROM bid_notices n
            JOIN bid_results r ON r.notice_id = n.notice_id
            WHERE {where_sql}
              AND r.award_amount > 0
              AND r.bid_rate > 0
              AND date(r.created_at, 'localtime') = date('now', 'localtime')
            """,
            params,
        ).fetchone()[0]
        evaluated_today = conn.execute(
            f"""
            SELECT COUNT(DISTINCT e.notice_id)
            FROM mock_bid_evaluations e
            JOIN bid_notices n ON n.notice_id = e.notice_id
            WHERE {where_sql}
              AND date(e.evaluated_at, 'localtime') = date('now', 'localtime')
            """,
            params,
        ).fetchone()[0]
        auto_covered = conn.execute(
            f"""
            SELECT COUNT(DISTINCT m.notice_id)
            FROM mock_bids m
            JOIN bid_notices n ON n.notice_id = m.notice_id
            LEFT JOIN bid_results r ON r.notice_id = n.notice_id
            WHERE {where_sql}
              AND m.note LIKE 'auto:%'
              AND (r.notice_id IS NULL OR r.bid_rate <= 0)
            """,
            params,
        ).fetchone()[0]
        api_row = conn.execute(
            """
            SELECT collect_api_calls, agency_api_calls, auto_bid_runs,
                   auto_bid_notices, auto_bid_customer_bids
            FROM automation_daily_stats
            WHERE stat_date = date('now', 'localtime')
            """
        ).fetchone()
        # Live counts from mock_bids (including in-flight sim). daily_stats only
        # gets bumped at the end of a run, so this complements that metric.
        live_row = conn.execute(
            """
            SELECT COUNT(*) AS customer_bids,
                   COUNT(DISTINCT notice_id) AS notices
            FROM mock_bids
            WHERE date(submitted_at, 'localtime') = date('now', 'localtime')
              AND simulation_id != ''
            """
        ).fetchone()
    run_row = get_latest_automation_run(db_path, "auto_bid_pending")
    progress_pct = 0.0
    live_run_notice_count = 0
    live_run_customer_bid_count = 0
    if run_row and (run_row["total_items"] or 0) > 0:
        progress_pct = round((run_row["processed_items"] / run_row["total_items"]) * 100.0, 1)
    if run_row:
        with connect(db_path) as conn:
            live_run_row = conn.execute(
                """
                SELECT COUNT(*) AS customer_bids,
                       COUNT(DISTINCT notice_id) AS notices
                FROM mock_bids
                WHERE simulation_id = ?
                """,
                (str(run_row["run_id"]),),
            ).fetchone()
        if live_run_row:
            live_run_notice_count = int(live_run_row["notices"] or 0)
            live_run_customer_bid_count = int(live_run_row["customer_bids"] or 0)
    return {
        "pending_total": pending_total,
        "new_today": new_today,
        "completed_today": completed_today,
        "evaluated_today": evaluated_today,
        "auto_covered_pending": auto_covered,
        "collect_api_calls_today": int(api_row["collect_api_calls"]) if api_row else 0,
        "agency_api_calls_today": int(api_row["agency_api_calls"]) if api_row else 0,
        "total_api_calls_today": (
            int(api_row["collect_api_calls"]) + int(api_row["agency_api_calls"])
            if api_row else 0
        ),
        "auto_bid_runs_today": int(api_row["auto_bid_runs"]) if api_row else 0,
        "auto_bid_notices_today": max(
            int(api_row["auto_bid_notices"]) if api_row else 0,
            int(live_row["notices"]) if live_row else 0,
        ),
        "auto_bid_customer_bids_today": max(
            int(api_row["auto_bid_customer_bids"]) if api_row else 0,
            int(live_row["customer_bids"]) if live_row else 0,
        ),
        "auto_bid_notices_live_today": int(live_row["notices"]) if live_row else 0,
        "auto_bid_customer_bids_live_today": int(live_row["customer_bids"]) if live_row else 0,
        "latest_auto_bid_saved_notices": live_run_notice_count,
        "latest_auto_bid_saved_customer_bids": live_run_customer_bid_count,
        "latest_auto_bid_run": dict(run_row) if run_row else None,
        "latest_auto_bid_progress_pct": progress_pct,
        "latest_auto_bid_task_summary": (
            summarize_run_tasks(db_path, run_row["run_id"]) if run_row else None
        ),
        "latest_auto_bid_active_task": _latest_active_task(db_path, run_row["run_id"]) if run_row else None,
        "latest_auto_bid_new_computed": (
            max(0, int(run_row["processed_items"]) - int(run_row["resumed_items"] or 0))
            if run_row else 0
        ),
    }


def _latest_active_task(db_path: str | Path, run_id: str) -> dict | None:
    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT task_id, category, contract_method, task_seq,
                   total_items, processed_items, success_items, failed_items,
                   status, message, started_at, updated_at
            FROM automation_run_tasks
            WHERE run_id = ? AND status = 'running'
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (run_id,),
        ).fetchone()
    return dict(row) if row else None


def get_monitoring_overview(db_path: str | Path) -> dict:
    categories = ["service", "goods", "construction"]
    freshness: list[dict] = []
    with connect(db_path) as conn:
        for category in categories:
            row = conn.execute(
                """
                SELECT
                    MAX(opened_at) AS latest_opened_at,
                    MAX(created_at) AS latest_ingested_at
                FROM bid_notices
                WHERE category = ?
                """,
                (category,),
            ).fetchone()
            freshness.append(
                {
                    "category": category,
                    "latest_opened_at": row["latest_opened_at"] if row else None,
                    "latest_ingested_at": row["latest_ingested_at"] if row else None,
                }
            )
        unresolved_results = conn.execute(
            """
            SELECT COUNT(DISTINCT r.notice_id)
            FROM bid_results r
            LEFT JOIN mock_bids m ON m.notice_id = r.notice_id
            LEFT JOIN mock_bid_evaluations e ON e.notice_id = r.notice_id
            WHERE r.award_amount > 0
              AND r.bid_rate > 0
              AND date(r.created_at, 'localtime') = date('now', 'localtime')
              AND (m.notice_id IS NULL OR e.notice_id IS NULL)
            """
        ).fetchone()[0]
    runs = list_latest_automation_runs(
        db_path,
        [
            "collect_recent:service",
            "collect_recent:goods",
            "collect_recent:construction",
            "sync_demand_agencies",
            "auto_bid_pending",
        ],
    )
    return {
        "freshness": freshness,
        "latest_runs": runs,
        "unresolved_results_today": unresolved_results,
    }


def get_monitoring_alerts(db_path: str | Path) -> list[dict]:
    alerts: list[dict] = []
    overview = get_monitoring_overview(db_path)
    summary = get_operations_summary(db_path)

    for item in overview["freshness"]:
        if not item["latest_opened_at"]:
            alerts.append(
                {
                    "severity": "high",
                    "title": f"{item['category']} 데이터 없음",
                    "detail": "해당 카테고리에 적재된 공고가 없습니다.",
                }
            )

    for run in overview["latest_runs"]:
        status = run.get("status")
        kind = run.get("kind")
        duration_min = _run_duration_minutes(run)
        if status == "failed":
            alerts.append(
                {
                    "severity": "high",
                    "title": f"{kind} 최근 실행 실패",
                    "detail": run.get("message") or "최근 배치가 실패했습니다.",
                }
            )
        elif status == "running":
            total = int(run.get("total_items") or 0)
            processed = int(run.get("processed_items") or 0)
            if total > 0 and processed < total:
                alerts.append(
                    {
                        "severity": "medium",
                        "title": f"{kind} 실행 중",
                        "detail": f"{processed}/{total} 처리 완료",
                    }
                )
            runtime_limit = 30.0 if kind == "auto_bid_pending" else 10.0
            if duration_min is not None and duration_min > runtime_limit:
                alerts.append(
                    {
                        "severity": "medium",
                        "title": f"{kind} 장시간 실행",
                        "detail": f"{duration_min:.1f}분째 실행 중입니다. 임계치 {runtime_limit:.0f}분 초과.",
                    }
                )
        elif status == "completed":
            runtime_limit = 30.0 if kind == "auto_bid_pending" else 10.0
            if duration_min is not None and duration_min > runtime_limit:
                alerts.append(
                    {
                        "severity": "low",
                        "title": f"{kind} 소요시간 증가",
                        "detail": f"최근 배치가 {duration_min:.1f}분 소요됐습니다. 임계치 {runtime_limit:.0f}분 초과.",
                    }
                )

    pending_total = int(summary["pending_total"])
    auto_covered = int(summary["auto_covered_pending"])
    if pending_total > 0 and auto_covered < pending_total:
        gap = pending_total - auto_covered
        alerts.append(
            {
                "severity": "medium",
                "title": "자동입찰 미커버 공고 존재",
                "detail": f"진행 중 {pending_total}건 중 {gap}건이 아직 자동입찰 미커버 상태입니다.",
            }
        )

    if overview["unresolved_results_today"] > 0:
        alerts.append(
            {
                "severity": "medium",
                "title": "오늘 낙찰 결과 미평가 공고 존재",
                "detail": f"오늘 결과가 들어온 공고 중 {overview['unresolved_results_today']}건은 mock_bids가 없어 평가되지 않습니다.",
            }
        )

    if summary["collect_api_calls_today"] == 0:
        alerts.append(
            {
                "severity": "medium",
                "title": "오늘 수집 API 호출 없음",
                "detail": "일일 증분 수집이 아직 돌지 않았거나 실패했을 수 있습니다.",
            }
        )

    baseline = _daily_notice_baseline(db_path)
    today_new = int(summary["new_today"])
    if baseline is not None and baseline >= 5 and today_new < max(1, baseline * 0.25):
        alerts.append(
            {
                "severity": "medium",
                "title": "오늘 신규 입수 건수 급감",
                "detail": f"오늘 신규 입수 {today_new}건으로 최근 기준치 {baseline:.0f}건 대비 낮습니다.",
            }
        )

    return alerts


def list_agencies_with_backtestable_notices(
    db_path: str | Path,
    category: str | None = None,
    min_notices: int = 1,
) -> list[tuple[str, int]]:
    """Agencies with at least `min_notices` backtestable awards, ordered by count desc."""
    filters = [
        "n.agency_name != ''",
        "n.contract_method != ''",
        "n.base_amount > 0",
        "r.bid_rate > 0",
        "r.award_amount > 0",
        "r.result_status = 'awarded'",
        "COALESCE(n.opened_at, '') != ''",
    ]
    params: list = []
    if category:
        filters.append("n.category = ?")
        params.append(category)
    where_sql = " AND ".join(filters)
    params.append(min_notices)

    with connect(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT n.agency_name, COUNT(*) AS notice_count
            FROM bid_notices n
            JOIN bid_results r ON r.notice_id = n.notice_id
            WHERE {where_sql}
            GROUP BY n.agency_name
            HAVING notice_count >= ?
            ORDER BY notice_count DESC, n.agency_name ASC
            """,
            params,
        ).fetchall()
    return [(row["agency_name"], row["notice_count"]) for row in rows]


def load_backtestable_notices_for_agency(
    db_path: str | Path,
    agency_name: str,
    category: str | None = None,
    limit: int | None = None,
) -> list[tuple[BidNoticeSnapshot, ActualAwardOutcome]]:
    filters = [
        "n.agency_name = ?",
        "n.contract_method != ''",
        "n.base_amount > 0",
        "r.bid_rate > 0",
        "r.award_amount > 0",
        "r.result_status = 'awarded'",
        "COALESCE(n.opened_at, '') != ''",
    ]
    params: list = [agency_name]
    if category:
        filters.append("n.category = ?")
        params.append(category)
    where_sql = " AND ".join(filters)

    tail = ""
    if limit is not None:
        tail = "ORDER BY n.opened_at DESC, n.notice_id DESC LIMIT ?"
        params.append(int(limit))
    else:
        tail = "ORDER BY n.opened_at ASC, n.notice_id ASC"

    with connect(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT
                n.notice_id, n.agency_name, n.category, n.contract_method, n.region,
                n.base_amount, n.floor_rate, n.opened_at,
                r.award_amount, r.bid_rate, r.bidder_count,
                COALESCE(r.winning_company, '') AS winning_company,
                COALESCE(r.result_status, '') AS result_status
            FROM bid_notices n
            JOIN bid_results r ON r.notice_id = n.notice_id
            WHERE {where_sql}
            {tail}
            """,
            params,
        ).fetchall()

    pairs: list[tuple[BidNoticeSnapshot, ActualAwardOutcome]] = []
    for row in rows:
        notice = BidNoticeSnapshot(
            notice_id=row["notice_id"],
            agency_name=row["agency_name"],
            category=row["category"],
            contract_method=row["contract_method"],
            region=row["region"],
            base_amount=row["base_amount"],
            floor_rate=row["floor_rate"],
            opened_at=row["opened_at"],
        )
        actual = ActualAwardOutcome(
            notice_id=row["notice_id"],
            award_amount=row["award_amount"],
            bid_rate=row["bid_rate"],
            bidder_count=row["bidder_count"],
            winning_company=row["winning_company"],
            result_status=row["result_status"],
        )
        pairs.append((notice, actual))
    return pairs


def sample_awarded_notice_ids(
    db_path: str | Path,
    category: str,
    sample_size: int,
    seed: int | None = None,
) -> list[str]:
    with connect(db_path) as conn:
        if seed is not None:
            conn.execute("SELECT ?", (seed,))
        rows = conn.execute(
            """
            SELECT n.notice_id
            FROM bid_notices n
            JOIN bid_results r ON r.notice_id = n.notice_id
            WHERE n.category = ?
              AND n.agency_name != ''
              AND n.contract_method != ''
              AND n.base_amount > 0
              AND r.award_amount > 0
              AND r.bid_rate > 0
              AND COALESCE(n.opened_at, '') != ''
            ORDER BY RANDOM()
            LIMIT ?
            """,
            (category, sample_size),
        ).fetchall()
    return [row["notice_id"] for row in rows]


def get_actual_award(db_path: str | Path, notice_id: str) -> ActualAwardOutcome | None:
    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT notice_id, award_amount, bid_rate, bidder_count,
                   COALESCE(winning_company, '') AS winning_company,
                   COALESCE(result_status, '') AS result_status
            FROM bid_results
            WHERE notice_id = ?
            """,
            (notice_id,),
        ).fetchone()

    if row is None:
        return None

    return ActualAwardOutcome(
        notice_id=row["notice_id"],
        award_amount=row["award_amount"],
        bid_rate=row["bid_rate"],
        bidder_count=row["bidder_count"],
        winning_company=row["winning_company"],
        result_status=row["result_status"],
    )


def search_notices(
    db_path: str | Path,
    query: str,
    category: str | None = None,
    limit: int = 100,
) -> list[dict]:
    """Search notices by agency_name substring or notice_id substring.

    Returns a list of notice summaries (notice_id, agency_name, category,
    contract_method, base_amount, opened_at, has_result, winning_company,
    award_amount, bid_rate). Ordered by opened_at DESC.
    """
    text = (query or "").strip()
    if not text:
        return []
    filters = [
        "(n.agency_name LIKE ? OR n.notice_id LIKE ?)",
    ]
    params: list = [f"%{text}%", f"%{text}%"]
    if category:
        filters.append("n.category = ?")
        params.append(category)
    where_sql = " AND ".join(filters)
    with connect(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT n.notice_id, n.agency_name, n.agency_code, n.category,
                   n.contract_method, n.region, n.base_amount, n.floor_rate,
                   n.opened_at,
                   CASE WHEN r.notice_id IS NOT NULL THEN 1 ELSE 0 END AS has_result,
                   COALESCE(r.winning_company, '') AS winning_company,
                   COALESCE(r.winner_biz_no, '') AS winner_biz_no,
                   r.award_amount, r.bid_rate, r.bidder_count,
                   COALESCE(r.result_status, '') AS result_status
            FROM bid_notices n
            LEFT JOIN bid_results r ON r.notice_id = n.notice_id
            WHERE {where_sql}
            ORDER BY n.opened_at DESC, n.notice_id DESC
            LIMIT ?
            """,
            params + [int(limit)],
        ).fetchall()
    return [dict(row) for row in rows]


def get_notice_snapshot(db_path: str | Path, notice_id: str) -> BidNoticeSnapshot | None:
    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT notice_id, agency_name, category, contract_method, region, base_amount, floor_rate, opened_at
            FROM bid_notices
            WHERE notice_id = ?
            """,
            (notice_id,),
        ).fetchone()

    if row is None:
        return None

    return BidNoticeSnapshot(
        notice_id=row["notice_id"],
        agency_name=row["agency_name"],
        category=row["category"],
        contract_method=row["contract_method"],
        region=row["region"],
        base_amount=row["base_amount"],
        floor_rate=row["floor_rate"],
        opened_at=row["opened_at"],
    )
