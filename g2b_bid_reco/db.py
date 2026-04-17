from __future__ import annotations

import sqlite3
from pathlib import Path

from .models import BidNoticeSnapshot, HistoricalBidCase


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS procurement_plans (
    plan_id TEXT PRIMARY KEY,
    agency_name TEXT NOT NULL,
    category TEXT NOT NULL,
    budget_amount REAL NOT NULL,
    planned_quarter TEXT,
    contract_method TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bid_notices (
    notice_id TEXT PRIMARY KEY,
    agency_name TEXT NOT NULL,
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
"""


def connect(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str | Path) -> None:
    with connect(db_path) as conn:
        conn.executescript(SCHEMA_SQL)


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
) -> None:
    conn.execute(
        """
        INSERT INTO bid_notices (
            notice_id, agency_name, category, contract_method, region, base_amount, estimated_amount, floor_rate, opened_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(notice_id) DO UPDATE SET
            agency_name = CASE WHEN excluded.agency_name != '' THEN excluded.agency_name ELSE bid_notices.agency_name END,
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


def upsert_bid_result(
    conn: sqlite3.Connection,
    notice_id: str,
    award_amount: float,
    bid_rate: float,
    bidder_count: int,
    winning_company: str,
    result_status: str,
    category: str = "",
) -> None:
    ensure_notice_stub(conn, notice_id, category=category)
    conn.execute(
        """
        INSERT INTO bid_results (
            notice_id, winning_company, award_amount, bid_rate, bidder_count, result_status
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(notice_id) DO UPDATE SET
            winning_company = CASE WHEN excluded.winning_company != '' THEN excluded.winning_company ELSE bid_results.winning_company END,
            award_amount = CASE WHEN excluded.award_amount > 0 THEN excluded.award_amount ELSE bid_results.award_amount END,
            bid_rate = CASE WHEN excluded.bid_rate > 0 THEN excluded.bid_rate ELSE bid_results.bid_rate END,
            bidder_count = CASE WHEN excluded.bidder_count > 0 THEN excluded.bidder_count ELSE bid_results.bidder_count END,
            result_status = CASE WHEN excluded.result_status != '' THEN excluded.result_status ELSE bid_results.result_status END
        """,
        (
            notice_id,
            winning_company,
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
) -> None:
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


def load_historical_cases_for_notice(
    db_path: str | Path,
    notice_id: str,
    cutoff_opened_at: str | None,
) -> list[HistoricalBidCase]:
    with connect(db_path) as conn:
        if cutoff_opened_at:
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
                  AND n.notice_id != ?
                  AND COALESCE(n.opened_at, '') < ?
                ORDER BY n.opened_at DESC, n.notice_id DESC
                """,
                (notice_id, cutoff_opened_at),
            ).fetchall()
        else:
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
                  AND n.notice_id != ?
                ORDER BY n.opened_at DESC, n.notice_id DESC
                """,
                (notice_id,),
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
