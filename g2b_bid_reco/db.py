from __future__ import annotations

import sqlite3
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
        _ensure_column(conn, "bid_notices", "agency_code", "TEXT DEFAULT ''")
        _ensure_column(conn, "procurement_plans", "agency_code", "TEXT DEFAULT ''")
        _ensure_column(conn, "bid_results", "winner_biz_no", "TEXT DEFAULT ''")
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
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_mock_bids_simulation ON mock_bids(simulation_id)"
        )


def _ensure_column(
    conn: sqlite3.Connection, table: str, column: str, type_decl: str
) -> None:
    existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
    if column in existing:
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {type_decl}")


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


def seed_agency_parent_mapping(
    db_path: str | Path,
    min_subunits: int = 10,
    min_parent_cases: int = 50,
    refresh: bool = False,
) -> dict:
    """Populate agency_parent_mapping with first-token-based parent suggestions.

    Only inserts rows for agencies belonging to a parent that (a) has at least
    `min_subunits` distinct sub-units, (b) has at least `min_parent_cases`
    awarded cases, and (c) whose member code prefixes are all within the safe
    single-entity set. Existing rows are preserved unless `refresh=True`.
    """
    with connect(db_path) as conn:
        if refresh:
            conn.execute("DELETE FROM agency_parent_mapping WHERE source='auto'")

        agency_stats = conn.execute(
            """
            SELECT n.agency_name,
                   SUBSTR(n.agency_code,1,1) AS code1,
                   COUNT(DISTINCT n.notice_id) AS notices,
                   SUM(CASE WHEN r.bid_rate > 0 THEN 1 ELSE 0 END) AS awarded
            FROM bid_notices n
            LEFT JOIN bid_results r ON r.notice_id = n.notice_id
            WHERE n.agency_name != ''
            GROUP BY n.agency_name
            """
        ).fetchall()

        parent_buckets: dict[str, list[sqlite3.Row]] = {}
        for row in agency_stats:
            token = _parent_token(row["agency_name"])
            if not token:
                continue
            parent_buckets.setdefault(token, []).append(row)

        inserted = 0
        skipped_unsafe = 0
        skipped_small = 0
        for parent, rows in parent_buckets.items():
            if len(rows) < min_subunits:
                skipped_small += 1
                continue
            parent_awarded_total = sum(row["awarded"] or 0 for row in rows)
            if parent_awarded_total < min_parent_cases:
                skipped_small += 1
                continue
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
    note, customer_idx."""
    if not rows:
        return 0
    with connect(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO mock_bids (notice_id, bid_amount, bid_rate,
                predicted_amount, predicted_rate, note, simulation_id,
                customer_idx, submitted_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                (r["notice_id"], r["bid_amount"], r["bid_rate"],
                 r.get("predicted_amount"), r.get("predicted_rate"),
                 r.get("note", ""), simulation_id, r.get("customer_idx", 0))
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


def load_pending_notices_for_prediction(
    db_path: str | Path,
    category: str | None = None,
    agency_name: str | None = None,
    since_days: int = 180,
    limit: int = 500,
) -> list[BidNoticeSnapshot]:
    """Notices that have predictor-ready metadata but no linked award yet."""
    filters = [
        "n.agency_name != ''",
        "n.contract_method != ''",
        "n.base_amount > 0",
        "COALESCE(n.opened_at, '') != ''",
        "n.opened_at >= date('now', ?)",
        "(r.notice_id IS NULL OR r.bid_rate <= 0)",
    ]
    params: list = [f"-{max(1, since_days)} days"]
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
            LIMIT ?
            """,
            params + [limit],
        ).fetchall()

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
