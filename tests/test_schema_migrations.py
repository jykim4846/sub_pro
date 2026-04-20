"""Schema migration tests — verify idempotent column adds and backfills.

Currently covers the n_customers column added to mock_bids and
mock_bid_evaluations in MODES.md auto-bid refactor work.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from g2b_bid_reco.db import _backfill_n_customers, connect, init_db


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return column in {row["name"] for row in rows}


def _simulate_legacy_mock_bids_schema(conn: sqlite3.Connection) -> None:
    """Recreate the mock_bids / mock_bid_evaluations shape that existed
    before the n_customers migration — customer_idx present, n_customers absent.
    """
    conn.executescript(
        """
        DROP TABLE IF EXISTS mock_bid_evaluations;
        DROP TABLE IF EXISTS mock_bids;
        CREATE TABLE mock_bids (
            mock_id INTEGER PRIMARY KEY AUTOINCREMENT,
            notice_id TEXT NOT NULL,
            bid_amount REAL NOT NULL,
            bid_rate REAL NOT NULL,
            predicted_amount REAL,
            predicted_rate REAL,
            note TEXT NOT NULL DEFAULT '',
            submitted_at TEXT DEFAULT CURRENT_TIMESTAMP,
            simulation_id TEXT DEFAULT '',
            customer_idx INTEGER DEFAULT 0
        );
        CREATE TABLE mock_bid_evaluations (
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
        """
    )


def test_fresh_db_has_n_customers_columns(tmp_path: Path) -> None:
    db = tmp_path / "fresh.db"
    init_db(db)
    with connect(db) as conn:
        assert _column_exists(conn, "mock_bids", "n_customers")
        assert _column_exists(conn, "mock_bid_evaluations", "n_customers")


def test_init_db_adds_n_customers_to_legacy_schema(tmp_path: Path) -> None:
    db = tmp_path / "legacy.db"
    init_db(db)  # create base schema
    with connect(db) as conn:
        _simulate_legacy_mock_bids_schema(conn)
        assert not _column_exists(conn, "mock_bids", "n_customers")
        assert not _column_exists(conn, "mock_bid_evaluations", "n_customers")
    init_db(db)  # re-run: should add the missing columns
    with connect(db) as conn:
        assert _column_exists(conn, "mock_bids", "n_customers")
        assert _column_exists(conn, "mock_bid_evaluations", "n_customers")


def test_backfill_sets_n_customers_to_max_customer_idx(tmp_path: Path) -> None:
    db = tmp_path / "backfill.db"
    init_db(db)
    with connect(db) as conn:
        _simulate_legacy_mock_bids_schema(conn)
        # Insert two legacy simulations with distinct sizes
        conn.executemany(
            """
            INSERT INTO mock_bids
                (notice_id, bid_amount, bid_rate, simulation_id, customer_idx)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                ("n1", 100.0, 0.80, "sim-a", 1),
                ("n1", 101.0, 0.81, "sim-a", 2),
                ("n1", 102.0, 0.82, "sim-a", 3),
                ("n2", 200.0, 0.75, "sim-b", 1),
                ("n2", 201.0, 0.76, "sim-b", 2),
                ("n3", 300.0, 0.70, "", 0),  # legacy pre-simulation_id row
            ],
        )
        # Also insert matching evaluations
        conn.executemany(
            """
            INSERT INTO mock_bid_evaluations
                (mock_id, notice_id, simulation_id, customer_idx)
            VALUES (?, ?, ?, ?)
            """,
            [
                (1, "n1", "sim-a", 1),
                (2, "n1", "sim-a", 2),
                (3, "n1", "sim-a", 3),
                (4, "n2", "sim-b", 1),
                (5, "n2", "sim-b", 2),
            ],
        )
    init_db(db)  # triggers migration + backfill
    with connect(db) as conn:
        rows = dict(
            (row["simulation_id"], row["n_customers"])
            for row in conn.execute(
                "SELECT simulation_id, n_customers FROM mock_bids "
                "WHERE simulation_id != '' GROUP BY simulation_id, n_customers"
            )
        )
        assert rows == {"sim-a": 3, "sim-b": 2}
        # Legacy row without simulation_id stays at 0 (sentinel — unknown N)
        legacy = conn.execute(
            "SELECT n_customers FROM mock_bids WHERE simulation_id = ''"
        ).fetchone()
        assert legacy["n_customers"] == 0
        eval_rows = dict(
            (row["simulation_id"], row["n_customers"])
            for row in conn.execute(
                "SELECT simulation_id, n_customers FROM mock_bid_evaluations "
                "WHERE simulation_id != '' GROUP BY simulation_id, n_customers"
            )
        )
        assert eval_rows == {"sim-a": 3, "sim-b": 2}


def test_backfill_is_idempotent(tmp_path: Path) -> None:
    db = tmp_path / "idempotent.db"
    init_db(db)
    with connect(db) as conn:
        conn.execute(
            "INSERT INTO mock_bids (notice_id, bid_amount, bid_rate, "
            "simulation_id, customer_idx, n_customers) VALUES "
            "('n1', 100.0, 0.80, 'sim-a', 1, 1)"
        )
        # Manually set an explicit n_customers to a non-derivable value
        conn.execute(
            "INSERT INTO mock_bids (notice_id, bid_amount, bid_rate, "
            "simulation_id, customer_idx, n_customers) VALUES "
            "('n2', 200.0, 0.75, 'sim-b', 1, 7)"
        )
    # Running backfill again must not clobber explicit values
    with connect(db) as conn:
        _backfill_n_customers(conn)
        explicit = conn.execute(
            "SELECT n_customers FROM mock_bids WHERE simulation_id = 'sim-b'"
        ).fetchone()
        assert explicit["n_customers"] == 7


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
