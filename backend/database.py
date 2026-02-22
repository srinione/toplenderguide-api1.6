"""
database.py — SQLite persistence for mortgage rates.

Schema
------
rates         : latest rate snapshot per lender
rate_history  : time-series of every recorded rate (for sparklines / charts)
"""

import sqlite3
import os
from datetime import datetime, timezone

DB_PATH = os.getenv("DB_PATH", "rates.db")


def _conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Create tables on first run."""
    with _conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS rates (
                lender_id       TEXT PRIMARY KEY,
                lender_name     TEXT NOT NULL,
                rate_30yr       REAL NOT NULL,
                rate_15yr       REAL NOT NULL,
                rate_arm_5_1    REAL NOT NULL,
                apr_30yr        REAL NOT NULL,
                min_credit      INTEGER NOT NULL,
                min_down_pct    REAL NOT NULL,
                updated_at      TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS rate_history (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                lender_id       TEXT NOT NULL,
                rate_30yr       REAL NOT NULL,
                rate_15yr       REAL NOT NULL,
                rate_arm_5_1    REAL NOT NULL,
                apr_30yr        REAL NOT NULL,
                recorded_at     TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_history_lender
                ON rate_history (lender_id, recorded_at DESC);
        """)
    print(f"[DB] Initialized at {DB_PATH}")


def upsert_rates(rate_list: list[dict]):
    """
    Insert-or-replace current rates and append to history.
    `rate_list` is a list of dicts matching the rates table columns.
    """
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as conn:
        for r in rate_list:
            r["updated_at"] = now
            # update snapshot
            conn.execute("""
                INSERT INTO rates
                    (lender_id, lender_name, rate_30yr, rate_15yr, rate_arm_5_1,
                     apr_30yr, min_credit, min_down_pct, updated_at)
                VALUES
                    (:lender_id, :lender_name, :rate_30yr, :rate_15yr, :rate_arm_5_1,
                     :apr_30yr, :min_credit, :min_down_pct, :updated_at)
                ON CONFLICT(lender_id) DO UPDATE SET
                    rate_30yr    = excluded.rate_30yr,
                    rate_15yr    = excluded.rate_15yr,
                    rate_arm_5_1 = excluded.rate_arm_5_1,
                    apr_30yr     = excluded.apr_30yr,
                    min_credit   = excluded.min_credit,
                    min_down_pct = excluded.min_down_pct,
                    updated_at   = excluded.updated_at
            """, r)
            # append to history
            conn.execute("""
                INSERT INTO rate_history
                    (lender_id, rate_30yr, rate_15yr, rate_arm_5_1, apr_30yr, recorded_at)
                VALUES
                    (:lender_id, :rate_30yr, :rate_15yr, :rate_arm_5_1, :apr_30yr, :updated_at)
            """, r)
    print(f"[DB] Upserted {len(rate_list)} lender rates at {now}")


def get_all_rates() -> list[dict]:
    """Return the latest rate snapshot for all lenders, ordered by rate_30yr ASC."""
    with _conn() as conn:
        rows = conn.execute(
            "SELECT * FROM rates ORDER BY rate_30yr ASC"
        ).fetchall()
    return [dict(r) for r in rows]


def get_rate_history(lender_id: str, days: int = 30) -> list[dict]:
    """Return up to `days` historical entries for a lender, newest first."""
    with _conn() as conn:
        rows = conn.execute("""
            SELECT * FROM rate_history
            WHERE lender_id = ?
            ORDER BY recorded_at DESC
            LIMIT ?
        """, (lender_id, days)).fetchall()
    return [dict(r) for r in rows]
