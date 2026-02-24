"""
database.py — Persistent storage for mortgage rates.

Auto-detects the database engine:
  • If DATABASE_URL env var is set → uses PostgreSQL (Render managed Postgres)
  • Otherwise                      → uses SQLite (local dev / Docker with volume)

Schema
------
rates        : latest rate snapshot per lender (one row per lender, upserted daily)
rate_history : time-series of every recorded rate — never deleted, grows daily
"""

import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

# ── Engine detection ─────────────────────────────────────────────────────────
DATABASE_URL = os.getenv("DATABASE_URL", "")

if DATABASE_URL:
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    try:
        import psycopg2
        import psycopg2.extras
        _USE_POSTGRES = True
        print("[DB] Using PostgreSQL")
    except ImportError:
        print("[DB] WARNING: psycopg2 not installed — falling back to SQLite")
        _USE_POSTGRES = False
else:
    _USE_POSTGRES = False

DB_PATH = os.getenv("DB_PATH", "rates.db")


# ── Connections ───────────────────────────────────────────────────────────────

def _pg_conn():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    return conn

def _sqlite_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ── Schema ────────────────────────────────────────────────────────────────────

def init_db():
    if _USE_POSTGRES:
        with _pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS rates (
                        lender_id    TEXT PRIMARY KEY,
                        lender_name  TEXT NOT NULL,
                        rate_30yr    NUMERIC(5,2) NOT NULL,
                        rate_15yr    NUMERIC(5,2) NOT NULL,
                        rate_arm_5_1 NUMERIC(5,2) NOT NULL,
                        apr_30yr     NUMERIC(5,2) NOT NULL,
                        min_credit   INTEGER NOT NULL,
                        min_down_pct NUMERIC(5,2) NOT NULL,
                        updated_at   TIMESTAMPTZ NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS rate_history (
                        id           SERIAL PRIMARY KEY,
                        lender_id    TEXT NOT NULL,
                        rate_30yr    NUMERIC(5,2) NOT NULL,
                        rate_15yr    NUMERIC(5,2) NOT NULL,
                        rate_arm_5_1 NUMERIC(5,2) NOT NULL,
                        apr_30yr     NUMERIC(5,2) NOT NULL,
                        recorded_at  TIMESTAMPTZ NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_history_lender
                        ON rate_history (lender_id, recorded_at DESC);
                    CREATE INDEX IF NOT EXISTS idx_history_date
                        ON rate_history (recorded_at DESC);
                """)
            conn.commit()
    else:
        with _sqlite_conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS rates (
                    lender_id    TEXT PRIMARY KEY,
                    lender_name  TEXT NOT NULL,
                    rate_30yr    REAL NOT NULL,
                    rate_15yr    REAL NOT NULL,
                    rate_arm_5_1 REAL NOT NULL,
                    apr_30yr     REAL NOT NULL,
                    min_credit   INTEGER NOT NULL,
                    min_down_pct REAL NOT NULL,
                    updated_at   TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS rate_history (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    lender_id    TEXT NOT NULL,
                    rate_30yr    REAL NOT NULL,
                    rate_15yr    REAL NOT NULL,
                    rate_arm_5_1 REAL NOT NULL,
                    apr_30yr     REAL NOT NULL,
                    recorded_at  TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_history_lender
                    ON rate_history (lender_id, recorded_at DESC);
                CREATE INDEX IF NOT EXISTS idx_history_date
                    ON rate_history (recorded_at DESC);
            """)
    print(f"[DB] Initialized ({'PostgreSQL' if _USE_POSTGRES else 'SQLite'})")


# ── Write ─────────────────────────────────────────────────────────────────────

def upsert_rates(rate_list: list[dict]):
    """
    Upsert latest snapshot AND append to history once per day per lender.
    Prevents duplicate history rows on service restarts.
    """
    now = datetime.now(timezone.utc).isoformat()

    if _USE_POSTGRES:
        with _pg_conn() as conn:
            with conn.cursor() as cur:
                for r in rate_list:
                    r["updated_at"] = now
                    cur.execute("""
                        INSERT INTO rates
                            (lender_id, lender_name, rate_30yr, rate_15yr, rate_arm_5_1,
                             apr_30yr, min_credit, min_down_pct, updated_at)
                        VALUES
                            (%(lender_id)s, %(lender_name)s, %(rate_30yr)s, %(rate_15yr)s,
                             %(rate_arm_5_1)s, %(apr_30yr)s, %(min_credit)s,
                             %(min_down_pct)s, %(updated_at)s)
                        ON CONFLICT (lender_id) DO UPDATE SET
                            rate_30yr    = EXCLUDED.rate_30yr,
                            rate_15yr    = EXCLUDED.rate_15yr,
                            rate_arm_5_1 = EXCLUDED.rate_arm_5_1,
                            apr_30yr     = EXCLUDED.apr_30yr,
                            min_credit   = EXCLUDED.min_credit,
                            min_down_pct = EXCLUDED.min_down_pct,
                            updated_at   = EXCLUDED.updated_at
                    """, r)
                    # Append to history only once per calendar day per lender
                    cur.execute("""
                        INSERT INTO rate_history
                            (lender_id, rate_30yr, rate_15yr, rate_arm_5_1, apr_30yr, recorded_at)
                        SELECT %(lender_id)s, %(rate_30yr)s, %(rate_15yr)s,
                               %(rate_arm_5_1)s, %(apr_30yr)s, %(updated_at)s
                        WHERE NOT EXISTS (
                            SELECT 1 FROM rate_history
                            WHERE lender_id = %(lender_id)s
                              AND recorded_at::date = %(updated_at)s::date
                        )
                    """, r)
            conn.commit()
    else:
        with _sqlite_conn() as conn:
            for r in rate_list:
                r["updated_at"] = now
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
                conn.execute("""
                    INSERT INTO rate_history
                        (lender_id, rate_30yr, rate_15yr, rate_arm_5_1, apr_30yr, recorded_at)
                    SELECT :lender_id, :rate_30yr, :rate_15yr, :rate_arm_5_1, :apr_30yr, :updated_at
                    WHERE NOT EXISTS (
                        SELECT 1 FROM rate_history
                        WHERE lender_id = :lender_id
                          AND date(recorded_at) = date(:updated_at)
                    )
                """, r)

    print(f"[DB] Upserted {len(rate_list)} lender rates at {now}")


# ── Read ──────────────────────────────────────────────────────────────────────

def get_all_rates() -> list[dict]:
    """Latest snapshot for all lenders, sorted cheapest first."""
    if _USE_POSTGRES:
        with _pg_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM rates ORDER BY rate_30yr ASC")
                return [dict(r) for r in cur.fetchall()]
    else:
        with _sqlite_conn() as conn:
            return [dict(r) for r in conn.execute(
                "SELECT * FROM rates ORDER BY rate_30yr ASC"
            ).fetchall()]


def get_rate_history(lender_id: str, days: int = 90) -> list[dict]:
    """Historical rates for one lender, newest first."""
    if _USE_POSTGRES:
        with _pg_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT * FROM rate_history
                    WHERE lender_id = %s
                    ORDER BY recorded_at DESC LIMIT %s
                """, (lender_id, days))
                return [dict(r) for r in cur.fetchall()]
    else:
        with _sqlite_conn() as conn:
            return [dict(r) for r in conn.execute("""
                SELECT * FROM rate_history
                WHERE lender_id = ?
                ORDER BY recorded_at DESC LIMIT ?
            """, (lender_id, days)).fetchall()]


def get_all_history(days: int = 365) -> list[dict]:
    """Full history for ALL lenders for the past N days, newest first."""
    if _USE_POSTGRES:
        with _pg_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT h.id, h.lender_id, r.lender_name,
                           h.rate_30yr, h.rate_15yr, h.rate_arm_5_1, h.apr_30yr,
                           h.recorded_at
                    FROM rate_history h
                    JOIN rates r USING (lender_id)
                    WHERE h.recorded_at >= NOW() - (%s || ' days')::INTERVAL
                    ORDER BY h.recorded_at DESC, h.lender_id
                """, (str(days),))
                return [dict(r) for r in cur.fetchall()]
    else:
        with _sqlite_conn() as conn:
            return [dict(r) for r in conn.execute("""
                SELECT h.id, h.lender_id, r.lender_name,
                       h.rate_30yr, h.rate_15yr, h.rate_arm_5_1, h.apr_30yr,
                       h.recorded_at
                FROM rate_history h
                JOIN rates r USING (lender_id)
                WHERE h.recorded_at >= datetime('now', ? || ' days')
                ORDER BY h.recorded_at DESC, h.lender_id
            """, (f"-{days}",)).fetchall()]


def get_history_stats() -> dict:
    """Summary: total rows, unique lenders, days covered, date range."""
    if _USE_POSTGRES:
        with _pg_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT COUNT(*)                          AS total_rows,
                           COUNT(DISTINCT lender_id)         AS lender_count,
                           COUNT(DISTINCT recorded_at::date) AS days_covered,
                           MIN(recorded_at)                  AS earliest,
                           MAX(recorded_at)                  AS latest
                    FROM rate_history
                """)
                return dict(cur.fetchone())
    else:
        with _sqlite_conn() as conn:
            return dict(conn.execute("""
                SELECT COUNT(*)                           AS total_rows,
                       COUNT(DISTINCT lender_id)          AS lender_count,
                       COUNT(DISTINCT date(recorded_at))  AS days_covered,
                       MIN(recorded_at)                   AS earliest,
                       MAX(recorded_at)                   AS latest
                FROM rate_history
            """).fetchone())
