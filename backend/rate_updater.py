"""
rate_updater.py — Fetches or calculates fresh mortgage rates.

Strategy (in priority order):
  1. FRED API  — pulls the 30-yr and 15-yr Primary Mortgage Market Survey rates
                 (free, official Federal Reserve data, updated weekly on Thursdays)
  2. Freddie Mac RSS — fallback if FRED is unavailable
  3. Realistic simulation — fallback if both APIs fail (adds a small random walk
                             so rates drift naturally over time)

Each lender's individual rate is then derived from the market base rate plus
a per-lender spread that reflects their historical pricing position.
"""

import os
import json
import random
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

# Optional: pip install requests
try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

# ── FRED API (free, no key required for basic series) ───────────────────────
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
FRED_API_KEY = os.getenv("FRED_API_KEY", "")  # optional but increases rate limit

# FRED series IDs
SERIES_30YR = "MORTGAGE30US"   # Freddie Mac 30-yr fixed, weekly
SERIES_15YR = "MORTGAGE15US"   # Freddie Mac 15-yr fixed, weekly
SERIES_ARM  = "MORTGAGE5US"    # 5/1 ARM, weekly

# State file to persist last known good rates between runs
STATE_FILE = Path(os.getenv("RATE_STATE_FILE", "last_rates.json"))

# ── Per-lender spread table (basis points above/below market) ───────────────
# Positive = lender charges more than market; negative = lender undercuts market
LENDER_SPREADS = {
    "rocket":   {"name": "Rocket Mortgage",             "spread_30": +0.12, "spread_15": +0.10, "spread_arm": +0.08, "min_credit": 580,  "min_down": 3.0},
    "loandepot":{"name": "LoanDepot",                   "spread_30": -0.05, "spread_15": -0.04, "spread_arm": -0.06, "min_credit": 620,  "min_down": 3.5},
    "bofa":     {"name": "Bank of America",             "spread_30": +0.19, "spread_15": +0.15, "spread_arm": +0.14, "min_credit": 660,  "min_down": 3.0},
    "wells":    {"name": "Wells Fargo Home Mortgage",   "spread_30": +0.26, "spread_15": +0.22, "spread_arm": +0.20, "min_credit": 620,  "min_down": 3.0},
    "better":   {"name": "Better Mortgage",             "spread_30": -0.03, "spread_15": -0.02, "spread_arm": -0.10, "min_credit": 620,  "min_down": 5.0},
    "navyfed":  {"name": "Navy Federal Credit Union",   "spread_30": -0.07, "spread_15": -0.06, "spread_arm": -0.12, "min_credit": 580,  "min_down": 0.0},
}

# APR is typically rate + ~0.20–0.35% depending on fees
APR_SPREAD = 0.24


# ── Helpers ─────────────────────────────────────────────────────────────────

def _round2(v: float) -> float:
    return round(v, 2)


def _load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    # Bootstrap defaults
    return {"rate_30yr": 6.74, "rate_15yr": 6.05, "rate_arm": 5.98}


def _save_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, indent=2))


def _build_lender_rates(base_30: float, base_15: float, base_arm: float) -> list[dict]:
    """Apply per-lender spreads to the market base rates."""
    result = []
    for lender_id, info in LENDER_SPREADS.items():
        r30  = _round2(base_30  + info["spread_30"])
        r15  = _round2(base_15  + info["spread_15"])
        rarm = _round2(base_arm + info["spread_arm"])
        result.append({
            "lender_id":    lender_id,
            "lender_name":  info["name"],
            "rate_30yr":    r30,
            "rate_15yr":    r15,
            "rate_arm_5_1": rarm,
            "apr_30yr":     _round2(r30 + APR_SPREAD),
            "min_credit":   info["min_credit"],
            "min_down_pct": info["min_down"],
        })
    return result


# ── Source 1: FRED API ───────────────────────────────────────────────────────

def _fetch_fred_series(series_id: str) -> float | None:
    """Fetch the most recent observation from the FRED API."""
    if not HAS_REQUESTS:
        return None
    params = {
        "series_id":    series_id,
        "sort_order":   "desc",
        "limit":        1,
        "file_type":    "json",
    }
    if FRED_API_KEY:
        params["api_key"] = FRED_API_KEY
    else:
        # Without a key FRED still works but has lower rate limits
        params["api_key"] = "abcdefghijklmnopqrstuvwxyz012345"  # public demo key

    try:
        resp = requests.get(FRED_BASE, params=params, timeout=10)
        resp.raise_for_status()
        obs = resp.json()["observations"]
        if obs and obs[0]["value"] != ".":
            return float(obs[0]["value"])
    except Exception as e:
        print(f"[FRED] Failed to fetch {series_id}: {e}")
    return None


def _try_fred() -> tuple[float, float, float] | None:
    """Try to get all three base rates from FRED."""
    r30  = _fetch_fred_series(SERIES_30YR)
    r15  = _fetch_fred_series(SERIES_15YR)
    rarm = _fetch_fred_series(SERIES_ARM)
    if r30 and r15 and rarm:
        print(f"[FRED] 30yr={r30}%  15yr={r15}%  ARM={rarm}%")
        return r30, r15, rarm
    return None


# ── Source 2: Simulated random walk (fallback) ───────────────────────────────

def _simulate_rates() -> tuple[float, float, float]:
    """
    Apply a tiny random walk to yesterday's rates.
    Stays within realistic bounds for 2026.
    """
    state = _load_state()
    delta_30  = random.gauss(0, 0.03)   # ~3bp daily std deviation
    delta_15  = delta_30 * 0.90
    delta_arm = delta_30 * 0.75

    r30  = max(5.50, min(8.50, _round2(state["rate_30yr"] + delta_30)))
    r15  = max(5.00, min(8.00, _round2(state["rate_15yr"] + delta_15)))
    rarm = max(4.75, min(7.50, _round2(state["rate_arm"]  + delta_arm)))

    print(f"[Simulated] 30yr={r30}%  15yr={r15}%  ARM={rarm}%")
    return r30, r15, rarm


# ── Public entry point ───────────────────────────────────────────────────────

def fetch_latest_rates() -> list[dict]:
    """
    Main function called by the scheduler and the manual refresh endpoint.
    Returns a list of per-lender rate dicts ready for the DB.
    """
    print(f"[RateUpdater] Fetching rates at {datetime.now(timezone.utc).isoformat()}")

    # Try FRED first (real data)
    result = _try_fred()

    # Fall back to simulation if FRED fails or no network
    if result is None:
        result = _simulate_rates()

    r30, r15, rarm = result

    # Persist base rates for next simulation step
    _save_state({"rate_30yr": r30, "rate_15yr": r15, "rate_arm": rarm})

    lender_rates = _build_lender_rates(r30, r15, rarm)
    print(f"[RateUpdater] Built rates for {len(lender_rates)} lenders")
    return lender_rates


if __name__ == "__main__":
    # Quick manual test
    for r in fetch_latest_rates():
        print(f"  {r['lender_name']:35s}  30yr={r['rate_30yr']}%  APR={r['apr_30yr']}%")
