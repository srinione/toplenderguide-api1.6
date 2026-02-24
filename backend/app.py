"""
TopLenderGuide.com — Backend API
"""

from fastapi import FastAPI, HTTPException, Depends, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from contextlib import asynccontextmanager
import io, csv
import uvicorn
import os

from database import init_db, get_all_rates, get_rate_history, upsert_rates, get_all_history, get_history_stats
from scheduler import start_scheduler, stop_scheduler
from rate_updater import fetch_latest_rates

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    upsert_rates(fetch_latest_rates())
    start_scheduler()
    yield
    stop_scheduler()

app = FastAPI(
    title="TopLenderGuide Rate API",
    description="Daily mortgage rate backend for toplenderguide.com",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

ADMIN_KEY = os.getenv("ADMIN_API_KEY", "change-me-in-production")

def require_admin(x_api_key: str = Header(...)):
    if x_api_key != ADMIN_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")


# ── Public endpoints ─────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {"status": "ok", "service": "TopLenderGuide Rate API v2"}


@app.get("/api/rates")
def get_rates():
    """Latest rate snapshot for all lenders."""
    rates = get_all_rates()
    return {
        "updated_at": rates[0]["updated_at"] if rates else None,
        "lenders": rates
    }


@app.get("/api/rates/{lender_id}")
def get_lender_rate(lender_id: str, days: int = Query(default=90, ge=1, le=365)):
    """Current rate + history for one lender."""
    history = get_rate_history(lender_id, days=days)
    if not history:
        raise HTTPException(status_code=404, detail="Lender not found")
    return {"lender_id": lender_id, "history": history}


@app.get("/api/history")
def get_history(days: int = Query(default=90, ge=1, le=365)):
    """
    Full rate history for ALL lenders for the past N days.
    Used by the frontend chart and data dashboard.
    """
    rows = get_all_history(days=days)
    stats = get_history_stats()
    return {
        "days_requested": days,
        "stats": stats,
        "history": rows
    }


@app.get("/api/export.csv")
def export_csv(days: int = Query(default=365, ge=1, le=3650)):
    """
    Download all historical rate data as a CSV file.
    No auth required — data is public.
    """
    rows = get_all_history(days=days)

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=[
        "recorded_at", "lender_id", "lender_name",
        "rate_30yr", "rate_15yr", "rate_arm_5_1", "apr_30yr"
    ])
    writer.writeheader()
    for r in rows:
        writer.writerow({k: r.get(k, "") for k in writer.fieldnames})

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=toplenderguide_rates_{days}days.csv"}
    )


@app.get("/api/stats")
def get_stats():
    """Summary statistics about stored rate history."""
    return get_history_stats()


# ── Admin endpoints (require API key) ────────────────────────────────────────

@app.post("/admin/refresh", dependencies=[Depends(require_admin)])
def manual_refresh():
    """Force an immediate rate update (bypasses scheduler)."""
    rates = fetch_latest_rates()
    upsert_rates(rates)
    return {
        "status": "ok",
        "updated": len(rates),
        "lenders": [r["lender_id"] for r in rates]
    }


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
