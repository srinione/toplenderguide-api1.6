"""
TopLenderGuide.com — Backend API
Serves mortgage rates and triggers daily rate updates.

Run with:  uvicorn app:app --host 0.0.0.0 --port 8000 --reload
"""

from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import uvicorn
import os

from database import init_db, get_all_rates, get_rate_history, upsert_rates
from scheduler import start_scheduler, stop_scheduler
from rate_updater import fetch_latest_rates

# ── Startup / Shutdown ──────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()                     # create tables if not exist
    upsert_rates(fetch_latest_rates())  # seed with today's rates on first boot
    start_scheduler()             # kick off daily background job
    yield
    stop_scheduler()

app = FastAPI(
    title="TopLenderGuide Rate API",
    description="Daily mortgage rate backend for toplenderguide.com",
    version="1.0.0",
    lifespan=lifespan,
)

# ── CORS — allow your frontend domain ──────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "https://toplenderguide.com",
        "https://www.toplenderguide.com",
        "*",  # remove in production, replace with your actual domain
    ],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ── Auth helper (optional API key guard) ───────────────────────────────────
ADMIN_KEY = os.getenv("ADMIN_API_KEY", "change-me-in-production")

def require_admin(x_api_key: str = Header(...)):
    if x_api_key != ADMIN_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")


# ── Public endpoints ────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {"status": "ok", "service": "TopLenderGuide Rate API"}


@app.get("/api/rates")
def get_rates():
    """
    Returns the latest mortgage rate for every lender.
    Called by the frontend on page load.
    """
    rates = get_all_rates()
    return {"updated_at": rates[0]["updated_at"] if rates else None, "lenders": rates}


@app.get("/api/rates/{lender_id}")
def get_lender_rate(lender_id: str):
    """Returns current rate + 30-day history for one lender."""
    history = get_rate_history(lender_id, days=30)
    if not history:
        raise HTTPException(status_code=404, detail="Lender not found")
    return {"lender_id": lender_id, "history": history}


# ── Admin / internal endpoints ──────────────────────────────────────────────

@app.post("/api/admin/refresh-rates")
def manual_refresh(key=Depends(require_admin)):
    """
    Manually trigger a rate refresh without waiting for the scheduler.
    POST /api/admin/refresh-rates  (header: X-API-Key: <your-key>)
    """
    new_rates = fetch_latest_rates()
    upsert_rates(new_rates)
    return {"status": "refreshed", "count": len(new_rates)}


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
