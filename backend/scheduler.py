"""
scheduler.py — APScheduler wrapper for the daily rate update job.

The job runs every day at 08:30 ET (when Freddie Mac typically releases
its weekly Primary Mortgage Market Survey on Thursdays; other days we
fall back to the simulation or cached FRED data).

You can override the run time via environment variables:
  RATE_JOB_HOUR   (default: 8)
  RATE_JOB_MINUTE (default: 30)
  RATE_JOB_TZ     (default: America/New_York)
"""

import os
import logging
from datetime import datetime, timezone

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

from rate_updater import fetch_latest_rates
from database import upsert_rates

logger = logging.getLogger("scheduler")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

_scheduler: BackgroundScheduler | None = None

# ── Job definition ───────────────────────────────────────────────────────────

def _run_rate_update():
    """The actual job payload — fetch new rates and persist them."""
    logger.info("⏰ Scheduled rate update starting…")
    try:
        rates = fetch_latest_rates()
        upsert_rates(rates)
        logger.info(f"✅ Rate update complete — {len(rates)} lenders updated at "
                    f"{datetime.now(timezone.utc).isoformat()}")
    except Exception as exc:
        logger.error(f"❌ Rate update FAILED: {exc}", exc_info=True)
        raise  # re-raise so APScheduler marks the job as errored


# ── Event listeners ──────────────────────────────────────────────────────────

def _on_job_executed(event):
    logger.info(f"Job '{event.job_id}' executed successfully (retval={event.retval})")

def _on_job_error(event):
    logger.error(f"Job '{event.job_id}' raised an exception: {event.exception}")


# ── Public API ───────────────────────────────────────────────────────────────

def start_scheduler():
    global _scheduler

    hour   = int(os.getenv("RATE_JOB_HOUR",   "8"))
    minute = int(os.getenv("RATE_JOB_MINUTE", "30"))
    tz     = os.getenv("RATE_JOB_TZ", "America/New_York")

    _scheduler = BackgroundScheduler(timezone=tz)

    _scheduler.add_listener(_on_job_executed, EVENT_JOB_EXECUTED)
    _scheduler.add_listener(_on_job_error,    EVENT_JOB_ERROR)

    _scheduler.add_job(
        func        = _run_rate_update,
        trigger     = CronTrigger(hour=hour, minute=minute, timezone=tz),
        id          = "daily_rate_update",
        name        = "Daily Mortgage Rate Update",
        replace_existing = True,
        misfire_grace_time = 3600,   # allow up to 1 hour late if server was down
    )

    _scheduler.start()
    logger.info(
        f"🗓  Scheduler started — daily rate update at {hour:02d}:{minute:02d} {tz}"
    )


def stop_scheduler():
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped.")


def get_next_run() -> str | None:
    """Returns ISO timestamp of the next scheduled run, or None."""
    if not _scheduler:
        return None
    job = _scheduler.get_job("daily_rate_update")
    if job and job.next_run_time:
        return job.next_run_time.isoformat()
    return None
