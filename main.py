import asyncio
import logging
import os
import time

from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Request

from analyzer import generate_report
from parser import parse_health_data
from telegram_sender import send_report

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
# Seconds to wait for the second payload before generating the report
MERGE_WINDOW = int(os.getenv("MERGE_WINDOW", "90"))

app = FastAPI(title="Health Report Bot")

# Buffer to accumulate payloads before merging
_buffer: dict = {
    "metrics": {},
    "workouts": [],
    "period": {},
    "received_at": 0.0,
    "pending_task": None,
}
_buffer_lock = asyncio.Lock()


def _merge_into_buffer(parsed: dict) -> None:
    """Merge a parsed payload into the buffer."""
    # Merge metrics (new keys override)
    _buffer["metrics"].update(parsed.get("metrics", {}))

    # Append workouts (avoid duplicates by start time)
    existing_starts = {w["start"] for w in _buffer["workouts"]}
    for w in parsed.get("workouts", []):
        if w["start"] not in existing_starts:
            _buffer["workouts"].append(w)

    # Extend period
    new_period = parsed.get("period", {})
    if new_period:
        if not _buffer["period"]:
            _buffer["period"] = new_period
        else:
            starts = [_buffer["period"].get("start", ""), new_period.get("start", "")]
            ends = [_buffer["period"].get("end", ""), new_period.get("end", "")]
            _buffer["period"]["start"] = min(s for s in starts if s)
            _buffer["period"]["end"] = max(e for e in ends if e)


def _flush_buffer() -> dict:
    """Return merged data and reset the buffer."""
    result = {
        "period": _buffer["period"],
        "metrics": _buffer["metrics"],
        "workouts": _buffer["workouts"],
    }
    _buffer["metrics"] = {}
    _buffer["workouts"] = []
    _buffer["period"] = {}
    _buffer["received_at"] = 0.0
    _buffer["pending_task"] = None
    return result


async def _delayed_report() -> None:
    """Wait for the merge window, then generate and send the report."""
    await asyncio.sleep(MERGE_WINDOW)

    async with _buffer_lock:
        merged = _flush_buffer()

    n_metrics = len(merged.get("metrics", {}))
    n_workouts = len(merged.get("workouts", []))
    logger.info(
        "Merge window elapsed — generating report (%d metrics, %d workouts)",
        n_metrics,
        n_workouts,
    )

    try:
        report = await generate_report(merged)
        logger.info("Report generated (%d chars)", len(report))
    except Exception as e:
        logger.error("Failed to generate report: %s", e)
        return

    try:
        sent = await send_report(report)
        if not sent:
            logger.warning("Telegram send returned False")
    except Exception as e:
        logger.error("Failed to send Telegram message: %s", e)


@app.get("/health")
async def health_check():
    return {"status": "ok"}


@app.post("/webhook/health")
async def receive_health_data(
    request: Request,
    authorization: str = Header(...),
):
    # Validate bearer token
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization header")

    token = authorization.removeprefix("Bearer ").strip()
    if token != WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Invalid token")

    # Parse incoming JSON
    try:
        payload = await request.json()
    except Exception:
        logger.error("Failed to parse JSON payload")
        raise HTTPException(status_code=400, detail="Invalid JSON")

    logger.info("Webhook received — parsing health data")

    try:
        report_data = parse_health_data(payload)
    except Exception as e:
        logger.error("Failed to parse health data: %s", e)
        raise HTTPException(status_code=422, detail=f"Parse error: {e}")

    n_metrics = len(report_data.get("metrics", {}))
    n_workouts = len(report_data.get("workouts", []))
    logger.info("Parsed OK — %d metrics, %d workouts", n_metrics, n_workouts)

    async with _buffer_lock:
        _merge_into_buffer(report_data)
        _buffer["received_at"] = time.time()

        # Cancel existing timer and restart it
        if _buffer["pending_task"] and not _buffer["pending_task"].done():
            _buffer["pending_task"].cancel()
            logger.info("Timer reset — waiting %ds for more data", MERGE_WINDOW)
        else:
            logger.info("First payload buffered — waiting %ds for more data", MERGE_WINDOW)

        _buffer["pending_task"] = asyncio.create_task(_delayed_report())

    return {
        "status": "buffered",
        "metrics_so_far": len(_buffer["metrics"]),
        "workouts_so_far": len(_buffer["workouts"]),
    }
