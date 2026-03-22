import logging
import os

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

app = FastAPI(title="Health Report Bot")


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

    # Extract & aggregate metrics
    try:
        report_data = parse_health_data(payload)
    except Exception as e:
        logger.error("Failed to parse health data: %s", e)
        raise HTTPException(status_code=422, detail=f"Parse error: {e}")

    logger.info(
        "Parsed OK — %d metrics, %d workouts",
        len(report_data.get("metrics", {})),
        len(report_data.get("workouts", [])),
    )

    # Generate AI report via OpenRouter
    try:
        report = await generate_report(report_data)
    except Exception as e:
        logger.error("Failed to generate report: %s", e)
        raise HTTPException(status_code=502, detail=f"AI analysis failed: {e}")

    # Send to Telegram
    try:
        sent = await send_report(report)
        if not sent:
            logger.warning("Telegram send returned False")
    except Exception as e:
        logger.error("Failed to send Telegram message: %s", e)

    return {"status": "ok", "report_length": len(report)}
