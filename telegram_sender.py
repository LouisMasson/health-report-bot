"""
Send the health report to Telegram, splitting if needed.
"""

import logging
import os

import httpx

logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

MAX_MESSAGE_LENGTH = 4096


def _split_message(text: str) -> list[str]:
    """Split text into chunks of MAX_MESSAGE_LENGTH, breaking at newlines."""
    if len(text) <= MAX_MESSAGE_LENGTH:
        return [text]

    chunks = []
    while text:
        if len(text) <= MAX_MESSAGE_LENGTH:
            chunks.append(text)
            break

        # Find last newline before limit
        cut = text.rfind("\n", 0, MAX_MESSAGE_LENGTH)
        if cut == -1:
            cut = MAX_MESSAGE_LENGTH

        chunks.append(text[:cut])
        text = text[cut:].lstrip("\n")

    return chunks


async def send_report(report: str) -> bool:
    """Send report to Telegram. Returns True on success."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set")
        return False

    chunks = _split_message(report)
    logger.info("Sending report to Telegram (%d message(s))", len(chunks))

    async with httpx.AsyncClient(timeout=30) as client:
        for i, chunk in enumerate(chunks):
            resp = await client.post(
                f"{TELEGRAM_API}/sendMessage",
                json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": chunk,
                    "parse_mode": "Markdown",
                },
            )

            if resp.status_code != 200:
                logger.error(
                    "Telegram send failed (chunk %d/%d): %s",
                    i + 1,
                    len(chunks),
                    resp.text,
                )
                return False

    logger.info("Report sent to Telegram successfully")
    return True
