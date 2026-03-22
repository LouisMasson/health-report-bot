"""
Generate a weekly health report using OpenRouter (Claude).
"""

import json
import logging
import os

import httpx

logger = logging.getLogger(__name__)

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "anthropic/claude-sonnet-4-5")
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

SYSTEM_PROMPT = """\
Tu es un coach santé et performance. Tu reçois les données de santé hebdomadaires \
d'un utilisateur (Apple Watch + Withings) et tu génères un rapport complet en français.

Format STRICT à respecter (Telegram Markdown) :

📊 *Rapport Santé — Semaine du {period}*

🔑 *Résumé exécutif*
[2-3 phrases clés sur la semaine]

👣 *Activité* | Moy: X pas/j | Total: X
[analyse courte + tendance]

😴 *Sommeil* | Moy: Xh | Deep: Xh | REM: Xh
[analyse courte + tendance]

❤️ *Cardio* | RHR: X bpm | HRV: X ms | VO2: X
[analyse courte + tendance]

🏋️ *Entraînements* (X sessions)
[liste des séances avec durée, calories, FC]

⚖️ *Composition corporelle*
[poids, body fat si disponible]

⚠️ *Points d'attention*
[alertes ou métriques à surveiller]

✅ *Recommandations semaine prochaine*
1. ...
2. ...
3. ...

Règles :
- Utilise uniquement les données fournies, n'invente rien
- Si une métrique est absente, ne la mentionne pas
- Sois concis mais actionnable
- Réponds UNIQUEMENT avec le rapport formaté, rien d'autre\
"""


async def generate_report(parsed_data: dict) -> str:
    """Call OpenRouter to generate the health report."""
    period = parsed_data.get("period", {})
    period_str = f"{period.get('start', '?')} au {period.get('end', '?')}"

    user_message = (
        f"Voici les données de santé de la semaine ({period_str}):\n\n"
        f"```json\n{json.dumps(parsed_data, ensure_ascii=False, indent=2)}\n```"
    )

    payload = {
        "model": OPENROUTER_MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT.format(period=period_str)},
            {"role": "user", "content": user_message},
        ],
        "max_tokens": 2000,
        "temperature": 0.3,
    }

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient(timeout=60) as client:
        logger.info("Calling OpenRouter (%s)...", OPENROUTER_MODEL)
        resp = await client.post(OPENROUTER_URL, json=payload, headers=headers)
        resp.raise_for_status()

    data = resp.json()
    report = data["choices"][0]["message"]["content"]
    logger.info("Report generated (%d chars)", len(report))
    return report
