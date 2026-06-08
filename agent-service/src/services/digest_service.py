"""Daily digest service — sends summary emails to opted-in users."""
from __future__ import annotations

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.preferences import UserPreference
from src.models.user import User

logger = structlog.get_logger(__name__)


async def send_daily_digest(db: AsyncSession, settings) -> int:
    """Send daily digest to users with digest_frequency='daily'. Returns count sent."""
    result = await db.execute(
        select(UserPreference).where(UserPreference.digest_frequency == "daily")
    )
    prefs = result.scalars().all()
    if not prefs:
        return 0

    user_ids = [p.user_id for p in prefs]
    users_result = await db.execute(
        select(User).where(User.id.in_(user_ids), User.is_active == True)  # noqa: E712
    )
    users = users_result.scalars().all()

    sent = 0
    for user in users:
        try:
            await _send_email(user.email, "FinStreamAI Daily Digest", _build_digest_body(), settings)
            sent += 1
        except Exception as exc:
            logger.warning("Digest send failed", user_id=str(user.id), error=str(exc))

    logger.info("Daily digest sent", count=sent)
    return sent


def _build_digest_body() -> str:
    return (
        "Good morning! Here is your FinStreamAI daily market digest.\n\n"
        "Visit your dashboard for full portfolio analysis and signals."
    )


async def _send_email(to: str, subject: str, body: str, settings) -> None:
    """Send email via SendGrid API."""
    import httpx

    if not settings.SENDGRID_API_KEY:
        logger.debug("SENDGRID_API_KEY not set — skipping email", to=to)
        return

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://api.sendgrid.com/v3/mail/send",
            headers={
                "Authorization": f"Bearer {settings.SENDGRID_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "personalizations": [{"to": [{"email": to}]}],
                "from": {"email": settings.SENDGRID_FROM_EMAIL},
                "subject": subject,
                "content": [{"type": "text/plain", "value": body}],
            },
            timeout=10.0,
        )
        if resp.status_code not in (200, 202):
            raise RuntimeError(f"SendGrid returned {resp.status_code}")
