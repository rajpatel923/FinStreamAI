"""Notification service: email (aiosmtplib/SendGrid), SMS (Twilio), Slack/webhook (httpx)."""
from __future__ import annotations

import structlog
import httpx

logger = structlog.get_logger(__name__)


async def send_email(
    to_email: str,
    subject: str,
    body: str,
    config: dict,
) -> bool:
    """Send email via SendGrid HTTP API."""
    from src.core.config import settings
    if not settings.SENDGRID_API_KEY:
        logger.warning("SendGrid not configured — skipping email", to=to_email)
        return False
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                "https://api.sendgrid.com/v3/mail/send",
                headers={
                    "Authorization": f"Bearer {settings.SENDGRID_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "personalizations": [{"to": [{"email": to_email}]}],
                    "from": {"email": settings.SENDGRID_FROM_EMAIL},
                    "subject": subject,
                    "content": [{"type": "text/plain", "value": body}],
                },
            )
            resp.raise_for_status()
            return True
    except Exception as exc:
        logger.error("Email send failed", to=to_email, error=str(exc))
        return False


async def send_sms(to_number: str, message: str) -> bool:
    """Send SMS via Twilio REST API."""
    from src.core.config import settings
    if not settings.TWILIO_ACCOUNT_SID:
        logger.warning("Twilio not configured — skipping SMS")
        return False
    try:
        async with httpx.AsyncClient(
            auth=(settings.TWILIO_ACCOUNT_SID, settings.TWILIO_AUTH_TOKEN),
            timeout=10,
        ) as client:
            resp = await client.post(
                f"https://api.twilio.com/2010-04-01/Accounts/{settings.TWILIO_ACCOUNT_SID}/Messages.json",
                data={"From": settings.TWILIO_FROM_NUMBER, "To": to_number, "Body": message},
            )
            resp.raise_for_status()
            return True
    except Exception as exc:
        logger.error("SMS send failed", to=to_number, error=str(exc))
        return False


async def send_slack(webhook_url: str, message: str) -> bool:
    """POST JSON payload to a Slack incoming webhook URL."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(webhook_url, json={"text": message})
            resp.raise_for_status()
            return True
    except Exception as exc:
        logger.error("Slack notification failed", error=str(exc))
        return False


async def send_webhook(webhook_url: str, payload: dict) -> bool:
    """POST JSON payload to an arbitrary webhook URL."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(webhook_url, json=payload)
            resp.raise_for_status()
            return True
    except Exception as exc:
        logger.error("Webhook delivery failed", url=webhook_url, error=str(exc))
        return False


async def dispatch_alert_notification(
    channels: list[str],
    config: dict,
    alert_name: str,
    message: str,
    user_email: str | None = None,
) -> None:
    """Fan-out notification to all configured channels."""
    for channel in channels:
        if channel == "email" and user_email:
            await send_email(user_email, f"Alert: {alert_name}", message, config)
        elif channel == "sms":
            to_number = config.get("sms_number")
            if to_number:
                await send_sms(to_number, f"[{alert_name}] {message}")
        elif channel == "slack":
            slack_url = config.get("slack_url")
            if slack_url:
                await send_slack(slack_url, f"*{alert_name}*: {message}")
        elif channel == "webhook":
            webhook_url = config.get("webhook_url")
            if webhook_url:
                await send_webhook(webhook_url, {"alert": alert_name, "message": message})
