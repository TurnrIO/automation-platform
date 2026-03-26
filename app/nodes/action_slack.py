"""Slack message action node."""
import os
import json
from app.nodes._utils import _render

NODE_TYPE = "action.slack"
LABEL = "Slack"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Send message to Slack webhook."""
    import httpx

    webhook_url = _render(config.get('webhook_url', ''), context, creds)
    message = _render(config.get('message', ''), context, creds)
    channel = _render(config.get('channel', ''), context, creds)

    # Structured credential shortcut (Slack or generic Webhook type)
    cred_name = _render(config.get('credential', ''), context, creds)
    if cred_name and creds and not webhook_url:
        raw = creds.get(cred_name)
        if raw:
            try:
                c = json.loads(raw)
                webhook_url = c.get('webhook_url', '') or c.get('url', '')
            except (json.JSONDecodeError, AttributeError):
                webhook_url = raw  # fallback: raw value is the URL

    if not webhook_url:
        raise ValueError("Slack: no webhook_url configured")
    if not message:
        raise ValueError("Slack: no message configured")

    body = {'text': message}
    if channel:
        body['channel'] = channel

    r = httpx.post(webhook_url, json=body, timeout=10)
    r.raise_for_status()

    return {'sent': True, 'message': message}

