"""Telegram message action node."""
import os
from app.nodes._utils import _render

NODE_TYPE = "action.telegram"
LABEL = "Telegram"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Send message via Telegram bot."""
    import httpx

    text = _render(config.get('text', ''), context, creds)
    token = _render(config.get('bot_token', ''), context, creds) or os.environ.get('TELEGRAM_TOKEN', '')
    chat = _render(config.get('chat_id', ''), context, creds) or os.environ.get('TELEGRAM_CHAT_ID', '')

    if not token or not chat:
        raise ValueError("Telegram: missing bot_token or chat_id")

    r = httpx.post(f"https://api.telegram.org/bot{token}/sendMessage",
                   json={'chat_id': chat, 'text': text}, timeout=10)
    r.raise_for_status()

    return {'sent': True, 'chat_id': chat}

