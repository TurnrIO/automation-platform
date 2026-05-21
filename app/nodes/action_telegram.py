"""Telegram message action node."""
import logging
import os
from json import JSONDecodeError

from app.nodes._utils import _render

logger = logging.getLogger(__name__)

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

    logger.info("Telegram: op=send")
    try:
        r = httpx.post(f"https://api.telegram.org/bot{token}/sendMessage",
                       json={'chat_id': chat, 'text': text}, timeout=10)
        r.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("Telegram: HTTP error — %s", exc)
        return {"__error": f"Telegram HTTP error: {exc}", "sent": False, "chat_id": chat}
    except OSError as exc:
        logger.warning("Telegram: connection error — %s", exc)
        raise OSError(f"Telegram connection error: {exc}") from exc
    except (JSONDecodeError, KeyError) as exc:
        logger.warning("Telegram: unexpected response shape — %s", exc)
        raise RuntimeError(f"Telegram response error: {exc}") from exc

    logger.info('Telegram: completed')
    return {'sent': True, 'chat_id': chat}
