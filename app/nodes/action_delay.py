"""Delay action node.
"""
import time
import logging

logger = logging.getLogger(__name__)
from app.nodes._utils import _render

NODE_TYPE = "action.delay"
LABEL = "Delay"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Sleep for specified seconds."""
    logger.info("Delay: node invoked")
    try:
        secs = float(_render(config.get('seconds', '1'), context, creds) or 1)
    except (ValueError, TypeError):
        secs = 1.0
    time.sleep(max(0.0, secs))
    logger.info("Delay: slept %s seconds", secs)
    return {'slept': secs}

