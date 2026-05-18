"""Log action node."""
import logging
from app.nodes._utils import _render

logger = logging.getLogger(__name__)

NODE_TYPE = "action.log"
LABEL = "Log"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Log a message to the execution logs."""
    logger.info("action_log: starting")
    msg = _render(config.get('message', ''), context, creds)
    logger.info("LOG: %s", msg)
    return {'logged': msg}
