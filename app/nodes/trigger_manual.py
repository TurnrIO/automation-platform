"""Manual trigger node."""
import logging

logger = logging.getLogger(__name__)

NODE_TYPE = "trigger.manual"
LABEL = "Manual Trigger"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Pass through input as-is."""
    logger.info("trigger.manual pass-through")
    return inp
