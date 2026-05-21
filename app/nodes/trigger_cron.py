"""Cron schedule trigger node."""
import logging

logger = logging.getLogger(__name__)

NODE_TYPE = "trigger.cron"
LABEL = "Cron Schedule"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Pass through input as-is."""
    logger.info("trigger.cron: pass-through complete")
    return inp
