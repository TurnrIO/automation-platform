"""Cron schedule trigger node."""

NODE_TYPE = "trigger.cron"
LABEL = "Cron Schedule"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Pass through input as-is."""
    return inp

