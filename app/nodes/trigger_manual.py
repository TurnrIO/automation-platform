"""Manual trigger node."""

NODE_TYPE = "trigger.manual"
LABEL = "Manual Trigger"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Pass through input as-is."""
    return inp

