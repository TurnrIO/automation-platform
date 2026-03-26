"""Webhook trigger node."""

NODE_TYPE = "trigger.webhook"
LABEL = "Webhook Trigger"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Pass through input as-is."""
    return inp

