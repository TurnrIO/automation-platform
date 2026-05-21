"""Webhook trigger node.

The actual HTTP handling (HMAC verification, rate-limiting, CORS) is done
by app/routers/webhooks.py.  This node exists so it appears in the canvas
and its config (secret, allowed_origins) is stored in graph_data where the
webhook router reads it at dispatch time.
"""
import logging

logger = logging.getLogger(__name__)

NODE_TYPE = "trigger.webhook"
LABEL = "Webhook Trigger"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Pass through input as-is (the payload arrives via the webhook endpoint)."""
    logger.info("trigger.webhook pass-through")
    logger.info("trigger.webhook: completed, forwarded %s bytes", len(str(inp)))
    return inp
