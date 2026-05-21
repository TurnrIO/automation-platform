"""Loop action node.
"""
import logging
from app.nodes._utils import _render

logger = logging.getLogger(__name__)

NODE_TYPE = "action.loop"
LABEL = "Loop"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Prepare items for looping."""
    logger.info("[action.loop] Starting loop (max_items=%s)", config.get('max_items', '100'))
    field = config.get('field', '')
    try:
        max_items = int(_render(config.get('max_items', '100'), context, creds) or 100)
    except (ValueError, TypeError):
        max_items = 100
    items = inp.get(field, inp) if field and isinstance(inp, dict) else inp

    if not isinstance(items, list):
        items = [items]

    items = items[:max_items]
    logger.info("Loop: prepared %s items (max=%s)", len(items), max_items)
    return {'items': items, 'count': len(items), '__loop__': True}
