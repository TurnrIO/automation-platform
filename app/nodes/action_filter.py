"""Filter / map action node."""
import logging
import re as _re
from app.nodes._utils import _render, _safe_eval

logger = logging.getLogger(__name__)

NODE_TYPE = "action.filter"
LABEL = "Filter"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Filter items in a list based on an expression."""
    field = config.get('field', '')
    expr = _render(config.get('expression', 'True'), context, creds)
    items = inp.get(field, inp) if field and isinstance(inp, dict) else inp

    if not isinstance(items, list):
        items = [items]

    local_vars = {'item': None, 'context': context, 'input': inp, 're': _re}
    try:
        kept = [item for item in items
                if _safe_eval(expr, {**local_vars, 'item': item})]
    except ValueError as e:
        logger.warning("Filter expression evaluation failed: %s — returning all items", e)
        kept = items

    logger.info("Filter: kept %s/%s items", len(kept), len(items))
    return {'items': kept, 'count': len(kept), 'total': len(items)}

