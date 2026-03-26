"""Loop action node."""

NODE_TYPE = "action.loop"
LABEL = "Loop"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Prepare items for looping."""
    field = config.get('field', '')
    max_items = int(config.get('max_items', 100))
    items = inp.get(field, inp) if field and isinstance(inp, dict) else inp

    if not isinstance(items, list):
        items = [items]

    items = items[:max_items]

    return {'items': items, 'count': len(items), '__loop__': True}

