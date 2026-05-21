"""Aggregate action node — collect loop-body results into one structure.

Place this node after a Loop body to gather per-item outputs back into
a single list, dict, or concatenated string.

The node reads previously stored per-item results from the run context.
Each iteration of a loop stores its output under the key
``__loop_items__`` (a list appended to by the executor).  If that key
is not present, the node collects all upstream node outputs instead.

Output (mode=list):    { items: [...], count: N }
Output (mode=dict):    { result: {...}, count: N }  (dicts merged, last wins)
Output (mode=concat):  { result: "...", count: N }
"""

import logging

logger = logging.getLogger(__name__)

NODE_TYPE = "action.aggregate"
LABEL = "Aggregate"


def run(config, inp, context, logger, creds=None, **kwargs):
    logger.info("Aggregate: starting")
    mode       = config.get("mode", "list")
    field      = config.get("field", "")          # optional: extract sub-field from each item
    separator  = config.get("separator", "\n")    # for concat mode

    # Prefer executor-supplied loop items, fall back to collecting context values
    raw_items = context.get("__loop_items__", None)

    if raw_items is None:
        # Fallback: collect all upstream node outputs from context
        upstream_ids = kwargs.get("upstream_ids", [])
        raw_items = [context[uid] for uid in upstream_ids if context.get(uid) is not None]

    if not isinstance(raw_items, list):
        raw_items = [raw_items]

    # Optionally extract a sub-field from each item
    items = []
    for item in raw_items:
        if field and isinstance(item, dict):
            items.append(item.get(field, item))
        else:
            items.append(item)

    count = len(items)

    if mode == "dict":
        result = {}
        for item in items:
            if isinstance(item, dict):
                result.update(item)
        logger.info("Aggregate (dict): merged %s items", count)
        return {"result": result, "count": count}

    elif mode == "concat":
        parts = [str(item) for item in items]
        result = separator.join(parts)
        logger.info("Aggregate (concat): joined %s items", count)
        return {"result": result, "count": count}

    else:  # list (default)
        logger.info("Aggregate (list): collected %s items", count)
        return {"items": items, "count": count}
