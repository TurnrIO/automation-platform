"""Call subgraph action node."""
import json
from app.nodes._utils import _render

NODE_TYPE = "action.call_graph"
LABEL = "Call Graph"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Call another graph (subgraph/nested automation)."""
    from app.core.db import get_graph as _get_graph
    from app.core.executor import run_graph

    target_id = config.get('graph_id', '')
    if not target_id:
        raise ValueError("Call Graph: no target graph_id configured")

    try:
        target_id = int(target_id)
    except:
        raise ValueError(f"Call Graph: graph_id must be an integer, got '{target_id}'")

    sub_payload = {}
    if config.get('payload'):
        try:
            sub_payload = json.loads(_render(config['payload'], context, creds))
        except:
            sub_payload = {}

    sub_payload = {**inp, **sub_payload} if isinstance(inp, dict) else sub_payload

    g = _get_graph(target_id)
    if not g:
        raise ValueError(f"Call Graph: graph {target_id} not found")

    try:
        gd = json.loads(g.get('graph_json') or '{}')
    except:
        gd = {}

    sub = run_graph(gd, initial_payload=sub_payload, logger=logger, _depth=kwargs.get('_depth', 0) + 1)

    return sub.get('context', {})

