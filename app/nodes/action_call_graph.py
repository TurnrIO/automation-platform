"""Call subgraph action node."""
import json
import logging
from json import JSONDecodeError
from app.nodes._utils import _render

logger = logging.getLogger(__name__)

NODE_TYPE = "action.call_graph"
LABEL = "Call Graph"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Call another graph (subgraph/nested automation)."""
    logger.info("[action.call_graph] Starting Call Graph run")
    from app.core.db import get_graph_by_slug as _get_graph_by_slug
    from app.core.executor import run_graph

    target_id = _render(config.get('target_graph_id', ''), context, creds).strip()
    if not target_id:
        raise ValueError("Call Graph: 'target_graph_id' is required")

    if config.get('payload'):
        try:
            sub_payload = json.loads(_render(config['payload'], context, creds))
        except (JSONDecodeError, ValueError):
            sub_payload = {}

    sub_payload = {**inp, **sub_payload} if isinstance(inp, dict) else sub_payload

    # Resolve caller's workspace_id to scope the subgraph lookup
    caller_workspace_id = kwargs.get('workspace_id')

    # Look up by slug (workspace-scoped) rather than by numeric graph_id alone.
    # This prevents a caller in workspace A from invoking graphs in workspace B.
    g = _get_graph_by_slug(target_id, workspace_id=caller_workspace_id)
    if not g:
        raise ValueError(f"Call Graph: graph '{target_id}' not found in workspace {caller_workspace_id}")

    try:
        gd = json.loads(g.get('graph_json') or '{}')
    except (JSONDecodeError, ValueError):
        gd = {}

    sub = run_graph(gd, initial_payload=sub_payload, logger=logger, _depth=kwargs.get('_depth', 0) + 1,
                    workspace_id=caller_workspace_id)

    logger.info("Call Graph: completed graph_id=%s workspace_id=%s", g['id'], caller_workspace_id)
    return sub.get('context', {})
