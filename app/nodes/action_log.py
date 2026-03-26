"""Log action node."""
from app.nodes._utils import _render

NODE_TYPE = "action.log"
LABEL = "Log"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Log a message to the execution logs."""
    msg = _render(config.get('message', ''), context, creds)
    logger(f"LOG: {msg}")
    return {'logged': msg}

