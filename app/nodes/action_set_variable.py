"""Set variable action node."""
import logging
from app.nodes._utils import _render

logger = logging.getLogger(__name__)

NODE_TYPE = "action.set_variable"
LABEL = "Set Variable"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Set a variable in the execution context."""
    key = config.get('key', 'var')
    val = _render(config.get('value', ''), context, creds)
    context[key] = val
    logger.info("Set variable: %s", key)
    return {key: val}
