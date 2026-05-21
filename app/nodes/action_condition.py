"""Condition / branching action node."""
import logging

logger = logging.getLogger(__name__)
from app.nodes._utils import _render, _safe_eval

NODE_TYPE = "action.condition"
LABEL = "Condition"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Evaluate a boolean expression and return {result, input}."""
    expr = _render(config.get('expression') or 'True', context, creds)
    try:
        result = _safe_eval(expr, {'input': inp, 'context': context})
    except (SyntaxError, NameError, TypeError, ArithmeticError):
        logger.warning("Condition: invalid expression=%s", expr)
        return {'result': False, 'input': inp, '__error': 'invalid_expression'}
    logger.info("Condition: expression=%s -> %s", expr, result)
    return {'result': bool(result), 'input': inp}

