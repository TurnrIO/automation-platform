"""Transform / evaluate expression action node."""
import logging
from app.nodes._utils import _render, _safe_eval

logger = logging.getLogger(__name__)
NODE_TYPE = "action.transform"
LABEL = "Transform"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Evaluate a Python expression and return result."""
    logger.info("Transform: evaluating expression")
    expr = _render(config.get('expression') or 'input', context, creds)
    try:
        result = _safe_eval(expr, {'input': inp, 'context': context, 'json': __import__('json')})
    except ValueError as exc:
        return {
            '__error': f"Unsafe or invalid expression: {exc}"
        }
    except (SyntaxError, NameError, TypeError, ZeroDivisionError, KeyError, AttributeError) as e:
        e_args = getattr(e, 'args', ())
        if isinstance(e, KeyError) and e_args and isinstance(e_args[0], slice):
            s = e_args[0]
            notation = f"[:{s.stop}]" if s.start is None and s.step is None else repr(s)
            keys = list(inp.keys()) if isinstance(inp, dict) else None
            hint = f" Available keys: {keys}. Try input['key']{notation} instead." if keys else ""
            return {
                '__error': f"Cannot slice a dict with {notation} — 'input' is a dict, not a list.{hint}"
            }
        raise RuntimeError(f"Transform expression error: {type(e).__name__}: {e}")
    logger.info("Transform: evaluated expression result_type=%s", type(result).__name__)
    return result
