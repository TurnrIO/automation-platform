"""Transform / evaluate expression action node."""
import json
from app.nodes._utils import _render

NODE_TYPE = "action.transform"
LABEL = "Transform"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Evaluate a Python expression and return result."""
    expr = _render(config.get('expression', 'input'), context, creds)
    safe_builtins = {'len': len, 'str': str, 'int': int, 'float': float, 'bool': bool, 'list': list, 'dict': dict, 'tuple': tuple}
    result = eval(expr, {'__builtins__': safe_builtins}, {'input': inp, 'context': context, 'json': json})
    return result

