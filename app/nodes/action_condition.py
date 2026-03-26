"""Condition / branching action node."""
from app.nodes._utils import _render

NODE_TYPE = "action.condition"
LABEL = "Condition"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Evaluate a boolean expression and return {result, input}."""
    expr = _render(config.get('expression', 'True'), context, creds)
    safe_builtins = {'len': len, 'str': str, 'int': int, 'float': float, 'bool': bool, 'list': list, 'dict': dict, 'tuple': tuple}
    result = eval(expr, {'__builtins__': safe_builtins}, {'input': inp, 'context': context})
    return {'result': bool(result), 'input': inp}

