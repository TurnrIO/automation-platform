"""Run script action node."""
import os
import time
import json
from app.nodes._utils import _render

NODE_TYPE = "action.run_script"
LABEL = "Run Script"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Execute arbitrary Python code in a sandbox."""
    script = _render(config.get('script', ''), context, creds)
    ns = {
        'input': inp,
        'context': context,
        'result': None,
        'log': logger,
        'json': json,
        'os': os,
        'time': time
    }
    exec(script, ns)
    return ns.get('result', inp)

