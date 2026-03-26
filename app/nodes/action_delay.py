"""Delay action node."""
import time

NODE_TYPE = "action.delay"
LABEL = "Delay"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Sleep for specified seconds."""
    secs = float(config.get('seconds', 1))
    time.sleep(secs)
    return {'slept': secs}

