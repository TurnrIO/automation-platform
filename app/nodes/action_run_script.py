"""Run script action node.

DANGER: this node executes arbitrary Python via exec() with os, time, and
json available in the namespace.  It is disabled by default.

Set ENABLE_RUN_SCRIPT=true in your environment to allow execution.
Every execution is written to the 'audit' logger at WARNING level so it is
always visible in Docker / system logs regardless of the application log level.
"""

import logging
import os
import time
import json
import hashlib
import tempfile
import multiprocessing

from app.nodes._utils import _render

logger = logging.getLogger(__name__)

NODE_TYPE = "action.run_script"
LABEL = "Run Script"

_audit = logging.getLogger("audit")

# Maximum time a user script can run before being terminated (seconds).
_SCRIPT_TIMEOUT_SEC = 30


def _run_script_worker(script: str, inp, context_json: str, ns_keys: list, result_path: str):
    """Worker function run in a subprocess. Writes result to result_path."""
    try:
        ns = {
            'input': json.loads(context_json)[0],
            'context': json.loads(context_json)[1],
            'log': logging.getLogger("script"),
            'json': json,
            'os': os,
            'time': time,
        }
        exec(script, ns)  # noqa: S102
        # Detect whether 'result' was assigned in the script (vs. never touched).
        # If the script never assigns 'result', 'result' is not in ns → no result.
        # If the script assigns 'result = None', 'result' IS in ns → return None.
        has_result = 'result' in ns
        result_val = ns['result'] if has_result else None
        with open(result_path, 'w') as f:
            json.dump({'has_result': has_result, 'result': result_val}, f)
    except (OSError, json.JSONDecodeError):
        # Script errors → result file not written; parent sees non-zero exit and raises.
        pass


def run(config, inp, context, logger, creds=None, **kwargs):
    """Execute arbitrary Python — requires ENABLE_RUN_SCRIPT=true."""
    logger.info("action.run_script: starting")
    # ── feature flag ──────────────────────────────────────────────────────
    enabled = os.environ.get("ENABLE_RUN_SCRIPT", "false").strip().lower() == "true"
    if not enabled:
        logger.info("action.run_script: disabled, rejecting")
        raise RuntimeError(
            "action.run_script is disabled. "
            "Set ENABLE_RUN_SCRIPT=true in your .env to enable it. "
            "WARNING: this node executes arbitrary Python code — only enable "
            "it if you fully trust every user who can edit flows."
        )

    script = _render(config.get('script', ''), context, creds)

    # ── audit log (always, even before exec) ─────────────────────────────
    script_hash    = hashlib.sha256(script.encode()).hexdigest()[:12]
    script_preview = script[:120].replace('\n', ' ')
    logger.info("action.run_script: script_hash=%s", script_hash)
    _audit.warning(
        "AUDIT run_script | hash=%s | preview=%s",
        script_hash,
        script_preview,
    )

    # ── run with timeout via subprocess ──────────────────────────────────
    context_json = json.dumps([inp, context])
    ns_keys = ['input', 'context', 'result', 'log', 'json', 'os', 'time']

    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tf:
        result_path = tf.name

    try:
        p = multiprocessing.Process(
            target=_run_script_worker,
            args=(script, inp, context_json, ns_keys, result_path),
            daemon=True,
        )
        p.start()
        p.join(timeout=_SCRIPT_TIMEOUT_SEC)

        if p.is_alive():
            p.terminate()
            p.join(timeout=5)
            raise RuntimeError(
                f"action.run_script: script exceeded {_SCRIPT_TIMEOUT_SEC}s timeout "
                f"and was terminated. Reduce computation or I/O in the script."
            )

        # Read the result file written by the worker.
        try:
            with open(result_path) as f:
                outcome = json.load(f)
            has_result = outcome['has_result']
            result_val = outcome['result']
        except (OSError, json.JSONDecodeError):
            # result_path not written or unreadable → script raised an exception before completion.
            raise RuntimeError(
                "action.run_script: script raised an exception. "
                "Check the worker logs for the traceback."
            )

        _audit.warning(
            "AUDIT run_script | hash=%s | completed_ok",
            script_hash,
        )
        logger.info("action.run_script: completed_ok hash=%s", script_hash)
        # has_result False means 'result' was never assigned → return input unchanged.
        return inp if not has_result else result_val

    finally:
        try:
            os.unlink(result_path)
        except OSError:
            pass

