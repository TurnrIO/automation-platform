"""OpenAI Assistants API node."""
import logging
import json, os
from json import JSONDecodeError
import time
import urllib.request
import urllib.error
from app.nodes._utils import _render

logger = logging.getLogger(__name__)
NODE_TYPE = "action.openai_assistant"
LABEL     = "OpenAI Assistant"

_BASE = "https://api.openai.com/v1"


def _req(method, path, api_key, body=None):
    url  = _BASE + path
    data = json.dumps(body).encode() if body is not None else None
    req  = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {api_key}")
    req.add_header("Content-Type", "application/json")
    req.add_header("OpenAI-Beta", "assistants=v2")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        body_txt = e.read().decode()
        try:   detail = json.loads(body_txt).get("error", {}).get("message", body_txt)
        except JSONDecodeError: detail = body_txt
        raise RuntimeError(f"OpenAI {e.code}: {detail}")
    except urllib.error.URLError as e:
        raise RuntimeError(f"OpenAI connection error: {e.reason}")
    except OSError as e:
        raise RuntimeError(f"OpenAI network error: {e}")


def run(config, inp, context, logger, creds=None, **kwargs):
    cred_name = config.get("credential", "")
    api_key = ""
    if cred_name and creds:
        raw = creds.get(cred_name, {})
        if isinstance(raw, str):
            try:   raw = json.loads(raw)
            except JSONDecodeError: raw = {}
        api_key = raw.get("api_key", raw.get("token", ""))
    if not api_key:
        api_key = _render(config.get("api_key", ""), context, creds)
    if not api_key:
        api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        raise ValueError("OpenAI Assistant: api_key is required")

    op           = _render(config.get("operation", "run_thread"), context, creds)
    assistant_id = _render(config.get("assistant_id", ""), context, creds)

    # ── create thread ─────────────────────────────────────────────────────────
    if op == "create_thread":
        logger.info("OpenAI Assistant: create_thread")
        thread = _req("POST", "/threads", api_key, {})
        return {"thread_id": thread["id"], "thread": thread}

    # ── add message ───────────────────────────────────────────────────────────
    elif op == "add_message":
        thread_id = _render(config.get("thread_id", ""), context, creds)
        content   = _render(config.get("content", ""), context, creds)
        role      = _render(config.get("role", "user"), context, creds)
        logger.info("OpenAI Assistant: add_message to thread=%s", thread_id)
        msg = _req("POST", f"/threads/{thread_id}/messages", api_key,
                   {"role": role, "content": content})
        return {"message_id": msg["id"], "thread_id": thread_id}

    # ── run thread (create run + poll until done) ─────────────────────────────
    elif op == "run_thread":
        thread_id    = _render(config.get("thread_id", ""), context, creds)
        instructions = _render(config.get("instructions", ""), context, creds)
        try: timeout = int(_render(config.get("timeout", "120"), context, creds))
        except (ValueError, TypeError): timeout = 120
        body = {"assistant_id": assistant_id}
        if instructions:
            body["instructions"] = instructions
        logger.info("OpenAI Assistant: run_thread thread=%s assistant=%s", thread_id, assistant_id)
        run_obj = _req("POST", f"/threads/{thread_id}/runs", api_key, body)
        run_id  = run_obj["id"]

        deadline = time.time() + timeout
        while time.time() < deadline:
            run_obj = _req("GET", f"/threads/{thread_id}/runs/{run_id}", api_key)
            status  = run_obj.get("status")
            if status in ("completed", "failed", "cancelled", "expired"):
                break
            time.sleep(0.1)  # interruptible — smaller step than 0.5s to reduce shutdown latency

        if run_obj.get("status") != "completed":
            raise RuntimeError(f"OpenAI run {run_id} ended with status: {run_obj.get('status')}")

        msgs = _req("GET", f"/threads/{thread_id}/messages?limit=10&order=desc", api_key)
        messages = msgs.get("data", [])
        # Latest assistant message text
        reply = ""
        for m in messages:
            if m.get("role") == "assistant":
                parts = m.get("content", [])
                for p in parts:
                    if p.get("type") == "text":
                        reply = p["text"]["value"]
                        break
                if reply:
                    break
        return {"reply": reply, "run_id": run_id, "status": run_obj.get("status"),
                "messages": messages, "thread_id": thread_id}

    # ── get run status ─────────────────────────────────────────────────────────
    elif op == "get_run_status":
        thread_id = _render(config.get("thread_id", ""), context, creds)
        run_id    = _render(config.get("run_id", ""), context, creds)
        logger.info("OpenAI Assistant: get_run_status thread=%s run=%s", thread_id, run_id)
        run_obj   = _req("GET", f"/threads/{thread_id}/runs/{run_id}", api_key)
        return {"run_id": run_id, "status": run_obj.get("status"), "run": run_obj}

    # ── list messages ──────────────────────────────────────────────────────────
    elif op == "list_messages":
        thread_id = _render(config.get("thread_id", ""), context, creds)
        try: limit = int(_render(config.get("limit", "20"), context, creds))
        except (ValueError, TypeError): limit = 20
        logger.info("OpenAI Assistant: list_messages thread=%s limit=%s", thread_id, limit)
        msgs = _req("GET", f"/threads/{thread_id}/messages?limit={min(limit,100)}&order=asc", api_key)
        messages = msgs.get("data", [])
        # Flatten text content
        flat = []
        for m in messages:
            text = " ".join(
                p["text"]["value"] for p in m.get("content", []) if p.get("type") == "text"
            )
            flat.append({"role": m["role"], "text": text, "id": m["id"]})
        return {"messages": flat, "count": len(flat), "thread_id": thread_id}

    else:
        raise ValueError(f"OpenAI Assistant: unknown operation {op!r}")
