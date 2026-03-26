"""LLM call action node."""
import os
from app.nodes._utils import _render

NODE_TYPE = "action.llm_call"
LABEL = "LLM Call"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Call OpenAI-compatible LLM API."""
    import httpx

    model = _render(config.get('model', 'gpt-4o-mini'), context, creds)
    prompt = _render(config.get('prompt', ''), context, creds)
    system = _render(config.get('system', 'You are a helpful assistant.'), context, creds)
    api_key = _render(config.get('api_key', ''), context, creds) or os.environ.get('OPENAI_API_KEY', '')
    api_base = _render(config.get('api_base', ''), context, creds) or 'https://api.openai.com/v1'

    if not api_key:
        raise ValueError("LLM Call: no api_key configured and OPENAI_API_KEY env not set")

    resp = httpx.post(
        f"{api_base.rstrip('/')}/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": prompt}
            ]
        },
        timeout=60
    )
    resp.raise_for_status()
    data = resp.json()
    reply = data['choices'][0]['message']['content']
    tokens = data.get('usage', {}).get('total_tokens', 0)

    return {'response': reply, 'model': model, 'tokens': tokens}

