"""HTTP request action node."""
import json
from app.nodes._utils import _render

NODE_TYPE = "action.http_request"
LABEL = "HTTP Request"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Execute HTTP request and return response."""
    import httpx

    url = _render(config.get('url', ''), context, creds)
    method = config.get('method', 'GET').upper()
    headers = {}
    body = None

    if config.get('headers_json'):
        try:
            headers = json.loads(_render(config['headers_json'], context, creds))
        except:
            pass

    if config.get('body_json'):
        try:
            body = json.loads(_render(config['body_json'], context, creds))
        except:
            body = _render(config['body_json'], context, creds)

    r = httpx.request(method, url, headers=headers,
                      json=body if isinstance(body, dict) else None,
                      content=body if isinstance(body, str) else None,
                      timeout=30)
    r.raise_for_status()

    try:
        rbody = r.json()
    except:
        rbody = r.text

    return {'status': r.status_code, 'body': rbody, 'headers': dict(r.headers)}

