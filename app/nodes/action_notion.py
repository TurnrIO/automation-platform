"""Notion API action node."""
import json
from app.nodes._utils import _render

NODE_TYPE = "action.notion"
LABEL = "Notion"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Interact with Notion API."""
    import httpx

    token = _render(config.get('token', ''), context, creds)
    cred_name = _render(config.get('credential', ''), context, creds)

    if cred_name and creds and not token:
        raw = creds.get(cred_name)
        if raw:
            try:
                token = json.loads(raw).get('token', raw)
            except:
                token = raw

    if not token:
        raise ValueError("Notion: no integration token configured")

    action = config.get('action', 'query_database')
    database_id = _render(config.get('database_id', ''), context, creds)
    page_id = _render(config.get('page_id', ''), context, creds)
    query = _render(config.get('query', ''), context, creds)

    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
        'Notion-Version': '2022-06-28',
    }
    base = 'https://api.notion.com/v1'

    def notion(method, url, **kw):
        r = httpx.request(method, url, headers=headers, timeout=30, **kw)
        r.raise_for_status()
        return r.json()

    if action == 'query_database':
        if not database_id:
            raise ValueError("Notion query_database: database_id required")

        body = {}
        if config.get('filter_json'):
            try:
                body['filter'] = json.loads(_render(config['filter_json'], context, creds))
            except:
                pass
        if config.get('sorts_json'):
            try:
                body['sorts'] = json.loads(_render(config['sorts_json'], context, creds))
            except:
                pass
        body['page_size'] = int(config.get('page_size', 50))

        data = notion('POST', f'{base}/databases/{database_id}/query', json=body)

        # Flatten page properties for easy downstream use
        pages = []
        for p in data.get('results', []):
            props = {}
            for k, v in p.get('properties', {}).items():
                t = v.get('type', '')
                if t == 'title':
                    props[k] = ''.join(x['plain_text'] for x in v.get('title', []))
                elif t == 'rich_text':
                    props[k] = ''.join(x['plain_text'] for x in v.get('rich_text', []))
                elif t == 'number':
                    props[k] = v.get('number')
                elif t == 'select':
                    props[k] = (v.get('select') or {}).get('name')
                elif t == 'multi_select':
                    props[k] = [x['name'] for x in v.get('multi_select', [])]
                elif t == 'checkbox':
                    props[k] = v.get('checkbox')
                elif t == 'date':
                    props[k] = (v.get('date') or {}).get('start')
                elif t == 'url':
                    props[k] = v.get('url')
                elif t == 'email':
                    props[k] = v.get('email')
                elif t == 'phone_number':
                    props[k] = v.get('phone_number')
                else:
                    props[k] = v
            pages.append({'id': p['id'], 'url': p.get('url'), 'properties': props})

        return {'pages': pages, 'count': len(pages), 'has_more': data.get('has_more', False)}

    elif action == 'get_page':
        if not page_id:
            raise ValueError("Notion get_page: page_id required")
        return notion('GET', f'{base}/pages/{page_id}')

    elif action == 'create_page':
        if not database_id:
            raise ValueError("Notion create_page: database_id required")

        props_raw = _render(config.get('properties_json', '{}'), context, creds)
        try:
            props = json.loads(props_raw)
        except:
            raise ValueError("Notion create_page: properties_json must be valid JSON")

        # Auto-wrap plain string values as title/rich_text
        wrapped = {}
        for k, v in props.items():
            if isinstance(v, str):
                wrapped[k] = {'rich_text': [{'type': 'text', 'text': {'content': v}}]}
            else:
                wrapped[k] = v

        body = {'parent': {'database_id': database_id}, 'properties': wrapped}

        # Title field special-casing
        title_val = _render(config.get('title', ''), context, creds)
        title_key = config.get('title_field', 'Name')
        if title_val:
            body['properties'][title_key] = {'title': [{'type': 'text', 'text': {'content': title_val}}]}

        return notion('POST', f'{base}/pages', json=body)

    elif action == 'update_page':
        if not page_id:
            raise ValueError("Notion update_page: page_id required")

        props_raw = _render(config.get('properties_json', '{}'), context, creds)
        try:
            props = json.loads(props_raw)
        except:
            raise ValueError("Notion update_page: properties_json must be valid JSON")

        return notion('PATCH', f'{base}/pages/{page_id}', json={'properties': props})

    elif action == 'search':
        body = {'page_size': int(config.get('page_size', 20))}
        if query:
            body['query'] = query
        return notion('POST', f'{base}/search', json=body)

    elif action == 'append_blocks':
        if not page_id:
            raise ValueError("Notion append_blocks: page_id required")

        content = _render(config.get('content', ''), context, creds)

        # Treat content as plain paragraph text
        blocks = [{'object': 'block', 'type': 'paragraph',
                   'paragraph': {'rich_text': [{'type': 'text', 'text': {'content': content}}]}}]

        return notion('PATCH', f'{base}/blocks/{page_id}/children', json={'children': blocks})

    else:
        raise ValueError(f"Notion: unknown action '{action}'")

