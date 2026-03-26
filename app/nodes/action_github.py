"""GitHub API action node."""
import os
import json
from app.nodes._utils import _render

NODE_TYPE = "action.github"
LABEL = "GitHub"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Interact with GitHub REST API."""
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
        raise ValueError("GitHub: no token configured")

    repo = _render(config.get('repo', ''), context, creds)
    action = config.get('action', 'get_repo')
    number = _render(config.get('number', ''), context, creds)
    title = _render(config.get('title', ''), context, creds)
    body = _render(config.get('body', ''), context, creds)
    path = _render(config.get('path', ''), context, creds)
    content = _render(config.get('content', ''), context, creds)
    branch = _render(config.get('branch', 'main'), context, creds)
    state = _render(config.get('state', 'open'), context, creds)
    labels = _render(config.get('labels', ''), context, creds)

    headers = {
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28'
    }
    base = 'https://api.github.com'

    def gh(method, url, **kw):
        r = httpx.request(method, url, headers=headers, timeout=30, **kw)
        r.raise_for_status()
        return r.json() if r.content else {}

    if action == 'get_repo':
        if not repo:
            raise ValueError("GitHub get_repo: repo required")
        return gh('GET', f'{base}/repos/{repo}')

    elif action == 'list_issues':
        if not repo:
            raise ValueError("GitHub list_issues: repo required")
        params = {'state': state or 'open', 'per_page': 25}
        if labels:
            params['labels'] = labels
        return {'issues': gh('GET', f'{base}/repos/{repo}/issues', params=params)}

    elif action == 'get_issue':
        if not repo or not number:
            raise ValueError("GitHub get_issue: repo and number required")
        return gh('GET', f'{base}/repos/{repo}/issues/{number}')

    elif action == 'create_issue':
        if not repo or not title:
            raise ValueError("GitHub create_issue: repo and title required")
        payload = {'title': title, 'body': body}
        if labels:
            payload['labels'] = [l.strip() for l in labels.split(',')]
        return gh('POST', f'{base}/repos/{repo}/issues', json=payload)

    elif action == 'close_issue':
        if not repo or not number:
            raise ValueError("GitHub close_issue: repo and number required")
        return gh('PATCH', f'{base}/repos/{repo}/issues/{number}', json={'state': 'closed'})

    elif action == 'add_comment':
        if not repo or not number or not body:
            raise ValueError("GitHub add_comment: repo, number, and body required")
        return gh('POST', f'{base}/repos/{repo}/issues/{number}/comments', json={'body': body})

    elif action == 'list_commits':
        if not repo:
            raise ValueError("GitHub list_commits: repo required")
        return {'commits': gh('GET', f'{base}/repos/{repo}/commits', params={'sha': branch, 'per_page': 20})}

    elif action == 'list_prs':
        if not repo:
            raise ValueError("GitHub list_prs: repo required")
        return {'pull_requests': gh('GET', f'{base}/repos/{repo}/pulls', params={'state': state or 'open', 'per_page': 20})}

    elif action == 'get_file':
        if not repo or not path:
            raise ValueError("GitHub get_file: repo and path required")
        data = gh('GET', f'{base}/repos/{repo}/contents/{path}', params={'ref': branch})
        import base64 as _b64
        text = _b64.b64decode(data.get('content', '')).decode('utf-8', errors='replace') if data.get('encoding') == 'base64' else ''
        return {**data, 'decoded_content': text}

    elif action == 'create_release':
        if not repo or not title:
            raise ValueError("GitHub create_release: repo and title (tag name) required")
        return gh('POST', f'{base}/repos/{repo}/releases', json={'tag_name': title, 'name': title, 'body': body, 'draft': False})

    else:
        raise ValueError(f"GitHub: unknown action '{action}'")

