"""GitHub API action node."""
import base64, json, ipaddress, socket
import logging
import httpx
from json import JSONDecodeError
from app.nodes._utils import _render, _resolve_cred_raw

NODE_TYPE = "action.github"
LABEL = "GitHub"

logger = logging.getLogger(__name__)

_BLOCKED_NETWORKS = [
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("0.0.0.0/8"),
    ipaddress.ip_network("224.0.0.0/4"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fe80::/10"),
    ipaddress.ip_network("ff00::/8"),
]


def _is_internal_ip(ip_str: str) -> bool:
    try:
        ip = ipaddress.ip_address(ip_str)
        return any(ip in net for net in _BLOCKED_NETWORKS)
    except ValueError:
        return True


def _check_url_ssrf(url: str) -> None:
    parsed = httpx.URL(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"SSRF blocked: invalid scheme '{parsed.scheme}'")
    hostname = parsed.host
    if not hostname:
        raise ValueError("SSRF blocked: no hostname in URL")
    try:
        infos = socket.getaddrinfo(hostname, None)
        for (_, _, _, _, sockaddr) in infos:
            ip_str = sockaddr[0]
            if _is_internal_ip(ip_str):
                raise ValueError(f"SSRF blocked: resolved to internal IP {ip_str}")
    except socket.gaierror:
        raise ValueError(f"SSRF blocked: could not resolve hostname '{hostname}'")


def _check_path_traversal(path: str, label: str) -> None:
    """Refuse paths that contain path traversal sequences."""
    if not path:
        return
    normalized = path.replace("\\", "/")
    if ".." in normalized or normalized.startswith("/"):
        raise ValueError(f"GitHub {label}: path '{path}' contains invalid traversal sequence")


def run(config, inp, context, logger, creds=None, **kwargs):
    """Interact with GitHub REST API."""
    logger.info("GitHub action started")

    token = _render(config.get('token', ''), context, creds)
    cred_name = _render(config.get('credential', ''), context, creds)

    if cred_name and creds and not token:
        raw = _resolve_cred_raw(cred_name, creds)
        if raw:
            try:
                token = json.loads(raw).get('token', raw)
            except (JSONDecodeError, ValueError):
                token = raw

    if not token:
        raise ValueError("GitHub: no token configured")

    repo = _render(config.get('repo', ''), context, creds)
    action = config.get('action', 'get_repo')
    number = _render(config.get('number', ''), context, creds)
    title = _render(config.get('title', ''), context, creds)
    body = _render(config.get('body', ''), context, creds)
    path = _render(config.get('path', ''), context, creds)
    branch = _render(config.get('branch', 'main'), context, creds)
    state = _render(config.get('state', 'open'), context, creds)
    labels = _render(config.get('labels', ''), context, creds)

    headers = {
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28'
    }
    base = 'https://api.github.com'
    _check_url_ssrf(base)

    def gh(method, url, **kw):
        _check_url_ssrf(url)
        max_redirects = 10
        while max_redirects > 0:
            try:
                r = httpx.request(method, url, headers=headers, timeout=httpx.Timeout(30.0), follow_redirects=False, **kw)
            except httpx.HTTPError as exc:
                logger.error("GitHub API request error action=%s url=%s error=%s", action, url, exc)
                raise
            except (OSError, httpx.ConnectError, httpx.Timeout) as exc:
                logger.error("GitHub API socket error action=%s url=%s error=%s", action, url, exc)
                raise
            except (AttributeError, KeyError, TypeError, ValueError) as exc:
                logger.error("GitHub API unexpected error action=%s url=%s error=%s", action, url, exc)
                raise
            location = r.headers.get("location") or r.headers.get("Location")
            if not location:
                try:
                    r.raise_for_status()
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code in (401, 403, 404, 422):
                        logger.warning("GitHub API client error action=%s url=%s status=%s response=%s",
                                       action, url, exc.response.status_code, exc.response.text[:200])
                    else:
                        logger.error("GitHub API HTTP error action=%s url=%s status=%s response=%s",
                                     action, url, exc.response.status_code, exc.response.text[:200])
                    raise exc
                except httpx.HTTPError as exc:
                    logger.error("GitHub API network error action=%s url=%s error=%s", action, url, exc)
                    raise exc
                return r.json() if r.content else {}
            max_redirects -= 1
            if max_redirects == 0:
                raise ValueError("GitHub: too many redirects")
            _check_url_ssrf(location)
            url = location
            logger.info("GitHub: following redirect to %s", url)
        raise ValueError("GitHub: redirect loop exceeded")

    if action == 'get_repo':
        if not repo:
            raise ValueError("GitHub get_repo: repo required")
        logger.info("GitHub: get_repo repo=%s", repo)
        return gh('GET', f'{base}/repos/{repo}')

    elif action == 'list_issues':
        if not repo:
            raise ValueError("GitHub list_issues: repo required")
        logger.info("GitHub: list_issues repo=%s state=%s", repo, state)
        params = {'state': state or 'open', 'per_page': 25}
        if labels:
            params['labels'] = labels
        return {'issues': gh('GET', f'{base}/repos/{repo}/issues', params=params)}

    elif action == 'get_issue':
        if not repo or not number:
            raise ValueError("GitHub get_issue: repo and number required")
        logger.info("GitHub: get_issue %s#%s", repo, number)
        return gh('GET', f'{base}/repos/{repo}/issues/{number}')

    elif action == 'create_issue':
        if not repo or not title:
            raise ValueError("GitHub create_issue: repo and title required")
        logger.info("GitHub: create_issue repo=%s title=%s", repo, title[:50])
        payload = {'title': title, 'body': body}
        if labels:
            payload['labels'] = [l.strip() for l in labels.split(',')]
        return gh('POST', f'{base}/repos/{repo}/issues', json=payload)

    elif action == 'close_issue':
        if not repo or not number:
            raise ValueError("GitHub close_issue: repo and number required")
        logger.info("GitHub: close_issue %s#%s", repo, number)
        return gh('PATCH', f'{base}/repos/{repo}/issues/{number}', json={'state': 'closed'})

    elif action == 'add_comment':
        if not repo or not number or not body:
            raise ValueError("GitHub add_comment: repo, number, and body required")
        logger.info("GitHub: add_comment %s#%s", repo, number)
        return gh('POST', f'{base}/repos/{repo}/issues/{number}/comments', json={'body': body})

    elif action == 'list_commits':
        if not repo:
            raise ValueError("GitHub list_commits: repo required")
        logger.info("GitHub: list_commits repo=%s branch=%s", repo, branch)
        return {'commits': gh('GET', f'{base}/repos/{repo}/commits', params={'sha': branch, 'per_page': 20})}

    elif action == 'list_prs':
        if not repo:
            raise ValueError("GitHub list_prs: repo required")
        logger.info("GitHub: list_prs repo=%s state=%s", repo, state)
        return {'pull_requests': gh('GET', f'{base}/repos/{repo}/pulls', params={'state': state or 'open', 'per_page': 20})}

    elif action == 'get_file':
        if not repo or not path:
            raise ValueError("GitHub get_file: repo and path required")
        _check_path_traversal(path, "get_file")
        logger.info("GitHub: get_file repo=%s path=%s branch=%s", repo, path, branch)
        data = gh('GET', f'{base}/repos/{repo}/contents/{path}', params={'ref': branch})
        text = base64.b64decode(data.get('content', '')).decode('utf-8', errors='replace') if data.get('encoding') == 'base64' else ''
        return {**data, 'decoded_content': text}

    elif action == 'create_release':
        if not repo or not title:
            raise ValueError("GitHub create_release: repo and title (tag name) required")
        logger.info("GitHub: create_release repo=%s tag=%s", repo, title)
        return gh('POST', f'{base}/repos/{repo}/releases', json={'tag_name': title, 'name': title, 'body': body, 'draft': False})

    else:
        raise ValueError(f"GitHub: unknown action '{action}'")
