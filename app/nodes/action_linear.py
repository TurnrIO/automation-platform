"""Linear.app issue tracker node (GraphQL API)."""
import ipaddress
import json
import logging
import socket
from json import JSONDecodeError
import urllib.request
import urllib.error
from app.nodes._utils import _render

NODE_TYPE = "action.linear"
LABEL     = "Linear"

logger = logging.getLogger(__name__)

_ENDPOINT = "https://api.linear.app/graphql"

# ── SSRF protection ───────────────────────────────────────────────────────────
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
    ipaddress.ip_network("fc00::/7"),
    ipaddress.ip_network("ff00::/8"),
]
_IMDS_IP = ipaddress.ip_address("169.254.169.254")


def _check_ssrf(host: str) -> None:
    """Resolve hostname and check it doesn't point to a blocked network."""
    try:
        infos = socket.getaddrinfo(host, 443, socket.AF_UNSPEC, socket.SOCK_STREAM)
    except socket.gaierror:
        raise ValueError(f"Linear: could not resolve hostname '{host}'")
    for (family, _, _, _, sockaddr) in infos:
        if family in (socket.AF_INET, socket.AF_INET6):
            ip_str = sockaddr[0]
            try:
                ip = ipaddress.ip_address(ip_str)
                if ip == _IMDS_IP:
                    raise ValueError(f"Linear: host '{host}' resolves to blocked IMDS IP {ip_str}")
                for net in _BLOCKED_NETWORKS:
                    if ip in net:
                        raise ValueError(f"Linear: host '{host}' resolves to blocked IP {ip_str}")
            except ValueError:
                raise


def _gql(api_key: str, query: str, variables: dict = None, logger=None):
    body = json.dumps({"query": query, "variables": variables or {}}).encode()
    req  = urllib.request.Request(_ENDPOINT, data=body)
    req.add_header("Authorization", api_key)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        msg = f"Linear {e.code}: {e.read().decode()[:300]}"
        if logger:
            logger.warning("Linear: HTTP error — %s", msg)
        return {"__error": msg}
    except urllib.error.URLError as e:
        msg = f"Linear connection error: {e.reason}"
        if logger:
            logger.warning("Linear: connection error — %s", msg)
        return {"__error": msg}
    except OSError as e:
        msg = f"Linear socket error: {e}"
        if logger:
            logger.warning("Linear: socket error — %s", msg)
        return {"__error": msg}
    if data.get("errors"):
        msg = f"Linear GraphQL error: {data['errors'][0]['message']}"
        if logger:
            logger.warning("Linear: GraphQL error — %s", msg)
        return {"__error": msg}
    return data.get("data", {})


def run(config, inp, context, logger, creds=None, **kwargs):
    logger.info("Linear action started")
    cred_name = config.get("credential", "")
    api_key   = ""
    if cred_name and creds:
        raw = creds.get(cred_name, {})
        if isinstance(raw, str):
            try:   raw = json.loads(raw)
            except JSONDecodeError: raw = {}
        api_key = raw.get("api_key", raw.get("token", ""))
    if not api_key:
        api_key = _render(config.get("api_key", ""), context, creds)
    if not api_key:
        raise ValueError("Linear: api_key is required (set via credential or api_key field)")

    op = _render(config.get("operation", "get_issue"), context, creds)
    logger.info("Linear: op=%s", op)

    # ── get issue ─────────────────────────────────────────────────────────────
    if op == "get_issue":
        issue_id = _render(config.get("issue_id", ""), context, creds)
        logger.info("Linear: get_issue %s", issue_id)
        data = _gql(api_key, """
            query($id: String!) {
              issue(id: $id) {
                id identifier title description state { name }
                assignee { name email } priority createdAt updatedAt url
              }
            }
        """, {"id": issue_id}, logger=logger)
        if data.get("__error"):
            return data
        issue = data.get("issue", {})
        return {"issue": issue, "id": issue.get("id"), "title": issue.get("title"),
                "state": (issue.get("state") or {}).get("name")}

    # ── create issue ──────────────────────────────────────────────────────────
    elif op == "create_issue":
        team_id     = _render(config.get("team_id", ""), context, creds)
        title       = _render(config.get("title", ""), context, creds)
        description = _render(config.get("description", ""), context, creds)
        priority_str = config.get("priority", "0")
        try:   priority = int(priority_str)
        except (ValueError, TypeError): priority = 0
        logger.info("Linear: create_issue team=%s title=%s", team_id, title[:50])
        data = _gql(api_key, """
            mutation($input: IssueCreateInput!) {
              issueCreate(input: $input) {
                success issue { id identifier title url state { name } }
              }
            }
        """, {"input": {"teamId": team_id, "title": title,
                        "description": description, "priority": priority}}, logger=logger)
        if data.get("__error"):
            return data
        issue = (data.get("issueCreate") or {}).get("issue", {})
        return {"issue": issue, "id": issue.get("id"), "title": issue.get("title"),
                "url": issue.get("url")}

    # ── update issue ──────────────────────────────────────────────────────────
    elif op == "update_issue":
        issue_id = _render(config.get("issue_id", ""), context, creds)
        updates_raw = _render(config.get("updates", "{}"), context, creds)
        try:   updates = json.loads(updates_raw) if isinstance(updates_raw, str) else updates_raw
        except JSONDecodeError: raise ValueError("Linear update_issue: updates must be valid JSON")
        logger.info("Linear: update_issue %s", issue_id)
        data = _gql(api_key, """
            mutation($id: String!, $input: IssueUpdateInput!) {
              issueUpdate(id: $id, input: $input) {
                success issue { id identifier title state { name } updatedAt }
              }
            }
        """, {"id": issue_id, "input": updates}, logger=logger)
        if data.get("__error"):
            return data
        issue = (data.get("issueUpdate") or {}).get("issue", {})
        return {"issue": issue, "id": issue.get("id"), "success": (data.get("issueUpdate") or {}).get("success")}

    # ── search issues ─────────────────────────────────────────────────────────
    elif op == "search_issues":
        query_str = _render(config.get("query", ""), context, creds)
        try: limit = int(_render(config.get("limit", "25"), context, creds))
        except (ValueError, TypeError): limit = 25
        logger.info("Linear: search_issues query=%r limit=%s", query_str, limit)
        data = _gql(api_key, """
            query($filter: IssueFilter, $first: Int) {
              issues(filter: $filter, first: $first) {
                nodes { id identifier title state { name } priority assignee { name } url }
              }
            }
        """, {"filter": {"title": {"containsIgnoreCase": query_str}} if query_str else {},
              "first": min(limit, 100)}, logger=logger)
        if data.get("__error"):
            return data
        issues = (data.get("issues") or {}).get("nodes", [])
        return {"issues": issues, "count": len(issues), "issue": issues[0] if issues else None}

    else:
        raise ValueError(f"Linear: unknown operation {op!r}")
