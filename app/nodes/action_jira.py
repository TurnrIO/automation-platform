"""Jira REST API action node.

Credential JSON fields:
  base_url   — e.g. "https://yourcompany.atlassian.net"
  email      — Atlassian account email
  api_token  — Atlassian API token (from id.atlassian.com/manage/api-tokens)

Operations
----------
  get-issue        — Fetch issue by key (e.g. "PROJ-123")
  create-issue     — Create a new issue
  update-issue     — Update fields on an existing issue
  add-comment      — Add a comment to an issue
  search           — JQL search; returns list of matching issues
  get-transitions  — List available transitions for an issue
  transition-issue — Move issue to a new status via transition ID
  delete-issue     — Delete an issue (use with care)

Output shape (varies by operation)
-----------------------------------
  get-issue / create-issue / update-issue:
    { "issue": {…}, "key": "PROJ-123", "id": "10001", "url": "https://…" }

  add-comment:
    { "comment": {…}, "id": "12345" }

  search:
    { "issues": [{…}, …], "count": N, "total": N, "issue": {first item} }

  get-transitions:
    { "transitions": [{id, name, to_status}, …] }

  transition-issue:
    { "ok": true, "transition_id": "21" }

  delete-issue:
    { "ok": true, "key": "PROJ-123" }
"""
from __future__ import annotations

import base64
import ipaddress
import json
import logging
import socket
import urllib.error
import urllib.parse
import urllib.request
from json import JSONDecodeError

logger = logging.getLogger(__name__)

from ._utils import _render, _resolve_cred_raw


NODE_TYPE = "action.jira"
LABEL     = "Jira"

# ── SSRF protection ────────────────────────────────────────────────────────────

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
_IMDS_IP = ipaddress.ip_address("169.254.169.254")


def _blocked_ip(ip_str: str) -> bool:
    try:
        ip = ipaddress.ip_address(ip_str)
        if ip == _IMDS_IP:
            return True
        for net in _BLOCKED_NETWORKS:
            if ip in net:
                return True
    except ValueError:
        pass
    return False


def _check_url_ssrf(url: str) -> None:
    parsed = urllib.parse.urlparse(url)
    scheme = parsed.scheme.lower()
    if scheme not in ("http", "https"):
        raise ValueError(
            f"Jira: only http/https URLs are allowed. "
            f"Got scheme '{scheme}' in URL: {url[:100]}"
        )
    host = parsed.hostname
    if not host:
        raise ValueError(f"Jira: could not determine hostname from URL: {url[:100]}")
    try:
        addr_info = socket.getaddrinfo(host, None)
    except socket.gaierror:
        raise ValueError(f"Jira: could not resolve hostname '{host}' in URL: {url[:100]}")
    for (family, _, _, _, sockaddr) in addr_info:
        ip_str = sockaddr[0]
        if _blocked_ip(ip_str):
            raise ValueError(
                f"Jira: URL resolves to blocked address {ip_str}. "
                f"URL: {url[:100]}"
            )


# ── HTTP helper ────────────────────────────────────────────────────────────────

def _jira_request(
    base_url: str,
    email: str,
    api_token: str,
    method: str,
    path: str,
    payload: dict | None = None,
    params: dict | None = None,
) -> tuple[int, dict | list | None]:
    """Make an authenticated Jira REST v3 request. Returns (status_code, body)."""
    url = base_url.rstrip("/") + path
    if params:
        url += "?" + urllib.parse.urlencode(params)

    credentials = base64.b64encode(f"{email}:{api_token}".encode()).decode()
    headers = {
        "Authorization": f"Basic {credentials}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }

    data = json.dumps(payload).encode() if payload is not None else None
    req  = urllib.request.Request(url, data=data, headers=headers, method=method.upper())

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read()
            if body:
                return resp.status, json.loads(body)
            return resp.status, None
    except urllib.error.HTTPError as exc:
        body = exc.read()
        try:
            err_body = json.loads(body)
        except JSONDecodeError:
            # Log a safe prefix only — raw response body may contain sensitive Jira data
            safe_body = body.decode(errors="replace")[:200]
            logger.warning("Jira API error %s: response was not JSON; body prefix: %r",
                        exc.code, safe_body)
            err_body = {"message": "(raw non-JSON response from Jira)"}
        raise RuntimeError(
            f"Jira API error {exc.code}: {err_body.get('errorMessages') or err_body.get('message') or err_body}"
        ) from exc
    except urllib.error.URLError as exc:
        logger.warning("Jira: URL error — %s", exc)
        raise RuntimeError(f"Jira request failed: {exc}") from exc
    except OSError as exc:
        logger.warning("Jira: connection error — %s", exc)
        raise RuntimeError(f"Jira connection error: {exc}") from exc
    except (KeyError, IndexError, TypeError, ValueError, AttributeError) as exc:
        logger.warning("Jira: unexpected error — %s", exc)
        raise RuntimeError(f"Jira request failed: {exc}") from exc


def _flatten_issue(raw: dict) -> dict:
    """Return a friendlier dict alongside the raw Jira issue object."""
    fields = raw.get("fields") or {}
    return {
        "id":          raw.get("id"),
        "key":         raw.get("key"),
        "url":         raw.get("self", ""),
        "summary":     fields.get("summary", ""),
        "status":      (fields.get("status") or {}).get("name", ""),
        "issue_type":  (fields.get("issuetype") or {}).get("name", ""),
        "priority":    (fields.get("priority") or {}).get("name", ""),
        "assignee":    ((fields.get("assignee") or {}).get("displayName") or
                        (fields.get("assignee") or {}).get("emailAddress", "")),
        "reporter":    ((fields.get("reporter") or {}).get("displayName") or
                        (fields.get("reporter") or {}).get("emailAddress", "")),
        "labels":      fields.get("labels", []),
        "description": ((fields.get("description") or {}).get("content") or
                        fields.get("description") or ""),
        "created":     fields.get("created", ""),
        "updated":     fields.get("updated", ""),
        "_raw":        raw,
    }


# ── Operations ────────────────────────────────────────────────────────────────

def _get_issue(base_url, email, token, issue_key, expand=""):
    params = {}
    if expand:
        params["expand"] = expand
    _, body = _jira_request(base_url, email, token, "GET",
                             f"/rest/api/3/issue/{issue_key}", params=params or None)
    flat = _flatten_issue(body)
    return {"issue": flat, "key": flat["key"], "id": flat["id"], "url": flat["url"]}


def _create_issue(base_url, email, token, project_key, issue_type, summary,
                  description="", priority="", assignee="", labels=None, extra_fields=None):
    fields: dict = {
        "project":   {"key": project_key},
        "issuetype": {"name": issue_type},
        "summary":   summary,
    }
    if description:
        # Jira v3 uses ADF for description; fall back to plain text wrapper
        fields["description"] = {
            "type":    "doc",
            "version": 1,
            "content": [{"type": "paragraph",
                         "content": [{"type": "text", "text": description}]}],
        }
    if priority:
        fields["priority"] = {"name": priority}
    if assignee:
        fields["assignee"] = {"id": assignee} if len(assignee) > 20 else {"emailAddress": assignee}
    if labels:
        fields["labels"] = labels if isinstance(labels, list) else [labels]
    if extra_fields and isinstance(extra_fields, dict):
        fields.update(extra_fields)

    _, body = _jira_request(base_url, email, token, "POST",
                             "/rest/api/3/issue", {"fields": fields})
    flat = _flatten_issue({"id": body.get("id"), "key": body.get("key"),
                            "self": body.get("self", ""), "fields": {}})
    return {"issue": flat, "key": body.get("key"), "id": body.get("id"),
            "url": body.get("self", "")}


def _update_issue(base_url, email, token, issue_key, fields_json):
    if isinstance(fields_json, str):
        try:
            fields = json.loads(fields_json)
        except JSONDecodeError as exc:
            raise ValueError(f"action.jira update-issue: 'fields' must be valid JSON — {exc}") from exc
    else:
        fields = fields_json or {}

    _jira_request(base_url, email, token, "PUT",
                  f"/rest/api/3/issue/{issue_key}", {"fields": fields})
    return {"ok": True, "key": issue_key}


def _add_comment(base_url, email, token, issue_key, comment_text):
    body_payload = {
        "body": {
            "type":    "doc",
            "version": 1,
            "content": [{"type": "paragraph",
                         "content": [{"type": "text", "text": comment_text}]}],
        }
    }
    _, body = _jira_request(base_url, email, token, "POST",
                             f"/rest/api/3/issue/{issue_key}/comment", body_payload)
    return {"comment": body, "id": (body or {}).get("id", "")}


def _search(base_url, email, token, jql, fields="summary,status,assignee,priority,issuetype",
            max_results=50, start_at=0):
    payload = {
        "jql":        jql,
        "maxResults": int(max_results),
        "startAt":    int(start_at),
        "fields":     [f.strip() for f in fields.split(",") if f.strip()],
    }
    _, body = _jira_request(base_url, email, token, "POST",
                             "/rest/api/3/issue/search", payload)
    raw_issues = (body or {}).get("issues", [])
    issues = [_flatten_issue(i) for i in raw_issues]
    return {
        "issues": issues,
        "count":  len(issues),
        "total":  (body or {}).get("total", len(issues)),
        "issue":  issues[0] if issues else {},
    }


def _get_transitions(base_url, email, token, issue_key):
    _, body = _jira_request(base_url, email, token, "GET",
                             f"/rest/api/3/issue/{issue_key}/transitions")
    transitions = [
        {
            "id":        t.get("id"),
            "name":      t.get("name"),
            "to_status": (t.get("to") or {}).get("name", ""),
        }
        for t in (body or {}).get("transitions", [])
    ]
    return {"transitions": transitions}


def _transition_issue(base_url, email, token, issue_key, transition_id, comment=""):
    payload: dict = {"transition": {"id": str(transition_id)}}
    if comment:
        payload["update"] = {
            "comment": [{"add": {
                "body": {
                    "type": "doc", "version": 1,
                    "content": [{"type": "paragraph",
                                 "content": [{"type": "text", "text": comment}]}],
                }
            }}]
        }
    _jira_request(base_url, email, token, "POST",
                  f"/rest/api/3/issue/{issue_key}/transitions", payload)
    return {"ok": True, "transition_id": str(transition_id)}


def _delete_issue(base_url, email, token, issue_key, delete_subtasks="false"):
    _jira_request(base_url, email, token, "DELETE",
                  f"/rest/api/3/issue/{issue_key}",
                  params={"deleteSubtasks": delete_subtasks})
    return {"ok": True, "key": issue_key}


# ── Node entry point ──────────────────────────────────────────────────────────

def run(config: dict, inp: dict, context: dict, logger, creds=None, **kwargs) -> dict:
    creds = creds or {}

    cred_name = _render(config.get("credential", ""), context, creds)
    raw_cred  = _resolve_cred_raw(cred_name, creds)
    try:
        cred = json.loads(raw_cred) if raw_cred else {}
    except (JSONDecodeError, TypeError):
        cred = {}

    if not cred:
        raise ValueError(
            "action.jira: no credential configured. "
            "Set 'credential' to a credential with base_url, email, api_token."
        )

    base_url  = cred.get("base_url", "").rstrip("/")
    email     = cred.get("email", "")
    api_token = cred.get("api_token", "") or cred.get("token", "")

    if not base_url or not email or not api_token:
        raise ValueError(
            "action.jira credential must contain: base_url, email, api_token"
        )

    # SSRF: validate base_url before making any HTTP request
    try:
        _check_url_ssrf(base_url)
    except ValueError as e:
        logger.warning("Jira: SSRF check failed — %s", e)
        return {"__error": str(e), "base_url": base_url}

    def r(key, default=""):
        return _render(str(config.get(key, default) or default), context, creds)

    operation = r("operation", "get-issue").strip().lower()

    logger.info("[action.jira] operation=%s base_url=%s", operation, base_url)

    if operation == "get-issue":
        logger.info("action.jira: get-issue key=%s", r("issue_key"))
        return _get_issue(base_url, email, api_token,
                          r("issue_key"), r("expand"))

    if operation == "create-issue":
        logger.info("action.jira: create-issue project=%s type=%s summary=%r",
                    r("project_key"), r("issue_type", "Task"), r("summary")[:50])
        extra_raw = r("extra_fields")
        try:
            extra = json.loads(extra_raw) if extra_raw.strip() else {}
        except JSONDecodeError:
            extra = {}
        labels_raw = r("labels")
        labels = [l.strip() for l in labels_raw.split(",") if l.strip()] if labels_raw else []
        return _create_issue(
            base_url, email, api_token,
            project_key  = r("project_key"),
            issue_type   = r("issue_type", "Task"),
            summary      = r("summary"),
            description  = r("description"),
            priority     = r("priority"),
            assignee     = r("assignee"),
            labels       = labels,
            extra_fields = extra,
        )

    if operation == "update-issue":
        logger.info("action.jira: update-issue key=%s", r("issue_key"))
        return _update_issue(base_url, email, api_token,
                             r("issue_key"), r("fields"))

    if operation == "add-comment":
        logger.info("action.jira: add-comment key=%s", r("issue_key"))
        return _add_comment(base_url, email, api_token,
                            r("issue_key"), r("comment"))

    if operation == "search":
        logger.info("action.jira: search jql=%r", r("jql")[:60])
        return _search(
            base_url, email, api_token,
            jql         = r("jql"),
            fields      = r("fields", "summary,status,assignee,priority,issuetype"),
            max_results = r("max_results", "50"),
            start_at    = r("start_at", "0"),
        )

    if operation == "get-transitions":
        logger.info("action.jira: get-transitions key=%s", r("issue_key"))
        return _get_transitions(base_url, email, api_token, r("issue_key"))

    if operation == "transition-issue":
        logger.info("action.jira: transition-issue key=%s transition=%s",
                    r("issue_key"), r("transition_id"))
        return _transition_issue(base_url, email, api_token,
                                 r("issue_key"), r("transition_id"), r("comment"))

    if operation == "delete-issue":
        logger.info("action.jira: delete-issue key=%s", r("issue_key"))
        return _delete_issue(base_url, email, api_token,
                             r("issue_key"), r("delete_subtasks", "false"))

    raise ValueError(f"action.jira: unknown operation '{operation}'")
