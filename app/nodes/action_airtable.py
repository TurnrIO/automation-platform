"""Airtable action node.

Wraps the Airtable REST API v0 using only httpx (already a project dependency).

Credential JSON fields:
  api_key  — Airtable personal access token (starts with "pat…")
  base_id  — Airtable base ID (starts with "app…")

These can also be overridden directly in config fields.

Operations
----------
  list_records    — fetch all records from a table (handles pagination)
  get_record      — fetch a single record by ID
  create_record   — create one record
  update_record   — update fields on an existing record (PATCH)
  upsert_record   — create or update based on a field match
  delete_record   — delete a record by ID
  search          — list_records with filter_formula + optional sort

Output
------
  list_records / search:
    { records[], count, offset }
  get_record:
    { record, id, fields }
  create_record:
    { record, id, fields, created: true }
  update_record / upsert_record:
    { record, id, fields, updated: true }
  delete_record:
    { id, deleted: true }
"""
import logging
import json
import ipaddress
import socket
import urllib.parse
import httpx
from json import JSONDecodeError
from app.nodes._utils import _render, _resolve_cred_raw

logger = logging.getLogger(__name__)

NODE_TYPE = "action.airtable"
LABEL = "Airtable"

_BASE_URL = "https://api.airtable.com/v0"

# ── SSRF protection (same pattern as action_graphql) ─────────────────────────

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
_ALLOWED_SCHEME = "https"


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


def _check_ssrf(url: str) -> None:
    """Validate URL scheme and resolve hostname for SSRF check.
    Raises ValueError if URL is unsafe (non-HTTPS or resolves to blocked IP).
    """
    parsed = urllib.parse.urlparse(url)
    scheme = parsed.scheme.lower()
    if scheme != _ALLOWED_SCHEME:
        raise ValueError(
            f"Airtable: only {_ALLOWED_SCHEME} URLs are allowed. "
            f"Got scheme '{scheme}' in URL: {url[:100]}"
        )
    host = parsed.hostname
    if not host:
        raise ValueError(f"Airtable: URL has no valid hostname: {url[:100]}")
    try:
        infos = socket.getaddrinfo(host, 443, socket.AF_UNSPEC, socket.SOCK_STREAM)
    except socket.gaierror:
        raise ValueError(f"Airtable: could not resolve hostname '{host}' in URL: {url[:100]}")
    for (family, _, _, _, sockaddr) in infos:
        if family in (socket.AF_INET, socket.AF_INET6):
            ip_str = sockaddr[0]
            if _blocked_ip(ip_str):
                raise ValueError(
                    f"Airtable: URL resolves to blocked IP {ip_str}. "
                    f"Hostname '{host}' is not allowed. URL: {url[:100]}"
                )


def _get_headers(api_key: str) -> dict:
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }


def _resolve_creds(config, context, creds):
    """Return (api_key, base_id) from credential or inline config fields."""
    api_key = _render(config.get("api_key", ""), context, creds).strip()
    base_id = _render(config.get("base_id", ""), context, creds).strip()

    cred_name = _render(config.get("credential", ""), context, creds).strip()
    if cred_name and creds and not (api_key and base_id):
        raw = _resolve_cred_raw(cred_name, creds)
        if raw:
            try:
                c = json.loads(raw)
                api_key = api_key or c.get("api_key", "") or c.get("token", "")
                base_id = base_id or c.get("base_id", "") or c.get("base", "")
            except (JSONDecodeError, AttributeError):
                api_key = api_key or raw.strip()

    if not api_key:
        raise ValueError("Airtable: 'api_key' is required (set in credential or config)")
    if not base_id:
        raise ValueError("Airtable: 'base_id' is required (set in credential or config)")
    # Validate base_id has valid structure and SSRF-check the base URL
    if not base_id.startswith("app"):
        raise ValueError(f"Airtable: base_id '{base_id}' does not look like a valid Airtable base ID")
    base_url = f"https://api.airtable.com/v0/{base_id}"
    try:
        _check_ssrf(base_url)
    except ValueError as e:
        logger.warning("Airtable: SSRF check failed — %s", e)
        return {"__error": f"SSRF check failed: {e}", "base_url": base_url}
    return api_key, base_id


def _list_all(client, url: str, headers: dict, params: dict) -> list:
    """Fetch all pages of records, following Airtable's offset pagination."""
    records = []
    offset = None
    while True:
        p = dict(params)
        if offset:
            p["offset"] = offset
        try:
            resp = client.get(url, headers=headers, params=p)
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning("Airtable: HTTP error during list — %s", exc)
            return {"__error": f"Airtable list failed: HTTP error — {exc}", "records": []}
        except OSError as exc:
            logger.warning("Airtable: connection error during list — %s", exc)
            return {"__error": f"Airtable list failed: connection error — {exc}", "records": []}
        data = resp.json()
        records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
    return records


def _clean_record(r: dict) -> dict:
    """Flatten an Airtable record for easier downstream use."""
    return {
        "id":          r.get("id", ""),
        "fields":      r.get("fields", {}),
        "created_time": r.get("createdTime", ""),
    }


def run(config, inp, context, logger, creds=None, **kwargs):
    import httpx

    logger.info("[action.airtable] Starting Airtable run")
    logger.info("Airtable: node invoked")

    result = _resolve_creds(config, context, creds)
    if isinstance(result, dict) and "__error" in result:
        return result
    api_key, base_id = result
    table      = _render(config.get("table", ""), context, creds).strip()
    operation  = _render(config.get("operation", "list_records"), context, creds).strip()
    record_id  = _render(config.get("record_id", ""), context, creds).strip()
    fields_raw = _render(config.get("fields_json", ""), context, creds).strip()
    formula    = _render(config.get("filter_formula", ""), context, creds).strip()
    sort_raw   = _render(config.get("sort_json", ""), context, creds).strip()
    view       = _render(config.get("view", ""), context, creds).strip()
    max_raw    = _render(config.get("max_records", ""), context, creds).strip()
    try: timeout = float(_render(config.get("timeout", "30"), context, creds))
    except (ValueError, TypeError): timeout = 30.0

    if not table:
        raise ValueError("Airtable: 'table' is required")

    # Parse JSON fields
    fields = {}
    if fields_raw:
        try:
            fields = json.loads(fields_raw)
        except JSONDecodeError as exc:
            raise ValueError(f"Airtable: fields_json is not valid JSON — {exc}") from exc

    sort = []
    if sort_raw:
        try:
            sort = json.loads(sort_raw)
        except JSONDecodeError as exc:
            logger.warning("Airtable: sort_json is not valid JSON — %s; proceeding with no sort", exc)

    table_url  = f"{_BASE_URL}/{base_id}/{table}"
    headers    = _get_headers(api_key)

    with httpx.Client(timeout=timeout) as client:

        # ── list_records / search ─────────────────────────────────────────────
        if operation in ("list_records", "search"):
            params = {}
            if formula:
                params["filterByFormula"] = formula
            if view:
                params["view"] = view
            if max_raw:
                try: max_records = int(max_raw)
                except (ValueError, TypeError): max_records = None
            if max_records is not None:
                params["maxRecords"] = max_records
            if sort:
                # Airtable expects sort[0][field]=... sort[0][direction]=...
                for i, s in enumerate(sort):
                    params[f"sort[{i}][field]"]     = s.get("field", "")
                    params[f"sort[{i}][direction]"] = s.get("direction", "asc")

            logger.info("Airtable: list_records %s/%s", base_id, table)
            records_raw = _list_all(client, table_url, headers, params)
            records     = [_clean_record(r) for r in records_raw]
            return {
                "records": records,
                "count":   len(records),
                "record":  records[0] if records else None,
            }

        # ── get_record ────────────────────────────────────────────────────────
        elif operation == "get_record":
            if not record_id:
                raise ValueError("Airtable get_record: 'record_id' is required")
            logger.info("Airtable: get_record %s", record_id)
            try:
                resp = client.get(f"{table_url}/{record_id}", headers=headers)
                resp.raise_for_status()
            except httpx.HTTPError as exc:
                logger.warning("Airtable: HTTP error on get_record — %s", exc)
                return {"__error": f"Airtable get_record failed: HTTP error — {exc}"}
            except OSError as exc:
                logger.warning("Airtable: connection error on get_record — %s", exc)
                return {"__error": f"Airtable get_record failed: connection error — {exc}"}
            r = _clean_record(resp.json())
            return {"record": r, "id": r["id"], "fields": r["fields"]}

        # ── create_record ─────────────────────────────────────────────────────
        elif operation == "create_record":
            if not fields:
                raise ValueError("Airtable create_record: 'fields_json' is required")
            logger.info("Airtable: create_record in %s", table)
            try:
                resp = client.post(table_url, headers=headers, json={"fields": fields})
                resp.raise_for_status()
            except httpx.HTTPError as exc:
                logger.warning("Airtable: HTTP error on create_record — %s", exc)
                return {"__error": f"Airtable create_record failed: HTTP error — {exc}"}
            except OSError as exc:
                logger.warning("Airtable: connection error on create_record — %s", exc)
                return {"__error": f"Airtable create_record failed: connection error — {exc}"}
            r = _clean_record(resp.json())
            return {"record": r, "id": r["id"], "fields": r["fields"], "created": True}

        # ── update_record ─────────────────────────────────────────────────────
        elif operation == "update_record":
            if not record_id:
                raise ValueError("Airtable update_record: 'record_id' is required")
            if not fields:
                raise ValueError("Airtable update_record: 'fields_json' is required")
            logger.info("Airtable: update_record %s", record_id)
            try:
                resp = client.patch(f"{table_url}/{record_id}", headers=headers,
                                    json={"fields": fields})
                resp.raise_for_status()
            except httpx.HTTPError as exc:
                logger.warning("Airtable: HTTP error on update_record — %s", exc)
                return {"__error": f"Airtable update_record failed: HTTP error — {exc}"}
            except OSError as exc:
                logger.warning("Airtable: connection error on update_record — %s", exc)
                return {"__error": f"Airtable update_record failed: connection error — {exc}"}
            r = _clean_record(resp.json())
            return {"record": r, "id": r["id"], "fields": r["fields"], "updated": True}

        # ── upsert_record ─────────────────────────────────────────────────────
        elif operation == "upsert_record":
            # Airtable upsert: PATCH the table with performUpsert
            upsert_field = _render(config.get("upsert_field", ""), context, creds).strip()
            if not upsert_field:
                raise ValueError("Airtable upsert_record: 'upsert_field' is required")
            if not fields:
                raise ValueError("Airtable upsert_record: 'fields_json' is required")
            logger.info("Airtable: upsert_record on field '%s'", upsert_field)
            payload = {
                "records":       [{"fields": fields}],
                "performUpsert": {"fieldsToMergeOn": [upsert_field]},
            }
            try:
                resp = client.patch(table_url, headers=headers, json=payload)
                resp.raise_for_status()
            except httpx.HTTPError as exc:
                logger.warning("Airtable: HTTP error on upsert_record — %s", exc)
                return {"__error": f"Airtable upsert_record failed: HTTP error — {exc}"}
            except OSError as exc:
                logger.warning("Airtable: connection error on upsert_record — %s", exc)
                return {"__error": f"Airtable upsert_record failed: connection error — {exc}"}
            data    = resp.json()
            updated = data.get("updatedRecords", [])
            created = data.get("createdRecords", [])
            records_out = [_clean_record(r) for r in data.get("records", [])]
            r = records_out[0] if records_out else {}
            return {
                "record":  r,
                "id":      r.get("id", ""),
                "fields":  r.get("fields", {}),
                "updated": bool(updated),
                "created": bool(created),
            }

        # ── delete_record ─────────────────────────────────────────────────────
        elif operation == "delete_record":
            if not record_id:
                raise ValueError("Airtable delete_record: 'record_id' is required")
            logger.info("Airtable: delete_record %s", record_id)
            try:
                resp = client.delete(f"{table_url}/{record_id}", headers=headers)
                resp.raise_for_status()
            except httpx.HTTPError as exc:
                logger.warning("Airtable: HTTP error on delete_record — %s", exc)
                return {"__error": f"Airtable delete_record failed: HTTP error — {exc}"}
            except OSError as exc:
                logger.warning("Airtable: connection error on delete_record — %s", exc)
                return {"__error": f"Airtable delete_record failed: connection error — {exc}"}
            data = resp.json()
            return {"id": data.get("id", record_id), "deleted": data.get("deleted", True)}

        else:
            raise ValueError(f"Airtable: unknown operation '{operation}'")
