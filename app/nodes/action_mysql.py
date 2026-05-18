"""MySQL query node — dedicated MySQL/MariaDB action.

Uses pymysql (install: pip install pymysql).

Credential JSON fields:
  host      — DB hostname (default: localhost)
  port      — port (default: 3306)
  username  — login user (alias: user)
  password  — login password
  database  — schema/database name (alias: db)
  charset   — character set (default: utf8mb4)
  dsn       — full DSN string, e.g. "mysql://user:pass@host:3306/db"
              (overrides individual fields when present)

Output shape
------------
{
  "rows":     [ {col: val, …}, … ],   — all result rows as dicts
  "count":    N,                       — len(rows)
  "columns":  ["col1", "col2", …],    — column names (empty for non-SELECT)
  "row":      {col: val},             — first row shortcut (or {})
  "affected": N,                       — rowcount for INSERT/UPDATE/DELETE
  "last_insert_id": N,                — LAST_INSERT_ID() after INSERT
}
"""
from __future__ import annotations

import json
import logging
import re
from json import JSONDecodeError

from ._utils import _render, _resolve_cred_raw

logger = logging.getLogger(__name__)

NODE_TYPE = "action.mysql"
LABEL     = "MySQL Query"


# ── Connection helpers ────────────────────────────────────────────────────────

def _parse_dsn(dsn: str) -> dict:
    """Very light DSN parser: mysql://user:pass@host:port/db"""
    m = re.match(
        r"mysql(?:2)?://(?:([^:@]*)(?::([^@]*))?@)?([^:/]+)(?::(\d+))?/(.+)",
        dsn, re.I,
    )
    if not m:
        return {}
    return {
        "username": m.group(1) or "",
        "password": m.group(2) or "",
        "host":     m.group(3) or "localhost",
        "port":     int(m.group(4) or 3306),
        "database": m.group(5) or "",
    }


def _build_connect_kwargs(cred: dict) -> dict:
    dsn = cred.get("dsn", "").strip()
    if dsn:
        parsed = _parse_dsn(dsn)
        cred = {**parsed, **{k: v for k, v in cred.items() if k != "dsn"}}

    host     = cred.get("host", "localhost")
    port     = int(cred.get("port", 3306))
    username = cred.get("username") or cred.get("user", "root")
    password = cred.get("password", "")
    database = cred.get("database") or cred.get("db", "")
    charset  = cred.get("charset", "utf8mb4")

    kw: dict = {
        "host":    host,
        "port":    port,
        "user":    username,
        "passwd":  password,
        "charset": charset,
    }
    if database:
        kw["db"] = database
    return kw


def _connect(kw: dict):
    try:
        import pymysql
        import pymysql.cursors
    except ImportError:
        raise RuntimeError(
            "MySQL support requires pymysql. "
            "Install it with: pip install pymysql"
        )
    conn = pymysql.connect(**kw, cursorclass=pymysql.cursors.DictCursor)
    return conn


def _normalize_params(raw: str, context: dict, creds: dict):
    if not raw or not raw.strip():
        return []
    rendered = _render(raw.strip(), context, creds)
    try:
        val = json.loads(rendered)
        if isinstance(val, (list, tuple)):
            return list(val)
        return [val]
    except (JSONDecodeError, TypeError):
        return []


# ── Node entry point ──────────────────────────────────────────────────────────

def run(config: dict, inp: dict, context: dict, logger, creds=None, **kwargs) -> dict:
    creds = creds or {}

    logger.info("[action.mysql] invoked")


    # Resolve credential
    cred_name = _render(config.get("credential", ""), context, creds)
    raw_cred  = _resolve_cred_raw(cred_name, creds)
    try:
        cred = json.loads(raw_cred) if raw_cred else {}
    except (JSONDecodeError, TypeError):
        cred = {}

    if not cred:
        raise ValueError(
            "action.mysql: no credential configured. "
            "Set 'credential' to a credential whose value is a JSON object "
            "with host/port/username/password/database fields (or a 'dsn' key)."
        )

    connect_kw = _build_connect_kwargs(cred)

    query = _render(config.get("query", ""), context, creds).strip()
    if not query:
        raise ValueError("action.mysql: 'query' is required")

    params    = _normalize_params(config.get("params", ""), context, creds)
    try: row_limit = int(_render(str(config.get("row_limit", "1000")), context, creds))
    except (ValueError, TypeError): row_limit = 1000

    logger.info(
        "[action.mysql] host=%s db=%s query=%s",
        connect_kw.get('host'), connect_kw.get('db'),
        query[:80] + ('…' if len(query) > 80 else ''),
    )

    conn = _connect(connect_kw)
    try:
        with conn.cursor() as cur:
            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)

            affected       = cur.rowcount if cur.rowcount is not None else 0
            last_insert_id = conn.insert_id()

            columns: list = []
            rows: list    = []

            if cur.description:
                columns = [d[0] for d in cur.description]
                raw_rows = cur.fetchall() or []
                rows = [dict(r) for r in raw_rows]
                if row_limit and len(rows) > row_limit:
                    rows = rows[:row_limit]
                    logger.info("[action.mysql] result truncated to %s rows", row_limit)

        conn.commit()
        logger.info("[action.mysql] rows=%s affected=%s", len(rows), affected)

        return {
            "rows":           rows,
            "count":          len(rows),
            "columns":        columns,
            "row":            rows[0] if rows else {},
            "affected":       affected,
            "last_insert_id": last_insert_id,
        }

    except (AttributeError, TypeError, ValueError, RuntimeError) as exc:
        try:
            conn.rollback()
        except (AttributeError, TypeError, RuntimeError):
            pass
        raise RuntimeError(f"action.mysql: query failed — {exc}") from exc
    finally:
        if conn is not None:
            try:
                conn.close()
            except (AttributeError, TypeError, RuntimeError):
                pass
