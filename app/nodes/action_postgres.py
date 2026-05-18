"""SQL query action node — PostgreSQL, MySQL, and SQLite.

Supported drivers (detected from the DSN prefix or explicit ``driver`` key):
  postgresql / postgres  — psycopg2-binary (already in requirements.txt)
  mysql                  — pymysql (install separately: pip install pymysql)
  sqlite                 — stdlib sqlite3

Credential JSON fields (store as a credential in the vault):
  dsn       — full connection string, e.g. "postgresql://user:pass@host/db"
              OR individual fields below:
  driver    — "postgresql" | "mysql" | "sqlite" (auto-detected from dsn if omitted)
  host      — database hostname
  port      — port (default: 5432 for PG, 3306 for MySQL)
  username  — login user
  password  — login password
  database  — database / schema name  (or file path for SQLite)

Output shape
------------
{
  "rows":     [ {col: val, …}, … ],   — all result rows as dicts
  "count":    N,                       — len(rows)
  "columns":  ["col1", "col2", …],    — column names (empty for non-SELECT)
  "row":      {col: val},             — first row shortcut (or {})
  "affected": N,                       — rowcount for INSERT/UPDATE/DELETE
}
"""
import ipaddress
import json
import socket
from json import JSONDecodeError
import psycopg2
import logging
logger = logging.getLogger(__name__)
from ._utils import _render, _resolve_cred_raw

# ── SSRF protection ───────────────────────────────────────────────────────────
_BLOCKED_NETWORKS = [
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("0.0.0.0/8"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fe80::/10"),
    ipaddress.ip_network("ff00::/8"),
]
_IMDS_IP = ipaddress.ip_address("169.254.169.254")


def _is_internal_ip(ip_str: str) -> bool:
    try:
        ip = ipaddress.ip_address(ip_str)
        if ip == _IMDS_IP:
            return True
        return any(ip in net for net in _BLOCKED_NETWORKS)
    except ValueError:
        return True


def _check_ssrf(host: str, port: int) -> None:
    """Block connections to internal/reserved IPs via DNS resolution."""
    try:
        infos = socket.getaddrinfo(host, port)
        for (_, _, _, _, sockaddr) in infos:
            if _is_internal_ip(sockaddr[0]):
                raise ValueError(f"SSRF blocked: resolved to internal IP {sockaddr[0]}")
    except socket.gaierror:
        raise ValueError(f"SSRF blocked: could not resolve hostname '{host}'")


def _dsn_host(dsn: str) -> str | None:
    """Extract hostname from a DSN connection string."""
    if not dsn or dsn.startswith("sqlite"):
        return None
    try:
        from urllib.parse import urlparse
        parsed = urlparse(dsn)
        return parsed.hostname or None
    except (AttributeError, TypeError):
        return None

NODE_TYPE = "action.postgres"
LABEL     = "SQL Query"


# ── Driver helpers ────────────────────────────────────────────────────────────

def _detect_driver(dsn: str) -> str:
    dsn_lower = dsn.lower()
    if dsn_lower.startswith(("postgresql://", "postgres://")):
        return "postgresql"
    if dsn_lower.startswith("mysql"):
        return "mysql"
    if dsn_lower.startswith("sqlite"):
        return "sqlite"
    return "postgresql"   # default


def _build_dsn(cred: dict) -> tuple:
    """Return (dsn_or_None, driver, kwargs_for_connect)."""
    dsn    = cred.get("dsn", "").strip()
    driver = cred.get("driver", "").strip().lower()

    if dsn:
        if not driver:
            driver = _detect_driver(dsn)
        return dsn, driver, {}

    # Build from individual fields
    host     = cred.get("host", "localhost")
    port     = cred.get("port")
    username = cred.get("username") or cred.get("user", "")
    password = cred.get("password", "")
    database = cred.get("database") or cred.get("db", "")
    if not driver:
        driver = "postgresql"

    if driver == "sqlite":
        return database, "sqlite", {}

    if driver == "mysql":
        default_port = 3306
        kw = {
            "host":   host,
            "user":   username,
            "passwd": password,
            "db":     database,
            "port":   int(port or default_port),
        }
        return None, "mysql", kw

    # postgresql
    default_port = 5432
    kw = {
        "host":     host,
        "user":     username,
        "password": password,
        "dbname":   database,
        "port":     int(port or default_port),
    }
    return None, "postgresql", kw


def _connect(dsn, driver, kwargs):
    if driver == "sqlite":
        import sqlite3
        return sqlite3.connect(dsn), "?"
    if driver == "mysql":
        try:
            import pymysql
            conn = pymysql.connect(**(kwargs or {}), cursorclass=pymysql.cursors.DictCursor)
            return conn, "%s"
        except ImportError:
            raise RuntimeError(
                "MySQL support requires pymysql. Install it with: pip install pymysql"
            )
    # postgresql (default)
    import psycopg2
    import psycopg2.extras
    if dsn:
        conn = psycopg2.connect(dsn)
    else:
        conn = psycopg2.connect(**kwargs)
    return conn, "%s"


def _normalize_params(raw: str, context: dict, creds: dict):
    """Render then JSON-parse the params field into a list or tuple."""
    if not raw or not raw.strip():
        return []
    rendered = _render(raw.strip(), context, creds)
    if not rendered or rendered == raw.strip():
        # Try direct parse
        pass
    try:
        val = json.loads(rendered)
        if isinstance(val, (list, tuple)):
            return list(val)
        if isinstance(val, dict):
            return val   # named params (psycopg2 supports %(name)s)
        return [val]
    except (JSONDecodeError, TypeError):
        return []


def _rows_to_dicts(cursor, driver: str):
    """Fetch all rows and return (columns, rows_as_dicts)."""
    if driver == "mysql":
        # DictCursor already returns dicts
        rows = cursor.fetchall() or []
        cols = list(rows[0].keys()) if rows else []
        return cols, [dict(r) for r in rows]

    if driver == "sqlite":
        cols = [d[0] for d in (cursor.description or [])]
        rows = cursor.fetchall() or []
        return cols, [dict(zip(cols, row)) for row in rows]

    # psycopg2 — RealDictCursor not used here; we get regular tuples
    cols = [d[0] for d in (cursor.description or [])]
    rows = cursor.fetchall() or []
    return cols, [dict(zip(cols, row)) for row in rows]


# ── Node entry point ──────────────────────────────────────────────────────────

def run(config: dict, inp: dict, context: dict, logger, creds=None, **kwargs) -> dict:
    creds = creds or {}
    logger.info("[action.postgres] firing node")

    # Resolve credential
    cred_name = _render(config.get("credential", ""), context, creds)
    raw_cred  = _resolve_cred_raw(cred_name, creds)
    try:
        cred = json.loads(raw_cred) if raw_cred else {}
    except (JSONDecodeError, TypeError):
        cred = {}

    # Allow DSN to be set directly in config as a fallback
    if not cred and config.get("dsn"):
        cred = {"dsn": _render(config["dsn"], context, creds)}

    if not cred:
        raise ValueError(
            "action.postgres: no credential configured. "
            "Set 'credential' to a credential name whose value is a JSON object "
            "with a 'dsn' key (or host/port/username/password/database fields)."
        )

    dsn, driver, connect_kwargs = _build_dsn(cred)

    query = _render(config.get("query", ""), context, creds).strip()
    if not query:
        raise ValueError("action.postgres: 'query' is required")

    params      = _normalize_params(config.get("params", ""), context, creds)
    try: row_limit = int(_render(str(config.get("row_limit", "1000")), context, creds))
    except (ValueError, TypeError): row_limit = 1000

    logger.info("[action.postgres] driver=%s query=%s", driver, query[:80] + ('…' if len(query)>80 else ''))

    # ── SSRF: resolve hostname before connecting ───────────────────────────
    if driver in ("postgresql", "mysql") and dsn:
        _host = _dsn_host(dsn)
        if _host:
            _check_ssrf(_host, connect_kwargs.get("port", 5432 if driver == "postgresql" else 3306))
    elif driver in ("postgresql", "mysql") and not dsn:
        _check_ssrf(connect_kwargs.get("host", "localhost"),
                    connect_kwargs.get("port", 5432 if driver == "postgresql" else 3306))

    conn = _connect(dsn, driver, connect_kwargs)
    db_conn, placeholder = conn

    try:
        if driver == "sqlite":
            cur = db_conn.cursor()
        elif driver == "mysql":
            cur = db_conn.cursor()
        else:
            cur = db_conn.cursor()

        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)

        affected = cur.rowcount if cur.rowcount is not None else 0

        columns: list = []
        rows: list    = []

        if cur.description:
            columns, rows = _rows_to_dicts(cur, driver)
            if row_limit and len(rows) > row_limit:
                rows = rows[:row_limit]
                logger.info("[action.postgres] result truncated to %s rows", row_limit)

        # Commit for write statements (psycopg2/pymysql are not autocommit by default)
        db_conn.commit()

        logger.info("[action.postgres] rows=%s affected=%s", len(rows), affected)
        return {
            "rows":     rows,
            "count":    len(rows),
            "columns":  columns,
            "row":      rows[0] if rows else {},
            "affected": affected,
        }
    except psycopg2.Error as exc:
        try:
            db_conn.rollback()
        except (AttributeError, TypeError, RuntimeError):
            pass
        raise RuntimeError(f"action.postgres: query failed — {exc}") from exc
    finally:
        if db_conn is not None:
            try:
                db_conn.close()
            except (AttributeError, TypeError, RuntimeError):
                pass
