"""Database layer.

Schema management is handled by Alembic (see migrations/).

IMPORTANT — RealDictCursor:
  Most queries use cursor_factory=psycopg2.extras.RealDictCursor so rows are
  returned as dicts.  Always access columns by name: row["id"], not row[0].
  When you need a scalar (e.g. COUNT), use an alias and fetch by name:
      cur.execute("SELECT COUNT(*) AS n FROM runs")
      n = cur.fetchone()["n"]   # correct
      n = cur.fetchone()[0]     # WRONG — raises TypeError with RealDictCursor
"""
import os
import json
import logging
import threading
from contextlib import contextmanager
from json import JSONDecodeError
import psycopg2
import psycopg2.extras
import psycopg2.pool

log = logging.getLogger(__name__)
# NOTE: DSN is read at module-import time so that Alembic and any code that
# references DSN directly still gets the right value.  The connection pool
# reads DATABASE_URL lazily at first-use time so that load_secrets() can
# inject the value before the first real DB call is made.
DSN = os.environ.get("DATABASE_URL", "postgresql://hiverunr:hiverunr@db:5432/hiverunr")

# ── Connection pool ───────────────────────────────────────────────────────────
# ThreadedConnectionPool is safe for use in multi-threaded processes (FastAPI
# workers, Celery workers, APScheduler threads).  Connections are checked-out
# via get_conn() and returned to the pool on context-manager exit.
#
# Tuning env vars (all optional, safe to leave at defaults):
#   DB_POOL_MIN  — keep-alive connections (default 2)
#   DB_POOL_MAX  — hard cap on concurrent connections (default 10)
_pool_lock: threading.Lock = threading.Lock()
_pool: psycopg2.pool.ThreadedConnectionPool | None = None


def _get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    """Lazily create the pool on first DB call (double-checked locking)."""
    global _pool
    if _pool is not None:
        return _pool
    with _pool_lock:
        if _pool is not None:
            return _pool
        dsn     = os.environ.get("DATABASE_URL", DSN)
        min_c   = int(os.environ.get("DB_POOL_MIN", "2"))
        max_c   = int(os.environ.get("DB_POOL_MAX", "10"))
        _pool   = psycopg2.pool.ThreadedConnectionPool(min_c, max_c, dsn)
        host    = dsn.split("@")[-1] if "@" in dsn else dsn
        log.info("DB connection pool initialised (min=%d max=%d host=%s)", min_c, max_c, host)
    return _pool


def get_pool_stats() -> dict:
    """Return current pool utilisation — used by /api/system/status.

    Returns an empty dict if the pool has not been initialised yet (i.e. no
    DB call has been made since startup).
    """
    if _pool is None:
        return {}
    with _pool_lock:
        if _pool is None:
            return {}
        free   = len(_pool._pool)               # type: ignore[attr-defined]
        in_use = len(_pool._used)               # type: ignore[attr-defined]
        return {
            "pool_min":       _pool.minconn,
            "pool_max":       _pool.maxconn,
            "pool_size":      free + in_use,    # total allocated connections
            "pool_available": free,             # idle, ready to be checked out
            "pool_in_use":    in_use,           # currently checked out
        }


def decode_json_value(value, fallback):
    """Coerce a JSON/JSONB-ish DB value into a Python object.

    psycopg2 may return JSONB columns as already-decoded objects or as raw
    strings depending on driver/config. This helper keeps callers consistent
    and fail-safe.
    """
    if value is None:
        return fallback
    if isinstance(value, str):
        try:
            return json.loads(value)
        except JSONDecodeError:
            return fallback
    return value


def decode_run_row(row):
    """Normalise run JSON fields (`traces`, `result`, `initial_payload`)."""
    d = dict(row)
    d["traces"] = decode_json_value(d.get("traces"), [])
    d["result"] = decode_json_value(d.get("result"), {})
    d["initial_payload"] = decode_json_value(d.get("initial_payload"), {})
    return d


@contextmanager
def get_conn():
    """Check out a connection from the pool; return it when the block exits.

    NOTE: autocommit=True means every statement is its own transaction.
    For multi-statement operations that must be atomic, set conn.autocommit=False
    and call conn.commit() / conn.rollback() explicitly.

    If an OperationalError or InterfaceError is raised (broken connection,
    DB restart, TCP timeout), the bad connection is discarded from the pool
    so subsequent callers get a fresh one.
    """
    pool = _get_pool()
    conn = pool.getconn()
    conn.autocommit = True
    close_conn = False
    try:
        yield conn
    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        # Broken connection — discard rather than returning to pool
        close_conn = True
        raise
    finally:
        try:
            pool.putconn(conn, close=close_conn)
        except (AttributeError, ValueError, RuntimeError):
            pass

psycopg2.extras.register_uuid()

def run_migrations() -> None:
    """Apply all pending Alembic migrations (runs `alembic upgrade head`).

    Called at startup from main.py, worker.py, and scheduler.py in place of
    the legacy init_db().  Safe to call concurrently — Alembic's migration
    lock prevents duplicate execution.

    Falls back to the legacy CREATE TABLE IF NOT EXISTS approach if alembic is
    not installed or alembic.ini cannot be found (e.g. image not yet rebuilt).
    """
    import os as _os
    try:
        from alembic.config import Config as _Cfg
        from alembic import command as _cmd
    except ImportError:
        log.warning("alembic not installed — falling back to legacy init_db")
        _init_db_legacy()
        return

    # Resolve alembic.ini: try CWD first (Docker WORKDIR /app), then relative
    # to this file (two levels up = repo root)
    _here = _os.path.dirname(_os.path.abspath(__file__))
    _candidates = [
        _os.path.join(_os.getcwd(), "alembic.ini"),
        _os.path.join(_here, "..", "..", "alembic.ini"),
    ]
    _ini = next((p for p in _candidates if _os.path.isfile(p)), None)
    if _ini is None:
        log.warning("alembic.ini not found — falling back to legacy init_db")
        _init_db_legacy()
        return

    cfg = _Cfg(_ini)
    # Always override with the live DATABASE_URL so Docker env vars take effect
    cfg.set_main_option("sqlalchemy.url", DSN)
    _cmd.upgrade(cfg, "head")


def init_db():
    """Legacy wrapper — calls run_migrations().  Kept for back-compat."""
    run_migrations()


def _init_db_legacy():
    """Original CREATE TABLE IF NOT EXISTS implementation — kept for reference only."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS runs (
                id              SERIAL PRIMARY KEY,
                task_id         TEXT,
                graph_id        INTEGER,
                workflow        TEXT,
                status          TEXT DEFAULT 'queued',
                result          JSONB DEFAULT '{}',
                traces          JSONB DEFAULT '[]',
                initial_payload JSONB DEFAULT '{}',
                created_at      TIMESTAMPTZ DEFAULT NOW(),
                updated_at      TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        # idempotent column additions for upgrades from any prior version
        for col, defn in [
            ("result",          "JSONB DEFAULT '{}'"),
            ("traces",          "JSONB DEFAULT '[]'"),
            ("initial_payload", "JSONB DEFAULT '{}'"),
            ("created_at",      "TIMESTAMPTZ DEFAULT NOW()"),
            ("updated_at",      "TIMESTAMPTZ DEFAULT NOW()"),
        ]:
            cur.execute(f"ALTER TABLE runs ADD COLUMN IF NOT EXISTS {col} {defn}")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS workflows (
                id      SERIAL PRIMARY KEY,
                name    TEXT UNIQUE NOT NULL,
                enabled BOOLEAN DEFAULT TRUE
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS schedules (
                id       SERIAL PRIMARY KEY,
                name     TEXT NOT NULL,
                workflow TEXT,
                graph_id INTEGER,
                cron     TEXT,
                payload  JSONB DEFAULT '{}',
                timezone TEXT DEFAULT 'UTC',
                enabled  BOOLEAN DEFAULT TRUE,
                run_at   TIMESTAMPTZ DEFAULT NULL
            )
        """)
        cur.execute("ALTER TABLE schedules ALTER COLUMN cron DROP NOT NULL")
        cur.execute("ALTER TABLE schedules ADD COLUMN IF NOT EXISTS run_at TIMESTAMPTZ DEFAULT NULL")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS graph_workflows (
                id            SERIAL PRIMARY KEY,
                name          TEXT NOT NULL,
                description   TEXT DEFAULT '',
                graph_json    TEXT DEFAULT '{}',
                enabled       BOOLEAN DEFAULT TRUE,
                webhook_token TEXT DEFAULT md5(random()::text),
                slug          VARCHAR(12) UNIQUE,
                created_at    TIMESTAMPTZ DEFAULT NOW(),
                updated_at    TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        # ── v7 new tables ──────────────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS credentials (
                id         SERIAL PRIMARY KEY,
                name       TEXT UNIQUE NOT NULL,
                type       TEXT DEFAULT 'generic',
                secret     TEXT NOT NULL,
                note       TEXT DEFAULT '',
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS graph_versions (
                id         SERIAL PRIMARY KEY,
                graph_id   INTEGER NOT NULL,
                version    INTEGER NOT NULL,
                name       TEXT NOT NULL,
                graph_json TEXT NOT NULL,
                note       TEXT DEFAULT '',
                saved_at   TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        # ── graph_workflows column migrations (idempotent for older installs)
        for col, defn in [
            ("description",   "TEXT DEFAULT ''"),
            ("graph_json",     "TEXT DEFAULT '{}'"),
            ("enabled",        "BOOLEAN DEFAULT TRUE"),
            ("webhook_token",  "TEXT DEFAULT md5(random()::text)"),
            ("created_at",     "TIMESTAMPTZ DEFAULT NOW()"),
            ("updated_at",     "TIMESTAMPTZ DEFAULT NOW()"),
        ]:
            cur.execute(f"ALTER TABLE graph_workflows ADD COLUMN IF NOT EXISTS {col} {defn}")
        # ── slug migration: add column + backfill existing rows ────────
        cur.execute("ALTER TABLE graph_workflows ADD COLUMN IF NOT EXISTS slug VARCHAR(12) UNIQUE")
        cur.execute("SELECT id FROM graph_workflows WHERE slug IS NULL")
        rows_needing_slug = cur.fetchall()
        if rows_needing_slug:
            import secrets as _sec
            for (rid,) in rows_needing_slug:
                while True:
                    candidate = _sec.token_hex(4)
                    cur.execute("SELECT 1 FROM graph_workflows WHERE slug=%s", (candidate,))
                    if not cur.fetchone():
                        break
                cur.execute("UPDATE graph_workflows SET slug=%s WHERE id=%s", (candidate, rid))

        # ── users + sessions (v12 auth) ────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id            SERIAL PRIMARY KEY,
                username      TEXT UNIQUE NOT NULL,
                email         TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role          TEXT DEFAULT 'owner',
                created_at    TIMESTAMPTZ DEFAULT NOW(),
                updated_at    TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                id         SERIAL PRIMARY KEY,
                user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                token_hash TEXT UNIQUE NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                last_seen  TIMESTAMPTZ DEFAULT NOW(),
                expires_at TIMESTAMPTZ DEFAULT NOW() + INTERVAL '30 days'
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS api_tokens (
                id         SERIAL PRIMARY KEY,
                name       TEXT NOT NULL,
                token_hash TEXT UNIQUE NOT NULL,
                created_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                last_used  TIMESTAMPTZ,
                scope      TEXT NOT NULL DEFAULT 'manage',
                expires_at TIMESTAMPTZ DEFAULT NULL
            )
        """)

# ── runs ──────────────────────────────────────────────────────────────────
def list_runs(page: int = 1, page_size: int = 50,
              status: str = None, flow_id: int = None, q: str = None,
              workspace_id: int | None = None):
    """Return a page of runs with optional filtering.

    Args:
        page:      1-based page number.
        page_size: rows per page (max 200).
        status:    filter to a single status string (e.g. "failed").
        flow_id:   filter to a specific graph_workflows.id.
        q:         case-insensitive substring match on flow name or task_id.

    Returns:
        dict with keys: runs (list), total (int), page (int), pages (int).
    """
    page      = max(1, int(page))
    page_size = max(1, min(200, int(page_size)))
    offset    = (page - 1) * page_size

    conditions = []
    params: list = []

    if status:
        conditions.append("r.status = %s")
        params.append(status)
    if flow_id:
        conditions.append("r.graph_id = %s")
        params.append(int(flow_id))
    if q:
        conditions.append(
            "(COALESCE(g.name, r.workflow) ILIKE %s OR r.task_id ILIKE %s OR CAST(r.id AS TEXT) = %s)"
        )
        like = f"%{q}%"
        params.extend([like, like, q.strip()])
    if workspace_id is not None:
        # Include runs with NULL workspace_id so pre-workspace-migration data
        # (scripts, replays, webhooks created before the workspace_id fix)
        # remains visible rather than permanently disappearing.
        conditions.append("(r.workspace_id = %s OR r.workspace_id IS NULL)")
        params.append(workspace_id)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    base_query = f"""
        FROM runs r
        LEFT JOIN graph_workflows g ON r.graph_id = g.id
        {where}
    """

    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute(f"SELECT COUNT(*) AS n {base_query}", params)
        total = cur.fetchone()["n"]

        cur.execute(
            f"""SELECT r.*,
                    COALESCE(
                        g.name,
                        INITCAP(REPLACE(REPLACE(REPLACE(REPLACE(
                            SUBSTRING(r.workflow FROM '[^/]+$'),
                            '_', ' '), '__', '_'), '.py', ''), '.', ' '))
                    ) AS flow_name
                {base_query}
                ORDER BY r.id DESC
                LIMIT %s OFFSET %s""",
            params + [page_size, offset],
        )
        runs = []
        for r in cur.fetchall():
            runs.append(decode_run_row(r))

    pages = max(1, (total + page_size - 1) // page_size)
    return {"runs": runs, "total": total, "page": page, "pages": pages}

def get_run_by_task(task_id):
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """SELECT r.*,
                    COALESCE(
                        g.name,
                        INITCAP(REPLACE(REPLACE(REPLACE(REPLACE(
                            SUBSTRING(r.workflow FROM '[^/]+$'),
                            '_', ' '), '__', '_'), '.py', ''), '.', ' '))
                    ) AS flow_name
               FROM runs r
               LEFT JOIN graph_workflows g ON r.graph_id = g.id
               WHERE r.task_id=%s""",
            (task_id,)
        )
        row = cur.fetchone()
        if not row:
            return None
        return decode_run_row(row)

def update_run(task_id, status, result=None, traces=None, retry_count: int | None = None):
    """Update a run's status, result, traces, and optionally its retry_count.

    retry_count is only written when explicitly passed (not None) so callers
    that don't use retries don't accidentally reset the counter.
    """
    with get_conn() as conn:
        if retry_count is not None:
            conn.cursor().execute(
                "UPDATE runs SET status=%s, result=%s, traces=%s, retry_count=%s, updated_at=NOW() WHERE task_id=%s",
                (status, json.dumps(result or {}), json.dumps(traces or []), retry_count, task_id),
            )
        else:
            conn.cursor().execute(
                "UPDATE runs SET status=%s, result=%s, traces=%s, updated_at=NOW() WHERE task_id=%s",
                (status, json.dumps(result or {}), json.dumps(traces or []), task_id),
            )

def set_run_note(run_id: int, note: str | None):
    with get_conn() as conn:
        conn.cursor().execute(
            "UPDATE runs SET note=%s WHERE id=%s",
            (note or None, run_id),
        )

def delete_run(run_id):
    with get_conn() as conn:
        conn.cursor().execute("DELETE FROM runs WHERE id=%s", (run_id,))

def bulk_delete_runs(ids: list, workspace_id: int | None = None) -> int:
    """Delete multiple runs by ID list. Returns the number actually deleted."""
    if not ids:
        return 0
    with get_conn() as conn:
        cur = conn.cursor()
        if workspace_id is not None:
            cur.execute(
                "DELETE FROM runs WHERE id = ANY(%s) AND workspace_id = %s",
                (list(ids), workspace_id),
            )
        else:
            cur.execute(
                "DELETE FROM runs WHERE id = ANY(%s)",
                (list(ids),),
            )
        return cur.rowcount

def clear_runs(workspace_id: int | None = None):
    if workspace_id is not None:
        with get_conn() as conn:
            conn.cursor().execute("DELETE FROM runs WHERE workspace_id = %s", (workspace_id,))
    else:
        with get_conn() as conn:
            conn.cursor().execute("DELETE FROM runs")

# ── workflows ─────────────────────────────────────────────────────────────
def list_workflows():
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM workflows ORDER BY id")
        return [dict(r) for r in cur.fetchall()]

def upsert_workflow(name):
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "INSERT INTO workflows(name) VALUES(%s) ON CONFLICT(name) DO NOTHING RETURNING *",
            (name,)
        )
        row = cur.fetchone()
        if not row:
            cur.execute("SELECT * FROM workflows WHERE name=%s", (name,))
            row = cur.fetchone()
        return dict(row) if row else None

def toggle_workflow(name):
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("UPDATE workflows SET enabled=NOT enabled WHERE name=%s RETURNING *", (name,))
        row = cur.fetchone()
        return dict(row) if row else None

# ── schedules ─────────────────────────────────────────────────────────────
def list_schedules(workspace_id: int | None = None):
    """Return all schedules enriched with the graph name and last-run info."""
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        ws_filter = "WHERE s.workspace_id = %(wid)s" if workspace_id is not None else ""
        cur.execute(f"""
            SELECT
                s.*,
                g.name AS graph_name,
                lr.id              AS last_run_id,
                lr.status          AS last_run_status,
                lr.created_at      AS last_run_at,
                lr.duration_ms     AS last_run_duration_ms
            FROM schedules s
            LEFT JOIN graph_workflows g ON g.id = s.graph_id
            LEFT JOIN LATERAL (
                SELECT
                    id,
                    status,
                    created_at,
                    GREATEST(
                        ROUND(EXTRACT(EPOCH FROM (updated_at - created_at)) * 1000),
                        0
                    )::int AS duration_ms
                FROM   runs
                WHERE  graph_id = s.graph_id
                  AND  s.graph_id IS NOT NULL
                ORDER  BY created_at DESC
                LIMIT  1
            ) lr ON TRUE
            {ws_filter}
            ORDER BY s.id
        """, {"wid": workspace_id} if workspace_id is not None else {})
        return [dict(r) for r in cur.fetchall()]

def get_schedule(sid):
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM schedules WHERE id=%s", (sid,))
        row = cur.fetchone()
        return dict(row) if row else None

def create_schedule(
    name,
    workflow=None,
    graph_id=None,
    cron=None,
    payload=None,
    timezone="UTC",
    run_at=None,
    workspace_id: int | None = None,
):
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "INSERT INTO schedules("
            "(name,workflow,graph_id,cron,payload,timezone,run_at,workspace_id) "
            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
            (name, workflow, graph_id, cron, json.dumps(payload or {}), timezone, run_at, workspace_id),
        )
        return dict(cur.fetchone())

def update_schedule(
    sid,
    name,
    workflow,
    graph_id,
    cron,
    payload,
    timezone,
    run_at=None,
    workspace_id: int | None = None,
):
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "UPDATE schedules SET "
            "name=%s,workspace=%s,graph_id=%s,cron=%s,payload=%s,timezone=%s,run_at=%s,workspace_id=%s "
            "WHERE id=%s RETURNING *",
            (name, workflow, graph_id, cron, json.dumps(payload or {}), timezone, run_at, workspace_id, sid),
        )
        row = cur.fetchone()
        return dict(row) if row else None

def toggle_schedule(sid):
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("UPDATE schedules SET enabled=NOT enabled WHERE id=%s RETURNING *", (sid,))
        row = cur.fetchone()
        return dict(row) if row else None

def delete_schedule(sid):
    with get_conn() as conn:
        conn.cursor().execute("DELETE FROM schedules WHERE id=%s", (sid,))

def sync_graph_schedules(graph_id: int, cron_nodes: list):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM schedules WHERE graph_id=%s", (graph_id,))
        for node in cron_nodes:
            cfg  = node.get('data', {}).get('config', {})
            cron = (cfg.get('cron') or '0 9 * * *').strip()
            tz   = (cfg.get('timezone') or 'UTC').strip() or 'UTC'
            name = (node.get('data', {}).get('label') or 'Cron Trigger').strip()
            cur.execute(
                "INSERT INTO schedules(name,graph_id,cron,timezone,payload,enabled) VALUES(%s,%s,%s,%s,%s,%s)",
                (name, graph_id, cron, tz, json.dumps({}), True)
            )

# ── graph_workflows ───────────────────────────────────────────────────────
def list_graphs(workspace_id: int | None = None, page: int = 1, page_size: int = 50):
    """List graphs for a workspace with pagination. page is 1-indexed."""
    offset = (max(1, page) - 1) * min(max(1, page_size), 100)
    limit = min(max(1, page_size), 100)
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if workspace_id is not None:
            cur.execute(
                "SELECT * FROM graph_workflows WHERE workspace_id=%s ORDER BY id LIMIT %s OFFSET %s",
                (workspace_id, limit, offset),
            )
        else:
            cur.execute("SELECT * FROM graph_workflows ORDER BY id LIMIT %s OFFSET %s",
                        (limit, offset))
        return [dict(r) for r in cur.fetchall()]  # fetchall for small page sizes; offset/limit in query


def count_graphs(workspace_id: int | None = None) -> int:
    """Return total graph count for a workspace (for pagination metadata)."""
    with get_conn() as conn:
        cur = conn.cursor()
        if workspace_id is not None:
            cur.execute("SELECT COUNT(*) FROM graph_workflows WHERE workspace_id=%s",
                        (workspace_id,))
        else:
            cur.execute("SELECT COUNT(*) FROM graph_workflows")
        return cur.fetchone()[0] or 0

def create_graph(name, description, graph_json, workspace_id: int | None = None, tags: list | None = None):
    import secrets as _sec
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        # Generate a unique 8-char hex slug
        while True:
            slug = _sec.token_hex(4)
            cur.execute("SELECT 1 FROM graph_workflows WHERE slug=%s", (slug,))
            if not cur.fetchone():
                break
        cur.execute(
            "INSERT INTO graph_workflows("
            "VALUES(%s,%s,%s,%s,%s,%s) RETURNING *",
            (name, description, graph_json, slug, workspace_id, tags or []),
        )
        return dict(cur.fetchone())

def get_graph(graph_id):
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM graph_workflows WHERE id=%s", (graph_id,))
        row = cur.fetchone()
        return dict(row) if row else None

def get_graph_by_slug(slug, workspace_id=None):
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if workspace_id is not None:
            cur.execute("SELECT * FROM graph_workflows WHERE slug=%s AND workspace_id=%s", (slug, workspace_id))
        else:
            cur.execute("SELECT * FROM graph_workflows WHERE slug=%s", (slug,))
        row = cur.fetchone()
        return dict(row) if row else None

def get_graph_by_name(name):
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM graph_workflows WHERE name=%s", (name,))
        row = cur.fetchone()
        return dict(row) if row else None

def get_graph_by_token(token):
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM graph_workflows WHERE webhook_token=%s", (token,))
        row = cur.fetchone()
        return dict(row) if row else None

def update_graph(
    graph_id,
    name=None,
    description=None,
    graph_json=None,
    enabled=None,
    tags=None,
    priority=None,
    pinned=None,
):
    with get_conn() as conn:
        cur = conn.cursor()
        if name is not None:
            cur.execute(
                "UPDATE graph_workflows SET name=%s, updated_at=NOW() WHERE id=%s",
                (name, graph_id),
            )
        if description is not None:
            cur.execute(
                "UPDATE graph_workflows SET description=%s, updated_at=NOW() WHERE id=%s",
                (description, graph_id),
            )
        if graph_json is not None:
            cur.execute(
                "UPDATE graph_workflows SET graph_json=%s, updated_at=NOW() WHERE id=%s",
                (graph_json, graph_id),
            )
        if enabled is not None:
            cur.execute(
                "UPDATE graph_workflows SET enabled=%s, updated_at=NOW() WHERE id=%s",
                (enabled, graph_id),
            )
        if tags is not None:
            cur.execute(
                "UPDATE graph_workflows SET tags=%s, updated_at=NOW() WHERE id=%s",
                (tags, graph_id),
            )
        if priority is not None:
            cur.execute(
                "UPDATE graph_workflows SET priority=%s, updated_at=NOW() WHERE id=%s",
                (int(priority), graph_id),
            )
        if pinned is not None:
            cur.execute(
                "UPDATE graph_workflows SET pinned=%s, updated_at=NOW() WHERE id=%s",
                (bool(pinned), graph_id),
            )

def duplicate_graph(graph_id: int) -> dict:
    """Clone a graph, appending ' (copy)' to the name and generating a fresh slug."""
    src = get_graph(graph_id)
    if not src:
        raise ValueError(f"Graph {graph_id} not found")
    import re as _re
    base = _re.sub(r'\s*\(copy(?:\s+\d+)?\)\s*$', '', src['name']).strip()
    # Find a unique name: "Name (copy)", "Name (copy 2)", …
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        candidate = f"{base} (copy)"
        cur.execute("SELECT 1 FROM graph_workflows WHERE name=%s", (candidate,))
        n = 2
        while cur.fetchone():
            candidate = f"{base} (copy {n})"
            cur.execute("SELECT 1 FROM graph_workflows WHERE name=%s", (candidate,))
            n += 1
    return create_graph(candidate, src.get('description', ''), src.get('graph_json') or '{}', workspace_id=src.get('workspace_id'))


def delete_graph(graph_id):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM schedules      WHERE graph_id=%s", (graph_id,))
        cur.execute("DELETE FROM graph_versions WHERE graph_id=%s", (graph_id,))
        cur.execute("DELETE FROM graph_workflows WHERE id=%s",      (graph_id,))

# ── credentials ───────────────────────────────────────────────────────────
def list_credentials(workspace_id: int | None = None):
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        # never return secret values in the list
        if workspace_id is not None:
            cur.execute(
                "SELECT id, name, type, note, created_at, updated_at FROM credentials "
                "WHERE workspace_id=%s ORDER BY name",
                (workspace_id,),
            )
        else:
            cur.execute("SELECT id, name, type, note, created_at, updated_at FROM credentials ORDER BY name")
        return [dict(r) for r in cur.fetchall()]

def get_credential_secret(name):
    """Fetch the decrypted secret for a named credential. Used by executor only."""
    from app.crypto import decrypt
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT secret FROM credentials WHERE name=%s", (name,))
        row = cur.fetchone()
        if row is None:
            return None
        return decrypt(row['secret'])

def load_all_credentials(workspace_id: int | None = None):
    """Return name→decrypted-secret mapping. Called once per graph run.

    When workspace_id is supplied only credentials from that workspace are
    loaded, so graphs cannot accidentally access secrets from other workspaces.
    Falls back to all credentials if workspace_id is None (legacy path).
    """
    from app.crypto import decrypt
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if workspace_id is not None:
            cur.execute(
                "SELECT name, secret FROM credentials WHERE workspace_id=%s",
                (workspace_id,),
            )
        else:
            cur.execute("SELECT name, secret FROM credentials")
        return {r['name']: decrypt(r['secret']) for r in cur.fetchall()}

def upsert_credential(name, type_, secret, note="", workspace_id: int | None = None):
    if workspace_id is None:
        raise ValueError("workspace_id is required — cannot create credentials without a workspace")
    from app.crypto import encrypt, encryption_configured
    if not encryption_configured():
        raise RuntimeError(
            "SECRET_KEY is not set — cannot create or update credentials. "
            "Set SECRET_KEY in .env before storing credentials. "
            "Generate one with: python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
        )
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            INSERT INTO credentials(name, type, secret, note, workspace_id)
            VALUES(%s, %s, %s, %s, %s)
            ON CONFLICT(name) DO UPDATE
              SET type=EXCLUDED.type, secret=EXCLUDED.secret,
                  note=EXCLUDED.note, workspace_id=EXCLUDED.workspace_id, updated_at=NOW()
            RETURNING id, name, type, note, created_at, updated_at
        """, (name, type_, encrypt(secret), note, workspace_id))
        return dict(cur.fetchone())

def update_credential(cred_id, type_, secret, note, workspace_id: int | None = None):
    if workspace_id is None:
        raise ValueError("workspace_id is required — cannot update credentials without a workspace")
    from app.crypto import encrypt, encryption_configured
    if not encryption_configured():
        raise RuntimeError(
            "SECRET_KEY is not set — cannot create or update credentials. "
            "Set SECRET_KEY in .env before storing credentials. "
            "Generate one with: python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
        )
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if workspace_id is not None:
            # Verify ownership before mutating
            cur.execute("SELECT id FROM credentials WHERE id=%s AND workspace_id=%s", (cred_id, workspace_id))
            if cur.fetchone() is None:
                return None  # Credential not found in this workspace
        if secret:
            cur.execute(
                "UPDATE credentials SET type=%s, secret=%s, note=%s, updated_at=NOW() WHERE id=%s RETURNING id, name, type, note, created_at, updated_at",
                (type_, encrypt(secret), note, cred_id)
            )
        else:
            cur.execute(
                "UPDATE credentials SET type=%s, note=%s, updated_at=NOW() WHERE id=%s RETURNING id, name, type, note, created_at, updated_at",
                (type_, note, cred_id)
            )
        return dict(cur.fetchone())

def delete_credential(cred_id, workspace_id: int | None = None):
    if workspace_id is None:
        raise ValueError("workspace_id is required — cannot delete credentials without a workspace")
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("DELETE FROM credentials WHERE id=%s AND workspace_id=%s RETURNING id", (cred_id, workspace_id))

# ── graph_versions ────────────────────────────────────────────────────────
def list_graph_versions(graph_id):
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT id, graph_id, version, name, note, saved_at, graph_json FROM graph_versions WHERE graph_id=%s ORDER BY version DESC LIMIT 20",
            (graph_id,)
        )
        return [dict(r) for r in cur.fetchall()]

def save_graph_version(graph_id, name, graph_json, note=""):
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT COALESCE(MAX(version),0)+1 AS nxt FROM graph_versions WHERE graph_id=%s",
            (graph_id,)
        )
        nxt = cur.fetchone()['nxt']
        cur.execute(
            "INSERT INTO graph_versions(graph_id,version,name,graph_json,note) VALUES(%s,%s,%s,%s,%s) RETURNING *",
            (graph_id, nxt, name, graph_json, note)
        )
        row = dict(cur.fetchone())
        # keep only the 20 most recent per graph
        cur.execute("""
            DELETE FROM graph_versions WHERE graph_id=%s AND id NOT IN (
                SELECT id FROM graph_versions WHERE graph_id=%s ORDER BY version DESC LIMIT 20
            )
        """, (graph_id, graph_id))
        return row

def get_graph_version(version_id):
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM graph_versions WHERE id=%s", (version_id,))
        row = cur.fetchone()
        return dict(row) if row else None

# ── metrics ───────────────────────────────────────────────────────────────
def get_run_metrics(workspace_id: int | None = None):
    ws_filter = "(r.workspace_id = %s OR r.workspace_id IS NULL)" if workspace_id is not None else "TRUE"
    ws_params = [workspace_id] if workspace_id is not None else []
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # 30-day summary — alias runs as r so ws_filter can use r.workspace_id
        cur.execute(f"""
            SELECT
                COUNT(*)                                                           AS total,
                SUM(CASE WHEN r.status='succeeded' THEN 1 ELSE 0 END)             AS succeeded,
                SUM(CASE WHEN r.status='failed'    THEN 1 ELSE 0 END)             AS failed,
                ROUND(AVG(EXTRACT(EPOCH FROM (r.updated_at - r.created_at))*1000)) AS avg_ms
            FROM runs r
            WHERE r.created_at >= NOW() - INTERVAL '30 days'
              AND {ws_filter}
        """, ws_params)
        s = dict(cur.fetchone())

        # Daily counts for the last 14 days (fill gaps with zeros)
        cur.execute(f"""
            WITH days AS (
                SELECT generate_series(
                    CURRENT_DATE - 13, CURRENT_DATE, '1 day'::interval
                )::date AS day
            )
            SELECT days.day::text,
                   COALESCE(SUM(CASE WHEN r.status='succeeded' THEN 1 ELSE 0 END), 0) AS succeeded,
                   COALESCE(SUM(CASE WHEN r.status='failed'    THEN 1 ELSE 0 END), 0) AS failed,
                   COALESCE(COUNT(r.id), 0) AS total
            FROM days
            LEFT JOIN runs r ON DATE(r.created_at) = days.day
              AND {ws_filter}
            GROUP BY days.day
            ORDER BY days.day
        """, ws_params)
        daily = [
            {'day': r['day'], 'succeeded': int(r['succeeded']), 'failed': int(r['failed']), 'total': int(r['total'])}
            for r in cur.fetchall()
        ]

        # Top 5 failing flows (last 30 days)
        cur.execute(f"""
            SELECT COALESCE(g.name, 'legacy:' || r.workflow) AS name,
                   COUNT(*) AS failures
            FROM runs r
            LEFT JOIN graph_workflows g ON r.graph_id = g.id
            WHERE r.status = 'failed'
              AND r.created_at >= NOW() - INTERVAL '30 days'
              AND {ws_filter}
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 5
        """, ws_params)
        top_failing = [{'name': r['name'], 'failures': int(r['failures'])} for r in cur.fetchall()]

        # Last 10 runs for the activity feed
        cur.execute(f"""
            SELECT r.id, r.status,
                   r.created_at::text AS created_at,
                   COALESCE(g.name, SUBSTRING(r.workflow FROM '[^/]+$'), 'unknown') AS flow_name,
                   GREATEST(ROUND(EXTRACT(EPOCH FROM (r.updated_at - r.created_at))*1000), 0)::int AS duration_ms
            FROM runs r
            LEFT JOIN graph_workflows g ON r.graph_id = g.id
            WHERE {ws_filter}
            ORDER BY r.id DESC LIMIT 10
        """, ws_params)
        recent = [dict(r) for r in cur.fetchall()]

        total     = int(s['total']     or 0)
        succeeded = int(s['succeeded'] or 0)
        return {
            'total':        total,
            'succeeded':    succeeded,
            'failed':       int(s['failed'] or 0),
            'success_rate': round(succeeded / max(total, 1) * 100, 1),
            'avg_ms':       int(s['avg_ms'] or 0),
            'daily':        daily,
            'top_failing':  top_failing,
            'recent':       recent,
        }

# ── analytics ─────────────────────────────────────────────────────────────

def get_flow_analytics(days: int = 30, workspace_id: int | None = None) -> list:
    """Per-flow performance stats for the last N days.

    Returns a list of dicts ordered by total runs descending:
        graph_id, flow_name, total, succeeded, failed, error_rate,
        avg_ms, p95_ms, p99_ms, last_run
    """
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT
                r.graph_id,
                COALESCE(g.name, 'legacy:' || SUBSTRING(r.workflow FROM '[^/]+$'), 'unknown') AS flow_name,
                COUNT(*)                                               AS total,
                SUM(CASE WHEN r.status = 'succeeded' THEN 1 ELSE 0 END) AS succeeded,
                SUM(CASE WHEN r.status = 'failed'    THEN 1 ELSE 0 END) AS failed,
                ROUND(
                    SUM(CASE WHEN r.status = 'failed' THEN 1 ELSE 0 END)::numeric
                    / NULLIF(COUNT(*), 0) * 100, 1
                ) AS error_rate,
                ROUND(AVG(
                    EXTRACT(EPOCH FROM (r.updated_at - r.created_at)) * 1000
                ))::bigint AS avg_ms,
                ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (r.updated_at - r.created_at)) * 1000
                ))::bigint AS p95_ms,
                ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (r.updated_at - r.created_at)) * 1000
                ))::bigint AS p99_ms,
                MAX(r.created_at)::text AS last_run
            FROM runs r
            LEFT JOIN graph_workflows g ON r.graph_id = g.id
            WHERE r.created_at >= NOW() - (%(days)s || ' days')::interval
              AND r.status IN ('succeeded', 'failed')
              AND (%(ws)s::int IS NULL OR r.workspace_id = %(ws)s OR r.workspace_id IS NULL)
            GROUP BY r.graph_id, flow_name
            ORDER BY total DESC
            LIMIT 50
        """, {"days": days, "ws": workspace_id})
        rows = cur.fetchall()
        return [
            {
                "graph_id":   r["graph_id"],
                "flow_name":  r["flow_name"],
                "total":      int(r["total"]),
                "succeeded":  int(r["succeeded"] or 0),
                "failed":     int(r["failed"] or 0),
                "error_rate": float(r["error_rate"] or 0),
                "avg_ms":     int(r["avg_ms"] or 0),
                "p95_ms":     int(r["p95_ms"] or 0),
                "p99_ms":     int(r["p99_ms"] or 0),
                "last_run":   r["last_run"],
            }
            for r in rows
        ]


def get_daily_analytics(days: int = 30, workspace_id: int | None = None) -> list:
    """Daily run volume + average duration for the last N days.

    Returns a list of dicts ordered by day ascending:
        day (YYYY-MM-DD), succeeded, failed, total, avg_ms
    """
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            WITH day_series AS (
                SELECT generate_series(
                    CURRENT_DATE - (%(days)s - 1),
                    CURRENT_DATE,
                    '1 day'::interval
                )::date AS day
            )
            SELECT
                ds.day::text AS day,
                COALESCE(SUM(CASE WHEN r.status='succeeded' THEN 1 ELSE 0 END), 0)::int AS succeeded,
                COALESCE(SUM(CASE WHEN r.status='failed'    THEN 1 ELSE 0 END), 0)::int AS failed,
                COALESCE(COUNT(r.id), 0)::int AS total,
                COALESCE(ROUND(AVG(
                    EXTRACT(EPOCH FROM (r.updated_at - r.created_at)) * 1000
                )), 0)::int AS avg_ms
            FROM day_series ds
            LEFT JOIN runs r
              ON DATE(r.created_at) = ds.day
             AND r.status IN ('succeeded', 'failed')
             AND (%(ws)s::int IS NULL OR r.workspace_id = %(ws)s OR r.workspace_id IS NULL)
            GROUP BY ds.day
            ORDER BY ds.day
        """, {"days": days, "ws": workspace_id})
        return [dict(r) for r in cur.fetchall()]


# ── users ──────────────────────────────────────────────────────────────────
def users_exist() -> bool:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM users LIMIT 1")
        return cur.fetchone() is not None

def create_user(username: str, email: str, password_hash: str, role: str = 'owner') -> dict:
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "INSERT INTO users (username, email, password_hash, role) VALUES (%s, %s, %s, %s) RETURNING *",
            (username, email, password_hash, role)
        )
        return dict(cur.fetchone())

def get_user_by_username(username: str):
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM users WHERE username=%s", (username,))
        row = cur.fetchone()
        return dict(row) if row else None

def get_user_by_id(user_id: int):
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM users WHERE id=%s", (user_id,))
        row = cur.fetchone()
        return dict(row) if row else None

def list_users() -> list:
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT id, username, email, role, created_at FROM users ORDER BY id")
        return [dict(r) for r in cur.fetchall()]

def update_user_password(user_id: int, password_hash: str):
    with get_conn() as conn:
        conn.cursor().execute(
            "UPDATE users SET password_hash=%s, updated_at=NOW() WHERE id=%s",
            (password_hash, user_id)
        )

def update_user_role(user_id: int, role: str):
    with get_conn() as conn:
        conn.cursor().execute(
            "UPDATE users SET role=%s, updated_at=NOW() WHERE id=%s",
            (role, user_id)
        )

def delete_user(user_id: int):
    with get_conn() as conn:
        conn.cursor().execute("DELETE FROM users WHERE id=%s", (user_id,))

# ── sessions ───────────────────────────────────────────────────────────────
def create_session(user_id: int, token_hash: str):
    with get_conn() as conn:
        conn.cursor().execute(
            "INSERT INTO sessions (user_id, token_hash) VALUES (%s, %s)",
            (user_id, token_hash)
        )

def get_session_by_token_hash(token_hash: str):
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT * FROM sessions WHERE token_hash=%s AND expires_at > NOW()",
            (token_hash,)
        )
        row = cur.fetchone()
        return dict(row) if row else None

def refresh_session(token_hash: str):
    with get_conn() as conn:
        conn.cursor().execute(
            "UPDATE sessions SET last_seen=NOW(), expires_at=NOW() + INTERVAL '30 days' WHERE token_hash=%s",
            (token_hash,)
        )

def delete_session_by_token_hash(token_hash: str):
    with get_conn() as conn:
        conn.cursor().execute("DELETE FROM sessions WHERE token_hash=%s", (token_hash,))

def purge_expired_sessions():
    with get_conn() as conn:
        conn.cursor().execute("DELETE FROM sessions WHERE expires_at < NOW()")

# ── api_tokens ─────────────────────────────────────────────────────────────

# Valid scopes in ascending permission order.
API_TOKEN_SCOPES = ("read", "run", "manage")

def create_api_token(name: str, token_hash: str, created_by: int,
                     scope: str = "manage", expires_at=None,
                     workspace_id: int | None = None) -> dict:
    if scope not in API_TOKEN_SCOPES:
        raise ValueError(f"scope must be one of {API_TOKEN_SCOPES}")
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """INSERT INTO api_tokens (name, token_hash, created_by, scope, expires_at, workspace_id)
               VALUES (%s, %s, %s, %s, %s, %s)
               RETURNING id, name, created_at, scope, expires_at""",
            (name, token_hash, created_by, scope, expires_at, workspace_id)
        )
        return dict(cur.fetchone())

def list_api_tokens(workspace_id: int | None = None) -> list:
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if workspace_id is not None:
            cur.execute("""
                SELECT t.id, t.name, t.created_at, t.last_used,
                       t.scope, t.expires_at,
                       u.username AS created_by_username
                FROM api_tokens t LEFT JOIN users u ON t.created_by = u.id
                WHERE t.workspace_id = %s
                ORDER BY t.created_at DESC
            """, (workspace_id,))
        else:
            cur.execute("""
                SELECT t.id, t.name, t.created_at, t.last_used,
                       t.scope, t.expires_at,
                       u.username AS created_by_username
                FROM api_tokens t LEFT JOIN users u ON t.created_by = u.id
                ORDER BY t.created_at DESC
            """)
        return [dict(r) for r in cur.fetchall()]

def get_api_token_by_hash(token_hash: str):
    """Return the token row if it exists and has not expired; else None."""
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT * FROM api_tokens WHERE token_hash=%s"
            " AND (expires_at IS NULL OR expires_at > NOW())",
            (token_hash,),
        )
        row = cur.fetchone()
        return dict(row) if row else None

def touch_api_token(token_hash: str):
    with get_conn() as conn:
        conn.cursor().execute(
            "UPDATE api_tokens SET last_used=NOW() WHERE token_hash=%s", (token_hash,)
        )

def delete_api_token(token_id: int, workspace_id: int | None = None):
    with get_conn() as conn:
        if workspace_id is not None:
            conn.cursor().execute(
                "DELETE FROM api_tokens WHERE id=%s AND workspace_id=%s",
                (token_id, workspace_id),
            )
        else:
            conn.cursor().execute("DELETE FROM api_tokens WHERE id=%s", (token_id,))

# ── Graph alert config ─────────────────────────────────────────────────────────
def get_graph_alerts(graph_id: int):
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT alert_emails, alert_webhook, alert_on_success, "
            "COALESCE(alert_min_failures, 1) AS alert_min_failures "
            "FROM graph_workflows WHERE id=%s",
            (graph_id,)
        )
        row = cur.fetchone()
        return dict(row) if row else None

def update_graph_alerts(graph_id: int, alert_emails: str, alert_webhook: str,
                        alert_on_success: bool, alert_min_failures: int = 1):
    with get_conn() as conn:
        conn.cursor().execute(
            "UPDATE graph_workflows "
            "SET alert_emails=%s, alert_webhook=%s, alert_on_success=%s, alert_min_failures=%s "
            "WHERE id=%s",
            (alert_emails or None, alert_webhook or None,
             bool(alert_on_success), max(1, int(alert_min_failures or 1)),
             graph_id)
        )

def count_trailing_failures(graph_id: int, limit: int = 20) -> int:
    """Return the number of consecutive failed runs at the end of the run history.

    Only considers terminal statuses (succeeded, failed, dead).  Running / queued
    runs are ignored so an in-progress run doesn't reset the streak.
    """
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT status FROM runs
            WHERE graph_id = %s
              AND status IN ('succeeded', 'failed', 'dead')
            ORDER BY id DESC
            LIMIT %s
            """,
            (graph_id, limit),
        )
        rows = cur.fetchall()
    streak = 0
    for row in rows:
        if row["status"] in ("failed", "dead"):
            streak += 1
        else:
            break
    return streak

# ── Password reset tokens ──────────────────────────────────────────────────────
def get_user_by_email(email: str):
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM users WHERE email=%s", (email.strip().lower(),))
        row = cur.fetchone()
        return dict(row) if row else None

def get_owner_user():
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM users WHERE role='owner' ORDER BY id LIMIT 1")
        row = cur.fetchone()
        return dict(row) if row else None

def create_password_reset_token(user_id: int, token_hash: str, expires_at):
    with get_conn() as conn:
        conn.cursor().execute(
            "DELETE FROM password_resets WHERE user_id=%s",
            (user_id,)
        )
        conn.cursor().execute(
            "INSERT INTO password_resets(user_id, token_hash, expires_at) "
            "VALUES(%s, %s, %s)",
            (user_id, token_hash, expires_at)
        )

def get_password_reset_by_token(token_hash: str):
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT pr.*, u.username, u.email "
            "FROM password_resets pr JOIN users u ON pr.user_id=u.id "
            "WHERE pr.token_hash=%s AND pr.used=FALSE AND pr.expires_at > NOW()",
            (token_hash,)
        )
        row = cur.fetchone()
        return dict(row) if row else None

def consume_password_reset_token(token_hash: str):
    with get_conn() as conn:
        conn.cursor().execute(
            "UPDATE password_resets SET used=TRUE WHERE token_hash=%s",
            (token_hash,)
        )

# ── App settings (KV store) ────────────────────────────────────────────────────
# NOTE: Use get_setting / set_setting for any new scalar config value —
# do NOT add new columns to app_settings or other tables for simple on/off flags.
# All values are strings; cast to int/bool at the call site.
def get_setting(key: str, default: str = "") -> str:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT value FROM app_settings WHERE key=%s", (key,))
        row = cur.fetchone()
        return row[0] if row else default

def set_setting(key: str, value: str) -> None:
    with get_conn() as conn:
        conn.cursor().execute("""
            INSERT INTO app_settings(key, value, updated_at)
            VALUES(%s, %s, NOW())
            ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=NOW()
        """, (key, value))

def get_ratelimit_policy() -> dict:
    """Return the webhook rate-limit policy with typed values."""
    import os
    return {
        "limit":  int(get_setting("ratelimit_limit",  os.environ.get("WEBHOOK_RATE_LIMIT",  "60"))),
        "window": int(get_setting("ratelimit_window", os.environ.get("WEBHOOK_RATE_WINDOW", "60"))),
    }

def set_ratelimit_policy(limit: int, window: int) -> None:
    set_setting("ratelimit_limit",  str(max(0, int(limit))))
    set_setting("ratelimit_window", str(max(1, int(window))))

def get_retention_policy() -> dict:
    """Return the run retention policy as a dict with typed values."""
    return {
        "enabled": get_setting("retention_enabled", "false") == "true",
        "mode":    get_setting("retention_mode",    "count"),   # "count" | "age"
        "count":   int(get_setting("retention_count", "500")),
        "days":    int(get_setting("retention_days",  "30")),
    }

def set_retention_policy(enabled: bool, mode: str, count: int, days: int) -> None:
    set_setting("retention_enabled", "true" if enabled else "false")
    set_setting("retention_mode",    mode)
    set_setting("retention_count",   str(max(1, int(count))))
    set_setting("retention_days",    str(max(1, int(days))))

def trim_runs_by_count(keep: int, workspace_id: int | None = None) -> int:
    """Delete all but the `keep` most recent runs per-workspace (or global if workspace_id is None). Returns deleted count."""
    with get_conn() as conn:
        cur = conn.cursor()
        if workspace_id is not None:
            cur.execute("""
                DELETE FROM runs
                WHERE workspace_id = %s
                  AND id NOT IN (
                      SELECT id FROM runs
                      WHERE workspace_id = %s
                      ORDER BY id DESC LIMIT %s
                  )
            """, (workspace_id, workspace_id, max(1, keep)))
        else:
            cur.execute("""
                DELETE FROM runs
                WHERE id NOT IN (
                    SELECT id FROM runs ORDER BY id DESC LIMIT %s
                )
            """, (max(1, keep),))
        return cur.rowcount

def trim_runs_by_age(days: int, workspace_id: int | None = None) -> int:
    """Delete runs older than `days` days per-workspace (or global if workspace_id is None). Returns deleted count."""
    with get_conn() as conn:
        cur = conn.cursor()
        if workspace_id is not None:
            cur.execute(
                "DELETE FROM runs WHERE workspace_id = %s AND created_at < NOW() - (%s || ' days')::INTERVAL",
                (workspace_id, str(max(1, days)))
            )
        else:
            cur.execute(
                "DELETE FROM runs WHERE created_at < NOW() - (%s || ' days')::INTERVAL",
                (str(max(1, days)),)
            )
        return cur.rowcount


# ── Audit log ─────────────────────────────────────────────────────────────────
def log_audit(
    actor: str,
    action: str,
    target_type: str | None = None,
    target_id: str | None = None,
    detail: dict | None = None,
    ip: str | None = None,
) -> None:
    """Append a single audit event.  Fire-and-forget — never raises."""
    try:
        with get_conn() as conn:
            conn.cursor().execute(
                """
                INSERT INTO audit_log (actor, action, target_type, target_id, detail, ip)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    actor,
                    action,
                    target_type,
                    str(target_id) if target_id is not None else None,
                    json.dumps(detail) if detail else None,
                    ip,
                ),
            )
    except (psycopg2.Error, IOError, OSError):
        log.exception("audit_log write failed (non-fatal)")


def get_audit_log(
    limit: int = 200,
    offset: int = 0,
    actor: str | None = None,
    action: str | None = None,
) -> list[dict]:
    """Return audit log rows newest-first, optionally filtered."""
    clauses = []
    params: list = []
    if actor:
        clauses.append("actor = %s")
        params.append(actor)
    if action:
        clauses.append("action LIKE %s")
        params.append(action + "%")

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    params += [max(1, min(limit, 500)), max(0, offset)]

    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            f"""
            SELECT id, actor, action, target_type, target_id, detail, ip,
                   created_at
            FROM audit_log
            {where}
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """,
            params,
        )
        return [dict(r) for r in cur.fetchall()]


# ── Flow permissions ───────────────────────────────────────────────────────────
FLOW_ROLE_LEVELS = {"viewer": 0, "runner": 1, "editor": 2}


def set_flow_permission(user_id: int, graph_id: int, role: str, granted_by: int | None = None) -> None:
    """Insert or update a per-flow role for a user."""
    with get_conn() as conn:
        conn.cursor().execute(
            """
            INSERT INTO flow_permissions (user_id, graph_id, role, granted_by, granted_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (user_id, graph_id)
            DO UPDATE SET role=EXCLUDED.role, granted_by=EXCLUDED.granted_by, granted_at=NOW()
            """,
            (user_id, graph_id, role, granted_by),
        )


def get_flow_permission(user_id: int, graph_id: int) -> dict | None:
    """Return the flow_permissions row for (user_id, graph_id), or None."""
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT * FROM flow_permissions WHERE user_id=%s AND graph_id=%s",
            (user_id, graph_id),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def list_flow_permissions(graph_id: int) -> list:
    """Return all flow_permissions rows for a graph, joined with user info."""
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT fp.user_id, fp.graph_id, fp.role, fp.granted_at,
                   u.username, u.email,
                   gb.username AS granted_by_username
            FROM flow_permissions fp
            JOIN users u ON fp.user_id = u.id
            LEFT JOIN users gb ON fp.granted_by = gb.id
            WHERE fp.graph_id = %s
            ORDER BY u.username
            """,
            (graph_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def delete_flow_permission(user_id: int, graph_id: int) -> None:
    """Remove a per-flow permission row."""
    with get_conn() as conn:
        conn.cursor().execute(
            "DELETE FROM flow_permissions WHERE user_id=%s AND graph_id=%s",
            (user_id, graph_id),
        )


def get_permitted_graph_ids(user_id: int) -> list[int]:
    """Return all graph_ids for which the user has any explicit flow permission."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT graph_id FROM flow_permissions WHERE user_id=%s",
            (user_id,),
        )
        return [row[0] for row in cur.fetchall()]


# ── Invite tokens ──────────────────────────────────────────────────────────────
def create_invite_token(
    token_hash: str,
    email: str,
    graph_id: int | None,
    role: str,
    invited_by: int | None,
    expires_at,
) -> dict:
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            INSERT INTO invite_tokens (token_hash, email, graph_id, role, invited_by, expires_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id, email, graph_id, role, expires_at
            """,
            (token_hash, email.strip().lower(), graph_id, role, invited_by, expires_at),
        )
        row = cur.fetchone()
        return dict(row)


def get_invite_by_hash(token_hash: str) -> dict | None:
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT it.*, g.name AS graph_name
            FROM invite_tokens it
            LEFT JOIN graph_workflows g ON it.graph_id = g.id
            WHERE it.token_hash=%s AND it.used_at IS NULL AND it.expires_at > NOW()
            """,
            (token_hash,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def consume_invite_token(token_hash: str) -> None:
    with get_conn() as conn:
        conn.cursor().execute(
            "UPDATE invite_tokens SET used_at=NOW() WHERE token_hash=%s",
            (token_hash,),
        )


# ── Workspaces ─────────────────────────────────────────────────────────────────
WORKSPACE_ROLE_LEVELS = {"viewer": 0, "admin": 1, "owner": 2}


def _slugify(name: str) -> str:
    """Convert a workspace name to a URL-safe slug."""
    import re
    slug = name.strip().lower()
    slug = re.sub(r"[^a-z0-9]+", "-", slug)
    slug = slug.strip("-")
    return slug or "workspace"


def create_workspace(name: str, slug: str | None = None, plan: str = "free") -> dict:
    """Create a new workspace.  Raises if the slug already exists."""
    final_slug = slug or _slugify(name)
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            INSERT INTO workspaces (name, slug, plan, created_at, updated_at)
            VALUES (%s, %s, %s, NOW(), NOW())
            RETURNING *
            """,
            (name.strip(), final_slug, plan),
        )
        row = cur.fetchone()
        return dict(row)


def get_workspace(workspace_id: int) -> dict | None:
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM workspaces WHERE id=%s", (workspace_id,))
        row = cur.fetchone()
        return dict(row) if row else None


def get_workspace_by_slug(slug: str) -> dict | None:
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM workspaces WHERE slug=%s", (slug,))
        row = cur.fetchone()
        return dict(row) if row else None


def list_workspaces() -> list:
    """Return all workspaces ordered by name (super-admin view)."""
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT w.*,
                   COUNT(wm.user_id) AS member_count
            FROM workspaces w
            LEFT JOIN workspace_members wm ON w.id = wm.workspace_id
            GROUP BY w.id
            ORDER BY w.name
            """
        )
        return [dict(r) for r in cur.fetchall()]


def update_workspace(workspace_id: int, name: str | None = None,
                     slug: str | None = None, plan: str | None = None) -> dict | None:
    """Update mutable workspace fields.  Returns the updated row."""
    parts = []
    params: list = []
    if name is not None:
        parts.append("name=%s")
        params.append(name.strip())
    if slug is not None:
        parts.append("slug=%s")
        params.append(slug.strip())
    if plan is not None:
        parts.append("plan=%s")
        params.append(plan)
    if not parts:
        return get_workspace(workspace_id)
    parts.append("updated_at=NOW()")
    params.append(workspace_id)
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            f"UPDATE workspaces SET {', '.join(parts)} WHERE id=%s RETURNING *",
            params,
        )
        row = cur.fetchone()
        return dict(row) if row else None


def delete_workspace(workspace_id: int) -> None:
    """Delete a workspace and all its members (CASCADE)."""
    with get_conn() as conn:
        conn.cursor().execute("DELETE FROM workspaces WHERE id=%s", (workspace_id,))


# ── Workspace members ──────────────────────────────────────────────────────────
def list_workspace_members(workspace_id: int) -> list:
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT wm.workspace_id, wm.user_id, wm.role, wm.joined_at,
                   u.username, u.email, u.role AS global_role
            FROM workspace_members wm
            JOIN users u ON wm.user_id = u.id
            WHERE wm.workspace_id = %s
            ORDER BY wm.role DESC, u.username
            """,
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_workspace_member(workspace_id: int, user_id: int) -> dict | None:
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT * FROM workspace_members WHERE workspace_id=%s AND user_id=%s",
            (workspace_id, user_id),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def set_workspace_member(workspace_id: int, user_id: int, role: str) -> None:
    """Upsert a member's workspace role."""
    with get_conn() as conn:
        conn.cursor().execute(
            """
            INSERT INTO workspace_members (workspace_id, user_id, role, joined_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (workspace_id, user_id)
            DO UPDATE SET role=EXCLUDED.role
            """,
            (workspace_id, user_id, role),
        )


def remove_workspace_member(workspace_id: int, user_id: int) -> None:
    with get_conn() as conn:
        conn.cursor().execute(
            "DELETE FROM workspace_members WHERE workspace_id=%s AND user_id=%s",
            (workspace_id, user_id),
        )


def list_user_workspaces(user_id: int) -> list:
    """Return all workspaces the user belongs to, with their role in each."""
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT w.*, wm.role AS member_role, wm.joined_at
            FROM workspaces w
            JOIN workspace_members wm ON w.id = wm.workspace_id
            WHERE wm.user_id = %s
            ORDER BY wm.role DESC, w.name
            """,
            (user_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_default_workspace() -> dict | None:
    """Return the 'default' workspace (always exists after migration 0008)."""
    return get_workspace_by_slug("default")


# ── Plan limits + workspace usage ─────────────────────────────────────────────
PLAN_LIMITS: dict = {
    "free": {
        "flows":       5,
        "members":     3,
        "runs_per_day": 100,
    },
    "pro": {
        "flows":       50,
        "members":     20,
        "runs_per_day": 5_000,
    },
    "enterprise": {
        "flows":       None,   # None = unlimited
        "members":     None,
        "runs_per_day": None,
    },
}


def get_workspace_usage(workspace_id: int) -> dict:
    """Return current usage counts for a workspace (flows, members, runs today)."""
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT COUNT(*) AS n FROM graph_workflows WHERE workspace_id=%s",
            (workspace_id,),
        )
        flows = (cur.fetchone() or {}).get("n", 0)

        cur.execute(
            "SELECT COUNT(*) AS n FROM workspace_members WHERE workspace_id=%s",
            (workspace_id,),
        )
        members = (cur.fetchone() or {}).get("n", 0)

        cur.execute(
            """
            SELECT COUNT(*) AS n FROM runs
            WHERE workspace_id=%s
              AND created_at >= NOW() - INTERVAL '1 day'
            """,
            (workspace_id,),
        )
        runs_today = (cur.fetchone() or {}).get("n", 0)

    return {
        "flows":      int(flows),
        "members":    int(members),
        "runs_today": int(runs_today),
    }
