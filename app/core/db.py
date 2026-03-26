"""Database layer — v8
New: credentials vault, graph version history, node traces + initial_payload in runs
"""
import os, json, logging
from contextlib import contextmanager
import psycopg2
import psycopg2.extras

log = logging.getLogger(__name__)
DSN = os.environ.get("DATABASE_URL", "postgresql://auto:auto@db:5432/auto")

@contextmanager
def get_conn():
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    try:
        yield conn
    finally:
        conn.close()

psycopg2.extras.register_uuid()

def init_db():
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
                cron     TEXT NOT NULL,
                payload  JSONB DEFAULT '{}',
                timezone TEXT DEFAULT 'UTC',
                enabled  BOOLEAN DEFAULT TRUE
            )
        """)
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

# ── runs ──────────────────────────────────────────────────────────────────
def list_runs():
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT r.*,
                   COALESCE(g.name, r.workflow) AS flow_name
            FROM runs r
            LEFT JOIN graph_workflows g ON r.graph_id = g.id
            ORDER BY r.id DESC LIMIT 200
        """)
        return [dict(r) for r in cur.fetchall()]

def get_run_by_task(task_id):
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM runs WHERE task_id=%s", (task_id,))
        row = cur.fetchone()
        return dict(row) if row else None

def update_run(task_id, status, result=None, traces=None):
    with get_conn() as conn:
        conn.cursor().execute(
            "UPDATE runs SET status=%s, result=%s, traces=%s, updated_at=NOW() WHERE task_id=%s",
            (status, json.dumps(result or {}), json.dumps(traces or []), task_id)
        )

def delete_run(run_id):
    with get_conn() as conn:
        conn.cursor().execute("DELETE FROM runs WHERE id=%s", (run_id,))

def clear_runs():
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
def list_schedules():
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM schedules ORDER BY id")
        return [dict(r) for r in cur.fetchall()]

def create_schedule(name, workflow=None, graph_id=None, cron="0 * * * *", payload=None, timezone="UTC"):
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "INSERT INTO schedules(name,workflow,graph_id,cron,payload,timezone) VALUES(%s,%s,%s,%s,%s,%s) RETURNING *",
            (name, workflow, graph_id, cron, json.dumps(payload or {}), timezone)
        )
        return dict(cur.fetchone())

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
def list_graphs():
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM graph_workflows ORDER BY id")
        return [dict(r) for r in cur.fetchall()]

def create_graph(name, description, graph_json):
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
            "INSERT INTO graph_workflows(name,description,graph_json,slug) VALUES(%s,%s,%s,%s) RETURNING *",
            (name, description, graph_json, slug)
        )
        return dict(cur.fetchone())

def get_graph(graph_id):
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM graph_workflows WHERE id=%s", (graph_id,))
        row = cur.fetchone()
        return dict(row) if row else None

def get_graph_by_slug(slug):
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
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

def update_graph(graph_id, name=None, description=None, graph_json=None, enabled=None):
    with get_conn() as conn:
        cur = conn.cursor()
        if name        is not None: cur.execute("UPDATE graph_workflows SET name=%s,        updated_at=NOW() WHERE id=%s", (name,        graph_id))
        if description is not None: cur.execute("UPDATE graph_workflows SET description=%s, updated_at=NOW() WHERE id=%s", (description, graph_id))
        if graph_json  is not None: cur.execute("UPDATE graph_workflows SET graph_json=%s,  updated_at=NOW() WHERE id=%s", (graph_json,  graph_id))
        if enabled     is not None: cur.execute("UPDATE graph_workflows SET enabled=%s,      updated_at=NOW() WHERE id=%s", (enabled,     graph_id))

def delete_graph(graph_id):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM schedules      WHERE graph_id=%s", (graph_id,))
        cur.execute("DELETE FROM graph_versions WHERE graph_id=%s", (graph_id,))
        cur.execute("DELETE FROM graph_workflows WHERE id=%s",      (graph_id,))

# ── credentials ───────────────────────────────────────────────────────────
def list_credentials():
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        # never return secret values in the list
        cur.execute("SELECT id, name, type, note, created_at, updated_at FROM credentials ORDER BY name")
        return [dict(r) for r in cur.fetchall()]

def get_credential_secret(name):
    """Fetch the raw secret for a named credential. Used by executor only."""
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT secret FROM credentials WHERE name=%s", (name,))
        row = cur.fetchone()
        return row['secret'] if row else None

def load_all_credentials():
    """Return name→secret mapping. Called once per graph run."""
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT name, secret FROM credentials")
        return {r['name']: r['secret'] for r in cur.fetchall()}

def upsert_credential(name, type_, secret, note=""):
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            INSERT INTO credentials(name, type, secret, note)
            VALUES(%s, %s, %s, %s)
            ON CONFLICT(name) DO UPDATE
              SET type=EXCLUDED.type, secret=EXCLUDED.secret,
                  note=EXCLUDED.note, updated_at=NOW()
            RETURNING id, name, type, note, created_at, updated_at
        """, (name, type_, secret, note))
        return dict(cur.fetchone())

def delete_credential(cred_id):
    with get_conn() as conn:
        conn.cursor().execute("DELETE FROM credentials WHERE id=%s", (cred_id,))

# ── graph_versions ────────────────────────────────────────────────────────
def list_graph_versions(graph_id):
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT id, graph_id, version, name, note, saved_at FROM graph_versions WHERE graph_id=%s ORDER BY version DESC LIMIT 20",
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
def get_run_metrics():
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # 30-day summary
        cur.execute("""
            SELECT
                COUNT(*)                                                        AS total,
                SUM(CASE WHEN status='succeeded' THEN 1 ELSE 0 END)            AS succeeded,
                SUM(CASE WHEN status='failed'    THEN 1 ELSE 0 END)            AS failed,
                ROUND(AVG(EXTRACT(EPOCH FROM (updated_at - created_at))*1000)) AS avg_ms
            FROM runs
            WHERE created_at >= NOW() - INTERVAL '30 days'
        """)
        s = dict(cur.fetchone())

        # Daily counts for the last 14 days (fill gaps with zeros)
        cur.execute("""
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
            GROUP BY days.day
            ORDER BY days.day
        """)
        daily = [
            {'day': r['day'], 'succeeded': int(r['succeeded']), 'failed': int(r['failed']), 'total': int(r['total'])}
            for r in cur.fetchall()
        ]

        # Top 5 failing flows (last 30 days)
        cur.execute("""
            SELECT COALESCE(g.name, 'legacy:' || r.workflow) AS name,
                   COUNT(*) AS failures
            FROM runs r
            LEFT JOIN graph_workflows g ON r.graph_id = g.id
            WHERE r.status = 'failed'
              AND r.created_at >= NOW() - INTERVAL '30 days'
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 5
        """)
        top_failing = [{'name': r['name'], 'failures': int(r['failures'])} for r in cur.fetchall()]

        # Last 10 runs for the activity feed
        cur.execute("""
            SELECT r.id, r.status,
                   r.created_at::text AS created_at,
                   COALESCE(g.name, r.workflow, 'unknown') AS flow_name,
                   GREATEST(ROUND(EXTRACT(EPOCH FROM (r.updated_at - r.created_at))*1000), 0)::int AS duration_ms
            FROM runs r
            LEFT JOIN graph_workflows g ON r.graph_id = g.id
            ORDER BY r.id DESC LIMIT 10
        """)
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
