#!/usr/bin/env bash
# ============================================================
#  Automations Platform — v11 bootstrap
#  New in v11:
#    Executor   : Pure orchestration; node registry dispatch
#    Nodes      : Modular handlers in app/nodes/; continue-on-error
#    Merge Node : Multi-input join with upstream_ids injection
#    Run Replay : Rerun failed workflows from trace
#    Webhooks   : Rate limiting per token (v11)
# ============================================================
set -euo pipefail
PROJECT="${1:-automations}"
echo "▶  Bootstrapping v11 into ./$PROJECT"
mkdir -p "$PROJECT"/{app/{core,workflows,static,templates,nodes/custom},caddy,runlogs}

# ── 1. docker-compose.yml ──────────────────────────────────────────────────
cat > "$PROJECT/docker-compose.yml" << 'EOF'
services:
  db:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: automations
      POSTGRES_USER: automations
      POSTGRES_PASSWORD: automations
    volumes:
      - pg_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL","pg_isready -U automations"]
      interval: 5s
      retries: 10

  redis:
    image: redis:7-alpine
    healthcheck:
      test: ["CMD","redis-cli","ping"]
      interval: 5s
      retries: 10

  api:
    build: .
    command: uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
    volumes:
      - ./app:/app/app
      - ./runlogs:/app/runlogs
    env_file: .env
    depends_on:
      db: {condition: service_healthy}
      redis: {condition: service_healthy}

  worker:
    build: .
    command: celery -A app.worker worker --loglevel=info --concurrency=4
    volumes:
      - ./app:/app/app
      - ./runlogs:/app/runlogs
    env_file: .env
    depends_on:
      db: {condition: service_healthy}
      redis: {condition: service_healthy}

  scheduler:
    build: .
    command: python -m app.scheduler
    volumes:
      - ./app:/app/app
    env_file: .env
    depends_on:
      db: {condition: service_healthy}
      redis: {condition: service_healthy}

  flower:
    build: .
    command: celery -A app.worker flower --port=5555 --url_prefix=flower
    env_file: .env
    depends_on:
      - redis

  caddy:
    image: caddy:2-alpine
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./caddy/Caddyfile:/etc/caddy/Caddyfile
      - caddy_data:/data
      - caddy_config:/config
    depends_on:
      - api
      - flower

volumes:
  pg_data:
  caddy_data:
  caddy_config:
EOF


# ── 2. Caddyfile ───────────────────────────────────────────────────────────
cat > "$PROJECT/caddy/Caddyfile" << 'EOF'
:80 {
  handle /flower/* {
    reverse_proxy flower:5555
  }
  handle {
    reverse_proxy api:8000
  }
}
EOF


# ── 3. .env ────────────────────────────────────────────────────────────────
cat > "$PROJECT/.env" << 'EOF'
DATABASE_URL=postgresql://automations:automations@db:5432/automations
REDIS_URL=redis://redis:6379/0
API_KEY=dev_api_key
ADMIN_TOKEN=dev_admin_token
# Email (SMTP)
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=
SMTP_PASS=
SMTP_FROM=
# Telegram
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
# OpenAI (for action.llm_call)
OPENAI_API_KEY=
# Slack (for action.slack node)
SLACK_WEBHOOK_URL=

# ── Failure notifications (v9) ────────────────────────────────────────────
# Set either or both to receive alerts when any run fails.
# NOTIFY_SLACK_WEBHOOK sends a Slack message via an incoming webhook URL.
# NOTIFY_EMAIL sends an email — requires SMTP_HOST/USER/PASS above.
NOTIFY_SLACK_WEBHOOK=
NOTIFY_EMAIL=

# ── Webhook rate limiting (v11) ───────────────────────────────────────────
# Max webhook calls per token per window. Set WEBHOOK_RATE_LIMIT=0 to disable.
WEBHOOK_RATE_LIMIT=60
WEBHOOK_RATE_WINDOW=60
EOF


# ── 4. Dockerfile ──────────────────────────────────────────────────────────
cat > "$PROJECT/Dockerfile" << 'EOF'
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app/ ./app/
CMD ["uvicorn","app.main:app","--host","0.0.0.0","--port","8000"]
EOF


# ── 5. requirements.txt ────────────────────────────────────────────────────
cat > "$PROJECT/requirements.txt" << 'EOF'
fastapi==0.111.0
uvicorn[standard]==0.29.0
celery==5.4.0
redis==5.0.4
psycopg2-binary==2.9.9
flower==2.0.1
python-multipart==0.0.9
pydantic==2.7.1
httpx==0.27.0
apscheduler==3.10.4
openai>=1.30.0
paramiko==3.4.0
google-auth
EOF


# ── 6. app/core/db.py ──────────────────────────────────────────────────────
cat > "$PROJECT/app/core/db.py" << 'EOF'
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

# ── runs ──────────────────────────────────────────────────────────────────
def list_runs():
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM runs ORDER BY id DESC LIMIT 200")
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
EOF


# ── 7. app/core/wait.py ────────────────────────────────────────────────────
cat > "$PROJECT/app/core/wait.py" << 'EOF'

def do_wait(seconds: float) -> dict:
    """Block the Celery worker for `seconds` seconds."""
    t = max(0.0, min(float(seconds), 3600.0))
    time.sleep(t)
    return {"waited_seconds": t}
EOF


# ── 8. app/core/executor.py ────────────────────────────────────────────────
cat > "$PROJECT/app/core/executor.py" << 'EXEC_EOF'
"""Graph workflow executor — v11
Architecture: pure orchestration. All node logic lives in app/nodes/.
New in v11:
  - Node registry dispatch (app/nodes/)
  - continue-on-error per node (fail_mode: "abort" | "continue")
  - Merge/Join node with upstream_ids injection
  - _render moved to app/nodes/_utils (executor re-exports for compat)
"""
import re, json, time, logging
from typing import Any

log = logging.getLogger(__name__)

# ── re-export _render for backward compatibility (call_graph etc) ──────────
from app.nodes._utils import _render  # noqa: F401

# ── topological sort (Kahn's) ─────────────────────────────────────────────
def _topo(nodes, edges):
    ids   = [n['id'] for n in nodes]
    indeg = {i: 0 for i in ids}
    succ  = {i: [] for i in ids}
    for e in edges:
        s, t = e['source'], e['target']
        if s in succ and t in indeg:
            succ[s].append((t, e.get('sourceHandle')))
            indeg[t] += 1
    queue = [i for i in ids if indeg[i] == 0]
    order = []
    while queue:
        n = queue.pop(0)
        order.append(n)
        for (nb, _) in succ[n]:
            indeg[nb] -= 1
            if indeg[nb] == 0:
                queue.append(nb)
    return order, succ

def _subgraph_order(entry, subset, edges):
    sub_edges = [e for e in edges if e['source'] in subset and e['target'] in subset]
    sub_nodes = [{'id': i} for i in subset]
    order, succ = _topo(sub_nodes, sub_edges)
    if entry in order:
        order.remove(entry)
    return order, succ

def _reachable_via_handle(start_id: str, handle: str, succ: dict) -> set:
    """BFS from start_id's successors reached via `handle`."""
    frontier = {t for (t, h) in succ.get(start_id, []) if h == handle}
    visited  = set()
    queue    = list(frontier)
    while queue:
        n = queue.pop()
        if n not in visited:
            visited.add(n)
            for (t, _) in succ.get(n, []):
                if t not in visited:
                    queue.append(t)
    return visited

# ── node dispatch ─────────────────────────────────────────────────────────
def _run_node(node_type, config, inp, context, logger, edges, nodes_map, creds=None, **kwargs):
    """Dispatch to the registered node handler."""
    from app.nodes import get_handler
    handler = get_handler(node_type)
    if handler is None:
        raise ValueError(f"Unknown node type: {node_type!r} — not found in node registry")

    # Build upstream_ids for merge node (and any future fan-in nodes)
    upstream_ids = [e['source'] for e in edges if e['target'] == kwargs.get('_nid', '')]

    return handler(
        config, inp, context, logger,
        creds=creds,
        upstream_ids=upstream_ids,
        edges=edges,
        nodes_map=nodes_map,
        **{k: v for k, v in kwargs.items() if k != '_nid'},
    )

# ── main graph runner ─────────────────────────────────────────────────────
def run_graph(graph_data: dict, initial_payload: dict = None, logger=None, _depth: int = 0) -> dict:
    if logger is None:
        logger = lambda msg: log.info(msg)
    if _depth > 5:
        raise RuntimeError("Call Graph: maximum sub-flow nesting depth (5) exceeded")

    nodes   = graph_data.get('nodes', [])
    edges   = graph_data.get('edges', [])
    payload = initial_payload or {}
    context = {}
    results = {}
    traces  = []

    # Load credentials once for this run
    try:
        from app.core.db import load_all_credentials
        creds = load_all_credentials()
    except Exception as e:
        log.warning(f"Could not load credentials: {e}")
        creds = {}

    nodes_map  = {n['id']: n for n in nodes}
    order, succ = _topo(nodes, edges)
    skip_nodes  = set()   # nodes on un-taken condition branches

    for nid in order:
        node  = nodes_map.get(nid)
        if not node:
            continue
        ntype  = node.get('type', '')
        ndata  = node.get('data', {})
        config = ndata.get('config', {})

        # Collect input: prefer the most-recently-connected upstream node
        inp = payload.copy()
        for e in edges:
            if e['target'] == nid and e['source'] in context:
                inp = context[e['source']]
                break
        context[nid] = inp

        # Store input in trace (capped at 5MB)
        _inp_str    = json.dumps(inp, default=str) if isinstance(inp, (dict, list)) else str(inp)
        _inp_stored = inp if len(_inp_str) < 5000000 else {'__truncated': True, '__size': len(_inp_str)}

        trace = {
            'node_id':     nid,
            'type':        ntype,
            'label':       ndata.get('label', ''),
            'status':      'skipped',
            'duration_ms': 0,
            'attempts':    0,
            'input':       _inp_stored,
            'output':      None,
            'error':       None,
        }

        # ── skip disabled nodes ───────────────────────────────────────
        if ndata.get('disabled', False):
            logger(f"SKIP (disabled) {ntype} [{nid}]")
            results[nid] = {'__disabled': True}
            trace['status'] = 'skipped'
            traces.append(trace)
            continue

        # ── skip nodes on un-taken condition branch ───────────────────
        if nid in skip_nodes:
            logger(f"SKIP (branch not taken) {ntype} [{nid}]")
            results[nid] = {'__skipped': True}
            trace['status'] = 'skipped'
            trace['error']  = 'Branch not taken'
            traces.append(trace)
            continue

        # ── retry + fail_mode policy ──────────────────────────────────
        retry_max   = int(ndata.get('retry_max', 0))
        retry_delay = float(ndata.get('retry_delay', 5))
        # fail_mode: "abort" raises and stops the graph (default, v10 behaviour)
        #            "continue" stores the error in context and keeps going
        fail_mode   = ndata.get('fail_mode', 'abort')

        t_start  = time.time()
        last_err = None
        result   = None

        for attempt in range(retry_max + 1):
            try:
                if attempt > 0:
                    logger(f"RETRY {attempt}/{retry_max} {ntype} [{nid}]")
                    time.sleep(retry_delay)
                result = _run_node(
                    ntype, config, inp, context, logger,
                    edges, nodes_map, creds,
                    _depth=_depth, _nid=nid,
                )
                last_err = None
                break
            except Exception as e:
                last_err = e
                logger(f"ERROR attempt {attempt+1} {ntype} [{nid}]: {e}")

        t_end = time.time()
        trace['duration_ms'] = int((t_end - t_start) * 1000)
        trace['attempts']    = attempt + 1

        if last_err is not None:
            err_msg = f"{type(last_err).__name__}: {last_err}"
            trace['status'] = 'error'
            trace['error']  = err_msg

            if fail_mode == 'continue':
                # Store error in context so downstream nodes can inspect it
                error_out = {'__error': err_msg, '__node': nid, '__type': ntype}
                context[nid] = error_out
                results[nid] = error_out
                trace['output'] = error_out
                traces.append(trace)
                logger(f"CONTINUE-ON-ERROR [{nid}]: {err_msg}")
                continue
            else:
                results[nid] = {'__error': err_msg}
                traces.append(trace)
                raise RuntimeError(
                    f"Node [{nid}] ({ntype}) failed after {attempt+1} attempt(s): {last_err}"
                ) from last_err

        context[nid] = result
        results[nid] = result
        trace['status'] = 'ok'
        _out_str = json.dumps(result, default=str) if isinstance(result, (dict, list)) else str(result)
        trace['output'] = result if len(_out_str) < 5000000 else {'__truncated': True, '__size': len(_out_str)}
        traces.append(trace)

        # ── condition branching ───────────────────────────────────────
        if ntype == 'action.condition':
            condition_val = result.get('result', False) if isinstance(result, dict) else bool(result)
            true_reach  = _reachable_via_handle(nid, 'true',  succ)
            false_reach = _reachable_via_handle(nid, 'false', succ)
            if condition_val:
                skip_nodes |= (false_reach - true_reach)
                logger(f"Condition [{nid}] = True  → skipping {len(false_reach - true_reach)} false-branch node(s)")
            else:
                skip_nodes |= (true_reach - false_reach)
                logger(f"Condition [{nid}] = False → skipping {len(true_reach - false_reach)} true-branch node(s)")

        # ── loop body expansion ───────────────────────────────────────
        if isinstance(result, dict) and result.get('__loop__'):
            items        = result.get('items', [])
            body_targets = [t for (t, h) in succ.get(nid, []) if h == 'body']
            body_set     = set()
            for bt in body_targets:
                body_set.add(bt)
                for (nb, _) in succ.get(bt, []):
                    body_set.add(nb)
            loop_results = []
            for item in items:
                body_order, _ = _subgraph_order(nid, body_set, edges)
                loop_ctx = {**context, nid: item, 'item': item}
                for bid in body_order:
                    bn = nodes_map.get(bid)
                    if not bn: continue
                    b_inp = loop_ctx.get(bid, item)
                    for e in edges:
                        if e['target'] == bid and e['source'] in loop_ctx:
                            b_inp = loop_ctx[e['source']]; break
                    try:
                        from app.nodes import get_handler as _gh
                        _h = _gh(bn.get('type', ''))
                        if _h:
                            loop_ctx[bid] = _h(
                                bn.get('data', {}).get('config', {}),
                                b_inp, loop_ctx, logger, creds=creds,
                                upstream_ids=[], edges=edges, nodes_map=nodes_map,
                            )
                        else:
                            loop_ctx[bid] = {'__error': f"Unknown node type in loop: {bn.get('type')}"}
                    except Exception as e:
                        loop_ctx[bid] = {'__error': str(e)}
                loop_results.append(loop_ctx.get(body_targets[0]) if body_targets else item)
            context[nid] = {'loop_results': loop_results, 'count': len(loop_results)}
            results[nid] = context[nid]

    return {'context': context, 'results': results, 'traces': traces}


EXEC_EOF

# ── 9. app/workflows/example.py ────────────────────────────────────────────
cat > "$PROJECT/app/workflows/example.py" << 'EOF'
from datetime import datetime, timezone

def run_example(payload: dict) -> dict:
    return {"message": "Hello from v8!", "time": datetime.now(timezone.utc).isoformat(), "echo": payload}
EOF


# ── 10. app/workflows/health_check.py ──────────────────────────────────────
cat > "$PROJECT/app/workflows/health_check.py" << 'EOF'
"""
Health Check — polls a list of URLs and prints a status report.

Edit TARGETS below to add your own endpoints.
Run via the Scripts page or schedule it on a cron trigger.
"""

import urllib.request
import urllib.error
import time

# ── Configuration ──────────────────────────────────────────────────────────────
TARGETS = [
    {"name": "Google",       "url": "https://www.google.com"},
    {"name": "Cloudflare",   "url": "https://1.1.1.1"},
    {"name": "Example API",  "url": "https://httpbin.org/get"},
]
TIMEOUT = 10   # seconds per request
# ───────────────────────────────────────────────────────────────────────────────

results = []

for target in TARGETS:
    name = target["name"]
    url  = target["url"]
    t0   = time.time()
    try:
        req  = urllib.request.Request(url, headers={"User-Agent": "automations-health-check/1.0"})
        resp = urllib.request.urlopen(req, timeout=TIMEOUT)
        ms   = int((time.time() - t0) * 1000)
        results.append({"name": name, "url": url, "status": resp.status, "ms": ms, "ok": True})
        print(f"  ✓  {name:20s}  HTTP {resp.status}  ({ms} ms)")
    except urllib.error.HTTPError as e:
        ms = int((time.time() - t0) * 1000)
        results.append({"name": name, "url": url, "status": e.code, "ms": ms, "ok": False})
        print(f"  ✗  {name:20s}  HTTP {e.code}  ({ms} ms)")
    except Exception as e:
        ms = int((time.time() - t0) * 1000)
        results.append({"name": name, "url": url, "status": None, "ms": ms, "ok": False, "error": str(e)})
        print(f"  ✗  {name:20s}  ERROR: {e}")

total   = len(results)
healthy = sum(1 for r in results if r["ok"])
print(f"\n{'─'*50}")
print(f"  {healthy}/{total} targets healthy")
if healthy < total:
    print("  DEGRADED — some targets are unreachable")
else:
    print("  ALL CLEAR")
EOF


# ── 11. app/workflows/cleanup_old_runs.py ──────────────────────────────────
cat > "$PROJECT/app/workflows/cleanup_old_runs.py" << 'EOF'
"""
Cleanup Old Runs — deletes run records older than KEEP_DAYS.

Useful as a scheduled maintenance task to keep the runs table lean.
Run via the Scripts page or wire up a daily trigger.cron in the canvas.
"""

import os
import psycopg2

# ── Configuration ──────────────────────────────────────────────────────────────
KEEP_DAYS = 30   # delete runs older than this many days
DRY_RUN   = False  # set True to preview without deleting
# ───────────────────────────────────────────────────────────────────────────────

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://automations:automations@db:5432/automations")

conn = psycopg2.connect(DATABASE_URL)
cur  = conn.cursor()

# Count how many rows would be affected
cur.execute(
    "SELECT COUNT(*) FROM runs WHERE created_at < NOW() - INTERVAL '%s days'",
    (KEEP_DAYS,)
)
count = cur.fetchone()[0]

if count == 0:
    print(f"Nothing to delete — all runs are within the last {KEEP_DAYS} days.")
elif DRY_RUN:
    print(f"DRY RUN: would delete {count} run(s) older than {KEEP_DAYS} days.")
    print("Set DRY_RUN = False to actually delete them.")
else:
    cur.execute(
        "DELETE FROM runs WHERE created_at < NOW() - INTERVAL '%s days'",
        (KEEP_DAYS,)
    )
    conn.commit()
    print(f"Deleted {count} run(s) older than {KEEP_DAYS} days.")

# Show what remains
cur.execute("SELECT COUNT(*), MIN(created_at)::text, MAX(created_at)::text FROM runs")
total, oldest, newest = cur.fetchone()
print(f"\nRuns table now: {total} record(s)")
if oldest:
    print(f"  Oldest: {oldest}")
    print(f"  Newest: {newest}")

cur.close()
conn.close()
EOF


# ── 12. app/workflows/daily_summary.py ─────────────────────────────────────
cat > "$PROJECT/app/workflows/daily_summary.py" << 'EOF'
"""
Daily Summary — prints a human-readable report of the last 24 hours of run activity.

Pair this with a trigger.cron node (e.g. "0 8 * * *") and an action.send_email
node in the canvas, or simply run it from the Scripts page for an instant snapshot.
"""

import os
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://automations:automations@db:5432/automations")

conn = psycopg2.connect(DATABASE_URL)
cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

now_utc = datetime.now(timezone.utc)
print(f"Daily Run Summary — {now_utc.strftime('%Y-%m-%d %H:%M UTC')}")
print("=" * 52)

# Overall counts for the last 24 h
cur.execute("""
    SELECT
        COUNT(*)                                             AS total,
        SUM(CASE WHEN status = 'succeeded' THEN 1 ELSE 0 END) AS succeeded,
        SUM(CASE WHEN status = 'failed'    THEN 1 ELSE 0 END) AS failed,
        SUM(CASE WHEN status = 'running'   THEN 1 ELSE 0 END) AS running,
        ROUND(AVG(EXTRACT(EPOCH FROM (updated_at - created_at))*1000))::int AS avg_ms
    FROM runs
    WHERE created_at >= NOW() - INTERVAL '24 hours'
""")
s = cur.fetchone()

total     = s['total']     or 0
succeeded = s['succeeded'] or 0
failed    = s['failed']    or 0
running   = s['running']   or 0
avg_ms    = s['avg_ms']    or 0

if total == 0:
    print("  No runs in the last 24 hours.")
else:
    rate = round(succeeded / total * 100) if total else 0
    print(f"  Total runs    : {total}")
    print(f"  Succeeded     : {succeeded}  ({rate}%)")
    print(f"  Failed        : {failed}")
    print(f"  Still running : {running}")
    print(f"  Avg duration  : {avg_ms} ms")

# Failures detail
if failed > 0:
    print("\nFailed runs:")
    cur.execute("""
        SELECT r.id, COALESCE(g.name, r.workflow, 'unknown') AS flow,
               r.created_at::text AS started
        FROM runs r
        LEFT JOIN graph_workflows g ON r.graph_id = g.id
        WHERE r.status = 'failed'
          AND r.created_at >= NOW() - INTERVAL '24 hours'
        ORDER BY r.id DESC
        LIMIT 10
    """)
    for row in cur.fetchall():
        print(f"  #{row['id']:>6}  {row['flow']:30s}  {row['started']}")

# Most active flows
print("\nMost active flows (last 24 h):")
cur.execute("""
    SELECT COALESCE(g.name, r.workflow, 'unknown') AS flow,
           COUNT(*) AS runs,
           SUM(CASE WHEN r.status = 'succeeded' THEN 1 ELSE 0 END) AS ok,
           SUM(CASE WHEN r.status = 'failed'    THEN 1 ELSE 0 END) AS err
    FROM runs r
    LEFT JOIN graph_workflows g ON r.graph_id = g.id
    WHERE r.created_at >= NOW() - INTERVAL '24 hours'
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 5
""")
rows = cur.fetchall()
if rows:
    for row in rows:
        bar = "✓" * int(row['ok']) + "✗" * int(row['err'])
        print(f"  {row['flow']:30s}  {row['runs']} run(s)  [{bar}]")
else:
    print("  (none)")

print("\n" + "=" * 52)
print("Report complete.")

cur.close()
conn.close()
EOF


# ── 13. app/worker.py ──────────────────────────────────────────────────────
cat > "$PROJECT/app/worker.py" << 'EOF'
import os, json, logging, io, sys, runpy, smtplib
from pathlib import Path
from email.mime.text import MIMEText
from celery import Celery
from app.core.db import (
    get_run_by_task, update_run,
    list_workflows, get_graph
)

SCRIPTS_DIR = Path(__file__).parent / 'workflows'

log    = logging.getLogger(__name__)
broker = os.environ.get("CELERY_BROKER_URL", "redis://redis:6379/0")
app    = Celery("automations", broker=broker, backend=broker)
app.conf.task_serializer   = "json"
app.conf.result_serializer = "json"
app.conf.accept_content    = ["json"]


# ── failure notifications ─────────────────────────────────────────────────
def _notify_failure(name: str, error: str, task_id: str):
    """Send failure alerts via NOTIFY_SLACK_WEBHOOK and/or NOTIFY_EMAIL env vars."""
    slack_url    = os.environ.get("NOTIFY_SLACK_WEBHOOK", "")
    notify_email = os.environ.get("NOTIFY_EMAIL", "")
    short_id     = task_id[:8] if task_id else "?"
    short_err    = str(error)[:300]

    if slack_url:
        try:
            import httpx
            httpx.post(slack_url, json={
                "text": f":x: *{name}* failed (task `{short_id}`)\n```{short_err}```"
            }, timeout=10)
        except Exception as ex:
            log.warning(f"Slack notify failed: {ex}")

    if notify_email:
        try:
            smtp_host = os.environ.get("SMTP_HOST", "")
            smtp_user = os.environ.get("SMTP_USER", "")
            smtp_pass = os.environ.get("SMTP_PASS", "")
            smtp_port = int(os.environ.get("SMTP_PORT", 465))
            if not smtp_host:
                log.warning("NOTIFY_EMAIL set but SMTP_HOST not configured")
                return
            msg = MIMEText(
                f"Task ID:  {task_id}\nWorkflow: {name}\n\nError:\n{error}"
            )
            msg["Subject"] = f"[HiveRunr] {name} failed"
            msg["From"]    = smtp_user or "hiverunr@noreply.local"
            msg["To"]      = notify_email
            with smtplib.SMTP_SSL(smtp_host, smtp_port) as s:
                if smtp_user and smtp_pass:
                    s.login(smtp_user, smtp_pass)
                s.sendmail(smtp_user, notify_email, msg.as_string())
        except Exception as ex:
            log.warning(f"Email notify failed: {ex}")

@app.task(bind=True, name="app.worker.enqueue_workflow")
def enqueue_workflow(self, workflow_name: str, payload: dict):
    task_id = self.request.id
    try:
        from app.core.db import init_db
        init_db()
    except Exception:
        pass
    update_run(task_id, "running")
    try:
        from app.workflows import example
        workflows = {"example": example.run}
        if workflow_name not in workflows:
            raise ValueError(f"Unknown workflow: {workflow_name}")
        result = workflows[workflow_name](payload)
        update_run(task_id, "succeeded", result=result)
    except Exception as e:
        log.exception(f"Workflow {workflow_name} failed")
        update_run(task_id, "failed", result={"error": str(e)})
        _notify_failure(workflow_name, str(e), task_id)

@app.task(bind=True, name="app.worker.enqueue_script")
def enqueue_script(self, script_name: str, payload: dict):
    """Execute a standalone Python script from the workflows directory."""
    task_id = self.request.id
    try:
        from app.core.db import init_db
        init_db()
    except Exception:
        pass
    buf = io.StringIO()
    old_stdout, old_stderr = sys.stdout, sys.stderr
    try:
        # Mark running first — inside try so any DB error is caught too
        update_run(task_id, "running")
        script_path = SCRIPTS_DIR / f"{script_name}.py"
        if not script_path.exists():
            raise FileNotFoundError(f"Script not found: {script_name}.py")
        sys.stdout = sys.stderr = buf
        runpy.run_path(str(script_path), run_name="__main__",
                       init_globals={"__payload__": payload})
        sys.stdout, sys.stderr = old_stdout, old_stderr
        output = buf.getvalue()
        update_run(task_id, "succeeded", result={"output": output, "script": script_name})
    except Exception as e:
        sys.stdout, sys.stderr = old_stdout, old_stderr
        log.exception(f"Script {script_name} failed")
        _notify_failure(script_name, str(e), task_id)
        try:
            update_run(task_id, "failed",
                       result={"error": str(e), "script": script_name, "output": buf.getvalue()})
        except Exception:
            pass  # best-effort — don't let a DB error create a second FAILURE


@app.task(bind=True, name="app.worker.enqueue_graph")
def enqueue_graph(self, graph_id: int, payload: dict):
    task_id = self.request.id
    try:
        from app.core.db import init_db
        init_db()
    except Exception:
        pass
    update_run(task_id, "running")
    traces = []
    try:
        g = get_graph(graph_id)
        if not g:
            raise ValueError(f"Graph {graph_id} not found")
        graph_data = json.loads(g.get('graph_json') or '{}')
        from app.core.executor import run_graph
        msgs = []
        result = run_graph(graph_data, payload, logger=msgs.append)
        traces = result.get('traces', [])
        update_run(task_id, "succeeded", result=result, traces=traces)
    except Exception as e:
        log.exception(f"Graph {graph_id} failed")
        update_run(task_id, "failed", result={"error": str(e)}, traces=traces)
        _notify_failure(f"graph#{graph_id}", str(e), task_id)
EOF


# ── 14. app/scheduler.py ───────────────────────────────────────────────────
cat > "$PROJECT/app/scheduler.py" << 'EOF'
import os, time, logging
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from app.core.db import init_db, list_schedules

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def _make_job(sched):
    def job():
        from app.worker import enqueue_workflow, enqueue_graph
        import json
        payload = json.loads(sched["payload"]) if isinstance(sched["payload"], str) else (sched["payload"] or {})
        if sched.get("graph_id"):
            enqueue_graph.delay(sched["graph_id"], payload)
        elif sched.get("workflow"):
            enqueue_workflow.delay(sched["workflow"], payload)
    return job

def main():
    init_db()
    scheduler = BlockingScheduler()
    known = {}

    def refresh():
        schedules = list_schedules()
        current_ids = {s["id"] for s in schedules if s["enabled"]}
        for sid in list(known):
            if sid not in current_ids:
                try: scheduler.remove_job(str(sid))
                except Exception: pass
                del known[sid]
        for s in schedules:
            if not s["enabled"]: continue
            sid = s["id"]
            if sid not in known:
                try:
                    scheduler.add_job(_make_job(s), CronTrigger.from_crontab(s["cron"], timezone=s.get("timezone","UTC")),
                                      id=str(sid), replace_existing=True)
                    known[sid] = s
                    log.info(f"Scheduled: {s['name']} ({s['cron']})")
                except Exception as e:
                    log.error(f"Failed to schedule {s['name']}: {e}")

    scheduler.add_job(refresh, "interval", seconds=30, id="__refresh__")
    refresh()
    log.info("Scheduler started")
    scheduler.start()

if __name__ == "__main__":
    main()
EOF


# ── 15. app/static/admin.html ──────────────────────────────────────────────
cat > "$PROJECT/app/static/admin.html" << 'ADMIN_EOF'
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>HiveRunr</title>
<script src="https://unpkg.com/react@18/umd/react.production.min.js"></script>
<script src="https://unpkg.com/react-dom@18/umd/react-dom.production.min.js"></script>
<script src="https://unpkg.com/@babel/standalone/babel.min.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Inter',system-ui,sans-serif;background:#0f1117;color:#e2e8f0;min-height:100vh}
#root{display:flex;min-height:100vh}
.sidebar{width:220px;background:#1a1d2e;border-right:1px solid #2a2d3e;display:flex;flex-direction:column;padding:20px 0;flex-shrink:0;position:fixed;height:100vh;z-index:10}
.sidebar .logo{font-size:16px;font-weight:700;color:#a78bfa;padding:0 20px 24px;letter-spacing:-.3px}
.nav-item{display:flex;align-items:center;gap:10px;padding:10px 20px;cursor:pointer;color:#94a3b8;font-size:13px;font-weight:500;border-radius:0;transition:all .15s;border-left:3px solid transparent}
.nav-item:hover{background:#252840;color:#e2e8f0}
.nav-item.active{background:#252840;color:#a78bfa;border-left-color:#7c3aed}
.nav-icon{font-size:16px;width:20px;text-align:center}
.sidebar-footer{margin-top:auto;border-top:1px solid #2a2d3e;padding:14px 12px 10px}
.sidebar-footer .footer-label{font-size:10px;color:#4b5563;padding:0 8px;margin-bottom:8px;text-transform:uppercase;letter-spacing:.07em;font-weight:600}
.ext-link{display:flex;align-items:center;gap:9px;padding:8px 10px;border-radius:7px;color:#64748b;font-size:12px;font-weight:500;text-decoration:none;transition:all .15s;cursor:pointer}
.ext-link:hover{background:#252840;color:#e2e8f0}
.ext-link .el-icon{font-size:15px;width:20px;text-align:center}
.main{margin-left:220px;flex:1;padding:32px;max-width:1100px}
.page-title{font-size:22px;font-weight:700;margin-bottom:24px;color:#f1f5f9}
.card{background:#1a1d2e;border:1px solid #2a2d3e;border-radius:10px;padding:20px;margin-bottom:16px}
.card-title{font-size:13px;font-weight:600;color:#94a3b8;text-transform:uppercase;letter-spacing:.07em;margin-bottom:14px}
.stat-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:24px}
.stat-card{background:#1a1d2e;border:1px solid #2a2d3e;border-radius:10px;padding:18px 20px}
.stat-val{font-size:28px;font-weight:700;color:#e2e8f0}
.stat-lbl{font-size:12px;color:#64748b;margin-top:4px}
.badge{display:inline-flex;align-items:center;gap:4px;padding:3px 9px;border-radius:20px;font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.05em}
.badge-queued{background:#1e3a5f;color:#60a5fa}
.badge-running{background:#1c3238;color:#34d399;animation:pulse 1.5s infinite}
.badge-succeeded{background:#14532d;color:#4ade80}
.badge-failed{background:#3f1111;color:#f87171}
.badge-cancelled{background:#2d2d2d;color:#9ca3af}
.badge-credential{display:inline-block;padding:4px 10px;border-radius:6px;font-size:11px;font-weight:600;font-family:monospace;background:#0f1117;border:1px solid #2a2d3e}
.badge-credential.openai_api{background:#5b21b633;color:#d8b4fe;border-color:#7c3aed}
.badge-credential.smtp{background:#1e3a8a33;color:#93c5fd;border-color:#0284c7}
.badge-credential.telegram{background:#164e6333;color:#67e8f9;border-color:#0891b2}
.badge-credential.aws{background:#7c2d1233;color:#fed7aa;border-color:#d97706}
.badge-credential.ssh{background:#0f4c3a33;color:#34d399;border-color:#059669}
.badge-credential.ftp{background:#1e3a5f33;color:#7dd3fc;border-color:#0284c7}
.badge-credential.generic,.badge-credential.other{background:#2d2d2d33;color:#9ca3af;border-color:#4b5563}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.6}}
table{width:100%;border-collapse:collapse;font-size:13px}
th{text-align:left;color:#64748b;font-weight:500;padding:8px 12px;border-bottom:1px solid #2a2d3e;font-size:12px}
td{padding:10px 12px;border-bottom:1px solid #1e2235;color:#cbd5e1;vertical-align:middle}
tr:last-child td{border-bottom:none}
tr:hover td{background:#1e2235}
.btn{display:inline-flex;align-items:center;gap:5px;padding:6px 14px;border-radius:6px;font-size:12px;font-weight:500;border:none;cursor:pointer;transition:all .15s}
.btn-primary{background:#7c3aed;color:#fff}.btn-primary:hover{background:#6d28d9}
.btn-success{background:#059669;color:#fff}.btn-success:hover{background:#047857}
.btn-danger{background:#dc2626;color:#fff}.btn-danger:hover{background:#b91c1c}
.btn-ghost{background:#2a2d3e;color:#94a3b8;border:1px solid #374151}.btn-ghost:hover{color:#e2e8f0;background:#374151}
.btn-warning{background:#d97706;color:#fff}.btn-warning:hover{background:#b45309}
.btn:disabled{opacity:.45;cursor:not-allowed}
.form-row{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px}
.form-group{margin-bottom:12px}
label{display:block;font-size:11px;color:#94a3b8;font-weight:500;margin-bottom:5px}
input,select,textarea{width:100%;background:#0f1117;color:#e2e8f0;border:1px solid #2a2d3e;border-radius:6px;padding:8px 10px;font-size:13px;font-family:inherit}
input:focus,select:focus,textarea:focus{outline:none;border-color:#7c3aed}
.wf-pill{display:flex;align-items:center;justify-content:space-between;background:#0f1117;border:1px solid #2a2d3e;border-radius:8px;padding:10px 14px;margin-bottom:8px}
.wf-name{font-size:13px;font-weight:500}
.wf-enabled{font-size:11px;color:#059669}
.wf-disabled{font-size:11px;color:#dc2626}
.log-file{background:#0f1117;border:1px solid #2a2d3e;border-radius:6px;padding:10px 14px;font-size:12px;font-family:monospace;cursor:pointer;margin-bottom:6px;color:#94a3b8;transition:border-color .15s}
.log-file:hover{border-color:#7c3aed;color:#e2e8f0}
.log-content{background:#0a0c14;border:1px solid #2a2d3e;border-radius:6px;padding:14px;font-size:11px;font-family:monospace;white-space:pre-wrap;max-height:400px;overflow-y:auto;color:#94a3b8;line-height:1.7}
.actions-row{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px}
.empty-state{color:#4b5563;font-size:13px;text-align:center;padding:40px 0}
.toast{position:fixed;bottom:20px;right:20px;background:#1a1d2e;border:1px solid #2a2d3e;border-radius:8px;padding:10px 16px;font-size:13px;z-index:9999;display:flex;align-items:center;gap:8px;box-shadow:0 8px 24px rgba(0,0,0,.4);animation:slideUp .2s ease}
.toast.ok{border-color:#059669;color:#34d399}
.toast.err{border-color:#dc2626;color:#f87171}
@keyframes slideUp{from{transform:translateY(12px);opacity:0}to{transform:translateY(0);opacity:1}}
.info-box{background:#0f1117;border:1px solid #2a2d3e;border-left:3px solid #7c3aed;border-radius:6px;padding:12px 14px;margin-bottom:16px;font-size:12px;color:#94a3b8;line-height:1.6}
.trace-table{margin-top:10px;background:#0f1117;border:1px solid #2a2d3e;border-radius:6px;padding:0;overflow:hidden}
.trace-table table{margin:0;font-size:12px}
.trace-table th,.trace-table td{padding:8px 10px}
.trace-status-dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:5px}
.trace-status-dot.ok{background:#4ade80}
.trace-status-dot.err{background:#f87171}
.trace-status-dot.skipped{background:#64748b}
.modal-overlay{position:fixed;inset:0;background:rgba(0,0,0,.75);z-index:1000;display:flex;align-items:center;justify-content:center;backdrop-filter:blur(4px)}
.modal{background:#1a1d2e;border:1px solid #2a2d3e;border-radius:12px;padding:22px;min-width:400px;max-width:600px;max-height:80vh;overflow-y:auto;box-shadow:0 20px 60px rgba(0,0,0,.6)}
.modal h2{font-size:15px;font-weight:600;margin-bottom:14px;color:#e2e8f0}
.modal-section{font-size:10px;color:#64748b;font-weight:600;text-transform:uppercase;letter-spacing:.06em;margin:12px 0 8px;padding-bottom:5px;border-bottom:1px solid #2a2d3e}
.modal-btns{display:flex;gap:8px;justify-content:flex-end;margin-top:16px}
.version-row{padding:10px;border-bottom:1px solid #2a2d3e;display:flex;align-items:center;justify-content:space-between}
.version-row:last-child{border-bottom:none}
.version-info{flex:1;min-width:0;font-size:12px}
.version-num{font-weight:600;color:#e2e8f0;margin-bottom:2px}
.version-date{font-size:11px;color:#64748b}
.version-note{font-size:11px;color:#94a3b8;margin-top:2px}
.expandable-row{cursor:pointer;transition:background .15s}
.expandable-row:hover td{background:#252840}
.trace-collapsed{background:#252840;border:1px solid #2a2d3e;border-radius:6px;padding:12px 14px;font-size:11px;color:#94a3b8;display:none;margin-top:8px}
.trace-collapsed.show{display:block}
.stat-cards{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:24px}
.stat-card{background:#1a1d2e;border:1px solid #2a2d3e;border-radius:10px;padding:20px;text-align:center}
.stat-value{font-size:32px;font-weight:700;margin-bottom:4px;line-height:1}
.stat-label{font-size:12px;color:#64748b}
.bar-chart{display:flex;align-items:flex-end;gap:3px;height:80px;margin:12px 0 6px}
.bar-col{display:flex;flex-direction:column-reverse;align-items:center;flex:1;gap:1px}
.bar-seg{width:100%;border-radius:2px 2px 0 0;min-height:2px;transition:height .3s}
.bar-label{font-size:9px;color:#4b5563;margin-top:4px;text-align:center}
.script-list{width:210px;flex-shrink:0;background:#1a1d2e;border-right:1px solid #2a2d3e;display:flex;flex-direction:column;height:calc(100vh - 160px)}
.script-item{padding:10px 14px;cursor:pointer;border-bottom:1px solid #1e2235;transition:background .12s}
.script-item:hover{background:#2a2d3e}
.script-item.active{background:#2a2d3e;border-left:2px solid #7c3aed}
.script-item-name{font-size:13px;color:#e2e8f0;font-weight:500}
.script-item-meta{font-size:10px;color:#4b5563;margin-top:2px}
.editor-wrap{flex:1;display:flex;flex-direction:column;overflow:hidden}
.editor-toolbar{padding:8px 14px;background:#1a1d2e;border-bottom:1px solid #2a2d3e;display:flex;align-items:center;gap:8px}
.editor-container{flex:1}
</style>
</head>
<body>
<div id="root"></div>
<script type="text/babel">
const { useState, useEffect, useCallback, useRef } = React;

const params = new URLSearchParams(window.location.search);
const TOKEN  = params.get("token") || "";

async function api(method, path, body) {
  const r = await fetch(path, {
    method,
    headers: {"Content-Type":"application/json","x-admin-token": TOKEN},
    body: body ? JSON.stringify(body) : undefined
  });
  if (!r.ok) { const e = await r.json().catch(()=>({detail:r.statusText})); throw new Error(e.detail || r.statusText); }
  return r.json();
}

function Toast({ msg, type, onDone }) {
  useEffect(()=>{ const t=setTimeout(onDone,3000); return ()=>clearTimeout(t); },[]);
  return <div className={`toast ${type==="error"?"err":"ok"}`}>{type==="error"?"✗":"✓"} {msg}</div>;
}

// ── Dashboard ──────────────────────────────────────────────────────────────
function Dashboard({ showToast }) {
  const [runs, setRuns]       = useState([]);
  const [workflows, setWfs]   = useState([]);
  const [loading, setLoading] = useState(true);
  const [expandedRunId, setExpandedRunId] = useState(null);

  const load = useCallback(async () => {
    try {
      const [r, w] = await Promise.all([api("GET","/api/runs"), api("GET","/api/workflows")]);
      setRuns(r); setWfs(w);
    } catch(e) { showToast(e.message,"error"); }
    setLoading(false);
  }, []);

  useEffect(() => { load(); const t=setInterval(load,5000); return ()=>clearInterval(t); }, []);

  const stats = {
    total: runs.length,
    active: runs.filter(r=>r.status==="running"||r.status==="queued").length,
    ok: runs.filter(r=>r.status==="succeeded").length,
    failed: runs.filter(r=>r.status==="failed").length
  };

  async function toggleWf(name) {
    try { await api("POST",`/api/workflows/${name}/toggle`); load(); showToast("Toggled"); }
    catch(e) { showToast(e.message,"error"); }
  }
  async function runWf(name) {
    try { await api("POST",`/api/workflows/${name}/run`,{payload:{}}); load(); showToast("Queued!"); }
    catch(e) { showToast(e.message,"error"); }
  }
  async function deleteRun(id) {
    try { await api("DELETE",`/api/runs/${id}`); load(); }
    catch(e) { showToast(e.message,"error"); }
  }
  async function cancelRun(id) {
    try { await api("POST",`/api/runs/${id}/cancel`); load(); showToast("Cancelled"); }
    catch(e) { showToast(e.message,"error"); }
  }
  async function replayRun(id) {
    try { await api("POST",`/api/runs/${id}/replay`); load(); showToast("Queued for replay"); }
    catch(e) { showToast(e.message,"error"); }
  }

  return (
    <div>
      <h1 className="page-title">Dashboard</h1>
      <div className="stat-grid">
        <div className="stat-card"><div className="stat-val">{stats.total}</div><div className="stat-lbl">Total Runs</div></div>
        <div className="stat-card"><div className="stat-val" style={{color:"#60a5fa"}}>{stats.active}</div><div className="stat-lbl">Active</div></div>
        <div className="stat-card"><div className="stat-val" style={{color:"#4ade80"}}>{stats.ok}</div><div className="stat-lbl">Succeeded</div></div>
        <div className="stat-card"><div className="stat-val" style={{color:"#f87171"}}>{stats.failed}</div><div className="stat-lbl">Failed</div></div>
      </div>
      {workflows.length > 0 && (
        <div className="card" style={{marginBottom:24}}>
          <div className="card-title">Python Workflows</div>
          {workflows.map(w=>(
            <div key={w.name} className="wf-pill">
              <div>
                <div className="wf-name">{w.name}</div>
                <div className={w.enabled?"wf-enabled":"wf-disabled"}>{w.enabled?"● enabled":"○ disabled"}</div>
              </div>
              <div style={{display:"flex",gap:8}}>
                <button className="btn btn-ghost" onClick={()=>toggleWf(w.name)}>Toggle</button>
                <button className="btn btn-success" disabled={!w.enabled} onClick={()=>runWf(w.name)}>▶ Run</button>
              </div>
            </div>
          ))}
        </div>
      )}
      <div className="card">
        <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:14}}>
          <div className="card-title" style={{marginBottom:0}}>Recent Runs</div>
          <button className="btn btn-ghost" onClick={()=>api("DELETE","/api/runs").then(load)}>Clear all</button>
        </div>
        {loading ? <div className="empty-state">Loading…</div> : runs.length === 0 ? (
          <div className="empty-state">No runs yet. Trigger a workflow to get started.</div>
        ) : (
          <>
            <table>
              <thead><tr><th>#</th><th>Workflow</th><th>Status</th><th>Started</th><th></th></tr></thead>
              <tbody>
                {runs.map(r=>(
                  <React.Fragment key={r.id}>
                    <tr className="expandable-row" onClick={()=>setExpandedRunId(expandedRunId===r.id?null:r.id)} style={{cursor:'pointer'}}>
                      <td style={{color:"#4b5563"}}>{r.id}</td>
                      <td>{r.workflow || (r.graph_id ? `graph #${r.graph_id}` : "—")}</td>
                      <td><span className={`badge badge-${r.status}`}>{r.status}</span></td>
                      <td style={{color:"#64748b"}}>{r.created_at ? new Date(r.created_at).toLocaleString() : "—"}</td>
                      <td>
                        <div style={{display:"flex",gap:6}}>
                          {r.graph_id && <button className="btn btn-ghost" onClick={(e)=>{e.stopPropagation();replayRun(r.id)}}>▶ Replay</button>}
                          {(r.status==="queued"||r.status==="running") &&
                            <button className="btn btn-warning" onClick={(e)=>{e.stopPropagation();cancelRun(r.id)}}>Cancel</button>}
                          <button className="btn btn-danger" onClick={(e)=>{e.stopPropagation();deleteRun(r.id)}}>✕</button>
                        </div>
                      </td>
                    </tr>
                    {expandedRunId===r.id && (
                      <tr style={{background:"#0f1117"}}>
                        <td colSpan="5" style={{padding:"12px"}}>
                          {r.initial_payload && Object.keys(r.initial_payload).length > 0 && (
                            <div style={{marginBottom:12}}>
                              <div style={{fontSize:11,fontWeight:600,color:"#94a3b8",marginBottom:6}}>Initial Payload</div>
                              <div style={{background:"#0a0c14",border:"1px solid #2a2d3e",borderRadius:6,padding:10,fontFamily:"monospace",fontSize:11,color:"#94a3b8",maxHeight:200,overflowY:"auto"}}>
                                {JSON.stringify(r.initial_payload,null,2)}
                              </div>
                            </div>
                          )}
                          {/* ── run output / error ── */}
                          {r.result && (r.result.output || r.result.error) && (
                            <div style={{marginBottom:12}}>
                              <div style={{fontSize:11,fontWeight:600,color: r.result.error?"#f87171":"#4ade80",marginBottom:6}}>
                                {r.result.error ? "❌ Error" : "✅ Output"}
                              </div>
                              <div style={{
                                background:"#0a0c14",border:`1px solid ${r.result.error?"#7f1d1d":"#14532d"}`,
                                borderRadius:6,padding:10,fontFamily:"monospace",fontSize:11,
                                color: r.result.error?"#fca5a5":"#86efac",
                                maxHeight:300,overflowY:"auto",whiteSpace:"pre-wrap",wordBreak:"break-word"
                              }}>
                                {r.result.error || r.result.output}
                              </div>
                            </div>
                          )}
                          {r.traces && r.traces.length > 0 ? (
                            <div>
                              <div style={{fontSize:11,fontWeight:600,color:"#94a3b8",marginBottom:6}}>Trace</div>
                              <div className="trace-table">
                                <table>
                                  <thead><tr><th>Node</th><th>Type</th><th>Status</th><th>Duration (ms)</th><th>Attempts</th><th>Output/Error</th></tr></thead>
                                  <tbody>
                                    {r.traces.map((t,idx)=>(
                                      <tr key={idx}>
                                        <td style={{fontFamily:"monospace",fontSize:11}}>{t.node_id}</td>
                                        <td style={{fontSize:11}}>{t.type}</td>
                                        <td>
                                          <span className={`trace-status-dot ${t.status===undefined||t.status==="ok"?"ok":t.status==="error"?"err":"skipped"}`}/>
                                          {t.status===undefined||t.status==="ok"?"ok":t.status==="error"?"error":"skipped"}
                                        </td>
                                        <td>{t.duration_ms ?? "—"}</td>
                                        <td>{t.attempts ?? 1}</td>
                                        <td>
                                          <div style={{maxWidth:200,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap",fontSize:11}}>
                                            {t.error ? <span style={{color:"#f87171"}}>{String(t.error).slice(0,50)}</span> :
                                             t.output ? <span style={{color:"#94a3b8"}}>{typeof t.output==="string"?t.output:JSON.stringify(t.output).slice(0,50)}</span> :
                                             "—"}
                                          </div>
                                        </td>
                                      </tr>
                                    ))}
                                  </tbody>
                                </table>
                              </div>
                            </div>
                          ) : <div style={{color:"#4b5563",fontSize:12}}>No trace data available</div>}
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                ))}
              </tbody>
            </table>
          </>
        )}
      </div>
    </div>
  );
}

// ── Metrics ────────────────────────────────────────────────────────────────
function relativeTime(ts) {
  const d = Math.floor((Date.now() - new Date(ts)) / 1000);
  if (d < 60) return `${d}s ago`;
  if (d < 3600) return `${Math.floor(d/60)}m ago`;
  if (d < 86400) return `${Math.floor(d/3600)}h ago`;
  return `${Math.floor(d/86400)}d ago`;
}

function Metrics({ showToast }) {
  const [metrics, setMetrics] = useState(null);
  const [failingFlows, setFailingFlows] = useState([]);
  const [recentRuns, setRecentRuns] = useState([]);
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    try {
      const m = await api("GET", "/api/metrics");
      setMetrics(m);

      const runs = await api("GET", "/api/runs");
      const failMap = {};
      runs.forEach(r => {
        if (r.status === "failed") {
          const wf = r.workflow || `graph #${r.graph_id || '?'}`;
          failMap[wf] = (failMap[wf] || 0) + 1;
        }
      });
      const failing = Object.entries(failMap).sort((a, b) => b[1] - a[1]).slice(0, 5);
      setFailingFlows(failing);
      setRecentRuns(runs.slice(0, 10));
    } catch(e) { showToast(e.message, "error"); }
    setLoading(false);
  }, []);

  useEffect(() => { load(); }, []);

  if (loading) return <div className="empty-state">Loading metrics…</div>;
  if (!metrics) return <div className="empty-state">No metrics available.</div>;

  const lastDay = (metrics.run_counts || []).slice(-14);
  const maxRuns = Math.max(...lastDay.map(d => (d.succeeded || 0) + (d.failed || 0)), 1);

  return (
    <div>
      <h1 className="page-title">Metrics</h1>

      <div className="stat-cards">
        <div className="stat-card">
          <div className="stat-value" style={{color:"#a78bfa"}}>{metrics.total_runs || 0}</div>
          <div className="stat-label">Total Runs</div>
        </div>
        <div className="stat-card">
          <div className="stat-value" style={{color:"#4ade80"}}>{metrics.succeeded_runs || 0}</div>
          <div className="stat-label">Succeeded</div>
        </div>
        <div className="stat-card">
          <div className="stat-value" style={{color:"#f87171"}}>{metrics.failed_runs || 0}</div>
          <div className="stat-label">Failed</div>
        </div>
        <div className="stat-card">
          <div className="stat-value" style={{color:"#38bdf8"}}>{Math.round(metrics.avg_duration_ms || 0)}</div>
          <div className="stat-label">Avg Duration (ms)</div>
        </div>
      </div>

      <div className="card">
        <div className="card-title">Last 14 Days</div>
        <div className="bar-chart">
          {lastDay.map((day, idx) => {
            const succeeded = day.succeeded || 0;
            const failed = day.failed || 0;
            const total = succeeded + failed;
            const sucHeight = maxRuns > 0 ? (succeeded / maxRuns) * 70 : 0;
            const failHeight = maxRuns > 0 ? (failed / maxRuns) * 70 : 0;
            const dateStr = new Date(day.date).toLocaleDateString().slice(-2).padStart(2, '0');
            return (
              <div key={idx} className="bar-col" title={`${dateStr}: ${succeeded}✓ ${failed}✗`}>
                {succeeded > 0 && <div className="bar-seg" style={{height:`${sucHeight}px`,background:"#4ade80"}}/>}
                {failed > 0 && <div className="bar-seg" style={{height:`${failHeight}px`,background:"#f87171"}}/>}
                <div className="bar-label">{dateStr}</div>
              </div>
            );
          })}
        </div>
      </div>

      {failingFlows.length > 0 && (
        <div className="card">
          <div className="card-title">Top Failing Flows</div>
          <table>
            <thead><tr><th>Flow</th><th>Failed</th></tr></thead>
            <tbody>
              {failingFlows.map(([name, count]) => (
                <tr key={name}>
                  <td>{name}</td>
                  <td><span className="badge badge-failed">{count}</span></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {recentRuns.length > 0 && (
        <div className="card">
          <div className="card-title">Recent Runs</div>
          <table>
            <thead><tr><th>Flow</th><th>Status</th><th>Duration (ms)</th><th>Time</th></tr></thead>
            <tbody>
              {recentRuns.map(r => (
                <tr key={r.id}>
                  <td style={{fontSize:12}}>{r.workflow || `graph #${r.graph_id || '?'}`}</td>
                  <td><span className={`badge badge-${r.status}`}>{r.status}</span></td>
                  <td style={{fontSize:12}}>{r.duration_ms || "—"}</td>
                  <td style={{fontSize:12,color:"#64748b"}}>{relativeTime(r.started_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// ── Scripts ────────────────────────────────────────────────────────────────
function Scripts({ showToast }) {
  const [scripts, setScripts]           = useState([]);
  const [selectedScript, setSelectedScript] = useState(null);
  const [loadingContent, setLoadingContent] = useState(false);
  const [newName, setNewName]           = useState("");
  const [creating, setCreating]         = useState(false);
  const monacoRef          = useRef(null);
  const editorContainerRef = useRef(null);

  // Fetch the script list; if nothing selected yet, auto-select the first
  const load = useCallback(async (keepSelected) => {
    try {
      const s = await api("GET", "/api/scripts");
      setScripts(Array.isArray(s) ? s : []);
      if (!keepSelected && s.length > 0) {
        await selectScript(s[0].name);
      }
    } catch(e) { showToast(e.message, "error"); }
  }, []);

  // Fetch full script content and set as selected
  async function selectScript(name) {
    setLoadingContent(true);
    try {
      const full = await api("GET", `/api/scripts/${name}`);
      setSelectedScript(full);        // full = { name, content }
    } catch(e) { showToast(e.message, "error"); }
    setLoadingContent(false);
  }

  useEffect(() => { load(false); }, []);

  // (Re-)create Monaco editor whenever the selected script name changes
  useEffect(() => {
    if (!selectedScript || !editorContainerRef.current) return;
    if (monacoRef.current) { monacoRef.current.dispose(); monacoRef.current = null; }

    function initEditor() {
      monacoRef.current = window.monaco.editor.create(editorContainerRef.current, {
        value: selectedScript.content || '',
        language: 'python',
        theme: 'vs-dark',
        fontSize: 13,
        minimap: { enabled: false },
        scrollBeyondLastLine: false,
        automaticLayout: true,
      });
    }

    if (window.monaco) { initEditor(); }
    else {
      const script = document.createElement('script');
      script.src = 'https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.44.0/min/vs/loader.min.js';
      script.onload = () => {
        window.require.config({ paths: { vs: 'https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.44.0/min/vs' }});
        window.require(['vs/editor/editor.main'], initEditor);
      };
      document.head.appendChild(script);
    }

    return () => { if (monacoRef.current) { monacoRef.current.dispose(); monacoRef.current = null; } };
  }, [selectedScript?.name]);

  async function createScript(e) {
    e.preventDefault();
    if (!newName.trim()) { showToast("Script name required", "error"); return; }
    setCreating(true);
    try {
      await api("POST", "/api/scripts", { name: newName, content: `# ${newName}.py\n` });
      const created = newName;
      setNewName("");
      await load(true);
      await selectScript(created);
      showToast("Script created");
    } catch(e) { showToast(e.message, "error"); }
    setCreating(false);
  }

  async function saveScript() {
    if (!selectedScript || !monacoRef.current) return;
    try {
      const content = monacoRef.current.getValue();
      await api("PUT", `/api/scripts/${selectedScript.name}`, { content });
      // Update local state so content is current without re-fetching
      setSelectedScript(s => ({ ...s, content }));
      await load(true);
      showToast("Saved");
    } catch(e) { showToast(e.message, "error"); }
  }

  async function deleteScript() {
    if (!selectedScript) return;
    if (!window.confirm(`Delete "${selectedScript.name}"?`)) return;
    try {
      await api("DELETE", `/api/scripts/${selectedScript.name}`);
      showToast("Deleted");
      setSelectedScript(null);
      await load(false);
    } catch(e) { showToast(e.message, "error"); }
  }

  async function runScript() {
    if (!selectedScript) return;
    try {
      const res = await api("POST", `/api/scripts/${selectedScript.name}/run`);
      showToast(`Queued: task ${res.task_id}`);
    } catch(e) { showToast(e.message, "error"); }
  }

  return (
    <div style={{display:"flex",flexDirection:"column",height:"100vh"}}>
      <h1 className="page-title">Scripts</h1>
      <div style={{display:"flex",flex:1,overflow:"hidden",marginTop:-24,marginLeft:-32,marginRight:-32,marginBottom:-32}}>
        <div className="script-list">
          <div style={{padding:"10px 14px",borderBottom:"1px solid #2a2d3e"}}>
            <form onSubmit={createScript}>
              <input
                type="text"
                placeholder="New script name"
                value={newName}
                onChange={e => setNewName(e.target.value)}
                style={{width:"100%",marginBottom:8}}
              />
              <button type="submit" className="btn btn-primary" style={{width:"100%"}} disabled={creating}>
                {creating ? "Creating…" : "+ Create"}
              </button>
            </form>
          </div>
          <div style={{flex:1,overflowY:"auto"}}>
            {scripts.length === 0 && (
              <div style={{padding:"16px 14px",color:"#475569",fontSize:12}}>No scripts yet. Create one above.</div>
            )}
            {scripts.map(s => (
              <div
                key={s.name}
                className={`script-item${selectedScript?.name === s.name ? " active" : ""}`}
                onClick={() => selectScript(s.name)}
              >
                <div className="script-item-name">{s.name}</div>
                <div className="script-item-meta">{s.size} bytes · {new Date(s.modified * 1000).toLocaleDateString()}</div>
              </div>
            ))}
          </div>
        </div>
        <div className="editor-wrap">
          {loadingContent ? (
            <div className="empty-state" style={{alignSelf:"center",marginTop:"40%"}}>Loading…</div>
          ) : selectedScript ? (
            <>
              <div className="editor-toolbar">
                <span style={{color:"#64748b",fontSize:12,marginRight:"auto",fontFamily:"monospace"}}>{selectedScript.name}.py</span>
                <button className="btn btn-primary" onClick={saveScript}>💾 Save</button>
                <button className="btn btn-success" onClick={runScript}>▶ Run</button>
                <button className="btn btn-danger" onClick={deleteScript}>✕ Delete</button>
              </div>
              <div className="editor-container" ref={editorContainerRef}/>
            </>
          ) : (
            <div className="empty-state" style={{alignSelf:"center",marginTop:"40%"}}>Select a script or create a new one.</div>
          )}
        </div>
      </div>
    </div>
  );
}

// ── Credentials ────────────────────────────────────────────────────────────

// Structured field definitions per credential type
const CRED_SCHEMAS = {
  // ── generic ──────────────────────────────────────────────────────────────
  generic:    { label:"Generic Secret",          fields:[{k:"secret",  l:"Secret Value",      ph:"any secret value",                secret:true}] },

  // ── messaging / notifications ─────────────────────────────────────────────
  slack:      { label:"Slack Incoming Webhook",  fields:[{k:"webhook_url", l:"Webhook URL",   ph:"https://hooks.slack.com/services/T…/B…/…", secret:true}] },
  webhook:    { label:"Webhook URL",             fields:[{k:"url",         l:"Webhook URL",   ph:"https://discord.com/api/webhooks/…",        secret:true}] },
  telegram:   { label:"Telegram Bot",            fields:[{k:"secret",  l:"Bot Token",         ph:"123456:ABC…",                     secret:true},
                                                         {k:"chat_id", l:"Default Chat ID",   ph:"-100123456789"}] },

  // ── HTTP / API auth ───────────────────────────────────────────────────────
  api_key:    { label:"API Key / Bearer Token",  fields:[{k:"key",      l:"API Key / Token",  ph:"sk-… or eyJ…",                   secret:true},
                                                         {k:"header",   l:"Header name",      ph:"Authorization"}] },
  basic_auth: { label:"HTTP Basic Auth",         fields:[{k:"username", l:"Username",         ph:"admin"},
                                                         {k:"password", l:"Password",         ph:"••••••••",                        secret:true}] },
  openai_api: { label:"OpenAI / LLM API Key",   fields:[{k:"secret",   l:"API Key",          ph:"sk-…",                            secret:true},
                                                         {k:"base_url", l:"Base URL (optional)", ph:"https://api.openai.com/v1"}] },

  // ── email ─────────────────────────────────────────────────────────────────
  smtp:       { label:"SMTP Server",             fields:[{k:"host",    l:"Host",              ph:"smtp.gmail.com"},
                                                         {k:"port",    l:"Port",              ph:"465"},
                                                         {k:"user",    l:"Username",          ph:"you@gmail.com"},
                                                         {k:"pass",    l:"Password",          ph:"app-password",                    secret:true}] },

  // ── infrastructure ────────────────────────────────────────────────────────
  ssh:        { label:"SSH Server",              fields:[{k:"host",    l:"Host / IP",         ph:"192.168.1.1"},
                                                         {k:"port",    l:"Port",              ph:"22"},
                                                         {k:"username",l:"Username",          ph:"admin"},
                                                         {k:"password",l:"Password",          ph:"••••••••",                        secret:true},
                                                         {k:"key",     l:"Private Key (PEM, optional)", ph:"-----BEGIN RSA PRIVATE KEY-----…", textarea:true}] },
  sftp:       { label:"SFTP / FTP Server",       fields:[{k:"protocol",l:"Protocol",         ph:"sftp", select:["sftp","ftp"]},
                                                         {k:"host",    l:"Host / IP",         ph:"files.example.com"},
                                                         {k:"port",    l:"Port",              ph:"22 (sftp) · 21 (ftp)"},
                                                         {k:"username",l:"Username",          ph:"ftpuser"},
                                                         {k:"password",l:"Password",          ph:"••••••••",                        secret:true}] },
  aws:        { label:"AWS Credentials",         fields:[{k:"access_key_id",     l:"Access Key ID",     ph:"AKIAIOSFODNN7EXAMPLE"},
                                                         {k:"secret_access_key", l:"Secret Access Key", ph:"wJalrXUt…",            secret:true},
                                                         {k:"region",            l:"Default Region",    ph:"us-east-1"}] },
};


// Build blank sub-field state for a given type
function blankSubFields(type) {
  const schema = CRED_SCHEMAS[type] || CRED_SCHEMAS.generic;
  return Object.fromEntries(schema.fields.map(f => [f.k, '']));
}

// Serialize sub-fields to the `secret` string stored in DB
function encodeSecret(type, subFields) {
  const schema = CRED_SCHEMAS[type] || CRED_SCHEMAS.generic;
  if (schema.fields.length === 1 && schema.fields[0].k === 'secret') {
    return subFields.secret || '';
  }
  // Structured: JSON-encode all non-empty fields
  const obj = {};
  schema.fields.forEach(f => { if (subFields[f.k]) obj[f.k] = subFields[f.k]; });
  return JSON.stringify(obj);
}

// Human-readable summary of stored fields (for table display)
function credFieldSummary(type) {
  const schema = CRED_SCHEMAS[type] || CRED_SCHEMAS.generic;
  if (schema.fields.length === 1) return null;
  return schema.fields.filter(f=>!f.secret).map(f=>f.l).join(' · ');
}

function Credentials({ showToast }) {
  const [credentials, setCredentials] = useState([]);
  const [form, setForm] = useState({name:"", type:"generic", note:"", sub: blankSubFields("generic")});
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    try { setCredentials(await api("GET","/api/credentials")); }
    catch(e) { showToast(e.message,"error"); }
    setLoading(false);
  }, []);

  useEffect(()=>{ load(); },[]);

  const validateName = (name) => /^[a-zA-Z0-9_-]+$/.test(name);

  function changeType(t) {
    setForm(f => ({...f, type:t, sub: blankSubFields(t)}));
  }

  async function create(e) {
    e.preventDefault();
    if(!form.name.trim()) { showToast("Name required","error"); return }
    if(!validateName(form.name)) { showToast("Name may only contain letters, numbers, hyphens, underscores","error"); return }
    const secret = encodeSecret(form.type, form.sub);
    if(!secret.trim()) { showToast("At least one field is required","error"); return }
    try {
      await api("POST","/api/credentials",{name:form.name, type:form.type, secret, note:form.note});
      setForm({name:"", type:"generic", note:"", sub: blankSubFields("generic")});
      load();
      showToast("Credential created");
    } catch(err) { showToast(err.message,"error"); }
  }

  async function del(id) {
    if(!window.confirm("Delete this credential? This cannot be undone.")) return;
    try { await api("DELETE",`/api/credentials/${id}`); load(); showToast("Deleted"); }
    catch(e) { showToast(e.message,"error"); }
  }

  const schema = CRED_SCHEMAS[form.type] || CRED_SCHEMAS.generic;

  // Usage hint shown in the info box based on selected type
  const usageHints = {
    generic:    <>Use <code style={{color:"#a78bfa"}}>{"{{creds.name}}"}</code> to inject the secret value anywhere in a node config.</>,
    slack:      <>In the Slack node set <strong>Webhook URL</strong> to <code style={{color:"#a78bfa"}}>{"{{creds.name.webhook_url}}"}</code>. The URL is stored encrypted.</>,
    webhook:    <>Use <code style={{color:"#a78bfa"}}>{"{{creds.name.url}}"}</code> in any node field that accepts a URL (e.g. HTTP Request, Slack).</>,
    telegram:   <>Use <code style={{color:"#a78bfa"}}>{"{{creds.name}}"}</code> for the token, <code style={{color:"#a78bfa"}}>{"{{creds.name.chat_id}}"}</code> for the chat ID in Telegram nodes.</>,
    api_key:    <>In HTTP Request headers use <code style={{color:"#a78bfa"}}>{'{"Authorization":"Bearer {{creds.name.key}}"}'}</code>. <code style={{color:"#a78bfa"}}>{"{{creds.name.header}}"}</code> gives the header name if set.</>,
    basic_auth: <>In HTTP Request headers use <code style={{color:"#a78bfa"}}>{'{"Authorization":"Basic <base64>"}'}</code>, or reference <code style={{color:"#a78bfa"}}>{"{{creds.name.username}}"}</code> and <code style={{color:"#a78bfa"}}>{"{{creds.name.password}}"}</code> directly.</>,
    openai_api: <>Use <code style={{color:"#a78bfa"}}>{"{{creds.name}}"}</code> as the API key in LLM Call nodes. Set <code style={{color:"#a78bfa"}}>{"{{creds.name.base_url}}"}</code> as the API base for non-OpenAI providers.</>,
    smtp:       <>In Send Email nodes set <strong>SMTP Credential</strong> to <code style={{color:"#a78bfa"}}>name</code>. Fields are filled automatically. Or reference: <code style={{color:"#a78bfa"}}>{"{{creds.name.host}}"}</code>, <code style={{color:"#a78bfa"}}>{"{{creds.name.user}}"}</code>.</>,
    ssh:        <>In SSH nodes set <strong>SSH Credential</strong> to <code style={{color:"#a78bfa"}}>name</code>. Fields are filled automatically. Or reference: <code style={{color:"#a78bfa"}}>{"{{creds.name.host}}"}</code>, <code style={{color:"#a78bfa"}}>{"{{creds.name.username}}"}</code>.</>,
    sftp:       <>In SFTP nodes set <strong>SFTP Credential</strong> to <code style={{color:"#a78bfa"}}>name</code>. Fields are filled automatically. Or reference: <code style={{color:"#a78bfa"}}>{"{{creds.name.host}}"}</code>, <code style={{color:"#a78bfa"}}>{"{{creds.name.username}}"}</code>.</>,
    aws:        <>Reference fields directly: <code style={{color:"#a78bfa"}}>{"{{creds.name.access_key_id}}"}</code>, <code style={{color:"#a78bfa"}}>{"{{creds.name.secret_access_key}}"}</code>, <code style={{color:"#a78bfa"}}>{"{{creds.name.region}}"}</code>.</>,
  };

  return (
    <div>
      <h1 className="page-title">Credentials</h1>
      <div className="info-box">
        🔑 Secrets are never exposed in flow exports or logs.&nbsp;
        {usageHints[form.type] || usageHints.generic}
      </div>
      <div className="card">
        <div className="card-title">New Credential</div>
        <form onSubmit={create}>
          <div className="form-row">
            <div className="form-group">
              <label>Name <span style={{color:"#64748b",fontWeight:400}}>(unique · letters, numbers, hyphens, underscores)</span></label>
              <input required value={form.name} onChange={e=>setForm({...form,name:e.target.value})} placeholder="my-smtp-server"/>
            </div>
            <div className="form-group">
              <label>Type</label>
              <select value={form.type} onChange={e=>changeType(e.target.value)}>
                <optgroup label="Generic">
                  <option value="generic">Generic Secret</option>
                </optgroup>
                <optgroup label="Messaging &amp; Notifications">
                  <option value="slack">Slack Incoming Webhook</option>
                  <option value="webhook">Webhook URL (Discord, Teams…)</option>
                  <option value="telegram">Telegram Bot</option>
                </optgroup>
                <optgroup label="HTTP / API Auth">
                  <option value="api_key">API Key / Bearer Token</option>
                  <option value="basic_auth">HTTP Basic Auth</option>
                  <option value="openai_api">OpenAI / LLM API Key</option>
                </optgroup>
                <optgroup label="Email">
                  <option value="smtp">SMTP Server</option>
                </optgroup>
                <optgroup label="Infrastructure">
                  <option value="ssh">SSH Server</option>
                  <option value="sftp">SFTP / FTP Server</option>
                  <option value="aws">AWS Credentials</option>
                </optgroup>
              </select>
            </div>
          </div>
          {/* Type-specific fields */}
          <div style={{background:"#0f1117",border:"1px solid #1e2130",borderRadius:8,padding:"14px 16px",marginBottom:12}}>
            <div style={{fontSize:11,color:"#64748b",marginBottom:10,fontWeight:600,textTransform:"uppercase",letterSpacing:"0.05em"}}>{schema.label}</div>
            <div className="form-row" style={{flexWrap:"wrap"}}>
              {schema.fields.map(f => (
                <div key={f.k} className="form-group" style={{minWidth:f.textarea||f.k==="key"?"100%":"180px",flex:f.textarea||f.k==="key"?"1 1 100%":"1 1 180px"}}>
                  <label>{f.l}</label>
                  {f.select ? (
                    <select value={form.sub[f.k]||f.select[0]} onChange={e=>setForm({...form,sub:{...form.sub,[f.k]:e.target.value}})}>
                      {f.select.map(o=><option key={o} value={o}>{o}</option>)}
                    </select>
                  ) : f.textarea ? (
                    <textarea rows={3} value={form.sub[f.k]||""} onChange={e=>setForm({...form,sub:{...form.sub,[f.k]:e.target.value}})} placeholder={f.ph} style={{fontFamily:"monospace",fontSize:12}}/>
                  ) : (
                    <input type={f.secret?"password":"text"} value={form.sub[f.k]||""} onChange={e=>setForm({...form,sub:{...form.sub,[f.k]:e.target.value}})} placeholder={f.ph}/>
                  )}
                </div>
              ))}
            </div>
          </div>
          <div className="form-group">
            <label>Note <span style={{color:"#64748b",fontWeight:400}}>(optional)</span></label>
            <input value={form.note} onChange={e=>setForm({...form,note:e.target.value})} placeholder="e.g. Production server · rotates monthly"/>
          </div>
          <button type="submit" className="btn btn-primary">+ Create</button>
        </form>
      </div>
      <div className="card">
        <div className="card-title">Your Credentials</div>
        {loading ? <div className="empty-state">Loading…</div> : credentials.length === 0 ? (
          <div className="empty-state">No credentials yet. Create one above to get started.</div>
        ) : (
          <table>
            <thead><tr><th>Name</th><th>Type</th><th>Fields</th><th>Note</th><th>Created</th><th></th></tr></thead>
            <tbody>
              {credentials.map(c=>(
                <tr key={c.id}>
                  <td><code className="badge-credential" style={{background:"#0f1117",color:"#a78bfa"}}>{c.name}</code></td>
                  <td><span className={`badge-credential ${typeColors[c.type]||"generic"}`}>{c.type}</span></td>
                  <td style={{color:"#475569",fontSize:11}}>{credFieldSummary(c.type) || <span style={{color:"#334155"}}>••••••••</span>}</td>
                  <td style={{color:"#64748b",fontSize:12}}>{c.note||"—"}</td>
                  <td style={{color:"#64748b",fontSize:12}}>{new Date(c.created_at).toLocaleString()}</td>
                  <td><button className="btn btn-danger" onClick={()=>del(c.id)}>✕</button></td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

// ── Schedules ─────────────────────────────────────────────────────────────
function Schedules({ showToast }) {
  const [schedules, setSchedules] = useState([]);
  const [form, setForm] = useState({name:"",workflow:"",cron:"0 9 * * *",payload:"{}",timezone:"UTC"});

  const load = async () => { try { setSchedules(await api("GET","/api/schedules")); } catch(e){} };
  useEffect(()=>{ load(); },[]);

  async function create(e) {
    e.preventDefault();
    try {
      await api("POST","/api/schedules",{...form,payload:JSON.parse(form.payload||"{}")});
      setForm({name:"",workflow:"",cron:"0 9 * * *",payload:"{}",timezone:"UTC"});
      load(); showToast("Schedule created");
    } catch(err) { showToast(err.message,"error"); }
  }
  async function toggle(id) { try { await api("POST",`/api/schedules/${id}/toggle`); load(); } catch(e){ showToast(e.message,"error"); }}
  async function del(id)    { try { await api("DELETE",`/api/schedules/${id}`); load(); } catch(e){ showToast(e.message,"error"); }}

  return (
    <div>
      <h1 className="page-title">Schedules</h1>
      <div className="card">
        <div className="card-title">New Schedule</div>
        <form onSubmit={create}>
          <div className="form-row">
            <div className="form-group"><label>Name</label><input required value={form.name} onChange={e=>setForm({...form,name:e.target.value})} placeholder="Daily digest"/></div>
            <div className="form-group"><label>Workflow / Graph name</label><input required value={form.workflow} onChange={e=>setForm({...form,workflow:e.target.value})} placeholder="example"/></div>
          </div>
          <div className="form-row">
            <div className="form-group"><label>Cron</label><input required value={form.cron} onChange={e=>setForm({...form,cron:e.target.value})} placeholder="0 9 * * *"/></div>
            <div className="form-group"><label>Timezone</label><input value={form.timezone} onChange={e=>setForm({...form,timezone:e.target.value})} placeholder="UTC"/></div>
          </div>
          <div className="form-group"><label>Payload (JSON)</label><input value={form.payload} onChange={e=>setForm({...form,payload:e.target.value})} placeholder="{}"/></div>
          <button type="submit" className="btn btn-primary">+ Create</button>
        </form>
      </div>
      <div className="card">
        <div className="card-title">Scheduled Jobs</div>
        {schedules.length===0 ? <div className="empty-state">No schedules yet.</div> : (
          <table>
            <thead><tr><th>Name</th><th>Workflow</th><th>Cron</th><th>Timezone</th><th>Status</th><th></th></tr></thead>
            <tbody>
              {schedules.map(s=>(
                <tr key={s.id}>
                  <td>{s.name}</td>
                  <td style={{fontFamily:"monospace",fontSize:12}}>{s.workflow}</td>
                  <td style={{fontFamily:"monospace",fontSize:12}}>{s.cron}</td>
                  <td>{s.timezone}</td>
                  <td><span className={`badge ${s.enabled?"badge-succeeded":"badge-cancelled"}`}>{s.enabled?"active":"paused"}</span></td>
                  <td><div style={{display:"flex",gap:6}}>
                    <button className="btn btn-ghost" onClick={()=>toggle(s.id)}>{s.enabled?"Pause":"Resume"}</button>
                    <button className="btn btn-danger" onClick={()=>del(s.id)}>✕</button>
                  </div></td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

// ── Logs ──────────────────────────────────────────────────────────────────
function Logs({ showToast }) {
  const [runs, setRuns]           = useState([]);
  const [loading, setLoading]     = useState(true);
  const [selected, setSelected]   = useState(null);
  const [search, setSearch]       = useState("");
  const [statusFilter, setStatus] = useState("all");

  const load = async () => {
    try { setRuns(await api("GET","/api/runs")); }
    catch(e){ showToast(e.message,"error"); }
    setLoading(false);
  };
  useEffect(()=>{ load(); },[]);

  const STATUS_COLOR = {succeeded:"#4ade80",failed:"#f87171",running:"#34d399",queued:"#60a5fa",cancelled:"#9ca3af"};
  const TRACE_DOT    = {ok:"#4ade80",error:"#f87171",skipped:"#64748b"};

  const filtered = runs.filter(r => {
    if (statusFilter !== "all" && r.status !== statusFilter) return false;
    if (search) {
      const q = search.toLowerCase();
      return (r.flow_name||"").toLowerCase().includes(q) || String(r.id).includes(q);
    }
    return true;
  });

  function fmtTs(ts) {
    if (!ts) return "—";
    try { return new Date(ts).toLocaleString(); } catch { return ts; }
  }
  function fmtDur(ms) {
    if (!ms && ms !== 0) return "—";
    if (ms < 1000) return `${ms}ms`;
    return `${(ms/1000).toFixed(1)}s`;
  }

  const selRun = selected != null ? runs.find(r=>r.id===selected) : null;
  const traces = selRun?.traces || [];

  return (
    <div>
      <h1 className="page-title">Run Logs</h1>
      <div className="info-box" style={{marginBottom:16}}>
        Per-node execution traces for every run. Each entry shows status, duration, retries,
        and the input/output at each step. Data is stored in the database — there are no
        separate log files.
      </div>

      {/* Filter bar */}
      <div style={{display:"flex",gap:8,marginBottom:16,alignItems:"center"}}>
        <input placeholder="Search by flow name or run ID…" value={search}
          onChange={e=>setSearch(e.target.value)}
          style={{maxWidth:280,flex:"none"}}/>
        <select value={statusFilter} onChange={e=>setStatus(e.target.value)} style={{width:130,flex:"none"}}>
          <option value="all">All statuses</option>
          <option value="succeeded">Succeeded</option>
          <option value="failed">Failed</option>
          <option value="running">Running</option>
          <option value="queued">Queued</option>
          <option value="cancelled">Cancelled</option>
        </select>
        <button className="btn btn-ghost" onClick={load}>↻ Refresh</button>
        <span style={{marginLeft:"auto",fontSize:12,color:"#64748b"}}>{filtered.length} run{filtered.length!==1?"s":""}</span>
      </div>

      <div style={{display:"grid",gridTemplateColumns:"340px 1fr",gap:16,alignItems:"start"}}>
        {/* Run list */}
        <div style={{background:"#1a1d2e",border:"1px solid #2a2d3e",borderRadius:10,overflow:"hidden"}}>
          {loading && <div className="empty-state" style={{padding:"30px 0"}}>Loading…</div>}
          {!loading && filtered.length===0 && <div className="empty-state" style={{padding:"30px 0"}}>No runs match.</div>}
          {filtered.map(r=>(
            <div key={r.id} onClick={()=>setSelected(r.id===selected?null:r.id)}
              style={{padding:"10px 14px",borderBottom:"1px solid #1e2235",cursor:"pointer",
                background:selected===r.id?"#252840":"transparent",
                borderLeft:`3px solid ${selected===r.id?"#7c3aed":"transparent"}`,
                transition:"background .12s"}}>
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:3}}>
                <span style={{fontWeight:600,fontSize:13,color:"#e2e8f0"}}>
                  {r.flow_name || `run #${r.id}`}
                </span>
                <span style={{fontSize:11,fontWeight:600,color:STATUS_COLOR[r.status]||"#94a3b8"}}>
                  {r.status}
                </span>
              </div>
              <div style={{fontSize:11,color:"#64748b"}}>
                #{r.id} · {fmtTs(r.started_at)}
                {(r.traces||[]).length > 0 &&
                  <span style={{marginLeft:6}}>{r.traces.length} node{r.traces.length!==1?"s":""}</span>}
              </div>
            </div>
          ))}
        </div>

        {/* Trace detail */}
        <div>
          {!selRun
            ? <div className="empty-state" style={{paddingTop:60,background:"#1a1d2e",border:"1px solid #2a2d3e",borderRadius:10}}>
                ← Select a run to view its node traces
              </div>
            : <>
                <div className="card" style={{marginBottom:12}}>
                  <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start"}}>
                    <div>
                      <div style={{fontSize:15,fontWeight:700,color:"#e2e8f0",marginBottom:4}}>
                        {selRun.flow_name || `Run #${selRun.id}`}
                      </div>
                      <div style={{fontSize:12,color:"#64748b"}}>
                        Started: {fmtTs(selRun.started_at)}
                        {selRun.finished_at && <> · Finished: {fmtTs(selRun.finished_at)}</>}
                      </div>
                    </div>
                    <span style={{fontSize:12,fontWeight:700,padding:"4px 10px",borderRadius:20,
                      background:selRun.status==="succeeded"?"#14532d":selRun.status==="failed"?"#3f1111":"#1e3a5f",
                      color:STATUS_COLOR[selRun.status]||"#94a3b8"}}>
                      {selRun.status}
                    </span>
                  </div>
                  {selRun.error && <div style={{marginTop:10,padding:"8px 10px",background:"#3f1111",borderRadius:6,fontSize:12,color:"#f87171"}}>{selRun.error}</div>}
                </div>

                {traces.length===0
                  ? <div className="empty-state" style={{padding:"30px 0",background:"#1a1d2e",border:"1px solid #2a2d3e",borderRadius:10}}>No trace data for this run.</div>
                  : <div className="trace-table">
                      <table>
                        <thead>
                          <tr>
                            <th style={{width:18}}></th>
                            <th>Node</th>
                            <th>Type</th>
                            <th>Duration</th>
                            <th>Attempts</th>
                            <th>Input / Output</th>
                          </tr>
                        </thead>
                        <tbody>
                          {traces.map((t,i)=>(
                            <TraceRow key={i} t={t} fmtDur={fmtDur} dot={TRACE_DOT}/>
                          ))}
                        </tbody>
                      </table>
                    </div>
                }
              </>
          }
        </div>
      </div>
    </div>
  );
}

function TraceRow({ t, fmtDur, dot }) {
  const [open, setOpen] = useState(false);
  const hasDetail = t.input || t.output || t.error;
  return (
    <>
      <tr className="expandable-row" onClick={()=>hasDetail&&setOpen(o=>!o)}
        style={{cursor:hasDetail?"pointer":"default"}}>
        <td><span className="trace-status-dot" style={{background:dot[t.status]||"#64748b"}}/></td>
        <td style={{color:"#e2e8f0",fontWeight:500}}>{t.label||t.node_id}</td>
        <td style={{fontFamily:"monospace",fontSize:11,color:"#94a3b8"}}>{t.type}</td>
        <td style={{color:"#94a3b8"}}>{fmtDur(t.duration_ms)}</td>
        <td style={{color:"#94a3b8",textAlign:"center"}}>{t.attempts||1}</td>
        <td style={{color:t.status==="error"?"#f87171":"#64748b",fontSize:11}}>
          {t.status==="error" ? t.error : (hasDetail ? (open?"▲ hide":"▼ show") : "—")}
        </td>
      </tr>
      {open && hasDetail && (
        <tr>
          <td colSpan={6} style={{padding:"0 10px 10px"}}>
            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:8}}>
              {t.input && <div>
                <div style={{fontSize:10,color:"#64748b",marginBottom:4,textTransform:"uppercase",letterSpacing:".06em"}}>Input</div>
                <pre style={{background:"#0a0c14",border:"1px solid #2a2d3e",borderRadius:6,padding:"8px 10px",fontSize:11,color:"#94a3b8",whiteSpace:"pre-wrap",maxHeight:200,overflow:"auto",margin:0}}>
                  {typeof t.input==="string"?t.input:JSON.stringify(t.input,null,2)}
                </pre>
              </div>}
              {(t.output||t.error) && <div>
                <div style={{fontSize:10,color:"#64748b",marginBottom:4,textTransform:"uppercase",letterSpacing:".06em"}}>{t.error?"Error":"Output"}</div>
                <pre style={{background:"#0a0c14",border:`1px solid ${t.error?"#7f1d1d":"#2a2d3e"}`,borderRadius:6,padding:"8px 10px",fontSize:11,color:t.error?"#f87171":"#94a3b8",whiteSpace:"pre-wrap",maxHeight:200,overflow:"auto",margin:0}}>
                  {t.error || (typeof t.output==="string"?t.output:JSON.stringify(t.output,null,2))}
                </pre>
              </div>}
            </div>
          </td>
        </tr>
      )}
    </>
  );
}

// ── Settings ──────────────────────────────────────────────────────────────
function Settings({ showToast }) {
  const [resetting, setResetting] = useState(false);
  async function resetSeqs() {
    if (!window.confirm("Reset ID sequences? Only safe when all tables are empty.")) return;
    setResetting(true);
    try { await api("POST","/api/maintenance/reset_sequences"); showToast("Sequences reset"); }
    catch(e) { showToast(e.message,"error"); }
    setResetting(false);
  }
  return (
    <div>
      <h1 className="page-title">Settings</h1>
      <div className="card">
        <div className="card-title">Admin Token</div>
        <p style={{fontSize:13,color:"#94a3b8",marginBottom:12}}>Your admin token is passed as a URL query param (<code style={{color:"#a78bfa"}}>?token=...</code>) and as the <code style={{color:"#a78bfa"}}>x-admin-token</code> header.</p>
        <code style={{display:"block",padding:"10px 12px",background:"#0f1117",borderRadius:6,fontSize:13,color:"#a78bfa",userSelect:"all"}}>{TOKEN || "(no token set)"}</code>
      </div>
      <div className="card">
        <div className="card-title">Node Canvas</div>
        <p style={{fontSize:13,color:"#94a3b8",marginBottom:12}}>Open the visual workflow builder.</p>
        <button className="btn btn-primary" onClick={()=>window.open(`/canvas?token=${TOKEN}`)}>Open Canvas →</button>
      </div>
      <div className="card">
        <div className="card-title">External Links</div>
        <div style={{display:"flex",gap:10}}>
          <a href="/flower/" target="_blank" className="btn btn-ghost">Flower monitor ↗</a>
          <a href="/docs" target="_blank" className="btn btn-ghost">Swagger docs ↗</a>
        </div>
      </div>
      <div className="card">
        <div className="card-title">Maintenance</div>
        <p style={{fontSize:13,color:"#94a3b8",marginBottom:12}}>Reset PostgreSQL ID sequences. Only run this when all tables are empty.</p>
        <button className="btn btn-danger" disabled={resetting} onClick={resetSeqs}>{resetting?"Resetting…":"Reset sequences"}</button>
      </div>
    </div>
  );
}

// ── Graphs ────────────────────────────────────────────────────────────────
function Graphs({ showToast }) {
  const [graphs, setGraphs]     = useState([]);
  const [loading, setLoading]   = useState(true);
  const [running, setRunning]   = useState(null);
  const [search, setSearch]     = useState("");
  const [reseeding, setReseeding] = useState(false);

  const load = useCallback(async () => {
    try { setGraphs(await api("GET","/api/graphs")); }
    catch(e) { showToast(e.message,"error"); }
    setLoading(false);
  }, []);
  useEffect(()=>{ load(); },[]);

  async function reseedExamples() {
    if(!window.confirm("Re-seed all example flows? Missing examples will be restored; existing ones are left unchanged.")) return;
    setReseeding(true);
    try { await api("POST","/api/graphs/reseed"); showToast("Examples re-seeded"); load(); }
    catch(e) { showToast(e.message,"error"); }
    setReseeding(false);
  }

  async function renameGraph(g) {
    const newName = window.prompt("Rename flow:", g.name);
    if(!newName || newName.trim() === g.name) return;
    try { await api("PUT",`/api/graphs/${g.id}`,{name:newName.trim()}); showToast("Renamed"); load(); }
    catch(e) { showToast(e.message,"error"); }
  }

  async function runGraph(id, name) {
    setRunning(id);
    try { await api("POST",`/api/graphs/${id}/run`,{source:"admin"}); showToast(`Queued: ${name}`); }
    catch(e) { showToast(e.message,"error"); }
    setRunning(null);
  }

  async function toggleGraph(id, enabled) {
    try { await api("PUT",`/api/graphs/${id}`,{enabled:!enabled}); load(); showToast(enabled?"Disabled":"Enabled"); }
    catch(e) { showToast(e.message,"error"); }
  }

  async function duplicateGraph(g) {
    try {
      const full = await api("GET",`/api/graphs/${g.id}`);
      const baseName = full.name.replace(/^[📧🤖🔄⚠⚡🔍🧪📊🐍⏰💻📁]\s*/u,"");
      const copy = await api("POST","/api/graphs",{
        name: baseName + " (copy)",
        description: full.description || "",
        graph_data: full.graph_data || {}
      });
      showToast(`Duplicated as "${copy.name}"`); load();
    } catch(e) { showToast(e.message,"error"); }
  }

  async function deleteGraph(id, name) {
    if(!window.confirm(`Delete "${name}"? This cannot be undone.`)) return;
    try { await api("DELETE",`/api/graphs/${id}`); showToast("Deleted"); load(); }
    catch(e) { showToast(e.message,"error"); }
  }

  const EXAMPLE_RE = /^[📧🤖🔄⚠⚡🔍🧪📊🐍⏰💻📁]/u;
  const isExample = (name) => EXAMPLE_RE.test(name);

  const q = search.toLowerCase();
  const filtered = graphs.filter(g => !q || g.name.toLowerCase().includes(q) || (g.description||"").toLowerCase().includes(q));
  const exampleFlows = filtered.filter(g=>isExample(g.name));
  const userFlows    = filtered.filter(g=>!isExample(g.name));

  return (
    <div>
      <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:16}}>
        <h1 className="page-title" style={{marginBottom:0}}>Canvas Flows</h1>
        <div style={{display:"flex",gap:8}}>
          <button className="btn btn-ghost" disabled={reseeding} onClick={reseedExamples} title="Restore any missing example flows">
            {reseeding ? "Reseeding…" : "↺ Restore examples"}
          </button>
          <a href={`/canvas?token=${TOKEN}`} className="btn btn-primary">+ New in Canvas</a>
        </div>
      </div>

      <div style={{marginBottom:16}}>
        <input style={{width:"100%",background:"#1a1d2e",color:"#e2e8f0",border:"1px solid #2a2d3e",borderRadius:8,padding:"8px 12px",fontSize:13}}
          placeholder="🔍  Search flows…" value={search} onChange={e=>setSearch(e.target.value)}/>
      </div>

      {loading ? <div className="empty-state">Loading…</div> :
       graphs.length === 0 ? (
        <div className="card">
          <div className="empty-state" style={{padding:"40px 0"}}>
            No graphs yet.<br/>
            <a href={`/canvas?token=${TOKEN}`} style={{color:"#a78bfa",textDecoration:"none",marginTop:8,display:"inline-block"}}>Open the canvas to create your first flow →</a>
          </div>
        </div>
      ) : filtered.length === 0 ? (
        <div className="card"><div className="empty-state">No flows match "{search}".</div></div>
      ) : (
        <div>
          {/* ── example graphs ── */}
          {exampleFlows.length > 0 && (
            <div className="card" style={{marginBottom:16}}>
              <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:12}}>
                <div className="card-title" style={{marginBottom:0}}>Example Flows <span style={{fontSize:11,color:"#64748b",fontWeight:400}}>({exampleFlows.length})</span></div>
                <span style={{fontSize:11,color:"#64748b"}}>Duplicate to make your own copy</span>
              </div>
              {exampleFlows.map(g=>(
                <GraphRow key={g.id} g={g} running={running} onRun={runGraph} onToggle={toggleGraph} onDuplicate={duplicateGraph} onDelete={deleteGraph} onRename={renameGraph} showToast={showToast} load={load} isExample={true}/>
              ))}
            </div>
          )}
          {/* ── custom graphs ── */}
          {userFlows.length > 0 && (
            <div className="card">
              <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:12}}>
                <div className="card-title" style={{marginBottom:0}}>Your Flows <span style={{fontSize:11,color:"#64748b",fontWeight:400}}>({userFlows.length})</span></div>
              </div>
              {userFlows.map(g=>(
                <GraphRow key={g.id} g={g} running={running} onRun={runGraph} onToggle={toggleGraph} onDuplicate={duplicateGraph} onDelete={deleteGraph} onRename={renameGraph} showToast={showToast} load={load} isExample={false}/>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function GraphRow({ g, running, onRun, onToggle, onDuplicate, onDelete, onRename, showToast, load, isExample }) {
  const [open, setOpen] = useState(false);
  const [showVersions, setShowVersions] = useState(false);
  const [versions, setVersions] = useState([]);
  const [loadingVersions, setLoadingVersions] = useState(false);
  const nodeCount = ((g.graph_data?.nodes)||[]).filter(n=>n.type!=="note").length;

  async function openVersionHistory() {
    setLoadingVersions(true);
    try {
      const v = await api("GET",`/api/graphs/${g.id}/versions`);
      setVersions(v);
      setShowVersions(true);
    } catch(e) { showToast(e.message,"error"); }
    setLoadingVersions(false);
  }

  async function restoreVersion(vid) {
    if(!window.confirm("Restore this version? Current changes will be lost.")) return;
    try {
      await api("POST",`/api/graphs/${g.id}/versions/${vid}/restore`);
      showToast("Version restored");
      load();
      setShowVersions(false);
    } catch(e) { showToast(e.message,"error"); }
  }

  return (
    <>
      <div style={{borderBottom:"1px solid #1e2235",paddingBottom:12,marginBottom:12}}>
        <div style={{display:"flex",alignItems:"center",gap:10}}>
          <div style={{flex:1,minWidth:0}}>
            <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:3}}>
              <span style={{fontWeight:600,fontSize:14,color:"#e2e8f0"}}>{g.name}</span>
              <span className={`badge ${g.enabled?"badge-succeeded":"badge-cancelled"}`}>{g.enabled?"enabled":"disabled"}</span>
              <span style={{fontSize:11,color:"#4b5563"}}>#{g.id} · {nodeCount} node{nodeCount!==1?"s":""}</span>
            </div>
            {g.description && <div style={{fontSize:12,color:"#64748b"}}>{g.description}</div>}
          </div>
          <div style={{display:"flex",gap:6,flex:"none"}}>
            <a href={`/canvas?token=${TOKEN}`} className="btn btn-ghost"
               onClick={()=>sessionStorage.setItem("canvas_open_graph",g.id)}>✏️ Edit</a>
            <button className="btn btn-success" disabled={running===g.id||!g.enabled}
                    onClick={()=>onRun(g.id,g.name)}>{running===g.id?"…":"▶"}</button>
            <button className="btn btn-ghost" onClick={()=>setOpen(o=>!o)}>⋯</button>
          </div>
        </div>
        {open && (
          <div style={{display:"flex",gap:8,marginTop:10,paddingTop:10,borderTop:"1px solid #1e2235",flexWrap:"wrap"}}>
            <button className="btn btn-ghost" onClick={()=>onRename(g)}>✏ Rename</button>
            <button className="btn btn-ghost" onClick={()=>onDuplicate(g)}>📋 Duplicate</button>
            <button className="btn btn-ghost" onClick={openVersionHistory} disabled={loadingVersions}>📜 History</button>
            <button className="btn btn-ghost" onClick={()=>onToggle(g.id,g.enabled)}>{g.enabled?"⏸ Disable":"▶ Enable"}</button>
            <div style={{flex:1}}/>
            <div style={{fontSize:11,color:"#4b5563",alignSelf:"center"}}>
              Webhook: <code style={{color:"#6b7280"}}>{g.webhook_token?.slice(0,12)}…</code>
            </div>
            <button className="btn btn-danger" onClick={()=>onDelete(g.id,g.name)}
              title={isExample ? "⚠ This is an example flow — use ↺ Restore examples to get it back" : undefined}>
              🗑 Delete{isExample?" (example)":""}
            </button>
          </div>
        )}
      </div>

      {showVersions && (
        <div className="modal-overlay" onClick={e=>{ if(e.target.className==="modal-overlay") setShowVersions(false) }}>
          <div className="modal">
            <h2>📜 Version History</h2>
            {versions.length === 0 ? (
              <div className="empty-state" style={{padding:"20px 0"}}>No versions yet.</div>
            ) : (
              <div style={{marginTop:12}}>
                {versions.map((v,idx)=>(
                  <div key={idx} className="version-row">
                    <div className="version-info">
                      <div className="version-num">Version {v.version}</div>
                      <div className="version-date">{new Date(v.saved_at).toLocaleString()}</div>
                      {v.note && <div className="version-note">{v.note}</div>}
                    </div>
                    <button className="btn btn-ghost btn-sm" onClick={()=>restoreVersion(v.version)}>Restore</button>
                  </div>
                ))}
              </div>
            )}
            <div className="modal-btns">
              <button className="btn btn-ghost" onClick={()=>setShowVersions(false)}>Close</button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}

// ── Templates ─────────────────────────────────────────────────────────────
const CATEGORY_COLORS = {
  "Monitoring":   "#0891b2",
  "Integrations": "#7c3aed",
  "Productivity": "#059669",
  "Reporting":    "#d97706",
  "AI":           "#8b5cf6",
  "General":      "#475569",
};

function Templates({ showToast }) {
  const [templates, setTemplates] = useState([]);
  const [loading, setLoading]     = useState(true);
  const [importing, setImporting] = useState(null);

  useEffect(() => {
    api("GET", "/api/templates")
      .then(setTemplates)
      .catch(e => showToast(e.message, "error"))
      .finally(() => setLoading(false));
  }, []);

  async function useTemplate(t) {
    setImporting(t.id);
    try {
      const g = await api("POST", `/api/templates/${t.id}/use`, {});
      showToast(`Flow "${g.name}" created! Open it in Canvas Flows.`);
    } catch(e) {
      showToast("Failed: " + e.message, "error");
    }
    setImporting(null);
  }

  // Group by category
  const groups = {};
  templates.forEach(t => {
    if (!groups[t.category]) groups[t.category] = [];
    groups[t.category].push(t);
  });

  return (
    <div className="page">
      <div style={{display:"flex",alignItems:"flex-start",justifyContent:"space-between",marginBottom:24}}>
        <div>
          <h1 className="page-title" style={{marginBottom:6}}>📐 Workflow Templates</h1>
          <div style={{fontSize:13,color:"#64748b"}}>Start from a pre-built template — click <strong style={{color:"#94a3b8"}}>Use template</strong> to create a flow, then edit it in Canvas Flows.</div>
        </div>
      </div>
      {loading && <div style={{color:"#64748b",padding:24}}>Loading templates…</div>}
      {!loading && templates.length === 0 && (
        <div style={{color:"#64748b",padding:24}}>No templates found. Add JSON files to <code>app/templates/</code>.</div>
      )}
      {Object.entries(groups).map(([category, items]) => (
        <div key={category} style={{marginBottom:32}}>
          <div style={{fontSize:11,fontWeight:600,color:(CATEGORY_COLORS[category]||"#94a3b8"),
            textTransform:"uppercase",letterSpacing:".08em",marginBottom:12,paddingBottom:6,
            borderBottom:`1px solid ${(CATEGORY_COLORS[category]||"#2a2d3e")}44`}}>
            {category}
          </div>
          <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(300px,1fr))",gap:14}}>
            {items.map(t => (
              <div key={t.id} style={{background:"#181b2e",border:"1px solid #2a2d3e",borderRadius:10,
                padding:"16px 18px",display:"flex",flexDirection:"column",gap:8,
                transition:"border-color .15s",cursor:"default"}}
                onMouseEnter={e=>e.currentTarget.style.borderColor=(CATEGORY_COLORS[t.category]||"#7c3aed")}
                onMouseLeave={e=>e.currentTarget.style.borderColor="#2a2d3e"}>
                <div style={{display:"flex",alignItems:"flex-start",justifyContent:"space-between",gap:8}}>
                  <div style={{fontWeight:600,color:"#e2e8f0",fontSize:14}}>{t.name}</div>
                  <span style={{fontSize:10,color:(CATEGORY_COLORS[t.category]||"#475569"),
                    background:(CATEGORY_COLORS[t.category]||"#475569")+"22",
                    borderRadius:4,padding:"2px 7px",whiteSpace:"nowrap"}}>{t.category}</span>
                </div>
                <div style={{fontSize:12,color:"#94a3b8",lineHeight:1.6}}>{t.description}</div>
                <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",marginTop:4}}>
                  <div style={{display:"flex",gap:6,flexWrap:"wrap"}}>
                    {(t.tags||[]).map(tag=>(
                      <span key={tag} style={{fontSize:10,color:"#64748b",background:"#0f1117",
                        borderRadius:3,padding:"1px 6px",border:"1px solid #2a2d3e"}}>{tag}</span>
                    ))}
                    <span style={{fontSize:10,color:"#4b5563"}}>
                      {t.node_count} node{t.node_count!==1?"s":""}
                    </span>
                  </div>
                  <button
                    className="btn btn-primary btn-sm"
                    disabled={importing === t.id}
                    onClick={()=>useTemplate(t)}
                    style={{minWidth:80}}>
                    {importing===t.id ? "Importing…" : "Use template"}
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

// ── App shell ─────────────────────────────────────────────────────────────
const PAGES = [
  {id:"dashboard",  icon:"⬛", label:"Dashboard"},
  {id:"graphs",     icon:"🎨", label:"Canvas Flows"},
  {id:"templates",  icon:"📐", label:"Templates"},
  {id:"metrics",    icon:"📊", label:"Metrics"},
  {id:"scripts",    icon:"🐍", label:"Scripts"},
  {id:"credentials",icon:"🔑", label:"Credentials"},
  {id:"schedules",  icon:"⏰", label:"Schedules"},
  {id:"logs",       icon:"📋", label:"Logs"},
  {id:"settings",   icon:"⚙",  label:"Settings"},
];

function App() {
  const validIds = PAGES.map(p=>p.id);
  const hashPage = () => {
    const h = window.location.hash.slice(1);
    return validIds.includes(h) ? h : "dashboard";
  };
  const [page, setPageState] = useState(hashPage);
  const [toast, setToast]    = useState(null);
  const showToast = useCallback((msg, type="success") => setToast({msg,type,key:Date.now()}), []);

  // Keep hash in sync with page state
  const setPage = useCallback((id) => {
    window.location.hash = id;
    setPageState(id);
  }, []);

  // Handle browser back/forward
  useEffect(() => {
    const onHashChange = () => setPageState(hashPage());
    window.addEventListener("hashchange", onHashChange);
    return () => window.removeEventListener("hashchange", onHashChange);
  }, []);

  return (
    <>
      <div className="sidebar">
        <div className="logo">⚡ HiveRunr</div>
        {PAGES.map(p=>(
          <div key={p.id} className={`nav-item${page===p.id?" active":""}`} onClick={()=>setPage(p.id)}>
            <span className="nav-icon">{p.icon}</span>{p.label}
          </div>
        ))}
        <div className="sidebar-footer">
          <div className="footer-label">Quick links</div>
          <a className="ext-link" href={`/canvas?token=${TOKEN}`}>
            <span className="el-icon">🎨</span>Node Canvas
          </a>
          <a className="ext-link" href="/flower/" target="_blank" rel="noopener">
            <span className="el-icon">🌸</span>Flower / Celery
          </a>
          <a className="ext-link" href="/docs" target="_blank" rel="noopener">
            <span className="el-icon">📖</span>API Docs
          </a>
          <a className="ext-link" href="/health" target="_blank" rel="noopener">
            <span className="el-icon">💚</span>Health check
          </a>
        </div>
      </div>
      <div className="main">
        {page==="dashboard"  && <Dashboard  showToast={showToast}/>}
        {page==="graphs"     && <Graphs     showToast={showToast}/>}
        {page==="templates"  && <Templates  showToast={showToast}/>}
        {page==="metrics"    && <Metrics    showToast={showToast}/>}
        {page==="scripts"    && <Scripts    showToast={showToast}/>}
        {page==="credentials"&& <Credentials showToast={showToast}/>}
        {page==="schedules"  && <Schedules  showToast={showToast}/>}
        {page==="logs"       && <Logs       showToast={showToast}/>}
        {page==="settings"   && <Settings   showToast={showToast}/>}
      </div>
      {toast && <Toast key={toast.key} msg={toast.msg} type={toast.type} onDone={()=>setToast(null)}/>}
    </>
  );
}

const root = ReactDOM.createRoot(document.getElementById("root"));
root.render(<App/>);
</script>
</body>
</head>

ADMIN_EOF


# ── 16. app/main.py ────────────────────────────────────────────────────────
cat > "$PROJECT/app/main.py" << 'MAIN_EOF'
import os, json, logging
from pathlib import Path
from fastapi import FastAPI, HTTPException, Header, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional

TEMPLATES_DIR = Path(__file__).parent / 'templates'

from app.core.db import (
    init_db, list_runs, delete_run, clear_runs, get_run_by_task,
    list_workflows, upsert_workflow, toggle_workflow,
    list_schedules, create_schedule, toggle_schedule, delete_schedule, sync_graph_schedules,
    list_graphs, create_graph, get_graph, get_graph_by_token,
    get_graph_by_name, get_graph_by_slug, update_graph, delete_graph, update_run,
    list_credentials, upsert_credential, delete_credential,
    list_graph_versions, save_graph_version, get_graph_version,
    get_run_metrics,
)
from app.worker import enqueue_workflow, enqueue_graph, enqueue_script

import re as _re
log          = logging.getLogger(__name__)
SCRIPTS_DIR  = Path(__file__).parent / 'workflows'
app          = FastAPI(title="HiveRunr")
STATIC_DIR   = Path(__file__).parent / "static"
RUNLOGS_DIR  = Path("/app/runlogs")
WORKFLOWS    = ["example"]
API_KEY      = os.environ.get("API_KEY",      "dev_api_key")
ADMIN_TOKEN  = os.environ.get("ADMIN_TOKEN",  "dev_admin_token")

# ── auth ──────────────────────────────────────────────────────────────────
def _check_admin(request: Request):
    token = request.headers.get("x-admin-token") or request.query_params.get("token", "")
    if ADMIN_TOKEN and token != ADMIN_TOKEN:
        raise HTTPException(401, "Admin token required")

def _check_api_key(key: str):
    if API_KEY and key != API_KEY:
        raise HTTPException(401, "Invalid API key")

# ── startup ───────────────────────────────────────────────────────────────
@app.on_event("startup")
def startup():
    init_db()
    _seed_example_graphs()
    for name in WORKFLOWS:
        try: upsert_workflow(name)
        except Exception: pass

# ── example graph seeds ───────────────────────────────────────────────────
EXAMPLE_GRAPHS = [
    {
        "name": "📧 Daily Digest Email",
        "description": "Fetch top Hacker News stories, transform, and email a digest.",
        "graph_data": {
            "nodes": [
                {"id":"ex1_n1","type":"trigger.manual","position":{"x":60,"y":200},"data":{"label":"Start","config":{}}},
                {"id":"ex1_n2","type":"action.http_request","position":{"x":300,"y":200},"data":{"label":"Fetch HN Top Stories","config":{"url":"https://hacker-news.firebaseio.com/v0/topstories.json","method":"GET"}}},
                {"id":"ex1_n3","type":"action.transform","position":{"x":560,"y":200},"data":{"label":"Take Top 5","config":{"expression":"{'ids': input[:5], 'count': len(input[:5])}"}}},
                {"id":"ex1_n4","type":"action.send_email","position":{"x":820,"y":200},"data":{"label":"Send Digest","config":{"to":"you@example.com","subject":"Daily HN Digest","body":"Today's top HN story IDs: {{ex1_n3.ids}}\n\nFetched {{ex1_n3.count}} stories."}}}
            ],
            "edges": [
                {"id":"ex1_e1","source":"ex1_n1","target":"ex1_n2"},
                {"id":"ex1_e2","source":"ex1_n2","target":"ex1_n3"},
                {"id":"ex1_e3","source":"ex1_n3","target":"ex1_n4"}
            ]
        }
    },
    {
        "name": "🤖 LLM Summariser",
        "description": "Fetch a webpage, summarise with LLM, log the result. Requires OPENAI_API_KEY or use {{creds.openai}}.",
        "graph_data": {
            "nodes": [
                {"id":"ex2_n1","type":"trigger.manual","position":{"x":60,"y":200},"data":{"label":"Start","config":{}}},
                {"id":"ex2_n2","type":"action.http_request","position":{"x":300,"y":200},"data":{"label":"Fetch Content","config":{"url":"https://hacker-news.firebaseio.com/v0/item/1.json","method":"GET"}}},
                {"id":"ex2_n3","type":"action.llm_call","position":{"x":560,"y":200},"data":{"label":"Summarise","config":{"model":"gpt-4o-mini","prompt":"Summarise this in 2 sentences: {{ex2_n2.body}}","system":"You are a concise technical summariser."}}},
                {"id":"ex2_n4","type":"action.log","position":{"x":820,"y":200},"data":{"label":"Log Summary","config":{"message":"Summary: {{ex2_n3.response}} ({{ex2_n3.tokens}} tokens)"}}}
            ],
            "edges": [
                {"id":"ex2_e1","source":"ex2_n1","target":"ex2_n2"},
                {"id":"ex2_e2","source":"ex2_n2","target":"ex2_n3"},
                {"id":"ex2_e3","source":"ex2_n3","target":"ex2_n4"}
            ]
        }
    },
    {
        "name": "🔄 Loop + Notify Per Item",
        "description": "Fetch a list, filter it, loop, and notify per item.",
        "graph_data": {
            "nodes": [
                {"id":"ex3_n1","type":"trigger.manual","position":{"x":60,"y":220},"data":{"label":"Start","config":{}}},
                {"id":"ex3_n2","type":"action.http_request","position":{"x":280,"y":220},"data":{"label":"Fetch Todos","config":{"url":"https://jsonplaceholder.typicode.com/todos?_limit=10","method":"GET"}}},
                {"id":"ex3_n3","type":"action.filter","position":{"x":500,"y":220},"data":{"label":"Filter Incomplete","config":{"expression":"not item.get('completed', True)"}}},
                {"id":"ex3_n4","type":"action.loop","position":{"x":720,"y":220},"data":{"label":"Loop Items","config":{"field":"items","max_items":"20"}}},
                {"id":"ex3_n5","type":"action.log","position":{"x":960,"y":140},"data":{"label":"Log Item","config":{"message":"Todo: {{item.title}}"}}},
                {"id":"ex3_n6","type":"action.log","position":{"x":720,"y":400},"data":{"label":"Done","config":{"message":"Loop complete"}}}
            ],
            "edges": [
                {"id":"ex3_e1","source":"ex3_n1","target":"ex3_n2"},
                {"id":"ex3_e2","source":"ex3_n2","target":"ex3_n3"},
                {"id":"ex3_e3","source":"ex3_n3","target":"ex3_n4"},
                {"id":"ex3_e4","source":"ex3_n4","target":"ex3_n5","sourceHandle":"body"},
                {"id":"ex3_e5","source":"ex3_n4","target":"ex3_n6","sourceHandle":"done"}
            ]
        }
    },
    {
        "name": "⚠ Conditional Alert",
        "description": "Fetch data, check a condition, branch to alert or log.",
        "graph_data": {
            "nodes": [
                {"id":"ex4_n1","type":"trigger.manual","position":{"x":60,"y":200},"data":{"label":"Start","config":{}}},
                {"id":"ex4_n2","type":"action.http_request","position":{"x":280,"y":200},"data":{"label":"Fetch Status","config":{"url":"https://httpbin.org/status/200","method":"GET"}}},
                {"id":"ex4_n3","type":"action.condition","position":{"x":500,"y":200},"data":{"label":"Is OK?","config":{"expression":"input.get('status') == 200"}}},
                {"id":"ex4_n4","type":"action.log","position":{"x":740,"y":120},"data":{"label":"All good","config":{"message":"Service is healthy ✓"}}},
                {"id":"ex4_n5","type":"action.log","position":{"x":740,"y":300},"data":{"label":"Alert!","config":{"message":"Service may be down — status {{ex4_n2.status}}"}}}
            ],
            "edges": [
                {"id":"ex4_e1","source":"ex4_n1","target":"ex4_n2"},
                {"id":"ex4_e2","source":"ex4_n2","target":"ex4_n3"},
                {"id":"ex4_e3","source":"ex4_n3","target":"ex4_n4","sourceHandle":"true"},
                {"id":"ex4_e4","source":"ex4_n3","target":"ex4_n5","sourceHandle":"false"}
            ]
        }
    },
    {
        "name": "🐍 Python Data Transform",
        "description": "Fetch JSON data, reshape it with a Python script, and log the result.",
        "graph_data": {
            "nodes": [
                {"id":"ex5_n1","type":"trigger.manual","position":{"x":60,"y":200},"data":{"label":"Start","config":{}}},
                {"id":"ex5_n2","type":"action.http_request","position":{"x":280,"y":200},"data":{"label":"Fetch Users","config":{"url":"https://jsonplaceholder.typicode.com/users?_limit=5","method":"GET"}}},
                {"id":"ex5_n3","type":"action.run_script","position":{"x":520,"y":200},"data":{"label":"Extract Emails","config":{"script":"users = input if isinstance(input, list) else input.get('body', [])\nresult = {'emails': [u['email'] for u in users], 'count': len(users)}"}}},
                {"id":"ex5_n4","type":"action.log","position":{"x":760,"y":200},"data":{"label":"Log Emails","config":{"message":"Found {{ex5_n3.count}} users: {{ex5_n3.emails}}"}}},
                {"id":"ex5_note","type":"note","position":{"x":60,"y":360},"data":{"label":"Note","config":{"text":"Run Script node can do any Python.\nAssign your output to the 'result' variable."}}}
            ],
            "edges": [
                {"id":"ex5_e1","source":"ex5_n1","target":"ex5_n2"},
                {"id":"ex5_e2","source":"ex5_n2","target":"ex5_n3"},
                {"id":"ex5_e3","source":"ex5_n3","target":"ex5_n4"}
            ]
        }
    },
    {
        "name": "⏰ Scheduled Cron Flow",
        "description": "Runs on a cron schedule. Edit the Cron node to set your timing.",
        "graph_data": {
            "nodes": [
                {"id":"ex6_n1","type":"trigger.cron","position":{"x":60,"y":200},"data":{"label":"Every day at 9am","config":{"cron":"0 9 * * *","timezone":"UTC","description":"Daily at 9am UTC"}}},
                {"id":"ex6_n2","type":"action.http_request","position":{"x":320,"y":200},"data":{"label":"Fetch Data","config":{"url":"https://hacker-news.firebaseio.com/v0/topstories.json","method":"GET"}}},
                {"id":"ex6_n3","type":"action.log","position":{"x":580,"y":200},"data":{"label":"Log Result","config":{"message":"Scheduled run complete. First story ID: {{ex6_n2.body.0}}"}}},
                {"id":"ex6_note","type":"note","position":{"x":60,"y":360},"data":{"label":"Note","config":{"text":"Save this flow to activate the schedule.\nEdit the Cron node to change timing.\nTimezone examples: UTC, Europe/London, US/Eastern"}}}
            ],
            "edges": [
                {"id":"ex6_e1","source":"ex6_n1","target":"ex6_n2"},
                {"id":"ex6_e2","source":"ex6_n2","target":"ex6_n3"}
            ]
        }
    },
]

def _seed_example_graphs():
    seeded = 0
    for eg in EXAMPLE_GRAPHS:
        try:
            if not get_graph_by_name(eg["name"]):
                create_graph(eg["name"], eg["description"], json.dumps(eg["graph_data"]))
                log.info(f"Seeded example graph: {eg['name']}")
                seeded += 1
        except Exception as e:
            log.warning(f"Could not seed '{eg['name']}': {e}")
    return seeded

# ── helpers ───────────────────────────────────────────────────────────────
def _graph_with_data(g):
    if not g: return None
    try:    gd = json.loads(g.get('graph_json') or '{}')
    except: gd = {}
    return {**{k:v for k,v in g.items() if k != 'graph_json'}, 'graph_data': gd}

def _sync_cron_triggers(graph_id: int, graph_data: dict):
    try:
        nodes = (graph_data or {}).get('nodes', [])
        sync_graph_schedules(graph_id, [n for n in nodes if n.get('type') == 'trigger.cron'])
    except Exception as e:
        log.warning(f"Could not sync schedules for graph {graph_id}: {e}")

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# ── page routes ───────────────────────────────────────────────────────────
@app.get("/")
def root(): return FileResponse(str(STATIC_DIR / "admin.html"), media_type="text/html")

@app.get("/admin")
@app.get("/admin/{rest:path}")
def admin_page(rest: str = ""): return FileResponse(str(STATIC_DIR / "admin.html"), media_type="text/html")

@app.get("/canvas")
@app.get("/admin/canvas")
def canvas_page(): return FileResponse(str(STATIC_DIR / "canvas.html"), media_type="text/html")

# ── health ────────────────────────────────────────────────────────────────
@app.get("/health")
def health(): return {"status": "ok", "version": "8"}

# ── API: runs ─────────────────────────────────────────────────────────────
def _sync_stuck_runs():
    """Reconcile queued/running runs against the Celery result backend.

    Runs >5 s old are checked; PENDING means Celery has no record — the task
    was likely lost (worker restarted before it started, or task wasn't
    registered). In that case we re-dispatch it so it actually executes.
    Runs stuck PENDING for >2 minutes are marked failed instead of looping.
    """
    try:
        from app.core.db import get_conn
        import psycopg2.extras
        from celery.result import AsyncResult
        from app.worker import app as _celery_app
        with get_conn() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("""
                SELECT task_id, workflow, status,
                       EXTRACT(EPOCH FROM (NOW() - created_at)) AS age_seconds
                FROM runs
                WHERE status IN ('queued','running')
                  AND created_at < NOW() - INTERVAL '5 seconds'
            """)
            stuck = cur.fetchall()
        for row in stuck:
            try:
                res   = AsyncResult(row['task_id'], app=_celery_app)
                state = res.state   # PENDING / STARTED / SUCCESS / FAILURE / RETRY / REVOKED
                age   = float(row['age_seconds'] or 0)

                if state == 'SUCCESS':
                    update_run(row['task_id'], 'succeeded',
                               result=res.result if isinstance(res.result, dict) else {'output': str(res.result)})

                elif state == 'FAILURE':
                    err = str(res.result) if res.result else 'Task failed (check worker logs)'
                    update_run(row['task_id'], 'failed', result={'error': err})

                elif state == 'REVOKED':
                    update_run(row['task_id'], 'failed', result={'error': 'Task was revoked'})

                elif state == 'PENDING':
                    # Celery has no result yet — task was never picked up or result expired.
                    if age > 120:
                        # Give up after 2 minutes — mark as failed
                        update_run(row['task_id'], 'failed',
                                   result={'error': 'Task was lost — worker may have been restarting. Please re-run.'})
                    elif row['workflow']:
                        # Re-dispatch the script so it actually runs
                        from app.worker import enqueue_script as _enqueue_script
                        _enqueue_script.apply_async(
                            args=[row['workflow'], {}],
                            task_id=row['task_id']
                        )
                        log.info(f"Re-dispatched lost task {row['task_id']} for {row['workflow']}")

            except Exception:
                pass
    except Exception:
        pass  # never let a sync error break the runs list


@app.get("/api/runs")
def api_runs(request: Request):
    _check_admin(request)
    _sync_stuck_runs()
    return list_runs()

@app.get("/api/runs/by-task/{task_id}")
def api_run_by_task(task_id: str, request: Request):
    """Lightweight single-run polling endpoint — used by canvas during execution."""
    _check_admin(request)
    run = get_run_by_task(task_id)
    if not run:
        raise HTTPException(404, "Run not found")
    return run

@app.delete("/api/runs/{run_id}")
def api_delete_run(run_id: int, request: Request): _check_admin(request); delete_run(run_id); return {"deleted": True}

@app.delete("/api/runs")
def api_clear_runs(request: Request): _check_admin(request); clear_runs(); return {"cleared": True}

@app.post("/api/runs/{run_id}/replay")
async def api_replay_run(run_id: int, request: Request):
    _check_admin(request)
    runs = list_runs()
    run  = next((r for r in runs if r['id'] == run_id), None)
    if not run: raise HTTPException(404, "Run not found")
    gid = run.get('graph_id')
    if not gid: raise HTTPException(400, "Run has no associated graph")
    g = get_graph(gid)
    if not g: raise HTTPException(404, "Graph not found")
    try:    payload = run.get('initial_payload') or {}
    except: payload = {}
    task = enqueue_graph.delay(gid, payload)
    # create a new run record for the replay
    from app.core.db import init_db
    try:
        from app.core.db import get_conn
        import psycopg2.extras
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO runs(task_id, graph_id, status, initial_payload) VALUES(%s,%s,'queued',%s)",
                (task.id, gid, json.dumps(payload))
            )
    except Exception as e:
        log.warning(f"Could not record replay run: {e}")
    return {"queued": True, "task_id": task.id, "graph": g["name"]}

# ── API: workflows ────────────────────────────────────────────────────────
@app.get("/api/workflows")
def api_workflows(request: Request): _check_admin(request); return list_workflows()

@app.post("/api/workflows/{name}/toggle")
def api_toggle_workflow(name: str, request: Request): _check_admin(request); return toggle_workflow(name) or {"name": name}

class RunRequest(BaseModel):
    payload: dict = {}

@app.post("/run/{workflow}")
def run_workflow(workflow: str, req: RunRequest, x_api_key: str = Header(default="")):
    _check_api_key(x_api_key)
    if workflow not in WORKFLOWS: raise HTTPException(404, "Unknown workflow")
    task = enqueue_workflow.delay(workflow, req.payload)
    return {"queued": True, "task_id": task.id, "workflow": workflow}

# ── API: schedules ────────────────────────────────────────────────────────
@app.get("/api/schedules")
def api_schedules(request: Request): _check_admin(request); return list_schedules()

class ScheduleCreate(BaseModel):
    name: str; graph_id: Optional[int] = None; workflow: Optional[str] = None
    cron: str = "0 9 * * *"; payload: dict = {}; timezone: str = "UTC"

@app.post("/api/schedules")
def api_create_schedule(body: ScheduleCreate, request: Request):
    _check_admin(request)
    return create_schedule(body.name, body.workflow, body.graph_id, body.cron, body.payload, body.timezone)

@app.post("/api/schedules/{sid}/toggle")
def api_toggle_schedule(sid: int, request: Request): _check_admin(request); return toggle_schedule(sid) or {"id": sid}

@app.delete("/api/schedules/{sid}")
def api_delete_schedule(sid: int, request: Request): _check_admin(request); delete_schedule(sid); return {"deleted": True}

# ── API: graphs ───────────────────────────────────────────────────────────
class GraphCreate(BaseModel):
    name: str; description: str = ""; graph_data: dict = {}

class GraphUpdate(BaseModel):
    name: Optional[str] = None; description: Optional[str] = None
    graph_data: Optional[dict] = None; enabled: Optional[bool] = None

@app.get("/api/graphs")
def api_graphs(request: Request): _check_admin(request); return [_graph_with_data(g) for g in list_graphs()]

@app.post("/api/graphs")
def api_graph_create(body: GraphCreate, request: Request):
    _check_admin(request)
    g = create_graph(body.name, body.description, json.dumps(body.graph_data))
    _sync_cron_triggers(g['id'], body.graph_data)
    save_graph_version(g['id'], body.name, json.dumps(body.graph_data), note="Initial version")
    return _graph_with_data(g)

@app.get("/api/graphs/by-slug/{slug}")
def api_graph_by_slug(slug: str, request: Request):
    _check_admin(request)
    g = get_graph_by_slug(slug)
    if not g: raise HTTPException(404, "Graph not found")
    return _graph_with_data(g)

@app.get("/api/graphs/{graph_id}")
def api_graph_get(graph_id: int, request: Request):
    _check_admin(request)
    g = get_graph(graph_id)
    if not g: raise HTTPException(404, "Graph not found")
    return _graph_with_data(g)

@app.put("/api/graphs/{graph_id}")
def api_graph_update(graph_id: int, body: GraphUpdate, request: Request):
    _check_admin(request)
    g = get_graph(graph_id)
    if not g: raise HTTPException(404, "Graph not found")
    update_graph(graph_id, name=body.name, description=body.description,
                 graph_json=json.dumps(body.graph_data) if body.graph_data is not None else None,
                 enabled=body.enabled)
    if body.graph_data is not None:
        _sync_cron_triggers(graph_id, body.graph_data)
        gname = body.name or g['name']
        save_graph_version(graph_id, gname, json.dumps(body.graph_data))
    return _graph_with_data(get_graph(graph_id))

@app.delete("/api/graphs/{graph_id}")
def api_graph_delete(graph_id: int, request: Request):
    _check_admin(request)
    if not get_graph(graph_id): raise HTTPException(404, "Graph not found")
    delete_graph(graph_id); return {"deleted": True, "id": graph_id}

@app.post("/api/graphs/reseed")
def api_graphs_reseed(request: Request):
    _check_admin(request)
    n = _seed_example_graphs()
    return {"seeded": n, "message": f"Re-seeded {n} missing example flow(s)"}

@app.post("/api/graphs/{graph_id}/run")
async def api_graph_run(graph_id: int, request: Request):
    _check_admin(request)
    g = get_graph(graph_id)
    if not g: raise HTTPException(404, "Graph not found")
    try:    body = await request.json()
    except: body = {}
    payload = body or {"source": "api"}
    task = enqueue_graph.delay(graph_id, payload)
    # record the run with its initial payload
    try:
        from app.core.db import get_conn
        with get_conn() as conn:
            conn.cursor().execute(
                "INSERT INTO runs(task_id, graph_id, status, initial_payload) VALUES(%s,%s,'queued',%s)",
                (task.id, graph_id, json.dumps(payload))
            )
    except Exception as e:
        log.warning(f"Could not pre-create run record: {e}")
    return {"queued": True, "task_id": task.id, "graph": g["name"]}

# ── API: graph versions ───────────────────────────────────────────────────
@app.get("/api/graphs/{graph_id}/versions")
def api_graph_versions(graph_id: int, request: Request):
    _check_admin(request)
    if not get_graph(graph_id): raise HTTPException(404, "Graph not found")
    return list_graph_versions(graph_id)

@app.post("/api/graphs/{graph_id}/versions/{version_id}/restore")
def api_restore_version(graph_id: int, version_id: int, request: Request):
    _check_admin(request)
    g = get_graph(graph_id)
    if not g: raise HTTPException(404, "Graph not found")
    v = get_graph_version(version_id)
    if not v or v['graph_id'] != graph_id: raise HTTPException(404, "Version not found")
    update_graph(graph_id, graph_json=v['graph_json'])
    try:   gd = json.loads(v['graph_json'])
    except: gd = {}
    _sync_cron_triggers(graph_id, gd)
    save_graph_version(graph_id, g['name'], v['graph_json'], note=f"Restored from v{v['version']}")
    return _graph_with_data(get_graph(graph_id))

# ── API: credentials ──────────────────────────────────────────────────────
class CredCreate(BaseModel):
    name: str; type: str = "generic"; secret: str; note: str = ""

@app.get("/api/credentials")
def api_creds(request: Request): _check_admin(request); return list_credentials()

@app.post("/api/credentials")
def api_cred_create(body: CredCreate, request: Request):
    _check_admin(request)
    return upsert_credential(body.name, body.type, body.secret, body.note)

@app.put("/api/credentials/{cred_id}")
def api_cred_update(cred_id: int, body: CredCreate, request: Request):
    _check_admin(request)
    return upsert_credential(body.name, body.type, body.secret, body.note)

@app.delete("/api/credentials/{cred_id}")
def api_cred_delete(cred_id: int, request: Request):
    _check_admin(request); delete_credential(cred_id); return {"deleted": True}

# ── Webhook trigger ───────────────────────────────────────────────────────
@app.post("/webhook/{token}")
async def webhook_trigger(token: str, request: Request):
    g = get_graph_by_token(token)
    if not g: raise HTTPException(404, "Unknown webhook token")
    if not g.get('enabled'): raise HTTPException(403, "Graph is disabled")
    if not _check_webhook_rate(token):
        raise HTTPException(429, f"Rate limit exceeded — max {_WEBHOOK_RATE_LIMIT} calls per {_WEBHOOK_RATE_WINDOW}s")
    try:    payload = await request.json()
    except: payload = {}
    task = enqueue_graph.delay(g['id'], payload)
    try:
        from app.core.db import get_conn
        with get_conn() as conn:
            conn.cursor().execute(
                "INSERT INTO runs(task_id, graph_id, status, initial_payload) VALUES(%s,%s,'queued',%s)",
                (task.id, g['id'], json.dumps(payload))
            )
    except Exception: pass
    return {"queued": True, "task_id": task.id, "graph": g["name"]}

# ── Admin API: misc ───────────────────────────────────────────────────────
@app.get("/api/runlogs")
def api_runlogs(request: Request):
    _check_admin(request)
    if not RUNLOGS_DIR.exists(): return []
    return sorted([f.name for f in RUNLOGS_DIR.glob("*.log")], reverse=True)[:50]

@app.get("/api/runlogs/{filename}")
def api_runlog_file(filename: str, request: Request):
    _check_admin(request)
    p = RUNLOGS_DIR / filename
    if not p.exists() or not p.name.endswith(".log"): raise HTTPException(404)
    return {"content": p.read_text(errors="replace")[-8000:]}

@app.post("/api/admin/reset")
def api_reset(request: Request):
    _check_admin(request)
    from app.core.db import get_conn
    with get_conn() as conn:
        cur = conn.cursor()
        for t in ["runs", "schedules", "graph_versions", "graph_workflows", "workflows"]:
            cur.execute(f"DELETE FROM {t}")
    return {"reset": True}

# ── API: metrics ────────────────────────────────────────────────────────────
@app.get("/api/metrics")
def api_metrics(request: Request):
    _check_admin(request)
    return get_run_metrics()

# ── API: scripts ─────────────────────────────────────────────────────────────
def _safe_script_name(name: str) -> str:
    if not _re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', name):
        raise HTTPException(400, "Script name must start with a letter and contain only letters, digits, underscores")
    if name.startswith('_'):
        raise HTTPException(400, "Script names starting with _ are reserved")
    return name

@app.get("/api/scripts")
def api_list_scripts(request: Request):
    _check_admin(request)
    scripts = []
    for p in sorted(SCRIPTS_DIR.glob("*.py")):
        if p.stem.startswith("_"): continue
        scripts.append({
            "name": p.stem,
            "size": p.stat().st_size,
            "modified": p.stat().st_mtime,
        })
    return scripts

@app.get("/api/scripts/{name}")
def api_get_script(name: str, request: Request):
    _check_admin(request)
    _safe_script_name(name)
    p = SCRIPTS_DIR / f"{name}.py"
    if not p.exists(): raise HTTPException(404, "Script not found")
    return {"name": name, "content": p.read_text(errors="replace")}

@app.post("/api/scripts")
async def api_create_script(request: Request):
    _check_admin(request)
    body = await request.json()
    name = _safe_script_name(body.get("name", ""))
    content = body.get("content", "# New script\n")
    p = SCRIPTS_DIR / f"{name}.py"
    if p.exists(): raise HTTPException(409, "Script already exists")
    p.write_text(content)
    return {"name": name, "created": True}

@app.put("/api/scripts/{name}")
async def api_update_script(name: str, request: Request):
    _check_admin(request)
    _safe_script_name(name)
    body = await request.json()
    content = body.get("content", "")
    p = SCRIPTS_DIR / f"{name}.py"
    if not p.exists(): raise HTTPException(404, "Script not found")
    p.write_text(content)
    return {"name": name, "saved": True}

@app.delete("/api/scripts/{name}")
def api_delete_script(name: str, request: Request):
    _check_admin(request)
    _safe_script_name(name)
    p = SCRIPTS_DIR / f"{name}.py"
    if not p.exists(): raise HTTPException(404, "Script not found")
    p.unlink()
    return {"name": name, "deleted": True}

@app.post("/api/scripts/{name}/run")
async def api_run_script(name: str, request: Request):
    _check_admin(request)
    _safe_script_name(name)
    try: payload = await request.json()
    except: payload = {}
    import uuid
    from app.core.db import get_conn
    task_id = str(uuid.uuid4())
    # Insert the run row BEFORE dispatching so the worker always finds it
    with get_conn() as conn:
        conn.cursor().execute(
            "INSERT INTO runs(task_id,workflow,status) VALUES(%s,%s,'queued')",
            (task_id, name)
        )
    enqueue_script.apply_async(args=[name, payload], task_id=task_id)
    return {"task_id": task_id, "workflow": name}


# ── Workflow Templates ────────────────────────────────────────────────────────

@app.get("/api/templates")
def api_list_templates(request: Request):
    """Return all workflow templates (JSON files from app/templates/)."""
    _check_admin(request)
    TEMPLATES_DIR.mkdir(exist_ok=True)
    results = []
    for p in sorted(TEMPLATES_DIR.glob("*.json")):
        try:
            data = json.loads(p.read_text())
            results.append({
                "id":          p.stem,
                "name":        data.get("name", p.stem),
                "description": data.get("description", ""),
                "category":    data.get("category", "General"),
                "tags":        data.get("tags", []),
                "node_count":  len(data.get("graph_data", {}).get("nodes", [])),
            })
        except Exception:
            pass
    return results

@app.post("/api/templates/{template_id}/use")
def api_use_template(template_id: str, request: Request):
    """Instantiate a template as a new graph and return the created graph."""
    _check_admin(request)
    if not _re.match(r'^[a-zA-Z0-9_\-]+$', template_id):
        raise HTTPException(400, "Invalid template id")
    p = TEMPLATES_DIR / f"{template_id}.json"
    if not p.exists():
        raise HTTPException(404, f"Template '{template_id}' not found")
    data = json.loads(p.read_text())
    name = data.get("name", template_id)
    graph_data = data.get("graph_data", {"nodes": [], "edges": []})
    g = create_graph(name, data.get("description", ""), graph_data)
    return g


# ── Run Replay ────────────────────────────────────────────────────────────────

@app.post("/api/runs/{run_id}/replay")
def api_replay_run(run_id: int, request: Request):
    """Re-enqueue a past run using its stored initial_payload."""
    _check_admin(request)
    from app.core.db import get_conn
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT graph_id, initial_payload FROM runs WHERE id=%s", (run_id,))
        row = cur.fetchone()
    if not row:
        raise HTTPException(404, f"Run {run_id} not found")
    graph_id, initial_payload = row
    if not graph_id:
        raise HTTPException(400, "Run is not associated with a graph (script run?)")
    g = get_graph(graph_id)
    if not g:
        raise HTTPException(404, f"Graph {graph_id} not found")
    try:
        payload = json.loads(initial_payload) if initial_payload else {}
    except Exception:
        payload = {}
    task = enqueue_graph.delay(graph_id, payload)
    with get_conn() as conn:
        conn.cursor().execute(
            "INSERT INTO runs(task_id, graph_id, status, initial_payload) VALUES(%s,%s,'queued',%s)",
            (task.id, graph_id, json.dumps(payload))
        )
    return {"queued": True, "task_id": task.id, "graph": g["name"], "replayed_run_id": run_id}


# ── Node registry management ──────────────────────────────────────────────────

@app.get("/api/nodes")
def api_list_nodes(request: Request):
    """List all registered node types (built-in + custom)."""
    _check_admin(request)
    from app.nodes import list_node_types
    return {"node_types": list_node_types()}

@app.post("/api/admin/reload_nodes")
def api_reload_nodes(request: Request):
    """Hot-reload custom nodes from app/nodes/custom/ without restarting."""
    _check_admin(request)
    from app.nodes import reload_custom, list_node_types
    reload_custom()
    return {"reloaded": True, "node_types": list_node_types()}


# ── Webhook rate limiting ─────────────────────────────────────────────────────
# Token-bucket: max WEBHOOK_RATE_LIMIT calls per WEBHOOK_RATE_WINDOW seconds per token.
# Stored in Redis. Silently skipped if Redis is unavailable.

_WEBHOOK_RATE_LIMIT  = int(os.environ.get("WEBHOOK_RATE_LIMIT", "60"))   # max calls
_WEBHOOK_RATE_WINDOW = int(os.environ.get("WEBHOOK_RATE_WINDOW", "60"))  # per N seconds

def _check_webhook_rate(token: str) -> bool:
    """Returns True if the request is allowed, False if rate-limited."""
    if _WEBHOOK_RATE_LIMIT <= 0:
        return True  # rate limiting disabled
    try:
        import redis as _redis
        r = _redis.from_url(os.environ.get("CELERY_BROKER_URL", "redis://redis:6379/0"))
        key = f"wh_rate:{token}"
        pipe = r.pipeline()
        pipe.incr(key)
        pipe.expire(key, _WEBHOOK_RATE_WINDOW)
        count, _ = pipe.execute()
        return count <= _WEBHOOK_RATE_LIMIT
    except Exception:
        return True  # fail open if Redis unavailable



MAIN_EOF

# ── 17. app/static/admin.html ──────────────────────────────────────────────
cat > "$PROJECT/app/static/admin.html" << 'HTML_EOF'
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>HiveRunr</title>
<script src="https://unpkg.com/react@18/umd/react.production.min.js"></script>
<script src="https://unpkg.com/react-dom@18/umd/react-dom.production.min.js"></script>
<script src="https://unpkg.com/@babel/standalone/babel.min.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Inter',system-ui,sans-serif;background:#0f1117;color:#e2e8f0;min-height:100vh}
#root{display:flex;min-height:100vh}
.sidebar{width:220px;background:#1a1d2e;border-right:1px solid #2a2d3e;display:flex;flex-direction:column;padding:20px 0;flex-shrink:0;position:fixed;height:100vh;z-index:10}
.sidebar .logo{font-size:16px;font-weight:700;color:#a78bfa;padding:0 20px 24px;letter-spacing:-.3px}
.nav-item{display:flex;align-items:center;gap:10px;padding:10px 20px;cursor:pointer;color:#94a3b8;font-size:13px;font-weight:500;border-radius:0;transition:all .15s;border-left:3px solid transparent}
.nav-item:hover{background:#252840;color:#e2e8f0}
.nav-item.active{background:#252840;color:#a78bfa;border-left-color:#7c3aed}
.nav-icon{font-size:16px;width:20px;text-align:center}
.sidebar-footer{margin-top:auto;border-top:1px solid #2a2d3e;padding:14px 12px 10px}
.sidebar-footer .footer-label{font-size:10px;color:#4b5563;padding:0 8px;margin-bottom:8px;text-transform:uppercase;letter-spacing:.07em;font-weight:600}
.ext-link{display:flex;align-items:center;gap:9px;padding:8px 10px;border-radius:7px;color:#64748b;font-size:12px;font-weight:500;text-decoration:none;transition:all .15s;cursor:pointer}
.ext-link:hover{background:#252840;color:#e2e8f0}
.ext-link .el-icon{font-size:15px;width:20px;text-align:center}
.main{margin-left:220px;flex:1;padding:32px;max-width:1100px}
.page-title{font-size:22px;font-weight:700;margin-bottom:24px;color:#f1f5f9}
.card{background:#1a1d2e;border:1px solid #2a2d3e;border-radius:10px;padding:20px;margin-bottom:16px}
.card-title{font-size:13px;font-weight:600;color:#94a3b8;text-transform:uppercase;letter-spacing:.07em;margin-bottom:14px}
.stat-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:24px}
.stat-card{background:#1a1d2e;border:1px solid #2a2d3e;border-radius:10px;padding:18px 20px}
.stat-val{font-size:28px;font-weight:700;color:#e2e8f0}
.stat-lbl{font-size:12px;color:#64748b;margin-top:4px}
.badge{display:inline-flex;align-items:center;gap:4px;padding:3px 9px;border-radius:20px;font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.05em}
.badge-queued{background:#1e3a5f;color:#60a5fa}
.badge-running{background:#1c3238;color:#34d399;animation:pulse 1.5s infinite}
.badge-succeeded{background:#14532d;color:#4ade80}
.badge-failed{background:#3f1111;color:#f87171}
.badge-cancelled{background:#2d2d2d;color:#9ca3af}
.badge-credential{display:inline-block;padding:4px 10px;border-radius:6px;font-size:11px;font-weight:600;font-family:monospace;background:#0f1117;border:1px solid #2a2d3e}
.badge-credential.openai_api{background:#5b21b633;color:#d8b4fe;border-color:#7c3aed}
.badge-credential.smtp{background:#1e3a8a33;color:#93c5fd;border-color:#0284c7}
.badge-credential.telegram{background:#164e6333;color:#67e8f9;border-color:#0891b2}
.badge-credential.aws{background:#7c2d1233;color:#fed7aa;border-color:#d97706}
.badge-credential.ssh{background:#0f4c3a33;color:#34d399;border-color:#059669}
.badge-credential.ftp{background:#1e3a5f33;color:#7dd3fc;border-color:#0284c7}
.badge-credential.generic,.badge-credential.other{background:#2d2d2d33;color:#9ca3af;border-color:#4b5563}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.6}}
table{width:100%;border-collapse:collapse;font-size:13px}
th{text-align:left;color:#64748b;font-weight:500;padding:8px 12px;border-bottom:1px solid #2a2d3e;font-size:12px}
td{padding:10px 12px;border-bottom:1px solid #1e2235;color:#cbd5e1;vertical-align:middle}
tr:last-child td{border-bottom:none}
tr:hover td{background:#1e2235}
.btn{display:inline-flex;align-items:center;gap:5px;padding:6px 14px;border-radius:6px;font-size:12px;font-weight:500;border:none;cursor:pointer;transition:all .15s}
.btn-primary{background:#7c3aed;color:#fff}.btn-primary:hover{background:#6d28d9}
.btn-success{background:#059669;color:#fff}.btn-success:hover{background:#047857}
.btn-danger{background:#dc2626;color:#fff}.btn-danger:hover{background:#b91c1c}
.btn-ghost{background:#2a2d3e;color:#94a3b8;border:1px solid #374151}.btn-ghost:hover{color:#e2e8f0;background:#374151}
.btn-warning{background:#d97706;color:#fff}.btn-warning:hover{background:#b45309}
.btn:disabled{opacity:.45;cursor:not-allowed}
.form-row{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px}
.form-group{margin-bottom:12px}
label{display:block;font-size:11px;color:#94a3b8;font-weight:500;margin-bottom:5px}
input,select,textarea{width:100%;background:#0f1117;color:#e2e8f0;border:1px solid #2a2d3e;border-radius:6px;padding:8px 10px;font-size:13px;font-family:inherit}
input:focus,select:focus,textarea:focus{outline:none;border-color:#7c3aed}
.wf-pill{display:flex;align-items:center;justify-content:space-between;background:#0f1117;border:1px solid #2a2d3e;border-radius:8px;padding:10px 14px;margin-bottom:8px}
.wf-name{font-size:13px;font-weight:500}
.wf-enabled{font-size:11px;color:#059669}
.wf-disabled{font-size:11px;color:#dc2626}
.log-file{background:#0f1117;border:1px solid #2a2d3e;border-radius:6px;padding:10px 14px;font-size:12px;font-family:monospace;cursor:pointer;margin-bottom:6px;color:#94a3b8;transition:border-color .15s}
.log-file:hover{border-color:#7c3aed;color:#e2e8f0}
.log-content{background:#0a0c14;border:1px solid #2a2d3e;border-radius:6px;padding:14px;font-size:11px;font-family:monospace;white-space:pre-wrap;max-height:400px;overflow-y:auto;color:#94a3b8;line-height:1.7}
.actions-row{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px}
.empty-state{color:#4b5563;font-size:13px;text-align:center;padding:40px 0}
.toast{position:fixed;bottom:20px;right:20px;background:#1a1d2e;border:1px solid #2a2d3e;border-radius:8px;padding:10px 16px;font-size:13px;z-index:9999;display:flex;align-items:center;gap:8px;box-shadow:0 8px 24px rgba(0,0,0,.4);animation:slideUp .2s ease}
.toast.ok{border-color:#059669;color:#34d399}
.toast.err{border-color:#dc2626;color:#f87171}
@keyframes slideUp{from{transform:translateY(12px);opacity:0}to{transform:translateY(0);opacity:1}}
.info-box{background:#0f1117;border:1px solid #2a2d3e;border-left:3px solid #7c3aed;border-radius:6px;padding:12px 14px;margin-bottom:16px;font-size:12px;color:#94a3b8;line-height:1.6}
.trace-table{margin-top:10px;background:#0f1117;border:1px solid #2a2d3e;border-radius:6px;padding:0;overflow:hidden}
.trace-table table{margin:0;font-size:12px}
.trace-table th,.trace-table td{padding:8px 10px}
.trace-status-dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:5px}
.trace-status-dot.ok{background:#4ade80}
.trace-status-dot.err{background:#f87171}
.trace-status-dot.skipped{background:#64748b}
.modal-overlay{position:fixed;inset:0;background:rgba(0,0,0,.75);z-index:1000;display:flex;align-items:center;justify-content:center;backdrop-filter:blur(4px)}
.modal{background:#1a1d2e;border:1px solid #2a2d3e;border-radius:12px;padding:22px;min-width:400px;max-width:600px;max-height:80vh;overflow-y:auto;box-shadow:0 20px 60px rgba(0,0,0,.6)}
.modal h2{font-size:15px;font-weight:600;margin-bottom:14px;color:#e2e8f0}
.modal-section{font-size:10px;color:#64748b;font-weight:600;text-transform:uppercase;letter-spacing:.06em;margin:12px 0 8px;padding-bottom:5px;border-bottom:1px solid #2a2d3e}
.modal-btns{display:flex;gap:8px;justify-content:flex-end;margin-top:16px}
.version-row{padding:10px;border-bottom:1px solid #2a2d3e;display:flex;align-items:center;justify-content:space-between}
.version-row:last-child{border-bottom:none}
.version-info{flex:1;min-width:0;font-size:12px}
.version-num{font-weight:600;color:#e2e8f0;margin-bottom:2px}
.version-date{font-size:11px;color:#64748b}
.version-note{font-size:11px;color:#94a3b8;margin-top:2px}
.expandable-row{cursor:pointer;transition:background .15s}
.expandable-row:hover td{background:#252840}
.trace-collapsed{background:#252840;border:1px solid #2a2d3e;border-radius:6px;padding:12px 14px;font-size:11px;color:#94a3b8;display:none;margin-top:8px}
.trace-collapsed.show{display:block}
.stat-cards{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:24px}
.stat-card{background:#1a1d2e;border:1px solid #2a2d3e;border-radius:10px;padding:20px;text-align:center}
.stat-value{font-size:32px;font-weight:700;margin-bottom:4px;line-height:1}
.stat-label{font-size:12px;color:#64748b}
.bar-chart{display:flex;align-items:flex-end;gap:3px;height:80px;margin:12px 0 6px}
.bar-col{display:flex;flex-direction:column-reverse;align-items:center;flex:1;gap:1px}
.bar-seg{width:100%;border-radius:2px 2px 0 0;min-height:2px;transition:height .3s}
.bar-label{font-size:9px;color:#4b5563;margin-top:4px;text-align:center}
.script-list{width:210px;flex-shrink:0;background:#1a1d2e;border-right:1px solid #2a2d3e;display:flex;flex-direction:column;height:calc(100vh - 160px)}
.script-item{padding:10px 14px;cursor:pointer;border-bottom:1px solid #1e2235;transition:background .12s}
.script-item:hover{background:#2a2d3e}
.script-item.active{background:#2a2d3e;border-left:2px solid #7c3aed}
.script-item-name{font-size:13px;color:#e2e8f0;font-weight:500}
.script-item-meta{font-size:10px;color:#4b5563;margin-top:2px}
.editor-wrap{flex:1;display:flex;flex-direction:column;overflow:hidden}
.editor-toolbar{padding:8px 14px;background:#1a1d2e;border-bottom:1px solid #2a2d3e;display:flex;align-items:center;gap:8px}
.editor-container{flex:1}
</style>
</head>
<body>
<div id="root"></div>
<script type="text/babel">
const { useState, useEffect, useCallback, useRef } = React;

const params = new URLSearchParams(window.location.search);
const TOKEN  = params.get("token") || "";

async function api(method, path, body) {
  const r = await fetch(path, {
    method,
    headers: {"Content-Type":"application/json","x-admin-token": TOKEN},
    body: body ? JSON.stringify(body) : undefined
  });
  if (!r.ok) { const e = await r.json().catch(()=>({detail:r.statusText})); throw new Error(e.detail || r.statusText); }
  return r.json();
}

function Toast({ msg, type, onDone }) {
  useEffect(()=>{ const t=setTimeout(onDone,3000); return ()=>clearTimeout(t); },[]);
  return <div className={`toast ${type==="error"?"err":"ok"}`}>{type==="error"?"✗":"✓"} {msg}</div>;
}

// ── Dashboard ──────────────────────────────────────────────────────────────
function Dashboard({ showToast }) {
  const [runs, setRuns]       = useState([]);
  const [workflows, setWfs]   = useState([]);
  const [loading, setLoading] = useState(true);
  const [expandedRunId, setExpandedRunId] = useState(null);

  const load = useCallback(async () => {
    try {
      const [r, w] = await Promise.all([api("GET","/api/runs"), api("GET","/api/workflows")]);
      setRuns(r); setWfs(w);
    } catch(e) { showToast(e.message,"error"); }
    setLoading(false);
  }, []);

  useEffect(() => { load(); const t=setInterval(load,5000); return ()=>clearInterval(t); }, []);

  const stats = {
    total: runs.length,
    active: runs.filter(r=>r.status==="running"||r.status==="queued").length,
    ok: runs.filter(r=>r.status==="succeeded").length,
    failed: runs.filter(r=>r.status==="failed").length
  };

  async function toggleWf(name) {
    try { await api("POST",`/api/workflows/${name}/toggle`); load(); showToast("Toggled"); }
    catch(e) { showToast(e.message,"error"); }
  }
  async function runWf(name) {
    try { await api("POST",`/api/workflows/${name}/run`,{payload:{}}); load(); showToast("Queued!"); }
    catch(e) { showToast(e.message,"error"); }
  }
  async function deleteRun(id) {
    try { await api("DELETE",`/api/runs/${id}`); load(); }
    catch(e) { showToast(e.message,"error"); }
  }
  async function cancelRun(id) {
    try { await api("POST",`/api/runs/${id}/cancel`); load(); showToast("Cancelled"); }
    catch(e) { showToast(e.message,"error"); }
  }
  async function replayRun(id) {
    try { await api("POST",`/api/runs/${id}/replay`); load(); showToast("Queued for replay"); }
    catch(e) { showToast(e.message,"error"); }
  }

  return (
    <div>
      <h1 className="page-title">Dashboard</h1>
      <div className="stat-grid">
        <div className="stat-card"><div className="stat-val">{stats.total}</div><div className="stat-lbl">Total Runs</div></div>
        <div className="stat-card"><div className="stat-val" style={{color:"#60a5fa"}}>{stats.active}</div><div className="stat-lbl">Active</div></div>
        <div className="stat-card"><div className="stat-val" style={{color:"#4ade80"}}>{stats.ok}</div><div className="stat-lbl">Succeeded</div></div>
        <div className="stat-card"><div className="stat-val" style={{color:"#f87171"}}>{stats.failed}</div><div className="stat-lbl">Failed</div></div>
      </div>
      {workflows.length > 0 && (
        <div className="card" style={{marginBottom:24}}>
          <div className="card-title">Python Workflows</div>
          {workflows.map(w=>(
            <div key={w.name} className="wf-pill">
              <div>
                <div className="wf-name">{w.name}</div>
                <div className={w.enabled?"wf-enabled":"wf-disabled"}>{w.enabled?"● enabled":"○ disabled"}</div>
              </div>
              <div style={{display:"flex",gap:8}}>
                <button className="btn btn-ghost" onClick={()=>toggleWf(w.name)}>Toggle</button>
                <button className="btn btn-success" disabled={!w.enabled} onClick={()=>runWf(w.name)}>▶ Run</button>
              </div>
            </div>
          ))}
        </div>
      )}
      <div className="card">
        <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:14}}>
          <div className="card-title" style={{marginBottom:0}}>Recent Runs</div>
          <button className="btn btn-ghost" onClick={()=>api("DELETE","/api/runs").then(load)}>Clear all</button>
        </div>
        {loading ? <div className="empty-state">Loading…</div> : runs.length === 0 ? (
          <div className="empty-state">No runs yet. Trigger a workflow to get started.</div>
        ) : (
          <>
            <table>
              <thead><tr><th>#</th><th>Workflow</th><th>Status</th><th>Started</th><th></th></tr></thead>
              <tbody>
                {runs.map(r=>(
                  <React.Fragment key={r.id}>
                    <tr className="expandable-row" onClick={()=>setExpandedRunId(expandedRunId===r.id?null:r.id)} style={{cursor:'pointer'}}>
                      <td style={{color:"#4b5563"}}>{r.id}</td>
                      <td>{r.workflow || (r.graph_id ? `graph #${r.graph_id}` : "—")}</td>
                      <td><span className={`badge badge-${r.status}`}>{r.status}</span></td>
                      <td style={{color:"#64748b"}}>{r.created_at ? new Date(r.created_at).toLocaleString() : "—"}</td>
                      <td>
                        <div style={{display:"flex",gap:6}}>
                          {r.graph_id && <button className="btn btn-ghost" onClick={(e)=>{e.stopPropagation();replayRun(r.id)}}>▶ Replay</button>}
                          {(r.status==="queued"||r.status==="running") &&
                            <button className="btn btn-warning" onClick={(e)=>{e.stopPropagation();cancelRun(r.id)}}>Cancel</button>}
                          <button className="btn btn-danger" onClick={(e)=>{e.stopPropagation();deleteRun(r.id)}}>✕</button>
                        </div>
                      </td>
                    </tr>
                    {expandedRunId===r.id && (
                      <tr style={{background:"#0f1117"}}>
                        <td colSpan="5" style={{padding:"12px"}}>
                          {r.initial_payload && Object.keys(r.initial_payload).length > 0 && (
                            <div style={{marginBottom:12}}>
                              <div style={{fontSize:11,fontWeight:600,color:"#94a3b8",marginBottom:6}}>Initial Payload</div>
                              <div style={{background:"#0a0c14",border:"1px solid #2a2d3e",borderRadius:6,padding:10,fontFamily:"monospace",fontSize:11,color:"#94a3b8",maxHeight:200,overflowY:"auto"}}>
                                {JSON.stringify(r.initial_payload,null,2)}
                              </div>
                            </div>
                          )}
                          {/* ── run output / error ── */}
                          {r.result && (r.result.output || r.result.error) && (
                            <div style={{marginBottom:12}}>
                              <div style={{fontSize:11,fontWeight:600,color: r.result.error?"#f87171":"#4ade80",marginBottom:6}}>
                                {r.result.error ? "❌ Error" : "✅ Output"}
                              </div>
                              <div style={{
                                background:"#0a0c14",border:`1px solid ${r.result.error?"#7f1d1d":"#14532d"}`,
                                borderRadius:6,padding:10,fontFamily:"monospace",fontSize:11,
                                color: r.result.error?"#fca5a5":"#86efac",
                                maxHeight:300,overflowY:"auto",whiteSpace:"pre-wrap",wordBreak:"break-word"
                              }}>
                                {r.result.error || r.result.output}
                              </div>
                            </div>
                          )}
                          {r.traces && r.traces.length > 0 ? (
                            <div>
                              <div style={{fontSize:11,fontWeight:600,color:"#94a3b8",marginBottom:6}}>Trace</div>
                              <div className="trace-table">
                                <table>
                                  <thead><tr><th>Node</th><th>Type</th><th>Status</th><th>Duration (ms)</th><th>Attempts</th><th>Output/Error</th></tr></thead>
                                  <tbody>
                                    {r.traces.map((t,idx)=>(
                                      <tr key={idx}>
                                        <td style={{fontFamily:"monospace",fontSize:11}}>{t.node_id}</td>
                                        <td style={{fontSize:11}}>{t.type}</td>
                                        <td>
                                          <span className={`trace-status-dot ${t.status===undefined||t.status==="ok"?"ok":t.status==="error"?"err":"skipped"}`}/>
                                          {t.status===undefined||t.status==="ok"?"ok":t.status==="error"?"error":"skipped"}
                                        </td>
                                        <td>{t.duration_ms ?? "—"}</td>
                                        <td>{t.attempts ?? 1}</td>
                                        <td>
                                          <div style={{maxWidth:200,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap",fontSize:11}}>
                                            {t.error ? <span style={{color:"#f87171"}}>{String(t.error).slice(0,50)}</span> :
                                             t.output ? <span style={{color:"#94a3b8"}}>{typeof t.output==="string"?t.output:JSON.stringify(t.output).slice(0,50)}</span> :
                                             "—"}
                                          </div>
                                        </td>
                                      </tr>
                                    ))}
                                  </tbody>
                                </table>
                              </div>
                            </div>
                          ) : <div style={{color:"#4b5563",fontSize:12}}>No trace data available</div>}
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                ))}
              </tbody>
            </table>
          </>
        )}
      </div>
    </div>
  );
}

// ── Metrics ────────────────────────────────────────────────────────────────
function relativeTime(ts) {
  const d = Math.floor((Date.now() - new Date(ts)) / 1000);
  if (d < 60) return `${d}s ago`;
  if (d < 3600) return `${Math.floor(d/60)}m ago`;
  if (d < 86400) return `${Math.floor(d/3600)}h ago`;
  return `${Math.floor(d/86400)}d ago`;
}

function Metrics({ showToast }) {
  const [metrics, setMetrics] = useState(null);
  const [failingFlows, setFailingFlows] = useState([]);
  const [recentRuns, setRecentRuns] = useState([]);
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    try {
      const m = await api("GET", "/api/metrics");
      setMetrics(m);

      const runs = await api("GET", "/api/runs");
      const failMap = {};
      runs.forEach(r => {
        if (r.status === "failed") {
          const wf = r.workflow || `graph #${r.graph_id || '?'}`;
          failMap[wf] = (failMap[wf] || 0) + 1;
        }
      });
      const failing = Object.entries(failMap).sort((a, b) => b[1] - a[1]).slice(0, 5);
      setFailingFlows(failing);
      setRecentRuns(runs.slice(0, 10));
    } catch(e) { showToast(e.message, "error"); }
    setLoading(false);
  }, []);

  useEffect(() => { load(); }, []);

  if (loading) return <div className="empty-state">Loading metrics…</div>;
  if (!metrics) return <div className="empty-state">No metrics available.</div>;

  const lastDay = (metrics.run_counts || []).slice(-14);
  const maxRuns = Math.max(...lastDay.map(d => (d.succeeded || 0) + (d.failed || 0)), 1);

  return (
    <div>
      <h1 className="page-title">Metrics</h1>

      <div className="stat-cards">
        <div className="stat-card">
          <div className="stat-value" style={{color:"#a78bfa"}}>{metrics.total_runs || 0}</div>
          <div className="stat-label">Total Runs</div>
        </div>
        <div className="stat-card">
          <div className="stat-value" style={{color:"#4ade80"}}>{metrics.succeeded_runs || 0}</div>
          <div className="stat-label">Succeeded</div>
        </div>
        <div className="stat-card">
          <div className="stat-value" style={{color:"#f87171"}}>{metrics.failed_runs || 0}</div>
          <div className="stat-label">Failed</div>
        </div>
        <div className="stat-card">
          <div className="stat-value" style={{color:"#38bdf8"}}>{Math.round(metrics.avg_duration_ms || 0)}</div>
          <div className="stat-label">Avg Duration (ms)</div>
        </div>
      </div>

      <div className="card">
        <div className="card-title">Last 14 Days</div>
        <div className="bar-chart">
          {lastDay.map((day, idx) => {
            const succeeded = day.succeeded || 0;
            const failed = day.failed || 0;
            const total = succeeded + failed;
            const sucHeight = maxRuns > 0 ? (succeeded / maxRuns) * 70 : 0;
            const failHeight = maxRuns > 0 ? (failed / maxRuns) * 70 : 0;
            const dateStr = new Date(day.date).toLocaleDateString().slice(-2).padStart(2, '0');
            return (
              <div key={idx} className="bar-col" title={`${dateStr}: ${succeeded}✓ ${failed}✗`}>
                {succeeded > 0 && <div className="bar-seg" style={{height:`${sucHeight}px`,background:"#4ade80"}}/>}
                {failed > 0 && <div className="bar-seg" style={{height:`${failHeight}px`,background:"#f87171"}}/>}
                <div className="bar-label">{dateStr}</div>
              </div>
            );
          })}
        </div>
      </div>

      {failingFlows.length > 0 && (
        <div className="card">
          <div className="card-title">Top Failing Flows</div>
          <table>
            <thead><tr><th>Flow</th><th>Failed</th></tr></thead>
            <tbody>
              {failingFlows.map(([name, count]) => (
                <tr key={name}>
                  <td>{name}</td>
                  <td><span className="badge badge-failed">{count}</span></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {recentRuns.length > 0 && (
        <div className="card">
          <div className="card-title">Recent Runs</div>
          <table>
            <thead><tr><th>Flow</th><th>Status</th><th>Duration (ms)</th><th>Time</th></tr></thead>
            <tbody>
              {recentRuns.map(r => (
                <tr key={r.id}>
                  <td style={{fontSize:12}}>{r.workflow || `graph #${r.graph_id || '?'}`}</td>
                  <td><span className={`badge badge-${r.status}`}>{r.status}</span></td>
                  <td style={{fontSize:12}}>{r.duration_ms || "—"}</td>
                  <td style={{fontSize:12,color:"#64748b"}}>{relativeTime(r.started_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// ── Scripts ────────────────────────────────────────────────────────────────
function Scripts({ showToast }) {
  const [scripts, setScripts]           = useState([]);
  const [selectedScript, setSelectedScript] = useState(null);
  const [loadingContent, setLoadingContent] = useState(false);
  const [newName, setNewName]           = useState("");
  const [creating, setCreating]         = useState(false);
  const monacoRef          = useRef(null);
  const editorContainerRef = useRef(null);

  // Fetch the script list; if nothing selected yet, auto-select the first
  const load = useCallback(async (keepSelected) => {
    try {
      const s = await api("GET", "/api/scripts");
      setScripts(Array.isArray(s) ? s : []);
      if (!keepSelected && s.length > 0) {
        await selectScript(s[0].name);
      }
    } catch(e) { showToast(e.message, "error"); }
  }, []);

  // Fetch full script content and set as selected
  async function selectScript(name) {
    setLoadingContent(true);
    try {
      const full = await api("GET", `/api/scripts/${name}`);
      setSelectedScript(full);        // full = { name, content }
    } catch(e) { showToast(e.message, "error"); }
    setLoadingContent(false);
  }

  useEffect(() => { load(false); }, []);

  // (Re-)create Monaco editor whenever the selected script name changes
  useEffect(() => {
    if (!selectedScript || !editorContainerRef.current) return;
    if (monacoRef.current) { monacoRef.current.dispose(); monacoRef.current = null; }

    function initEditor() {
      monacoRef.current = window.monaco.editor.create(editorContainerRef.current, {
        value: selectedScript.content || '',
        language: 'python',
        theme: 'vs-dark',
        fontSize: 13,
        minimap: { enabled: false },
        scrollBeyondLastLine: false,
        automaticLayout: true,
      });
    }

    if (window.monaco) { initEditor(); }
    else {
      const script = document.createElement('script');
      script.src = 'https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.44.0/min/vs/loader.min.js';
      script.onload = () => {
        window.require.config({ paths: { vs: 'https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.44.0/min/vs' }});
        window.require(['vs/editor/editor.main'], initEditor);
      };
      document.head.appendChild(script);
    }

    return () => { if (monacoRef.current) { monacoRef.current.dispose(); monacoRef.current = null; } };
  }, [selectedScript?.name]);

  async function createScript(e) {
    e.preventDefault();
    if (!newName.trim()) { showToast("Script name required", "error"); return; }
    setCreating(true);
    try {
      await api("POST", "/api/scripts", { name: newName, content: `# ${newName}.py\n` });
      const created = newName;
      setNewName("");
      await load(true);
      await selectScript(created);
      showToast("Script created");
    } catch(e) { showToast(e.message, "error"); }
    setCreating(false);
  }

  async function saveScript() {
    if (!selectedScript || !monacoRef.current) return;
    try {
      const content = monacoRef.current.getValue();
      await api("PUT", `/api/scripts/${selectedScript.name}`, { content });
      // Update local state so content is current without re-fetching
      setSelectedScript(s => ({ ...s, content }));
      await load(true);
      showToast("Saved");
    } catch(e) { showToast(e.message, "error"); }
  }

  async function deleteScript() {
    if (!selectedScript) return;
    if (!window.confirm(`Delete "${selectedScript.name}"?`)) return;
    try {
      await api("DELETE", `/api/scripts/${selectedScript.name}`);
      showToast("Deleted");
      setSelectedScript(null);
      await load(false);
    } catch(e) { showToast(e.message, "error"); }
  }

  async function runScript() {
    if (!selectedScript) return;
    try {
      const res = await api("POST", `/api/scripts/${selectedScript.name}/run`);
      showToast(`Queued: task ${res.task_id}`);
    } catch(e) { showToast(e.message, "error"); }
  }

  return (
    <div style={{display:"flex",flexDirection:"column",height:"100vh"}}>
      <h1 className="page-title">Scripts</h1>
      <div style={{display:"flex",flex:1,overflow:"hidden",marginTop:-24,marginLeft:-32,marginRight:-32,marginBottom:-32}}>
        <div className="script-list">
          <div style={{padding:"10px 14px",borderBottom:"1px solid #2a2d3e"}}>
            <form onSubmit={createScript}>
              <input
                type="text"
                placeholder="New script name"
                value={newName}
                onChange={e => setNewName(e.target.value)}
                style={{width:"100%",marginBottom:8}}
              />
              <button type="submit" className="btn btn-primary" style={{width:"100%"}} disabled={creating}>
                {creating ? "Creating…" : "+ Create"}
              </button>
            </form>
          </div>
          <div style={{flex:1,overflowY:"auto"}}>
            {scripts.length === 0 && (
              <div style={{padding:"16px 14px",color:"#475569",fontSize:12}}>No scripts yet. Create one above.</div>
            )}
            {scripts.map(s => (
              <div
                key={s.name}
                className={`script-item${selectedScript?.name === s.name ? " active" : ""}`}
                onClick={() => selectScript(s.name)}
              >
                <div className="script-item-name">{s.name}</div>
                <div className="script-item-meta">{s.size} bytes · {new Date(s.modified * 1000).toLocaleDateString()}</div>
              </div>
            ))}
          </div>
        </div>
        <div className="editor-wrap">
          {loadingContent ? (
            <div className="empty-state" style={{alignSelf:"center",marginTop:"40%"}}>Loading…</div>
          ) : selectedScript ? (
            <>
              <div className="editor-toolbar">
                <span style={{color:"#64748b",fontSize:12,marginRight:"auto",fontFamily:"monospace"}}>{selectedScript.name}.py</span>
                <button className="btn btn-primary" onClick={saveScript}>💾 Save</button>
                <button className="btn btn-success" onClick={runScript}>▶ Run</button>
                <button className="btn btn-danger" onClick={deleteScript}>✕ Delete</button>
              </div>
              <div className="editor-container" ref={editorContainerRef}/>
            </>
          ) : (
            <div className="empty-state" style={{alignSelf:"center",marginTop:"40%"}}>Select a script or create a new one.</div>
          )}
        </div>
      </div>
    </div>
  );
}

// ── Credentials ────────────────────────────────────────────────────────────

// Structured field definitions per credential type
const CRED_SCHEMAS = {
  // ── generic ──────────────────────────────────────────────────────────────
  generic:    { label:"Generic Secret",          fields:[{k:"secret",  l:"Secret Value",      ph:"any secret value",                secret:true}] },

  // ── messaging / notifications ─────────────────────────────────────────────
  slack:      { label:"Slack Incoming Webhook",  fields:[{k:"webhook_url", l:"Webhook URL",   ph:"https://hooks.slack.com/services/T…/B…/…", secret:true}] },
  webhook:    { label:"Webhook URL",             fields:[{k:"url",         l:"Webhook URL",   ph:"https://discord.com/api/webhooks/…",        secret:true}] },
  telegram:   { label:"Telegram Bot",            fields:[{k:"secret",  l:"Bot Token",         ph:"123456:ABC…",                     secret:true},
                                                         {k:"chat_id", l:"Default Chat ID",   ph:"-100123456789"}] },

  // ── HTTP / API auth ───────────────────────────────────────────────────────
  api_key:    { label:"API Key / Bearer Token",  fields:[{k:"key",      l:"API Key / Token",  ph:"sk-… or eyJ…",                   secret:true},
                                                         {k:"header",   l:"Header name",      ph:"Authorization"}] },
  basic_auth: { label:"HTTP Basic Auth",         fields:[{k:"username", l:"Username",         ph:"admin"},
                                                         {k:"password", l:"Password",         ph:"••••••••",                        secret:true}] },
  openai_api: { label:"OpenAI / LLM API Key",   fields:[{k:"secret",   l:"API Key",          ph:"sk-…",                            secret:true},
                                                         {k:"base_url", l:"Base URL (optional)", ph:"https://api.openai.com/v1"}] },

  // ── email ─────────────────────────────────────────────────────────────────
  smtp:       { label:"SMTP Server",             fields:[{k:"host",    l:"Host",              ph:"smtp.gmail.com"},
                                                         {k:"port",    l:"Port",              ph:"465"},
                                                         {k:"user",    l:"Username",          ph:"you@gmail.com"},
                                                         {k:"pass",    l:"Password",          ph:"app-password",                    secret:true}] },

  // ── infrastructure ────────────────────────────────────────────────────────
  ssh:        { label:"SSH Server",              fields:[{k:"host",    l:"Host / IP",         ph:"192.168.1.1"},
                                                         {k:"port",    l:"Port",              ph:"22"},
                                                         {k:"username",l:"Username",          ph:"admin"},
                                                         {k:"password",l:"Password",          ph:"••••••••",                        secret:true},
                                                         {k:"key",     l:"Private Key (PEM, optional)", ph:"-----BEGIN RSA PRIVATE KEY-----…", textarea:true}] },
  sftp:       { label:"SFTP / FTP Server",       fields:[{k:"protocol",l:"Protocol",         ph:"sftp", select:["sftp","ftp"]},
                                                         {k:"host",    l:"Host / IP",         ph:"files.example.com"},
                                                         {k:"port",    l:"Port",              ph:"22 (sftp) · 21 (ftp)"},
                                                         {k:"username",l:"Username",          ph:"ftpuser"},
                                                         {k:"password",l:"Password",          ph:"••••••••",                        secret:true}] },
  aws:        { label:"AWS Credentials",         fields:[{k:"access_key_id",     l:"Access Key ID",     ph:"AKIAIOSFODNN7EXAMPLE"},
                                                         {k:"secret_access_key", l:"Secret Access Key", ph:"wJalrXUt…",            secret:true},
                                                         {k:"region",            l:"Default Region",    ph:"us-east-1"}] },
};


// Build blank sub-field state for a given type
function blankSubFields(type) {
  const schema = CRED_SCHEMAS[type] || CRED_SCHEMAS.generic;
  return Object.fromEntries(schema.fields.map(f => [f.k, '']));
}

// Serialize sub-fields to the `secret` string stored in DB
function encodeSecret(type, subFields) {
  const schema = CRED_SCHEMAS[type] || CRED_SCHEMAS.generic;
  if (schema.fields.length === 1 && schema.fields[0].k === 'secret') {
    return subFields.secret || '';
  }
  // Structured: JSON-encode all non-empty fields
  const obj = {};
  schema.fields.forEach(f => { if (subFields[f.k]) obj[f.k] = subFields[f.k]; });
  return JSON.stringify(obj);
}

// Human-readable summary of stored fields (for table display)
function credFieldSummary(type) {
  const schema = CRED_SCHEMAS[type] || CRED_SCHEMAS.generic;
  if (schema.fields.length === 1) return null;
  return schema.fields.filter(f=>!f.secret).map(f=>f.l).join(' · ');
}

function Credentials({ showToast }) {
  const [credentials, setCredentials] = useState([]);
  const [form, setForm] = useState({name:"", type:"generic", note:"", sub: blankSubFields("generic")});
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    try { setCredentials(await api("GET","/api/credentials")); }
    catch(e) { showToast(e.message,"error"); }
    setLoading(false);
  }, []);

  useEffect(()=>{ load(); },[]);

  const validateName = (name) => /^[a-zA-Z0-9_-]+$/.test(name);

  function changeType(t) {
    setForm(f => ({...f, type:t, sub: blankSubFields(t)}));
  }

  async function create(e) {
    e.preventDefault();
    if(!form.name.trim()) { showToast("Name required","error"); return }
    if(!validateName(form.name)) { showToast("Name may only contain letters, numbers, hyphens, underscores","error"); return }
    const secret = encodeSecret(form.type, form.sub);
    if(!secret.trim()) { showToast("At least one field is required","error"); return }
    try {
      await api("POST","/api/credentials",{name:form.name, type:form.type, secret, note:form.note});
      setForm({name:"", type:"generic", note:"", sub: blankSubFields("generic")});
      load();
      showToast("Credential created");
    } catch(err) { showToast(err.message,"error"); }
  }

  async function del(id) {
    if(!window.confirm("Delete this credential? This cannot be undone.")) return;
    try { await api("DELETE",`/api/credentials/${id}`); load(); showToast("Deleted"); }
    catch(e) { showToast(e.message,"error"); }
  }

  const schema = CRED_SCHEMAS[form.type] || CRED_SCHEMAS.generic;

  // Usage hint shown in the info box based on selected type
  const usageHints = {
    generic:    <>Use <code style={{color:"#a78bfa"}}>{"{{creds.name}}"}</code> to inject the secret value anywhere in a node config.</>,
    slack:      <>In the Slack node set <strong>Webhook URL</strong> to <code style={{color:"#a78bfa"}}>{"{{creds.name.webhook_url}}"}</code>. The URL is stored encrypted.</>,
    webhook:    <>Use <code style={{color:"#a78bfa"}}>{"{{creds.name.url}}"}</code> in any node field that accepts a URL (e.g. HTTP Request, Slack).</>,
    telegram:   <>Use <code style={{color:"#a78bfa"}}>{"{{creds.name}}"}</code> for the token, <code style={{color:"#a78bfa"}}>{"{{creds.name.chat_id}}"}</code> for the chat ID in Telegram nodes.</>,
    api_key:    <>In HTTP Request headers use <code style={{color:"#a78bfa"}}>{'{"Authorization":"Bearer {{creds.name.key}}"}'}</code>. <code style={{color:"#a78bfa"}}>{"{{creds.name.header}}"}</code> gives the header name if set.</>,
    basic_auth: <>In HTTP Request headers use <code style={{color:"#a78bfa"}}>{'{"Authorization":"Basic <base64>"}'}</code>, or reference <code style={{color:"#a78bfa"}}>{"{{creds.name.username}}"}</code> and <code style={{color:"#a78bfa"}}>{"{{creds.name.password}}"}</code> directly.</>,
    openai_api: <>Use <code style={{color:"#a78bfa"}}>{"{{creds.name}}"}</code> as the API key in LLM Call nodes. Set <code style={{color:"#a78bfa"}}>{"{{creds.name.base_url}}"}</code> as the API base for non-OpenAI providers.</>,
    smtp:       <>In Send Email nodes set <strong>SMTP Credential</strong> to <code style={{color:"#a78bfa"}}>name</code>. Fields are filled automatically. Or reference: <code style={{color:"#a78bfa"}}>{"{{creds.name.host}}"}</code>, <code style={{color:"#a78bfa"}}>{"{{creds.name.user}}"}</code>.</>,
    ssh:        <>In SSH nodes set <strong>SSH Credential</strong> to <code style={{color:"#a78bfa"}}>name</code>. Fields are filled automatically. Or reference: <code style={{color:"#a78bfa"}}>{"{{creds.name.host}}"}</code>, <code style={{color:"#a78bfa"}}>{"{{creds.name.username}}"}</code>.</>,
    sftp:       <>In SFTP nodes set <strong>SFTP Credential</strong> to <code style={{color:"#a78bfa"}}>name</code>. Fields are filled automatically. Or reference: <code style={{color:"#a78bfa"}}>{"{{creds.name.host}}"}</code>, <code style={{color:"#a78bfa"}}>{"{{creds.name.username}}"}</code>.</>,
    aws:        <>Reference fields directly: <code style={{color:"#a78bfa"}}>{"{{creds.name.access_key_id}}"}</code>, <code style={{color:"#a78bfa"}}>{"{{creds.name.secret_access_key}}"}</code>, <code style={{color:"#a78bfa"}}>{"{{creds.name.region}}"}</code>.</>,
  };

  return (
    <div>
      <h1 className="page-title">Credentials</h1>
      <div className="info-box">
        🔑 Secrets are never exposed in flow exports or logs.&nbsp;
        {usageHints[form.type] || usageHints.generic}
      </div>
      <div className="card">
        <div className="card-title">New Credential</div>
        <form onSubmit={create}>
          <div className="form-row">
            <div className="form-group">
              <label>Name <span style={{color:"#64748b",fontWeight:400}}>(unique · letters, numbers, hyphens, underscores)</span></label>
              <input required value={form.name} onChange={e=>setForm({...form,name:e.target.value})} placeholder="my-smtp-server"/>
            </div>
            <div className="form-group">
              <label>Type</label>
              <select value={form.type} onChange={e=>changeType(e.target.value)}>
                <optgroup label="Generic">
                  <option value="generic">Generic Secret</option>
                </optgroup>
                <optgroup label="Messaging &amp; Notifications">
                  <option value="slack">Slack Incoming Webhook</option>
                  <option value="webhook">Webhook URL (Discord, Teams…)</option>
                  <option value="telegram">Telegram Bot</option>
                </optgroup>
                <optgroup label="HTTP / API Auth">
                  <option value="api_key">API Key / Bearer Token</option>
                  <option value="basic_auth">HTTP Basic Auth</option>
                  <option value="openai_api">OpenAI / LLM API Key</option>
                </optgroup>
                <optgroup label="Email">
                  <option value="smtp">SMTP Server</option>
                </optgroup>
                <optgroup label="Infrastructure">
                  <option value="ssh">SSH Server</option>
                  <option value="sftp">SFTP / FTP Server</option>
                  <option value="aws">AWS Credentials</option>
                </optgroup>
              </select>
            </div>
          </div>
          {/* Type-specific fields */}
          <div style={{background:"#0f1117",border:"1px solid #1e2130",borderRadius:8,padding:"14px 16px",marginBottom:12}}>
            <div style={{fontSize:11,color:"#64748b",marginBottom:10,fontWeight:600,textTransform:"uppercase",letterSpacing:"0.05em"}}>{schema.label}</div>
            <div className="form-row" style={{flexWrap:"wrap"}}>
              {schema.fields.map(f => (
                <div key={f.k} className="form-group" style={{minWidth:f.textarea||f.k==="key"?"100%":"180px",flex:f.textarea||f.k==="key"?"1 1 100%":"1 1 180px"}}>
                  <label>{f.l}</label>
                  {f.select ? (
                    <select value={form.sub[f.k]||f.select[0]} onChange={e=>setForm({...form,sub:{...form.sub,[f.k]:e.target.value}})}>
                      {f.select.map(o=><option key={o} value={o}>{o}</option>)}
                    </select>
                  ) : f.textarea ? (
                    <textarea rows={3} value={form.sub[f.k]||""} onChange={e=>setForm({...form,sub:{...form.sub,[f.k]:e.target.value}})} placeholder={f.ph} style={{fontFamily:"monospace",fontSize:12}}/>
                  ) : (
                    <input type={f.secret?"password":"text"} value={form.sub[f.k]||""} onChange={e=>setForm({...form,sub:{...form.sub,[f.k]:e.target.value}})} placeholder={f.ph}/>
                  )}
                </div>
              ))}
            </div>
          </div>
          <div className="form-group">
            <label>Note <span style={{color:"#64748b",fontWeight:400}}>(optional)</span></label>
            <input value={form.note} onChange={e=>setForm({...form,note:e.target.value})} placeholder="e.g. Production server · rotates monthly"/>
          </div>
          <button type="submit" className="btn btn-primary">+ Create</button>
        </form>
      </div>
      <div className="card">
        <div className="card-title">Your Credentials</div>
        {loading ? <div className="empty-state">Loading…</div> : credentials.length === 0 ? (
          <div className="empty-state">No credentials yet. Create one above to get started.</div>
        ) : (
          <table>
            <thead><tr><th>Name</th><th>Type</th><th>Fields</th><th>Note</th><th>Created</th><th></th></tr></thead>
            <tbody>
              {credentials.map(c=>(
                <tr key={c.id}>
                  <td><code className="badge-credential" style={{background:"#0f1117",color:"#a78bfa"}}>{c.name}</code></td>
                  <td><span className={`badge-credential ${typeColors[c.type]||"generic"}`}>{c.type}</span></td>
                  <td style={{color:"#475569",fontSize:11}}>{credFieldSummary(c.type) || <span style={{color:"#334155"}}>••••••••</span>}</td>
                  <td style={{color:"#64748b",fontSize:12}}>{c.note||"—"}</td>
                  <td style={{color:"#64748b",fontSize:12}}>{new Date(c.created_at).toLocaleString()}</td>
                  <td><button className="btn btn-danger" onClick={()=>del(c.id)}>✕</button></td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

// ── Schedules ─────────────────────────────────────────────────────────────
function Schedules({ showToast }) {
  const [schedules, setSchedules] = useState([]);
  const [form, setForm] = useState({name:"",workflow:"",cron:"0 9 * * *",payload:"{}",timezone:"UTC"});

  const load = async () => { try { setSchedules(await api("GET","/api/schedules")); } catch(e){} };
  useEffect(()=>{ load(); },[]);

  async function create(e) {
    e.preventDefault();
    try {
      await api("POST","/api/schedules",{...form,payload:JSON.parse(form.payload||"{}")});
      setForm({name:"",workflow:"",cron:"0 9 * * *",payload:"{}",timezone:"UTC"});
      load(); showToast("Schedule created");
    } catch(err) { showToast(err.message,"error"); }
  }
  async function toggle(id) { try { await api("POST",`/api/schedules/${id}/toggle`); load(); } catch(e){ showToast(e.message,"error"); }}
  async function del(id)    { try { await api("DELETE",`/api/schedules/${id}`); load(); } catch(e){ showToast(e.message,"error"); }}

  return (
    <div>
      <h1 className="page-title">Schedules</h1>
      <div className="card">
        <div className="card-title">New Schedule</div>
        <form onSubmit={create}>
          <div className="form-row">
            <div className="form-group"><label>Name</label><input required value={form.name} onChange={e=>setForm({...form,name:e.target.value})} placeholder="Daily digest"/></div>
            <div className="form-group"><label>Workflow / Graph name</label><input required value={form.workflow} onChange={e=>setForm({...form,workflow:e.target.value})} placeholder="example"/></div>
          </div>
          <div className="form-row">
            <div className="form-group"><label>Cron</label><input required value={form.cron} onChange={e=>setForm({...form,cron:e.target.value})} placeholder="0 9 * * *"/></div>
            <div className="form-group"><label>Timezone</label><input value={form.timezone} onChange={e=>setForm({...form,timezone:e.target.value})} placeholder="UTC"/></div>
          </div>
          <div className="form-group"><label>Payload (JSON)</label><input value={form.payload} onChange={e=>setForm({...form,payload:e.target.value})} placeholder="{}"/></div>
          <button type="submit" className="btn btn-primary">+ Create</button>
        </form>
      </div>
      <div className="card">
        <div className="card-title">Scheduled Jobs</div>
        {schedules.length===0 ? <div className="empty-state">No schedules yet.</div> : (
          <table>
            <thead><tr><th>Name</th><th>Workflow</th><th>Cron</th><th>Timezone</th><th>Status</th><th></th></tr></thead>
            <tbody>
              {schedules.map(s=>(
                <tr key={s.id}>
                  <td>{s.name}</td>
                  <td style={{fontFamily:"monospace",fontSize:12}}>{s.workflow}</td>
                  <td style={{fontFamily:"monospace",fontSize:12}}>{s.cron}</td>
                  <td>{s.timezone}</td>
                  <td><span className={`badge ${s.enabled?"badge-succeeded":"badge-cancelled"}`}>{s.enabled?"active":"paused"}</span></td>
                  <td><div style={{display:"flex",gap:6}}>
                    <button className="btn btn-ghost" onClick={()=>toggle(s.id)}>{s.enabled?"Pause":"Resume"}</button>
                    <button className="btn btn-danger" onClick={()=>del(s.id)}>✕</button>
                  </div></td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

// ── Logs ──────────────────────────────────────────────────────────────────
function Logs({ showToast }) {
  const [runs, setRuns]           = useState([]);
  const [loading, setLoading]     = useState(true);
  const [selected, setSelected]   = useState(null);
  const [search, setSearch]       = useState("");
  const [statusFilter, setStatus] = useState("all");

  const load = async () => {
    try { setRuns(await api("GET","/api/runs")); }
    catch(e){ showToast(e.message,"error"); }
    setLoading(false);
  };
  useEffect(()=>{ load(); },[]);

  const STATUS_COLOR = {succeeded:"#4ade80",failed:"#f87171",running:"#34d399",queued:"#60a5fa",cancelled:"#9ca3af"};
  const TRACE_DOT    = {ok:"#4ade80",error:"#f87171",skipped:"#64748b"};

  const filtered = runs.filter(r => {
    if (statusFilter !== "all" && r.status !== statusFilter) return false;
    if (search) {
      const q = search.toLowerCase();
      return (r.flow_name||"").toLowerCase().includes(q) || String(r.id).includes(q);
    }
    return true;
  });

  function fmtTs(ts) {
    if (!ts) return "—";
    try { return new Date(ts).toLocaleString(); } catch { return ts; }
  }
  function fmtDur(ms) {
    if (!ms && ms !== 0) return "—";
    if (ms < 1000) return `${ms}ms`;
    return `${(ms/1000).toFixed(1)}s`;
  }

  const selRun = selected != null ? runs.find(r=>r.id===selected) : null;
  const traces = selRun?.traces || [];

  return (
    <div>
      <h1 className="page-title">Run Logs</h1>
      <div className="info-box" style={{marginBottom:16}}>
        Per-node execution traces for every run. Each entry shows status, duration, retries,
        and the input/output at each step. Data is stored in the database — there are no
        separate log files.
      </div>

      {/* Filter bar */}
      <div style={{display:"flex",gap:8,marginBottom:16,alignItems:"center"}}>
        <input placeholder="Search by flow name or run ID…" value={search}
          onChange={e=>setSearch(e.target.value)}
          style={{maxWidth:280,flex:"none"}}/>
        <select value={statusFilter} onChange={e=>setStatus(e.target.value)} style={{width:130,flex:"none"}}>
          <option value="all">All statuses</option>
          <option value="succeeded">Succeeded</option>
          <option value="failed">Failed</option>
          <option value="running">Running</option>
          <option value="queued">Queued</option>
          <option value="cancelled">Cancelled</option>
        </select>
        <button className="btn btn-ghost" onClick={load}>↻ Refresh</button>
        <span style={{marginLeft:"auto",fontSize:12,color:"#64748b"}}>{filtered.length} run{filtered.length!==1?"s":""}</span>
      </div>

      <div style={{display:"grid",gridTemplateColumns:"340px 1fr",gap:16,alignItems:"start"}}>
        {/* Run list */}
        <div style={{background:"#1a1d2e",border:"1px solid #2a2d3e",borderRadius:10,overflow:"hidden"}}>
          {loading && <div className="empty-state" style={{padding:"30px 0"}}>Loading…</div>}
          {!loading && filtered.length===0 && <div className="empty-state" style={{padding:"30px 0"}}>No runs match.</div>}
          {filtered.map(r=>(
            <div key={r.id} onClick={()=>setSelected(r.id===selected?null:r.id)}
              style={{padding:"10px 14px",borderBottom:"1px solid #1e2235",cursor:"pointer",
                background:selected===r.id?"#252840":"transparent",
                borderLeft:`3px solid ${selected===r.id?"#7c3aed":"transparent"}`,
                transition:"background .12s"}}>
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:3}}>
                <span style={{fontWeight:600,fontSize:13,color:"#e2e8f0"}}>
                  {r.flow_name || `run #${r.id}`}
                </span>
                <span style={{fontSize:11,fontWeight:600,color:STATUS_COLOR[r.status]||"#94a3b8"}}>
                  {r.status}
                </span>
              </div>
              <div style={{fontSize:11,color:"#64748b"}}>
                #{r.id} · {fmtTs(r.started_at)}
                {(r.traces||[]).length > 0 &&
                  <span style={{marginLeft:6}}>{r.traces.length} node{r.traces.length!==1?"s":""}</span>}
              </div>
            </div>
          ))}
        </div>

        {/* Trace detail */}
        <div>
          {!selRun
            ? <div className="empty-state" style={{paddingTop:60,background:"#1a1d2e",border:"1px solid #2a2d3e",borderRadius:10}}>
                ← Select a run to view its node traces
              </div>
            : <>
                <div className="card" style={{marginBottom:12}}>
                  <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start"}}>
                    <div>
                      <div style={{fontSize:15,fontWeight:700,color:"#e2e8f0",marginBottom:4}}>
                        {selRun.flow_name || `Run #${selRun.id}`}
                      </div>
                      <div style={{fontSize:12,color:"#64748b"}}>
                        Started: {fmtTs(selRun.started_at)}
                        {selRun.finished_at && <> · Finished: {fmtTs(selRun.finished_at)}</>}
                      </div>
                    </div>
                    <span style={{fontSize:12,fontWeight:700,padding:"4px 10px",borderRadius:20,
                      background:selRun.status==="succeeded"?"#14532d":selRun.status==="failed"?"#3f1111":"#1e3a5f",
                      color:STATUS_COLOR[selRun.status]||"#94a3b8"}}>
                      {selRun.status}
                    </span>
                  </div>
                  {selRun.error && <div style={{marginTop:10,padding:"8px 10px",background:"#3f1111",borderRadius:6,fontSize:12,color:"#f87171"}}>{selRun.error}</div>}
                </div>

                {traces.length===0
                  ? <div className="empty-state" style={{padding:"30px 0",background:"#1a1d2e",border:"1px solid #2a2d3e",borderRadius:10}}>No trace data for this run.</div>
                  : <div className="trace-table">
                      <table>
                        <thead>
                          <tr>
                            <th style={{width:18}}></th>
                            <th>Node</th>
                            <th>Type</th>
                            <th>Duration</th>
                            <th>Attempts</th>
                            <th>Input / Output</th>
                          </tr>
                        </thead>
                        <tbody>
                          {traces.map((t,i)=>(
                            <TraceRow key={i} t={t} fmtDur={fmtDur} dot={TRACE_DOT}/>
                          ))}
                        </tbody>
                      </table>
                    </div>
                }
              </>
          }
        </div>
      </div>
    </div>
  );
}

function TraceRow({ t, fmtDur, dot }) {
  const [open, setOpen] = useState(false);
  const hasDetail = t.input || t.output || t.error;
  return (
    <>
      <tr className="expandable-row" onClick={()=>hasDetail&&setOpen(o=>!o)}
        style={{cursor:hasDetail?"pointer":"default"}}>
        <td><span className="trace-status-dot" style={{background:dot[t.status]||"#64748b"}}/></td>
        <td style={{color:"#e2e8f0",fontWeight:500}}>{t.label||t.node_id}</td>
        <td style={{fontFamily:"monospace",fontSize:11,color:"#94a3b8"}}>{t.type}</td>
        <td style={{color:"#94a3b8"}}>{fmtDur(t.duration_ms)}</td>
        <td style={{color:"#94a3b8",textAlign:"center"}}>{t.attempts||1}</td>
        <td style={{color:t.status==="error"?"#f87171":"#64748b",fontSize:11}}>
          {t.status==="error" ? t.error : (hasDetail ? (open?"▲ hide":"▼ show") : "—")}
        </td>
      </tr>
      {open && hasDetail && (
        <tr>
          <td colSpan={6} style={{padding:"0 10px 10px"}}>
            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:8}}>
              {t.input && <div>
                <div style={{fontSize:10,color:"#64748b",marginBottom:4,textTransform:"uppercase",letterSpacing:".06em"}}>Input</div>
                <pre style={{background:"#0a0c14",border:"1px solid #2a2d3e",borderRadius:6,padding:"8px 10px",fontSize:11,color:"#94a3b8",whiteSpace:"pre-wrap",maxHeight:200,overflow:"auto",margin:0}}>
                  {typeof t.input==="string"?t.input:JSON.stringify(t.input,null,2)}
                </pre>
              </div>}
              {(t.output||t.error) && <div>
                <div style={{fontSize:10,color:"#64748b",marginBottom:4,textTransform:"uppercase",letterSpacing:".06em"}}>{t.error?"Error":"Output"}</div>
                <pre style={{background:"#0a0c14",border:`1px solid ${t.error?"#7f1d1d":"#2a2d3e"}`,borderRadius:6,padding:"8px 10px",fontSize:11,color:t.error?"#f87171":"#94a3b8",whiteSpace:"pre-wrap",maxHeight:200,overflow:"auto",margin:0}}>
                  {t.error || (typeof t.output==="string"?t.output:JSON.stringify(t.output,null,2))}
                </pre>
              </div>}
            </div>
          </td>
        </tr>
      )}
    </>
  );
}

// ── Settings ──────────────────────────────────────────────────────────────
function Settings({ showToast }) {
  const [resetting, setResetting] = useState(false);
  async function resetSeqs() {
    if (!window.confirm("Reset ID sequences? Only safe when all tables are empty.")) return;
    setResetting(true);
    try { await api("POST","/api/maintenance/reset_sequences"); showToast("Sequences reset"); }
    catch(e) { showToast(e.message,"error"); }
    setResetting(false);
  }
  return (
    <div>
      <h1 className="page-title">Settings</h1>
      <div className="card">
        <div className="card-title">Admin Token</div>
        <p style={{fontSize:13,color:"#94a3b8",marginBottom:12}}>Your admin token is passed as a URL query param (<code style={{color:"#a78bfa"}}>?token=...</code>) and as the <code style={{color:"#a78bfa"}}>x-admin-token</code> header.</p>
        <code style={{display:"block",padding:"10px 12px",background:"#0f1117",borderRadius:6,fontSize:13,color:"#a78bfa",userSelect:"all"}}>{TOKEN || "(no token set)"}</code>
      </div>
      <div className="card">
        <div className="card-title">Node Canvas</div>
        <p style={{fontSize:13,color:"#94a3b8",marginBottom:12}}>Open the visual workflow builder.</p>
        <button className="btn btn-primary" onClick={()=>window.open(`/canvas?token=${TOKEN}`)}>Open Canvas →</button>
      </div>
      <div className="card">
        <div className="card-title">External Links</div>
        <div style={{display:"flex",gap:10}}>
          <a href="/flower/" target="_blank" className="btn btn-ghost">Flower monitor ↗</a>
          <a href="/docs" target="_blank" className="btn btn-ghost">Swagger docs ↗</a>
        </div>
      </div>
      <div className="card">
        <div className="card-title">Maintenance</div>
        <p style={{fontSize:13,color:"#94a3b8",marginBottom:12}}>Reset PostgreSQL ID sequences. Only run this when all tables are empty.</p>
        <button className="btn btn-danger" disabled={resetting} onClick={resetSeqs}>{resetting?"Resetting…":"Reset sequences"}</button>
      </div>
    </div>
  );
}

// ── Graphs ────────────────────────────────────────────────────────────────
function Graphs({ showToast }) {
  const [graphs, setGraphs]     = useState([]);
  const [loading, setLoading]   = useState(true);
  const [running, setRunning]   = useState(null);
  const [search, setSearch]     = useState("");
  const [reseeding, setReseeding] = useState(false);

  const load = useCallback(async () => {
    try { setGraphs(await api("GET","/api/graphs")); }
    catch(e) { showToast(e.message,"error"); }
    setLoading(false);
  }, []);
  useEffect(()=>{ load(); },[]);

  async function reseedExamples() {
    if(!window.confirm("Re-seed all example flows? Missing examples will be restored; existing ones are left unchanged.")) return;
    setReseeding(true);
    try { await api("POST","/api/graphs/reseed"); showToast("Examples re-seeded"); load(); }
    catch(e) { showToast(e.message,"error"); }
    setReseeding(false);
  }

  async function renameGraph(g) {
    const newName = window.prompt("Rename flow:", g.name);
    if(!newName || newName.trim() === g.name) return;
    try { await api("PUT",`/api/graphs/${g.id}`,{name:newName.trim()}); showToast("Renamed"); load(); }
    catch(e) { showToast(e.message,"error"); }
  }

  async function runGraph(id, name) {
    setRunning(id);
    try { await api("POST",`/api/graphs/${id}/run`,{source:"admin"}); showToast(`Queued: ${name}`); }
    catch(e) { showToast(e.message,"error"); }
    setRunning(null);
  }

  async function toggleGraph(id, enabled) {
    try { await api("PUT",`/api/graphs/${id}`,{enabled:!enabled}); load(); showToast(enabled?"Disabled":"Enabled"); }
    catch(e) { showToast(e.message,"error"); }
  }

  async function duplicateGraph(g) {
    try {
      const full = await api("GET",`/api/graphs/${g.id}`);
      const baseName = full.name.replace(/^[📧🤖🔄⚠⚡🔍🧪📊🐍⏰💻📁]\s*/u,"");
      const copy = await api("POST","/api/graphs",{
        name: baseName + " (copy)",
        description: full.description || "",
        graph_data: full.graph_data || {}
      });
      showToast(`Duplicated as "${copy.name}"`); load();
    } catch(e) { showToast(e.message,"error"); }
  }

  async function deleteGraph(id, name) {
    if(!window.confirm(`Delete "${name}"? This cannot be undone.`)) return;
    try { await api("DELETE",`/api/graphs/${id}`); showToast("Deleted"); load(); }
    catch(e) { showToast(e.message,"error"); }
  }

  const EXAMPLE_RE = /^[📧🤖🔄⚠⚡🔍🧪📊🐍⏰💻📁]/u;
  const isExample = (name) => EXAMPLE_RE.test(name);

  const q = search.toLowerCase();
  const filtered = graphs.filter(g => !q || g.name.toLowerCase().includes(q) || (g.description||"").toLowerCase().includes(q));
  const exampleFlows = filtered.filter(g=>isExample(g.name));
  const userFlows    = filtered.filter(g=>!isExample(g.name));

  return (
    <div>
      <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:16}}>
        <h1 className="page-title" style={{marginBottom:0}}>Canvas Flows</h1>
        <div style={{display:"flex",gap:8}}>
          <button className="btn btn-ghost" disabled={reseeding} onClick={reseedExamples} title="Restore any missing example flows">
            {reseeding ? "Reseeding…" : "↺ Restore examples"}
          </button>
          <a href={`/canvas?token=${TOKEN}`} className="btn btn-primary">+ New in Canvas</a>
        </div>
      </div>

      <div style={{marginBottom:16}}>
        <input style={{width:"100%",background:"#1a1d2e",color:"#e2e8f0",border:"1px solid #2a2d3e",borderRadius:8,padding:"8px 12px",fontSize:13}}
          placeholder="🔍  Search flows…" value={search} onChange={e=>setSearch(e.target.value)}/>
      </div>

      {loading ? <div className="empty-state">Loading…</div> :
       graphs.length === 0 ? (
        <div className="card">
          <div className="empty-state" style={{padding:"40px 0"}}>
            No graphs yet.<br/>
            <a href={`/canvas?token=${TOKEN}`} style={{color:"#a78bfa",textDecoration:"none",marginTop:8,display:"inline-block"}}>Open the canvas to create your first flow →</a>
          </div>
        </div>
      ) : filtered.length === 0 ? (
        <div className="card"><div className="empty-state">No flows match "{search}".</div></div>
      ) : (
        <div>
          {/* ── example graphs ── */}
          {exampleFlows.length > 0 && (
            <div className="card" style={{marginBottom:16}}>
              <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:12}}>
                <div className="card-title" style={{marginBottom:0}}>Example Flows <span style={{fontSize:11,color:"#64748b",fontWeight:400}}>({exampleFlows.length})</span></div>
                <span style={{fontSize:11,color:"#64748b"}}>Duplicate to make your own copy</span>
              </div>
              {exampleFlows.map(g=>(
                <GraphRow key={g.id} g={g} running={running} onRun={runGraph} onToggle={toggleGraph} onDuplicate={duplicateGraph} onDelete={deleteGraph} onRename={renameGraph} showToast={showToast} load={load} isExample={true}/>
              ))}
            </div>
          )}
          {/* ── custom graphs ── */}
          {userFlows.length > 0 && (
            <div className="card">
              <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:12}}>
                <div className="card-title" style={{marginBottom:0}}>Your Flows <span style={{fontSize:11,color:"#64748b",fontWeight:400}}>({userFlows.length})</span></div>
              </div>
              {userFlows.map(g=>(
                <GraphRow key={g.id} g={g} running={running} onRun={runGraph} onToggle={toggleGraph} onDuplicate={duplicateGraph} onDelete={deleteGraph} onRename={renameGraph} showToast={showToast} load={load} isExample={false}/>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function GraphRow({ g, running, onRun, onToggle, onDuplicate, onDelete, onRename, showToast, load, isExample }) {
  const [open, setOpen] = useState(false);
  const [showVersions, setShowVersions] = useState(false);
  const [versions, setVersions] = useState([]);
  const [loadingVersions, setLoadingVersions] = useState(false);
  const nodeCount = ((g.graph_data?.nodes)||[]).filter(n=>n.type!=="note").length;

  async function openVersionHistory() {
    setLoadingVersions(true);
    try {
      const v = await api("GET",`/api/graphs/${g.id}/versions`);
      setVersions(v);
      setShowVersions(true);
    } catch(e) { showToast(e.message,"error"); }
    setLoadingVersions(false);
  }

  async function restoreVersion(vid) {
    if(!window.confirm("Restore this version? Current changes will be lost.")) return;
    try {
      await api("POST",`/api/graphs/${g.id}/versions/${vid}/restore`);
      showToast("Version restored");
      load();
      setShowVersions(false);
    } catch(e) { showToast(e.message,"error"); }
  }

  return (
    <>
      <div style={{borderBottom:"1px solid #1e2235",paddingBottom:12,marginBottom:12}}>
        <div style={{display:"flex",alignItems:"center",gap:10}}>
          <div style={{flex:1,minWidth:0}}>
            <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:3}}>
              <span style={{fontWeight:600,fontSize:14,color:"#e2e8f0"}}>{g.name}</span>
              <span className={`badge ${g.enabled?"badge-succeeded":"badge-cancelled"}`}>{g.enabled?"enabled":"disabled"}</span>
              <span style={{fontSize:11,color:"#4b5563"}}>#{g.id} · {nodeCount} node{nodeCount!==1?"s":""}</span>
            </div>
            {g.description && <div style={{fontSize:12,color:"#64748b"}}>{g.description}</div>}
          </div>
          <div style={{display:"flex",gap:6,flex:"none"}}>
            <a href={`/canvas?token=${TOKEN}`} className="btn btn-ghost"
               onClick={()=>sessionStorage.setItem("canvas_open_graph",g.id)}>✏️ Edit</a>
            <button className="btn btn-success" disabled={running===g.id||!g.enabled}
                    onClick={()=>onRun(g.id,g.name)}>{running===g.id?"…":"▶"}</button>
            <button className="btn btn-ghost" onClick={()=>setOpen(o=>!o)}>⋯</button>
          </div>
        </div>
        {open && (
          <div style={{display:"flex",gap:8,marginTop:10,paddingTop:10,borderTop:"1px solid #1e2235",flexWrap:"wrap"}}>
            <button className="btn btn-ghost" onClick={()=>onRename(g)}>✏ Rename</button>
            <button className="btn btn-ghost" onClick={()=>onDuplicate(g)}>📋 Duplicate</button>
            <button className="btn btn-ghost" onClick={openVersionHistory} disabled={loadingVersions}>📜 History</button>
            <button className="btn btn-ghost" onClick={()=>onToggle(g.id,g.enabled)}>{g.enabled?"⏸ Disable":"▶ Enable"}</button>
            <div style={{flex:1}}/>
            <div style={{fontSize:11,color:"#4b5563",alignSelf:"center"}}>
              Webhook: <code style={{color:"#6b7280"}}>{g.webhook_token?.slice(0,12)}…</code>
            </div>
            <button className="btn btn-danger" onClick={()=>onDelete(g.id,g.name)}
              title={isExample ? "⚠ This is an example flow — use ↺ Restore examples to get it back" : undefined}>
              🗑 Delete{isExample?" (example)":""}
            </button>
          </div>
        )}
      </div>

      {showVersions && (
        <div className="modal-overlay" onClick={e=>{ if(e.target.className==="modal-overlay") setShowVersions(false) }}>
          <div className="modal">
            <h2>📜 Version History</h2>
            {versions.length === 0 ? (
              <div className="empty-state" style={{padding:"20px 0"}}>No versions yet.</div>
            ) : (
              <div style={{marginTop:12}}>
                {versions.map((v,idx)=>(
                  <div key={idx} className="version-row">
                    <div className="version-info">
                      <div className="version-num">Version {v.version}</div>
                      <div className="version-date">{new Date(v.saved_at).toLocaleString()}</div>
                      {v.note && <div className="version-note">{v.note}</div>}
                    </div>
                    <button className="btn btn-ghost btn-sm" onClick={()=>restoreVersion(v.version)}>Restore</button>
                  </div>
                ))}
              </div>
            )}
            <div className="modal-btns">
              <button className="btn btn-ghost" onClick={()=>setShowVersions(false)}>Close</button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}

// ── Templates ─────────────────────────────────────────────────────────────
const CATEGORY_COLORS = {
  "Monitoring":   "#0891b2",
  "Integrations": "#7c3aed",
  "Productivity": "#059669",
  "Reporting":    "#d97706",
  "AI":           "#8b5cf6",
  "General":      "#475569",
};

function Templates({ showToast }) {
  const [templates, setTemplates] = useState([]);
  const [loading, setLoading]     = useState(true);
  const [importing, setImporting] = useState(null);

  useEffect(() => {
    api("GET", "/api/templates")
      .then(setTemplates)
      .catch(e => showToast(e.message, "error"))
      .finally(() => setLoading(false));
  }, []);

  async function useTemplate(t) {
    setImporting(t.id);
    try {
      const g = await api("POST", `/api/templates/${t.id}/use`, {});
      showToast(`Flow "${g.name}" created! Open it in Canvas Flows.`);
    } catch(e) {
      showToast("Failed: " + e.message, "error");
    }
    setImporting(null);
  }

  // Group by category
  const groups = {};
  templates.forEach(t => {
    if (!groups[t.category]) groups[t.category] = [];
    groups[t.category].push(t);
  });

  return (
    <div className="page">
      <div style={{display:"flex",alignItems:"flex-start",justifyContent:"space-between",marginBottom:24}}>
        <div>
          <h1 className="page-title" style={{marginBottom:6}}>📐 Workflow Templates</h1>
          <div style={{fontSize:13,color:"#64748b"}}>Start from a pre-built template — click <strong style={{color:"#94a3b8"}}>Use template</strong> to create a flow, then edit it in Canvas Flows.</div>
        </div>
      </div>
      {loading && <div style={{color:"#64748b",padding:24}}>Loading templates…</div>}
      {!loading && templates.length === 0 && (
        <div style={{color:"#64748b",padding:24}}>No templates found. Add JSON files to <code>app/templates/</code>.</div>
      )}
      {Object.entries(groups).map(([category, items]) => (
        <div key={category} style={{marginBottom:32}}>
          <div style={{fontSize:11,fontWeight:600,color:(CATEGORY_COLORS[category]||"#94a3b8"),
            textTransform:"uppercase",letterSpacing:".08em",marginBottom:12,paddingBottom:6,
            borderBottom:`1px solid ${(CATEGORY_COLORS[category]||"#2a2d3e")}44`}}>
            {category}
          </div>
          <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(300px,1fr))",gap:14}}>
            {items.map(t => (
              <div key={t.id} style={{background:"#181b2e",border:"1px solid #2a2d3e",borderRadius:10,
                padding:"16px 18px",display:"flex",flexDirection:"column",gap:8,
                transition:"border-color .15s",cursor:"default"}}
                onMouseEnter={e=>e.currentTarget.style.borderColor=(CATEGORY_COLORS[t.category]||"#7c3aed")}
                onMouseLeave={e=>e.currentTarget.style.borderColor="#2a2d3e"}>
                <div style={{display:"flex",alignItems:"flex-start",justifyContent:"space-between",gap:8}}>
                  <div style={{fontWeight:600,color:"#e2e8f0",fontSize:14}}>{t.name}</div>
                  <span style={{fontSize:10,color:(CATEGORY_COLORS[t.category]||"#475569"),
                    background:(CATEGORY_COLORS[t.category]||"#475569")+"22",
                    borderRadius:4,padding:"2px 7px",whiteSpace:"nowrap"}}>{t.category}</span>
                </div>
                <div style={{fontSize:12,color:"#94a3b8",lineHeight:1.6}}>{t.description}</div>
                <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",marginTop:4}}>
                  <div style={{display:"flex",gap:6,flexWrap:"wrap"}}>
                    {(t.tags||[]).map(tag=>(
                      <span key={tag} style={{fontSize:10,color:"#64748b",background:"#0f1117",
                        borderRadius:3,padding:"1px 6px",border:"1px solid #2a2d3e"}}>{tag}</span>
                    ))}
                    <span style={{fontSize:10,color:"#4b5563"}}>
                      {t.node_count} node{t.node_count!==1?"s":""}
                    </span>
                  </div>
                  <button
                    className="btn btn-primary btn-sm"
                    disabled={importing === t.id}
                    onClick={()=>useTemplate(t)}
                    style={{minWidth:80}}>
                    {importing===t.id ? "Importing…" : "Use template"}
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

// ── App shell ─────────────────────────────────────────────────────────────
const PAGES = [
  {id:"dashboard",  icon:"⬛", label:"Dashboard"},
  {id:"graphs",     icon:"🎨", label:"Canvas Flows"},
  {id:"templates",  icon:"📐", label:"Templates"},
  {id:"metrics",    icon:"📊", label:"Metrics"},
  {id:"scripts",    icon:"🐍", label:"Scripts"},
  {id:"credentials",icon:"🔑", label:"Credentials"},
  {id:"schedules",  icon:"⏰", label:"Schedules"},
  {id:"logs",       icon:"📋", label:"Logs"},
  {id:"settings",   icon:"⚙",  label:"Settings"},
];

function App() {
  const validIds = PAGES.map(p=>p.id);
  const hashPage = () => {
    const h = window.location.hash.slice(1);
    return validIds.includes(h) ? h : "dashboard";
  };
  const [page, setPageState] = useState(hashPage);
  const [toast, setToast]    = useState(null);
  const showToast = useCallback((msg, type="success") => setToast({msg,type,key:Date.now()}), []);

  // Keep hash in sync with page state
  const setPage = useCallback((id) => {
    window.location.hash = id;
    setPageState(id);
  }, []);

  // Handle browser back/forward
  useEffect(() => {
    const onHashChange = () => setPageState(hashPage());
    window.addEventListener("hashchange", onHashChange);
    return () => window.removeEventListener("hashchange", onHashChange);
  }, []);

  return (
    <>
      <div className="sidebar">
        <div className="logo">⚡ HiveRunr</div>
        {PAGES.map(p=>(
          <div key={p.id} className={`nav-item${page===p.id?" active":""}`} onClick={()=>setPage(p.id)}>
            <span className="nav-icon">{p.icon}</span>{p.label}
          </div>
        ))}
        <div className="sidebar-footer">
          <div className="footer-label">Quick links</div>
          <a className="ext-link" href={`/canvas?token=${TOKEN}`}>
            <span className="el-icon">🎨</span>Node Canvas
          </a>
          <a className="ext-link" href="/flower/" target="_blank" rel="noopener">
            <span className="el-icon">🌸</span>Flower / Celery
          </a>
          <a className="ext-link" href="/docs" target="_blank" rel="noopener">
            <span className="el-icon">📖</span>API Docs
          </a>
          <a className="ext-link" href="/health" target="_blank" rel="noopener">
            <span className="el-icon">💚</span>Health check
          </a>
        </div>
      </div>
      <div className="main">
        {page==="dashboard"  && <Dashboard  showToast={showToast}/>}
        {page==="graphs"     && <Graphs     showToast={showToast}/>}
        {page==="templates"  && <Templates  showToast={showToast}/>}
        {page==="metrics"    && <Metrics    showToast={showToast}/>}
        {page==="scripts"    && <Scripts    showToast={showToast}/>}
        {page==="credentials"&& <Credentials showToast={showToast}/>}
        {page==="schedules"  && <Schedules  showToast={showToast}/>}
        {page==="logs"       && <Logs       showToast={showToast}/>}
        {page==="settings"   && <Settings   showToast={showToast}/>}
      </div>
      {toast && <Toast key={toast.key} msg={toast.msg} type={toast.type} onDone={()=>setToast(null)}/>}
    </>
  );
}

const root = ReactDOM.createRoot(document.getElementById("root"));
root.render(<App/>);
</script>
</body>
</head>


HTML_EOF

# ── 18. app/static/canvas.html ─────────────────────────────────────────────
cat > "$PROJECT/app/static/canvas.html" << 'CANVAS_EOF'
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>HiveRunr — Canvas</title>
<script src="https://unpkg.com/react@18/umd/react.production.min.js"></script>
<script src="https://unpkg.com/react-dom@18/umd/react-dom.production.min.js"></script>
<script src="https://unpkg.com/@babel/standalone/babel.min.js"></script>
<script src="https://unpkg.com/reactflow@11/dist/umd/index.js"></script>
<link rel="stylesheet" href="https://unpkg.com/reactflow@11/dist/style.css"/>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Inter',system-ui,sans-serif;background:#0f1117;color:#e2e8f0;height:100vh;overflow:hidden}
#root{height:100vh;display:flex;flex-direction:column}

/* ── top bar ── */
.topbar{height:52px;background:#1a1d2e;border-bottom:1px solid #2a2d3e;display:flex;align-items:center;gap:8px;padding:0 14px;flex-shrink:0;z-index:10}
.topbar .logo{font-weight:700;font-size:15px;color:#a78bfa;margin-right:4px;white-space:nowrap}
.topbar input.name-input{background:#0f1117;color:#e2e8f0;border:1px solid #2a2d3e;border-radius:6px;padding:5px 10px;font-size:13px;width:180px}
.topbar input.name-input:focus{outline:none;border-color:#7c3aed}
.topbar-spacer{flex:1}
.btn{display:inline-flex;align-items:center;gap:5px;padding:6px 12px;border-radius:6px;font-size:12px;font-weight:500;border:none;cursor:pointer;transition:all .15s;white-space:nowrap}
.btn-primary{background:#7c3aed;color:#fff}.btn-primary:hover{background:#6d28d9}
.btn-success{background:#059669;color:#fff}.btn-success:hover{background:#047857}
.btn-danger{background:#dc2626;color:#fff}.btn-danger:hover{background:#b91c1c}
.btn-ghost{background:#2a2d3e;color:#94a3b8}.btn-ghost:hover{background:#374151;color:#e2e8f0}
.btn-sm{padding:4px 9px;font-size:11px}
.btn:disabled{opacity:.5;cursor:not-allowed}
.tb-divider{width:1px;height:20px;background:#2a2d3e;margin:0 2px}
/* ── topbar "More" dropdown ── */
.tb-more-wrap{position:relative}
.tb-more-menu{position:absolute;top:calc(100% + 6px);left:0;background:#1a1d2e;border:1px solid #2a2d3e;border-radius:8px;min-width:160px;z-index:999;box-shadow:0 8px 24px rgba(0,0,0,.5);padding:4px 0;display:flex;flex-direction:column}
.tb-more-item{display:flex;align-items:center;gap:8px;padding:7px 14px;font-size:12px;color:#94a3b8;cursor:pointer;background:none;border:none;width:100%;text-align:left;transition:background .1s,color .1s}
.tb-more-item:hover{background:#2a2d3e;color:#e2e8f0}
.tb-more-item.danger:hover{background:#7f1d1d22;color:#f87171}
.tb-more-sep{height:1px;background:#2a2d3e;margin:3px 0}

/* ── error banner ── */
.error-banner{background:#1f0a0a;border-bottom:1px solid #7f1d1d;padding:8px 16px;display:flex;align-items:center;gap:10px;flex-shrink:0;z-index:9}
.eb-icon{font-size:16px;color:#f87171;flex-shrink:0}
.eb-body{flex:1;min-width:0}
.eb-title{font-size:12px;font-weight:600;color:#f87171}
.eb-msg{font-size:11px;color:#fca5a5;margin-top:1px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.eb-close{background:none;border:none;color:#64748b;cursor:pointer;font-size:16px;padding:0 4px;flex-shrink:0}.eb-close:hover{color:#e2e8f0}

/* ── layout ── */
.canvas-layout{flex:1;display:flex;overflow:hidden}
.sidebar{width:210px;background:#1a1d2e;border-right:1px solid #2a2d3e;display:flex;flex-direction:column;flex-shrink:0}
.sidebar-search{padding:8px 10px;border-bottom:1px solid #2a2d3e}
.sidebar-search input{width:100%;background:#0f1117;color:#e2e8f0;border:1px solid #2a2d3e;border-radius:6px;padding:5px 9px;font-size:12px;font-family:inherit}
.sidebar-search input:focus{outline:none;border-color:#7c3aed}
.sidebar-scroll{flex:1;overflow-y:auto}
.sidebar-scroll::-webkit-scrollbar{width:3px}
.sidebar-scroll::-webkit-scrollbar-thumb{background:#2a2d3e;border-radius:3px}
.sidebar-title{font-size:10px;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:.08em;padding:10px 12px 5px}
.node-palette-item{display:flex;align-items:center;gap:9px;padding:7px 12px;cursor:grab;border-radius:5px;margin:1px 5px;transition:background .12s;user-select:none}
.node-palette-item:hover{background:#2a2d3e}
.node-palette-item:active{cursor:grabbing}
.node-icon{width:26px;height:26px;border-radius:5px;display:flex;align-items:center;justify-content:center;font-size:13px;flex-shrink:0}
.node-label{font-size:11px;color:#cbd5e1;font-weight:500;line-height:1.2}
.node-sublabel{font-size:10px;color:#64748b;line-height:1.2}

/* ── config panel ── */
.config-panel{width:290px;background:#1a1d2e;border-left:1px solid #2a2d3e;display:flex;flex-direction:column;flex-shrink:0}
.config-panel .panel-header{padding:12px 14px;border-bottom:1px solid #2a2d3e;display:flex;align-items:center;justify-content:space-between;gap:8px}
.config-panel .panel-header h3{font-size:13px;font-weight:600;color:#e2e8f0;flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.config-panel .panel-body{flex:1;overflow-y:auto;padding:12px 14px}
.config-panel .panel-body::-webkit-scrollbar{width:4px}
.config-panel .panel-body::-webkit-scrollbar-track{background:#0f1117}
.config-panel .panel-body::-webkit-scrollbar-thumb{background:#2a2d3e;border-radius:4px}
.config-empty{color:#64748b;font-size:12px;text-align:center;margin-top:50px;line-height:1.7}
.field-group{margin-bottom:12px}
.field-label{font-size:11px;color:#94a3b8;font-weight:500;margin-bottom:4px;display:flex;align-items:center;justify-content:space-between}
.field-hint{font-size:10px;color:#64748b;font-weight:400;margin-top:3px;display:block}
.secret-hint{font-size:10px;color:#a78bfa;margin-top:3px;display:block}
.field-input{width:100%;background:#0f1117;color:#e2e8f0;border:1px solid #2a2d3e;border-radius:6px;padding:6px 9px;font-size:12px;font-family:inherit;resize:vertical}
.field-input:focus{outline:none;border-color:#7c3aed}
.field-input.mono{font-family:'JetBrains Mono','Fira Code',monospace;font-size:11px}
textarea.field-input{min-height:70px}
select.field-input{appearance:none;cursor:pointer;background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 24 24' fill='none' stroke='%2364748b' stroke-width='2'%3E%3Cpath d='M6 9l6 6 6-6'/%3E%3C/svg%3E");background-repeat:no-repeat;background-position:right 9px center;padding-right:28px}
.node-id-badge{display:inline-flex;align-items:center;gap:6px;background:#0f1117;border:1px solid #2a2d3e;border-radius:6px;padding:5px 9px;margin-bottom:12px;cursor:pointer;width:100%;justify-content:space-between}
.node-id-badge:hover{border-color:#7c3aed}
.node-id-badge code{font-family:'JetBrains Mono','Fira Code',monospace;font-size:11px;color:#a78bfa}
.node-id-badge .copy-hint{font-size:10px;color:#64748b}
.section-divider{font-size:10px;color:#64748b;font-weight:600;text-transform:uppercase;letter-spacing:.06em;margin:14px 0 8px;padding-bottom:5px;border-bottom:1px solid #2a2d3e}
.delete-node-btn{width:100%;margin-top:10px;padding:7px;background:#1f1f2e;border:1px solid #dc2626;color:#dc2626;border-radius:6px;font-size:12px;cursor:pointer;transition:all .15s}
.delete-node-btn:hover{background:#dc2626;color:#fff}
.disable-toggle-row{display:flex;align-items:center;justify-content:space-between;background:#0f1117;border:1px solid #2a2d3e;border-radius:6px;padding:8px 10px;margin-bottom:12px}
.disable-toggle-label{font-size:12px;color:#94a3b8;font-weight:500}
.toggle-switch{position:relative;width:36px;height:20px;cursor:pointer;flex-shrink:0}
.toggle-switch input{opacity:0;width:0;height:0}
.toggle-track{position:absolute;inset:0;background:#374151;border-radius:20px;transition:background .2s}
.toggle-thumb{position:absolute;top:3px;left:3px;width:14px;height:14px;background:#fff;border-radius:50%;transition:transform .2s}
.toggle-switch input:checked+.toggle-track{background:#059669}
.toggle-switch input:checked~.toggle-thumb,.toggle-switch input:checked+.toggle-track+.toggle-thumb{transform:translateX(16px)}

/* run output panel */
.run-output-panel{background:#0a0c14;border:1px solid #2a2d3e;border-radius:6px;padding:8px 10px;margin-top:10px}
.run-output-title{font-size:10px;color:#64748b;font-weight:600;text-transform:uppercase;letter-spacing:.05em;margin-bottom:6px;display:flex;align-items:center;gap:6px}
.run-output-status{display:inline-flex;align-items:center;gap:4px;font-size:10px;font-weight:600;padding:2px 7px;border-radius:10px}
.run-status-ok{background:#14532d;color:#4ade80}
.run-status-err{background:#3f1111;color:#f87171}
.run-status-skip{background:#1e293b;color:#94a3b8}
.run-output-body{font-size:11px;font-family:'JetBrains Mono','Fira Code',monospace;color:#94a3b8;white-space:pre-wrap;max-height:140px;overflow-y:auto;line-height:1.5}

/* ── Node I/O Panel ── */
.nio-panel{margin-top:12px;border:1px solid #2a2d3e;border-radius:8px;overflow:hidden;background:#0f1117}
.nio-header{display:flex;align-items:center;justify-content:space-between;padding:6px 10px;background:#13161f;border-bottom:1px solid #2a2d3e}
.nio-side-tabs{display:flex;gap:3px}
.nio-tab{font-size:10px;font-weight:700;letter-spacing:.04em;padding:3px 10px;border-radius:4px;border:1px solid #2a2d3e;background:transparent;color:#64748b;cursor:pointer;transition:all .15s}
.nio-tab:hover{color:#94a3b8;border-color:#3b4255}
.nio-tab-active{background:#7c3aed22 !important;color:#a78bfa !important;border-color:#7c3aed !important}
.nio-view-tabs{display:flex;border-bottom:1px solid #1e2130;background:#0c0e14}
.nio-view-tab{flex:1;font-size:10px;font-weight:500;padding:5px 0;border:none;background:transparent;color:#475569;cursor:pointer;border-bottom:2px solid transparent;transition:all .15s}
.nio-view-tab:hover{color:#94a3b8}
.nio-view-tab.active{color:#a78bfa;border-bottom-color:#7c3aed}
.nio-content{padding:8px 10px;max-height:260px;overflow-y:auto}
.nio-json{font-size:10px;font-family:'JetBrains Mono','Fira Code',monospace;color:#94a3b8;white-space:pre-wrap;word-break:break-all;margin:0;line-height:1.6}
.nio-tree-key{color:#94a3b8;font-size:10px}
.nio-tree-str{color:#4ade80;word-break:break-all}
.nio-tree-num{color:#60a5fa}
.nio-tree-bool{color:#f59e0b}
.nio-tree-null{color:#64748b;font-style:italic}
.nio-tree-expand{cursor:pointer;color:#a78bfa;user-select:none;font-size:10px}
.nio-tree-indent{margin-left:14px;border-left:1px solid #2a2d3e;padding-left:8px}
/* drag rows */
.nio-drag-row{display:flex;align-items:flex-start;gap:3px;border-radius:3px;padding:1px 2px;transition:background .1s}
.nio-drag-row:hover{background:#1a1d2e}
.nio-drag-row:hover .nio-drag-handle{opacity:1}
.nio-drag-handle{opacity:0;cursor:grab;color:#64748b;font-size:9px;user-select:none;flex-shrink:0;margin-top:3px;padding:0 1px;transition:opacity .1s,color .1s;line-height:1}
.nio-drag-handle:hover{color:#a78bfa}
.nio-drag-handle:active{cursor:grabbing;color:#a78bfa}
/* expand modal */
.nio-expand-btn{background:none;border:1px solid #2a2d3e;border-radius:4px;color:#64748b;cursor:pointer;font-size:10px;padding:2px 6px;transition:all .15s}
.nio-expand-btn:hover{color:#a78bfa;border-color:#7c3aed}
.nio-modal-overlay{position:fixed;inset:0;background:#000000cc;z-index:9999;display:flex;align-items:center;justify-content:center}
.nio-modal{background:#13161f;border:1px solid #3b4255;border-radius:12px;width:min(720px,94vw);max-height:88vh;display:flex;flex-direction:column;overflow:hidden;box-shadow:0 24px 64px #000}
.nio-modal-hdr{display:flex;align-items:center;justify-content:space-between;padding:12px 16px;border-bottom:1px solid #2a2d3e;gap:12px}
.nio-modal-title{font-size:12px;font-weight:600;color:#e2e8f0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.nio-modal-close{background:none;border:none;color:#64748b;font-size:18px;cursor:pointer;line-height:1;padding:0 4px}
.nio-modal-close:hover{color:#e2e8f0}
.nio-modal-body{flex:1;overflow:hidden;display:flex;flex-direction:column}
.nio-modal-body .nio-content{max-height:none;flex:1;overflow-y:auto;padding:12px 16px}
/* drop targets */
.field-input.drag-over,.field-input.drag-over:focus{border-color:#7c3aed !important;background:#7c3aed14 !important;box-shadow:0 0 0 2px #7c3aed33 !important}

/* ── Node Editor Modal (n8n-style 3-column) ── */
.nem-overlay{position:fixed;inset:0;z-index:800;display:flex;pointer-events:none}
.nem-backdrop{flex:1;pointer-events:all;cursor:default}
.nem-panel{width:min(940px,94vw);background:#0f1117;border-left:1px solid #2a2d3e;display:flex;flex-direction:column;pointer-events:all;box-shadow:-12px 0 40px #0009;overflow:hidden}
.nem-hdr{display:flex;align-items:center;gap:10px;padding:10px 16px;border-bottom:1px solid #2a2d3e;background:#13161f;flex-shrink:0}
.nem-hdr-icon{font-size:20px;flex-shrink:0}
.nem-hdr-title{flex:1;font-size:13px;font-weight:600;color:#e2e8f0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.nem-hdr-close{background:none;border:none;color:#64748b;font-size:20px;cursor:pointer;line-height:1;padding:0 4px;flex-shrink:0}
.nem-hdr-close:hover{color:#e2e8f0}
.nem-columns{flex:1;display:flex;overflow:hidden;min-height:0}
.nem-io-col{width:240px;flex-shrink:0;display:flex;flex-direction:column;overflow:hidden;background:#0c0e14}
.nem-io-col.left{border-right:1px solid #2a2d3e}
.nem-io-col.right{border-left:1px solid #2a2d3e}
.nem-io-col-hdr{font-size:9px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:#64748b;padding:7px 12px;border-bottom:1px solid #1e2130;display:flex;align-items:center;gap:8px;flex-shrink:0}
.nem-drag-hint{font-size:9px;color:#475569;font-style:italic;font-weight:400;text-transform:none;letter-spacing:0}
.nem-io-view-tabs{display:flex;border-bottom:1px solid #1e2130;flex-shrink:0}
.nem-io-view-tab{flex:1;font-size:10px;font-weight:500;padding:5px 0;border:none;background:transparent;color:#475569;cursor:pointer;border-bottom:2px solid transparent;transition:all .15s}
.nem-io-view-tab:hover{color:#94a3b8}
.nem-io-view-tab.active{color:#a78bfa;border-bottom-color:#7c3aed}
.nem-io-col-body{flex:1;overflow-y:auto;padding:8px 10px;font-size:11px;line-height:1.8}
.nem-cfg-col{flex:1;overflow-y:auto;padding:16px 20px;min-width:0;border-right:1px solid #2a2d3e}

/* ── Variable autocomplete ── */
.var-wrap{position:relative}
.var-dropdown{position:absolute;top:100%;left:0;right:0;z-index:9999;background:#1a1d2e;border:1px solid #7c3aed;border-radius:6px;box-shadow:0 8px 24px #0008;max-height:220px;overflow-y:auto;margin-top:2px}
.var-dropdown-item{display:flex;align-items:center;justify-content:space-between;padding:6px 10px;cursor:pointer;gap:8px;border-bottom:1px solid #1e2130;transition:background .1s}
.var-dropdown-item:last-child{border-bottom:none}
.var-dropdown-item:hover,.var-dropdown-item.active{background:#2a2060}
.var-dropdown-label{font-size:11px;color:#e2e8f0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;flex:1}
.var-dropdown-node{font-size:9px;color:#7c3aed;font-weight:600;white-space:nowrap;flex-shrink:0}
.var-dropdown-tmpl{font-size:9px;color:#64748b;white-space:nowrap;font-family:monospace;flex-shrink:0}
.var-dropdown-section{font-size:9px;color:#475569;font-weight:700;letter-spacing:.08em;text-transform:uppercase;padding:5px 10px 2px;background:#0f1117;position:sticky;top:0}
.var-trigger-hint{font-size:9px;color:#475569;position:absolute;right:8px;top:50%;transform:translateY(-50%);pointer-events:none;font-style:italic}

/* ── flow canvas ── */
.flow-wrap{flex:1;position:relative;background:#0f1117}
.reactflow-attribution{display:none}
.react-flow__background{background:#0f1117}
.react-flow__controls{background:#1a1d2e;border:1px solid #2a2d3e;border-radius:8px;overflow:hidden}
.react-flow__controls-button{background:#1a1d2e;border:none;color:#94a3b8;border-bottom:1px solid #2a2d3e}
.react-flow__controls-button:hover{background:#2a2d3e;color:#e2e8f0}
.react-flow__minimap{background:#1a1d2e;border:1px solid #2a2d3e;border-radius:8px}
.react-flow__edge-path{stroke:#4b5563;stroke-width:2}
.react-flow__edge.selected .react-flow__edge-path{stroke:#7c3aed}
.react-flow__edge-text{fill:#94a3b8;font-size:11px}
.react-flow__edge-textbg{fill:#1a1d2e}
.react-flow__handle{width:10px;height:10px;border-radius:50%;border:2px solid #1a1d2e}
.react-flow__handle-left{background:#4b5563}
.react-flow__handle-right{background:#7c3aed}
.react-flow__handle-bottom{background:#64748b}
.edge-label-input{background:#1a1d2e;border:1px solid #7c3aed;border-radius:4px;color:#e2e8f0;font-size:11px;padding:3px 7px;outline:none;width:130px}

/* ── custom nodes ── */
.custom-node{background:#1a1d2e;border:1.5px solid #2a2d3e;border-radius:10px;min-width:170px;box-shadow:0 4px 12px rgba(0,0,0,.4);transition:border-color .15s,opacity .2s}
.custom-node:hover,.custom-node.selected{border-color:#7c3aed;box-shadow:0 0 0 2px rgba(124,58,237,.2)}
.custom-node.disabled-node{opacity:.45}
.custom-node.node-running{border-color:#7c3aed;animation:node-run-pulse 1.4s ease-in-out infinite}
.custom-node .node-header{display:flex;align-items:center;gap:7px;padding:9px 10px 7px;border-bottom:1px solid #2a2d3e;border-radius:10px 10px 0 0}
.custom-node .node-header .nh-icon{width:22px;height:22px;border-radius:5px;display:flex;align-items:center;justify-content:center;font-size:12px;flex-shrink:0}
.custom-node .node-header .nh-title{font-size:11px;font-weight:600;color:#e2e8f0;flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.custom-node .node-header .nh-type{font-size:9px;color:#64748b;white-space:nowrap}
.custom-node .node-body{padding:5px 10px;font-size:10px;color:#94a3b8;min-height:22px}
.custom-node .node-footer{display:flex;align-items:center;justify-content:space-between;padding:3px 10px 6px}
.custom-node .node-id{font-size:9px;color:#4b5563;font-family:monospace}
.node-status-badge{display:flex;align-items:center;gap:3px;font-size:9px;font-weight:600;border-radius:4px;padding:1px 5px;letter-spacing:.02em}
.node-status-badge.ok{color:#4ade80;background:#4ade8012}
.node-status-badge.err{color:#f87171;background:#f8717112}
.node-status-badge.skip{color:#64748b;background:#64748b12}
.node-status-badge.pending{color:#a78bfa;background:#7c3aed14}
.node-off-chip{background:#374151;color:#94a3b8;font-size:9px;font-weight:700;padding:1px 5px;border-radius:4px;letter-spacing:.04em}

/* ── sticky note node ── */
.note-node{background:#854d0e22;border:1.5px solid #854d0e;border-radius:8px;min-width:160px;max-width:240px;box-shadow:0 4px 12px rgba(0,0,0,.3)}
.note-node.selected{border-color:#ca8a04;box-shadow:0 0 0 2px rgba(202,138,4,.2)}
.note-node .note-body{padding:10px 12px;font-size:11px;color:#fde68a;line-height:1.6;white-space:pre-wrap;word-break:break-word}
.note-node .note-id{font-size:9px;color:#78350f;padding:0 10px 6px;font-family:monospace}

/* ── context menu ── */
.ctx-menu{position:fixed;background:#1a1d2e;border:1px solid #2a2d3e;border-radius:8px;padding:5px 0;min-width:170px;z-index:9000;box-shadow:0 8px 24px rgba(0,0,0,.5);animation:slideIn .1s ease}
.ctx-item{padding:7px 14px;font-size:12px;color:#cbd5e1;cursor:pointer;display:flex;align-items:center;gap:8px;transition:background .1s}
.ctx-item:hover{background:#2a2d3e;color:#e2e8f0}
.ctx-item.danger{color:#f87171}
.ctx-item.danger:hover{background:#3f1111;color:#f87171}
.ctx-divider{height:1px;background:#2a2d3e;margin:4px 0}
.ctx-label{padding:5px 14px 3px;font-size:10px;color:#4b5563;font-weight:600;text-transform:uppercase;letter-spacing:.06em;pointer-events:none}

/* ── toast ── */
.toast{position:fixed;bottom:20px;right:20px;background:#1a1d2e;border:1px solid #2a2d3e;border-radius:8px;padding:10px 16px;font-size:13px;color:#e2e8f0;z-index:9999;display:flex;align-items:center;gap:10px;box-shadow:0 8px 24px rgba(0,0,0,.4);animation:slideIn .2s ease}
.toast.success{border-color:#059669;color:#34d399}
.toast.error{border-color:#dc2626;color:#f87171}
@keyframes slideIn{from{transform:translateY(20px);opacity:0}to{transform:translateY(0);opacity:1}}
@keyframes spin{to{transform:rotate(360deg)}}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
@keyframes node-run-pulse{0%,100%{box-shadow:0 0 0 0 rgba(124,58,237,.55),0 4px 12px rgba(0,0,0,.4)}60%{box-shadow:0 0 0 6px rgba(124,58,237,0),0 4px 12px rgba(0,0,0,.4)}}

/* ── modal ── */
.modal-overlay{position:fixed;inset:0;background:rgba(0,0,0,.75);z-index:1000;display:flex;align-items:center;justify-content:center;backdrop-filter:blur(4px)}
.modal{background:#1a1d2e;border:1px solid #2a2d3e;border-radius:12px;padding:22px;min-width:360px;max-width:600px;max-height:80vh;overflow-y:auto;box-shadow:0 20px 60px rgba(0,0,0,.6)}
.modal h2{font-size:15px;font-weight:600;margin-bottom:14px}
.modal-btns{display:flex;gap:8px;justify-content:flex-end;margin-top:16px}
.graph-row{padding:9px 11px;border:1px solid #2a2d3e;border-radius:8px;margin-bottom:7px;cursor:pointer;display:flex;align-items:center;justify-content:space-between;transition:border-color .15s}
.graph-row:hover{border-color:#7c3aed}
.graph-row-name{font-size:12px;font-weight:600;color:#e2e8f0}
.graph-row-desc{font-size:10px;color:#64748b;margin-top:1px}
.graph-row-badge{font-size:10px;font-weight:600;flex-shrink:0}
.modal-section{font-size:10px;color:#64748b;font-weight:600;text-transform:uppercase;letter-spacing:.06em;margin:14px 0 8px;padding-bottom:5px;border-bottom:1px solid #2a2d3e}
.modal-section.templates{color:#854d0e;border-bottom-color:#854d0e44}
.graph-row-actions{display:flex;gap:5px;flex-shrink:0;opacity:0;transition:opacity .12s}
.graph-row:hover .graph-row-actions{opacity:1}
.btn-icon{padding:4px 7px;font-size:12px;border-radius:5px;border:none;cursor:pointer;transition:all .12s;background:#2a2d3e;color:#94a3b8}
.btn-icon:hover{background:#374151;color:#e2e8f0}
.btn-icon.danger:hover{background:#dc2626;color:#fff}
.tmpl-badge{font-size:9px;font-weight:700;padding:1px 6px;border-radius:4px;background:#854d0e33;color:#ca8a04;letter-spacing:.04em;flex-shrink:0}
.your-flows-empty{text-align:center;padding:20px 0;font-size:12px;color:#4b5563;border:1px dashed #2a2d3e;border-radius:8px;margin-bottom:6px}
.version-row{padding:10px;border-bottom:1px solid #2a2d3e;display:flex;align-items:center;justify-content:space-between}
.version-row:last-child{border-bottom:none}
.version-info{flex:1;min-width:0;font-size:12px}
.version-num{font-weight:600;color:#e2e8f0;margin-bottom:2px}
.version-date{font-size:11px;color:#64748b}
.version-note{font-size:11px;color:#94a3b8;margin-top:2px}
</style>
</head>
<body>
<div id="root"></div>
<script type="text/babel">
const {
  useState, useEffect, useRef, useCallback
} = React;

// ReactFlow UMD exposes library as window.ReactFlow; alias component to FlowCanvas
const {
  ReactFlow: FlowCanvas, ReactFlowProvider, addEdge, Background, Controls, MiniMap,
  Handle, Position, useNodesState, useEdgesState, useReactFlow,
  MarkerType, BackgroundVariant
} = ReactFlow;

// ── Node definitions ─────────────────────────────────────────────────────────
const NODE_DEFS = {
  "trigger.manual":      { label:"Manual Trigger",    icon:"▶",  color:"#059669", group:"trigger",   fields:[] },
  "trigger.webhook":     { label:"Webhook Trigger",   icon:"🔗", color:"#0284c7", group:"trigger",   fields:[{k:"description",l:"Description",ph:"What this webhook receives…"}] },
  "trigger.cron":        { label:"Cron Schedule",     icon:"⏰", color:"#7c3aed", group:"trigger",   fields:[{k:"cron",l:"Cron expression",ph:"0 9 * * 1-5"},{k:"timezone",l:"Timezone",ph:"UTC"},{k:"description",l:"Description (optional)",ph:"Weekdays at 9am"}] },
  "action.http_request": { label:"HTTP Request",      icon:"🌐", color:"#0891b2", group:"action",    fields:[{k:"url",l:"URL",ph:"https://api.example.com/data"},{k:"method",l:"Method",ph:"GET"},{k:"headers_json",l:"Headers (JSON)",ph:'{"Authorization":"Bearer {{token}}"}',mono:true},{k:"body_json",l:"Body (JSON)",ph:'{"key":"{{value}}"}',mono:true,textarea:true}] },
  "action.transform":    { label:"Transform",         icon:"⚙",  color:"#7c3aed", group:"action",    fields:[{k:"expression",l:"Python expression",ph:"{'out': input['value'] * 2}",mono:true,textarea:true}] },
  "action.condition":    { label:"Condition",         icon:"◈",  color:"#d97706", group:"action",    fields:[{k:"expression",l:"Condition (Python bool)",ph:"input.get('status') == 'ok'",mono:true}] },
  "action.filter":       { label:"Filter / Where",    icon:"🔽", color:"#0d9488", group:"action",    fields:[{k:"field",l:"Array field (blank = whole input)",ph:"items"},{k:"expression",l:"Keep condition (use 'item')",ph:"item.get('active') == True",mono:true,textarea:true}] },
  "action.log":          { label:"Log",               icon:"📋", color:"#475569", group:"action",    fields:[{k:"message",l:"Message",ph:"{{input.value}}"}] },
  "action.set_variable": { label:"Set Variable",      icon:"📌", color:"#be185d", group:"action",    fields:[{k:"key",l:"Variable name",ph:"my_var"},{k:"value",l:"Value",ph:"{{input.result}}"}] },
  "action.delay":        { label:"Delay / Wait",      icon:"⏱", color:"#4b5563", group:"action",    fields:[{k:"seconds",l:"Seconds",ph:"5"}] },
  "action.run_script":   { label:"Run Python Script", icon:"🐍", color:"#84cc16", group:"action",    fields:[{k:"script",l:"Python script (assign result)",ph:"# input contains upstream output\nresult = {'doubled': input.get('value',0) * 2}",mono:true,textarea:true}] },
  "action.llm_call":     { label:"LLM Call",          icon:"🤖", color:"#8b5cf6", group:"action",    fields:[{k:"model",l:"Model",ph:"gpt-4o-mini"},{k:"prompt",l:"Prompt",ph:"Summarise this: {{upstream.body}}",textarea:true},{k:"system",l:"System prompt (optional)",ph:"You are a helpful assistant.",textarea:true},{k:"api_key",l:"API Key (blank = env)",ph:"sk-…",secret:true},{k:"api_base",l:"API Base (blank = OpenAI)",ph:"https://api.groq.com/openai/v1"}] },
  "action.send_email":   { label:"Send Email",        icon:"✉",  color:"#0e7490", group:"action",    fields:[{k:"to",l:"To",ph:"user@example.com"},{k:"subject",l:"Subject",ph:"Hello from automations"},{k:"body",l:"Body",ph:"Hi,\n\nYour task completed.",textarea:true},{k:"credential",l:"SMTP Credential (name)",ph:"my-smtp-server"},{k:"smtp_host",l:"SMTP Host (overrides credential)",ph:"smtp.gmail.com"},{k:"smtp_user",l:"SMTP User (overrides credential)",ph:"you@gmail.com"},{k:"smtp_pass",l:"SMTP Pass (overrides credential)",ph:"app-password",secret:true}] },
  "action.telegram":     { label:"Telegram Message",  icon:"✈",  color:"#0088cc", group:"action",    fields:[{k:"text",l:"Message",ph:"Task done: {{input.result}}",textarea:true},{k:"bot_token",l:"Bot Token (blank = env)",ph:"123456:ABC…",secret:true},{k:"chat_id",l:"Chat ID (blank = env)",ph:"-100123456789"}] },
  "action.loop":         { label:"Loop / For Each",   icon:"🔄", color:"#7e22ce", group:"action",    fields:[{k:"field",l:"Array field (blank = whole input)",ph:"items"},{k:"max_items",l:"Max iterations",ph:"100"}] },
  "action.slack":        { label:"Slack Message",     icon:"💬", color:"#4a154b", group:"action",  fields:[{k:"credential",l:"Slack Credential (name)",ph:"my-slack"},{k:"webhook_url",l:"Webhook URL (overrides credential)",ph:"https://hooks.slack.com/services/...",secret:true},{k:"message",l:"Message",ph:"Flow done: {{upstream.result}}",textarea:true},{k:"channel",l:"Channel (optional)",ph:"#notifications"}] },
  "action.call_graph":   { label:"Call Sub-flow",      icon:"⛓",  color:"#6366f1", group:"action",  fields:[{k:"graph_id",l:"Target Graph ID",ph:"123"},{k:"payload",l:"Payload override (JSON)",ph:'{"key":"{{upstream.value}}"}',textarea:true,mono:true}] },
  "action.ssh":          { label:"SSH Command",        icon:"💻", color:"#0f766e", group:"action",    fields:[{k:"credential",l:"SSH Credential (name)",ph:"my-ssh-server"},{k:"host",l:"Host (overrides credential)",ph:"192.168.1.1"},{k:"port",l:"Port (overrides credential)",ph:"22"},{k:"username",l:"Username (overrides credential)",ph:"admin"},{k:"password",l:"Password (overrides credential)",ph:"••••••••",secret:true},{k:"command",l:"Command",ph:"ls -la /var/log",mono:true,textarea:true}] },
  "action.sftp":         { label:"SFTP / FTP",         icon:"📁", color:"#1d4ed8", group:"action",    fields:[{k:"credential",l:"SFTP Credential (name)",ph:"my-sftp-server"},{k:"protocol",l:"Protocol (overrides credential)",ph:"sftp",type:"select",options:["sftp","ftp"]},{k:"host",l:"Host (overrides credential)",ph:"files.example.com"},{k:"port",l:"Port (overrides credential)",ph:"22 (sftp) · 21 (ftp)"},{k:"username",l:"Username (overrides credential)",ph:"admin"},{k:"password",l:"Password (overrides credential)",ph:"••••••••",secret:true},{k:"operation",l:"Operation",ph:"list",type:"select",options:["list","upload","download","delete","mkdir","rename","exists","stat"]},{k:"remote_path",l:"Remote path",ph:"/uploads/report.csv",mono:true},{k:"recursive",l:"Recursive (list only)",ph:"false",type:"select",options:["false","true"]},{k:"new_path",l:"New path (rename only)",ph:"/uploads/renamed.csv",mono:true},{k:"content",l:"Content (upload only)",ph:"{{upstream.body}}",textarea:true},{k:"timeout",l:"Timeout (seconds)",ph:"30"}] },
  "action.github":       { label:"GitHub",             icon:"🐙", color:"#24292f", group:"integration", fields:[
    {k:"credential",l:"GitHub Credential (name)",ph:"my-github"},
    {k:"token",l:"Token (overrides credential)",ph:"ghp_…",secret:true},
    {k:"action",l:"Action",ph:"list_issues",type:"select",options:["get_repo","list_issues","get_issue","create_issue","close_issue","add_comment","list_commits","list_prs","get_file","create_release"]},
    {k:"owner",l:"Owner",ph:"my-org or username"},
    {k:"repo",l:"Repository",ph:"my-repo"},
    {k:"issue_number",l:"Issue / PR number",ph:"42"},
    {k:"title",l:"Title (create_issue / create_release)",ph:"Bug: something broken"},
    {k:"body",l:"Body / Comment",ph:"Details…",textarea:true},
    {k:"labels",l:"Labels (comma-separated)",ph:"bug,priority"},
    {k:"path",l:"File path (get_file)",ph:"src/main.py"},
    {k:"ref",l:"Branch / tag (get_file / list_commits)",ph:"main"},
    {k:"tag",l:"Tag name (create_release)",ph:"v1.0.0"},
  ]},
  "action.google_sheets":{ label:"Google Sheets",      icon:"📊", color:"#0f9d58", group:"integration", fields:[
    {k:"credential",l:"Google SA Credential (name)",ph:"my-google-sa"},
    {k:"service_account_json",l:"Service Account JSON (overrides credential)",ph:'{"type":"service_account",…}',secret:true,textarea:true,mono:true},
    {k:"action",l:"Action",ph:"read_range",type:"select",options:["read_range","write_range","append_rows","clear_range"]},
    {k:"spreadsheet_id",l:"Spreadsheet ID",ph:"1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms"},
    {k:"range",l:"Range (A1 notation)",ph:"Sheet1!A1:D10"},
    {k:"rows_json",l:"Rows JSON (write / append)",ph:'[["Name","Age"],["Alice",30]]',textarea:true,mono:true},
    {k:"header_row",l:"First row is header? (read_range)",ph:"true"},
  ]},
  "action.notion":       { label:"Notion",             icon:"🗒", color:"#000000", group:"integration", fields:[
    {k:"credential",l:"Notion Credential (name)",ph:"my-notion"},
    {k:"token",l:"Integration Token (overrides credential)",ph:"secret_…",secret:true},
    {k:"action",l:"Action",ph:"query_database",type:"select",options:["query_database","get_page","create_page","update_page","search","append_blocks"]},
    {k:"database_id",l:"Database ID (query_database / create_page)",ph:"abc123…"},
    {k:"page_id",l:"Page ID (get/update/append)",ph:"def456…"},
    {k:"filter_json",l:"Filter JSON (query_database)",ph:'{"property":"Status","select":{"equals":"Done"}}',textarea:true,mono:true},
    {k:"properties_json",l:"Properties JSON (create/update)",ph:'{"Name":{"title":[{"text":{"content":"Hello"}}]}}',textarea:true,mono:true},
    {k:"query",l:"Search query (search)",ph:"Meeting notes"},
    {k:"content",l:"Content text (append_blocks)",ph:"New paragraph text"},
  ]},
  "action.merge":        { label:"Merge / Join",       icon:"⇒",  color:"#0f766e", group:"action", fields:[
    {k:"mode",l:"Mode",ph:"dict",type:"select",options:["dict","all","first"]},
  ]},
  "note":                { label:"Sticky Note",        icon:"📝", color:"#ca8a04", group:"utility",   fields:[{k:"text",l:"Note text",ph:"Document what this part of the flow does…",textarea:true}] },
};

const GROUPS = [
  { id:"trigger",     label:"Triggers"     },
  { id:"action",      label:"Actions"      },
  { id:"integration", label:"Integrations" },
  { id:"utility",     label:"Utilities"    },
];

// ── helpers ───────────────────────────────────────────────────────────────────
function uid(){ return Math.random().toString(36).slice(2,9) }
function getToken(){ return new URLSearchParams(window.location.search).get("token") || "" }

async function api(method, path, body){
  const r = await fetch(path, {
    method,
    headers:{"Content-Type":"application/json","x-admin-token":getToken()},
    body: body ? JSON.stringify(body) : undefined
  });
  if(!r.ok){ const e = await r.json().catch(()=>({detail:r.statusText})); throw new Error(e.detail||r.statusText) }
  return r.json();
}

// ── Toast ─────────────────────────────────────────────────────────────────────
function Toast({ msg, type, onDone }){
  useEffect(()=>{ const t=setTimeout(onDone,3500); return ()=>clearTimeout(t) },[]);
  return <div className={`toast ${type}`}>{type==="success"?"✓":"✗"} {msg}</div>;
}

// ── Custom Node ───────────────────────────────────────────────────────────────
function StickyNote({ id, data, selected }){
  const text = (data.config||{}).text || "Double-click to edit in config panel →";
  return (
    <div className={`note-node${selected?" selected":""}`}>
      <div className="note-body">{text}</div>
      <div className="note-id">#{id}</div>
    </div>
  );
}

function CustomNode({ id, data, selected }){
  if(data.type === "note") return <StickyNote id={id} data={data} selected={selected}/>;

  const def = NODE_DEFS[data.type] || { label:data.type, icon:"?", color:"#475569" };
  const isCondition = data.type === "action.condition";
  const isLoop      = data.type === "action.loop";
  const isTrigger   = data.type?.startsWith("trigger.");
  const isDisabled  = !!data.disabled;
  const runStatus   = data._runStatus; // "ok" | "err" | "skip" | "pending"
  const runOutput   = data._runOutput;
  const runDuration = data._runDurationMs;

  const isCron = data.type === "trigger.cron";
  const cronExpr = isCron ? ((data.config||{}).cron || "not set") : null;
  const cronDesc = isCron ? ((data.config||{}).description || null) : null;
  const summary = isCron ? null : Object.entries(data.config||{})
    .filter(([k,v])=>v && !["smtp_pass","api_key","bot_token"].includes(k))
    .map(([k,v])=>`${k}: ${String(v).slice(0,22)}`).join(" · ").slice(0,55);

  return (
    <div className={`custom-node${selected?" selected":""}${isDisabled?" disabled-node":""}${runStatus==="pending"?" node-running":""}`}
         style={{borderColor: selected ? def.color : undefined}}>
      {!isTrigger && <Handle type="target" position={Position.Left} style={{background: def.color}} />}
      <div className="node-header">
        <div className="nh-icon" style={{background: def.color+"22", color: def.color}}>{def.icon}</div>
        <div style={{flex:1,minWidth:0}}>
          <div className="nh-title">{data.label||def.label}</div>
          <div className="nh-type">{data.type}</div>
        </div>
        {isDisabled && <span className="node-off-chip">OFF</span>}
      </div>
      {isCron ? (
        <div className="node-body" style={{padding:"5px 10px 6px"}}>
          <div style={{fontFamily:"'JetBrains Mono','Fira Code',monospace",fontSize:11,color:"#a78bfa",letterSpacing:".03em"}}>{cronExpr}</div>
          {cronDesc && <div style={{fontSize:10,color:"#64748b",marginTop:2}}>{cronDesc}</div>}
        </div>
      ) : summary ? (
        <div className="node-body">{summary}</div>
      ) : null}
      <div className="node-footer">
        <span className="node-id">#{id}</span>
        {runStatus && (
          <span className={`node-status-badge ${runStatus}`}>
            {runStatus==="ok" ? "✓" : runStatus==="err" ? "✗" : runStatus==="pending" ? "⟳" : "—"}
            {runStatus==="ok" && runDuration!=null ? ` ${runDuration}ms` : ""}
            {runStatus==="err" ? " err" : ""}
            {runStatus==="skip" ? " skip" : ""}
          </span>
        )}
      </div>
      {isCondition ? (
        <>
          <Handle type="source" position={Position.Right} id="true"  style={{top:"35%",background:"#059669"}} />
          <Handle type="source" position={Position.Right} id="false" style={{top:"65%",background:"#dc2626"}} />
        </>
      ) : isLoop ? (
        <>
          <Handle type="source" position={Position.Right} id="body" style={{top:"35%",background:def.color}} title="Loop body"/>
          <Handle type="source" position={Position.Bottom} id="done" style={{background:"#64748b"}} title="After loop"/>
        </>
      ) : (
        <Handle type="source" position={Position.Right} style={{background: def.color}} />
      )}
    </div>
  );
}

const nodeTypes = { custom: CustomNode };

// ── Node I/O Panel helpers ─────────────────────────────────────────────────────
// sourceNodeId: the node whose output this data came from (used to build {{id.path}} template)
// fieldPath: dot-notation path from root, e.g. "customer.name"
function JsonSchemaTree({ data, depth, sourceNodeId, fieldPath }){
  depth = depth || 0;
  fieldPath = fieldPath || null;
  const [collapsed, setCollapsed] = React.useState(depth > 1);

  const template = (sourceNodeId && fieldPath) ? `{{${sourceNodeId}.${fieldPath}}}` : null;

  function startDrag(e){
    if(!template) return;
    e.dataTransfer.setData("text/plain", template);
    e.dataTransfer.effectAllowed = "copy";
    e.stopPropagation();
  }

  // Leaf primitives — render inline with optional drag row
  function Draggable({ children }){
    if(!template) return children;
    return (
      <span className="nio-drag-row" draggable="true" onDragStart={startDrag} title={`Drag to insert: ${template}`}>
        <span className="nio-drag-handle" title={template}>⠿</span>
        {children}
      </span>
    );
  }

  if(data === null || data === undefined)
    return <Draggable><span className="nio-tree-null">null</span></Draggable>;
  if(typeof data === 'boolean')
    return <Draggable><span className="nio-tree-bool">{String(data)}</span></Draggable>;
  if(typeof data === 'number')
    return <Draggable><span className="nio-tree-num">{String(data)}</span></Draggable>;
  if(typeof data === 'string'){
    const display = data.length > 120 ? data.slice(0,120)+"…" : data;
    return <Draggable><span className="nio-tree-str">"{display}"</span></Draggable>;
  }

  if(Array.isArray(data)){
    if(data.length === 0) return <Draggable><span style={{color:"#94a3b8"}}>[ ] (empty)</span></Draggable>;
    const preview = `[ ${data.length} item${data.length>1?"s":""}  ]`;
    return (
      <div>
        <span className="nio-drag-row" draggable={!!template} onDragStart={startDrag}>
          {template && <span className="nio-drag-handle" title={template}>⠿</span>}
          <span className="nio-tree-expand" onClick={()=>setCollapsed(c=>!c)}>
            {collapsed?"▶":"▼"} {preview}
          </span>
        </span>
        {!collapsed && (
          <div className="nio-tree-indent">
            {data.slice(0,30).map((item,i)=>{
              const childPath = fieldPath ? `${fieldPath}[${i}]` : `[${i}]`;
              return (
                <div key={i} style={{marginBottom:2}}>
                  <span style={{color:"#64748b",fontSize:9}}>[{i}]</span>{" "}
                  <JsonSchemaTree data={item} depth={depth+1} sourceNodeId={sourceNodeId} fieldPath={childPath}/>
                </div>
              );
            })}
            {data.length>30 && <div style={{color:"#64748b",fontSize:9,fontStyle:"italic"}}>…{data.length-30} more</div>}
          </div>
        )}
      </div>
    );
  }

  if(typeof data === 'object'){
    const keys = Object.keys(data);
    if(keys.length === 0) return <Draggable><span style={{color:"#94a3b8"}}>{"{ }"} (empty)</span></Draggable>;
    const preview = `{ ${keys.slice(0,3).join(", ")}${keys.length>3?"…":""} }`;
    return (
      <div>
        <span className="nio-drag-row" draggable={!!template} onDragStart={startDrag}>
          {template && <span className="nio-drag-handle" title={template}>⠿</span>}
          <span className="nio-tree-expand" onClick={()=>setCollapsed(c=>!c)}>
            {collapsed?"▶":"▼"} {preview}
          </span>
        </span>
        {!collapsed && (
          <div className="nio-tree-indent">
            {keys.map(k=>{
              const childPath = fieldPath ? `${fieldPath}.${k}` : k;
              return (
                <div key={k} className="nio-drag-row" draggable={!!sourceNodeId}
                  onDragStart={e=>{ e.dataTransfer.setData("text/plain",`{{${sourceNodeId}.${childPath}}}`); e.dataTransfer.effectAllowed="copy"; e.stopPropagation(); }}
                  style={{marginBottom:2,lineHeight:1.7}}>
                  {sourceNodeId && <span className="nio-drag-handle" title={`{{${sourceNodeId}.${childPath}}}`}>⠿</span>}
                  <div style={{flex:1}}>
                    <span className="nio-tree-key">{k}:{" "}</span>
                    <JsonSchemaTree data={data[k]} depth={depth+1} sourceNodeId={sourceNodeId} fieldPath={childPath}/>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>
    );
  }
  return <Draggable><span style={{color:"#94a3b8"}}>{String(data)}</span></Draggable>;
}

const NIO_DISPLAY_LINES = 500; // max lines to render before capping

// Inner panel body — shared between inline panel and expanded modal
function NioBody({ data, view, sourceNodeId, isTruncated }){
  if(isTruncated){
    const kb = data.__size ? Math.round(data.__size / 1024) : "?";
    return (
      <div style={{color:"#64748b",fontSize:11,padding:"6px 0"}}>
        <div style={{color:"#f59e0b",marginBottom:4}}>⚠ Payload too large to display ({kb} KB)</div>
        <div style={{fontSize:10,color:"#475569"}}>Exceeds the 5 MB display limit. Use a Transform or Run Script node to extract only the fields you need.</div>
      </div>
    );
  }
  if(data === undefined)
    return <div style={{color:"#475569",fontSize:11,fontStyle:"italic"}}>No data yet — run the flow first</div>;
  if(view === "schema")
    return (
      <div style={{fontSize:11,lineHeight:1.8}}>
        {sourceNodeId && (
          <div style={{fontSize:9,color:"#64748b",marginBottom:6,fontStyle:"italic"}}>
            ⠿ drag any field into a config input to insert a template reference
          </div>
        )}
        <JsonSchemaTree data={data} depth={0} sourceNodeId={sourceNodeId} fieldPath={null}/>
      </div>
    );
  const fullStr = (data === null) ? "null" : (typeof data === "string" ? data : JSON.stringify(data, null, 2));
  const lines = fullStr.split("\n");
  const capped = lines.length > NIO_DISPLAY_LINES;
  const displayed = capped ? lines.slice(0, NIO_DISPLAY_LINES).join("\n") : fullStr;
  function copyFull(){ navigator.clipboard.writeText(fullStr).catch(()=>{}); }
  return (
    <div>
      <pre className="nio-json">{displayed}</pre>
      {capped && (
        <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",padding:"5px 8px",background:"#0f1117",borderTop:"1px solid #2a2d3e",fontSize:10,color:"#64748b"}}>
          <span>Showing {NIO_DISPLAY_LINES} of {lines.length} lines ({Math.round(fullStr.length/1024)} KB)</span>
          <button onClick={copyFull} style={{background:"#2a2d3e",border:"none",color:"#94a3b8",borderRadius:4,padding:"2px 8px",cursor:"pointer",fontSize:10}}>⎘ Copy all</button>
        </div>
      )}
    </div>
  );
}

function NodeIOPanel({ runInput, runOutput, runStatus, runDurationMs, runAttempts, nodeId, upstreamNodeId, nodeLabel }){
  const [side, setSide]     = useState("output");
  const [view, setView]     = useState("schema");
  const [expanded, setExpanded] = useState(false);

  const data         = side === "input" ? runInput : runOutput;
  // sourceNodeId for drag templates:
  // OUTPUT tab → current node (nodeId) is what downstream refs; INPUT tab → upstream node produced it
  const sourceNodeId = side === "output" ? nodeId : upstreamNodeId;

  const isErr   = runStatus === "err";
  const isOk    = runStatus === "ok";
  const statusColor = isOk ? "#4ade80" : isErr ? "#f87171" : "#94a3b8";
  const statusLabel = isOk ? "Success"  : isErr ? "Error"   : "Skipped";
  const isTruncated = !!(data && data.__truncated);

  const Tabs = () => (
    <>
      <div className="nio-side-tabs">
        <button className={`nio-tab${side==="input"?" nio-tab-active":""}`} onClick={()=>setSide("input")}>INPUT</button>
        <button className={`nio-tab${side==="output"?" nio-tab-active":""}`} onClick={()=>setSide("output")}>OUTPUT</button>
      </div>
      <div style={{display:"flex",alignItems:"center",gap:6}}>
        {runDurationMs !== undefined && (
          <span style={{fontSize:9,color:"#64748b"}}>{runDurationMs}ms{runAttempts>1?` · ${runAttempts}×`:""}</span>
        )}
        {runStatus && (
          <span style={{fontSize:9,fontWeight:700,color:statusColor,background:isOk?"#14532d33":isErr?"#7f1d1d33":"#1e293b",padding:"2px 7px",borderRadius:4}}>
            {statusLabel}
          </span>
        )}
        <button className="nio-expand-btn" onClick={()=>setExpanded(true)} title="Expand to full view">⛶</button>
      </div>
    </>
  );

  const ViewTabs = () => (
    <div className="nio-view-tabs">
      <button className={`nio-view-tab${view==="schema"?" active":""}`} onClick={()=>setView("schema")}>Schema</button>
      <button className={`nio-view-tab${view==="json"?" active":""}`} onClick={()=>setView("json")}>JSON</button>
    </div>
  );

  return (
    <>
      <div className="nio-panel">
        <div className="nio-header"><Tabs/></div>
        <ViewTabs/>
        <div className="nio-content">
          <NioBody data={data} view={view} sourceNodeId={sourceNodeId} isTruncated={isTruncated}/>
        </div>
      </div>

      {/* Expanded modal overlay */}
      {expanded && (
        <div className="nio-modal-overlay" onClick={e=>{ if(e.target===e.currentTarget) setExpanded(false); }}>
          <div className="nio-modal">
            <div className="nio-modal-hdr">
              <div className="nio-side-tabs"><Tabs/></div>
              <button className="nio-modal-close" onClick={()=>setExpanded(false)}>✕</button>
            </div>
            <ViewTabs/>
            <div className="nio-modal-body">
              <div className="nio-content" style={{maxHeight:"calc(85vh - 90px)"}}>
                <NioBody data={data} view={view} sourceNodeId={sourceNodeId} isTruncated={isTruncated}/>
              </div>
            </div>
          </div>
        </div>
      )}
    </>
  );
}

// ── Config Panel ──────────────────────────────────────────────────────────────
function ConfigPanel({ node, onChange, onDelete, edges }){
  if(!node){
    return (
      <div className="config-panel">
        <div className="panel-header"><h3>Node Config</h3></div>
        <div className="panel-body">
          <div className="config-empty">
            <div style={{fontSize:28,marginBottom:10}}>⬅</div>
            Click any node to configure it.<br/>
            Drag from the palette to add.<br/><br/>
            <span style={{fontSize:10,color:"#4b5563"}}>
              Use <code style={{color:"#a78bfa"}}>{"{{node_id.field}}"}</code><br/>
              to pass data between nodes.
            </span>
          </div>
        </div>
      </div>
    );
  }

  const isNote = node.data.type === "note";
  const isTrigger = node.data.type?.startsWith("trigger.");
  const def = NODE_DEFS[node.data.type] || { label:node.data.type, icon:"?", color:"#475569", fields:[] };
  const isDisabled = !!node.data.disabled;
  const runStatus      = node.data._runStatus;
  const runOutput      = node.data._runOutput;
  const runInput       = node.data._runInput;
  const runDurationMs  = node.data._runDurationMs;
  const runAttempts    = node.data._runAttempts;
  const retryMax = node.data.retry_max ?? 0;
  const retryDelay = node.data.retry_delay ?? 5;

  // Compute the upstream node id (for INPUT drag references)
  const upstreamNodeId = React.useMemo(()=>{
    if(!edges || !node) return null;
    const inEdge = edges.find(e => e.target === node.id);
    return inEdge ? inEdge.source : null;
  }, [edges, node && node.id]);

  // Drop handler factory for config input/textarea fields
  function dropProps(fieldKey, currentVal){
    return {
      onDragOver: e => { e.preventDefault(); e.currentTarget.classList.add("drag-over"); },
      onDragLeave: e => e.currentTarget.classList.remove("drag-over"),
      onDrop: e => {
        e.preventDefault();
        e.currentTarget.classList.remove("drag-over");
        const tmpl = e.dataTransfer.getData("text/plain");
        if(!tmpl) return;
        const el = e.currentTarget;
        const start = el.selectionStart ?? (currentVal||"").length;
        const end   = el.selectionEnd   ?? start;
        const val   = currentVal || "";
        const newVal = val.slice(0,start) + tmpl + val.slice(end);
        update(fieldKey, newVal);
        setTimeout(()=>{ try{ el.selectionStart = el.selectionEnd = start + tmpl.length; }catch(e){} }, 0);
      }
    };
  }

  function update(key, val){
    onChange(node.id, { ...node.data, config:{ ...node.data.config, [key]:val }});
  }
  function updateLabel(val){ onChange(node.id, { ...node.data, label:val }) }
  function updateRetryMax(val){ onChange(node.id, { ...node.data, retry_max: parseInt(val)||0 }) }
  function updateRetryDelay(val){ onChange(node.id, { ...node.data, retry_delay: parseInt(val)||5 }) }
  function updateFailMode(val){ onChange(node.id, { ...node.data, fail_mode: val }) }
  const failMode = node.data.fail_mode || 'abort';
  function copyId(){ navigator.clipboard.writeText(node.id).catch(()=>{}) }
  function toggleDisabled(){
    onChange(node.id, { ...node.data, disabled: !isDisabled });
  }

  const statusLabel = runStatus==="ok" ? "Succeeded" : runStatus==="err" ? "Failed" : runStatus==="skip" ? "Skipped" : null;
  const statusCls   = runStatus==="ok" ? "run-status-ok" : runStatus==="err" ? "run-status-err" : "run-status-skip";

  return (
    <div className="config-panel">
      <div className="panel-header">
        <h3>{def.icon} {def.label}</h3>
      </div>
      <div className="panel-body">

        {/* Node ID badge */}
        <div className="node-id-badge" onClick={copyId} title="Click to copy node ID">
          <div>
            <div style={{fontSize:10,color:"#64748b",marginBottom:2}}>NODE ID (for templates)</div>
            <code>{node.id}</code>
          </div>
          <span className="copy-hint">📋 copy</span>
        </div>

        {/* Disable / Enable toggle (not for sticky notes) */}
        {!isNote && (
          <div className="disable-toggle-row">
            <span className="disable-toggle-label">{isDisabled ? "⏸ Node disabled" : "▶ Node enabled"}</span>
            <label className="toggle-switch">
              <input type="checkbox" checked={!isDisabled} onChange={toggleDisabled}/>
              <div className="toggle-track"/>
              <div className="toggle-thumb"/>
            </label>
          </div>
        )}

        {/* Label */}
        {!isNote && (
          <>
            <div className="section-divider">Label</div>
            <div className="field-group">
              <input className="field-input" placeholder={def.label}
                value={node.data.label||""} onChange={e=>updateLabel(e.target.value)}/>
            </div>
          </>
        )}

        {/* Fields */}
        {def.fields.length > 0 && <div className="section-divider">Config</div>}
        {def.fields.map(f => {
          const cfgVal = (node.data.config||{})[f.k]||"";
          const dp = (!f.secret && f.type !== "select") ? dropProps(f.k, cfgVal) : {};
          return (
          <div className="field-group" key={f.k}>
            <div className="field-label">
              {f.l}
              {f.secret && <span className="field-hint">🔒 sensitive</span>}
            </div>
            {f.type === "select" ? (
              <select className="field-input" value={cfgVal||f.options[0]}
                onChange={e=>update(f.k,e.target.value)}>
                {f.options.map(o=><option key={o} value={o}>{o}</option>)}
              </select>
            ) : f.textarea ? (
              <textarea className={`field-input${f.mono?" mono":""}`} placeholder={f.ph}
                value={cfgVal} onChange={e=>update(f.k,e.target.value)} rows={4} {...dp}/>
            ) : (
              <input className={`field-input${f.mono?" mono":""}`} placeholder={f.ph}
                type={f.secret?"password":"text"}
                value={cfgVal} onChange={e=>update(f.k,e.target.value)} {...dp}/>
            )}
            {f.secret && <span className="secret-hint">💡 Or use {"{{creds.your-credential-name}}"}</span>}
          </div>
          );
        })}

        {/* Condition / Loop hints */}
        {node.data.type === "action.condition" && (
          <div style={{background:"#0f1117",border:"1px solid #2a2d3e",borderRadius:6,padding:"8px 10px",fontSize:10,color:"#94a3b8",marginTop:6,lineHeight:1.8}}>
            Expression must evaluate to a Python bool.<br/>
            <div style={{color:"#059669",marginTop:4}}>● True handle → nodes that run when condition is met</div>
            <div style={{color:"#dc2626"}}>● False handle → nodes that run when condition is not met</div>
            <div style={{marginTop:4}}>Nodes reachable from the un-taken handle are <strong>skipped</strong> automatically.<br/>
            Nodes after both branches converge always run.</div>
            <div style={{marginTop:4}}>Output: <code style={{color:"#a78bfa"}}>{"{ result: bool, input: ... }"}</code></div>
          </div>
        )}
        {node.data.type === "action.loop" && (
          <div style={{background:"#0f1117",border:"1px solid #2a2d3e",borderRadius:6,padding:"8px 10px",fontSize:10,color:"#94a3b8",marginTop:6}}>
            <div style={{color:"#7e22ce",marginBottom:3}}>● Body → runs per item</div>
            <div style={{color:"#64748b",marginBottom:4}}>● Done → after all iterations</div>
            <div>Each body node receives the current item as input.</div>
          </div>
        )}
        {node.data.type === "action.filter" && (
          <div style={{background:"#0f1117",border:"1px solid #2a2d3e",borderRadius:6,padding:"8px 10px",fontSize:10,color:"#94a3b8",marginTop:6}}>
            Output: <code style={{color:"#a78bfa"}}>{"{ items: [...], count: N }"}</code><br/>
            Use <code style={{color:"#a78bfa"}}>item</code> in the expression.
          </div>
        )}
        {node.data.type === "action.llm_call" && (
          <div style={{background:"#0f1117",border:"1px solid #2a2d3e",borderRadius:6,padding:"8px 10px",fontSize:10,color:"#94a3b8",marginTop:6}}>
            Output: <code style={{color:"#a78bfa"}}>{"{ response, model, tokens }"}</code><br/>
            Set <code style={{color:"#a78bfa"}}>api_base</code> for Groq, Together AI, Ollama etc.
          </div>
        )}
        {node.data.type === "action.slack" && (
          <div style={{background:"#0f1117",border:"1px solid #2a2d3e",borderRadius:6,padding:"8px 10px",fontSize:10,color:"#94a3b8",marginTop:6,lineHeight:1.9}}>
            <div style={{color:"#a78bfa",fontWeight:600,marginBottom:4}}>💡 Credential shortcut</div>
            Create a <strong>Slack Incoming Webhook</strong> credential, then set <strong>Slack Credential</strong> to its name — the webhook URL is filled automatically.<br/>
            The Webhook URL field overrides the credential if both are set.<br/>
            <div style={{marginTop:4}}>Output: <code style={{color:"#a78bfa"}}>{"{ sent: true, message }"}</code></div>
          </div>
        )}
        {node.data.type === "action.call_graph" && (
          <div style={{background:"#0f1117",border:"1px solid #2a2d3e",borderRadius:6,padding:"8px 10px",fontSize:10,color:"#94a3b8",marginTop:6}}>
            Runs another flow as a sub-routine.<br/>
            Output: the sub-flow's final context dict.<br/>
            Max nesting depth: <code style={{color:"#a78bfa"}}>5</code> levels.
          </div>
        )}
        {node.data.type === "action.run_script" && (
          <div style={{background:"#0f1117",border:"1px solid #2a2d3e",borderRadius:6,padding:"8px 10px",fontSize:10,color:"#94a3b8",marginTop:6}}>
            Variables: <code style={{color:"#a78bfa"}}>input</code>, <code style={{color:"#a78bfa"}}>context</code>, <code style={{color:"#a78bfa"}}>log</code>, <code style={{color:"#a78bfa"}}>json</code>, <code style={{color:"#a78bfa"}}>os</code>, <code style={{color:"#a78bfa"}}>time</code><br/>
            Assign your output to <code style={{color:"#a78bfa"}}>result</code>.
          </div>
        )}
        {node.data.type === "action.send_email" && (
          <div style={{background:"#0f1117",border:"1px solid #2a2d3e",borderRadius:6,padding:"8px 10px",fontSize:10,color:"#94a3b8",marginTop:6,lineHeight:1.9}}>
            <div style={{color:"#a78bfa",fontWeight:600,marginBottom:4}}>💡 Credential shortcut</div>
            Create an <strong>SMTP Server</strong> credential, then set <strong>SMTP Credential</strong> to its name — host, port, user and password are filled automatically.<br/>
            The individual host/user/pass fields override the credential if both are set.
          </div>
        )}
        {node.data.type === "action.ssh" && (
          <div style={{background:"#0f1117",border:"1px solid #2a2d3e",borderRadius:6,padding:"8px 10px",fontSize:10,color:"#94a3b8",marginTop:6,lineHeight:1.9}}>
            <div style={{color:"#a78bfa",fontWeight:600,marginBottom:4}}>💡 Credential shortcut</div>
            Create an <strong>SSH Server</strong> credential in the Credentials page, then set <strong>SSH Credential</strong> to its name — host, port, username and password are filled automatically.<br/>
            Individual fields override the credential if both are set.<br/>
            <div style={{marginTop:6}}>Output: <code style={{color:"#a78bfa"}}>{"{ stdout, stderr, exit_code, success }"}</code></div>
            <span style={{color:"#64748b"}}>Host keys are auto-accepted (self-hosted use only).</span>
          </div>
        )}
        {node.data.type === "action.sftp" && (
          <div style={{background:"#0f1117",border:"1px solid #2a2d3e",borderRadius:6,padding:"8px 10px",fontSize:10,color:"#94a3b8",marginTop:6,lineHeight:1.9}}>
            <div style={{color:"#a78bfa",fontWeight:600,marginBottom:4}}>📁 SFTP / FTP Operations</div>
            Create an <strong>SFTP / FTP Server</strong> credential and set <strong>SFTP Credential</strong> to its name.<br/>
            <div style={{display:"grid",gridTemplateColumns:"auto 1fr",gap:"0 10px",marginTop:6}}>
              <code style={{color:"#a78bfa"}}>list</code><span>→ {"{ files:[{name,path,size,is_dir,depth}], count }"} — set <em>recursive: true</em> to walk all subdirs</span>
              <code style={{color:"#a78bfa"}}>upload</code><span>→ put <em>content</em> at remote_path</span>
              <code style={{color:"#a78bfa"}}>download</code><span>→ {"{ content, size }"}</span>
              <code style={{color:"#a78bfa"}}>delete</code><span>→ remove remote file</span>
              <code style={{color:"#a78bfa"}}>mkdir</code><span>→ create remote directory</span>
              <code style={{color:"#a78bfa"}}>rename</code><span>→ move/rename — set <em>new_path</em> as destination</span>
              <code style={{color:"#a78bfa"}}>exists</code><span>→ {"{ exists, is_dir }"} — non-destructive check</span>
              <code style={{color:"#a78bfa"}}>stat</code><span>→ {"{ size, is_dir, modified }"} — metadata for one path</span>
            </div>
          </div>
        )}
        {node.data.type === "action.github" && (
          <div style={{background:"#0f1117",border:"1px solid #2a2d3e",borderRadius:6,padding:"8px 10px",fontSize:10,color:"#94a3b8",marginTop:6,lineHeight:1.9}}>
            <div style={{color:"#a78bfa",fontWeight:600,marginBottom:4}}>🐙 GitHub Integration</div>
            <div style={{color:"#94a3b8",marginBottom:6}}>Create a <strong>GitHub Token</strong> credential (type: API Key, paste your PAT in the <em>api_key</em> field), then set <strong>GitHub Credential</strong> to its name.</div>
            <div style={{display:"grid",gridTemplateColumns:"auto 1fr",gap:"0 10px"}}>
              <code style={{color:"#a78bfa"}}>get_repo</code><span>→ repo details (stars, forks, description…)</span>
              <code style={{color:"#a78bfa"}}>list_issues</code><span>→ open issues list</span>
              <code style={{color:"#a78bfa"}}>get_issue</code><span>→ single issue (needs issue_number)</span>
              <code style={{color:"#a78bfa"}}>create_issue</code><span>→ new issue (title, body, labels)</span>
              <code style={{color:"#a78bfa"}}>close_issue</code><span>→ close issue by number</span>
              <code style={{color:"#a78bfa"}}>add_comment</code><span>→ comment on issue</span>
              <code style={{color:"#a78bfa"}}>list_commits</code><span>→ recent commits</span>
              <code style={{color:"#a78bfa"}}>list_prs</code><span>→ open PRs</span>
              <code style={{color:"#a78bfa"}}>get_file</code><span>→ raw file content (needs path)</span>
              <code style={{color:"#a78bfa"}}>create_release</code><span>→ new release (tag, title, body)</span>
            </div>
          </div>
        )}
        {node.data.type === "action.google_sheets" && (
          <div style={{background:"#0f1117",border:"1px solid #2a2d3e",borderRadius:6,padding:"8px 10px",fontSize:10,color:"#94a3b8",marginTop:6,lineHeight:1.9}}>
            <div style={{color:"#a78bfa",fontWeight:600,marginBottom:4}}>📊 Google Sheets Integration</div>
            <div style={{color:"#94a3b8",marginBottom:6}}>Create a <strong>Google Service Account</strong> credential and paste the full SA JSON into the <em>value</em> field. Then set <strong>Google SA Credential</strong> to its name. Share the spreadsheet with the service account email.</div>
            <div style={{display:"grid",gridTemplateColumns:"auto 1fr",gap:"0 10px"}}>
              <code style={{color:"#a78bfa"}}>read_range</code><span>→ {"{ rows: [...], headers?, count }"}</span>
              <code style={{color:"#a78bfa"}}>write_range</code><span>→ overwrite cells with rows_json 2D array</span>
              <code style={{color:"#a78bfa"}}>append_rows</code><span>→ append rows_json below existing data</span>
              <code style={{color:"#a78bfa"}}>clear_range</code><span>→ clear all values in range</span>
            </div>
            <div style={{marginTop:6,color:"#64748b"}}>Range format: <code>Sheet1!A1:D10</code> or just <code>Sheet1</code></div>
          </div>
        )}
        {node.data.type === "action.notion" && (
          <div style={{background:"#0f1117",border:"1px solid #2a2d3e",borderRadius:6,padding:"8px 10px",fontSize:10,color:"#94a3b8",marginTop:6,lineHeight:1.9}}>
            <div style={{color:"#a78bfa",fontWeight:600,marginBottom:4}}>🗒 Notion Integration</div>
            <div style={{color:"#94a3b8",marginBottom:6}}>Create a <strong>Notion Integration Token</strong> credential (API Key type, paste <em>secret_…</em> token), then set <strong>Notion Credential</strong> to its name. Share the database or page with your integration in Notion.</div>
            <div style={{display:"grid",gridTemplateColumns:"auto 1fr",gap:"0 10px"}}>
              <code style={{color:"#a78bfa"}}>query_database</code><span>→ rows with flattened properties</span>
              <code style={{color:"#a78bfa"}}>get_page</code><span>→ page with flattened properties</span>
              <code style={{color:"#a78bfa"}}>create_page</code><span>→ new page in database (properties_json)</span>
              <code style={{color:"#a78bfa"}}>update_page</code><span>→ update page properties</span>
              <code style={{color:"#a78bfa"}}>search</code><span>→ workspace-wide search</span>
              <code style={{color:"#a78bfa"}}>append_blocks</code><span>→ add paragraph to page</span>
            </div>
          </div>
        )}
        {node.data.type === "trigger.cron" && (
          <div style={{background:"#0f1117",border:"1px solid #2a2d3e",borderRadius:6,padding:"8px 10px",fontSize:10,color:"#94a3b8",marginTop:6,lineHeight:1.8}}>
            <div style={{color:"#a78bfa",fontWeight:600,marginBottom:4}}>⏰ Saved automatically as a schedule</div>
            <div style={{display:"grid",gridTemplateColumns:"auto 1fr",gap:"0 10px"}}>
              <span style={{color:"#64748b"}}>Every minute</span><code>* * * * *</code>
              <span style={{color:"#64748b"}}>Every hour</span><code>0 * * * *</code>
              <span style={{color:"#64748b"}}>Daily at 9am</span><code>0 9 * * *</code>
              <span style={{color:"#64748b"}}>Weekdays 9am</span><code>0 9 * * 1-5</code>
              <span style={{color:"#64748b"}}>Every 15 min</span><code>*/15 * * * *</code>
              <span style={{color:"#64748b"}}>1st of month</span><code>0 0 1 * *</code>
            </div>
          </div>
        )}

        {/* Retry Policy (for action nodes only) */}
        {!isNote && !isTrigger && (
          <>
            <div className="section-divider">Retry Policy</div>
            <div className="form-row" style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:12}}>
              <div className="field-group" style={{marginBottom:0}}>
                <label style={{fontSize:11,color:"#94a3b8",fontWeight:500,marginBottom:4}}>Max retries</label>
                <input className="field-input" type="number" min="0" max="5" value={retryMax} onChange={e=>updateRetryMax(e.target.value)}/>
              </div>
              <div className="field-group" style={{marginBottom:0}}>
                <label style={{fontSize:11,color:"#94a3b8",fontWeight:500,marginBottom:4}}>Retry delay (sec)</label>
                <input className="field-input" type="number" min="1" max="60" value={retryDelay} onChange={e=>updateRetryDelay(e.target.value)}/>
              </div>
            </div>
            {/* Fail mode */}
            <div className="field-group" style={{marginTop:10}}>
              <label style={{fontSize:11,color:"#94a3b8",fontWeight:500,marginBottom:4}}>On failure</label>
              <select className="field-input" value={failMode} onChange={e=>updateFailMode(e.target.value)}>
                <option value="abort">abort — stop the graph (default)</option>
                <option value="continue">continue — store error, keep going</option>
              </select>
              {failMode==="continue" && (
                <div style={{fontSize:10,color:"#f59e0b",marginTop:4}}>
                  ⚠ Downstream nodes receive {"{ __error, __node, __type }"} as input. Use a Condition node to check for errors.
                </div>
              )}
            </div>
          </>
        )}

        {/* Merge node hint */}
        {node.data.type === "action.merge" && (
          <div style={{background:"#0f1117",border:"1px solid #2a2d3e",borderRadius:6,padding:"8px 10px",fontSize:10,color:"#94a3b8",marginTop:6,lineHeight:1.9}}>
            <div style={{color:"#a78bfa",fontWeight:600,marginBottom:4}}>⇒ Merge / Join</div>
            Connect multiple upstream nodes to this node to combine their outputs.
            <div style={{display:"grid",gridTemplateColumns:"auto 1fr",gap:"0 10px",marginTop:6}}>
              <code style={{color:"#4ade80"}}>dict</code><span>→ merge all upstream dicts (last wins on collision)</span>
              <code style={{color:"#60a5fa"}}>all</code><span>→ {"{ merged: [...], count: N }"}</span>
              <code style={{color:"#94a3b8"}}>first</code><span>→ pass through the first upstream value</span>
            </div>
          </div>
        )}

        {/* Node I/O Panel — shows last-run input/output; drag fields into config inputs above */}
        {!isNote && (
          <NodeIOPanel
            runInput={runInput}
            runOutput={runOutput}
            runStatus={runStatus}
            runDurationMs={runDurationMs}
            runAttempts={runAttempts}
            nodeId={node.id}
            upstreamNodeId={upstreamNodeId}
            nodeLabel={node.data.label || def.label}
          />
        )}

        <button className="delete-node-btn" onClick={()=>onDelete(node.id)}>🗑 Remove node</button>
      </div>
    </div>
  );
}

// ── Node Context Menu ─────────────────────────────────────────────────────────
function NodeContextMenu({ menu, onClose, onDuplicate, onDelete, onToggleDisabled, onCopyId, onRename }){
  const node = menu.node;
  const isDisabled = !!node.data.disabled;
  const isNote = node.data.type === "note";

  useEffect(()=>{
    const close = (e)=>{ if(!e.target.closest('.ctx-menu')) onClose(); };
    window.addEventListener("mousedown", close, true);
    return ()=>window.removeEventListener("mousedown", close, true);
  },[]);

  // Keep menu within viewport
  const style = { left: Math.min(menu.x, window.innerWidth-190), top: Math.min(menu.y, window.innerHeight-240) };

  return (
    <div className="ctx-menu" style={style}>
      <div className="ctx-label">{(NODE_DEFS[node.data.type]||{}).label||node.data.type}</div>
      <div className="ctx-item" onClick={()=>{onRename(node); onClose()}}>✏ Rename</div>
      <div className="ctx-item" onClick={()=>{onCopyId(node.id); onClose()}}>📋 Copy ID</div>
      <div className="ctx-divider"/>
      <div className="ctx-item" onClick={()=>{onDuplicate(node); onClose()}}>⧉ Duplicate node</div>
      {!isNote && (
        <div className="ctx-item" onClick={()=>{onToggleDisabled(node.id); onClose()}}>
          {isDisabled ? "▶ Enable node" : "⏸ Disable node"}
        </div>
      )}
      <div className="ctx-divider"/>
      <div className="ctx-item danger" onClick={()=>{onDelete(node.id); onClose()}}>🗑 Delete node</div>
    </div>
  );
}

// ── Palette ───────────────────────────────────────────────────────────────────
function Palette({ search, onSearch }){
  function onDragStart(e, type){
    e.dataTransfer.setData("application/reactflow-type", type);
    e.dataTransfer.effectAllowed = "move";
  }
  const q = search.toLowerCase();
  const filtered = Object.entries(NODE_DEFS).filter(([type, def])=>
    !q || def.label.toLowerCase().includes(q) || type.includes(q)
  );

  return (
    <div className="sidebar">
      <div className="sidebar-search">
        <input placeholder="🔍 Search nodes…" value={search} onChange={e=>onSearch(e.target.value)}/>
      </div>
      <div className="sidebar-scroll">
        {GROUPS.map(g => {
          const items = filtered.filter(([,d])=>d.group===g.id);
          if(!items.length) return null;
          return (
            <div key={g.id}>
              <div className="sidebar-title">{g.label}</div>
              {items.map(([type,def]) => (
                <div key={type} className="node-palette-item" draggable
                     onDragStart={e=>onDragStart(e,type)} title={type}>
                  <div className="node-icon" style={{background:def.color+"22",color:def.color}}>{def.icon}</div>
                  <div>
                    <div className="node-label">{def.label}</div>
                    <div className="node-sublabel">{type}</div>
                  </div>
                </div>
              ))}
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ── Open/New modal ────────────────────────────────────────────────────────────
const TEMPLATE_EMOJIS = /^[📧🤖🔄⚠🔍📊⚡🧪🐍⏰💻📁]/u;
function isTemplate(name){ return TEMPLATE_EMOJIS.test(name) }

function OpenModal({ graphs, onClose, onSelect, onNew, onDuplicate, onDelete, onRename }){
  const [name, setName]         = useState("");
  const [search, setSearch]     = useState("");
  const [busyId, setBusyId]     = useState(null);
  const q = search.toLowerCase();
  const allFiltered = q ? graphs.filter(g=>g.name.toLowerCase().includes(q)||(g.description||"").toLowerCase().includes(q)) : graphs;
  const templates  = allFiltered.filter(g => isTemplate(g.name));
  const userFlows  = allFiltered.filter(g => !isTemplate(g.name));

  async function handleDuplicate(e, g){
    e.stopPropagation();
    setBusyId(g.id);
    await onDuplicate(g);
    setBusyId(null);
  }
  async function handleDelete(e, g){
    e.stopPropagation();
    const msg = isTemplate(g.name)
      ? `Delete example "${g.name}"?\nYou can restore it via ↺ Restore examples.\nThis cannot be undone.`
      : `Delete "${g.name}"?\nThis cannot be undone.`;
    if(!window.confirm(msg)) return;
    setBusyId(g.id);
    await onDelete(g.id, g.name);
    setBusyId(null);
  }

  async function handleRename(e, g){
    e.stopPropagation();
    await onRename(g);
  }

  function nodeCount(g){ return ((g.graph_data?.nodes)||[]).filter(n=>n.type!=="note").length }

  function renderRow(g, isTempl){
    const busy = busyId === g.id;
    const nc = nodeCount(g);
    return (
      <div key={g.id} className="graph-row" onClick={()=>{ if(!busy) onSelect(g) }}>
        <div style={{flex:1,minWidth:0}}>
          <div style={{display:"flex",alignItems:"center",gap:6,marginBottom:2}}>
            <span className="graph-row-name">{g.name}</span>
            {isTempl && <span className="tmpl-badge">EXAMPLE</span>}
          </div>
          <div style={{display:"flex",gap:8,alignItems:"center"}}>
            {g.description && <div className="graph-row-desc">{g.description.slice(0,60)}{g.description.length>60?"…":""}</div>}
            <span style={{fontSize:10,color:"#4b5563",flexShrink:0}}>{nc} node{nc!==1?"s":""}</span>
          </div>
        </div>
        <span className="graph-row-badge" style={{color:g.enabled?"#4ade80":"#f87171",marginRight:6}}>
          {g.enabled?"●":"○"}
        </span>
        <div className="graph-row-actions">
          <button className="btn-icon" title="Rename" disabled={busy} onClick={e=>handleRename(e,g)}>✏</button>
          <button className="btn-icon" title="Duplicate" disabled={busy} onClick={e=>handleDuplicate(e,g)}>📋</button>
          <button className="btn-icon danger" title="Delete" disabled={busy} onClick={e=>handleDelete(e,g)}>🗑</button>
        </div>
      </div>
    );
  }

  return (
    <div className="modal-overlay" onClick={e=>{ if(e.target.className==="modal-overlay") onClose() }}>
      <div className="modal">
        <h2>📂 Flows</h2>

        {/* ── Create new ── */}
        <div className="field-group" style={{marginBottom:10}}>
          <div className="field-label">New flow</div>
          <div style={{display:"flex",gap:8}}>
            <input className="field-input" placeholder="Flow name…" value={name} onChange={e=>setName(e.target.value)}
              onKeyDown={e=>{ if(e.key==="Enter"&&name.trim()) onNew(name.trim()) }} style={{flex:1}}/>
            <button className="btn btn-primary btn-sm" disabled={!name.trim()} onClick={()=>onNew(name.trim())}>Create</button>
          </div>
        </div>

        {/* ── Search ── */}
        <div className="field-group" style={{marginBottom:6}}>
          <input className="field-input" placeholder="🔍 Search flows…" value={search} onChange={e=>setSearch(e.target.value)}/>
        </div>

        {/* ── Your Flows ── */}
        <div className="modal-section">⚡ Your Flows{userFlows.length > 0 ? ` (${userFlows.length})` : ""}</div>
        {userFlows.length === 0 && !q
          ? <div className="your-flows-empty">No flows yet — create one above or duplicate an example below.</div>
          : userFlows.length === 0 && q
          ? <div className="your-flows-empty">No matching flows.</div>
          : userFlows.map(g=>renderRow(g, false))
        }

        {/* ── Examples ── */}
        {templates.length > 0 && <>
          <div className="modal-section templates">📚 Examples{templates.length > 0 ? ` (${templates.length})` : ""}</div>
          <div style={{fontSize:11,color:"#64748b",marginBottom:8}}>
            Click to open, ✏ rename, or 📋 duplicate to make your own copy.
          </div>
          {templates.map(g=>renderRow(g, true))}
        </>}

        <div className="modal-btns" style={{marginTop:10}}>
          <button className="btn btn-ghost btn-sm" onClick={onClose}>Close</button>
        </div>
      </div>
    </div>
  );
}

// ── Test Payload Modal ─────────────────────────────────────────────────────────
function TestPayloadModal({ isOpen, onClose, onRun, testPayload, onPayloadChange }) {
  if (!isOpen) return null;
  return (
    <div className="modal-overlay" onClick={e=>{ if(e.target.className==="modal-overlay") onClose() }}>
      <div className="modal">
        <h2>🧪 Test Payload</h2>
        <div className="field-group">
          <label style={{fontSize:11,color:"#94a3b8",fontWeight:500,marginBottom:4}}>JSON</label>
          <textarea className="field-input mono" rows={8} value={testPayload} onChange={e=>onPayloadChange(e.target.value)} placeholder='{"key":"value"}' />
        </div>
        <div className="modal-btns">
          <button className="btn btn-ghost" onClick={onClose}>Cancel</button>
          <button className="btn btn-success" onClick={()=>{onRun(testPayload); onClose()}}>Run with payload</button>
        </div>
      </div>
    </div>
  );
}

// ── History Modal ──────────────────────────────────────────────────────────────
function HistoryModal({ isOpen, onClose, graphId, showToast }) {
  const [versions, setVersions] = useState([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!isOpen || !graphId) return;
    async function load() {
      setLoading(true);
      try {
        const v = await api("GET", `/api/graphs/${graphId}/versions`);
        setVersions(v);
      } catch (e) {
        showToast(e.message, "error");
      }
      setLoading(false);
    }
    load();
  }, [isOpen, graphId]);

  async function restore(vid) {
    if (!window.confirm("Restore this version? Current changes will be lost.")) return;
    try {
      await api("POST", `/api/graphs/${graphId}/versions/${vid}/restore`);
      showToast("Version restored. Reloading…");
      setTimeout(() => window.location.reload(), 500);
    } catch (e) {
      showToast(e.message, "error");
    }
  }

  if (!isOpen) return null;
  return (
    <div className="modal-overlay" onClick={e=>{ if(e.target.className==="modal-overlay") onClose() }}>
      <div className="modal">
        <h2>📜 Version History</h2>
        {loading ? (
          <div style={{textAlign:"center",color:"#64748b",padding:"20px"}}>Loading…</div>
        ) : versions.length === 0 ? (
          <div style={{textAlign:"center",color:"#64748b",padding:"20px"}}>No versions yet.</div>
        ) : (
          <div style={{marginTop:12}}>
            {versions.map((v,idx)=>(
              <div key={idx} className="version-row">
                <div className="version-info">
                  <div className="version-num">Version {v.version}</div>
                  <div className="version-date">{new Date(v.saved_at).toLocaleString()}</div>
                  {v.note && <div className="version-note">{v.note}</div>}
                </div>
                <button className="btn btn-ghost btn-sm" onClick={()=>restore(v.version)}>Restore</button>
              </div>
            ))}
          </div>
        )}
        <div className="modal-btns">
          <button className="btn btn-ghost" onClick={onClose}>Close</button>
        </div>
      </div>
    </div>
  );
}

// ── Edge Label Modal ───────────────────────────────────────────────────────────
function EdgeLabelModal({ edge, value, onChange, onConfirm, onClose }) {
  if (!edge) return null;
  return (
    <div className="modal-overlay" onClick={e=>{ if(e.target.className==="modal-overlay") onClose() }}>
      <div className="modal" style={{minWidth:300}}>
        <h2>✏ Edge Label</h2>
        <div className="field-group">
          <input className="field-input" placeholder="Label (leave blank to remove)"
            value={value} onChange={e=>onChange(e.target.value)}
            onKeyDown={e=>{ if(e.key==="Enter") onConfirm(value); if(e.key==="Escape") onClose(); }}
            autoFocus/>
        </div>
        <div className="modal-btns">
          <button className="btn btn-ghost" onClick={onClose}>Cancel</button>
          <button className="btn btn-primary" onClick={()=>onConfirm(value)}>Set label</button>
          {edge.label && <button className="btn btn-danger" onClick={()=>onConfirm("")}>Remove</button>}
        </div>
      </div>
    </div>
  );
}

// ── Validation Modal ───────────────────────────────────────────────────────────
function ValidationModal({ issues, onClose, onRunAnyway }) {
  if (!issues) return null;
  const errors = issues.filter(i=>i.level==="error");
  const warnings = issues.filter(i=>i.level==="warning");
  return (
    <div className="modal-overlay" onClick={e=>{ if(e.target.className==="modal-overlay") onClose() }}>
      <div className="modal" style={{minWidth:420}}>
        <h2>{issues.length===0 ? "✅ Flow looks good" : errors.length?"❌ Validation errors":"⚠ Validation warnings"}</h2>
        {issues.length===0 ? (
          <p style={{color:"#94a3b8",fontSize:13,marginBottom:12}}>No issues found. The flow is ready to run.</p>
        ) : (
          <div style={{marginBottom:12}}>
            {issues.map((iss,i)=>(
              <div key={i} style={{display:"flex",gap:8,alignItems:"flex-start",padding:"6px 0",borderBottom:"1px solid #2a2d3e",fontSize:12}}>
                <span style={{color:iss.level==="error"?"#f87171":"#fbbf24",flexShrink:0}}>{iss.level==="error"?"✗":"⚠"}</span>
                <span style={{color:"#cbd5e1"}}>{iss.msg}</span>
              </div>
            ))}
          </div>
        )}
        <div className="modal-btns">
          <button className="btn btn-ghost" onClick={onClose}>Close</button>
          {issues.length===0
            ? <button className="btn btn-success" onClick={onRunAnyway}>▶ Run now</button>
            : errors.length===0 && <button className="btn btn-ghost" onClick={onRunAnyway}>Run anyway</button>
          }
        </div>
      </div>
    </div>
  );
}

// ── Auto-layout function ──────────────────────────────────────────────────────
function computeAutoLayout(nodes, edges) {
  const ids = nodes.map(n => n.id);
  const indeg = Object.fromEntries(ids.map(id => [id, 0]));
  const succ  = Object.fromEntries(ids.map(id => [id, []]));
  edges.forEach(e => {
    if (succ[e.source] !== undefined && indeg[e.target] !== undefined) {
      succ[e.source].push(e.target);
      indeg[e.target]++;
    }
  });
  // BFS depth
  const depth = Object.fromEntries(ids.map(id => [id, 0]));
  const visited = new Set();
  const queue = ids.filter(id => indeg[id] === 0);
  while (queue.length) {
    const n = queue.shift();
    if (visited.has(n)) continue;
    visited.add(n);
    (succ[n]||[]).forEach(nb => {
      depth[nb] = Math.max(depth[nb], depth[n] + 1);
      indeg[nb]--;
      if (indeg[nb] === 0) queue.push(nb);
    });
  }
  // Group by column
  const cols = {};
  ids.forEach(id => {
    const d = depth[id] || 0;
    if (!cols[d]) cols[d] = [];
    cols[d].push(id);
  });
  const COL_W = 265, ROW_H = 130, PAD_X = 60, PAD_Y = 80;
  const newPos = {};
  Object.entries(cols).forEach(([col, nodeIds]) => {
    const c = parseInt(col);
    nodeIds.forEach((id, row) => {
      newPos[id] = { x: PAD_X + c * COL_W, y: PAD_Y + row * ROW_H };
    });
  });
  return nodes.map(n => ({ ...n, position: newPos[n.id] || n.position }));
}

// ── Flow validation function ──────────────────────────────────────────────────
const REQUIRED_FIELDS = {
  "trigger.cron":        ["cron"],
  "action.http_request": ["url"],
  "action.send_email":   ["to","subject"],
  "action.ssh":          ["host","command"],
  "action.sftp":         ["host","remote_path"],
  "action.call_graph":   ["graph_id"],
  "action.slack":        ["webhook_url","message"],
  "action.llm_call":     ["prompt"],
  "action.run_script":   ["script"],
  "action.transform":    ["expression"],
  "action.condition":    ["expression"],
  "action.github":       ["owner","repo","action"],
  "action.google_sheets":["spreadsheet_id","range","action"],
  "action.notion":       ["action"],
};

function validateFlow(nodes, edges, credentials) {
  const issues = [];
  const triggers = nodes.filter(n => n.data.type?.startsWith("trigger.") && !n.data.disabled);
  if (triggers.length === 0)
    issues.push({level:"error", msg:"No trigger node — the flow can't start"});

  const connectedIds = new Set([...edges.map(e=>e.source), ...edges.map(e=>e.target)]);
  const credNames = new Set((credentials||[]).map(c=>c.name));

  nodes.forEach(node => {
    if (node.data.type==="note" || node.data.disabled) return;
    const label = node.data.label || node.data.type || node.id;
    const isTrigger = node.data.type?.startsWith("trigger.");
    const cfg = node.data.config || {};

    // Disconnected non-trigger nodes
    if (!isTrigger && !connectedIds.has(node.id))
      issues.push({level:"warning", msg:`"${label}" is not connected to anything`});

    // Required fields
    (REQUIRED_FIELDS[node.data.type]||[]).forEach(f => {
      if (!cfg[f] || !String(cfg[f]).trim())
        issues.push({level:"warning", msg:`"${label}" — required field "${f}" is empty`});
    });

    // Missing creds
    const configStr = JSON.stringify(cfg);
    [...configStr.matchAll(/\{\{creds\.([^}]+)\}\}/g)].forEach(m => {
      const name = m[1].trim();
      if (!credNames.has(name))
        issues.push({level:"warning", msg:`"${label}" references missing credential "{{creds.${name}}}"`});
    });
  });
  return issues;
}

// ── Variable autocomplete helpers ─────────────────────────────────────────────
// Recursively flatten object keys to dot-notation paths, max 3 levels deep
function flattenVarPaths(obj, nodeId, nodeLabel, prefix, depth){
  prefix = prefix || ""; depth = depth || 0;
  const results = [];
  if(!obj || typeof obj !== "object" || depth > 3) return results;
  if(Array.isArray(obj)){
    // For arrays, expose [0] and [1] as samples
    obj.slice(0,2).forEach((item,i)=>{
      const p = prefix ? `${prefix}[${i}]` : `[${i}]`;
      results.push({ template:`{{${nodeId}.${p}}}`, label:p, nodeLabel });
      results.push(...flattenVarPaths(item, nodeId, nodeLabel, p, depth+1));
    });
    return results;
  }
  for(const k of Object.keys(obj)){
    if(k.startsWith("__")) continue; // skip internal fields like __truncated
    const p = prefix ? `${prefix}.${k}` : k;
    results.push({ template:`{{${nodeId}.${p}}}`, label:p, nodeLabel });
    if(obj[k] && typeof obj[k] === "object" && !Array.isArray(obj[k])){
      results.push(...flattenVarPaths(obj[k], nodeId, nodeLabel, p, depth+1));
    }
  }
  return results;
}

// Build the full variable list for a node given its upstream nodes
function buildVarList(targetNodeId, allNodes, edges, credentials){
  const vars = [];
  // Upstream node outputs
  const upstreamIds = edges.filter(e=>e.target===targetNodeId).map(e=>e.source);
  for(const upId of upstreamIds){
    const upNode = allNodes.find(n=>n.id===upId);
    if(!upNode) continue;
    const nodeLabel = upNode.data.label || (NODE_DEFS[upNode.data.type]||{}).label || upId;
    const output = upNode.data._runOutput;
    if(output && !output.__truncated && typeof output === "object"){
      const paths = flattenVarPaths(output, upId, nodeLabel, "", 0);
      vars.push(...paths);
      if(!paths.length){
        // Output exists but is empty — still add the root ref
        vars.push({ template:`{{${upId}}}`, label:"(output)", nodeLabel });
      }
    } else {
      // No run data yet — expose just the node reference so user knows it exists
      vars.push({ template:`{{${upId}}}`, label:"(run flow to see fields)", nodeLabel, dimmed:true });
    }
  }
  // Credential references
  if(credentials && credentials.length){
    for(const c of credentials){
      vars.push({ template:`{{creds.${c.name}}}`, label:`creds.${c.name}`, nodeLabel:"Credentials", isCred:true });
    }
  }
  return vars;
}

// VarField — input or textarea with {{ autocomplete
function VarField({ multiline, value, onChangeValue, vars, className, placeholder, type, rows, ...rest }){
  const [show, setShow]     = useState(false);
  const [filter, setFilter] = useState("");
  const [cursor, setCursor] = useState(0);
  const elRef = useRef(null);

  // Detect {{ trigger after any keystroke
  function detectTrigger(el){
    const pos = el.selectionStart || 0;
    const before = (el.value || "").slice(0, pos);
    const ddIdx  = before.lastIndexOf("{{");
    if(ddIdx !== -1 && !before.slice(ddIdx).includes("}}")){
      setFilter(before.slice(ddIdx + 2).toLowerCase());
      setShow(true);
      setCursor(0);
    } else {
      setShow(false);
    }
  }

  function handleChange(e){
    onChangeValue(e.target.value);
    detectTrigger(e.target);
  }

  function handleKeyDown(e){
    if(!show || !filtered.length) return;
    if(e.key === "ArrowDown"){ e.preventDefault(); setCursor(c=>Math.min(c+1, filtered.length-1)); }
    else if(e.key === "ArrowUp"){ e.preventDefault(); setCursor(c=>Math.max(c-1,0)); }
    else if(e.key === "Enter"){ e.preventDefault(); insertVar(filtered[cursor]); }
    else if(e.key === "Escape"){ e.preventDefault(); setShow(false); }
  }

  function insertVar(v){
    const el = elRef.current;
    if(!el) return;
    const pos = el.selectionStart || 0;
    const val = value || "";
    const before = val.slice(0, pos);
    const after  = val.slice(pos);
    const ddIdx  = before.lastIndexOf("{{");
    const newVal = before.slice(0, ddIdx) + v.template + after;
    onChangeValue(newVal);
    setShow(false);
    setTimeout(()=>{
      try{
        const newPos = ddIdx + v.template.length;
        el.selectionStart = el.selectionEnd = newPos;
        el.focus();
      }catch(e){}
    }, 0);
  }

  const filtered = vars.filter(v=>{
    if(!filter) return true;
    return v.label.toLowerCase().includes(filter) || v.template.toLowerCase().includes(filter) || (v.nodeLabel||"").toLowerCase().includes(filter);
  }).slice(0,30);

  // Group by nodeLabel for the dropdown
  const groups = [];
  const seen = new Map();
  for(const v of filtered){
    const g = v.isCred ? "Credentials" : (v.nodeLabel || "Upstream");
    if(!seen.has(g)){ seen.set(g, []); groups.push({ label:g, items:[] }); }
    seen.get(g).push(v);
    groups.find(x=>x.label===g).items.push(v);
  }

  const sharedProps = {
    ref: elRef,
    className: className || "field-input",
    placeholder,
    value: value || "",
    onChange: handleChange,
    onKeyDown: handleKeyDown,
    onBlur: ()=>setTimeout(()=>setShow(false), 180),
    onClick: e=>detectTrigger(e.target),
    ...rest
  };

  return (
    <div className="var-wrap">
      {multiline
        ? <textarea {...sharedProps} rows={rows||4}/>
        : <input {...sharedProps} type={type||"text"}/>
      }
      {!show && <span className="var-trigger-hint">type {"{{"}  for vars</span>}
      {show && filtered.length > 0 && (
        <div className="var-dropdown">
          {groups.map((g,gi)=>(
            <div key={gi}>
              <div className="var-dropdown-section">{g.label}</div>
              {g.items.map((v,i)=>{
                const globalIdx = filtered.indexOf(v);
                return (
                  <div key={v.template} className={`var-dropdown-item${globalIdx===cursor?" active":""}`}
                    onMouseDown={e=>{ e.preventDefault(); insertVar(v); }}>
                    <span className="var-dropdown-label" style={v.dimmed?{color:"#475569"}:{}}>{v.label}</span>
                    <span className="var-dropdown-tmpl">{v.template}</span>
                  </div>
                );
              })}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ── Node Editor Modal (n8n-style) ─────────────────────────────────────────────
function NodeEditorModal({ node, onChange, onDelete, onClose, edges, allNodes, credentials }){
  const isNote    = node.data.type === "note";
  const isTrigger = node.data.type?.startsWith("trigger.");
  const def       = NODE_DEFS[node.data.type] || { label:node.data.type, icon:"?", color:"#475569", fields:[] };
  const isDisabled  = !!node.data.disabled;
  const runStatus   = node.data._runStatus;
  const runOutput   = node.data._runOutput;
  const runInput    = node.data._runInput;
  const runDurationMs = node.data._runDurationMs;
  const runAttempts = node.data._runAttempts;
  const retryMax    = node.data.retry_max ?? 0;
  const retryDelay  = node.data.retry_delay ?? 5;
  const failMode    = node.data.fail_mode || "abort";

  const upstreamNodeId = React.useMemo(()=>{
    if(!edges||!node) return null;
    const e = edges.find(e=>e.target===node.id);
    return e ? e.source : null;
  },[edges, node&&node.id]);

  // Build variable list for autocomplete
  const vars = React.useMemo(()=>
    buildVarList(node.id, allNodes||[], edges||[], credentials||[])
  ,[node.id, allNodes, edges, credentials]);

  const [inputView,  setInputView]  = useState("schema");
  const [outputView, setOutputView] = useState("schema");

  function update(key,val){ onChange(node.id,{...node.data,config:{...node.data.config,[key]:val}}); }
  function updateLabel(val){ onChange(node.id,{...node.data,label:val}); }
  function updateRetryMax(val){ onChange(node.id,{...node.data,retry_max:parseInt(val)||0}); }
  function updateRetryDelay(val){ onChange(node.id,{...node.data,retry_delay:parseInt(val)||5}); }
  function updateFailMode(val){ onChange(node.id,{...node.data,fail_mode:val}); }
  function toggleDisabled(){ onChange(node.id,{...node.data,disabled:!isDisabled}); }
  function copyId(){ navigator.clipboard.writeText(node.id).catch(()=>{}); }

  function dropProps(fieldKey, currentVal){
    return {
      onDragOver: e=>{ e.preventDefault(); e.currentTarget.classList.add("drag-over"); },
      onDragLeave: e=>e.currentTarget.classList.remove("drag-over"),
      onDrop: e=>{
        e.preventDefault();
        e.currentTarget.classList.remove("drag-over");
        const tmpl = e.dataTransfer.getData("text/plain");
        if(!tmpl) return;
        const el = e.currentTarget;
        const start = el.selectionStart ?? (currentVal||"").length;
        const end   = el.selectionEnd ?? start;
        const newVal = (currentVal||"").slice(0,start)+tmpl+(currentVal||"").slice(end);
        update(fieldKey, newVal);
        setTimeout(()=>{ try{ el.selectionStart=el.selectionEnd=start+tmpl.length; }catch(e){} },0);
      }
    };
  }

  useEffect(()=>{
    const h=e=>{ if(e.key==="Escape") onClose(); };
    window.addEventListener("keydown",h);
    return ()=>window.removeEventListener("keydown",h);
  },[]);

  const statusLabel = runStatus==="ok"?"Succeeded":runStatus==="err"?"Failed":runStatus==="skip"?"Skipped":null;
  const statusCls   = runStatus==="ok"?"run-status-ok":runStatus==="err"?"run-status-err":"run-status-skip";

  return (
    <div className="nem-overlay">
      <div className="nem-backdrop" onClick={onClose}/>
      <div className="nem-panel">

        {/* Header */}
        <div className="nem-hdr">
          <span className="nem-hdr-icon">{def.icon}</span>
          <span className="nem-hdr-title">{node.data.label||def.label}</span>
          {statusLabel && <span className={`run-output-status ${statusCls}`} style={{fontSize:10,flexShrink:0}}>{statusLabel}</span>}
          {runDurationMs!==undefined && <span style={{fontSize:10,color:"#64748b",flexShrink:0}}>{runDurationMs}ms</span>}
          <button className="nem-hdr-close" onClick={onClose}>✕</button>
        </div>

        {/* Three columns */}
        <div className="nem-columns">

          {/* ── LEFT: INPUT ── */}
          <div className="nem-io-col left">
            <div className="nem-io-col-hdr">
              ← Input
              {upstreamNodeId && <span className="nem-drag-hint">drag → config field</span>}
            </div>
            <div className="nem-io-view-tabs">
              <button className={`nem-io-view-tab${inputView==="schema"?" active":""}`} onClick={()=>setInputView("schema")}>Schema</button>
              <button className={`nem-io-view-tab${inputView==="json"?" active":""}`} onClick={()=>setInputView("json")}>JSON</button>
            </div>
            <div className="nem-io-col-body">
              <NioBody data={runInput} view={inputView} sourceNodeId={upstreamNodeId} isTruncated={!!(runInput&&runInput.__truncated)}/>
            </div>
          </div>

          {/* ── CENTER: CONFIG ── */}
          <div className="nem-cfg-col">

            {/* Node ID */}
            <div className="node-id-badge" onClick={copyId} title="Click to copy" style={{marginBottom:12}}>
              <div>
                <div style={{fontSize:10,color:"#64748b",marginBottom:2}}>NODE ID (for templates)</div>
                <code>{node.id}</code>
              </div>
              <span className="copy-hint">📋 copy</span>
            </div>

            {/* Enable toggle */}
            {!isNote && (
              <div className="disable-toggle-row">
                <span className="disable-toggle-label">{isDisabled?"⏸ Node disabled":"▶ Node enabled"}</span>
                <label className="toggle-switch">
                  <input type="checkbox" checked={!isDisabled} onChange={toggleDisabled}/>
                  <div className="toggle-track"/>
                  <div className="toggle-thumb"/>
                </label>
              </div>
            )}

            {/* Label */}
            {!isNote && (<>
              <div className="section-divider">Label</div>
              <div className="field-group">
                <input className="field-input" placeholder={def.label}
                  value={node.data.label||""} onChange={e=>updateLabel(e.target.value)}/>
              </div>
            </>)}

            {/* Config fields */}
            {def.fields.length>0 && <div className="section-divider">Config</div>}
            {def.fields.map(f=>{
              const cfgVal=(node.data.config||{})[f.k]||"";
              const dp=(!f.secret&&f.type!=="select")?dropProps(f.k,cfgVal):{};
              const useAutocomplete = !f.secret && f.type!=="select";
              return (
                <div className="field-group" key={f.k}>
                  <div className="field-label">
                    {f.l}
                    {f.secret&&<span className="field-hint">🔒 sensitive</span>}
                  </div>
                  {f.type==="select"?(
                    <select className="field-input" value={cfgVal||f.options[0]} onChange={e=>update(f.k,e.target.value)}>
                      {f.options.map(o=><option key={o} value={o}>{o}</option>)}
                    </select>
                  ):useAutocomplete?(
                    <VarField
                      multiline={!!f.textarea}
                      rows={4}
                      className={`field-input${f.mono?" mono":""}`}
                      placeholder={f.ph}
                      value={cfgVal}
                      onChangeValue={v=>update(f.k,v)}
                      vars={vars}
                      {...dp}
                    />
                  ):(
                    <input className={`field-input${f.mono?" mono":""}`} placeholder={f.ph}
                      type="password"
                      value={cfgVal} onChange={e=>update(f.k,e.target.value)}/>
                  )}
                  {f.secret&&<span className="secret-hint">💡 Or use {"{{creds.your-credential-name}}"}</span>}
                </div>
              );
            })}

            {/* Type hints */}
            {node.data.type==="action.condition"&&(
              <div style={{background:"#0f1117",border:"1px solid #2a2d3e",borderRadius:6,padding:"8px 10px",fontSize:10,color:"#94a3b8",marginTop:6,lineHeight:1.8}}>
                Expression must evaluate to a Python bool.<br/>
                <div style={{color:"#059669",marginTop:4}}>● True handle → nodes that run when condition is met</div>
                <div style={{color:"#dc2626"}}>● False handle → nodes that run when condition is not met</div>
                <div style={{marginTop:4}}>Output: <code style={{color:"#a78bfa"}}>{"{ result: bool, input: ... }"}</code></div>
              </div>
            )}
            {node.data.type==="action.loop"&&(
              <div style={{background:"#0f1117",border:"1px solid #2a2d3e",borderRadius:6,padding:"8px 10px",fontSize:10,color:"#94a3b8",marginTop:6}}>
                <div style={{color:"#7e22ce",marginBottom:3}}>● Body → runs per item</div>
                <div style={{color:"#64748b",marginBottom:4}}>● Done → after all iterations</div>
                Each body node receives the current item as input.
              </div>
            )}
            {node.data.type==="action.filter"&&(
              <div style={{background:"#0f1117",border:"1px solid #2a2d3e",borderRadius:6,padding:"8px 10px",fontSize:10,color:"#94a3b8",marginTop:6}}>
                Output: <code style={{color:"#a78bfa"}}>{"{ items: [...], count: N }"}</code><br/>
                Use <code style={{color:"#a78bfa"}}>item</code> in the expression.
              </div>
            )}
            {node.data.type==="action.llm_call"&&(
              <div style={{background:"#0f1117",border:"1px solid #2a2d3e",borderRadius:6,padding:"8px 10px",fontSize:10,color:"#94a3b8",marginTop:6}}>
                Output: <code style={{color:"#a78bfa"}}>{"{ response, model, tokens }"}</code><br/>
                Set <code style={{color:"#a78bfa"}}>api_base</code> for Groq, Together AI, Ollama etc.
              </div>
            )}
            {node.data.type==="action.slack"&&(
              <div style={{background:"#0f1117",border:"1px solid #2a2d3e",borderRadius:6,padding:"8px 10px",fontSize:10,color:"#94a3b8",marginTop:6,lineHeight:1.9}}>
                <div style={{color:"#a78bfa",fontWeight:600,marginBottom:4}}>💡 Credential shortcut</div>
                Create a <strong>Slack Incoming Webhook</strong> credential → set <strong>Slack Credential</strong> to its name.<br/>
                Output: <code style={{color:"#a78bfa"}}>{"{ sent: true, message }"}</code>
              </div>
            )}
            {node.data.type==="action.call_graph"&&(
              <div style={{background:"#0f1117",border:"1px solid #2a2d3e",borderRadius:6,padding:"8px 10px",fontSize:10,color:"#94a3b8",marginTop:6}}>
                Runs another flow as a sub-routine.<br/>Output: the sub-flow's final context dict.<br/>
                Max nesting depth: <code style={{color:"#a78bfa"}}>5</code> levels.
              </div>
            )}
            {node.data.type==="action.run_script"&&(
              <div style={{background:"#0f1117",border:"1px solid #2a2d3e",borderRadius:6,padding:"8px 10px",fontSize:10,color:"#94a3b8",marginTop:6}}>
                Variables: <code style={{color:"#a78bfa"}}>input</code>, <code style={{color:"#a78bfa"}}>context</code>, <code style={{color:"#a78bfa"}}>log</code>, <code style={{color:"#a78bfa"}}>json</code>, <code style={{color:"#a78bfa"}}>os</code>, <code style={{color:"#a78bfa"}}>time</code><br/>
                Assign output to <code style={{color:"#a78bfa"}}>result</code>.
              </div>
            )}
            {node.data.type==="action.send_email"&&(
              <div style={{background:"#0f1117",border:"1px solid #2a2d3e",borderRadius:6,padding:"8px 10px",fontSize:10,color:"#94a3b8",marginTop:6,lineHeight:1.9}}>
                <div style={{color:"#a78bfa",fontWeight:600,marginBottom:4}}>💡 Credential shortcut</div>
                Create an <strong>SMTP Server</strong> credential → set <strong>SMTP Credential</strong> to its name.
              </div>
            )}
            {node.data.type==="action.ssh"&&(
              <div style={{background:"#0f1117",border:"1px solid #2a2d3e",borderRadius:6,padding:"8px 10px",fontSize:10,color:"#94a3b8",marginTop:6,lineHeight:1.9}}>
                <div style={{color:"#a78bfa",fontWeight:600,marginBottom:4}}>💡 Credential shortcut</div>
                Create an <strong>SSH Server</strong> credential → set <strong>SSH Credential</strong> to its name.<br/>
                Output: <code style={{color:"#a78bfa"}}>{"{ stdout, stderr, exit_code, success }"}</code>
              </div>
            )}
            {node.data.type==="action.sftp"&&(
              <div style={{background:"#0f1117",border:"1px solid #2a2d3e",borderRadius:6,padding:"8px 10px",fontSize:10,color:"#94a3b8",marginTop:6,lineHeight:1.9}}>
                <div style={{color:"#a78bfa",fontWeight:600,marginBottom:4}}>📁 SFTP / FTP Operations</div>
                Create an <strong>SFTP / FTP Server</strong> credential → set <strong>SFTP Credential</strong> to its name.<br/>
                <div style={{display:"grid",gridTemplateColumns:"auto 1fr",gap:"0 10px",marginTop:6}}>
                  <code style={{color:"#a78bfa"}}>list</code><span>→ files[] — set <em>recursive: true</em> to walk subdirs</span>
                  <code style={{color:"#a78bfa"}}>upload</code><span>→ put <em>content</em> at remote_path</span>
                  <code style={{color:"#a78bfa"}}>download</code><span>→ {"{ content, size }"}</span>
                  <code style={{color:"#a78bfa"}}>delete</code><span>→ remove file</span>
                  <code style={{color:"#a78bfa"}}>mkdir</code><span>→ create directory</span>
                  <code style={{color:"#a78bfa"}}>rename</code><span>→ move/rename (set <em>new_path</em>)</span>
                  <code style={{color:"#a78bfa"}}>exists</code><span>→ {"{ exists, is_dir }"}</span>
                  <code style={{color:"#a78bfa"}}>stat</code><span>→ {"{ size, is_dir, modified }"}</span>
                </div>
              </div>
            )}
            {node.data.type==="action.github"&&(
              <div style={{background:"#0f1117",border:"1px solid #2a2d3e",borderRadius:6,padding:"8px 10px",fontSize:10,color:"#94a3b8",marginTop:6,lineHeight:1.9}}>
                <div style={{color:"#a78bfa",fontWeight:600,marginBottom:4}}>🐙 GitHub</div>
                Create a <strong>GitHub Token</strong> credential (API Key type, paste PAT) → set <strong>GitHub Credential</strong>.<br/>
                <div style={{display:"grid",gridTemplateColumns:"auto 1fr",gap:"0 10px",marginTop:4}}>
                  <code style={{color:"#a78bfa"}}>get_repo</code><span>→ repo details</span>
                  <code style={{color:"#a78bfa"}}>list_issues</code><span>→ open issues</span>
                  <code style={{color:"#a78bfa"}}>create_issue</code><span>→ new issue</span>
                  <code style={{color:"#a78bfa"}}>add_comment</code><span>→ comment on issue</span>
                  <code style={{color:"#a78bfa"}}>list_commits</code><span>→ recent commits</span>
                  <code style={{color:"#a78bfa"}}>get_file</code><span>→ raw file content</span>
                  <code style={{color:"#a78bfa"}}>create_release</code><span>→ new release</span>
                </div>
              </div>
            )}
            {node.data.type==="action.google_sheets"&&(
              <div style={{background:"#0f1117",border:"1px solid #2a2d3e",borderRadius:6,padding:"8px 10px",fontSize:10,color:"#94a3b8",marginTop:6,lineHeight:1.9}}>
                <div style={{color:"#a78bfa",fontWeight:600,marginBottom:4}}>📊 Google Sheets</div>
                Service Account credential → set <strong>Google SA Credential</strong>.<br/>
                <div style={{display:"grid",gridTemplateColumns:"auto 1fr",gap:"0 10px",marginTop:4}}>
                  <code style={{color:"#a78bfa"}}>read_range</code><span>→ rows[]</span>
                  <code style={{color:"#a78bfa"}}>write_range</code><span>→ overwrite range</span>
                  <code style={{color:"#a78bfa"}}>append_rows</code><span>→ append below data</span>
                  <code style={{color:"#a78bfa"}}>clear_range</code><span>→ clear range</span>
                </div>
              </div>
            )}
            {node.data.type==="action.notion"&&(
              <div style={{background:"#0f1117",border:"1px solid #2a2d3e",borderRadius:6,padding:"8px 10px",fontSize:10,color:"#94a3b8",marginTop:6,lineHeight:1.9}}>
                <div style={{color:"#a78bfa",fontWeight:600,marginBottom:4}}>🗒 Notion</div>
                Integration Token credential → set <strong>Notion Credential</strong>.<br/>
                <div style={{display:"grid",gridTemplateColumns:"auto 1fr",gap:"0 10px",marginTop:4}}>
                  <code style={{color:"#a78bfa"}}>query_database</code><span>→ rows[]</span>
                  <code style={{color:"#a78bfa"}}>get_page</code><span>→ page properties</span>
                  <code style={{color:"#a78bfa"}}>create_page</code><span>→ new page</span>
                  <code style={{color:"#a78bfa"}}>update_page</code><span>→ update properties</span>
                  <code style={{color:"#a78bfa"}}>append_blocks</code><span>→ add content</span>
                </div>
              </div>
            )}
            {node.data.type==="trigger.cron"&&(
              <div style={{background:"#0f1117",border:"1px solid #2a2d3e",borderRadius:6,padding:"8px 10px",fontSize:10,color:"#94a3b8",marginTop:6,lineHeight:1.8}}>
                <div style={{color:"#a78bfa",fontWeight:600,marginBottom:4}}>⏰ Saved automatically as a schedule</div>
                <div style={{display:"grid",gridTemplateColumns:"auto 1fr",gap:"0 10px"}}>
                  <span style={{color:"#64748b"}}>Every minute</span><code>* * * * *</code>
                  <span style={{color:"#64748b"}}>Every hour</span><code>0 * * * *</code>
                  <span style={{color:"#64748b"}}>Daily at 9am</span><code>0 9 * * *</code>
                  <span style={{color:"#64748b"}}>Weekdays 9am</span><code>0 9 * * 1-5</code>
                  <span style={{color:"#64748b"}}>Every 15 min</span><code>*/15 * * * *</code>
                </div>
              </div>
            )}
            {node.data.type==="action.merge"&&(
              <div style={{background:"#0f1117",border:"1px solid #2a2d3e",borderRadius:6,padding:"8px 10px",fontSize:10,color:"#94a3b8",marginTop:6,lineHeight:1.9}}>
                <div style={{color:"#a78bfa",fontWeight:600,marginBottom:4}}>⇒ Merge / Join</div>
                <div style={{display:"grid",gridTemplateColumns:"auto 1fr",gap:"0 10px",marginTop:4}}>
                  <code style={{color:"#4ade80"}}>dict</code><span>→ merge dicts (last wins)</span>
                  <code style={{color:"#60a5fa"}}>all</code><span>→ {"{ merged: [...], count }"}</span>
                  <code style={{color:"#94a3b8"}}>first</code><span>→ pass first upstream value</span>
                </div>
              </div>
            )}

            {/* Retry Policy */}
            {!isNote&&!isTrigger&&(<>
              <div className="section-divider">Retry Policy</div>
              <div className="form-row" style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:12}}>
                <div className="field-group" style={{marginBottom:0}}>
                  <label style={{fontSize:11,color:"#94a3b8",fontWeight:500,marginBottom:4}}>Max retries</label>
                  <input className="field-input" type="number" min="0" max="5" value={retryMax} onChange={e=>updateRetryMax(e.target.value)}/>
                </div>
                <div className="field-group" style={{marginBottom:0}}>
                  <label style={{fontSize:11,color:"#94a3b8",fontWeight:500,marginBottom:4}}>Retry delay (sec)</label>
                  <input className="field-input" type="number" min="1" max="60" value={retryDelay} onChange={e=>updateRetryDelay(e.target.value)}/>
                </div>
              </div>
              <div className="field-group" style={{marginTop:10}}>
                <label style={{fontSize:11,color:"#94a3b8",fontWeight:500,marginBottom:4}}>On failure</label>
                <select className="field-input" value={failMode} onChange={e=>updateFailMode(e.target.value)}>
                  <option value="abort">abort — stop the graph (default)</option>
                  <option value="continue">continue — store error, keep going</option>
                </select>
                {failMode==="continue"&&(
                  <div style={{fontSize:10,color:"#f59e0b",marginTop:4}}>
                    ⚠ Downstream nodes receive {"{ __error, __node, __type }"} as input.
                  </div>
                )}
              </div>
            </>)}

            <button className="delete-node-btn" style={{marginTop:16}} onClick={()=>{ onDelete(node.id); onClose(); }}>🗑 Remove node</button>
          </div>

          {/* ── RIGHT: OUTPUT ── */}
          <div className="nem-io-col right">
            <div className="nem-io-col-hdr">Output →</div>
            <div className="nem-io-view-tabs">
              <button className={`nem-io-view-tab${outputView==="schema"?" active":""}`} onClick={()=>setOutputView("schema")}>Schema</button>
              <button className={`nem-io-view-tab${outputView==="json"?" active":""}`} onClick={()=>setOutputView("json")}>JSON</button>
            </div>
            <div className="nem-io-col-body">
              <NioBody data={runOutput} view={outputView} sourceNodeId={node.id} isTruncated={!!(runOutput&&runOutput.__truncated)}/>
            </div>
          </div>

        </div>{/* end nem-columns */}
      </div>{/* end nem-panel */}
    </div>
  );
}

// ── Topbar "More" dropdown ────────────────────────────────────────────────────
function MoreMenu({ onExport, onImport, onLayout, onValidate, onHistory, disabled }){
  const [open, setOpen] = useState(false);
  const wrapRef = useRef(null);
  useEffect(()=>{
    if(!open) return;
    function close(e){ if(wrapRef.current && !wrapRef.current.contains(e.target)) setOpen(false); }
    document.addEventListener("mousedown", close);
    return ()=>document.removeEventListener("mousedown", close);
  },[open]);
  function act(fn){ setOpen(false); setTimeout(fn, 0); }
  return (
    <div className="tb-more-wrap" ref={wrapRef}>
      <button className="btn btn-ghost btn-sm" onClick={()=>setOpen(o=>!o)} title="More actions">⋯</button>
      {open && (
        <div className="tb-more-menu">
          <button className="tb-more-item" onClick={()=>act(onExport)} disabled={disabled}>⬇ Export flow</button>
          <button className="tb-more-item" onClick={()=>act(onImport)}>⬆ Import flow</button>
          <div className="tb-more-sep"/>
          <button className="tb-more-item" onClick={()=>act(onLayout)}>⊞ Auto-layout</button>
          <button className="tb-more-item" onClick={()=>act(onValidate)} disabled={disabled}>✔ Validate</button>
          <div className="tb-more-sep"/>
          <button className="tb-more-item" onClick={()=>act(onHistory)} disabled={disabled}>📜 Version history</button>
        </div>
      )}
    </div>
  );
}

// ── Main App ──────────────────────────────────────────────────────────────────
function App(){
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [selectedNode, setSelectedNode]  = useState(null);
  const [selectedEdge, setSelectedEdge]  = useState(null);
  const [edgeLabelInput, setEdgeLabelInput] = useState("");
  const [currentGraph, setCurrentGraph]  = useState(null);
  const [graphName, setGraphName]        = useState("Untitled Graph");
  const [graphs, setGraphs]              = useState([]);
  const [showModal, setShowModal]        = useState(false);
  const [showTestModal, setShowTestModal] = useState(false);
  const [showHistoryModal, setShowHistoryModal] = useState(false);
  const [contextMenu, setContextMenu]    = useState(null); // {x,y,node}
  const [toast, setToast]                = useState(null);
  const [saving, setSaving]              = useState(false);
  const [running, setRunning]            = useState(false);
  const [runError, setRunError]          = useState(null);  // {msg, nodeName}
  const [showMap, setShowMap]            = useState(true);
  const [paletteSearch, setPaletteSearch]= useState("");
  const [testPayload, setTestPayload]    = useState("{}");
  const [validationIssues, setValidationIssues] = useState(null); // null=not shown, []=clear
  const [inspectorRuns, setInspectorRuns] = useState([]);  // recent runs for current graph
  const [inspectorRunId, setInspectorRunId] = useState(null); // selected run for inspection
  const [credentials, setCredentials]    = useState([]);   // for variable autocomplete

  // undo/redo
  const histRef = useRef([]);
  const histIdx = useRef(-1);
  const restoring = useRef(false);
  const [histState, setHistState] = useState({idx:-1, len:0});
  function syncHistState(){ setHistState({idx:histIdx.current, len:histRef.current.length}) }

  const reactFlowWrapper = useRef(null);
  const importFileRef    = useRef(null);
  const { screenToFlowPosition } = useReactFlow();

  const showToast = useCallback((msg, type="success")=>{
    setToast({msg,type,key:Date.now()});
  },[]);

  useEffect(()=>{
    loadGraphList();
    // Pre-fetch credentials for the variable autocomplete dropdown
    api("GET","/api/credentials").then(setCredentials).catch(()=>{});
  },[]);

  // keyboard shortcuts
  useEffect(()=>{
    function onKey(e){
      const mac = e.metaKey;
      const ctrl = e.ctrlKey;
      if((ctrl||mac) && e.key==="z" && !e.shiftKey){ e.preventDefault(); doUndo() }
      if((ctrl&&e.key==="y")||(mac&&e.shiftKey&&e.key==="z")){ e.preventDefault(); doRedo() }
      if((ctrl||mac) && e.key==="s"){ e.preventDefault(); saveGraph() }
    }
    window.addEventListener("keydown", onKey);
    return ()=>window.removeEventListener("keydown", onKey);
  },[]);

  function saveSnap(ns, es){
    if(restoring.current) return;
    histRef.current = histRef.current.slice(0, histIdx.current + 1);
    histRef.current.push({
      nodes: JSON.parse(JSON.stringify(ns)),
      edges: JSON.parse(JSON.stringify(es))
    });
    if(histRef.current.length > 60) histRef.current.shift();
    histIdx.current = histRef.current.length - 1;
    syncHistState();
  }

  function doUndo(){
    if(histIdx.current <= 0) return;
    histIdx.current--;
    const snap = histRef.current[histIdx.current];
    restoring.current = true;
    setNodes(snap.nodes.map(n=>({...n})));
    setEdges(snap.edges.map(e=>({...e,type:"smoothstep",animated:true,style:{stroke:"#7c3aed",strokeWidth:2},markerEnd:{type:MarkerType.ArrowClosed,color:"#7c3aed"}})));
    setSelectedNode(null);
    syncHistState();
    setTimeout(()=>{ restoring.current = false }, 50);
  }

  function doRedo(){
    if(histIdx.current >= histRef.current.length - 1) return;
    histIdx.current++;
    const snap = histRef.current[histIdx.current];
    restoring.current = true;
    setNodes(snap.nodes.map(n=>({...n})));
    setEdges(snap.edges.map(e=>({...e,type:"smoothstep",animated:true,style:{stroke:"#7c3aed",strokeWidth:2},markerEnd:{type:MarkerType.ArrowClosed,color:"#7c3aed"}})));
    setSelectedNode(null);
    syncHistState();
    setTimeout(()=>{ restoring.current = false }, 50);
  }

  async function loadGraphList(){
    try { setGraphs(await api("GET","/api/graphs")) } catch(e){}
  }

  // Auto-open a graph from sessionStorage (admin page link) or URL hash (refresh)
  useEffect(()=>{
    const ssId      = sessionStorage.getItem("canvas_open_graph");
    const slugMatch = window.location.hash.match(/^#flow-([a-f0-9]{8})$/);
    const idMatch   = window.location.hash.match(/^#graph-(\d+)$/); // legacy fallback
    const endpoint  = ssId
      ? `/api/graphs/${ssId}`
      : slugMatch
        ? `/api/graphs/by-slug/${slugMatch[1]}`
        : idMatch
          ? `/api/graphs/${idMatch[1]}`
          : null;
    if(!endpoint) return;
    sessionStorage.removeItem("canvas_open_graph");
    api("GET", endpoint)
      .then(full => {
        const gd = full.graph_data || { nodes:[], edges:[] };
        const newNodes = (gd.nodes||[]).map(n=>({
          id:n.id, type:"custom", position:n.position||{x:100,y:100},
          data:{ type:n.type, label:n.data?.label||"", config:n.data?.config||{},
                 disabled:!!n.data?.disabled, retry_max:n.data?.retry_max||0, retry_delay:n.data?.retry_delay||5 }
        }));
        const newEdges = (gd.edges||[]).map(e=>({
          ...e, type:"smoothstep", animated:true,
          style:{stroke:"#7c3aed",strokeWidth:2},
          markerEnd:{type:MarkerType.ArrowClosed,color:"#7c3aed"}
        }));
        setNodes(newNodes); setEdges(newEdges);
        setCurrentGraph(full); setGraphName(full.name);
        setSelectedNode(null); setRunError(null);
        histRef.current=[]; histIdx.current=-1; syncHistState();
        saveSnap(newNodes, newEdges);
        window.location.hash = "flow-" + full.slug;
        showToast(`Opened: ${full.name}`);
      })
      .catch(()=>{ window.location.hash = ""; });
  },[]);

  const onConnect = useCallback((params)=>{
    setEdges(eds => {
      const newEdges = addEdge({
        ...params, type:"smoothstep", animated:true,
        style:{stroke:"#7c3aed",strokeWidth:2},
        markerEnd:{type:MarkerType.ArrowClosed,color:"#7c3aed"},
        label: undefined
      }, eds);
      saveSnap(nodes, newEdges);
      return newEdges;
    });
  },[nodes]);

  function onNodeClick(e, node){ setSelectedNode(node); setContextMenu(null) }
  function onPaneClick(){ setSelectedNode(null); setContextMenu(null); setSelectedEdge(null) }

  function onEdgeClick(e, edge){
    setSelectedEdge(edge);
    setEdgeLabelInput(edge.label || "");
    setSelectedNode(null);
    setContextMenu(null);
  }

  function updateEdgeLabel(label){
    setEdges(es => {
      const updated = es.map(e => e.id===selectedEdge.id ? {...e, label: label||undefined} : e);
      saveSnap(nodes, updated);
      return updated;
    });
    setSelectedEdge(null);
  }

  function onNodeContextMenu(e, node){
    e.preventDefault();
    setContextMenu({ x: e.clientX, y: e.clientY, node });
    setSelectedNode(node);
  }

  function ctxDuplicateNode(node){
    const newId = uid();
    const newNode = {
      ...node,
      id: newId,
      position: { x: node.position.x + 40, y: node.position.y + 40 },
      data: { ...node.data, label: (node.data.label||"") + " (copy)", _runStatus:undefined, _runOutput:undefined },
      selected: false
    };
    setNodes(ns => { const u=[...ns, newNode]; saveSnap(u,edges); return u });
    setSelectedNode(newNode);
  }

  function ctxToggleDisabled(id){
    setNodes(ns => {
      const u = ns.map(n => n.id===id ? {...n, data:{...n.data, disabled:!n.data.disabled}} : n);
      saveSnap(u, edges);
      return u;
    });
    setSelectedNode(s => s && s.id===id ? {...s, data:{...s.data, disabled:!s.data.disabled}} : s);
  }

  function ctxCopyId(id){
    navigator.clipboard.writeText(id).catch(()=>{});
    showToast(`Copied: ${id}`);
  }

  function ctxRenameNode(node){
    const newLabel = window.prompt("Rename node:", node.data.label || (NODE_DEFS[node.data.type]||{}).label || "");
    if(newLabel === null || newLabel.trim() === "") return;
    onNodeChange(node.id, { ...node.data, label: newLabel.trim() });
  }

  function onNodeChange(id, newData){
    setNodes(ns => {
      const updated = ns.map(n => n.id===id ? {...n, data:newData} : n);
      saveSnap(updated, edges);
      return updated;
    });
    setSelectedNode(s => s && s.id===id ? {...s, data:newData} : s);
  }

  function onDeleteNode(id){
    setNodes(ns => { const u=ns.filter(n=>n.id!==id); saveSnap(u,edges); return u });
    setEdges(es => es.filter(e=>e.source!==id && e.target!==id));
    setSelectedNode(null);
  }

  function onDragOver(e){ e.preventDefault(); e.dataTransfer.dropEffect="move" }

  function onDrop(e){
    e.preventDefault();
    const type = e.dataTransfer.getData("application/reactflow-type");
    if(!type) return;
    const def = NODE_DEFS[type];
    const pos = screenToFlowPosition({ x:e.clientX, y:e.clientY });
    const id = uid();
    setNodes(ns => {
      const updated = [...ns, { id, type:"custom", position:pos, data:{ type, label:def.label, config:{}, retry_max:0, retry_delay:5 } }];
      saveSnap(updated, edges);
      return updated;
    });
  }

  function buildGraphData(){
    return {
      nodes: nodes.map(n=>({
        id:n.id, type:n.data.type, position:n.position,
        data:{
          label:n.data.label||"",
          config:n.data.config||{},
          disabled:!!n.data.disabled,
          retry_max:n.data.retry_max||0,
          retry_delay:n.data.retry_delay||5,
          fail_mode:n.data.fail_mode||"abort"
        }
      })),
      edges: edges.map(e=>({ id:e.id, source:e.source, target:e.target, sourceHandle:e.sourceHandle||null, targetHandle:e.targetHandle||null, label:e.label }))
    };
  }

  function doAutoLayout(){
    setNodes(ns => {
      const laid = computeAutoLayout(ns, edges);
      saveSnap(laid, edges);
      return laid;
    });
    showToast("Layout applied");
  }

  async function saveGraph(){
    setSaving(true);
    try {
      const gd = buildGraphData();
      if(currentGraph){
        await api("PUT",`/api/graphs/${currentGraph.id}`,{ name:graphName, graph_data:gd });
        showToast("Graph saved!");
        await loadGraphList();
      } else {
        const g = await api("POST","/api/graphs",{ name:graphName, description:"", graph_data:gd });
        setCurrentGraph(g);
        showToast("Graph created!");
        await loadGraphList();
      }
    } catch(e){ showToast(e.message,"error") }
    setSaving(false);
  }

  function exportFlow(){
    if (!currentGraph) return;
    const data = {
      version: 8,
      name: graphName,
      description: currentGraph.description || "",
      graph_data: buildGraphData(),
      exported_at: new Date().toISOString(),
    };
    const blob = new Blob([JSON.stringify(data, null, 2)], {type:"application/json"});
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement("a");
    a.href     = url;
    a.download = `${graphName.replace(/[^a-z0-9]/gi,"_").toLowerCase()}.flow.json`;
    a.click();
    URL.revokeObjectURL(url);
    showToast("Exported flow JSON");
  }

  async function importFlow(e){
    const file = e.target.files[0];
    if (!file) return;
    e.target.value = "";
    try {
      const text = await file.text();
      const data = JSON.parse(text);
      const name   = (data.name || file.name.replace(/\.flow\.json$/i,"").replace(/_/g," ")) + " (imported)";
      const gd     = data.graph_data || data; // support raw graph_data too
      const created = await api("POST","/api/graphs",{name, description: data.description||"", graph_data: gd});
      showToast(`Imported as "${created.name}"`);
      await loadGraphList();
      onSelectGraph(created);
    } catch(err){ showToast("Import failed: " + err.message, "error") }
  }

  async function validateAndRun(payload){
    // load credentials list for reference checking
    let creds = [];
    try { creds = await api("GET","/api/credentials"); } catch(e){}
    const issues = validateFlow(nodes, edges, creds);
    if (issues.length === 0) {
      runGraph(payload);
    } else {
      setValidationIssues({issues, payload});
    }
  }

  async function runGraph(payload){
    if(!currentGraph){ showToast("Save the graph first","error"); return }
    setRunning(true);
    setRunError(null);
    // Mark all non-note nodes as pending so they pulse while running
    setNodes(ns => ns.map(n=>({...n, data:{ ...n.data,
      _runStatus: n.data.type==="note" ? undefined : "pending",
      _runOutput:undefined, _runInput:undefined
    }})));
    setSelectedNode(s => s ? {...s, data:{...s.data, _runStatus:"pending", _runOutput:undefined}} : s);

    let taskId;
    try {
      let runPayload = {};
      if (payload) {
        try { runPayload = JSON.parse(payload); } catch(e) { showToast("Invalid JSON in test payload","error"); setRunning(false); return }
      }
      const r = await api("POST",`/api/graphs/${currentGraph.id}/run`,{source:"canvas", payload: runPayload});
      taskId = r.task_id;
    } catch(e){
      showToast(e.message,"error");
      setRunning(false);
      return;
    }

    // Poll the single-run endpoint at 800ms — much faster and cheaper than fetching all runs
    const poll = setInterval(async()=>{
      try {
        const run = await api("GET",`/api/runs/by-task/${taskId}`);
        if(!run || run.status==="running" || run.status==="queued") return;
        clearInterval(poll);
        setRunning(false);
        refreshInspectorRuns();

        const traces = run.traces || [];
        const traceMap = {};
        traces.forEach(t => { if(t.node_id) traceMap[t.node_id] = t; });
        const ctx = (run.result?.context) || {};
        const err = run.result?.error || run.error || "";

        if(run.status==="succeeded"){
          showToast("Run succeeded ✓");
        } else {
          let failedNodeId = null;
          const m = err.match(/\[([a-zA-Z0-9_]+)\]/);
          if(m) failedNodeId = m[1];
          let failedNodeName = failedNodeId;
          setNodes(ns=>{ const n=ns.find(x=>x.id===failedNodeId); if(n) failedNodeName=n.data?.label||failedNodeId; return ns; });
          setRunError({ msg: err, nodeName: failedNodeName });
        }

        // Annotate nodes with trace results (status + duration)
        setNodes(ns => ns.map(n=>{
          if(n.data.type==="note") return n;
          const t = traceMap[n.id];
          if(t){
            const status = t.status==="ok"?"ok":t.status==="error"?"err":"skip";
            return { ...n, data:{ ...n.data, _runStatus:status, _runOutput:t.output, _runInput:t.input, _runDurationMs:t.duration_ms }};
          }
          // Node wasn't reached — clear pending
          return { ...n, data:{ ...n.data, _runStatus:undefined }};
        }));

        // Update selected node if open
        setSelectedNode(s => {
          if(!s) return s;
          const t = traceMap[s.id];
          if(!t) return {...s, data:{...s.data, _runStatus:undefined}};
          const st = t.status==="ok"?"ok":t.status==="error"?"err":"skip";
          return { ...s, data:{ ...s.data, _runStatus:st, _runOutput:t.output, _runInput:t.input, _runDurationMs:t.duration_ms }};
        });
      } catch(e){ /* keep polling */ }
    }, 800);

    // Safety timeout after 5 min
    setTimeout(()=>{ clearInterval(poll); setRunning(false) }, 300000);
  }

  // ── Live Data Inspector ────────────────────────────────────────────────────
  async function refreshInspectorRuns() {
    if (!currentGraph) { setInspectorRuns([]); return; }
    try {
      const allRuns = await api("GET", "/api/runs");
      const graphRuns = allRuns
        .filter(r => r.workflow === currentGraph.name || String(r.graph_id) === String(currentGraph.id))
        .slice(0, 20);
      setInspectorRuns(graphRuns);
    } catch(e) { /* silent */ }
  }

  async function loadInspectorRun(runId) {
    setInspectorRunId(runId);
    if (!runId) {
      // Clear annotations
      setNodes(ns => ns.map(n => ({...n, data:{...n.data, _runStatus:undefined, _runOutput:undefined, _runInput:undefined}})));
      setSelectedNode(s => s ? {...s, data:{...s.data, _runStatus:undefined, _runOutput:undefined, _runInput:undefined}} : s);
      return;
    }
    try {
      const allRuns = await api("GET", "/api/runs");
      const run = allRuns.find(r => String(r.id) === String(runId) || r.task_id === runId);
      if (!run) { showToast("Run not found","error"); return; }
      const traces = run.traces || [];
      // Build a map: node_id → trace
      const traceMap = {};
      traces.forEach(t => { if (t.node_id) traceMap[t.node_id] = t; });
      const ctx = run.result?.context || {};
      setNodes(ns => ns.map(n => {
        if (n.data.type === "note") return n;
        const t = traceMap[n.id];
        if (!t) return n;
        let status = t.status === "ok" ? "ok" : t.status === "error" ? "err" : "skip";
        return { ...n, data: { ...n.data, _runStatus: status, _runOutput: t.output, _runInput: t.input, _runDurationMs: t.duration_ms, _runAttempts: t.attempts } };
      }));
      setSelectedNode(s => {
        if (!s) return s;
        const t = traceMap[s.id];
        if (!t) return s;
        let st = t.status === "ok" ? "ok" : t.status === "error" ? "err" : "skip";
        return { ...s, data: { ...s.data, _runStatus: st, _runOutput: t.output, _runInput: t.input, _runDurationMs: t.duration_ms, _runAttempts: t.attempts } };
      });
      showToast(`Loaded run #${run.id || runId.slice(0,8)}`);
    } catch(e) { showToast("Failed to load run: " + e.message, "error"); }
  }

  async function onSelectGraph(g){
    try {
      const full = await api("GET",`/api/graphs/${g.id}`);
      const gd = full.graph_data || { nodes:[], edges:[] };
      const newNodes = (gd.nodes||[]).map(n=>({
        id:n.id, type:"custom", position:n.position||{x:100,y:100},
        data:{ type:n.type, label:n.data?.label||"", config:n.data?.config||{}, disabled:!!n.data?.disabled, retry_max:n.data?.retry_max||0, retry_delay:n.data?.retry_delay||5, fail_mode:n.data?.fail_mode||"abort" }
      }));
      const newEdges = (gd.edges||[]).map(e=>({
        ...e, type:"smoothstep", animated:true,
        style:{stroke:"#7c3aed",strokeWidth:2},
        markerEnd:{type:MarkerType.ArrowClosed,color:"#7c3aed"}
      }));
      setNodes(newNodes);
      setEdges(newEdges);
      setCurrentGraph(full);
      setGraphName(full.name);
      setSelectedNode(null);
      setShowModal(false);
      setRunError(null);
      setInspectorRunId(null);
      setInspectorRuns([]);
      // Reset undo history on graph load
      histRef.current = [];
      histIdx.current = -1;
      syncHistState();
      saveSnap(newNodes, newEdges);
      window.location.hash = "flow-" + full.slug;
      showToast(`Loaded: ${full.name}`);
      // Auto-load most recent run traces so the I/O panel shows data immediately
      try {
        const allRuns = await api("GET", "/api/runs");
        const graphRuns = allRuns.filter(r =>
          r.workflow === full.name || String(r.graph_id) === String(full.id)
        );
        if(graphRuns.length > 0){
          const latest = graphRuns[0];
          setInspectorRuns(graphRuns.slice(0,20));
          setInspectorRunId(String(latest.id || latest.task_id));
          const traces = latest.traces || [];
          const traceMap = {};
          traces.forEach(t => { if(t.node_id) traceMap[t.node_id] = t; });
          setNodes(ns => ns.map(n => {
            if(n.data.type === "note") return n;
            const t = traceMap[n.id];
            if(!t) return n;
            const st = t.status === "ok" ? "ok" : t.status === "error" ? "err" : "skip";
            return { ...n, data:{ ...n.data, _runStatus:st, _runOutput:t.output, _runInput:t.input, _runDurationMs:t.duration_ms, _runAttempts:t.attempts }};
          }));
        }
      } catch(e){ /* non-fatal: run data is optional */ }
    } catch(err){ showToast(err.message,"error") }
  }

  async function onNewGraph(name){
    setNodes([]); setEdges([]); setCurrentGraph(null);
    setGraphName(name); setSelectedNode(null); setShowModal(false); setRunError(null);
    histRef.current = []; histIdx.current = -1;
    window.location.hash = "";
  }

  async function duplicateGraph(g){
    try {
      const full = await api("GET",`/api/graphs/${g.id}`);
      const copy = await api("POST","/api/graphs",{
        name: full.name.replace(TEMPLATE_EMOJIS,"").trim() + " (copy)",
        description: full.description || "",
        graph_data: full.graph_data || {}
      });
      showToast(`Duplicated as "${copy.name}"`);
      await loadGraphList();
    } catch(e){ showToast(e.message,"error") }
  }

  async function renameGraph(g){
    const newName = window.prompt("Rename flow:", g.name);
    if(!newName || newName.trim() === g.name) return;
    try {
      await api("PUT",`/api/graphs/${g.id}`,{name:newName.trim()});
      showToast(`Renamed to "${newName.trim()}"`);
      // Update currently open graph name if it's the renamed one
      if(currentGraph && currentGraph.id === g.id) setGraphName(newName.trim());
      await loadGraphList();
    } catch(e){ showToast(e.message,"error") }
  }

  async function deleteGraph(id, name){
    try {
      await api("DELETE",`/api/graphs/${id}`);
      showToast(`Deleted "${name}"`);
      // If we just deleted the currently open graph, reset
      if(currentGraph && currentGraph.id === id){
        setNodes([]); setEdges([]); setCurrentGraph(null);
        setGraphName("Untitled Graph"); setSelectedNode(null); setRunError(null);
        histRef.current = []; histIdx.current = -1;
        window.location.hash = "";
      }
      await loadGraphList();
    } catch(e){ showToast(e.message,"error") }
  }

  const token = getToken();

  return (
    <>
      {/* Hidden import file input */}
      <input type="file" accept=".json" style={{display:"none"}} ref={importFileRef} onChange={importFlow}/>

      {/* Top bar */}
      <div className="topbar">
        <span className="logo">⚡ HiveRunr</span>
        <div className="tb-divider"/>
        <button className="btn btn-ghost btn-sm" onClick={()=>setShowModal(true)}>📂 Open</button>
        <MoreMenu
          onExport={exportFlow}
          onImport={()=>importFileRef.current&&importFileRef.current.click()}
          onLayout={doAutoLayout}
          onValidate={()=>validateAndRun()}
          onHistory={()=>setShowHistoryModal(true)}
          disabled={!currentGraph}
        />
        <div className="tb-divider"/>
        <input className="name-input" value={graphName}
          onChange={e=>setGraphName(e.target.value)} placeholder="Flow name…"/>
        {currentGraph && (
          <span style={{fontSize:10,color:"#475569",whiteSpace:"nowrap"}}>
            #{currentGraph.id}
          </span>
        )}
        <div className="topbar-spacer"/>
        {/* Undo / Redo */}
        <button className="btn btn-ghost btn-sm" onClick={doUndo}
          disabled={histState.idx<=0} title="Undo (Ctrl+Z)">↩</button>
        <button className="btn btn-ghost btn-sm" onClick={doRedo}
          disabled={histState.idx>=histState.len-1} title="Redo (Ctrl+Y)">↪</button>
        <div className="tb-divider"/>
        <button className="btn btn-ghost btn-sm" onClick={()=>setShowMap(m=>!m)}
          title="Toggle minimap" style={{opacity:showMap?1:0.45}}>🗺</button>
        <div className="tb-divider"/>
        {/* Live run inspector */}
        <select
          className="btn btn-ghost btn-sm"
          style={{cursor:"pointer",maxWidth:165,padding:"2px 6px"}}
          title="Load a past run to inspect node inputs/outputs"
          value={inspectorRunId||""}
          onClick={refreshInspectorRuns}
          onChange={e=>loadInspectorRun(e.target.value||null)}
          disabled={!currentGraph}
        >
          <option value="">🔬 Inspect run…</option>
          {inspectorRuns.map(r=>(
            <option key={r.task_id||r.id} value={r.task_id||r.id}>
              #{r.id} {r.status==="succeeded"?"✓":r.status==="failed"?"✗":"⟳"} {r.created_at?new Date(r.created_at).toLocaleTimeString():""}
            </option>
          ))}
        </select>
        {inspectorRunId && (
          <button className="btn btn-ghost btn-sm" style={{color:"#f87171"}} onClick={()=>loadInspectorRun(null)} title="Clear run overlay">✕</button>
        )}
        <div className="tb-divider"/>
        <a href={`/admin?token=${token}`} className="btn btn-ghost btn-sm">← Admin</a>
        <button className="btn btn-ghost btn-sm" onClick={()=>setShowTestModal(true)} disabled={!currentGraph}>🧪 Test</button>
        <button className="btn btn-ghost btn-sm" onClick={saveGraph} disabled={saving} title="Save (Ctrl+S)">
          {saving ? <><span style={{animation:"spin .8s linear infinite",display:"inline-block"}}>⟳</span> Saving…</> : "💾 Save"}
        </button>
        <button className="btn btn-success btn-sm" onClick={()=>runGraph()} disabled={running||!currentGraph}>
          {running ? <><span style={{animation:"spin .8s linear infinite",display:"inline-block"}}>⟳</span> Running…</> : "▶ Run"}
        </button>
      </div>

      {/* Error banner */}
      {runError && (
        <div className="error-banner">
          <span className="eb-icon">✗</span>
          <div className="eb-body">
            <div className="eb-title">Run failed{runError.nodeName ? ` — node "${runError.nodeName}"` : ""}</div>
            <div className="eb-msg">{runError.msg}</div>
          </div>
          <button className="eb-close" onClick={()=>setRunError(null)}>✕</button>
        </div>
      )}

      {/* Main layout */}
      <div className="canvas-layout">
        <Palette search={paletteSearch} onSearch={setPaletteSearch}/>
        <div className="flow-wrap" ref={reactFlowWrapper}>
          <FlowCanvas
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={onConnect}
            onNodeClick={onNodeClick}
            onPaneClick={onPaneClick}
            onNodeContextMenu={onNodeContextMenu}
            onEdgeClick={onEdgeClick}
            onMoveStart={()=>setContextMenu(null)}
            onDrop={onDrop}
            onDragOver={onDragOver}
            nodeTypes={nodeTypes}
            fitView
            deleteKeyCode="Delete"
            style={{background:"#0f1117"}}
          >
            <Background variant={BackgroundVariant.Dots} color="#2a2d3e" gap={20} size={1}/>
            <Controls/>
            {showMap && (
              <MiniMap
                nodeColor={n=>{ const def=NODE_DEFS[n.data?.type]; return def?def.color:"#475569" }}
                maskColor="#0f111788"
              />
            )}
          </FlowCanvas>
        </div>
        {/* Slim right panel: only shows the empty-state hint */}
        <ConfigPanel node={null} onChange={onNodeChange} onDelete={onDeleteNode} edges={edges}/>
      </div>

      {/* Node Editor Modal — opens when a node is selected */}
      {selectedNode && (
        <NodeEditorModal
          node={selectedNode}
          onChange={onNodeChange}
          onDelete={onDeleteNode}
          onClose={()=>setSelectedNode(null)}
          edges={edges}
          allNodes={nodes}
          credentials={credentials}
        />
      )}

      {showModal && (
        <OpenModal
          graphs={graphs}
          onClose={()=>setShowModal(false)}
          onSelect={onSelectGraph}
          onNew={onNewGraph}
          onDuplicate={duplicateGraph}
          onDelete={deleteGraph}
          onRename={renameGraph}
        />
      )}

      <TestPayloadModal isOpen={showTestModal} onClose={()=>setShowTestModal(false)} onRun={runGraph} testPayload={testPayload} onPayloadChange={setTestPayload} />

      <HistoryModal isOpen={showHistoryModal} onClose={()=>setShowHistoryModal(false)} graphId={currentGraph?.id} showToast={showToast} />

      {selectedEdge && (
        <EdgeLabelModal edge={selectedEdge} value={edgeLabelInput} onChange={setEdgeLabelInput}
          onConfirm={updateEdgeLabel} onClose={()=>setSelectedEdge(null)}/>
      )}

      {validationIssues && (
        <ValidationModal
          issues={validationIssues.issues}
          onClose={()=>setValidationIssues(null)}
          onRunAnyway={()=>{ runGraph(validationIssues.payload); setValidationIssues(null); }}
        />
      )}

      {contextMenu && (
        <NodeContextMenu
          menu={contextMenu}
          onClose={()=>setContextMenu(null)}
          onDuplicate={ctxDuplicateNode}
          onDelete={id=>{ onDeleteNode(id); setContextMenu(null); }}
          onToggleDisabled={ctxToggleDisabled}
          onCopyId={ctxCopyId}
          onRename={ctxRenameNode}
        />
      )}
      {toast && <Toast key={toast.key} msg={toast.msg} type={toast.type} onDone={()=>setToast(null)}/>}
    </>
  );
}

function Root(){
  return (
    <ReactFlowProvider>
      <App/>
    </ReactFlowProvider>
  );
}

const root = ReactDOM.createRoot(document.getElementById("root"));
root.render(<Root/>);
</script>
</body>
</html>



CANVAS_EOF

# ── 18. app/templates/*.json ────────────────────────────────────────────────
cat > "$PROJECT/app/templates/daily_health_check.json" << 'EOF'
{
  "name": "Daily Health Check",
  "description": "Runs every morning, hits an HTTP endpoint, and sends a Slack alert if it's down.",
  "category": "Monitoring",
  "tags": ["http", "slack", "cron", "monitoring"],
  "graph_data": {
    "nodes": [
      {
        "id": "trigger1",
        "type": "trigger.cron",
        "position": {"x": 60, "y": 180},
        "data": {
          "label": "Every morning 8am",
          "config": {"cron": "0 8 * * *", "timezone": "UTC", "description": "Daily at 8am UTC"},
          "disabled": false,
          "retry_max": 0,
          "retry_delay": 5
        }
      },
      {
        "id": "http1",
        "type": "action.http_request",
        "position": {"x": 325, "y": 180},
        "data": {
          "label": "Check endpoint",
          "config": {"url": "https://your-service.example.com/health", "method": "GET"},
          "disabled": false,
          "retry_max": 1,
          "retry_delay": 10
        }
      },
      {
        "id": "cond1",
        "type": "action.condition",
        "position": {"x": 590, "y": 180},
        "data": {
          "label": "Status 200?",
          "config": {"expression": "input.get('status') == 200"},
          "disabled": false,
          "retry_max": 0,
          "retry_delay": 5
        }
      },
      {
        "id": "slack1",
        "type": "action.slack",
        "position": {"x": 855, "y": 310},
        "data": {
          "label": "Alert: service DOWN",
          "config": {
            "credential": "my-slack",
            "message": ":red_circle: *Health check FAILED* — status {{http1.status}}\nEndpoint returned unexpected response."
          },
          "disabled": false,
          "retry_max": 0,
          "retry_delay": 5
        }
      },
      {
        "id": "log1",
        "type": "action.log",
        "position": {"x": 855, "y": 80},
        "data": {
          "label": "Log: OK",
          "config": {"message": "Health check passed — status {{http1.status}}"},
          "disabled": false,
          "retry_max": 0,
          "retry_delay": 5
        }
      }
    ],
    "edges": [
      {"id": "e1", "source": "trigger1", "target": "http1", "sourceHandle": null, "targetHandle": null},
      {"id": "e2", "source": "http1", "target": "cond1", "sourceHandle": null, "targetHandle": null},
      {"id": "e3", "source": "cond1", "target": "log1", "sourceHandle": "true", "targetHandle": null},
      {"id": "e4", "source": "cond1", "target": "slack1", "sourceHandle": "false", "targetHandle": null}
    ]
  }
}
EOF

cat > "$PROJECT/app/templates/github_issue_to_slack.json" << 'EOF'
{
  "name": "GitHub Issue → Slack",
  "description": "Fetches open GitHub issues and posts a summary to Slack. Run on a schedule or manually.",
  "category": "Integrations",
  "tags": ["github", "slack", "issues", "notifications"],
  "graph_data": {
    "nodes": [
      {
        "id": "trigger1",
        "type": "trigger.cron",
        "position": {"x": 60, "y": 200},
        "data": {
          "label": "Every weekday 9am",
          "config": {"cron": "0 9 * * 1-5", "timezone": "UTC", "description": "Weekdays at 9am"},
          "disabled": false,
          "retry_max": 0,
          "retry_delay": 5
        }
      },
      {
        "id": "github1",
        "type": "action.github",
        "position": {"x": 325, "y": 200},
        "data": {
          "label": "Fetch open issues",
          "config": {
            "credential": "my-github",
            "action": "list_issues",
            "owner": "your-org",
            "repo": "your-repo"
          },
          "disabled": false,
          "retry_max": 1,
          "retry_delay": 5
        }
      },
      {
        "id": "transform1",
        "type": "action.transform",
        "position": {"x": 590, "y": 200},
        "data": {
          "label": "Format summary",
          "config": {
            "expression": "{'count': len(input.get('issues', [])), 'titles': [i['title'] for i in input.get('issues', [])[:5]]}"
          },
          "disabled": false,
          "retry_max": 0,
          "retry_delay": 5
        }
      },
      {
        "id": "slack1",
        "type": "action.slack",
        "position": {"x": 855, "y": 200},
        "data": {
          "label": "Post to Slack",
          "config": {
            "credential": "my-slack",
            "message": ":github: *Open Issues Update* — {{transform1.count}} open issues\n{{transform1.titles}}"
          },
          "disabled": false,
          "retry_max": 0,
          "retry_delay": 5
        }
      }
    ],
    "edges": [
      {"id": "e1", "source": "trigger1", "target": "github1", "sourceHandle": null, "targetHandle": null},
      {"id": "e2", "source": "github1", "target": "transform1", "sourceHandle": null, "targetHandle": null},
      {"id": "e3", "source": "transform1", "target": "slack1", "sourceHandle": null, "targetHandle": null}
    ]
  }
}
EOF

cat > "$PROJECT/app/templates/llm_summarizer.json" << 'EOF'
{
  "name": "LLM Summarizer",
  "description": "Fetches content from a URL, passes it to an LLM for summarization, then sends the result via Slack.",
  "category": "AI",
  "tags": ["llm", "openai", "slack", "summarize", "http"],
  "graph_data": {
    "nodes": [
      {
        "id": "trigger1",
        "type": "trigger.manual",
        "position": {"x": 60, "y": 200},
        "data": {
          "label": "Manual trigger",
          "config": {},
          "disabled": false,
          "retry_max": 0,
          "retry_delay": 5
        }
      },
      {
        "id": "http1",
        "type": "action.http_request",
        "position": {"x": 325, "y": 200},
        "data": {
          "label": "Fetch content",
          "config": {
            "url": "https://your-content-source.example.com/latest",
            "method": "GET"
          },
          "disabled": false,
          "retry_max": 1,
          "retry_delay": 5
        }
      },
      {
        "id": "llm1",
        "type": "action.llm_call",
        "position": {"x": 590, "y": 200},
        "data": {
          "label": "Summarize with LLM",
          "config": {
            "model": "gpt-4o-mini",
            "system": "You are a concise summarizer. Keep summaries under 3 sentences.",
            "prompt": "Please summarize the following content:\n\n{{http1.body}}"
          },
          "disabled": false,
          "retry_max": 1,
          "retry_delay": 10
        }
      },
      {
        "id": "slack1",
        "type": "action.slack",
        "position": {"x": 855, "y": 200},
        "data": {
          "label": "Post summary",
          "config": {
            "credential": "my-slack",
            "message": ":robot_face: *AI Summary*\n{{llm1.response}}\n_Used {{llm1.tokens}} tokens_"
          },
          "disabled": false,
          "retry_max": 0,
          "retry_delay": 5
        }
      }
    ],
    "edges": [
      {"id": "e1", "source": "trigger1", "target": "http1", "sourceHandle": null, "targetHandle": null},
      {"id": "e2", "source": "http1", "target": "llm1", "sourceHandle": null, "targetHandle": null},
      {"id": "e3", "source": "llm1", "target": "slack1", "sourceHandle": null, "targetHandle": null}
    ]
  }
}
EOF

cat > "$PROJECT/app/templates/notion_daily_log.json" << 'EOF'
{
  "name": "Notion Daily Log",
  "description": "Creates a new daily log entry in a Notion database each morning with today's date.",
  "category": "Productivity",
  "tags": ["notion", "daily", "log", "cron"],
  "graph_data": {
    "nodes": [
      {
        "id": "trigger1",
        "type": "trigger.cron",
        "position": {"x": 60, "y": 200},
        "data": {
          "label": "Daily at 7am",
          "config": {"cron": "0 7 * * *", "timezone": "UTC", "description": "Daily at 7am"},
          "disabled": false,
          "retry_max": 0,
          "retry_delay": 5
        }
      },
      {
        "id": "script1",
        "type": "action.run_script",
        "position": {"x": 325, "y": 200},
        "data": {
          "label": "Build date string",
          "config": {
            "script": "import datetime\ntoday = datetime.date.today().isoformat()\nresult = {'date': today, 'title': f'Daily Log — {today}'}"
          },
          "disabled": false,
          "retry_max": 0,
          "retry_delay": 5
        }
      },
      {
        "id": "notion1",
        "type": "action.notion",
        "position": {"x": 590, "y": 200},
        "data": {
          "label": "Create log entry",
          "config": {
            "credential": "my-notion",
            "action": "create_page",
            "database_id": "YOUR_DATABASE_ID",
            "properties_json": "{\"Name\":{\"title\":[{\"text\":{\"content\":\"{{script1.title}}\"}}]},\"Date\":{\"date\":{\"start\":\"{{script1.date}}\"}}}"
          },
          "disabled": false,
          "retry_max": 1,
          "retry_delay": 5
        }
      },
      {
        "id": "log1",
        "type": "action.log",
        "position": {"x": 855, "y": 200},
        "data": {
          "label": "Log page URL",
          "config": {"message": "Created: {{notion1.url}}"},
          "disabled": false,
          "retry_max": 0,
          "retry_delay": 5
        }
      }
    ],
    "edges": [
      {"id": "e1", "source": "trigger1", "target": "script1", "sourceHandle": null, "targetHandle": null},
      {"id": "e2", "source": "script1", "target": "notion1", "sourceHandle": null, "targetHandle": null},
      {"id": "e3", "source": "notion1", "target": "log1", "sourceHandle": null, "targetHandle": null}
    ]
  }
}
EOF

cat > "$PROJECT/app/templates/sheets_to_slack_report.json" << 'EOF'
{
  "name": "Google Sheets → Slack Report",
  "description": "Reads rows from a Google Sheet and sends a formatted summary to Slack on a schedule.",
  "category": "Reporting",
  "tags": ["google_sheets", "slack", "report", "cron"],
  "graph_data": {
    "nodes": [
      {
        "id": "trigger1",
        "type": "trigger.cron",
        "position": {"x": 60, "y": 200},
        "data": {
          "label": "Every Friday 4pm",
          "config": {"cron": "0 16 * * 5", "timezone": "UTC", "description": "Fridays at 4pm"},
          "disabled": false,
          "retry_max": 0,
          "retry_delay": 5
        }
      },
      {
        "id": "sheets1",
        "type": "action.google_sheets",
        "position": {"x": 325, "y": 200},
        "data": {
          "label": "Read sheet data",
          "config": {
            "credential": "my-google-sa",
            "action": "read_range",
            "spreadsheet_id": "YOUR_SPREADSHEET_ID",
            "range": "Sheet1!A1:E50",
            "header_row": "true"
          },
          "disabled": false,
          "retry_max": 1,
          "retry_delay": 5
        }
      },
      {
        "id": "transform1",
        "type": "action.transform",
        "position": {"x": 590, "y": 200},
        "data": {
          "label": "Summarise rows",
          "config": {
            "expression": "{'row_count': input.get('count', 0), 'summary': f\"{input.get('count',0)} rows read from sheet\"}"
          },
          "disabled": false,
          "retry_max": 0,
          "retry_delay": 5
        }
      },
      {
        "id": "slack1",
        "type": "action.slack",
        "position": {"x": 855, "y": 200},
        "data": {
          "label": "Send weekly report",
          "config": {
            "credential": "my-slack",
            "message": ":bar_chart: *Weekly Sheet Report*\n{{transform1.summary}}"
          },
          "disabled": false,
          "retry_max": 0,
          "retry_delay": 5
        }
      }
    ],
    "edges": [
      {"id": "e1", "source": "trigger1", "target": "sheets1", "sourceHandle": null, "targetHandle": null},
      {"id": "e2", "source": "sheets1", "target": "transform1", "sourceHandle": null, "targetHandle": null},
      {"id": "e3", "source": "transform1", "target": "slack1", "sourceHandle": null, "targetHandle": null}
    ]
  }
}
EOF

cat > "$PROJECT/app/templates/webhook_to_notion.json" << 'EOF'
{
  "name": "Webhook → Notion",
  "description": "Receives a webhook payload and creates a page in Notion. Great for form submissions, CI events, or alerts.",
  "category": "Integrations",
  "tags": ["webhook", "notion", "trigger", "forms"],
  "graph_data": {
    "nodes": [
      {
        "id": "trigger1",
        "type": "trigger.webhook",
        "position": {"x": 60, "y": 200},
        "data": {
          "label": "Incoming webhook",
          "config": {"description": "POST your event payload here"},
          "disabled": false,
          "retry_max": 0,
          "retry_delay": 5
        }
      },
      {
        "id": "notion1",
        "type": "action.notion",
        "position": {"x": 325, "y": 200},
        "data": {
          "label": "Create Notion page",
          "config": {
            "credential": "my-notion",
            "action": "create_page",
            "database_id": "YOUR_DATABASE_ID",
            "properties_json": "{\"Name\":{\"title\":[{\"text\":{\"content\":\"{{trigger1.title}}\"}}]},\"Source\":{\"rich_text\":[{\"text\":{\"content\":\"webhook\"}}]}}"
          },
          "disabled": false,
          "retry_max": 1,
          "retry_delay": 5
        }
      },
      {
        "id": "log1",
        "type": "action.log",
        "position": {"x": 590, "y": 200},
        "data": {
          "label": "Log result",
          "config": {"message": "Page created: {{notion1.id}}"},
          "disabled": false,
          "retry_max": 0,
          "retry_delay": 5
        }
      }
    ],
    "edges": [
      {"id": "e1", "source": "trigger1", "target": "notion1", "sourceHandle": null, "targetHandle": null},
      {"id": "e2", "source": "notion1", "target": "log1", "sourceHandle": null, "targetHandle": null}
    ]
  }
}
EOF


# ── 20. app/nodes/ ─────────────────────────────────────────────────────────

# nodes/__init__.py
cat > "$PROJECT/app/nodes/__init__.py" << 'NODES_INIT_EOF'
"""
Node registry — auto-discovers all node modules in this package.

Each module must export:
  NODE_TYPE : str   — e.g. "action.http_request"
  run(config, inp, context, logger, creds=None, **kwargs) -> dict

Drop a .py file in app/nodes/custom/ to add a custom node with zero restart.
"""
import importlib
import pkgutil
import logging
from pathlib import Path

log = logging.getLogger(__name__)
_registry: dict = {}


def _load_package(package_name: str, path: str):
    """Load all node modules from a package directory."""
    for mod_info in pkgutil.iter_modules([path]):
        if mod_info.name.startswith('_'):
            continue
        full_name = f"{package_name}.{mod_info.name}"
        try:
            mod = importlib.import_module(full_name)
            if hasattr(mod, 'NODE_TYPE') and hasattr(mod, 'run'):
                _registry[mod.NODE_TYPE] = mod.run
                log.debug(f"Registered node: {mod.NODE_TYPE}")
        except Exception as e:
            log.warning(f"Failed to load node module {full_name}: {e}")


# Load built-in nodes
_load_package(__name__, str(Path(__file__).parent))

# Load custom nodes (app/nodes/custom/)
_custom_path = Path(__file__).parent / 'custom'
_custom_path.mkdir(exist_ok=True)
_load_package(f"{__name__}.custom", str(_custom_path))


def get_handler(node_type: str):
    """Return the run() function for a node type, or None if not found."""
    return _registry.get(node_type)


def list_node_types() -> list:
    """Return sorted list of all registered node types."""
    return sorted(_registry.keys())


def reload_custom():
    """Hot-reload custom nodes without restarting (call from admin API)."""
    _custom_path = Path(__file__).parent / 'custom'
    _load_package(f"{__name__}.custom", str(_custom_path))

NODES_INIT_EOF

# nodes/_utils.py
cat > "$PROJECT/app/nodes/_utils.py" << 'NODES_UTILS_EOF'
"""Shared utilities for node modules."""
import re
import json

_TMPL = re.compile(r'\{\{([^}]+)\}\}')


def _render(text: str, ctx: dict, creds: dict = None) -> str:
    """
    Render template variables in text.
    Supports:
    - {{node_id.field}} — reference output of a previous node
    - {{creds.name}} or {{creds.name.field}} — credential vault access
    """
    if not isinstance(text, str):
        return text

    def replace(m):
        key = m.group(1).strip()

        # Credential vault: {{creds.name}} or {{creds.name.field}}
        if key.startswith('creds.') and creds is not None:
            rest  = key[6:].strip()
            parts = rest.split('.', 1)
            cred_name  = parts[0]
            cred_field = parts[1] if len(parts) > 1 else None
            raw = creds.get(cred_name)
            if raw is None:
                return m.group(0)
            if cred_field:
                try:
                    data = json.loads(raw)
                    val  = data.get(cred_field)
                    return str(val) if val is not None else m.group(0)
                except (json.JSONDecodeError, AttributeError):
                    return m.group(0)
            return raw

        # Context reference: {{node_id.field}} or {{node_id}}
        parts = key.split('.', 1)
        node_id = parts[0]
        field   = parts[1] if len(parts) > 1 else None
        val = ctx.get(node_id)
        if val is None:
            return m.group(0)
        if field:
            if isinstance(val, dict):
                val = val.get(field, m.group(0))
            else:
                return m.group(0)
        return str(val) if not isinstance(val, str) else val

    return _TMPL.sub(replace, text)

NODES_UTILS_EOF

# nodes/custom/__init__.py
cat > "$PROJECT/app/nodes/custom/__init__.py" << 'CUSTOM_INIT_EOF'

CUSTOM_INIT_EOF

# nodes/custom/README.md
cat > "$PROJECT/app/nodes/custom/README.md" << 'CUSTOM_README_EOF'
# Custom Nodes

Drop a Python file here to add a custom node. No restart required — call `POST /api/admin/reload_nodes` to hot-reload.

## Template

```python
NODE_TYPE = "action.my_custom_node"
LABEL     = "My Custom Node"

def run(config: dict, inp: dict, context: dict, logger, creds: dict = None, **kwargs) -> dict:
    # config  — node's configured fields (already template-rendered by executor)
    # inp     — output of the previous node
    # context — full run context keyed by node_id
    # logger  — call logger("message") to emit a run log line
    # creds   — dict of {name: raw_value} from the credential vault
    value = config.get("my_field", "default")
    logger(f"Custom node running with value={value}")
    return {"result": value, "input_was": inp}
```

CUSTOM_README_EOF

# nodes/action_call_graph.py
cat > "$PROJECT/app/nodes/action_call_graph.py" << 'ACTION_CALL_GRAPH_EOF'
"""Call subgraph action node."""
import json
from app.nodes._utils import _render

NODE_TYPE = "action.call_graph"
LABEL = "Call Graph"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Call another graph (subgraph/nested automation)."""
    from app.core.db import get_graph as _get_graph
    from app.core.executor import run_graph

    target_id = config.get('graph_id', '')
    if not target_id:
        raise ValueError("Call Graph: no target graph_id configured")

    try:
        target_id = int(target_id)
    except:
        raise ValueError(f"Call Graph: graph_id must be an integer, got '{target_id}'")

    sub_payload = {}
    if config.get('payload'):
        try:
            sub_payload = json.loads(_render(config['payload'], context, creds))
        except:
            sub_payload = {}

    sub_payload = {**inp, **sub_payload} if isinstance(inp, dict) else sub_payload

    g = _get_graph(target_id)
    if not g:
        raise ValueError(f"Call Graph: graph {target_id} not found")

    try:
        gd = json.loads(g.get('graph_json') or '{}')
    except:
        gd = {}

    sub = run_graph(gd, initial_payload=sub_payload, logger=logger, _depth=kwargs.get('_depth', 0) + 1)

    return sub.get('context', {})

ACTION_CALL_GRAPH_EOF

# nodes/action_condition.py
cat > "$PROJECT/app/nodes/action_condition.py" << 'ACTION_CONDITION_EOF'
"""Condition / branching action node."""
from app.nodes._utils import _render

NODE_TYPE = "action.condition"
LABEL = "Condition"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Evaluate a boolean expression and return {result, input}."""
    expr = _render(config.get('expression', 'True'), context, creds)
    safe_builtins = {'len': len, 'str': str, 'int': int, 'float': float, 'bool': bool, 'list': list, 'dict': dict, 'tuple': tuple}
    result = eval(expr, {'__builtins__': safe_builtins}, {'input': inp, 'context': context})
    return {'result': bool(result), 'input': inp}

ACTION_CONDITION_EOF

# nodes/action_delay.py
cat > "$PROJECT/app/nodes/action_delay.py" << 'ACTION_DELAY_EOF'
"""Delay action node."""
import time

NODE_TYPE = "action.delay"
LABEL = "Delay"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Sleep for specified seconds."""
    secs = float(config.get('seconds', 1))
    time.sleep(secs)
    return {'slept': secs}

ACTION_DELAY_EOF

# nodes/action_filter.py
cat > "$PROJECT/app/nodes/action_filter.py" << 'ACTION_FILTER_EOF'
"""Filter / map action node."""
from app.nodes._utils import _render

NODE_TYPE = "action.filter"
LABEL = "Filter"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Filter items in a list based on an expression."""
    field = config.get('field', '')
    expr = _render(config.get('expression', 'True'), context, creds)
    items = inp.get(field, inp) if field and isinstance(inp, dict) else inp

    if not isinstance(items, list):
        items = [items]

    safe_builtins = {'len': len, 'str': str, 'int': int, 'float': float, 'bool': bool, 'list': list, 'dict': dict, 'tuple': tuple}
    kept = [item for item in items
            if eval(expr, {'__builtins__': safe_builtins}, {'item': item, 'context': context, 'input': inp})]

    return {'items': kept, 'count': len(kept), 'total': len(items)}

ACTION_FILTER_EOF

# nodes/action_github.py
cat > "$PROJECT/app/nodes/action_github.py" << 'ACTION_GITHUB_EOF'
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

ACTION_GITHUB_EOF

# nodes/action_google_sheets.py
cat > "$PROJECT/app/nodes/action_google_sheets.py" << 'ACTION_GOOGLE_SHEETS_EOF'
"""Google Sheets API action node."""
import json
from app.nodes._utils import _render

NODE_TYPE = "action.google_sheets"
LABEL = "Google Sheets"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Interact with Google Sheets API via service account."""
    import httpx

    cred_name = _render(config.get('credential', ''), context, creds)
    service_account_json = ''

    if cred_name and creds:
        raw = creds.get(cred_name)
        if raw:
            try:
                service_account_json = json.loads(raw).get('json', raw)
            except:
                service_account_json = raw

    if not service_account_json:
        raise ValueError("Google Sheets: no service account credential configured")

    spreadsheet_id = _render(config.get('spreadsheet_id', ''), context, creds)
    action = config.get('action', 'read_range')
    sheet_range = _render(config.get('range', 'Sheet1!A1:Z100'), context, creds)

    if not spreadsheet_id:
        raise ValueError("Google Sheets: spreadsheet_id required")

    # Get access token via service account JWT
    try:
        from google.oauth2 import service_account as _sa
        from google.auth.transport.requests import Request as _GReq

        _creds = _sa.Credentials.from_service_account_info(
            json.loads(service_account_json),
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        _creds.refresh(_GReq())
        access_token = _creds.token
    except Exception as e:
        raise ValueError(f"Google Sheets: auth failed — {e}")

    headers = {'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'}
    sheets_base = f'https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}'

    if action == 'read_range':
        r = httpx.get(f'{sheets_base}/values/{sheet_range}', headers=headers, timeout=30)
        r.raise_for_status()
        data = r.json()
        rows = data.get('values', [])

        # Auto-convert first row to headers if it looks like a header row
        if rows and len(rows) > 1:
            headers_row = rows[0]
            records = [dict(zip(headers_row, row)) for row in rows[1:]]
            return {'rows': rows, 'records': records, 'count': len(records), 'range': data.get('range')}

        return {'rows': rows, 'count': len(rows), 'range': data.get('range')}

    elif action == 'write_range':
        values_raw = _render(config.get('values_json', '[]'), context, creds)
        try:
            values = json.loads(values_raw)
        except:
            raise ValueError("Google Sheets write_range: values_json must be valid JSON array")

        body = {'values': values, 'majorDimension': 'ROWS'}
        r = httpx.put(f'{sheets_base}/values/{sheet_range}',
                      headers=headers, json={**body, 'valueInputOption': 'USER_ENTERED'}, timeout=30)
        r.raise_for_status()
        return r.json()

    elif action == 'append_rows':
        values_raw = _render(config.get('values_json', '[]'), context, creds)
        try:
            values = json.loads(values_raw)
        except:
            raise ValueError("Google Sheets append_rows: values_json must be valid JSON array")

        r = httpx.post(f'{sheets_base}/values/{sheet_range}:append',
                       headers=headers,
                       json={'values': values, 'majorDimension': 'ROWS'},
                       params={'valueInputOption': 'USER_ENTERED', 'insertDataOption': 'INSERT_ROWS'},
                       timeout=30)
        r.raise_for_status()
        return r.json()

    elif action == 'clear_range':
        r = httpx.post(f'{sheets_base}/values/{sheet_range}:clear', headers=headers, timeout=30)
        r.raise_for_status()
        return r.json()

    else:
        raise ValueError(f"Google Sheets: unknown action '{action}'")

ACTION_GOOGLE_SHEETS_EOF

# nodes/action_http_request.py
cat > "$PROJECT/app/nodes/action_http_request.py" << 'ACTION_HTTP_REQUEST_EOF'
"""HTTP request action node."""
import json
from app.nodes._utils import _render

NODE_TYPE = "action.http_request"
LABEL = "HTTP Request"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Execute HTTP request and return response."""
    import httpx

    url = _render(config.get('url', ''), context, creds)
    method = config.get('method', 'GET').upper()
    headers = {}
    body = None

    if config.get('headers_json'):
        try:
            headers = json.loads(_render(config['headers_json'], context, creds))
        except:
            pass

    if config.get('body_json'):
        try:
            body = json.loads(_render(config['body_json'], context, creds))
        except:
            body = _render(config['body_json'], context, creds)

    r = httpx.request(method, url, headers=headers,
                      json=body if isinstance(body, dict) else None,
                      content=body if isinstance(body, str) else None,
                      timeout=30)
    r.raise_for_status()

    try:
        rbody = r.json()
    except:
        rbody = r.text

    return {'status': r.status_code, 'body': rbody, 'headers': dict(r.headers)}

ACTION_HTTP_REQUEST_EOF

# nodes/action_llm_call.py
cat > "$PROJECT/app/nodes/action_llm_call.py" << 'ACTION_LLM_CALL_EOF'
"""LLM call action node."""
import os
from app.nodes._utils import _render

NODE_TYPE = "action.llm_call"
LABEL = "LLM Call"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Call OpenAI-compatible LLM API."""
    import httpx

    model = _render(config.get('model', 'gpt-4o-mini'), context, creds)
    prompt = _render(config.get('prompt', ''), context, creds)
    system = _render(config.get('system', 'You are a helpful assistant.'), context, creds)
    api_key = _render(config.get('api_key', ''), context, creds) or os.environ.get('OPENAI_API_KEY', '')
    api_base = _render(config.get('api_base', ''), context, creds) or 'https://api.openai.com/v1'

    if not api_key:
        raise ValueError("LLM Call: no api_key configured and OPENAI_API_KEY env not set")

    resp = httpx.post(
        f"{api_base.rstrip('/')}/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": prompt}
            ]
        },
        timeout=60
    )
    resp.raise_for_status()
    data = resp.json()
    reply = data['choices'][0]['message']['content']
    tokens = data.get('usage', {}).get('total_tokens', 0)

    return {'response': reply, 'model': model, 'tokens': tokens}

ACTION_LLM_CALL_EOF

# nodes/action_log.py
cat > "$PROJECT/app/nodes/action_log.py" << 'ACTION_LOG_EOF'
"""Log action node."""
from app.nodes._utils import _render

NODE_TYPE = "action.log"
LABEL = "Log"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Log a message to the execution logs."""
    msg = _render(config.get('message', ''), context, creds)
    logger(f"LOG: {msg}")
    return {'logged': msg}

ACTION_LOG_EOF

# nodes/action_loop.py
cat > "$PROJECT/app/nodes/action_loop.py" << 'ACTION_LOOP_EOF'
"""Loop action node."""

NODE_TYPE = "action.loop"
LABEL = "Loop"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Prepare items for looping."""
    field = config.get('field', '')
    max_items = int(config.get('max_items', 100))
    items = inp.get(field, inp) if field and isinstance(inp, dict) else inp

    if not isinstance(items, list):
        items = [items]

    items = items[:max_items]

    return {'items': items, 'count': len(items), '__loop__': True}

ACTION_LOOP_EOF

# nodes/action_merge.py
cat > "$PROJECT/app/nodes/action_merge.py" << 'ACTION_MERGE_EOF'
"""Merge / join action node."""

NODE_TYPE = "action.merge"
LABEL = "Merge / Join"


def run(config, inp, context, logger, creds=None, **kwargs):
    """
    Merge outputs from multiple upstream nodes.
    mode=first : pass through first upstream value (default)
    mode=all   : collect all upstream node outputs into a list
    mode=dict  : merge all upstream dicts into one dict (last wins on collision)
    """
    mode = config.get('mode', 'dict')
    upstream_ids = kwargs.get('upstream_ids', [])

    if mode == 'first' or not upstream_ids:
        return inp

    upstream_outputs = [context.get(uid) for uid in upstream_ids if context.get(uid) is not None]

    if mode == 'all':
        return {'merged': upstream_outputs, 'count': len(upstream_outputs)}

    elif mode == 'dict':
        result = {}
        for out in upstream_outputs:
            if isinstance(out, dict):
                result.update(out)
        return result

    else:
        return inp

ACTION_MERGE_EOF

# nodes/action_notion.py
cat > "$PROJECT/app/nodes/action_notion.py" << 'ACTION_NOTION_EOF'
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

ACTION_NOTION_EOF

# nodes/action_run_script.py
cat > "$PROJECT/app/nodes/action_run_script.py" << 'ACTION_RUN_SCRIPT_EOF'
"""Run script action node."""
import os
import time
import json
from app.nodes._utils import _render

NODE_TYPE = "action.run_script"
LABEL = "Run Script"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Execute arbitrary Python code in a sandbox."""
    script = _render(config.get('script', ''), context, creds)
    ns = {
        'input': inp,
        'context': context,
        'result': None,
        'log': logger,
        'json': json,
        'os': os,
        'time': time
    }
    exec(script, ns)
    return ns.get('result', inp)

ACTION_RUN_SCRIPT_EOF

# nodes/action_send_email.py
cat > "$PROJECT/app/nodes/action_send_email.py" << 'ACTION_SEND_EMAIL_EOF'
"""Send email action node."""
import os
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from app.nodes._utils import _render

NODE_TYPE = "action.send_email"
LABEL = "Send Email"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Send email via SMTP."""
    to = _render(config.get('to', ''), context, creds)
    subject = _render(config.get('subject', ''), context, creds)
    body = _render(config.get('body', ''), context, creds)
    host = _render(config.get('smtp_host', ''), context, creds)
    user = _render(config.get('smtp_user', ''), context, creds)
    pwd = _render(config.get('smtp_pass', ''), context, creds)
    port = None

    # Structured credential shortcut: credential field contains the name of an smtp credential
    cred_name = _render(config.get('credential', ''), context, creds)
    if cred_name and creds:
        raw = creds.get(cred_name)
        if raw:
            try:
                c = json.loads(raw)
                host = host or c.get('host', '')
                port = port or c.get('port')
                user = user or c.get('user', '')
                pwd = pwd or c.get('pass', '')
            except (json.JSONDecodeError, AttributeError):
                pass

    host = host or os.environ.get('SMTP_HOST', '')
    user = user or os.environ.get('SMTP_USER', '')
    pwd = pwd or os.environ.get('SMTP_PASS', '')
    smtp_port = int(port or 465)

    if not host:
        raise ValueError("Send Email: no SMTP host configured")

    msg = MIMEMultipart()
    msg['From'] = user
    msg['To'] = to
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    with smtplib.SMTP_SSL(host, smtp_port) as s:
        if user and pwd:
            s.login(user, pwd)
        s.sendmail(user, to, msg.as_string())

    return {'sent': True, 'to': to, 'subject': subject}

ACTION_SEND_EMAIL_EOF

# nodes/action_set_variable.py
cat > "$PROJECT/app/nodes/action_set_variable.py" << 'ACTION_SET_VARIABLE_EOF'
"""Set variable action node."""
from app.nodes._utils import _render

NODE_TYPE = "action.set_variable"
LABEL = "Set Variable"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Set a variable in the execution context."""
    key = config.get('key', 'var')
    val = _render(config.get('value', ''), context, creds)
    context[key] = val
    return {key: val}

ACTION_SET_VARIABLE_EOF

# nodes/action_sftp.py
cat > "$PROJECT/app/nodes/action_sftp.py" << 'ACTION_SFTP_EOF'
"""SFTP/FTP file transfer action node."""
import io
import stat as _stat
import json
from app.nodes._utils import _render

NODE_TYPE = "action.sftp"
LABEL = "SFTP / FTP"


# ── helpers ───────────────────────────────────────────────────────────────

def _sftp_walk(sftp, path, depth=0):
    """Recursively list an SFTP directory tree. Returns a flat list."""
    results = []
    try:
        attrs = sftp.listdir_attr(path)
    except Exception:
        return results
    for a in attrs:
        full_path = path.rstrip('/') + '/' + a.filename
        is_dir = _stat.S_ISDIR(a.st_mode or 0)
        results.append({
            'name':   a.filename,
            'path':   full_path,
            'size':   a.st_size,
            'is_dir': is_dir,
            'depth':  depth,
        })
        if is_dir:
            results.extend(_sftp_walk(sftp, full_path, depth + 1))
    return results


def _ftp_walk(ftp, path, depth=0):
    """Recursively list an FTP directory tree using MLSD (with nlst fallback)."""
    results = []
    try:
        entries = list(ftp.mlsd(path))
        for name, facts in entries:
            if name in ('.', '..'):
                continue
            full_path = path.rstrip('/') + '/' + name
            is_dir = facts.get('type', '').lower() in ('dir', 'cdir', 'pdir')
            results.append({
                'name':   name,
                'path':   full_path,
                'size':   int(facts.get('size', 0) or 0),
                'is_dir': is_dir,
                'depth':  depth,
            })
            if is_dir:
                results.extend(_ftp_walk(ftp, full_path, depth + 1))
    except Exception:
        # Fallback: nlst + cwd trick to detect directories
        try:
            names = ftp.nlst(path)
            for full_path in names:
                name = full_path.rstrip('/').split('/')[-1]
                if not name or name in ('.', '..'):
                    continue
                is_dir = False
                try:
                    orig = ftp.pwd()
                    ftp.cwd(full_path)
                    ftp.cwd(orig)
                    is_dir = True
                except Exception:
                    pass
                results.append({
                    'name':   name,
                    'path':   full_path,
                    'size':   0,
                    'is_dir': is_dir,
                    'depth':  depth,
                })
                if is_dir:
                    results.extend(_ftp_walk(ftp, full_path, depth + 1))
        except Exception:
            pass
    return results


def _ftp_list_flat(ftp, path):
    """Structured flat listing for FTP using MLSD (with nlst fallback)."""
    results = []
    try:
        for name, facts in ftp.mlsd(path):
            if name in ('.', '..'):
                continue
            full_path = path.rstrip('/') + '/' + name
            is_dir = facts.get('type', '').lower() in ('dir', 'cdir', 'pdir')
            results.append({
                'name':   name,
                'path':   full_path,
                'size':   int(facts.get('size', 0) or 0),
                'is_dir': is_dir,
                'depth':  0,
            })
    except Exception:
        # Fallback: nlst only
        try:
            for full_path in ftp.nlst(path):
                name = full_path.rstrip('/').split('/')[-1]
                if not name or name in ('.', '..'):
                    continue
                is_dir = False
                try:
                    orig = ftp.pwd()
                    ftp.cwd(full_path)
                    ftp.cwd(orig)
                    is_dir = True
                except Exception:
                    pass
                results.append({
                    'name':   name,
                    'path':   full_path,
                    'size':   0,
                    'is_dir': is_dir,
                    'depth':  0,
                })
        except Exception:
            pass
    return results


# ── main ──────────────────────────────────────────────────────────────────

def run(config, inp, context, logger, creds=None, **kwargs):
    """Transfer files and manage paths via SFTP or FTP.

    Operations: list, upload, download, delete, mkdir, rename, exists, stat
    """
    protocol    = (config.get('protocol', 'sftp') or 'sftp').lower()
    host        = _render(config.get('host', ''), context, creds)
    port_str    = _render(config.get('port', ''), context, creds)
    username    = _render(config.get('username', ''), context, creds)
    password    = _render(config.get('password', ''), context, creds)
    operation   = (config.get('operation', 'list') or 'list').lower()
    remote_path = _render(config.get('remote_path', '/'), context, creds)
    new_path    = _render(config.get('new_path', ''), context, creds)   # for rename
    content     = _render(config.get('content', ''), context, creds)
    recursive   = str(config.get('recursive', 'false')).lower() in ('true', '1', 'yes')
    timeout     = int(config.get('timeout', 30) or 30)

    # ── credential shortcut ───────────────────────────────────────────────
    cred_name = _render(config.get('credential', ''), context, creds)
    if cred_name and creds:
        raw = creds.get(cred_name)
        if raw:
            try:
                c = json.loads(raw)
                if not config.get('protocol'):
                    protocol = (c.get('protocol', protocol) or protocol).lower()
                host     = host     or c.get('host', '')
                port_str = port_str or str(c.get('port', ''))
                username = username or c.get('username', '')
                password = password or c.get('password', '')
            except (json.JSONDecodeError, AttributeError):
                pass

    default_port = 22 if protocol == 'sftp' else 21
    port = int(port_str or default_port)

    if not host:
        raise ValueError("SFTP node: no host configured")

    # ══════════════════════════════════════════════════════════════════════
    # SFTP
    # ══════════════════════════════════════════════════════════════════════
    if protocol == 'sftp':
        import paramiko

        transport = paramiko.Transport((host, port))
        transport.banner_timeout  = timeout
        transport.handshake_timeout = timeout
        try:
            transport.connect(username=username or None, password=password or None)
            sftp = paramiko.SFTPClient.from_transport(transport)
            try:

                # ── list ──────────────────────────────────────────────────
                if operation == 'list':
                    if recursive:
                        files = _sftp_walk(sftp, remote_path)
                        dirs  = [f for f in files if f['is_dir']]
                        return {
                            'files': files,
                            'count': len(files),
                            'dir_count':  len(dirs),
                            'file_count': len(files) - len(dirs),
                            'path':      remote_path,
                            'recursive': True,
                        }
                    else:
                        attrs = sftp.listdir_attr(remote_path)
                        files = [{
                            'name':   a.filename,
                            'path':   remote_path.rstrip('/') + '/' + a.filename,
                            'size':   a.st_size,
                            'is_dir': _stat.S_ISDIR(a.st_mode or 0),
                            'depth':  0,
                        } for a in attrs]
                        return {'files': files, 'count': len(files), 'path': remote_path, 'recursive': False}

                # ── upload ────────────────────────────────────────────────
                elif operation == 'upload':
                    data = content.encode('utf-8') if isinstance(content, str) else content
                    sftp.putfo(io.BytesIO(data), remote_path)
                    return {'uploaded': True, 'remote_path': remote_path, 'size': len(data)}

                # ── download ──────────────────────────────────────────────
                elif operation == 'download':
                    buf = io.BytesIO()
                    sftp.getfo(remote_path, buf)
                    text = buf.getvalue().decode('utf-8', errors='replace')
                    return {'content': text, 'remote_path': remote_path, 'size': len(text)}

                # ── delete ────────────────────────────────────────────────
                elif operation == 'delete':
                    sftp.remove(remote_path)
                    return {'deleted': True, 'remote_path': remote_path}

                # ── mkdir ─────────────────────────────────────────────────
                elif operation == 'mkdir':
                    sftp.mkdir(remote_path)
                    return {'created': True, 'remote_path': remote_path}

                # ── rename / move ─────────────────────────────────────────
                elif operation == 'rename':
                    if not new_path:
                        raise ValueError("SFTP rename: 'new_path' is required")
                    sftp.rename(remote_path, new_path)
                    return {'renamed': True, 'from': remote_path, 'to': new_path}

                # ── exists ────────────────────────────────────────────────
                elif operation == 'exists':
                    try:
                        a = sftp.stat(remote_path)
                        is_dir = _stat.S_ISDIR(a.st_mode or 0)
                        return {'exists': True, 'is_dir': is_dir, 'path': remote_path}
                    except FileNotFoundError:
                        return {'exists': False, 'is_dir': False, 'path': remote_path}

                # ── stat ──────────────────────────────────────────────────
                elif operation == 'stat':
                    import datetime
                    a = sftp.stat(remote_path)
                    is_dir = _stat.S_ISDIR(a.st_mode or 0)
                    mtime = datetime.datetime.utcfromtimestamp(a.st_mtime).isoformat() + 'Z' if a.st_mtime else None
                    return {
                        'path':     remote_path,
                        'size':     a.st_size,
                        'is_dir':   is_dir,
                        'modified': mtime,
                        'mode':     oct(a.st_mode) if a.st_mode else None,
                    }

                else:
                    raise ValueError(
                        f"SFTP: unknown operation '{operation}'. "
                        "Use: list, upload, download, delete, mkdir, rename, exists, stat"
                    )

            finally:
                sftp.close()
        finally:
            transport.close()

    # ══════════════════════════════════════════════════════════════════════
    # FTP
    # ══════════════════════════════════════════════════════════════════════
    elif protocol == 'ftp':
        import ftplib

        ftp = ftplib.FTP()
        ftp.connect(host, port, timeout=timeout)
        try:
            ftp.login(username or '', password or '')

            # ── list ──────────────────────────────────────────────────────
            if operation == 'list':
                if recursive:
                    files = _ftp_walk(ftp, remote_path)
                    dirs  = [f for f in files if f['is_dir']]
                    return {
                        'files': files,
                        'count': len(files),
                        'dir_count':  len(dirs),
                        'file_count': len(files) - len(dirs),
                        'path':      remote_path,
                        'recursive': True,
                    }
                else:
                    files = _ftp_list_flat(ftp, remote_path)
                    return {'files': files, 'count': len(files), 'path': remote_path, 'recursive': False}

            # ── upload ────────────────────────────────────────────────────
            elif operation == 'upload':
                data = content.encode('utf-8') if isinstance(content, str) else content
                ftp.storbinary(f'STOR {remote_path}', io.BytesIO(data))
                return {'uploaded': True, 'remote_path': remote_path}

            # ── download ──────────────────────────────────────────────────
            elif operation == 'download':
                buf = io.BytesIO()
                ftp.retrbinary(f'RETR {remote_path}', buf.write)
                text = buf.getvalue().decode('utf-8', errors='replace')
                return {'content': text, 'remote_path': remote_path, 'size': len(text)}

            # ── delete ────────────────────────────────────────────────────
            elif operation == 'delete':
                ftp.delete(remote_path)
                return {'deleted': True, 'remote_path': remote_path}

            # ── mkdir ─────────────────────────────────────────────────────
            elif operation == 'mkdir':
                ftp.mkd(remote_path)
                return {'created': True, 'remote_path': remote_path}

            # ── rename / move ─────────────────────────────────────────────
            elif operation == 'rename':
                if not new_path:
                    raise ValueError("FTP rename: 'new_path' is required")
                ftp.rename(remote_path, new_path)
                return {'renamed': True, 'from': remote_path, 'to': new_path}

            # ── exists ────────────────────────────────────────────────────
            elif operation == 'exists':
                is_dir = False
                found  = False
                try:
                    # Try MLST (most reliable)
                    resp = ftp.sendcmd(f'MLST {remote_path}')
                    found  = True
                    is_dir = 'type=dir' in resp.lower()
                except ftplib.error_perm:
                    # Try SIZE (works for files)
                    try:
                        ftp.size(remote_path)
                        found = True
                    except ftplib.error_perm:
                        pass
                    # Try CWD (works for directories)
                    if not found:
                        try:
                            orig = ftp.pwd()
                            ftp.cwd(remote_path)
                            ftp.cwd(orig)
                            found  = True
                            is_dir = True
                        except ftplib.error_perm:
                            pass
                return {'exists': found, 'is_dir': is_dir, 'path': remote_path}

            # ── stat ──────────────────────────────────────────────────────
            elif operation == 'stat':
                size   = None
                mtime  = None
                is_dir = False
                try:
                    resp = ftp.sendcmd(f'MLST {remote_path}')
                    is_dir = 'type=dir' in resp.lower()
                    for part in resp.split(';'):
                        part = part.strip()
                        if part.lower().startswith('size='):
                            size = int(part.split('=')[1])
                        elif part.lower().startswith('modify='):
                            raw = part.split('=')[1].strip()
                            # YYYYMMDDHHMMSS[.sss]
                            import datetime
                            mtime = datetime.datetime.strptime(raw[:14], '%Y%m%d%H%M%S').isoformat() + 'Z'
                except ftplib.error_perm:
                    try:
                        size = ftp.size(remote_path)
                    except Exception:
                        pass
                return {'path': remote_path, 'size': size, 'is_dir': is_dir, 'modified': mtime}

            else:
                raise ValueError(
                    f"FTP: unknown operation '{operation}'. "
                    "Use: list, upload, download, delete, mkdir, rename, exists, stat"
                )

        finally:
            try:
                ftp.quit()
            except Exception:
                pass

    else:
        raise ValueError(f"SFTP node: unknown protocol '{protocol}'. Use sftp or ftp")

ACTION_SFTP_EOF

# nodes/action_slack.py
cat > "$PROJECT/app/nodes/action_slack.py" << 'ACTION_SLACK_EOF'
"""Slack message action node."""
import os
import json
from app.nodes._utils import _render

NODE_TYPE = "action.slack"
LABEL = "Slack"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Send message to Slack webhook."""
    import httpx

    webhook_url = _render(config.get('webhook_url', ''), context, creds)
    message = _render(config.get('message', ''), context, creds)
    channel = _render(config.get('channel', ''), context, creds)

    # Structured credential shortcut (Slack or generic Webhook type)
    cred_name = _render(config.get('credential', ''), context, creds)
    if cred_name and creds and not webhook_url:
        raw = creds.get(cred_name)
        if raw:
            try:
                c = json.loads(raw)
                webhook_url = c.get('webhook_url', '') or c.get('url', '')
            except (json.JSONDecodeError, AttributeError):
                webhook_url = raw  # fallback: raw value is the URL

    if not webhook_url:
        raise ValueError("Slack: no webhook_url configured")
    if not message:
        raise ValueError("Slack: no message configured")

    body = {'text': message}
    if channel:
        body['channel'] = channel

    r = httpx.post(webhook_url, json=body, timeout=10)
    r.raise_for_status()

    return {'sent': True, 'message': message}

ACTION_SLACK_EOF

# nodes/action_ssh.py
cat > "$PROJECT/app/nodes/action_ssh.py" << 'ACTION_SSH_EOF'
"""SSH command action node."""
import os
import json
from app.nodes._utils import _render

NODE_TYPE = "action.ssh"
LABEL = "SSH"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Execute command over SSH."""
    import paramiko

    host = _render(config.get('host', ''), context, creds)
    port_str = _render(config.get('port', ''), context, creds)
    username = _render(config.get('username', ''), context, creds)
    password = _render(config.get('password', ''), context, creds)
    command = _render(config.get('command', ''), context, creds)

    # Structured credential shortcut
    cred_name = _render(config.get('credential', ''), context, creds)
    if cred_name and creds:
        raw = creds.get(cred_name)
        if raw:
            try:
                c = json.loads(raw)
                host = host or c.get('host', '')
                port_str = port_str or str(c.get('port', ''))
                username = username or c.get('username', '')
                password = password or c.get('password', '')
            except (json.JSONDecodeError, AttributeError):
                pass

    port = int(port_str or 22)

    if not host:
        raise ValueError("SSH: no host configured")
    if not command:
        raise ValueError("SSH: no command configured")

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        client.connect(host, port=port, username=username or None,
                       password=password or None, timeout=30)
        _, stdout_f, stderr_f = client.exec_command(command, timeout=60)
        exit_code = stdout_f.channel.recv_exit_status()
        out = stdout_f.read().decode('utf-8', errors='replace').strip()
        err = stderr_f.read().decode('utf-8', errors='replace').strip()
    finally:
        client.close()

    return {'stdout': out, 'stderr': err, 'exit_code': exit_code, 'success': exit_code == 0}

ACTION_SSH_EOF

# nodes/action_telegram.py
cat > "$PROJECT/app/nodes/action_telegram.py" << 'ACTION_TELEGRAM_EOF'
"""Telegram message action node."""
import os
from app.nodes._utils import _render

NODE_TYPE = "action.telegram"
LABEL = "Telegram"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Send message via Telegram bot."""
    import httpx

    text = _render(config.get('text', ''), context, creds)
    token = _render(config.get('bot_token', ''), context, creds) or os.environ.get('TELEGRAM_TOKEN', '')
    chat = _render(config.get('chat_id', ''), context, creds) or os.environ.get('TELEGRAM_CHAT_ID', '')

    if not token or not chat:
        raise ValueError("Telegram: missing bot_token or chat_id")

    r = httpx.post(f"https://api.telegram.org/bot{token}/sendMessage",
                   json={'chat_id': chat, 'text': text}, timeout=10)
    r.raise_for_status()

    return {'sent': True, 'chat_id': chat}

ACTION_TELEGRAM_EOF

# nodes/action_transform.py
cat > "$PROJECT/app/nodes/action_transform.py" << 'ACTION_TRANSFORM_EOF'
"""Transform / evaluate expression action node."""
import json
from app.nodes._utils import _render

NODE_TYPE = "action.transform"
LABEL = "Transform"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Evaluate a Python expression and return result."""
    expr = _render(config.get('expression', 'input'), context, creds)
    safe_builtins = {'len': len, 'str': str, 'int': int, 'float': float, 'bool': bool, 'list': list, 'dict': dict, 'tuple': tuple}
    result = eval(expr, {'__builtins__': safe_builtins}, {'input': inp, 'context': context, 'json': json})
    return result

ACTION_TRANSFORM_EOF

# nodes/trigger_cron.py
cat > "$PROJECT/app/nodes/trigger_cron.py" << 'TRIGGER_CRON_EOF'
"""Cron schedule trigger node."""

NODE_TYPE = "trigger.cron"
LABEL = "Cron Schedule"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Pass through input as-is."""
    return inp

TRIGGER_CRON_EOF

# nodes/trigger_manual.py
cat > "$PROJECT/app/nodes/trigger_manual.py" << 'TRIGGER_MANUAL_EOF'
"""Manual trigger node."""

NODE_TYPE = "trigger.manual"
LABEL = "Manual Trigger"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Pass through input as-is."""
    return inp

TRIGGER_MANUAL_EOF

# nodes/trigger_webhook.py
cat > "$PROJECT/app/nodes/trigger_webhook.py" << 'TRIGGER_WEBHOOK_EOF'
"""Webhook trigger node."""

NODE_TYPE = "trigger.webhook"
LABEL = "Webhook Trigger"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Pass through input as-is."""
    return inp

TRIGGER_WEBHOOK_EOF


# ── 21. Validate + finish ──────────────────────────────────────────────────
if [ ! -f "$PROJECT/app/main.py" ]; then
  echo "✗ Bootstrap failed: main.py not created"
  exit 1
fi

if [ ! -f "$PROJECT/requirements.txt" ]; then
  echo "✗ Bootstrap failed: requirements.txt not created"
  exit 1
fi

echo "✓ All files created. Next: cd $PROJECT && docker-compose up"
echo "✓ Bootstrapped v11"
