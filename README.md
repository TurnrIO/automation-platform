# HiveRunr

A self-hosted workflow automation platform with a visual node-canvas editor, modular node system, and a full admin dashboard. Think n8n, but Python-first and fully under your control.

---

## Stack

| Component | Technology |
|---|---|
| API server | FastAPI (Python 3.11) |
| Task queue | Celery + Redis |
| Database | PostgreSQL |
| Frontend | React 18 (UMD/Babel), React Flow v11 |
| Reverse proxy | Caddy |
| Worker monitor | Flower |

---

## Quick Start

```bash
# 1. Run the bootstrap — generates all project files from scratch
bash bootstrap.sh /your/path/hiverunr

# 2. Copy and configure environment
cd /your/path/hiverunr
cp .env.example .env
# Edit .env — at minimum set API_KEY and ADMIN_TOKEN

# 3. Start the stack
docker compose up -d --build
```

Then open **http://localhost/admin?token=your_admin_token**

---

## Services

| Service | Description | Internal port |
|---|---|---|
| `caddy` | Reverse proxy — single entry on port 80 | 80 |
| `api` | FastAPI app — UI + REST API | 8000 |
| `worker` | Celery worker — executes flow runs | — |
| `scheduler` | DB-driven cron scheduler | — |
| `db` | PostgreSQL | 5432 |
| `redis` | Celery broker + result backend | 6379 |
| `flower` | Celery monitoring UI at `/flower/` | 5555 |

---

## URLs

| Path | Description |
|---|---|
| `http://localhost/` | Admin dashboard |
| `http://localhost/admin?token=...` | Admin dashboard (explicit) |
| `http://localhost/canvas?token=...` | Visual node canvas editor |
| `http://localhost/flower/` | Flower / Celery monitor |
| `http://localhost/docs` | Swagger API docs |
| `http://localhost/health` | Health check |

---

## Admin Pages

| Page | What it does |
|---|---|
| **Dashboard** | Live run feed, workflow toggle, manual triggers, run replay |
| **Canvas Flows** | List and manage visual graph flows |
| **Templates** | One-click flow templates by category |
| **Metrics** | Run volume charts, success/failure rates, top failing flows |
| **Scripts** | Manage and edit Python scripts executed by `action.run_script` |
| **Credentials** | Encrypted credential store for nodes (SMTP, SSH, Telegram, etc.) |
| **Schedules** | Cron schedule manager with timezone support |
| **Run Logs** | Per-node execution traces with input/output inspector |
| **Settings** | Token info, maintenance tools |

URL state is preserved via hash routing — refreshing the page restores your last location.

---

## Node Canvas

The visual canvas editor lets you build flows by connecting nodes on a graph. Each node is one step in your automation.

### Trigger nodes

| Node | Description |
|---|---|
| `trigger.manual` | Run manually via the dashboard or API |
| `trigger.webhook` | HTTP webhook with optional secret + rate limiting |
| `trigger.cron` | Scheduled execution via cron expression |

### Action nodes

| Node | Description |
|---|---|
| `action.http_request` | HTTP GET/POST/PUT/DELETE with headers and body |
| `action.transform` | Expression to reshape data |
| `action.condition` | True/If-Else branching — skips the un-taken branch |
| `action.filter` | Stop execution if a condition is not met |
| `action.log` | Write a message to the run trace |
| `action.set_variable` | Store a value in flow context |
| `action.delay` | Pause execution for N seconds |
| `action.run_script` | Execute a Python script from the Scripts library |
| `action.llm_call` | OpenAI chat completion with prompt templating |
| `action.send_email` | Send email via SMTP |
| `action.telegram` | Send a Telegram message |
| `action.slack` | Post to Slack via incoming webhook |
| `action.loop` | Iterate over an array and run sub-nodes per item |
| `action.call_graph` | Invoke another flow as a sub-flow |
| `action.ssh` | Run a command on a remote server over SSH |
| `action.sftp` | Upload, download, or list files over SFTP |
| `action.github` | GitHub API — issues, PRs, releases |
| `action.google_sheets` | Read/write Google Sheets rows |
| `action.notion` | Create/update Notion pages |
| `action.merge` | Merge outputs from multiple upstream nodes |

### Node features

- **Retry policy** — configurable max attempts + delay per node
- **Fail mode** — `abort` (default) or `continue` (stores error in context, keeps flow running)
- **Disable/enable** per node without deleting it
- **Live data inspector** — overlay run input/output on any node from a past run
- **Undo/redo** — full history within a session
- **Auto-layout** — one-click graph arrangement
- **Version history** — save and restore named snapshots per flow

---

## Modular Node System

Nodes live in `app/nodes/` as individual Python files. The registry auto-discovers them at startup — no central registry file to edit.

Each node exports:

```python
NODE_TYPE = "action.example"
LABEL     = "Example"

def run(config, inp, context, logger, creds=None, **kwargs):
    # config  — dict of node config values from the canvas
    # inp     — output from the previous node
    # context — dict of all previous node outputs, keyed by node ID
    # logger  — structured logger (writes to run trace)
    # creds   — resolved credentials dict (if any)
    return {"result": "..."}
```

### Hot-loadable custom nodes

Drop a `.py` file into `app/nodes/custom/` and call `POST /api/admin/reload_nodes` — no container restart required.

---

## Flow URLs

Each flow has a unique 8-character slug generated at creation. The canvas URL reflects the currently open flow:

```
/canvas?token=...#flow-a3f2e1c9
```

Refreshing the page restores the same flow automatically.

---

## Webhook Rate Limiting

Inbound webhooks are rate-limited per token using a Redis token bucket. Configure via `.env`:

```
WEBHOOK_RATE_LIMIT=60    # max requests per window (0 = disabled)
WEBHOOK_RATE_WINDOW=60   # window in seconds
```

---

## Run Replay

Any completed run can be re-enqueued with its original payload via the dashboard (▶ Replay button) or API:

```
POST /api/runs/{id}/replay
```

---

## Environment Variables

See `.env.example` for the full list. Key variables:

| Variable | Description |
|---|---|
| `API_KEY` | Required for `/webhook/*` endpoints |
| `ADMIN_TOKEN` | Required for all admin UI + API routes |
| `DATABASE_URL` | PostgreSQL connection string |
| `REDIS_URL` | Redis connection string |
| `OPENAI_API_KEY` | Required for `action.llm_call` |
| `SLACK_WEBHOOK_URL` | For `action.slack` and failure notifications |
| `NOTIFY_EMAIL` | Email address for failure alerts |
| `WEBHOOK_RATE_LIMIT` | Max webhook calls per window per token |

---

## Docker Operations

```bash
# Start
docker compose up -d --build

# View logs
docker compose logs -f api
docker compose logs -f worker

# Restart a service
docker compose restart api

# Stop (keep data)
docker compose down

# Stop and wipe all volumes
docker compose down -v
```

---

## Security Notes

- Change `ADMIN_TOKEN` and `API_KEY` before any public deployment.
- Put Caddy behind HTTPS when exposing to the internet.
- The admin routes are protected by `ADMIN_TOKEN` only — consider adding IP allowlisting for production.

---

## Bootstrap

The entire project is generated from `bootstrap.sh`. Running it recreates all source files from scratch:

```bash
bash bootstrap.sh /path/to/deploy/dir
cd /path/to/deploy/dir
cp .env.example .env
# configure .env
docker compose up -d --build
```
