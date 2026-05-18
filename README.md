# HiveRunr

A self-hosted workflow automation platform with a visual node-canvas editor, modular node system, and a full admin dashboard. Think n8n, but Python-first and fully under your control.

---

## Stack

| Component | Technology |
|---|---|
| API server | FastAPI (Python 3.11) |
| Task queue | Celery + Redis |
| Database | PostgreSQL |
| Frontend | Vite + React 18, React Flow v11 (multi-page SPA) |
| Reverse proxy | Caddy |
| Worker monitor | Flower |

---

## Quick Start

**One-line install** (requires git + Docker):

```bash
curl -fsSL https://raw.githubusercontent.com/TurnrIO/HiveRunr/main/install.sh | bash
```

This clones the repo, runs the interactive setup wizard, and optionally starts the stack.

**Or manually:**

```bash
git clone https://github.com/TurnrIO/HiveRunr
cd HiveRunr
bash setup.sh          # interactive: generates SECRET_KEY, configures AgentMail.to, etc.
docker compose up -d --build
```

Then open **http://localhost** — on first run you will be prompted to create your owner account.

`setup.sh` now safely reuses existing `.env` values, appends missing keys, and avoids the older prompt-capture bug that could write interactive prompt text into `.env`.

---

## Authentication

HiveRunr uses session-based authentication. No tokens in URLs.

| Path | Access |
|---|---|
| `http://localhost/setup` | First-run owner account creation |
| `http://localhost/login` | Sign in with username + password |
| All other routes | Redirect to `/login` if not authenticated |

### Roles

| Role | Can do |
|---|---|
| **Owner** | Everything including user management and API token generation |
| **Admin** | All operational actions (create/edit/run flows, manage credentials) |
| **Viewer** | Read-only access — view runs, flows, metrics, schedules |

### API tokens

For CI/CD and service-to-service calls, owners can generate API tokens from **Settings → API Tokens**. Pass them via the `x-api-token` header:

```
x-api-token: hr_your_token_here
```

Recent reliability polish in the React admin/canvas UI also tightened session expiry handling: if the authenticated session disappears, the shared auth context now clears stale user state instead of leaving privileged UI visible until a full reload.

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
| `flower` | Celery monitoring UI at `/flower/` (auth-gated) | 5555 |

---

## Caddy Entry Point

The default stack keeps **Caddy in front of the app**. `docker compose up -d --build` exposes Caddy on `http://localhost`, and Caddy reverse-proxies both the FastAPI app and Flower.

For local review on a non-default port while still keeping Caddy in the path, use:

```bash
docker compose --env-file docker-compose.review.env up -d --build
```

That keeps the same service topology but publishes Caddy on `http://localhost:9999`.

---

## URLs

| Path | Description |
|---|---|
| `http://localhost/` | Admin dashboard |
| `http://localhost/canvas` | Visual node canvas editor |
| `http://localhost/flower/` | Flower / Celery monitor (requires login) |
| `http://localhost/health` | Health check — returns `{"status":"ok","version":"x.y.z"}` |
| `http://localhost/api/prometheus` | Prometheus metrics scrape endpoint |

---

## Admin Pages

| Page | What it does |
|---|---|
| **Dashboard** | Live run feed, workflow toggle, manual triggers, run replay |
| **Canvas Flows** | List and manage visual graph flows |
| **Templates** | One-click flow templates by category |
| **Metrics** | Run volume charts, success/failure rates, top failing flows |
| **Scripts** | Manage and edit Python scripts executed by `action.run_script` |
| **Credentials** | Credential store for nodes (SMTP, SSH, Telegram, etc.) |
| **Schedules** | Cron schedule manager with timezone support |
| **Run Logs** | Per-node execution traces with input/output inspector |
| **Users** | Manage users and roles (owner only) |
| **Settings** | API token management, maintenance tools |

The admin UI is a React Router SPA (`/admin`). The canvas editor is a separate Vite entry point (`/canvas`). Both are built to `app/static/dist/` and served by FastAPI.

Recent admin hardening work focused on post-action consistency and clearer failure states. Flow, template, workspace, script, user-management, dashboard, logs, settings, and diagnostics screens now await their refreshes more consistently, preserve good data during quiet background refresh failures, and show explicit load errors instead of silently leaving stale or misleading UI onscreen.

---

## Node Canvas

The visual canvas editor lets you build flows by connecting nodes on a graph. Each node is one step in your automation.

### Trigger nodes

| Node | Description |
|---|---|
| `trigger.manual` | Run manually via the dashboard or API |
| `trigger.webhook` | HTTP webhook with optional HMAC secret + rate limiting |
| `trigger.cron` | Scheduled execution via cron expression |
| `trigger.email` | Poll an IMAP inbox for new messages |
| `trigger.rss` | Poll an RSS/Atom feed for new entries |
| `trigger.file_watch` | Watch a local or SFTP path for new/modified files |

### Action nodes — control flow & data

| Node | Description |
|---|---|
| `action.condition` | True/False branching |
| `action.switch` | Multi-way routing by value |
| `action.filter` | Keep only list items matching an expression |
| `action.loop` | Iterate over an array |
| `action.aggregate` | Collect loop results into a list |
| `action.merge` | Join outputs from multiple upstream nodes |
| `action.transform` | Reshape data with a Python expression |
| `action.set_variable` | Store a value in flow context |
| `action.delay` | Pause for N seconds |
| `action.log` | Write a message to the run trace |
| `action.date` | Date/time operations (format, add, diff, parse) |
| `action.csv` | Parse or generate CSV strings |
| `action.run_script` | Execute a Python script from the Scripts library |
| `action.wait_for_approval` | Pause and send an approval email; resume on Approve/Reject |

### Action nodes — integrations

| Node | Description |
|---|---|
| `action.http_request` | HTTP GET/POST/PUT/DELETE with headers, body, credential |
| `action.llm_call` | OpenAI-compatible LLM chat completion |
| `action.send_email` | Send email via SMTP |
| `action.slack` | Post to Slack via incoming webhook |
| `action.discord` | Post to Discord via incoming webhook |
| `action.telegram` | Send a Telegram bot message |
| `action.linear` | Linear.app — create/update/search issues via GraphQL |
| `action.openai_assistant` | OpenAI Assistants API — threads, messages, runs |
| `action.call_graph` | Invoke another flow as a sub-flow |
| `action.ssh` | Run a command on a remote server over SSH |
| `action.sftp` | Upload, download, or list files over SFTP/FTP |
| `action.github` | GitHub REST API — issues, PRs, releases |
| `action.google_sheets` | Read/write Google Sheets rows |
| `action.notion` | Create/update Notion pages and databases |
| `action.airtable` | Airtable REST API — list, get, create, update, delete records |
| `action.postgres` | PostgreSQL / MySQL / SQLite query |
| `action.mysql` | MySQL / MariaDB query |
| `action.redis` | Redis GET / SET / DEL / LPUSH / RPOP / INCR |
| `action.mongodb` | MongoDB find, insert, update, delete, aggregate |
| `action.s3` | S3-compatible storage — get, put, list, delete, presign |
| `action.graphql` | GraphQL query or mutation |
| `action.pdf` | Render HTML to PDF |
| `action.hubspot` | HubSpot CRM v3 — contacts, deals, search, associate |
| `action.jira` | Jira REST API v3 — issues, comments, transitions, JQL |
| `action.twilio` | Twilio SMS, WhatsApp, and voice calls |

### Node features

- **Retry policy** — configurable max attempts + delay per node
- **Fail mode** — `abort` (default) or `continue` (stores error in context, keeps flow running)
- **Disable/enable** per node without deleting it
- **Quick-start templates** — the empty canvas view can create starter flows directly from built-in templates like Email → Slack, Webhook → Notion, Sheets report, and CSV → Database
- **Live data inspector** — overlay run input/output on any node from a past run
- **Real-time streaming** — nodes colour in as they execute via SSE
- **Undo/redo** — full history within a session
- **Auto-layout** — one-click graph arrangement
- **Version history** — save, diff, and restore named snapshots per flow
- **Import/export** — portable JSON bundles (credential names included, secrets never)

---

## Frontend Development

The frontend lives in `frontend/` and is built with Vite + React.

```bash
cd frontend
npm install
npm run dev        # continuous rebuild → app/static/dist/ (watch mode)
npm run build      # one-shot production build
```

FastAPI serves the built files from `app/static/dist/`. In development, run `npm run dev` in one terminal and `docker compose up api worker scheduler db redis` in another — file changes rebuild automatically and are served immediately (no container restart needed thanks to the bind mount).

Workspace switching in both the admin app and canvas now refreshes in-app state without forcing a full page reload, so multi-workspace testing is much smoother during development.

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

### Custom nodes

Drop a `.py` file into `app/nodes/custom/` and restart the `api` container — it will be auto-discovered at startup.

---

## Flow URLs

Each flow has a unique 8-character slug generated at creation. The canvas URL reflects the currently open flow:

```
/canvas#flow-a3f2e1c9
```

Refreshing the page restores the same flow automatically.

---

## Webhook Rate Limiting

Inbound webhooks are rate-limited per endpoint using a Redis token bucket. Configure via `.env`:

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
| `DATABASE_URL` | PostgreSQL connection string |
| `REDIS_URL` | Redis connection string |
| `SECRET_KEY` | Credential encryption key — see below |
| `API_KEY` | Required for `/webhook/*` endpoints |
| `OPENAI_API_KEY` | Required for `action.llm_call` |
| `SLACK_WEBHOOK_URL` | For `action.slack` and failure notifications |
| `NOTIFY_EMAIL` | Email address for failure alerts |
| `WEBHOOK_RATE_LIMIT` | Max webhook calls per window per endpoint |

---

## Credential Encryption

All credentials stored in HiveRunr are encrypted at rest using **Fernet** (AES-128-CBC + HMAC-SHA256). The encryption key is derived from the `SECRET_KEY` environment variable.

**`setup.sh` generates a unique `SECRET_KEY` automatically** and writes it into `.env` — no manual step required for new installs.

For existing installs, generate a key and add it to `.env`:

```bash
python3 -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"
# → paste the output as SECRET_KEY=... in your .env
docker compose restart api worker scheduler
```

**Important:**
- Back up your `SECRET_KEY`. If it is lost or changed after credentials are saved, those credentials cannot be decrypted and must be re-entered.
- If `SECRET_KEY` is not set, the app will start but will log a `WARNING` on every credential access and the Credentials page will show an amber warning banner.
- Existing plaintext credentials (from before encryption was introduced) are read transparently and re-encrypted the next time they are saved.

---

## Database Migrations

HiveRunr uses **Alembic** for schema management. Migrations run automatically at startup — no manual steps needed during normal development.

```bash
# Apply all pending migrations (runs automatically on container start)
docker compose exec api alembic upgrade head

# Check current migration state
docker compose exec api alembic current

# Create a new migration after changing the schema
docker compose exec api alembic revision -m "add my_new_column"
# Edit the generated file in migrations/versions/, then commit it.

# Roll back one step (rarely needed)
docker compose exec api alembic downgrade -1
```

Migration files live in `migrations/versions/`. Each file has an `upgrade()` and a `downgrade()` function. The initial migration (`0001_initial_schema.py`) is idempotent — safe to run against a database that was already created by an older version of HiveRunr.

---

## Docker Operations

```bash
# Start (development — hot-reload, source mounts)
docker compose up -d --build

# Start (production — no --reload, resource limits, non-root user)
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build

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

### Production overlay (`docker-compose.prod.yml`)

Merge `docker-compose.prod.yml` on top of the default file for a production-safe stack:

- API runs with `--workers 2` instead of `--reload`
- Source-code volume mounts are removed — code is baked into the image
- CPU and memory limits are applied to all services
- All services restart automatically after a host reboot (`restart: unless-stopped`)

### HA Scheduler

Running multiple `scheduler` replicas is safe — they use a Redis distributed lock so only one instance fires jobs at a time. If the active scheduler crashes, a standby takes over within ~45 seconds (configurable via `SCHEDULER_LOCK_TTL_MS`). Scale with:

```bash
docker compose up -d --scale scheduler=2
```

---

## Security Notes

- On first run, create an owner account immediately — the `/setup` page is only available before any user exists.
- `setup.sh` auto-generates a unique `SECRET_KEY` in `.env` — back it up, changing it later makes existing credentials unreadable.
- Use HTTPS (update the Caddyfile) before exposing HiveRunr to the internet.
- API tokens are generated per-owner from Settings and passed via `x-api-token` header — never put tokens in URLs.
- `action.run_script` executes arbitrary Python as the container user. Only grant admin/owner roles to users you trust.
- **SSRF protection** — `action.http_request`, `action.mongodb`, `action.postgres` all resolve hostnames and block RFC1918/loopback/multicast IP ranges.
- **Webhook HMAC verification** — `trigger.webhook` validates `X-Hub-Signature-256` HMAC-SHA256 when a secret is configured.
- **Credential encryption** — all credentials stored in HiveRunr are encrypted at rest using Fernet (AES-128-CBC + HMAC-SHA256).
- **Observability** — OpenTelemetry traces are emitted for graph runs, individual node execution, and Celery tasks when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.

---

## Setup Script

`setup.sh` is a one-time helper run after cloning. It copies `.env.example` to `.env` and generates a unique `SECRET_KEY`:

```bash
bash setup.sh
# edit .env as needed, then:
docker compose up -d --build
```

It requires either `python3` (stdlib only) or `openssl` to generate the key — both are available on any standard Linux/macOS system.
