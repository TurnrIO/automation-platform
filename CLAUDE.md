# HiveRunr — Claude Context File

> Read this at the start of every session. Update the backlog section when a sprint ships.

---

## Stack

| Layer | Tech |
|-------|------|
| API | FastAPI + uvicorn (`--reload`), Python 3.11 |
| Workers | Celery + Redis (broker + result backend) |
| DB | PostgreSQL via psycopg2 (`RealDictCursor`) + Alembic migrations |
| Scheduler | APScheduler `BlockingScheduler` + Redis leader-lock |
| Frontend | **Vite + React** multi-page app (F-series complete; ongoing polish tracked below) |
| Reverse proxy | Caddy |
| Email | AgentMail.to REST API (`POST /v0/inboxes/{inbox_id}/messages/send`, Bearer auth) |
| Container | Docker Compose — bind-mount `./app:/app/app` so host files are live |

---

## Repo layout (key paths)

```
app/
  core/db.py          — all DB helpers (psycopg2, RealDictCursor)
  routers/            — FastAPI routers (auth, graphs, runs, nodes, tokens, …)
  static/             — bundled frontend output and other static assets served by FastAPI
  static/dist/        — Vite build output (served by FastAPI in production)
  worker.py           — Celery tasks, alert/webhook dispatch
  scheduler.py        — APScheduler entry point + nightly jobs
  email.py            — AgentMail.to send helpers
  main.py             — FastAPI app + middleware
  nodes/              — built-in node modules (one file per node type)
  nodes/custom/       — hot-reloadable custom nodes (no restart needed)
migrations/versions/  — Alembic migration files (0001 … 0011)
frontend/             — Vite + React source (see Frontend architecture below)
install.sh            — curl | bash one-liner installer
setup.sh              — interactive first-run configurator
```

---

## Frontend architecture

The frontend now runs as a Vite + React multi-page application. The old
monolithic `admin.html` and `canvas.html` files have been deleted from the app
source; FastAPI serves the built assets from `app/static/dist/`.

**Key decisions:**

| Decision | Choice |
|----------|--------|
| Build tool | Vite with `@vitejs/plugin-react` |
| Language | JSX (plain JS — no TypeScript yet) |
| Routing | React Router v6 (admin SPA); standalone pages for auth/canvas |
| CSS | CSS Modules for new files; inline styles preserved during migration |
| State | React Context for workspace/user/toast; local useState elsewhere |
| Output | `app/static/dist/` — FastAPI's existing StaticFiles mount serves it |
| Dev mode | `npm run dev` (`vite build --watch`) — continuous rebuild, FastAPI serves dist/ |

**Directory structure:**
```
frontend/
  src/
    api/
      client.js             — the api() fetch helper (single source of truth)
    components/             — shared components used across multiple pages
      Toast.jsx
      ConfirmModal.jsx
      useFocusTrap.js
      StatusDot.jsx
      ViewerBanner.jsx
      RoleBadge.jsx
      ReplayEditModal.jsx
      TraceRow.jsx
    contexts/
      WorkspaceContext.jsx
      AuthContext.jsx
    pages/
      admin/                — one file per admin page
        Dashboard.jsx
        Flows.jsx
        Runs.jsx
        Credentials.jsx
        Schedules.jsx
        Settings.jsx
        AuditLog.jsx
        System.jsx
        Workspaces.jsx
        Users.jsx
        Templates.jsx
      auth/                 — standalone auth pages
        Login.jsx
        Signup.jsx
        Reset.jsx
        Invite.jsx
      canvas/               — canvas editor components
        CanvasApp.jsx
        ConfigPanel.jsx
        NodeEditorModal.jsx
        HistoryModal.jsx
        OpenModal.jsx
        Palette.jsx
        nodeDefs.js
        ... (see F7–F9)
    admin/
      App.jsx               — React Router root + AdminLayout
      index.jsx             — entry point
    canvas/
      index.jsx             — entry point
  admin.html                — Vite HTML entry for admin SPA
  canvas.html               — Vite HTML entry for canvas
  login.html / signup.html / reset.html / invite.html
  vite.config.js
  package.json
```

**NODE_DEFS note:** Add new nodes to
`frontend/src/pages/canvas/nodeDefs.js`. Canvas field hints and help panels now
live alongside the React canvas components, primarily in
`frontend/src/pages/canvas/ConfigPanel.jsx`.

---

## Node system

Each node is a Python module in `app/nodes/` that exports:
- `NODE_TYPE: str` — e.g. `"action.http_request"`
- `LABEL: str` — human-readable name
- `run(config, inp, context, logger, creds=None, **kwargs) -> dict`

The registry (`__init__.py`) auto-discovers all modules on startup. Custom nodes in `app/nodes/custom/` are hot-reloadable via the admin API.

Template rendering: `_render(text, context, creds)` in `_utils.py` resolves `{{node_id.field}}` and `{{creds.name.field}}` in any string config value.

**Canvas UI**: each node must also have an entry in
`frontend/src/pages/canvas/nodeDefs.js` with `label`, `icon`, `color`,
`group`, and `fields[]` (each field: `{k, l, ph, mono?, textarea?, secret?,
type?, options?}`). Special help panels and richer field rendering now live in
the React canvas components, mainly `ConfigPanel.jsx` and related modal files.

### Trigger nodes

| NODE_TYPE | Description |
|-----------|-------------|
| `trigger.manual` | Pass-through; started from the UI |
| `trigger.cron` | APScheduler cron expression |
| `trigger.webhook` | HTTP POST trigger; payload becomes input |

### Action nodes — Control flow

| NODE_TYPE | Description | Key config |
|-----------|-------------|------------|
| `action.condition` | Binary branch (true/false handles) | `expression` (Python bool) |
| `action.switch` | Multi-way routing (N cases) | `value` expr, `cases` JSON array `[{match,label}]` |
| `action.loop` | Iterate over a list; body handle per item | `field`, `max_items` |
| `action.aggregate` | Collect loop-body results into one list | `field` (extract sub-field), `mode` (list/dict/concat) |
| `action.filter` | Keep list items matching a Python expression | `field`, `expression` (use `item`) |
| `action.merge` | Join outputs from multiple upstream nodes | `mode` (first/all/dict) |
| `action.delay` | Sleep N seconds | `seconds` |

### Action nodes — Data

| NODE_TYPE | Description | Key config |
|-----------|-------------|------------|
| `action.transform` | Evaluate Python expression; full `input` + `context` + `json` available | `expression` |
| `action.set_variable` | Write a key into the run context | `key`, `value` |
| `action.log` | Emit a message to the run log | `message` |
| `action.date` | Date/time operations | `operation` (now/format/add/subtract/parse/diff), `date`, `format`, `amount`, `unit` |
| `action.csv` | Parse CSV string → list of dicts, or list → CSV string | `operation` (parse/generate), `content`/`field`, `delimiter` |

### Action nodes — Network / Integration

| NODE_TYPE | Description | Key config |
|-----------|-------------|------------|
| `action.http_request` | Generic HTTP call | `url`, `method`, `headers_json`, `body_json`, `credential`, `timeout`, `ignore_errors` |
| `action.llm_call` | OpenAI-compatible LLM API | `model`, `prompt`, `system`, `api_key`, `api_base` |
| `action.run_script` | Arbitrary Python (opt-in, `ENABLE_RUN_SCRIPT=true`) | `script` |
| `action.call_graph` | Run another flow as subroutine | `graph_id`, `payload` |
| `action.send_email` | SMTP email | `to`, `subject`, `body`, `credential` |
| `action.slack` | Slack incoming webhook | `credential`/`webhook_url`, `message`, `channel` |
| `action.discord` | Discord incoming webhook | `credential`/`webhook_url`, `message`, `username` |
| `action.telegram` | Telegram bot message | `bot_token`, `chat_id`, `text` |
| `action.github` | GitHub REST API | `credential`/`token`, `repo`, `action` |
| `action.notion` | Notion API | `credential`/`token`, `action`, `database_id` |
| `action.google_sheets` | Sheets API via service account | `credential`, `spreadsheet_id`, `action`, `range` |
| `action.sftp` | SFTP/FTP file operations | `credential`, `protocol`, `operation`, `remote_path` |
| `action.ssh` | SSH remote command | `credential`, `host`, `command` |

---

## Critical gotchas

- **RealDictCursor** — rows are dicts, never use `row[0]`; use column name `row["col"]`. Always alias `SELECT COUNT(*) AS n` etc.
- **BaseHTTPMiddleware swallows exceptions** — PrometheusMiddleware catches handler errors before they reach uvicorn's logger. A 500 may leave no log trace; check the response body directly (Network tab / curl).
- **Bind-mount + --reload** — editing host files takes effect immediately; no rebuild needed.
- **Alembic "Will assume transactional DDL"** — this is normal. It means Alembic connected and found nothing to migrate (already at head). Not a hang.
- **configure_logging() suppresses uvicorn access logs** — "Application startup complete" won't appear; check `/health` instead.
- **JSX Fragments `<>…</>`** — every opened `<>` needs a matching `</>` before the closing `);`. A missing close breaks the Babel parser for the entire remainder of the `<script>` block.
- **AgentMail.to** — `inbox_id` = full `AGENTMAIL_FROM` address (e.g. `alerts@agentmail.to`). Endpoint: `POST /v0/inboxes/{inbox_id}/messages/send`. Auth: `Authorization: Bearer <AGENTMAIL_API_KEY>`.
- **app_settings KV table** — generic key/value store for system config (added in 0005). Use `get_setting(key, default)` / `set_setting(key, value)` rather than adding new columns for simple scalar config.
- **NODE_DEFS live in the React canvas now** — every new node needs both a
  Python module in `app/nodes/` AND a `NODE_DEFS` entry in
  `frontend/src/pages/canvas/nodeDefs.js`. If the node needs richer help or
  custom field behavior, add it in the React canvas components such as
  `ConfigPanel.jsx` or `NodeEditorModal.jsx`.

---

## .env variables (current full set)

```
DATABASE_URL=
REDIS_URL=
SECRET_KEY=
OWNER_USERNAME=
OWNER_PASSWORD=
APP_URL=http://localhost
AGENTMAIL_API_KEY=
AGENTMAIL_FROM=
OWNER_EMAIL=
```

---

## Completed sprints

| # | Summary |
|---|---------|
| Bugs | Fixed silent 500 on `/api/runs` (RealDictCursor `[0]` → `["n"]`); fixed canvas black screen (missing JSX Fragment close in `HistoryModal`) |
| 7 | Per-flow alerting (email recipients + webhook URL + alert-on-success toggle); forgot-password / reset-password flow for owner via AgentMail; login page updated with forgot-password form |
| Install | `curl … \| bash` installer (`install.sh` + updated `setup.sh` with AgentMail prompts, health-check loop, docker compose v1/v2 detection) |
| 8 | Run retention policy — `app_settings` KV table (migration 0005), `GET/PUT /api/runs/retention`, nightly `_auto_trim_runs()` scheduler job at 03:00, Settings UI card with enable toggle / mode selector (count vs age) / Save + Trim now buttons |
| 9 | New node types: `action.switch`, `action.aggregate`, `action.date`, `action.discord`, `action.csv`; improved `action.http_request` (credential, timeout, ignore_errors, returns status_code); updated NODE_DEFS + hint panels in canvas.html |
| 10 | Scheduler UI — `list_schedules()` now joins graph name + last-run status/time/duration; `GET /api/schedules` returns enriched rows; new `POST /api/schedules/{sid}/run-now` endpoint; Schedules page adds ▶ Run now button, Last run column, graph name display; fixed ruff F841 lint error in action_http_request.py |
| 10b | Scheduler fixes — bug: `list_schedules()` LATERAL query crashed because `duration_ms` is not a real column (must compute from `updated_at - created_at`); `DateTimePicker` split into separate `date` + `time` inputs with `colorScheme:"dark"` for native calendar/clock pickers; quick-pick shortcuts kept; `APP_TIMEZONE` env var added to `.env.example` + `setup.sh` prompt (auto-detects system tz); exposed as `system.app_timezone` in `/api/system/status`; Schedules page defaults timezone to server setting → browser `Intl` API → UTC |
| 11 | Audit log — migration `0006_audit_log.py` creates `audit_log` table (actor, action, target_type, target_id, detail JSONB, ip, created_at); `log_audit()` fire-and-forget helper + `get_audit_log()` query in `db.py`; 21 audit points hooked across routers: graph.create/update/delete/run/duplicate/restore_version (graphs.py), run.delete/clear_all/trim/cancel/replay + settings.retention (runs.py), user.create/update_role/reset_password/delete + token.create/delete (auth.py), admin.reset/reset_sequences (admin.py); `GET /api/audit-log` endpoint (owner-only, filterable by actor + action prefix, paginated); new **Audit Log** nav page in admin.html with colour-coded action badges, detail expansion, actor/action filters, and prev/next pagination |
| 13 | Flow versioning UI — `HistoryModal` rebuilt as two-pane layout: left side lists all versions with CURRENT badge on latest; right side shows preview (node count, edge count, scrollable node list with type labels) loaded from `GET /api/graphs/{id}/versions/{vid}`; Restore button triggers in-place canvas reload via `onRestored` callback (no page reload); restore is disabled when the current version is already selected |
| 12 | Real-time log streaming — `executor.py` gains `node_callback` hook (called after each node with `node_start`/`node_done` events, zero breaking changes); `worker.py` `enqueue_graph` creates `_make_run_publisher()` for Redis pub/sub on channel `run:{task_id}:stream`, passes `_streaming_logger` + `_node_callback` to `run_graph`; `GET /api/runs/{task_id}/stream` SSE endpoint in `runs.py` — subscribes to Redis, replays from DB traces if run already finished, DB-poll fallback every 3 s if pub/sub missed, 15 s heartbeat comments, `X-Accel-Buffering: no` header; canvas replaces 800 ms setInterval poll with `EventSource` — nodes flip to "running" pulse on `node_start`, colour in real time on `node_done`, finalises on `run_done`; full polling fallback kept for EventSource init failures |
| 14 | Role improvements — migration `0007_flow_permissions.py` adds `flow_permissions` table `(user_id, graph_id, role)` + `invite_tokens` table; `FLOW_ROLE_LEVELS` hierarchy (viewer < runner < editor) in `db.py`; `_check_flow_access(request, graph_id, required_role)` dep added to `deps.py`; `GET /api/graphs` filters list for viewers to only their permitted flows; graph endpoints enforce per-flow roles (view/run/edit); new `GET/PUT /api/graphs/{id}/permissions` + `DELETE /api/graphs/{id}/permissions/{uid}` endpoints (admin/owner only); `POST /api/graphs/{id}/invite` sends AgentMail invite or returns link if email unconfigured; `GET /api/invite/accept` + `POST /api/invite/signup` endpoints in `auth.py`; `invite.html` accept/signup page; `PermissionsModal` component in canvas.html (accessible via ⋯ menu for admin/owner) with user list + role dropdowns + invite-by-email form with copyable fallback link |
| W1 | Workspace foundation — migration `0008_workspaces.py` creates `workspaces` (id, name, slug, plan) + `workspace_members` (workspace_id, user_id, role) tables; backfill seeds a "default" workspace and adds all existing users as members preserving their global role; `WORKSPACE_ROLE_LEVELS` + full CRUD helpers in `db.py` (`create_workspace`, `get/list/update/delete_workspace`, `list/get/set/remove_workspace_member`, `list_user_workspaces`, `get_default_workspace`); `app/routers/workspaces.py` — `GET/POST /api/workspaces`, `GET/PATCH/DELETE /api/workspaces/{id}`, `GET/PUT /api/workspaces/{id}/members`, `DELETE /api/workspaces/{id}/members/{uid}`, `GET /api/workspaces/my/list`; workspace admin guard `_require_workspace_admin()`; registered in `main.py`; app behaviour unchanged — workspace tables sit idle until W2 |
| W2 | Scope graphs + runs to workspace — migration `0009_workspace_graphs_runs.py` adds `workspace_id` FK to `graph_workflows` + `runs` with default-workspace backfill + indexes; `list_graphs(workspace_id)`, `create_graph(..., workspace_id)`, `list_runs(..., workspace_id)`, `duplicate_graph` propagates workspace; `_resolve_workspace(request, user)` helper in `deps.py` (resolution order: `X-Workspace-Id` header → `hr_workspace` cookie → user's first workspace → default); `POST /api/workspaces/{id}/switch` sets `hr_workspace` cookie; `graphs.py` + `runs.py` resolve and pass workspace_id; run INSERT stamped with workspace_id; canvas `api()` helper sends `X-Workspace-Id` header + workspace indicator/switcher in topbar; admin `api()` helper workspace-aware + workspace selector in sidebar |
| W3 | Scope credentials, schedules, tokens to workspace — migration `0010_workspace_credentials_schedules_tokens.py` adds `workspace_id` FK + index to `credentials`, `schedules`, `api_tokens` with default-workspace backfill; `list_credentials(workspace_id)`, `load_all_credentials(workspace_id)`, `upsert_credential(..., workspace_id)`, `list_schedules(workspace_id)`, `create_schedule(..., workspace_id)`, `list_api_tokens(workspace_id)`, `create_api_token(..., workspace_id)` in `db.py`; `credentials.py` + `schedules.py` + `auth.py` resolve workspace and pass it; `executor.py` `run_graph(workspace_id=)` passes workspace to `load_all_credentials` so graphs only access their workspace's secrets; `worker.py` `enqueue_graph` passes `g.get('workspace_id')` to `run_graph`; `scheduler.py` `_make_job` pre-creates run record stamped with `workspace_id` when APScheduler auto-fires cron jobs |

| W4 | Workspace management UI — new `WorkspacesPage` component in admin.html; current workspace rename + members table with role dropdowns + add/remove member; all-workspaces super-admin table with create-new-workspace form, switch, and delete; sidebar workspace block gains ⚙ gear button (admin/owner) navigating to Workspaces page |
| W5 | Self-serve onboarding — `ALLOW_SIGNUP` env var gate; `POST /api/auth/signup` creates user (viewer) + personal workspace + workspace owner membership + session cookies; `GET /signup` page route + `app/static/signup.html` (dark-theme matching login.html); login page shows "Create one free" link when `allow_signup=true` in `/api/auth/status`; `SubdomainWorkspaceMiddleware` in `main.py` reads `APP_DOMAIN`+`SUBDOMAIN_ROUTING` env vars, resolves `<slug>.domain` → workspace; `_resolve_workspace` checks subdomain context first (priority 0); `PLAN_LIMITS` dict + `get_workspace_usage()` in `db.py`; `GET /api/workspaces/{id}/usage` endpoint; WorkspacesPage gains Plan & Usage card with progress bars per limit; `.env.example` + `setup.sh` updated with `ALLOW_SIGNUP`, `SUBDOMAIN_ROUTING`, `APP_DOMAIN` |
| Email fix | Fixed AgentMail send endpoint: `inbox_id` is now full `AGENTMAIL_FROM` address (not just local-part); endpoint changed from `/messages` to `/messages/send`; forgot-password handler now returns HTTP 503 when delivery fails instead of silently swallowing the error |
| P0 | Security + reliability hardening: API_KEY fail-closed (disabled when unset, warning for `dev_api_key`); `is_secure_context()` helper drives `secure=True` on all session cookies; startup exceptions now logged (no silent swallow); `GET /api/runs/stats` server-side aggregates replace client-side `?page_size=200` on Dashboard + Metrics pages |
| P1-9 | Canvas dirty-state + autosave: amber "● Unsaved" badge; autosave 30 s after last change (setTimeout, resets on each edit, existing graphs only); "✓ Saved" timestamp tooltip; Save button turns amber when dirty; `uid()` upgraded to RFC 4122 v4 UUID via `crypto.randomUUID()` |
| P1-10 | Canvas node test button: already implemented; fixed workspace isolation bug — `load_all_credentials()` now receives `workspace_id` so test runs only access the correct workspace's credentials |
| P1-11 | Canvas version diff: `HistoryModal` pre-loads latest version on open; non-current versions show tabbed "⚡ Diff vs current" / "📋 All nodes"; diff is colour-coded `+`/`−`/`~`/`=` with summary chip bar; defaults to Diff tab when selecting a non-current version |
| P1-12 | Flow import / export: `GET /api/graphs/{id}/export` returns self-contained JSON bundle (`hiverunr_export`, `schema_version`, `graph_data`, `credential_slots` — names only, never values, `exported_at`, `exported_by`) with `Content-Disposition: attachment` header; `POST /api/graphs/import` accepts full bundle or minimal `{name, description, graph_data}`, creates graph in workspace, saves initial version, logs audit; canvas `importFlow()` updated to POST to `/api/graphs/import` |
| P1-13 | New node `trigger.email` (IMAP) — `app/nodes/trigger_email.py`; uses stdlib `imaplib` + `email`; config: credential (host/port/username/password/use_ssl), folder, search_criteria (IMAP search string, default UNSEEN), filter_expression (Python eval with `email` + `re`), max_messages, mark_read; output: `emails[]` list + `count` + first-email shortcut fields at top level; NODE_DEFS entry + hint panels in both canvas.html sections |
| P1-14 | New node `action.postgres` (SQL Query) — `app/nodes/action_postgres.py`; supports PostgreSQL (psycopg2, built-in), MySQL (pymysql, optional), SQLite (stdlib); credential JSON with `dsn` string or individual host/port/username/password/database fields; config: credential, query (template-rendered), params (JSON array for parameterised queries), row_limit; output: `rows[]`, `count`, `row` (first-row shortcut), `columns`, `affected`; NODE_DEFS entry + hint panels in both canvas.html sections |
| P1-15 | New node `action.s3` (S3 Storage) — `app/nodes/action_s3.py`; uses boto3 (added to requirements.txt); credential JSON with `access_key`, `secret_key`, `region`, `endpoint_url` (S3-compatible services: MinIO, R2, B2, etc.); operations: get/put/list/delete/presigned_url/head/copy; config: credential, operation, bucket, key, content, prefix, source_key/dest_key, expires_in, max_keys; NODE_DEFS entry + hint panels in both canvas.html sections |
| P1-16 | New node `trigger.file_watch` (File Watch) — `app/nodes/trigger_file_watch.py`; polls local filesystem or SFTP for recently-modified files; config: path, pattern (glob), lookback_minutes (sliding window), min_age_seconds (write-guard), min_size_bytes, recursive, sftp_credential (blank = local); output: `files[]` list + `count` + first-file `path`/`name` shortcuts; NODE_DEFS entry + hint panels in both canvas.html sections |
| P1-17 | Credential test connection — `POST /api/credentials/{id}/test` in `credentials.py`; auto-detects credential type (SMTP, SFTP, SSH, IMAP, PostgreSQL/MySQL/SQLite, S3/AWS, Telegram, OpenAI) from `type` column then field-based heuristics; runs lightweight probe with 10s timeout; returns `{ok, message, type, latency_ms}`; admin.html Credentials page gains "🔌 Test" button per row; result shown inline as green/red pill with message and latency |
| P2-19 | Versioned releases — `[0.1.0]` CHANGELOG entry covering all W-series + P0 + P1 sprints with upgrade notes (migrations, new env vars, boto3 dep); `v0.1.0` git tag created |
| P2-20 | Test coverage expansion — `tests/test_permissions.py` (26 tests: `_check_admin`, token scope hierarchy, role guards, per-flow access, workspace resolution); `tests/test_executor_failures.py` (26 tests: abort/continue/retry failure modes, condition branching, scheduler lock Lua helpers, edge cases); `tests/test_webhook_ratelimit.py` (12 tests: webhook rate-limit pipeline logic, Redis fail-open, login brute-force lockout/clear); `tests/integration/test_e2e_playwright.py` (Playwright E2E: login flow, dashboard, canvas, logout — auto-skipped unless `HIVERUNR_BASE_URL` set); total unit suite 117 tests, all green |
| P2-21 | Responsive / accessibility pass — `admin.html`: hamburger toggle + sidebar slide-in overlay (`@media ≤1024px`), `useFocusTrap` hook, ConfirmModal/Toast/HistoryModal/AlertSettingsModal all get `role="dialog" aria-modal="true"`; `canvas.html`: `@media ≤1024px` + `@media ≤768px` breakpoints (sidebar/config-panel collapse), ConfirmModal + FlowsModal + TestPayloadModal + HistoryModal + EdgeLabelModal + ValidationModal + NioModal + PermissionsModal all get `role="dialog" aria-modal="true" aria-label="..."`, overlay wrappers `aria-hidden="true"`; icon-only buttons across both pages get descriptive `aria-label`; sidebar nav gets `role="navigation" aria-label="Main navigation"` + keyboard navigation |
| 18 | Credential OAuth flows — `app/routers/oauth.py`: `GET /api/oauth/providers` (which providers have client_id set), `GET /api/oauth/{provider}/start?cred_name=` (stores state in Redis, redirects to provider), `GET /api/oauth/{provider}/callback` (exchanges code, saves credential, redirects to /admin); supports GitHub (repo/read:user), Google (Sheets + Drive, offline access + refresh token), Notion (Basic-auth token exchange); credential secret stored as provider-typed JSON; `OAuthConnectModal` component in admin.html with credential-name input + redirect flow; "Connect via OAuth" card shown when ≥1 provider env var is set; `?oauth_success`/`?oauth_error` URL params handled on App mount with toast + auto-navigate to credentials page; `.env.example` + CLAUDE.md updated |
| S-A | System diagnostics page — `GET /api/system/status` returns 9 subsystem checks (DB, Redis, Celery, scheduler leader, email, secrets, API key security, HTTPS, disk); each check returns `{status, message, fix?}`; new **System** nav page in admin.html with colour-coded `Check` components, inline fix guidance, auto-refresh every 30 s |
| S-B | Startup validation — `_validate_config()` in `main.py` called on startup; fatal checks (DB + Redis connectivity) call `sys.exit(1)` on failure with clear error; warning checks (SECRET_KEY, API_KEY, APP_URL, AGENTMAIL) log actionable messages; errors surfaced loudly rather than swallowed |
| S-C | Ops hardening — `OPERATIONS.md` runbook (health check, start/stop/restart, DB ops, worker/scheduler ops, common errors table, env vars reference); inline NOTE comments in `db.py`, `worker.py`, `email.py`; `PrometheusMiddleware` rewritten as pure ASGI (no `BaseHTTPMiddleware`) so exceptions propagate correctly to uvicorn; email.py two bug fixes: `inbox_id` = full address, endpoint `/messages/send` |
| S-D | DB performance + retry resilience — migration `0011_performance_indexes.py`: `runs.retry_count INT DEFAULT 0`, indexes on `(workspace_id, created_at DESC)`, `status`, `(graph_id, created_at DESC)`, audit_log indexes, schedules index; `enqueue_graph` Celery task gets `max_retries=3` + exponential backoff (30/60/120 s) for `_TRANSIENT_EXCEPTIONS`; run status lifecycle adds `retrying` + `dead`; admin.html Runs page shows DEAD/RETRY N badges, status filter adds Dead/Retrying options |
| Polish | Runs page bulk-delete — checkbox per row, select-all header checkbox, "Delete selected (N)" button using `POST /api/runs/bulk-delete`; checked rows highlighted purple; ruff --fix: removed unused imports across 8 files; `secrets.py` unused `ClientError` import removed |
| P3-22 | Mobile canvas layout — topbar collapses to icon-only (`≤768px`); `.topbar-secondary` wrapper uses `display:contents` desktop / `display:none !important` mobile; sidebar becomes off-canvas overlay with hamburger toggle (📦 button in topbar); config panel hidden on mobile; `mobileSidebarOpen` state drives overlay |
| P3-23 | Mobile admin layout — Runs page `.runs-split` grid stacks to single column; `.mobile-cards` CSS transforms Credentials / Schedules / Audit Log tables into card lists (`display:block` on all table elements + `td::before { content:attr(data-label) }` for column labels); `data-label` attrs added to all `<td>` cells |
| P3-24 | Keyboard shortcuts — canvas: `Ctrl+S` save, `Ctrl+Z` undo, `Ctrl+Y`/`Ctrl+Shift+Z` redo, `Escape` deselect/close, `?` toggle cheatsheet; `kbRef = useRef({})` pattern avoids stale closures; `ShortcutsModal` 4-section / 15-entry cheatsheet; admin: `?`/`Escape` + `AdminShortcutsModal` with `useFocusTrap`; "⌨️ Keyboard shortcuts" button in sidebar footer |
| P3-25 | Canvas minimap — `<MiniMap>` pannable + zoomable with styled colours; toggle via topbar 🗺 button (desktop) and MoreMenu (mobile); floating Panel button removed (was crashing on UMD undefined) |
| P3-fix | Canvas bug fixes — removed `Panel` from ReactFlow UMD destructure (undefined → React crash); removed overlapping floating Panel button; replaced `useFocusTrap` call in `ShortcutsModal` (not defined in canvas) with manual Escape `useEffect`; fixed overlay click guard to `target===currentTarget`; expanded shortcuts from 5 to 15 entries across 4 sections |
| P4-26 | Canvas search — `Ctrl+F` floating `NodeSearchBar` (text filter + group chips + jump-to via `setCenter`); 🔍 topbar button + MoreMenu item; `SEARCH_GROUPS` constant |
| P4-27 | Replay with payload override — `GET /api/runs/{id}/payload`; `POST .../replay` body `{payload?}`; `ReplayEditModal` in canvas inspector + admin Logs/Dashboard; audit logs `payload_overridden` flag |
| P4-28 | Templates gallery — `app/routers/templates.py` (`GET /api/templates`, `GET /api/templates/{slug}`); 9 bundled JSON templates in `app/templates/`; canvas OpenModal "Start from template" tab with category filter + 2-col cards |
| P4-29 | Webhook improvements — HMAC-SHA256 (`X-Hub-Signature-256`), allowed-origins CORS, `OPTIONS` preflight; `WebhookUrlPanel` in NodeEditorModal; secret + allowed_origins fields in NODE_DEFS |
| P4-30 | Sticky note enhancements — `NOTE_COLORS` palette (6 colours); StickyNote applies colour + minWidth/minHeight from config; NODE_DEFS adds colour select + width/height fields; `zIndex:-1` in all 4 node-load paths; hint panel with live colour swatches |
| P5-31 | New node `action.redis` — GET/SET/DEL/LPUSH/RPOP/INCR/EXPIRE; credential: `url`; NODE_DEFS + hint panels |
| P5-32 | New node `action.graphql` — query/mutation with variables; template rendering in query/variables; credential: endpoint + Authorization header |
| P5-33 | New node `action.pdf` — HTML → PDF via xhtml2pdf (optional dep); output: `pdf_bytes` (base64) + `size_bytes` + `filename`; pairs with S3/email nodes |
| P5-34 | New node `trigger.rss` — RSS/Atom feed polling; stdlib `xml.etree`; lookback window + filter expression; output: `entries[]` + `count` + first-entry shortcuts |
| P5-35 | New node `action.airtable` — Airtable REST API; credential: `api_key` + `base_id`; operations: list-records, get-record, create-record, update-record, delete-record |
| P6-36 | Webhook rate-limit UI — `GET/PUT /api/settings/ratelimit` (app_settings KV); Rate Limits card on Settings page with live counters per endpoint + global config form |
| P6-37 | CHANGELOG `[0.2.0]` + version bump — full changelog entry; `app/_version.py` (circular import fix); `pyproject.toml` bumped to 0.2.0; `v0.2.0` git tag; version chip + update-available banner in admin UI; `GET /api/version` endpoint with 24h GitHub cache |
| P6-38 | OpenTelemetry tracing — `app/telemetry.py` zero-overhead module (noop when OTLP endpoint unset); `graph.run` root span in worker; `run_graph` + per-node child spans in executor; `setup_tracing()` called in API/worker/scheduler; optional SDK + OTLP exporter |
| P6-39 | DB connection pool — `psycopg2.ThreadedConnectionPool` (min 2/max 10, `DB_POOL_MIN`/`DB_POOL_MAX`); broken-connection discard; `get_pool_stats()` exposed in `GET /api/system/status` System page |
| P6-40 | HA/DR runbook — `OPERATIONS.md` extended with Postgres streaming replication, Redis Sentinel failover, multi-region Celery workers, DR checklist + RTO/RPO table; `docker-compose.ha.yml` (Postgres primary+replica, Redis+2 replicas+3 Sentinels, 2× API/worker/scheduler) |
| F0 | Vite scaffolding — `frontend/` at repo root; `package.json`, `vite.config.js` multi-page build → `app/static/dist/`; HTML stubs for all 6 pages; `src/api/client.js` single `api()` helper; `_MIGRATED_PAGES` gate in `main.py`; multi-stage `Dockerfile`; `Makefile` dev/build/install/test targets; `.gitignore` updated |
| F1 | Shared component library — `frontend/src/components/`: `Toast.jsx`, `useFocusTrap.js`, `ConfirmModal.jsx`, `ViewerBanner.jsx`, `ReplayEditModal.jsx`, `TraceRow.jsx`, `StatusDot.jsx`, `RoleBadge.jsx` (+ `ROLE_META`); `frontend/src/contexts/WorkspaceContext.jsx` + `AuthContext.jsx`; 35 modules build cleanly, 118 tests pass |
| F2 | Auth pages — `frontend/src/pages/auth/`: `login.jsx` (sign-in + forgot-password toggle), `signup.jsx` (self-serve), `reset.jsx` (token-based), `invite.jsx` (accept + new-user signup); shared `AuthCard.jsx` layout; `_MIGRATED_PAGES` in `main.py` updated; legacy static HTML files removed from git; 36 modules, 14 chunks |
| F3 | Admin shell + routing — `src/admin/App.jsx` (AuthProvider + WorkspaceProvider + BrowserRouter + 13 routes); `AdminLayout.jsx` (sidebar, workspace switcher, NavLink nav, update banner, keyboard shortcuts modal, user info + sign-out); `src/admin/index.jsx` entry point; 13 stub page components in `src/pages/admin/`; 60 modules, 118 tests passing |
| F4 | Admin: Dashboard + Metrics + Flows + Logs — full React ports of four pages from admin.html; `GraphRow.jsx` sub-component with inline HistoryModal + AlertSettingsModal; 65 modules, 118 tests passing |
| F5 | Admin: Credentials + Schedules — `Credentials.jsx` with `CRED_SCHEMAS`, inline edit, OAuth connect card, test-connection pill; `Schedules.jsx` with `FlowPicker`, `CronBuilder`, `DateTimePicker`, `TimezoneSelect`, `CronNextRun`; 71 modules, 118 tests passing |
| F6 | Admin: remaining pages + delete admin.html — `Settings.jsx`, `AuditLog.jsx`, `System.jsx`, `Users.jsx`, `Workspaces.jsx`, `Templates.jsx`; `admin.html` added to `_MIGRATED_PAGES` + removed from git; dist updated; 73 modules, 118 tests passing |
| F7 | Canvas: node defs + primitives — `nodeDefs.js` (all NODE_DEFS + GROUPS, 40 node types incl. P1 nodes trigger.email/file_watch/action.postgres/s3); `StickyNote.jsx` (NOTE_COLORS palette); `CustomNode.jsx` (CustomNode + nodeTypes export, Handle logic for condition/loop/trigger); `NodeContextMenu.jsx`; `Palette.jsx` (grouped draggable sidebar); `ConfigPanel.jsx` (JsonSchemaTree, NioBody, NodeIOPanel, all node hint panels, retry policy, drag-to-insert); `reactflow@11` added to package.json; 73 modules, 118 tests passing |
| F8 | Canvas: modals — `NodeEditorModal.jsx` (3-col n8n-style editor, TestPanel, VarField autocomplete), `HistoryModal.jsx`, `OpenModal.jsx`, `TestPayloadModal.jsx`, `ValidationModal.jsx`, `EdgeLabelModal.jsx`, `PermissionsModal.jsx`, `ShortcutsModal.jsx`, `NodeSearchBar.jsx`; shared `canvasHelpers.js` (`isTemplate`, `validateFlow`, `computeAutoLayout`, `buildVarList`, `flattenVarPaths`); `VarField.jsx` (`{{` template autocomplete); `NioBody`/`JsonSchemaTree`/`NodeIOPanel` exported from `ConfigPanel.jsx`; SPA direct-nav routes added to `main.py`; `vite.config.js` `emptyOutDir:false` for bind-mount compat; 83 modules built cleanly |
| EB | Error boundaries — `ErrorBoundary.jsx` class component (dark-themed fallback, "Try again" + "Reload", collapsible stack trace, `fullPage` prop); admin `App.jsx` wrapped top-level + per-route via `page()` helper; canvas `index.jsx` wrapped with `fullPage`; fixed `React is not defined` in `AdminShortcutsModal` by adding `Fragment` to named imports in `AdminLayout.jsx` |
| C1 | Canvas multi-select + clipboard — `selectionOnDrag` + `panOnDrag={[1,2]}` + `SelectionMode.Partial`; `clipboardRef` with `doCopy`/`doPaste`/`doDuplicate`/`doSelectAll`; paste staggers by +60px per call; `NodeContextMenu` shows count-aware labels; `ShortcutsModal` gains multi-select section; Ctrl+A/C/V/D + Cmd equivalents |
| N7 | New node `action.wait_for_approval` — Celery task polls Redis every 10s for decision; DB `approvals` table (migration 0012); styled HTML email with Approve/Reject buttons; public `GET /api/approvals/{token}/approve\|reject` endpoints return dark-themed result page; admin `GET /api/approvals` list; NODE_DEFS + hint panel |
| A2 | Flow analytics — `get_flow_analytics(days)` + `get_daily_analytics(days)` in `db.py` using `PERCENTILE_CONT` for P95/P99; `GET /api/analytics/flows` + `GET /api/analytics/daily` in `admin.py`; `Metrics.jsx` rewritten with Overview/Per-flow tabs, 7/14/30/90d range selector, sortable `FlowTable`, `DailyChart` with stacked volume bars + SVG duration overlay |
| N1+N3 | New nodes `action.mysql` + `action.jira` — MySQL: pymysql DictCursor, host/port/user/pass/db or DSN, `last_insert_id` in output; Jira REST API v3: Basic auth (email + api_token), 8 operations (get/create/update/delete issue, add-comment, JQL search, get-transitions, transition-issue), ADF description wrapping; NODE_DEFS + ConfigPanel hint panels for both |
| C2 | Canvas alignment toolbar — floating bar visible when ≥2 nodes selected; 8 operations: align-left/centre/right/top/middle/bottom + distribute-horizontally/vertically; SVG icons; each op pushes undo snapshot |
| C4 | Subflow extraction — select ≥1 nodes, right-click → "Extract N nodes to subflow…"; `ExtractSubflowModal` name+description form; creates new graph via `POST /api/graphs`, replaces selection with `action.call_graph` node pointing to new flow |
| D1 | Flow tags — migration `0013_flow_tags.py` adds `tags TEXT[]` + GIN index to `graph_workflows`; `create_graph`/`update_graph` accept tags; `GraphRow` inline chip editor (add with Enter/comma, remove by clicking, normalised to lowercase-kebab); `Flows.jsx` tag filter bar above the list |
| R1 | Run annotations — migration `0014_run_notes.py` adds `note TEXT` to runs; `set_run_note()` db helper; `PUT /api/runs/{id}/note` endpoint; `Logs.jsx` 📝 button per row opens inline note editor (Enter saves, Esc cancels, Clear button); existing note shown as italic muted text under the run; note also shown in the detail panel |
| H1 | Flow health badges — `Flows.jsx` fetches `/api/analytics/flows?days=30` on load; `HealthBadge` component on each `GraphRow` shows colour-coded error-rate pill (green ≤5%, amber ≤25%, red >25%); tooltip shows total run count + last run timestamp; badge absent when no history |
| F9 | Canvas main app complete — `CanvasApp.jsx` is the full ReactFlow root with SSE run streaming, autosave (30 s debounce), dirty-state badge, undo/redo history, multi-select + clipboard, alignment toolbar, subflow extraction, `Ctrl+F` node search, minimap toggle, keyboard shortcuts modal; `canvas.html` deleted from `app/static/`; F-series migration complete |
| N5 | New nodes `action.hubspot` + `action.mongodb` + `action.twilio` — HubSpot CRM v3: get/create/update contact+object, search, get/create deal, associate; MongoDB: find/find_one/insert_one/update_one/delete_one/aggregate/count (pymongo, ObjectId auto-serialised); Twilio: send_sms/send_whatsapp/make_call/check_status/list_messages (stdlib urllib, no extra deps); nodeDefs + ConfigPanel hint panels for all three |
| Q1a | Worker queue dashboard card — `GET /api/runs/queue` (Celery inspect, 2 s timeout, fail-open); Dashboard shows Active/Reserved/Scheduled/Workers counters + per-worker rows; auto-refresh every 10 s |
| Q1b | Run priority — migration `0015_flow_priority.py` adds `priority INT DEFAULT 5`; `apply_async(priority=…)` in graphs.py + scheduler; priority dropdown 0–9 in GraphRow ⋯ menu |
| UX2a | Global search (`Ctrl+K`) — `CommandPalette.jsx` overlay; searches pages, flows, runs, credentials; 180 ms debounce; match highlight; ↑↓/↵/Esc navigation; Search button in sidebar footer |
| UX2b | Pinned flows — migration `0016_flow_pinned.py`; pin/unpin in GraphRow ⋯ menu; purple "Pinned" section at top of Flows page; pinned rows excluded from other sections |
| V1 | Release v0.3.0 — CHANGELOG covering F-series, N5, C/D/R/H sprints; `app/_version.py` + `pyproject.toml` bumped; `v0.3.0` git tag |
| S1 | Route audit + smoke tests — duplicate template endpoints removed from admin.py; startup duplicate-route guard; `tests/test_api_smoke.py` with 19 endpoint checks |
| S2 | Global exception handler — FastAPI `exception_handler(Exception)` logs full traceback + returns structured JSON; silent 500s are gone |
| S3 | Dist files removed from git — Vite build stays in Docker image; `static_dist` named volume in docker-compose prevents bind-mount clobber |
| S4 | VM push script — `scripts/vm-push.sh` wraps full plumbing workflow; lock file cleanup; single command |
| WS-fix | Workspace_id stamped on all run creation paths — scripts, replays, webhooks, and legacy workflow runner all now stamp workspace_id; `list_runs` uses NULL-inclusive filter for historical data; metrics + analytics endpoints workspace-scoped |
| V2 | Release v0.3.1 — CHANGELOG covering S1-S4 stability, workspace_id fixes, template 500, CSS mismatch, analytics scoping; `app/_version.py` + `pyproject.toml` bumped; `v0.3.1` tag |
| UX3 | Scripts page run status — inline status badge per script (⟳/✓/✗) polling `/api/runs/by-task/{id}` every 2 s; last-run summary loaded on page open; Run button disables + spins while running |
| T1 | Test coverage — `test_workspace_scoping.py` (5 tests: list_runs SQL filter, analytics params, script run INSERT); `test_nodes.py` (12 tests: transform, condition, http_request, webhook passthrough, llm_call output shape) |
| UX4 | Run from this node — executor accepts `start_node_id` + `prior_context`; BFS ancestor skip; right-click "▶ Run from here" in canvas context menu; canvas passes current `_runOutput` as prior_context |

---

## Active backlog — Q4

Pick the next item off the top. Cross it off and add a "Completed sprints" row when done.

### 🔴 V2 — v0.3.1 patch release

65. ~~**CHANGELOG `[0.3.1]` + version bump** — patch release covering all S-series stability fixes + workspace_id bug fixes; bump `app/_version.py` + `pyproject.toml` to 0.3.1; create `v0.3.1` tag.

---

### 🟠 UX3 — Scripts page improvements

66. ~~**Scripts page run status** — when you click ▶ Run on the Scripts page, show inline run status (queued → running → succeeded/failed) without navigating to Logs. Poll `GET /api/runs/by-task/{task_id}` every 2 s; show a status pill next to the script row. ~~ ✓ Done

67. ~~**Scripts page last-run summary** — for each script show the last run's status and timestamp fetched from `GET /api/runs?q={name}&page_size=1`. ~~ ✓ Done

---

### 🟡 T1 — Test coverage expansion

68. ~~**Workspace scoping tests** — `tests/test_workspace_scoping.py`: verify that script runs, replay runs, and webhook runs are stamped with workspace_id; verify list_runs filters correctly; verify analytics return workspace-scoped data. ~~ ✓ Done

69. ~~**Node unit tests** — `tests/test_nodes.py`: test the 5 most-used nodes (`action.http_request`, `action.transform`, `action.condition`, `action.llm_call`, `trigger.webhook`) with mocked I/O; assert output shapes and error handling. ~~ ✓ Done

---

### 🟢 UX4 — Canvas polish

70. ~~**"Run from this node"** — right-click context menu item on any non-trigger node; fires `POST /api/graphs/{id}/run` with `start_node_id` in the payload; executor skips nodes before the start node and uses previous run's output as context. ~~ ✓ Done

71. ~~**Canvas node output preview** — in ConfigPanel, when a run overlay is active and the selected node ran, show a collapsible "Last output" section with the full JSON tree (already have `_runOutput` on node data; just surface it cleanly). ~~ ✓ Done (NodeIOPanel already implemented)

---

## Active backlog — Q4 continued

### 🔴 Bugs & polish

72. **Logs page: show script run names clearly** — script runs use `workflow` column not `graph_id`; the Logs page flow_name shows raw script filename. Strip `.py`, capitalise words for display.

73. **Canvas: trigger node "first run" hint** — when a flow has a trigger.cron or trigger.webhook node but has never run, show a subtle hint in the config panel explaining how to fire it for the first time.

74. **Dashboard: show workspace name** — the Dashboard doesn't tell you which workspace you're viewing. Add a subtle "Workspace: {name}" chip near the page title.

---

### 🟠 N6 — More integrations

75. ~~**New node: `action.linear`**~~ ✓ Done — Linear.app issue tracker; credential: `api_key`; operations: create-issue, update-issue, get-issue, search-issues (GraphQL); output: `issue{}`, `issues[]`.

76. ~~**New node: `action.openai_assistant`**~~ ✓ Done — OpenAI Assistants API; credential: `api_key`; operations: create-thread, add-message, run-thread, get-run-status, list-messages; output: `messages[]`, `run_id`, `status`.

---

### 🟠 S5 — Production hardening (from Mistral report, 2026-05-18)

79. **Caddyfile security headers** — X-Frame-Options, Content-Security-Policy, Strict-Transport-Security, Referrer-Policy, Permissions-Policy added to Caddyfile. ~~✓ Done — IMP-BD committed.~~

80. ~~**API rate limiting** — FastAPI routes have no `slowapi` / rate-limit middleware. All endpoints are unprotected against burst/DoS. Add `slowapi` with configurable per-IP and per-token limits on critical routes.~~ ✓ Done — IMP-BF: slowapi==0.1.9, Redis-backed Limiter, @limiter.limit("10/minute") on login, "60/minute" on webhooks

81. **Scoped API tokens** — All tokens are unscoped (owner-level only). A compromised token grants full access. Implement token scopes: `runs:read`, `runs:write`, `graphs:read`, `graphs:write`, `credentials:read`, `credentials:write`, etc.

82. **CORS restrictions** — Caddyfile has no `Access-Control-Allow-Origin` restrictions. The React SPA and API are same-origin so CORS isn't triggered, but explicit allowlist improves defence-in-depth.

83. **Dependency pinning** — Most packages float (`fastapi>=0.0.0` style). A compromised upstream release could introduce vulnerabilities. Pin all runtime dependencies in `requirements.txt` with `>=` or `==` bounds.

84. **pydantic.BaseSettings for env validation** — Env vars are read with `os.environ.get()` and fail silently if malformed. Use `pydantic.BaseSettings` (or `pydantic-settings`) to validate required vars and types at startup.

85. ~~**S3 botocore timeouts** — `action_s3.py` passes no explicit timeouts to boto3 client calls. Long-hanging S3 ops could block workers indefinitely. Add per-operation timeouts (e.g. 30s default).~~ ✓ Done — IMP-BF: connect_timeout + read_timeout in BotoConfig, timeout config field on run()

---

### 🟡 UX5 — Canvas polish

77. **Canvas: dependency path highlight on hover** — when hovering a node, dim all unrelated edges and nodes; highlight upstream path in blue and downstream path in green.

78. **Canvas: execution timeline** — after a run, show a mini timeline bar below each executed node (relative duration). Pairs with the execution overlay.

---

## Active backlog (priority order)

1. ~~**W2 — Scope graphs + runs to workspace**~~ ✓ Done
2. ~~**W3 — Scope credentials, schedules, tokens**~~ ✓ Done
3. ~~**W4 — Workspace management UI**~~ ✓ Done
4. ~~**W5 — Self-serve onboarding (SaaS)**~~ ✓ Done

---

### 🔴 P0 — Hardening ✅ Done

5. ~~**Security: remove unsafe API key default**~~ ✓ — `API_KEY` with no value disables the legacy endpoint; `"dev_api_key"` logs a loud warning but is no longer the silent default.

6. ~~**Security: env-driven `secure=True` on session cookies**~~ ✓ — `is_secure_context()` helper in `app/auth.py`; all `set_cookie` calls (login, setup, signup, invite) now pass `secure=True` when `APP_URL` starts with `https://`.

7. ~~**Reliability: log startup failures loudly**~~ ✓ — `except Exception: pass` replaced with `except Exception as exc: log.warning(...)`.

8. ~~**Metrics accuracy: server-side run aggregates**~~ ✓ — `GET /api/runs/stats` endpoint in `runs.py` returns DB-accurate counts, top-5 failing flows, and 10 recent runs; Dashboard and Metrics page both switched from `?page_size=200` to this endpoint.

---

### 🟠 P1 — Editor & debugging experience

9. ~~**Canvas: dirty-state indicator + autosave**~~ ✓ — amber "● Unsaved" badge in topbar; autosave 30 s after last change (setTimeout resets on each new change, only fires for existing graphs); "✓ Saved" confirmation with last-save timestamp tooltip; Save button turns amber when dirty; `uid()` upgraded to RFC 4122 v4 UUID via `crypto.randomUUID()` with Math.random fallback.

10. ~~**Canvas: node test button**~~ ✓ — already fully implemented: `POST /api/graphs/{id}/nodes/{node_id}/test` endpoint in `graphs.py`; NodeEditorModal has a collapsible Test panel with JSON input textarea, Run button, output display, and 📌 pin-output button; fixed workspace isolation bug (credentials now scoped to workspace_id).

11. ~~**Canvas: version diff / restore UX**~~ ✓ — `HistoryModal` now pre-loads the latest version on open; non-current versions show a tabbed "⚡ Diff vs current" / "📋 All nodes" view; diff shows colour-coded `+` restored (green), `−` removed (red), `~` reverted (amber), `=` unchanged (muted) node rows with a summary chip bar; tab defaults to Diff when selecting a version.

12. ~~**Flow import / export**~~ ✓ Done

---

### 🟠 P1 — New nodes & connectors

13. ~~**New node: `trigger.email` (IMAP)**~~ ✓ Done

14. ~~**New node: `action.postgres`**~~ ✓ Done

15. ~~**New node: `action.s3`**~~ ✓ Done

16. ~~**New node: `trigger.file_watch`**~~ ✓ Done

---

### 🟠 P1 — Credential UX

17. ~~**Credential "test connection" button**~~ ✓ Done

18. ~~**Credential OAuth flows**~~ ✓ Done

---

### 🟡 P2 — Platform maturity

19. ~~**Versioned releases + upgrade notes**~~ ✓ Done — `[0.1.0]` entry added to `CHANGELOG.md` covering all W-series, P0, and P1 sprints with upgrade notes; `v0.1.0` tag created locally (push pending network restore).

20. ~~**Test coverage expansion**~~ ✓ Done — added `tests/test_permissions.py` (26 tests: `_check_admin`, `_require_scope`, role guards, `_check_flow_access`, `_resolve_workspace`); `tests/test_executor_failures.py` (26 tests: abort/continue failure modes, retry logic, condition branching, scheduler lock helpers, edge cases); `tests/test_webhook_ratelimit.py` (12 tests: webhook rate-limit counter logic, Redis fail-open, login brute-force lockout/clear); `tests/integration/test_e2e_playwright.py` (Playwright E2E: login flow, dashboard, canvas create/save/run, logout — auto-skipped unless `HIVERUNR_BASE_URL` set). Total unit suite: 117 tests, all passing.

21. ~~**Responsive / accessibility pass**~~ ✓ Done

---

### 🟢 P3 — Polish / UX ✅ Done

22. ~~**Mobile layout — canvas**~~ ✓ Done

23. ~~**Mobile layout — admin pages**~~ ✓ Done

24. ~~**Keyboard shortcuts**~~ ✓ Done

25. ~~**Canvas minimap**~~ ✓ Done

---

### 🔵 P4 — Power user features ✅ Done

26. ~~**Canvas search / filter**~~ ✓ — `Ctrl+F` floating search bar with text filter + group chips; jump-to-node centres viewport; 🔍 topbar button + MoreMenu item.

27. ~~**Run replay with payload override**~~ ✓ — `GET /api/runs/{id}/payload` + `POST .../replay` accepts optional body `{payload}`; "✏ Replay…" button opens pre-filled editor modal in both canvas inspector and admin Logs page.

28. ~~**Flow templates gallery**~~ ✓ — `GET/GET /api/templates/{slug}` router + 9 bundled JSON templates; canvas OpenModal gains "Start from template" tab with category chips + 2-col cards; selecting a template calls existing import endpoint.

29. ~~**Webhook trigger improvements**~~ ✓ — HMAC-SHA256 signature verification (`X-Hub-Signature-256`), configurable allowed-origins CORS, `WebhookUrlPanel` in NodeEditorModal with copy button; `OPTIONS` preflight handler added.

30. ~~**Canvas node grouping / labels**~~ ✓ — `NOTE_COLORS` palette (amber/blue/green/purple/red/slate); `StickyNote` applies colour + `minWidth`/`minHeight` from config; `NODE_DEFS["note"]` gains colour select + width/height fields; `zIndex:-1` on note nodes in all load paths so they render behind other nodes; hint panel shows inline colour swatches.

---

### 🟣 P5 — Integrations & ecosystem ✅ Done

31. ~~**New node: `action.redis`**~~ ✓ Done — GET/SET/DEL/LPUSH/RPOP/INCR/EXPIRE; credential: `url`.

32. ~~**New node: `action.graphql`**~~ ✓ Done — query/mutation with variables; credential stores endpoint + Authorization header.

33. ~~**New node: `action.pdf`**~~ ✓ Done — HTML → PDF via xhtml2pdf; output: `pdf_bytes` (base64) + `size_bytes`.

34. ~~**New node: `trigger.rss`**~~ ✓ Done — RSS/Atom feed polling; stdlib `xml.etree`; output: `entries[]` + `count`.

35. ~~**New node: `action.airtable`**~~ ✓ Done — Airtable REST API; list/get/create/update/delete-record.

---

### 🟤 P6 — Platform & ops ✅ Done

36. ~~**Webhook rate-limit UI**~~ ✓ Done — `GET/PUT /api/settings/ratelimit`; Rate Limits card on Settings page with live counters.

37. ~~**CHANGELOG + `v0.2.0` release tag**~~ ✓ Done — full `[0.2.0]` CHANGELOG entry; `app/_version.py`; `pyproject.toml` bumped; `v0.2.0` tag created.

38. ~~**Observability: OpenTelemetry traces**~~ ✓ Done — `app/telemetry.py` zero-overhead module; `graph.run` + `run_graph` + per-node spans; SDK+exporter optional; `OTEL_EXPORTER_OTLP_ENDPOINT` gates activation.

39. ~~**DB connection pool tuning**~~ ✓ Done — `ThreadedConnectionPool` in `db.py` (min 2/max 10, env-configurable); `get_pool_stats()` exposed in `GET /api/system/status` System page pool chip.

40. ~~**Multi-region / DR runbook**~~ ✓ Done — `OPERATIONS.md` extended with HA + DR sections; `docker-compose.ha.yml` (Postgres streaming replication, Redis Sentinel × 3, 2× API/worker/scheduler replicas).

---

### 🏗 F-series — Frontend restructure (Vite + React migration)

**Goal:** Replace the two 4,000+ line monolithic inline-JSX files (`admin.html`,
`canvas.html`) with a properly structured Vite + React multi-page application.
Each sprint keeps the app 100% functional — old files are only deleted once
their replacement is confirmed working.

**Rule:** After each sprint, update `CLAUDE.md`: tick the item, add a row to
Completed sprints, and update the "Frontend architecture" section if the
directory structure changed.

41. ~~**F0 — Vite scaffolding**~~ ✓ Done

42. ~~**F1 — Shared component library**~~ ✓ Done

43. ~~**F2 — Auth pages**~~ ✓ Done

44. ~~**F3 — Admin shell + routing**~~ ✓ Done

45. ~~**F4 — Admin: Dashboard + Flows + Runs**~~ ✓ Done

46. ~~**F5 — Admin: Credentials + Schedules**~~ ✓ Done

47. ~~**F6 — Admin: remaining pages + delete admin.html**~~ ✓ Done

48. ~~**F7 — Canvas: node defs + primitives**~~ ✓ Done

49. ~~**F8 — Canvas: modals**~~ ✓ Done — `NodeEditorModal.jsx` (3-col n8n-style, TestPanel, VarField autocomplete), `HistoryModal.jsx`, `OpenModal.jsx`, `TestPayloadModal.jsx`, `ValidationModal.jsx`, `EdgeLabelModal.jsx`, `PermissionsModal.jsx`, `ShortcutsModal.jsx`, `NodeSearchBar.jsx`; shared helpers in `canvasHelpers.js` + `VarField.jsx`; `NioBody`/`JsonSchemaTree`/`NodeIOPanel` exported from `ConfigPanel.jsx`; direct-navigation SPA routes added to `main.py` (`/settings`, `/graphs`, etc.).

50. ~~**F9 — Canvas: main app + delete canvas.html**~~ ✓ Done — `CanvasApp.jsx` (ReactFlow root, SSE streaming, keyboard shortcuts, minimap, autosave, dirty-state, multi-select/clipboard, alignment toolbar, subflow extraction, search bar); `canvas.html` deleted from `app/static/`.

---

## Active backlog — next sprints (Q3)

### 🔴 V1 — v0.3.0 release + upgrade docs

51. **CHANGELOG `[0.3.0]` + version bump** — full changelog entry covering all C/D/R/H/N sprints since 0.2.0; bump `app/_version.py` + `pyproject.toml`; create `v0.3.0` git tag.

---

### 🟠 N5 — More integrations

52. ~~**New node: `action.hubspot`**~~ ✓ Done — HubSpot CRM REST API v3; credential: `access_token`; operations: get/create/update contact+object, search, get/create deal, associate; output: `object`, `results[]`.

53. ~~**New node: `action.mongodb`**~~ ✓ Done — pymongo; credential JSON with `uri`; operations: find, find_one, insert_one, update_one, delete_one, aggregate, count; ObjectId/datetime auto-converted to strings.

54. ~~**New node: `action.twilio`**~~ ✓ Done — Twilio REST API; credential: `account_sid` + `auth_token`; operations: send_sms, send_whatsapp, make_call, check_status, list_messages.

---

### 🟡 Q1 — Queue & concurrency

55. ~~**Flow run queue depth**~~ ✓ Done — `GET /api/runs/queue` returns Celery `inspect().reserved()` + `scheduled()` counts per worker; Dashboard card shows queue depth + active workers; auto-refresh every 10 s.

56. ~~**Run priority**~~ ✓ Done — `priority` field on `graph_workflows` (0–9, default 5); Celery `apply_async(priority=…)` passes it through; priority selector in GraphRow ⋯ menu and canvas topbar.

---

### 🟢 UX2 — Polish round 2

57. ~~**Global search**~~ ✓ Done — `Ctrl+K` command palette in admin SPA; searches flows, runs (by ID/name), credentials, settings pages; keyboard-navigable results list; jump directly to result.

58. ~~**Pinned flows**~~ ✓ Done — `pinned BOOLEAN DEFAULT false` on `graph_workflows`; pin toggle in GraphRow ⋯ menu; Flows page shows a "Pinned" section at the top (no card separator needed if ≤5 pins).

---

## Active backlog — Stability & Maintainability

**Goal: make the codebase super stable and easy to maintain before adding features.**

The template 500 and CSS mismatch bugs revealed four systemic fragility points:
1. Duplicate route shadowing in admin.py (silently serves wrong handler)
2. No API smoke tests (broken endpoints undetected until users hit them)
3. Dist files tracked in git (CSS hash mismatches on restore/corruption)
4. Silent 500s with no logs (exceptions in psycopg2 bypass our loggers)

### 🔴 S1 — Route audit + smoke tests ✅ Done (this sprint)

59. ~~**Audit & remove duplicate routes**~~ ✓ Done — removed duplicate `GET /api/templates` and `POST /api/templates/{template_id}/use` from `admin.py` (they shadowed `templates_router`; admin.py is included first); added comment marking ownership; cleaned up debug endpoints.

60. ~~**Startup duplicate-route guard**~~ ✓ Done — loop over `app.routes` after all `include_router` calls; logs `ERROR: DUPLICATE ROUTE DETECTED` at startup so problems are caught immediately.

61. ~~**API smoke tests**~~ ✓ Done — `tests/test_api_smoke.py`: `test_no_duplicate_routes` (introspects live route table), `test_health` (checks version != "8"), parametrized `test_route_exists_returns_non_500` for 16 key endpoints. DB/Redis patched so tests run in CI without infrastructure.

---

### 🟠 S2 — Error visibility

62. ~~**Global exception handler with tracebacks** — add a FastAPI `exception_handler(Exception)` that logs the full traceback before returning 500. Currently some errors (psycopg2, import errors in handler body) produce 500 with zero log output.~~ ✓ Done

---

### 🟡 S3 — Remove dist files from git

63. ~~**Move Vite build into Dockerfile** — add `RUN npm ci && npm run build` to the Dockerfile so dist is baked into the image at build time. Remove `app/static/dist/` from git tracking (`.gitignore`). This eliminates the CSS-hash-mismatch class of bug entirely and removes ~50 hashed asset files from the repo.~~ ✓ Done

---

### 🟢 S4 — Git workflow

64. ~~**VM push script** — create `scripts/vm-push.sh` that wraps the git plumbing workflow (read-tree → add → write-tree → commit-tree → push) with automatic lock file cleanup. Single command instead of 10-step ritual.~~ ✓ Done

---

## Conventions

- **Migration naming**: `000N_short_description.py`, `down_revision` must chain correctly.
- **New scalar config**: add to `app_settings` via `get_setting`/`set_setting` — no new migration columns.
- **Alert dispatch**: always goes through `_send_run_alert()` in `worker.py` — do not add ad-hoc email calls elsewhere.
- **API auth**: all `/api/` routes require session cookie or `Authorization: Bearer <token>` header (or legacy `x-api-token`).
- **Frontend toasts**: call `showToast(message, "error"|"success")` passed as prop — do not use `alert()`.
- **Confirm dialogs**: set `confirmState({message, confirmLabel, fn})` — the `ConfirmModal` handles the rest.
- **New node**: create `app/nodes/action_xyz.py` with `NODE_TYPE`, `LABEL`, `run()`; add entry to `frontend/src/pages/canvas/nodeDefs.js` and hint panel to `frontend/src/pages/canvas/ConfigPanel.jsx`.
