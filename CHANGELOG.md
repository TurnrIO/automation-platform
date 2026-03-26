# Changelog — HiveRunr

All notable changes are documented here, newest first.

---

## [v11] — 2026-03-26 — Modular Nodes · HiveRunr Brand · Run Logs · URL Persistence

### Platform rename
- Project renamed from "Automations" to **HiveRunr**
- All UI titles, email notifications, and API docs updated to reflect the new name
- Flow URLs now use brand-consistent slugs: `#flow-a3f2e1c9` instead of `#graph-42`

### Modular node filesystem
- All 23 node types extracted from `executor.py` into individual files under `app/nodes/`
- Auto-discovering registry in `app/nodes/__init__.py` — add a file, it's available instantly
- Shared `_render()` template helper in `app/nodes/_utils.py`
- `app/nodes/custom/` — hot-loadable directory; drop a `.py` file and call `POST /api/admin/reload_nodes`, no restart needed
- `executor.py` reduced to ~200 lines of pure orchestration

### New nodes
- `action.merge` — merges outputs from multiple upstream nodes (modes: `dict`, `all`, `first`)
- `action.github` — GitHub API integration (issues, PRs, releases)
- `action.google_sheets` — read/write Google Sheets rows
- `action.notion` — create/update Notion pages

### Continue-on-error
- Per-node `fail_mode` setting: `abort` (default) or `continue`
- In `continue` mode, errors are stored in context and the flow keeps running
- Canvas shows a warning when `continue` mode is selected

### Run replay
- `POST /api/runs/{id}/replay` re-enqueues a run using its original payload
- ▶ Replay button in the dashboard

### Webhook rate limiting
- Redis token-bucket rate limiting per webhook token
- Configurable via `WEBHOOK_RATE_LIMIT` and `WEBHOOK_RATE_WINDOW` env vars

### Run Logs page (rebuilt)
- Previous file-based log viewer replaced with a DB-backed run trace viewer
- Per-node execution trace: status, duration, retry count, collapsible input/output/error inspector
- Filterable by status, searchable by flow name or run ID

### URL state persistence
- Admin dashboard: hash-based routing — `#dashboard`, `#logs`, `#metrics`, etc. — survives refresh and browser back/forward
- Canvas: open flow is reflected in the URL (`#flow-{slug}`), restored on refresh

### Flow slugs
- Each flow gets a unique 8-character hex slug on creation (e.g. `a3f2e1c9`)
- `GET /api/graphs/by-slug/{slug}` endpoint for slug-based lookup
- Automatic migration backfills slugs for existing flows on first startup

### Bug fixes
- SFTP `list` operation: fixed `module 'paramiko' has no attribute 'S_ISDIR'` — corrected to `stat.S_ISDIR` from Python stdlib
- Metrics/Dashboard: flows now show their actual name instead of `graph #12` — fixed via `LEFT JOIN graph_workflows` in `list_runs()`

---

## [v10] — 2026-03-25 — Integration Nodes · Live Data Inspector · Flow Templates

### New nodes
- `action.github` — GitHub API (issues, PRs, releases)
- `action.google_sheets` — Google Sheets read/write
- `action.notion` — Notion page creation/update

### Live data inspector
- "🔬 Inspect run…" dropdown in the canvas topbar
- Select any past run to overlay per-node input and output directly on the config panel
- Shows duration, attempt count, and truncation warning for large payloads

### Node input in traces
- Each trace record now stores the node's input alongside its output
- Inputs larger than 2000 characters are stored as a `__truncated` marker

### Flow templates
- `GET /api/templates` — lists available templates
- `POST /api/templates/{id}/use` — instantiates a template as a new flow
- Templates UI page in the admin dashboard with category grouping and tag badges
- Six built-in templates: Daily Health Check, GitHub Issue → Slack, Notion Daily Log, Sheets → Slack Report, LLM Summariser, Webhook → Notion

---

## [v9] — 2026-03-25 — True If/Else Branching · Run Output Viewer · Failure Notifications

### True If/Else branching
- The Condition node now genuinely branches — nodes on the un-taken handle are skipped
- BFS from each handle computes which nodes belong to each branch
- Nodes reachable from both handles (convergence points) always execute
- Skipped nodes appear in the trace with `status: "skipped"` for visibility

### Run output viewer
- Expanding a run in the dashboard now shows its output or error inline
- Script runs show full `stdout` in a monospace box
- Fixed "Invalid Date" in the Started column

### Failure notifications
- `_notify_failure()` helper in `worker.py`
- Sends Slack message and/or email when any run fails
- Configurable via `NOTIFY_SLACK_WEBHOOK` and `NOTIFY_EMAIL` env vars

---

## [v8] — 2026-03-24 — Sub-flows · Slack Node · Export/Import · Edge Labels · Pre-run Validation

### Sub-flow composition
- `action.call_graph` — invoke another flow and receive its output as a node result
- Circular dependency detection prevents infinite loops

### Slack node
- `action.slack` — post messages to Slack via incoming webhook
- Supports templated message body

### Flow export/import
- Export any flow as a JSON file from the canvas toolbar
- Import JSON to create a new flow — nodes and edges fully restored

### Edge labels
- Click any edge to add a label (shown on the canvas)
- Labels are saved with the flow

### Pre-run validation
- "▶ Validate" button checks for disconnected nodes, missing required fields, and invalid configs before running
- Validation issues shown in a panel with jump-to-node links

### Auto-layout
- One-click Dagre-based automatic graph layout

---

## [v7] — 2026-03-24 — SSH/SFTP · Loop Node · Credentials Store · Undo/Redo

### SSH and SFTP nodes
- `action.ssh` — execute commands on remote servers over SSH
- `action.sftp` — upload, download, list files over SFTP
- Both nodes support credential store references

### Loop node
- `action.loop` — iterate over an array, running downstream nodes once per item
- Loop context available in each iteration as `{{loop.item}}` and `{{loop.index}}`

### Credentials store
- Encrypted credential storage in the database
- Credentials panel in the admin UI — add/edit/delete credentials by name and type
- Nodes can reference credentials by name via `{{cred.name}}`

### Undo/redo
- Full undo/redo history in the canvas (Ctrl+Z / Ctrl+Shift+Z)
- History is reset when a new flow is loaded

### Node disable/enable
- Toggle individual nodes off without deleting them
- Disabled nodes are skipped during execution and shown greyed-out on the canvas

---

## [v6] — 2026-03-23 — LLM Node · Telegram Node · Script Manager · Version History

### LLM call node
- `action.llm_call` — OpenAI chat completion
- Configurable model, system prompt, and user prompt with `{{variable}}` templating

### Telegram node
- `action.telegram` — send messages to a Telegram chat via bot token

### Script manager
- Scripts page in the admin UI — create, edit, and delete Python scripts
- Scripts are executed by `action.run_script` nodes

### Version history
- Save named snapshots of any flow from the canvas toolbar
- Restore any previous version with one click
- Version list shows timestamp and optional note

---

## [v5] — 2026-03-23 — Metrics Dashboard · Retry Policy · Filter Node · Set Variable Node

### Metrics dashboard
- Run volume chart (7-day bar chart, success/failure split)
- Top failing flows
- Recent runs table
- Key stats: total runs, success rate, active runs, avg duration

### Retry policy
- Per-node retry configuration: max attempts and delay between retries
- Retry state visible in the run trace

### Filter node
- `action.filter` — evaluates a condition; stops flow execution if false

### Set variable node
- `action.set_variable` — stores a value in flow context for use by downstream nodes

---

## [v4] — 2026-03-22 — Canvas Flow Editor · React Flow · Graph Persistence

### Visual canvas editor
- Full React Flow v11 integration
- Drag-and-drop node placement, edge drawing, node deletion
- Flows saved to PostgreSQL as JSON

### Node types (initial)
- `trigger.manual`, `trigger.webhook`, `trigger.cron`
- `action.http_request`, `action.transform`, `action.condition`
- `action.log`, `action.delay`, `action.run_script`, `action.send_email`

### Graph API
- `GET/POST /api/graphs` — list and create flows
- `GET/PUT/DELETE /api/graphs/{id}` — read, update, delete a flow
- `POST /api/graphs/{id}/run` — trigger a flow run

---

## [v3] — 2026-03-21 — Admin UI · Schedules · Log Viewer

### Admin UI
- Single-page React admin dashboard
- Workflow enable/disable toggle
- Manual workflow trigger
- Recent runs table with cancel and delete

### Schedules
- Cron schedule manager with timezone support
- DB-driven scheduler (`app/scheduler.py`) polls for due schedules and enqueues tasks
- `next_run_at` calculated and stored after each enqueue

### Log viewer
- Per-run log files written to `runlogs/`
- Log list + viewer in the admin UI

---

## [v2] — 2026-03-20 — PostgreSQL · Run History · Celery Result Backend

### PostgreSQL integration
- Replaced in-memory state with PostgreSQL
- `runs` table stores task ID, status, result, timestamps
- `workflows` table stores enabled/disabled state

### Run history
- All runs persisted — survives worker restarts
- API endpoints to list, cancel, and delete runs

### Celery result backend
- Redis configured as both broker and result backend

---

## [v1] — 2026-03-19 — Initial Release

### Foundation
- FastAPI API server with `/run/{workflow}` trigger endpoint
- Celery + Redis task queue
- Example `health_check` and `daily_summary` workflows
- `docker-compose.yml` with Caddy reverse proxy
- Bootstrap script generates all files from scratch
- Flower worker monitor at `/flower/`
