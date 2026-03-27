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

@app.get("/api/system/status")
def api_system_status(request: Request):
    """Rich system health + info for the Settings page."""
    _check_admin(request)
    import sys, platform, socket

    results = {}

    # ── DB ────────────────────────────────────────────────────────────────
    try:
        from app.core.db import get_conn
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM runs")
            run_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM graph_workflows")
            flow_count = cur.fetchone()[0]
            cur.execute("SELECT pg_size_pretty(pg_database_size(current_database())) AS sz")
            db_size = cur.fetchone()[0]
        results["db"] = {"status": "ok", "run_count": run_count, "flow_count": flow_count, "db_size": db_size}
    except Exception as e:
        results["db"] = {"status": "error", "error": str(e)}

    # ── Redis ─────────────────────────────────────────────────────────────
    try:
        from app.worker import app as celery_app
        redis_url = celery_app.conf.broker_url or ""
        import redis as _redis
        r = _redis.from_url(redis_url, socket_connect_timeout=2)
        r.ping()
        results["redis"] = {"status": "ok", "url": redis_url.split("@")[-1]}
    except Exception as e:
        results["redis"] = {"status": "error", "error": str(e)}

    # ── Celery worker ─────────────────────────────────────────────────────
    try:
        from app.worker import app as celery_app
        pong = celery_app.control.ping(timeout=2)
        worker_count = len(pong)
        results["worker"] = {"status": "ok" if worker_count else "warning", "workers": worker_count}
    except Exception as e:
        results["worker"] = {"status": "error", "error": str(e)}

    # ── System info ───────────────────────────────────────────────────────
    results["system"] = {
        "app_version": "8",
        "python":      sys.version.split()[0],
        "platform":    platform.system() + " " + platform.release(),
        "hostname":    socket.gethostname(),
        "pid":         os.getpid(),
    }

    return results

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

class TrimRunsBody(BaseModel):
    keep: int = 100

@app.post("/api/runs/trim")
def api_trim_runs(body: TrimRunsBody, request: Request):
    """Keep only the most recent `keep` runs; delete the rest."""
    _check_admin(request)
    from app.core.db import get_conn
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            DELETE FROM runs
            WHERE id NOT IN (
                SELECT id FROM runs ORDER BY id DESC LIMIT %s
            )
        """, (body.keep,))
        deleted = cur.rowcount
    return {"deleted": deleted, "kept": body.keep}

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


