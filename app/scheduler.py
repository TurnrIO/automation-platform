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
