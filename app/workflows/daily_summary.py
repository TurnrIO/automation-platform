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
