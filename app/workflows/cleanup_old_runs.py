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
