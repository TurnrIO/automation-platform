"""
Daily Summary — prints a human-readable report of the last 24 hours of run activity.

Pair this with a trigger.cron node (e.g. "0 8 * * *") and an action.send_email
node in the canvas, or simply run it from the Scripts page for an instant snapshot.
"""

import logging
import os
import sys
from datetime import datetime, timezone

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    stream=sys.stdout,
)


def summary(workspace_id: int | None = None) -> dict:
    """
    Generate a daily summary report.

    Args:
        workspace_id: if set, only report on runs for this workspace

    Returns:
        dict with keys: total, succeeded, failed, running, avg_ms, failures, flows
    """
    import psycopg2
    import psycopg2.extras

    DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://hiverunr:hiverunr@db:5432/hiverunr")

    workspace_filter = (
        "AND r.workspace_id = %s" if workspace_id is not None else ""
    )
    params = (workspace_id,) if workspace_id is not None else ()

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    except (psycopg2.Error, OSError) as exc:
        logger.error("Failed to connect to database: %s", exc)
        raise

    try:
        now_utc = datetime.now(timezone.utc)
        logger.info("Generating daily summary report")
        print(f"Daily Run Summary — {now_utc.strftime('%Y-%m-%d %H:%M UTC')}")
        print("=" * 52)

        # Overall counts for the last 24 h
        cur.execute(
            f"""
            SELECT
                COUNT(*)                                              AS total,
                SUM(CASE WHEN status = 'succeeded' THEN 1 ELSE 0 END) AS succeeded,
                SUM(CASE WHEN status = 'failed'    THEN 1 ELSE 0 END) AS failed,
                SUM(CASE WHEN status = 'running'   THEN 1 ELSE 0 END) AS running,
                ROUND(
                    AVG(EXTRACT(EPOCH FROM (updated_at - created_at)) * 1000)
                )::int AS avg_ms
            FROM runs r
            WHERE created_at >= NOW() - INTERVAL '24 hours'
            {workspace_filter}
            """,
            params,
        )
        logger.info("Fetched overall counts")
        s = cur.fetchone()

        total     = s["total"]     or 0
        succeeded = s["succeeded"] or 0
        failed    = s["failed"]    or 0
        running   = s["running"]   or 0
        avg_ms    = s["avg_ms"]    or 0

        if total == 0:
            print("  No runs in the last 24 hours.")
            logger.info("No runs in the last 24 hours")
        else:
            rate = round(succeeded / total * 100) if total else 0
            print(f"  Total runs    : {total}")
            print(f"  Succeeded     : {succeeded}  ({rate}%)")
            print(f"  Failed        : {failed}")
            print(f"  Still running : {running}")
            print(f"  Avg duration  : {avg_ms} ms")
            logger.info(
                "Summary: total=%d succeeded=%d failed=%d running=%d avg_ms=%d",
                total, succeeded, failed, running, avg_ms,
            )

        # Failures detail
        failures = []
        if failed > 0:
            print("\nFailed runs:")
            cur.execute(
                f"""
                SELECT r.id,
                       COALESCE(g.name, r.workflow, 'unknown') AS flow,
                       r.created_at::text AS started
                FROM runs r
                LEFT JOIN graph_workflows g ON r.graph_id = g.id
                                              AND g.workspace_id = r.workspace_id
                WHERE r.status = 'failed'
                  AND r.created_at >= NOW() - INTERVAL '24 hours'
                {workspace_filter}
                ORDER BY r.id DESC
                LIMIT 10
                """,
                params,
            )
            for row in cur.fetchall():
                print(f"  #{row['id']:>6}  {row['flow']:30s}  {row['started']}")
                failures.append({"id": row["id"], "flow": row["flow"], "started": row["started"]})
            logger.info("Listed %d failed runs", failed)

        # Most active flows
        print("\nMost active flows (last 24 h):")
        cur.execute(
            f"""
            SELECT COALESCE(g.name, r.workflow, 'unknown') AS flow,
                   COUNT(*)       AS runs,
                   SUM(CASE WHEN r.status = 'succeeded' THEN 1 ELSE 0 END) AS ok,
                   SUM(CASE WHEN r.status = 'failed'    THEN 1 ELSE 0 END) AS err
            FROM runs r
            LEFT JOIN graph_workflows g ON r.graph_id = g.id
                                          AND g.workspace_id = r.workspace_id
            WHERE r.created_at >= NOW() - INTERVAL '24 hours'
            {workspace_filter}
            GROUP BY 1
            ORDER BY runs DESC
            LIMIT 5
            """,
            params,
        )
        rows = cur.fetchall()
        flows = []
        if rows:
            for row in rows:
                bar = "✓" * int(row["ok"]) + "✗" * int(row["err"])
                print(f"  {row['flow']:30s}  {row['runs']} run(s)  [{bar}]")
                flows.append({"flow": row["flow"], "runs": row["runs"], "ok": row["ok"], "err": row["err"]})
        else:
            print("  (none)")
        logger.info("Report complete")

        print("\n" + "=" * 52)
        print("Report complete.")

        return {
            "total": total,
            "succeeded": succeeded,
            "failed": failed,
            "running": running,
            "avg_ms": avg_ms,
            "failures": failures,
            "flows": flows,
        }

    finally:
        try:
            cur.close()
            conn.close()
            logger.info("Database connection closed")
        except (AttributeError, OSError):
            pass


if __name__ == "__main__":
    summary()

