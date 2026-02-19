#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from job_search.json_io import load_json
from job_search.paths import CONFIG
from job_search.storage.db import connect_sqlite


def main():
    parser = argparse.ArgumentParser(description="Show recent pipeline run history")
    parser.add_argument("--limit", type=int, default=10, help="Number of runs to display")
    parser.add_argument("--db-url", default="", help="Override DB URL")
    args = parser.parse_args()

    db_cfg = load_json(CONFIG / "database.json", default={})
    db_url = args.db_url.strip() or str(db_cfg.get("url") or "").strip()
    if not db_url:
        raise SystemExit("Database URL is missing. Configure config/database.json or pass --db-url.")

    conn = connect_sqlite(db_url)
    try:
        runs = conn.execute(
            """
            SELECT run_id, started_at, status, total_jobs, a_tier, b_tier, c_tier, source_errors
            FROM pipeline_runs
            ORDER BY started_at DESC
            LIMIT ?
            """,
            (max(1, args.limit),),
        ).fetchall()

        if not runs:
            print("No pipeline runs found.")
            return

        for row in runs:
            run_id = row["run_id"]
            print(
                f"run={run_id} started_at={row['started_at']} status={row['status']} "
                f"jobs={row['total_jobs']} A={row['a_tier']} B={row['b_tier']} C={row['c_tier']} errors={row['source_errors']}"
            )
            events = conn.execute(
                """
                SELECT source_name, source_kind, attempts, success, jobs_fetched, duration_ms
                FROM source_fetch_events
                WHERE run_id = ?
                ORDER BY source_name ASC
                """,
                (run_id,),
            ).fetchall()
            for e in events:
                ok = "ok" if int(e["success"]) == 1 else "fail"
                print(
                    f"  - {e['source_name']} ({e['source_kind']}): {ok}, "
                    f"attempts={e['attempts']}, jobs={e['jobs_fetched']}, duration_ms={e['duration_ms']}"
                )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
