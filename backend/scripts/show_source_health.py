#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from job_search.json_io import load_json
from job_search.paths import CONFIG, DB
from job_search.storage.repository import JobSearchRepository


def main():
    parser = argparse.ArgumentParser(description="Show source health statistics")
    parser.add_argument("--window-runs", type=int, default=12, help="Number of recent runs to evaluate")
    parser.add_argument("--stale-after-hours", type=int, default=72, help="Mark source stale after this many hours")
    parser.add_argument("--db-url", default="", help="Override DB URL")
    args = parser.parse_args()

    db_cfg = load_json(CONFIG / "database.json", default={})
    db_url = args.db_url.strip() or str(db_cfg.get("url") or "").strip()
    if not db_url:
        raise SystemExit("Database URL is missing. Configure config/database.json or pass --db-url.")

    repo = JobSearchRepository(
        db_url=db_url,
        migrations_dir=DB / "migrations",
        auto_migrate=bool(db_cfg.get("auto_migrate", False)),
    )
    repo.initialize()

    rows = repo.get_source_health(window_runs=args.window_runs, stale_after_hours=args.stale_after_hours)
    if not rows:
        print("No source health data found.")
        return
    for row in rows:
        stale = "stale" if row["stale"] else "fresh"
        print(
            f"{row['source_name']}: score={row['health_score']} success_rate={row['success_rate']} "
            f"events={row['total_events']} avg_jobs={row['avg_jobs_on_success']:.2f} {stale}"
        )


if __name__ == "__main__":
    main()
