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
    parser = argparse.ArgumentParser(description="Check recent pipeline runs for failures")
    parser.add_argument("--limit", type=int, default=5, help="Number of recent runs to inspect")
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

    runs = repo.get_recent_runs(limit=max(1, args.limit))
    failed = [r for r in runs if str(r.get("status") or "").lower() == "failed"]
    if failed:
        print(f"FAILED RUNS DETECTED: {len(failed)}")
        for run in failed:
            print(f"- {run.get('run_id')} at {run.get('started_at')}")
        raise SystemExit(2)
    print("No failed runs in recent window.")


if __name__ == "__main__":
    main()
