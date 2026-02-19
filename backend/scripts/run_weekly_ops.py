#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from job_search.json_io import load_json
from job_search.ops_digest import write_weekly_digest
from job_search.paths import CONFIG, DB
from job_search.pipeline import run_pipeline
from job_search.storage.repository import JobSearchRepository


def main():
    parser = argparse.ArgumentParser(description="Run pipeline + generate weekly operations digest")
    parser.add_argument("--skip-pipeline", action="store_true", help="Only generate digest from existing DB data")
    parser.add_argument("--db-url", default="", help="Override DB URL")
    args = parser.parse_args()

    if not args.skip_pipeline:
        run_pipeline()

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
    digest_path = write_weekly_digest(repo=repo)
    print(f"Weekly digest written: {digest_path}")


if __name__ == "__main__":
    main()
