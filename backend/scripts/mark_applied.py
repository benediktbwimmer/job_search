#!/usr/bin/env python3
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from job_search.json_io import load_json
from job_search.models import ApplicationRecord
from job_search.storage.repository import JobSearchRepository

BASE = ROOT
APPLIED_PATH = BASE / "data" / "applied_jobs.json"
CONFIG_PATH = BASE / "config" / "database.json"


def load():
    if not APPLIED_PATH.exists():
        return {"applied": []}
    return json.loads(APPLIED_PATH.read_text())


def save(data):
    APPLIED_PATH.parent.mkdir(parents=True, exist_ok=True)
    APPLIED_PATH.write_text(json.dumps(data, indent=2, ensure_ascii=False))


def _maybe_persist_db(record: dict):
    db_cfg = load_json(CONFIG_PATH, default={})
    if not db_cfg.get("enabled", False):
        return

    db_url = str(db_cfg.get("url") or "").strip()
    if not db_url:
        raise RuntimeError("database is enabled but url is missing")

    repo = JobSearchRepository(
        db_url=db_url,
        migrations_dir=BASE / "db" / "migrations",
        auto_migrate=bool(db_cfg.get("auto_migrate", False)),
    )
    repo.initialize()
    repo.upsert_applications([ApplicationRecord.from_applied_dict(record, user_id="default")])


def main():
    if len(sys.argv) < 3:
        print("Usage: mark_applied.py <url> <title> [company]")
        sys.exit(1)

    url = sys.argv[1].strip()
    title = sys.argv[2].strip()
    company = sys.argv[3].strip() if len(sys.argv) > 3 else ""

    data = load()
    applied = data.get("applied", [])

    if any((x.get("url", "").strip().lower() == url.lower()) for x in applied):
        print("Already present.")
        return

    record = {
        "url": url,
        "title": title,
        "company": company,
        "applied_at": datetime.now(timezone.utc).isoformat(),
    }
    applied.append(record)
    data["applied"] = applied
    save(data)

    _maybe_persist_db(record)
    print(f"Added: {title}")


if __name__ == "__main__":
    main()
