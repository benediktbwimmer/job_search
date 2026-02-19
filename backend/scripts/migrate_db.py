#!/usr/bin/env python3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from job_search.storage.db import apply_migrations


def main():
    db_url = sys.argv[1] if len(sys.argv) > 1 else "sqlite:///data/job_search.sqlite"
    migrations_dir = ROOT / "db" / "migrations"
    apply_migrations(db_url=db_url, migrations_dir=migrations_dir)
    print(f"Migrations applied to {db_url}")


if __name__ == "__main__":
    main()
