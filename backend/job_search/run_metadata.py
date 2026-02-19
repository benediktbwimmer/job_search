import json
from pathlib import Path


def append_run_log(path: Path, run_record: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(run_record, ensure_ascii=False) + "\n")


def persist_run_metadata(run_record: dict, run_log_path: Path, db_config: dict, migrations_dir: Path):
    notices = []
    append_run_log(run_log_path, run_record)

    return notices
