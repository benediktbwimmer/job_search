import json
import sqlite3
from pathlib import Path


def resolve_sqlite_path(db_url: str) -> Path:
    if not db_url:
        raise ValueError("Database URL is required")
    if db_url.startswith("sqlite:///"):
        return Path(db_url.replace("sqlite:///", "", 1)).expanduser().resolve()
    if db_url.startswith("sqlite://"):
        return Path(db_url.replace("sqlite://", "", 1)).expanduser().resolve()
    raise ValueError(f"Unsupported DB URL: {db_url}")


def connect_sqlite(db_url: str) -> sqlite3.Connection:
    db_path = resolve_sqlite_path(db_url)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def apply_migrations(db_url: str, migrations_dir: Path):
    conn = connect_sqlite(db_url)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version TEXT PRIMARY KEY,
                applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        existing = {row[0] for row in conn.execute("SELECT version FROM schema_migrations")}

        for path in sorted(migrations_dir.glob("*.sql")):
            version = path.name
            if version in existing:
                continue
            sql = path.read_text()
            conn.executescript(sql)
            conn.execute("INSERT INTO schema_migrations(version) VALUES (?)", (version,))
        conn.commit()
    finally:
        conn.close()


def insert_pipeline_run(db_url: str, run_record: dict):
    conn = connect_sqlite(db_url)
    try:
        conn.execute(
            """
            INSERT INTO pipeline_runs (
                run_id, started_at, ended_at, status, duration_ms,
                total_jobs, a_tier, b_tier, c_tier, skipped_applied,
                llm_enabled, llm_model, llm_scored_live, llm_cache_hits, llm_failed,
                source_errors, error_message, summary_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_record.get("run_id"),
                run_record.get("started_at"),
                run_record.get("ended_at"),
                run_record.get("status"),
                int(run_record.get("duration_ms", 0)),
                int(run_record.get("total_jobs", 0)),
                int(run_record.get("a_tier", 0)),
                int(run_record.get("b_tier", 0)),
                int(run_record.get("c_tier", 0)),
                int(run_record.get("skipped_applied", 0)),
                1 if run_record.get("llm_enabled") else 0,
                run_record.get("llm_model"),
                int(run_record.get("llm_scored_live", 0)),
                int(run_record.get("llm_cache_hits", 0)),
                int(run_record.get("llm_failed", 0)),
                int(run_record.get("source_errors", 0)),
                run_record.get("error_message"),
                json.dumps(run_record.get("summary", {}), ensure_ascii=False),
            ),
        )
        conn.commit()
    finally:
        conn.close()
