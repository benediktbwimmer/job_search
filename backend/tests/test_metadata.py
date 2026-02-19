import sqlite3
import tempfile
import unittest
from pathlib import Path

from job_search.storage.db import apply_migrations, insert_pipeline_run


class MetadataTests(unittest.TestCase):
    def test_migrations_and_pipeline_run_insert(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "job_search.sqlite"
            db_url = f"sqlite:///{db_path}"
            apply_migrations(db_url=db_url, migrations_dir=Path(__file__).resolve().parents[1] / "db/migrations")

            insert_pipeline_run(
                db_url=db_url,
                run_record={
                    "run_id": "test-run-1",
                    "started_at": "2026-01-01T00:00:00+00:00",
                    "ended_at": "2026-01-01T00:00:01+00:00",
                    "status": "success",
                    "duration_ms": 1000,
                    "total_jobs": 2,
                    "a_tier": 1,
                    "b_tier": 1,
                    "c_tier": 0,
                    "skipped_applied": 0,
                    "llm_enabled": False,
                    "llm_model": None,
                    "llm_scored_live": 0,
                    "llm_cache_hits": 0,
                    "llm_failed": 0,
                    "source_errors": 0,
                    "error_message": None,
                    "summary": {"total": 2},
                },
            )

            conn = sqlite3.connect(db_path)
            try:
                count = conn.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0]
                self.assertEqual(count, 1)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
