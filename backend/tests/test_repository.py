import sqlite3
import tempfile
import unittest
from pathlib import Path

from job_search.models import ApplicationRecord, JobRankingRecord, JobRecord, PipelineRunRecord
from job_search.storage.repository import JobSearchRepository


class RepositoryTests(unittest.TestCase):
    def test_upserts_and_run_idempotency(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "repo.sqlite"
            repo = JobSearchRepository(
                db_url=f"sqlite:///{db_path}",
                migrations_dir=Path(__file__).resolve().parents[1] / "db/migrations",
                auto_migrate=True,
            )
            repo.initialize()

            repo.upsert_applications(
                [
                    ApplicationRecord(
                        user_id="default",
                        job_url="https://jobs.example.com/1",
                        title="Role 1",
                        company="ACME",
                        status="applied",
                        applied_at="2026-01-01T00:00:00+00:00",
                        notes="",
                    ),
                    ApplicationRecord(
                        user_id="default",
                        job_url="https://jobs.example.com/1",
                        title="Role 1 updated",
                        company="ACME",
                        status="applied",
                        applied_at="2026-01-01T00:00:00+00:00",
                        notes="note",
                    ),
                ]
            )

            run = PipelineRunRecord.from_run_record(
                {
                    "run_id": "run-1",
                    "started_at": "2026-01-01T00:00:00+00:00",
                    "ended_at": "2026-01-01T00:00:01+00:00",
                    "status": "success",
                    "duration_ms": 1000,
                    "total_jobs": 1,
                    "a_tier": 1,
                    "b_tier": 0,
                    "c_tier": 0,
                    "skipped_applied": 0,
                    "llm_enabled": False,
                    "llm_model": None,
                    "llm_scored_live": 0,
                    "llm_cache_hits": 0,
                    "llm_failed": 0,
                    "source_errors": 0,
                    "error_message": None,
                    "summary": {"total": 1},
                }
            )
            job = JobRecord.from_job(
                {
                    "id": "fixture:1",
                    "source": "Fixture",
                    "source_type": "remote",
                    "title": "Senior Platform Engineer",
                    "company": "ACME",
                    "location": "Europe",
                    "remote_hint": True,
                    "url": "https://jobs.example.com/1",
                    "description": "Python and Kubernetes",
                    "published": "",
                    "fetched_at": "2026-01-01T00:00:00+00:00",
                }
            )
            ranking = JobRankingRecord.from_ranked_job(
                "run-1",
                {
                    "id": "fixture:1",
                    "score": 70,
                    "tier": "A",
                    "reasons": ["target role"],
                    "skill_hits": ["python"],
                },
            )

            repo.persist_pipeline_snapshot(run=run, jobs=[job], rankings=[ranking])
            ranking2 = JobRankingRecord.from_ranked_job(
                "run-1",
                {
                    "id": "fixture:1",
                    "score": 77,
                    "tier": "A",
                    "reasons": ["target role", "skills"],
                    "skill_hits": ["python", "kubernetes"],
                },
            )
            repo.persist_pipeline_snapshot(run=run, jobs=[job], rankings=[ranking2])

            conn = sqlite3.connect(db_path)
            try:
                app_count = conn.execute("SELECT COUNT(*) FROM applications").fetchone()[0]
                jobs_count = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
                runs_count = conn.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0]
                rankings_count = conn.execute("SELECT COUNT(*) FROM job_rankings").fetchone()[0]
                ranking_score = conn.execute(
                    "SELECT score FROM job_rankings WHERE run_id = ? AND job_id = ?",
                    ("run-1", "fixture:1"),
                ).fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(app_count, 1)
            self.assertEqual(jobs_count, 1)
            self.assertEqual(runs_count, 1)
            self.assertEqual(rankings_count, 1)
            self.assertEqual(ranking_score, 77)

    def test_search_ranked_jobs_sorts_newest_with_mixed_published_formats(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "repo.sqlite"
            repo = JobSearchRepository(
                db_url=f"sqlite:///{db_path}",
                migrations_dir=Path(__file__).resolve().parents[1] / "db/migrations",
                auto_migrate=True,
            )
            repo.initialize()

            run = PipelineRunRecord.from_run_record(
                {
                    "run_id": "run-sort",
                    "started_at": "2026-02-18T00:00:00+00:00",
                    "ended_at": "2026-02-18T00:00:01+00:00",
                    "status": "success",
                    "duration_ms": 1000,
                    "total_jobs": 4,
                    "a_tier": 4,
                    "b_tier": 0,
                    "c_tier": 0,
                    "skipped_applied": 0,
                    "llm_enabled": False,
                    "llm_model": None,
                    "llm_scored_live": 0,
                    "llm_cache_hits": 0,
                    "llm_failed": 0,
                    "source_errors": 0,
                    "error_message": None,
                    "summary": {"total": 4},
                }
            )

            jobs = [
                JobRecord.from_job(
                    {
                        "id": "fixture:older-rfc",
                        "source": "Fixture",
                        "source_type": "remote",
                        "title": "Older RFC",
                        "company": "ACME",
                        "location": "EU",
                        "remote_hint": True,
                        "url": "https://jobs.example.com/older-rfc",
                        "description": "older",
                        "published": "Wed, 23 Jul 2025 14:31:54 +0000",
                        "fetched_at": "2026-02-18T01:55:32.125989+00:00",
                    }
                ),
                JobRecord.from_job(
                    {
                        "id": "fixture:newer-rfc",
                        "source": "Fixture",
                        "source_type": "remote",
                        "title": "Newer RFC",
                        "company": "ACME",
                        "location": "EU",
                        "remote_hint": True,
                        "url": "https://jobs.example.com/newer-rfc",
                        "description": "newer",
                        "published": "Thu, 29 Jan 2026 21:06:16 +0000",
                        "fetched_at": "2026-02-18T01:55:32.117275+00:00",
                    }
                ),
                JobRecord.from_job(
                    {
                        "id": "fixture:iso",
                        "source": "Fixture",
                        "source_type": "remote",
                        "title": "ISO",
                        "company": "ACME",
                        "location": "EU",
                        "remote_hint": True,
                        "url": "https://jobs.example.com/iso",
                        "description": "iso",
                        "published": "2026-02-17T06:01:45+00:00",
                        "fetched_at": "2026-02-18T01:55:31.751091+00:00",
                    }
                ),
                JobRecord.from_job(
                    {
                        "id": "fixture:fallback-fetched",
                        "source": "Fixture",
                        "source_type": "remote",
                        "title": "Fallback fetched",
                        "company": "ACME",
                        "location": "EU",
                        "remote_hint": True,
                        "url": "https://jobs.example.com/fallback-fetched",
                        "description": "fallback",
                        "published": "",
                        "fetched_at": "2026-02-18T01:55:39.046977+00:00",
                    }
                ),
            ]
            rankings = [
                JobRankingRecord.from_ranked_job("run-sort", {"id": "fixture:older-rfc", "score": 50, "tier": "A"}),
                JobRankingRecord.from_ranked_job("run-sort", {"id": "fixture:newer-rfc", "score": 50, "tier": "A"}),
                JobRankingRecord.from_ranked_job("run-sort", {"id": "fixture:iso", "score": 50, "tier": "A"}),
                JobRankingRecord.from_ranked_job("run-sort", {"id": "fixture:fallback-fetched", "score": 50, "tier": "A"}),
            ]
            repo.persist_pipeline_snapshot(run=run, jobs=jobs, rankings=rankings)

            newest = repo.search_ranked_jobs(limit=10, run_id="run-sort", sort="newest")["jobs"]
            self.assertEqual(
                [j["job_id"] for j in newest],
                ["fixture:fallback-fetched", "fixture:iso", "fixture:newer-rfc", "fixture:older-rfc"],
            )

            oldest = repo.search_ranked_jobs(limit=10, run_id="run-sort", sort="oldest")["jobs"]
            self.assertEqual(
                [j["job_id"] for j in oldest],
                ["fixture:older-rfc", "fixture:newer-rfc", "fixture:iso", "fixture:fallback-fetched"],
            )


if __name__ == "__main__":
    unittest.main()
