import tempfile
import unittest
from pathlib import Path

from job_search.models import (
    ApplicationRecord,
    JobRankingRecord,
    JobRecord,
    PipelineRunRecord,
    SourceFetchEventRecord,
)
from job_search.ops_digest import build_weekly_digest
from job_search.storage.repository import JobSearchRepository


class OpsDigestTests(unittest.TestCase):
    def test_build_weekly_digest_includes_key_sections(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "ops.sqlite"
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
                        next_action_at="2100-01-01T09:00:00+00:00",
                        next_action_type="follow_up_email",
                    )
                ]
            )

            run1 = PipelineRunRecord.from_run_record(
                {
                    "run_id": "run-digest-1",
                    "started_at": "2026-01-01T09:00:00+00:00",
                    "ended_at": "2026-01-01T09:00:01+00:00",
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
                    "summary": {},
                }
            )
            run2 = PipelineRunRecord.from_run_record(
                {
                    "run_id": "run-digest-2",
                    "started_at": "2026-01-02T09:00:00+00:00",
                    "ended_at": "2026-01-02T09:00:01+00:00",
                    "status": "success",
                    "duration_ms": 1000,
                    "total_jobs": 2,
                    "a_tier": 2,
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
                    "summary": {},
                }
            )
            job1 = JobRecord.from_job(
                {
                    "id": "job:1",
                    "source": "Fixture RSS",
                    "source_type": "remote",
                    "title": "Senior Backend Engineer",
                    "company": "ACME",
                    "location": "Europe",
                    "remote_hint": True,
                    "url": "https://jobs.example.com/1",
                    "description": "Python platform",
                    "published": "",
                    "fetched_at": "2026-01-01T00:00:00+00:00",
                }
            )
            job2 = JobRecord.from_job(
                {
                    "id": "job:2",
                    "source": "Fixture RSS",
                    "source_type": "remote",
                    "title": "Senior Platform Engineer",
                    "company": "WatchCo",
                    "location": "Europe",
                    "remote_hint": True,
                    "url": "https://jobs.example.com/2",
                    "description": "Salary: €90000 per year",
                    "salary": {"annual_min_eur": 90000},
                    "published": "",
                    "fetched_at": "2026-01-02T00:00:00+00:00",
                }
            )

            repo.persist_pipeline_snapshot(
                run=run1,
                jobs=[job1],
                rankings=[JobRankingRecord.from_ranked_job("run-digest-1", {"id": "job:1", "score": 80, "tier": "A"})],
                source_events=[
                    SourceFetchEventRecord.from_dict(
                        {
                            "run_id": "run-digest-1",
                            "source_name": "Fixture RSS",
                            "source_kind": "rss",
                            "source_type": "remote",
                            "source_url": "https://example.com/rss",
                            "attempts": 1,
                            "success": True,
                            "jobs_fetched": 1,
                            "duration_ms": 100,
                            "error_message": None,
                        }
                    )
                ],
            )
            repo.persist_pipeline_snapshot(
                run=run2,
                jobs=[job1, job2],
                rankings=[
                    JobRankingRecord.from_ranked_job("run-digest-2", {"id": "job:1", "score": 78, "tier": "A"}),
                    JobRankingRecord.from_ranked_job(
                        "run-digest-2",
                        {
                            "id": "job:2",
                            "score": 88,
                            "tier": "A",
                            "reasons": ["company watchlist"],
                        },
                    ),
                ],
                source_events=[
                    SourceFetchEventRecord.from_dict(
                        {
                            "run_id": "run-digest-2",
                            "source_name": "Fixture RSS",
                            "source_kind": "rss",
                            "source_type": "remote",
                            "source_url": "https://example.com/rss",
                            "attempts": 1,
                            "success": True,
                            "jobs_fetched": 2,
                            "duration_ms": 110,
                            "error_message": None,
                        }
                    )
                ],
            )

            digest = build_weekly_digest(repo=repo)
            self.assertIn("Weekly Ops Digest", digest)
            self.assertIn("Priority New Jobs", digest)
            self.assertIn("Funnel Snapshot", digest)
            self.assertIn("Source Health", digest)


if __name__ == "__main__":
    unittest.main()
