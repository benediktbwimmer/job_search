import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from job_search.models import (
    ApplicationRecord,
    CoverLetterRecord,
    FeedbackEventRecord,
    JobRankingRecord,
    JobRecord,
    PipelineRunRecord,
    SourceFetchEventRecord,
)
from job_search.storage.repository import JobSearchRepository


def _seed_repo(repo: JobSearchRepository):
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
                job_url="https://jobs.example.com/2",
                title="Role 2",
                company="Beta",
                status="saved",
                applied_at="2026-01-02T00:00:00+00:00",
                notes="",
            ),
        ]
    )

    run1 = PipelineRunRecord.from_run_record(
        {
            "run_id": "run-1",
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
            "run_id": "run-2",
            "started_at": "2026-01-02T09:00:00+00:00",
            "ended_at": "2026-01-02T09:00:01+00:00",
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
            "summary": {},
        }
    )

    job1 = JobRecord.from_job(
        {
            "id": "job:1",
            "source": "Fixture",
            "source_type": "remote",
            "title": "Senior Backend Engineer",
            "company": "ACME",
            "location": "Europe",
            "remote_hint": True,
            "url": "https://jobs.example.com/1",
            "description": "Python distributed systems backend",
            "published": "",
            "fetched_at": "2026-01-01T00:00:00+00:00",
            "adaptive_bonus": 4,
            "adaptive_reasons": ["source affinity:remote"],
        }
    )
    job2 = JobRecord.from_job(
        {
            "id": "job:2",
            "source": "Fixture",
            "source_type": "innsbruck",
            "title": "Software Engineer",
            "company": "Beta",
            "location": "Innsbruck",
            "remote_hint": False,
            "url": "https://jobs.example.com/2",
            "description": "Go",
            "published": "",
            "fetched_at": "2026-01-02T00:00:00+00:00",
        }
    )

    repo.persist_pipeline_snapshot(
        run=run1,
        jobs=[job1],
        rankings=[JobRankingRecord.from_ranked_job("run-1", {"id": "job:1", "score": 82, "tier": "A"})],
        source_events=[
            SourceFetchEventRecord.from_dict(
                {
                    "run_id": "run-1",
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

    repo.add_feedback_events(
        [
            FeedbackEventRecord.from_dict(
                {
                    "job_url": "https://jobs.example.com/1",
                    "action": "applied",
                    "value": "manual",
                    "source": "test",
                    "created_at": "2026-01-02T10:00:00+00:00",
                },
                user_id="default",
            )
        ]
    )

    repo.persist_pipeline_snapshot(
        run=run2,
        jobs=[job1, job2],
        rankings=[
            JobRankingRecord.from_ranked_job("run-2", {"id": "job:1", "score": 76, "tier": "A"}),
            JobRankingRecord.from_ranked_job("run-2", {"id": "job:2", "score": 55, "tier": "B"}),
        ],
        source_events=[
            SourceFetchEventRecord.from_dict(
                {
                    "run_id": "run-2",
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


class RepositoryQueriesTests(unittest.TestCase):
    def test_query_methods(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "queries.sqlite"
            repo = JobSearchRepository(
                db_url=f"sqlite:///{db_path}",
                migrations_dir=Path(__file__).resolve().parents[1] / "db/migrations",
                auto_migrate=True,
            )
            repo.initialize()
            _seed_repo(repo)

            latest_run = repo.get_latest_run_id()
            runs = repo.get_recent_runs(limit=10)
            source_events = repo.get_run_source_events("run-2")
            jobs_a = repo.get_ranked_jobs(limit=10, tier="A")
            search_backend = repo.search_ranked_jobs(limit=10, query_text="backend", run_id="run-2")
            search_remote_false = repo.search_ranked_jobs(limit=10, remote=False, run_id="run-2")
            search_page_2 = repo.search_ranked_jobs(limit=1, offset=1, run_id="run-2")
            search_with_diagnostics = repo.search_ranked_jobs(
                limit=10, run_id="run-2", include_diagnostics=True, tier="A"
            )
            search_by_application_status = repo.search_ranked_jobs(
                limit=10, run_id="run-2", application_status="saved"
            )
            apps_applied = repo.list_applications(limit=10, status="applied")
            repo.set_application_followup(
                job_url="https://jobs.example.com/2",
                next_action_at=(datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
                next_action_type="follow_up_email",
                user_id="default",
            )
            due_followups = repo.list_due_followups(user_id="default", limit=10)
            saved_cover = repo.save_cover_letter(
                CoverLetterRecord(
                    user_id="default",
                    job_url="https://jobs.example.com/1",
                    job_id="job:1",
                    run_id="run-2",
                    cv_variant="en_short",
                    language="en",
                    style="concise",
                    company="ACME",
                    title="Senior Backend Engineer",
                    body="Draft body",
                    generated_at="2026-01-03T10:00:00+00:00",
                )
            )
            cover_letters = repo.list_cover_letters(user_id="default", limit=10)
            source_health = repo.get_source_health(window_runs=10, stale_after_hours=365 * 24)
            app_one = repo.get_application("https://jobs.example.com/1", user_id="default")
            feedback_applied = repo.list_feedback_events(limit=10, action="applied", user_id="default")
            metrics = repo.get_application_metrics(user_id="default", days=365)

            self.assertEqual(latest_run, "run-2")
            self.assertEqual(len(runs), 2)
            self.assertEqual(runs[0]["run_id"], "run-2")
            self.assertEqual(len(source_events), 1)
            self.assertEqual(source_events[0]["source_name"], "Fixture RSS")
            self.assertEqual(len(jobs_a), 1)
            self.assertEqual(jobs_a[0]["job_id"], "job:1")
            self.assertEqual(search_backend["total"], 1)
            self.assertEqual(search_backend["jobs"][0]["job_id"], "job:1")
            self.assertEqual(search_remote_false["total"], 1)
            self.assertEqual(search_remote_false["jobs"][0]["job_id"], "job:2")
            self.assertEqual(search_page_2["total"], 2)
            self.assertEqual(search_page_2["jobs"][0]["job_id"], "job:2")
            self.assertEqual(search_with_diagnostics["jobs"][0]["diagnostics"]["adaptive_bonus"], 4)
            self.assertEqual(search_by_application_status["total"], 1)
            self.assertEqual(search_by_application_status["jobs"][0]["job_id"], "job:2")
            self.assertEqual(len(apps_applied), 1)
            self.assertEqual(len(due_followups), 1)
            self.assertEqual(due_followups[0]["job_url"], "https://jobs.example.com/2")
            self.assertEqual(saved_cover["version"], 1)
            self.assertEqual(len(cover_letters), 1)
            self.assertEqual(source_health[0]["source_name"], "Fixture RSS")
            self.assertEqual(apps_applied[0]["job_url"], "https://jobs.example.com/1")
            self.assertEqual(app_one["status"], "applied")
            self.assertEqual(len(feedback_applied), 1)
            self.assertEqual(feedback_applied[0]["source"], "test")
            self.assertEqual(metrics["total_applications"], 2)
            self.assertEqual(metrics["status_counts"]["applied"], 1)
            self.assertEqual(metrics["status_counts"]["saved"], 1)
            self.assertEqual(metrics["feedback_counts"]["applied"], 1)
            self.assertEqual(metrics["followups"]["due_today"], 1)


if __name__ == "__main__":
    unittest.main()
