import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from job_search.api_server import serve_api
from job_search.models import (
    ApplicationRecord,
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

    run = PipelineRunRecord.from_run_record(
        {
            "run_id": "run-api-1",
            "started_at": "2026-01-02T09:00:00+00:00",
            "ended_at": "2026-01-02T09:00:01+00:00",
            "status": "success",
            "duration_ms": 1000,
            "total_jobs": 2,
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
    job = JobRecord.from_job(
        {
            "id": "job:api:1",
            "source": "Fixture",
            "source_type": "remote",
            "title": "Senior Platform Engineer",
            "company": "ACME",
            "location": "Europe",
            "remote_hint": True,
            "url": "https://jobs.example.com/1",
            "description": "Python platform backend",
            "published": "",
            "fetched_at": "2026-01-02T00:00:00+00:00",
            "adaptive_bonus": 5,
            "adaptive_reasons": ["company affinity:acme"],
        }
    )
    job2 = JobRecord.from_job(
        {
            "id": "job:api:2",
            "source": "Fixture",
            "source_type": "innsbruck",
            "title": "Software Engineer",
            "company": "Beta",
            "location": "Innsbruck",
            "remote_hint": False,
            "url": "https://jobs.example.com/2",
            "description": "Go backend",
            "published": "",
            "fetched_at": "2026-01-02T00:00:00+00:00",
        }
    )

    repo.persist_pipeline_snapshot(
        run=run,
        jobs=[job, job2],
        rankings=[
            JobRankingRecord.from_ranked_job("run-api-1", {"id": "job:api:1", "score": 88, "tier": "A"}),
            JobRankingRecord.from_ranked_job("run-api-1", {"id": "job:api:2", "score": 56, "tier": "B"}),
        ],
        source_events=[
            SourceFetchEventRecord.from_dict(
                {
                    "run_id": "run-api-1",
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


class ApiServerTests(unittest.TestCase):
    def test_api_endpoints(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "api.sqlite"
            repo = JobSearchRepository(
                db_url=f"sqlite:///{db_path}",
                migrations_dir=Path(__file__).resolve().parents[1] / "db/migrations",
                auto_migrate=True,
            )
            repo.initialize()
            _seed_repo(repo)

            with patch(
                "job_search.cover_letter.call_openai_json",
                return_value={
                    "body": "Dear ACME team,\\n\\nI am applying for this role.\\n\\nBest regards,\\nCandidate",
                    "language": "en",
                    "style": "concise",
                },
            ) as mocked_cover_llm:
                server = serve_api(repo=repo, host="127.0.0.1", port=0)
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()

                host, port = server.server_address
                base = f"http://{host}:{port}"

                try:
                    with urlopen(base + "/health", timeout=3) as resp:
                        health = json.loads(resp.read().decode("utf-8"))
                    with urlopen(base + "/api/runs?limit=5", timeout=3) as resp:
                        runs = json.loads(resp.read().decode("utf-8"))
                    with urlopen(base + "/jobs?tier=A&limit=5", timeout=3) as resp:
                        jobs = json.loads(resp.read().decode("utf-8"))
                    with urlopen(base + "/api/runs/run-api-1/sources", timeout=3) as resp:
                        sources = json.loads(resp.read().decode("utf-8"))

                    app_req = Request(
                        base + "/applications",
                        method="POST",
                        data=json.dumps(
                            {
                                "job_url": "https://jobs.example.com/1",
                                "status": "interview",
                                "notes": "phone screen booked",
                            }
                        ).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                    )
                    with urlopen(app_req, timeout=3) as resp:
                        app_update = json.loads(resp.read().decode("utf-8"))

                    feedback_req = Request(
                        base + "/feedback",
                        method="POST",
                        data=json.dumps(
                            {
                                "job_url": "https://jobs.example.com/1",
                                "action": "interview",
                                "value": "round1",
                                "created_at": "2026-01-03T00:00:00+00:00",
                            }
                        ).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                    )
                    with urlopen(feedback_req, timeout=3) as resp:
                        feedback_post = json.loads(resp.read().decode("utf-8"))

                    with urlopen(base + "/applications?status=interview", timeout=3) as resp:
                        apps = json.loads(resp.read().decode("utf-8"))
                    with urlopen(base + "/feedback?action=interview", timeout=3) as resp:
                        feedback = json.loads(resp.read().decode("utf-8"))
                    with urlopen(base + "/dashboard", timeout=3) as resp:
                        dashboard_html = resp.read().decode("utf-8")
                    with urlopen(base + "/board", timeout=3) as resp:
                        board_html = resp.read().decode("utf-8")
                    with urlopen(
                        base + "/jobs?run_id=run-api-1&q=backend&remote=true&include_diagnostics=true&limit=10",
                        timeout=3,
                    ) as resp:
                        jobs_filtered = json.loads(resp.read().decode("utf-8"))
                    with urlopen(base + "/jobs?run_id=run-api-1&application_status=saved&limit=10", timeout=3) as resp:
                        jobs_saved = json.loads(resp.read().decode("utf-8"))
                    with urlopen(base + "/applications/metrics?days=365", timeout=3) as resp:
                        metrics = json.loads(resp.read().decode("utf-8"))
                    followup_req = Request(
                        base + "/applications/followup",
                        method="POST",
                        data=json.dumps(
                            {
                                "job_url": "https://jobs.example.com/2",
                                "next_action_type": "follow_up_email",
                                "next_action_at": "2100-01-01T10:00:00Z",
                            }
                        ).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                    )
                    with urlopen(followup_req, timeout=3) as resp:
                        followup_update = json.loads(resp.read().decode("utf-8"))
                    with urlopen(base + "/applications/followups?due_before=2100-01-02T00:00:00Z", timeout=3) as resp:
                        followups = json.loads(resp.read().decode("utf-8"))
                    bulk_req = Request(
                        base + "/applications/bulk",
                        method="POST",
                        data=json.dumps(
                            {
                                "items": [
                                    {"job_url": "https://jobs.example.com/1", "status": "interview"},
                                    {"job_url": "https://jobs.example.com/2", "status": "saved"},
                                ]
                            }
                        ).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                    )
                    with urlopen(bulk_req, timeout=3) as resp:
                        bulk_update = json.loads(resp.read().decode("utf-8"))
                    cover_req = Request(
                        base + "/cover-letters/generate",
                        method="POST",
                        data=json.dumps(
                            {
                                "job_url": "https://jobs.example.com/1",
                                "cv_variant": "en_short",
                                "style": "concise",
                                "additional_context": "highlight temporal work at Company X",
                            }
                        ).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                    )
                    with urlopen(cover_req, timeout=3) as resp:
                        cover_letter = json.loads(resp.read().decode("utf-8"))
                    with urlopen(base + "/cover-letters?job_url=https://jobs.example.com/1", timeout=3) as resp:
                        cover_list = json.loads(resp.read().decode("utf-8"))
                    with urlopen(base + "/sources/health?window_runs=10&stale_after_hours=100000", timeout=3) as resp:
                        source_health = json.loads(resp.read().decode("utf-8"))
                finally:
                    server.shutdown()
                    thread.join(timeout=3)
                    server.server_close()

            called_kwargs = mocked_cover_llm.call_args.kwargs
            self.assertEqual(called_kwargs.get("model"), "gpt-5.2")
            prompt_payload = json.loads(called_kwargs.get("user_prompt") or "{}")
            self.assertEqual(prompt_payload.get("additional_context"), "highlight temporal work at Company X")

            self.assertTrue(health["ok"])
            self.assertEqual(len(runs["runs"]), 1)
            self.assertEqual(runs["runs"][0]["run_id"], "run-api-1")
            self.assertEqual(len(jobs["jobs"]), 1)
            self.assertEqual(jobs["jobs"][0]["job_id"], "job:api:1")
            self.assertEqual(len(sources["source_events"]), 1)
            self.assertEqual(sources["source_events"][0]["attempts"], 1)
            self.assertEqual(app_update["application"]["status"], "interview")
            self.assertTrue(feedback_post["ok"])
            self.assertEqual(len(apps["applications"]), 1)
            self.assertEqual(apps["applications"][0]["status"], "interview")
            self.assertEqual(len(feedback["feedback"]), 1)
            self.assertEqual(feedback["feedback"][0]["action"], "interview")
            self.assertEqual(jobs_filtered["total"], 1)
            self.assertEqual(jobs_filtered["jobs"][0]["job_id"], "job:api:1")
            self.assertEqual(jobs_filtered["jobs"][0]["diagnostics"]["adaptive_bonus"], 5)
            self.assertEqual(jobs_saved["total"], 1)
            self.assertEqual(jobs_saved["jobs"][0]["job_id"], "job:api:2")
            self.assertEqual(metrics["metrics"]["status_counts"]["saved"], 1)
            self.assertEqual(metrics["metrics"]["status_counts"]["interview"], 1)
            self.assertIn("Job Search Dashboard", dashboard_html)
            self.assertIn("Application Board", board_html)
            self.assertEqual(followup_update["application"]["next_action_type"], "follow_up_email")
            self.assertGreaterEqual(len(followups["followups"]), 1)
            self.assertEqual(bulk_update["updated"], 2)
            self.assertEqual(len(bulk_update["applications"]), 2)
            self.assertFalse(cover_letter["cached"])
            self.assertEqual(len(cover_list["cover_letters"]), 1)
            self.assertEqual(source_health["sources"][0]["source_name"], "Fixture RSS")

    def test_api_auth_enforcement(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "api-auth.sqlite"
            repo = JobSearchRepository(
                db_url=f"sqlite:///{db_path}",
                migrations_dir=Path(__file__).resolve().parents[1] / "db/migrations",
                auto_migrate=True,
            )
            repo.initialize()
            _seed_repo(repo)

            server = serve_api(
                repo=repo,
                host="127.0.0.1",
                port=0,
                auth_config={"enabled": True, "api_keys": {"token-user-a": "user-a"}},
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            host, port = server.server_address
            base = f"http://{host}:{port}"

            try:
                with self.assertRaises(HTTPError) as unauth_err:
                    urlopen(base + "/applications", timeout=3)
                self.assertEqual(unauth_err.exception.code, 401)
                unauth_err.exception.close()

                req = Request(base + "/applications", headers={"X-API-Key": "token-user-a"})
                with urlopen(req, timeout=3) as resp:
                    apps = json.loads(resp.read().decode("utf-8"))
            finally:
                server.shutdown()
                thread.join(timeout=3)
                server.server_close()

            self.assertIn("applications", apps)

    def test_invalid_auth_config_rejected_before_start(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "api-invalid-auth.sqlite"
            repo = JobSearchRepository(
                db_url=f"sqlite:///{db_path}",
                migrations_dir=Path(__file__).resolve().parents[1] / "db/migrations",
                auto_migrate=True,
            )
            repo.initialize()
            with self.assertRaises(ValueError):
                serve_api(
                    repo=repo,
                    host="127.0.0.1",
                    port=0,
                    auth_config={"enabled": True, "api_keys": {"bad key": ""}},
                )


if __name__ == "__main__":
    unittest.main()
