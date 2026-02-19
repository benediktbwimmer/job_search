import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
from uuid import UUID

from job_search.json_io import save_json
from job_search.pipeline import run_pipeline


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        base = datetime(2026, 1, 5, 9, 0, 0)
        if tz is not None:
            return base.replace(tzinfo=tz)
        return base


RSS_FIXTURE = """
<rss>
  <channel>
    <item>
      <title>Senior Platform Engineer at Alpine Systems</title>
      <link>https://jobs.example.com/1</link>
      <description><![CDATA[Remote in Europe. Distributed systems. Kubernetes. Python. Cloud platform work.]]></description>
      <pubDate>Mon, 01 Jan 2026 10:00:00 +0000</pubDate>
      <guid>job-1</guid>
    </item>
    <item>
      <title>Software Engineer (Innsbruck)</title>
      <link>https://jobs.example.com/2</link>
      <description><![CDATA[Onsite in Innsbruck. Platform and cloud engineering with Python.]]></description>
      <pubDate>Mon, 01 Jan 2026 11:00:00 +0000</pubDate>
      <guid>job-2</guid>
    </item>
  </channel>
</rss>
""".strip()


def _fake_llm_eval(job, profile, constraints, model, description_max_chars=2500, input_description_max_chars=20000):
    title = str(job.get("title") or "")
    score = 82 if "Senior" in title else 58
    tier = "A" if score >= 70 else "B"
    input_limit = int(input_description_max_chars) if int(input_description_max_chars) > 0 else None
    raw_desc = str(job.get("description") or "")
    description = raw_desc[:input_limit] if input_limit else raw_desc
    return {
        "is_job_posting": True,
        "title": title,
        "company": str(job.get("company") or "Unknown"),
        "location": str(job.get("location") or "Europe"),
        "remote_hint": bool(job.get("remote_hint", True)),
        "description": description,
        "published": str(job.get("published") or ""),
        "score": score,
        "tier": tier,
        "reasons": ["llm prototype scoring"],
        "summary": "Prototype parse+score output",
        "quality_flags": [],
        "confidence": 0.9,
    }


class PipelineDbIntegrationTests(unittest.TestCase):
    def test_db_persistence_and_rerun_idempotency(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config_dir = root / "config"
            data_dir = root / "data"
            output_dir = root / "output"
            config_dir.mkdir(parents=True, exist_ok=True)
            data_dir.mkdir(parents=True, exist_ok=True)
            output_dir.mkdir(parents=True, exist_ok=True)

            db_path = root / "job_search.sqlite"

            save_json(
                config_dir / "profile.json",
                {
                    "location": "Innsbruck, Austria",
                    "target_titles": ["Senior Software Engineer", "Platform Engineer"],
                    "must_have_any": ["senior", "lead", "staff", "architect"],
                    "skills": ["python", "kubernetes", "terraform"],
                    "preferred_keywords": ["distributed systems", "platform", "cloud"],
                    "exclude_keywords": ["intern", "junior"],
                    "local_first": True,
                },
            )
            save_json(
                config_dir / "constraints.json",
                {
                    "require_remote_or_target_location": True,
                    "prefer_local_strong": True,
                    "target_location_keywords": ["innsbruck", "tyrol", "tirol", "austria", "österreich"],
                    "preferred_remote_regions": ["europe", "eu", "cet", "cest", "germany", "austria", "dach"],
                    "disallowed_remote_markers": ["us only"],
                    "exclude_if_contains": ["security clearance", "on-site only", "onsite only"],
                },
            )
            save_json(
                config_dir / "sources.json",
                {
                    "rss_sources": [
                        {
                            "name": "Fixture RSS",
                            "url": "fixture://rss/software-jobs",
                            "type": "remote",
                        }
                    ],
                    "html_sources": [],
                    "browser_sources": [],
                },
            )
            save_json(
                config_dir / "scoring.json",
                {"llm_pipeline": {"enabled": True, "model": "gpt-5-mini", "max_jobs_per_run": 50}},
            )
            save_json(
                config_dir / "runtime.json",
                {"source_fetch": {"max_retries": 1, "backoff_seconds": 0}},
            )
            save_json(
                config_dir / "database.json",
                {
                    "enabled": True,
                    "url": f"sqlite:///{db_path}",
                    "auto_migrate": True,
                },
            )
            save_json(
                data_dir / "applied_jobs.json",
                {
                    "applied": [
                        {
                            "url": "https://jobs.example.com/2",
                            "title": "Software Engineer (Innsbruck)",
                            "company": "",
                            "applied_at": "2026-01-01T00:00:00+00:00",
                        }
                    ]
                },
            )

            def fake_fetch_url(url: str, timeout: int = 20) -> str:
                if url == "fixture://rss/software-jobs":
                    return RSS_FIXTURE
                raise RuntimeError(f"unexpected URL in fixture test: {url}")

            with (
                patch("job_search.pipeline.CONFIG", config_dir),
                patch("job_search.pipeline.DATA", data_dir),
                patch("job_search.pipeline.OUTPUT", output_dir),
                patch("job_search.pipeline.fetch_url", side_effect=fake_fetch_url),
                patch("job_search.pipeline.llm_parse_job", side_effect=_fake_llm_eval),
                patch("job_search.pipeline.datetime", FixedDateTime),
                patch("job_search.ingestion.datetime", FixedDateTime),
                patch("job_search.reporting.datetime", FixedDateTime),
                patch(
                    "job_search.pipeline.uuid.uuid4",
                    side_effect=[
                        UUID("00000000-0000-0000-0000-000000000101"),
                        UUID("00000000-0000-0000-0000-000000000102"),
                    ],
                ),
                patch("builtins.print"),
            ):
                summary1 = run_pipeline()
                summary2 = run_pipeline()

            conn = sqlite3.connect(db_path)
            try:
                apps_count = conn.execute("SELECT COUNT(*) FROM applications").fetchone()[0]
                jobs_count = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
                runs_count = conn.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0]
                rankings_count = conn.execute("SELECT COUNT(*) FROM job_rankings").fetchone()[0]
                source_events_count = conn.execute("SELECT COUNT(*) FROM source_fetch_events").fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(summary1["skipped_applied"], 1)
            self.assertEqual(summary2["skipped_applied"], 1)
            self.assertGreaterEqual(summary2["llm"]["cache_hits"], 1)
            self.assertEqual(apps_count, 1)
            self.assertEqual(jobs_count, 2)
            self.assertEqual(runs_count, 2)
            self.assertEqual(rankings_count, 2)
            self.assertEqual(source_events_count, 2)


if __name__ == "__main__":
    unittest.main()
