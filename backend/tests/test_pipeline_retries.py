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
        base = datetime(2026, 1, 6, 9, 0, 0)
        if tz is not None:
            return base.replace(tzinfo=tz)
        return base


RSS_FIXTURE = """
<rss>
  <channel>
    <item>
      <title>Senior Platform Engineer at Retry Labs</title>
      <link>https://jobs.example.com/retry-1</link>
      <description><![CDATA[Remote in Europe. Distributed systems.]]></description>
      <pubDate>Mon, 01 Jan 2026 10:00:00 +0000</pubDate>
      <guid>retry-1</guid>
    </item>
  </channel>
</rss>
""".strip()


def _fake_llm_eval(job, profile, constraints, model, description_max_chars=2500, input_description_max_chars=20000):
    input_limit = int(input_description_max_chars) if int(input_description_max_chars) > 0 else None
    raw_desc = str(job.get("description") or "")
    description = raw_desc[:input_limit] if input_limit else raw_desc
    return {
        "is_job_posting": True,
        "title": str(job.get("title") or ""),
        "company": str(job.get("company") or "Retry Labs"),
        "location": str(job.get("location") or "Europe"),
        "remote_hint": bool(job.get("remote_hint", True)),
        "description": description,
        "published": str(job.get("published") or ""),
        "score": 80,
        "tier": "A",
        "reasons": ["llm prototype scoring"],
        "summary": "Prototype parse+score output",
        "quality_flags": [],
        "confidence": 0.9,
    }


class PipelineRetriesTests(unittest.TestCase):
    def test_source_retry_attempts_are_recorded(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config_dir = root / "config"
            data_dir = root / "data"
            output_dir = root / "output"
            config_dir.mkdir(parents=True, exist_ok=True)
            data_dir.mkdir(parents=True, exist_ok=True)
            output_dir.mkdir(parents=True, exist_ok=True)

            db_path = root / "retry.sqlite"

            save_json(
                config_dir / "profile.json",
                {
                    "location": "Innsbruck, Austria",
                    "target_titles": ["Senior Software Engineer", "Platform Engineer"],
                    "must_have_any": ["senior", "lead", "staff", "architect"],
                    "skills": ["python"],
                    "preferred_keywords": ["distributed systems", "platform"],
                    "exclude_keywords": ["intern", "junior"],
                    "local_first": True,
                },
            )
            save_json(
                config_dir / "constraints.json",
                {
                    "require_remote_or_target_location": True,
                    "prefer_local_strong": True,
                    "target_location_keywords": ["innsbruck", "austria"],
                    "preferred_remote_regions": ["europe", "eu", "cet"],
                    "disallowed_remote_markers": ["us only"],
                    "exclude_if_contains": ["security clearance"],
                },
            )
            save_json(
                config_dir / "sources.json",
                {
                    "rss_sources": [
                        {
                            "name": "Retry RSS",
                            "url": "fixture://rss/retry",
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
            save_json(data_dir / "applied_jobs.json", {"applied": []})

            calls = {"n": 0}

            def fake_fetch_url(url: str, timeout: int = 20) -> str:
                if url != "fixture://rss/retry":
                    raise RuntimeError(f"unexpected URL in retry test: {url}")
                calls["n"] += 1
                if calls["n"] == 1:
                    raise RuntimeError("transient failure")
                return RSS_FIXTURE

            with (
                patch("job_search.pipeline.CONFIG", config_dir),
                patch("job_search.pipeline.DATA", data_dir),
                patch("job_search.pipeline.OUTPUT", output_dir),
                patch("job_search.pipeline.fetch_url", side_effect=fake_fetch_url),
                patch("job_search.pipeline.llm_parse_job", side_effect=_fake_llm_eval),
                patch("job_search.pipeline.datetime", FixedDateTime),
                patch("job_search.ingestion.datetime", FixedDateTime),
                patch("job_search.reporting.datetime", FixedDateTime),
                patch("job_search.pipeline.uuid.uuid4", return_value=UUID("00000000-0000-0000-0000-000000000201")),
                patch("builtins.print"),
            ):
                summary = run_pipeline()

            conn = sqlite3.connect(db_path)
            try:
                row = conn.execute(
                    """
                    SELECT attempts, success, jobs_fetched, error_message
                    FROM source_fetch_events
                    WHERE run_id = ? AND source_name = ?
                    """,
                    ("00000000-0000-0000-0000-000000000201", "Retry RSS"),
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(summary["total"], 1)
            self.assertEqual(calls["n"], 2)
            self.assertIsNotNone(row)
            self.assertEqual(row[0], 2)
            self.assertEqual(row[1], 1)
            self.assertEqual(row[2], 1)
            self.assertIsNone(row[3])


if __name__ == "__main__":
    unittest.main()
