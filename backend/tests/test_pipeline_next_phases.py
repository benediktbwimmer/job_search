import json
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
        base = datetime(2026, 1, 9, 9, 0, 0)
        if tz is not None:
            return base.replace(tzinfo=tz)
        return base


RSS_FIXTURE = """
<rss>
  <channel>
    <item>
      <title>Platform Engineer</title>
      <link>https://jobs.example.com/rss-platform</link>
      <description><![CDATA[Remote in Europe platform engineering role.]]></description>
      <pubDate>Mon, 01 Jan 2026 10:00:00 +0000</pubDate>
      <guid>rss-platform</guid>
    </item>
  </channel>
</rss>
""".strip()


GREENHOUSE_FIXTURE = """
{
  "jobs": [
    {
      "id": 101,
      "title": "Senior Platform Engineer",
      "absolute_url": "https://boards.greenhouse.io/acme/jobs/101",
      "content": "<p>Remote in Europe. Distributed systems and Kubernetes platform ownership.</p>",
      "location": {"name": "Remote - Europe"},
      "updated_at": "2026-01-08T09:00:00Z"
    }
  ]
}
""".strip()


LEVER_FIXTURE = """
[
  {
    "id": "lev-low",
    "text": "Not a real job card",
    "hostedUrl": "https://jobs.lever.co/acme/lev-low",
    "descriptionPlain": "This is listing noise and should be filtered in test",
    "categories": {"location": "Europe", "team": "Core"},
    "createdAt": 1760000000000
  }
]
""".strip()


def _fake_llm_eval(job, profile, constraints, model, description_max_chars=2500, input_description_max_chars=20000):
    url = str(job.get("url") or "")
    is_valid = not url.endswith("/lev-low")
    title = str(job.get("title") or "")
    input_limit = int(input_description_max_chars) if int(input_description_max_chars) > 0 else None
    raw_desc = str(job.get("description") or "")
    description = raw_desc[:input_limit] if input_limit else raw_desc
    return {
        "is_job_posting": is_valid,
        "title": title,
        "company": str(job.get("company") or "Acme"),
        "location": str(job.get("location") or "Europe"),
        "remote_hint": bool(job.get("remote_hint", True)),
        "description": description,
        "published": str(job.get("published") or ""),
        "score": 88 if "Senior" in title else 74,
        "tier": "A",
        "reasons": ["llm prototype scoring"],
        "summary": "Prototype parse+score output",
        "quality_flags": [],
        "confidence": 0.9,
    }


class PipelineNextPhasesTests(unittest.TestCase):
    def test_llm_pipeline_filters_invalid_items(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config_dir = root / "config"
            data_dir = root / "data"
            output_dir = root / "output"
            config_dir.mkdir(parents=True, exist_ok=True)
            data_dir.mkdir(parents=True, exist_ok=True)
            output_dir.mkdir(parents=True, exist_ok=True)

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
                    "target_location_keywords": ["innsbruck", "austria"],
                    "preferred_remote_regions": ["europe", "eu", "cet"],
                    "disallowed_remote_markers": ["us only"],
                    "exclude_if_contains": ["security clearance"],
                },
            )
            save_json(
                config_dir / "sources.json",
                {
                    "rss_sources": [{"name": "Fixture RSS", "url": "fixture://rss/phase-next", "type": "remote"}],
                    "html_sources": [],
                    "browser_sources": [],
                    "greenhouse_sources": [
                        {
                            "name": "Acme Greenhouse",
                            "url": "fixture://greenhouse/acme",
                            "type": "remote",
                            "company_name": "Acme",
                        }
                    ],
                    "lever_sources": [
                        {
                            "name": "Acme Lever",
                            "url": "fixture://lever/acme",
                            "type": "remote",
                            "company_name": "Acme",
                        }
                    ],
                },
            )
            save_json(
                config_dir / "scoring.json",
                {"llm_pipeline": {"enabled": True, "model": "gpt-5-mini", "max_jobs_per_run": 50, "drop_invalid": True}},
            )
            save_json(
                config_dir / "runtime.json",
                {
                    "source_fetch": {"max_retries": 0, "backoff_seconds": 0},
                },
            )
            save_json(
                config_dir / "database.json",
                {"enabled": False, "url": "sqlite:///data/job_search.sqlite", "auto_migrate": False},
            )
            save_json(data_dir / "applied_jobs.json", {"applied": []})

            def fake_fetch_url(url: str, timeout: int = 20) -> str:
                if url == "fixture://rss/phase-next":
                    return RSS_FIXTURE
                if url == "fixture://greenhouse/acme":
                    return GREENHOUSE_FIXTURE
                if url == "fixture://lever/acme":
                    return LEVER_FIXTURE
                raise RuntimeError(f"unexpected URL in phase test: {url}")

            with (
                patch("job_search.pipeline.CONFIG", config_dir),
                patch("job_search.pipeline.DATA", data_dir),
                patch("job_search.pipeline.OUTPUT", output_dir),
                patch("job_search.pipeline.fetch_url", side_effect=fake_fetch_url),
                patch("job_search.pipeline.llm_parse_job", side_effect=_fake_llm_eval),
                patch("job_search.pipeline.datetime", FixedDateTime),
                patch("job_search.ingestion.datetime", FixedDateTime),
                patch("job_search.reporting.datetime", FixedDateTime),
                patch("job_search.pipeline.uuid.uuid4", return_value=UUID("00000000-0000-0000-0000-000000000901")),
                patch("builtins.print"),
            ):
                summary = run_pipeline()

            ranked = json.loads((data_dir / "jobs_normalized.json").read_text())
            urls = {x["url"] for x in ranked}
            self.assertIn("https://boards.greenhouse.io/acme/jobs/101", urls)
            self.assertIn("https://jobs.example.com/rss-platform", urls)
            self.assertNotIn("https://jobs.lever.co/acme/lev-low", urls)
            self.assertEqual(summary["llm"]["filtered_invalid"], 1)
            self.assertEqual(summary["total"], 2)


if __name__ == "__main__":
    unittest.main()
