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
        base = datetime(2026, 1, 7, 9, 0, 0)
        if tz is not None:
            return base.replace(tzinfo=tz)
        return base


RSS_FIXTURE_A = """
<rss>
  <channel>
    <item>
      <title>Platform Engineer</title>
      <link>https://jobs.example.com/new-platform</link>
      <description><![CDATA[Remote in Europe platform role]]></description>
      <pubDate>Mon, 01 Jan 2026 10:00:00 +0000</pubDate>
      <guid>new-platform</guid>
    </item>
  </channel>
</rss>
""".strip()


RSS_FIXTURE_B = """
<rss>
  <channel>
    <item>
      <title>Platform Engineer</title>
      <link>https://jobs.example.com/new-platform</link>
      <description><![CDATA[Remote in Europe platform role UPDATED]]></description>
      <pubDate>Mon, 01 Jan 2026 10:00:00 +0000</pubDate>
      <guid>new-platform</guid>
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
        "company": str(job.get("company") or "Unknown"),
        "location": str(job.get("location") or "Europe"),
        "remote_hint": bool(job.get("remote_hint", True)),
        "description": description,
        "published": str(job.get("published") or ""),
        "score": 79,
        "tier": "A",
        "reasons": ["llm prototype scoring"],
        "summary": "Prototype parse+score output",
        "quality_flags": [],
        "confidence": 0.9,
    }


class PipelineAdaptiveTests(unittest.TestCase):
    def test_llm_cache_is_reused_and_invalidated_on_content_change(self):
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
                    "rss_sources": [
                        {"name": "Fixture RSS", "url": "fixture://rss/cache", "type": "remote"},
                    ],
                    "html_sources": [],
                    "browser_sources": [],
                },
            )
            save_json(
                config_dir / "scoring.json",
                {"llm_pipeline": {"enabled": True, "model": "gpt-5-mini", "max_jobs_per_run": 50}},
            )
            save_json(config_dir / "runtime.json", {"source_fetch": {"max_retries": 0, "backoff_seconds": 0}})
            save_json(
                config_dir / "database.json",
                {"enabled": False, "url": "sqlite:///data/job_search.sqlite", "auto_migrate": False},
            )
            save_json(data_dir / "applied_jobs.json", {"applied": []})

            state = {"fixture": RSS_FIXTURE_A}

            def fake_fetch_url(url: str, timeout: int = 20) -> str:
                if url == "fixture://rss/cache":
                    return state["fixture"]
                raise RuntimeError(f"unexpected URL in cache test: {url}")

            with (
                patch("job_search.pipeline.CONFIG", config_dir),
                patch("job_search.pipeline.DATA", data_dir),
                patch("job_search.pipeline.OUTPUT", output_dir),
                patch("job_search.pipeline.fetch_url", side_effect=fake_fetch_url),
                patch("job_search.pipeline.llm_parse_job", side_effect=_fake_llm_eval) as mocked_eval,
                patch("job_search.pipeline.datetime", FixedDateTime),
                patch("job_search.ingestion.datetime", FixedDateTime),
                patch("job_search.reporting.datetime", FixedDateTime),
                patch(
                    "job_search.pipeline.uuid.uuid4",
                    side_effect=[
                        UUID("00000000-0000-0000-0000-000000000301"),
                        UUID("00000000-0000-0000-0000-000000000302"),
                        UUID("00000000-0000-0000-0000-000000000303"),
                    ],
                ),
                patch("builtins.print"),
            ):
                run1 = run_pipeline()
                run2 = run_pipeline()
                state["fixture"] = RSS_FIXTURE_B
                run3 = run_pipeline()

            self.assertEqual(run1["llm"]["scored_live"], 1)
            self.assertEqual(run2["llm"]["cache_hits"], 1)
            self.assertEqual(run3["llm"]["scored_live"], 1)
            self.assertEqual(mocked_eval.call_count, 2)


if __name__ == "__main__":
    unittest.main()
