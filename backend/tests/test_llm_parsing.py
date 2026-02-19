import unittest
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from job_search.llm_parsing import (
    llm_parse_cache_key,
    llm_parse_job,
    load_llm_parse_cache,
    normalize_llm_parse_output,
    save_llm_parse_cache,
)


class LlmParsingTests(unittest.TestCase):
    def test_cache_roundtrip(self):
        with TemporaryDirectory() as td:
            path = Path(td) / "cache.json"
            cache = {"meta": {"version": 1}, "entries": {"a": {"score": 50}}}
            save_llm_parse_cache(path, cache)
            loaded = load_llm_parse_cache(path)
            self.assertEqual(loaded["entries"]["a"]["score"], 50)

    def test_cache_key_changes_with_description(self):
        job_a = {"url": "https://jobs.example.com/1", "title": "Engineer", "description": "A"}
        job_b = {"url": "https://jobs.example.com/1", "title": "Engineer", "description": "B"}
        key_a = llm_parse_cache_key(job_a, model="gpt-5-mini", prompt_version="v2")
        key_b = llm_parse_cache_key(job_b, model="gpt-5-mini", prompt_version="v2")
        self.assertNotEqual(key_a, key_b)

    def test_llm_parse_job_normalizes_output(self):
        with patch(
            "job_search.llm_parsing.call_openai_json",
            return_value={
                "is_job_posting": True,
                "title": "Senior Platform Engineer",
                "company": "ACME",
                "location": "Europe",
                "remote_hint": True,
                "description": "x" * 4000,
                "published": "2026-01-01T00:00:00Z",
                "score": 88,
                "tier": "A",
                "reasons": ["great fit"],
                "summary": "Strong fit",
                "quality_flags": ["low_company_confidence"],
                "confidence": 0.81,
            },
        ):
            out = llm_parse_job(
                job={"title": "x", "description": "y"},
                profile={"skills": [], "target_titles": [], "preferred_keywords": []},
                constraints={},
                model="gpt-5-mini",
                description_max_chars=500,
            )
        self.assertEqual(out["tier"], "A")
        self.assertEqual(out["score"], 88)
        self.assertLessEqual(len(out["description"]), 500)

    def test_llm_parse_job_company_fallback_from_description(self):
        with patch(
            "job_search.llm_parsing.call_openai_json",
            return_value={
                "is_job_posting": True,
                "title": "Staff Developer Advocate Platform Engineering",
                "company": "that partners with American software product companies to scale their development footprint",
                "location": "Remote",
                "remote_hint": True,
                "description": "Temporal is hiring a Staff Developer Advocate for platform engineering.",
                "published": "",
                "score": 88,
                "tier": "A",
                "reasons": ["fit"],
                "summary": "good fit",
                "quality_flags": [],
                "confidence": 0.8,
            },
        ):
            out = llm_parse_job(
                job={
                    "title": "Staff Developer Advocate Platform Engineering",
                    "company": "",
                    "description": "Temporal is hiring a Staff Developer Advocate for platform engineering.",
                },
                profile={"skills": [], "target_titles": [], "preferred_keywords": []},
                constraints={},
                model="gpt-5-mini",
                description_max_chars=0,
            )
        self.assertEqual(out["company"], "Temporal")

    def test_llm_parse_job_no_truncation_when_limit_zero(self):
        with patch(
            "job_search.llm_parsing.call_openai_json",
            return_value={
                "is_job_posting": True,
                "title": "Senior Platform Engineer",
                "company": "ACME",
                "location": "Europe",
                "remote_hint": True,
                "description": "x" * 7000,
                "published": "2026-01-01T00:00:00Z",
                "score": 80,
                "tier": "A",
                "reasons": ["good"],
                "summary": "strong",
                "quality_flags": [],
                "confidence": 0.9,
            },
        ):
            out = llm_parse_job(
                job={"title": "x", "description": "y"},
                profile={"skills": [], "target_titles": [], "preferred_keywords": []},
                constraints={},
                model="gpt-5-mini",
                description_max_chars=0,
            )
        self.assertEqual(len(out["description"]), 7000)

    def test_llm_parse_job_company_fallback_from_remoteok_url(self):
        with patch(
            "job_search.llm_parsing.call_openai_json",
            return_value={
                "is_job_posting": True,
                "title": "Senior Support Engineer",
                "company": "",
                "location": "Remote",
                "remote_hint": True,
                "description": "HiveMQ is the Industrial AI Platform helping enterprises.",
                "published": "",
                "score": 70,
                "tier": "A",
                "reasons": ["fit"],
                "summary": "good fit",
                "quality_flags": [],
                "confidence": 0.8,
            },
        ):
            out = llm_parse_job(
                job={
                    "title": "Senior Support Engineer",
                    "company": "",
                    "url": "https://remoteok.com/remote-jobs/remote-senior-support-engineer-hivemq-1129892",
                    "description": "HiveMQ is the Industrial AI Platform helping enterprises.",
                },
                profile={"skills": [], "target_titles": [], "preferred_keywords": []},
                constraints={},
                model="gpt-5-mini",
                description_max_chars=0,
            )
        self.assertEqual(out["company"], "HiveMQ")

    def test_llm_parse_job_company_fallback_from_weworkremotely_slug(self):
        with patch(
            "job_search.llm_parsing.call_openai_json",
            return_value={
                "is_job_posting": True,
                "title": "Head of Web Dev",
                "company": "",
                "location": "Remote",
                "remote_hint": True,
                "description": "Hands-on technical leadership role.",
                "published": "",
                "score": 62,
                "tier": "B",
                "reasons": ["ok fit"],
                "summary": "decent fit",
                "quality_flags": [],
                "confidence": 0.75,
            },
        ):
            out = llm_parse_job(
                job={
                    "title": "Activate Talent: Head of Web Dev",
                    "company": "",
                    "url": "https://weworkremotely.com/remote-jobs/activate-talent-head-of-web-dev",
                    "description": "Hands-on technical leadership role.",
                },
                profile={"skills": [], "target_titles": [], "preferred_keywords": []},
                constraints={},
                model="gpt-5-mini",
                description_max_chars=0,
            )
        self.assertEqual(out["company"], "Activate Talent")

    def test_normalize_canonicalizes_domain_style_company(self):
        normalized = normalize_llm_parse_output(
            job={
                "title": "Experienced Full-Stack Software Engineer",
                "company": "comparis.ch",
                "url": "https://weworkremotely.com/remote-jobs/comparis-ch-experienced-full-stack-software-engineer",
                "description": "Work on modern products",
            },
            llm_out={"company": "", "title": "Experienced Full-Stack Software Engineer", "description": "Work on modern products"},
            description_max_chars=0,
        )
        self.assertEqual(normalized["company"], "Comparis")

    def test_llm_parse_job_input_description_no_truncation_when_zero(self):
        seen = {"description_len": 0}

        def _fake_call(model, system_prompt, user_prompt):
            payload = json.loads(user_prompt)
            seen["description_len"] = len(str(payload["raw_item"]["description"]))
            return {
                "is_job_posting": True,
                "title": "Senior Platform Engineer",
                "company": "ACME",
                "location": "Europe",
                "remote_hint": True,
                "description": "ok",
                "published": "2026-01-01T00:00:00Z",
                "score": 75,
                "tier": "A",
                "reasons": ["fit"],
                "summary": "fit",
                "quality_flags": [],
                "confidence": 0.9,
            }

        long_desc = "abc " * 10000
        with patch("job_search.llm_parsing.call_openai_json", side_effect=_fake_call):
            llm_parse_job(
                job={"title": "x", "description": long_desc},
                profile={"skills": [], "target_titles": [], "preferred_keywords": []},
                constraints={},
                model="gpt-5-mini",
                description_max_chars=0,
                input_description_max_chars=0,
            )
        self.assertEqual(seen["description_len"], len(long_desc))

    def test_llm_parse_job_rejects_sentence_fragment_company(self):
        with patch(
            "job_search.llm_parsing.call_openai_json",
            return_value={
                "is_job_posting": True,
                "title": "Senior Engineer",
                "company": "Anthropic. In this hands-on technical role",
                "location": "Remote",
                "remote_hint": True,
                "description": "Anthropic is hiring a Senior Engineer to work on AI systems.",
                "published": "",
                "score": 81,
                "tier": "A",
                "reasons": ["fit"],
                "summary": "good fit",
                "quality_flags": [],
                "confidence": 0.8,
            },
        ):
            out = llm_parse_job(
                job={
                    "title": "Senior Engineer",
                    "company": "",
                    "description": "Anthropic is hiring a Senior Engineer to work on AI systems.",
                },
                profile={"skills": [], "target_titles": [], "preferred_keywords": []},
                constraints={},
                model="gpt-5-mini",
                description_max_chars=0,
            )
        self.assertEqual(out["company"], "Anthropic")

    def test_normalize_llm_parse_output_prefers_full_raw_description_when_unbounded(self):
        raw = "raw-description-" + ("x" * 2000)
        llm_out = {
            "title": "Senior Platform Engineer",
            "company": "ACME",
            "location": "Europe",
            "description": "short",
            "published": "2026-01-01T00:00:00Z",
            "summary": "ok",
        }
        normalized = normalize_llm_parse_output(
            job={"title": "Senior Platform Engineer", "description": raw},
            llm_out=llm_out,
            description_max_chars=0,
        )
        self.assertEqual(normalized["description"], raw)


if __name__ == "__main__":
    unittest.main()
