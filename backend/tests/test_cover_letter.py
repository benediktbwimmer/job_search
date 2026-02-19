import json
import unittest
from unittest.mock import patch

from job_search.cover_letter import generate_cover_letter


class CoverLetterPromptTests(unittest.TestCase):
    def test_prompt_includes_temporal_and_matched_experience(self):
        profile = {
            "name": "Benedikt Wimmer",
            "location": "Innsbruck, Austria",
            "target_titles": ["Senior Software Engineer", "Platform Engineer"],
            "skills": [
                "python",
                "typescript",
                "javascript",
                "go",
                "java",
                "c++",
                "react",
                "angular",
                "django",
                "fastapi",
                "temporal",
                "kubernetes",
            ],
            "preferred_keywords": ["distributed systems", "platform", "cloud"],
            "experience_highlights": [
                {
                    "company": "Luftblick",
                    "role": "Senior Software Engineer",
                    "summary": "Worked extensively with Temporal for workflow orchestration.",
                    "impact": "Built durable workflows for business-critical processes.",
                    "technologies": ["temporal", "go", "distributed systems"],
                }
            ],
        }
        job = {
            "title": "Staff Developer Advocate Platform Engineering",
            "company": "Temporal Technologies",
            "location": "Remote",
            "url": "https://example.com/jobs/temporal-staff-devrel",
            "description": "Temporal platform role for distributed systems and workflow orchestration.",
        }

        captured = {}

        def _fake_call_openai_json(model: str, system_prompt: str, user_prompt: str, timeout_sec: int = 60):
            captured["model"] = model
            captured["user_prompt"] = json.loads(user_prompt)
            return {"body": "Draft body", "language": "en", "style": "concise"}

        with patch("job_search.cover_letter.call_openai_json", side_effect=_fake_call_openai_json):
            out = generate_cover_letter(
                profile=profile,
                job=job,
                cv_variant="en_short",
                style="concise",
                additional_context="Please explicitly mention my prior Temporal work.",
            )

        payload = captured["user_prompt"]
        skill_tokens = [str(x).lower() for x in payload["candidate_profile"]["skills"]]
        self.assertIn("temporal", skill_tokens)
        self.assertEqual(captured["model"], "gpt-5.2")
        self.assertEqual(out["model"], "gpt-5.2")

        matched = payload.get("matched_experience_highlights", [])
        self.assertTrue(matched)
        self.assertEqual(matched[0].get("company"), "Luftblick")

        merged_context = str(payload.get("additional_context") or "")
        self.assertIn("Please explicitly mention my prior Temporal work.", merged_context)
        self.assertIn("Luftblick", merged_context)


if __name__ == "__main__":
    unittest.main()

