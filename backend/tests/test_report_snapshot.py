import unittest
from datetime import datetime
from pathlib import Path

from job_search.reporting import markdown_report


class ReportSnapshotTests(unittest.TestCase):
    def test_markdown_report_snapshot(self):
        jobs = [
            {
                "title": "Senior Backend Engineer",
                "url": "https://example.com/a",
                "score": 82,
                "tier": "A",
                "source": "Demo",
                "location": "Innsbruck",
                "llm_summary": "Strong local fit",
                "llm_pros": ["Python", "AWS"],
                "llm_risks": ["No salary"],
                "skill_hits": ["python", "aws"],
                "reasons": ["target geography"],
            },
            {
                "title": "Platform Engineer",
                "url": "https://example.com/b",
                "score": 61,
                "tier": "B",
                "company": "ACME",
                "source": "Demo",
                "reasons": ["remote fit"],
            },
        ]
        errors = [{"source": "Demo RSS", "error": "timeout"}]

        out = markdown_report(
            jobs_ranked=jobs,
            skipped_applied=1,
            errors=errors,
            now=datetime(2026, 1, 2, 9, 30),
        )
        expected = (Path(__file__).resolve().parent / "fixtures" / "report_snapshot.md").read_text().rstrip("\n")
        self.assertEqual(out, expected)


if __name__ == "__main__":
    unittest.main()
