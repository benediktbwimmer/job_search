import unittest

from job_search.ranking import score_job, skill_in_text


PROFILE = {
    "must_have_any": ["senior", "lead", "staff", "architect"],
    "target_titles": ["Senior Software Engineer", "Platform Engineer"],
    "skills": ["python", "go", "kubernetes"],
    "preferred_keywords": ["distributed systems"],
    "exclude_keywords": ["intern", "junior"],
}

CONSTRAINTS = {
    "require_remote_or_target_location": True,
    "prefer_local_strong": True,
    "target_location_keywords": ["innsbruck", "tirol", "austria"],
    "preferred_remote_regions": ["europe", "cet"],
    "disallowed_remote_markers": ["us only"],
    "exclude_if_contains": ["security clearance"],
}


class RankingTests(unittest.TestCase):
    def test_skill_matching_handles_go_edge_case(self):
        self.assertTrue(skill_in_text("go", "experience with Go and Python".lower()))
        self.assertFalse(skill_in_text("go", "work with go-to-market teams".lower()))

    def test_local_non_senior_role_gets_floor(self):
        job = {
            "title": "Software Engineer",
            "description": "Build backend services",
            "location": "Innsbruck",
            "source_type": "innsbruck",
            "remote_hint": False,
        }
        score, tier, reasons, _ = score_job(job, PROFILE, CONSTRAINTS)
        self.assertEqual(score, 55)
        self.assertEqual(tier, "B")
        self.assertIn("local-first role floor", reasons)

    def test_disallowed_geo_remote_is_rejected(self):
        job = {
            "title": "Senior Platform Engineer",
            "description": "Fully remote, US only",
            "location": "",
            "source_type": "remote",
            "remote_hint": True,
        }
        score, tier, reasons, _ = score_job(job, PROFILE, CONSTRAINTS)
        self.assertEqual(score, 0)
        self.assertEqual(tier, "C")
        self.assertEqual(reasons, ["geo restricted remote"])

    def test_company_watchlist_adds_boost(self):
        job = {
            "title": "Senior Platform Engineer",
            "description": "Remote in Europe with Python",
            "location": "Europe",
            "source_type": "remote",
            "remote_hint": True,
            "company": "Acme Labs",
            "url": "https://jobs.example.com/acme/1",
            "source": "Acme Greenhouse",
        }
        base_score, _, _, _ = score_job(job, PROFILE, CONSTRAINTS)
        boosted_score, _, reasons, _ = score_job(
            job,
            PROFILE,
            CONSTRAINTS,
            watchlist_cfg={"enabled": True, "companies": ["acme"], "score_boost": 12},
        )
        self.assertGreater(boosted_score, base_score)
        self.assertIn("company watchlist", reasons)


if __name__ == "__main__":
    unittest.main()
