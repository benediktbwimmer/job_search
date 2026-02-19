import unittest

from job_search.cv_recommendation import recommend_cv_variant


class CvRecommendationTests(unittest.TestCase):
    def test_recommends_de_long_for_local_senior_role(self):
        job = {
            "title": "Senior Software Engineer",
            "description": "Standort Innsbruck. Cloud platform architecture and distributed systems.",
            "location": "Innsbruck, Austria",
            "source_type": "innsbruck",
        }
        variant, reasons = recommend_cv_variant(job, cfg={"enabled": True, "default_language": "en"})
        self.assertEqual(variant, "de_long")
        self.assertTrue(reasons)

    def test_recommends_en_short_for_simple_remote_role(self):
        job = {
            "title": "Software Engineer",
            "description": "Remote role, English only. Build APIs.",
            "location": "Remote",
            "source_type": "remote",
        }
        variant, reasons = recommend_cv_variant(job, cfg={"enabled": True, "default_language": "en"})
        self.assertEqual(variant, "en_short")
        self.assertTrue(reasons)


if __name__ == "__main__":
    unittest.main()
