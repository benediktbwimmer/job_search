import unittest

from job_search.adaptive_scoring import adaptive_bonus_for_job, build_adaptive_profile


class AdaptiveScoringTests(unittest.TestCase):
    def test_positive_feedback_boosts_matching_job(self):
        signal_data = {
            "applications": [
                {
                    "status": "interview",
                    "source": "Fixture RSS",
                    "source_type": "remote",
                    "job_title": "Platform Engineer",
                    "job_company": "ACME",
                }
            ],
            "feedback": [
                {
                    "action": "applied",
                    "source": "Fixture RSS",
                    "source_type": "remote",
                    "job_title": "Platform Engineer",
                    "job_company": "ACME",
                }
            ],
        }
        profile = build_adaptive_profile(signal_data)
        bonus, reasons = adaptive_bonus_for_job(
            {
                "source": "Fixture RSS",
                "source_type": "remote",
                "title": "Senior Platform Reliability Engineer",
                "company": "ACME",
            },
            profile,
        )

        self.assertGreater(bonus, 0)
        self.assertTrue(any("source" in r or "title" in r for r in reasons))

    def test_negative_feedback_penalizes_matching_job(self):
        signal_data = {
            "applications": [
                {
                    "status": "rejected",
                    "source": "Bad Source",
                    "source_type": "remote",
                    "job_title": "QA Engineer",
                    "job_company": "Nope Inc",
                }
            ],
            "feedback": [
                {
                    "action": "dismissed",
                    "source": "Bad Source",
                    "source_type": "remote",
                    "job_title": "QA Engineer",
                    "job_company": "Nope Inc",
                }
            ],
        }
        profile = build_adaptive_profile(signal_data)
        bonus, _ = adaptive_bonus_for_job(
            {
                "source": "Bad Source",
                "source_type": "remote",
                "title": "QA Engineer",
                "company": "Nope Inc",
            },
            profile,
        )

        self.assertLess(bonus, 0)


if __name__ == "__main__":
    unittest.main()
