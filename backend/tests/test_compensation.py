import unittest

from job_search.compensation import extract_salary_info, salary_meets_threshold


class CompensationTests(unittest.TestCase):
    def test_extract_usd_salary_range(self):
        job = {
            "title": "Senior Engineer",
            "description": "Compensation: $140,000 - $170,000 per year plus bonus.",
        }
        salary = extract_salary_info(job)
        self.assertEqual(salary["currency"], "USD")
        self.assertEqual(salary["period"], "year")
        self.assertEqual(salary["min_amount"], 140000)
        self.assertEqual(salary["max_amount"], 170000)
        self.assertTrue(salary["annual_min_eur"] > 100000)

    def test_extract_eur_hourly_salary(self):
        job = {
            "title": "Contract Engineer",
            "description": "Rate: €80 per hour, long-term contract.",
        }
        salary = extract_salary_info(job)
        self.assertEqual(salary["currency"], "EUR")
        self.assertEqual(salary["period"], "hour")
        self.assertEqual(salary["min_amount"], 80)
        self.assertEqual(salary["annual_min_eur"], 166400)

    def test_salary_threshold(self):
        salary = {
            "currency": "EUR",
            "period": "year",
            "min_amount": 65000,
            "annual_min_eur": 65000,
        }
        self.assertTrue(salary_meets_threshold(salary, 60000))
        self.assertFalse(salary_meets_threshold(salary, 70000))
        self.assertIsNone(salary_meets_threshold({}, 70000))


if __name__ == "__main__":
    unittest.main()
