import unittest

from job_search.auth import generate_api_key, normalize_auth_config, validate_auth_config


class AuthTests(unittest.TestCase):
    def test_generate_api_key_shape(self):
        key = generate_api_key(prefix="job")
        self.assertTrue(key.startswith("job_"))
        self.assertGreaterEqual(len(key), 20)

    def test_validate_auth_config_errors(self):
        cfg = {
            "enabled": "yes",
            "api_keys": {
                "short": "",
            },
        }
        errors = validate_auth_config(cfg)
        self.assertTrue(errors)
        self.assertTrue(any("enabled" in e for e in errors))
        self.assertTrue(any("too short" in e or "cannot be empty" in e for e in errors))

    def test_normalize_auth_config(self):
        cfg = normalize_auth_config(
            {
                "enabled": 1,
                "api_keys": {
                    " token-1 ": " user-a ",
                    "": "user-b",
                },
            }
        )
        self.assertEqual(cfg["enabled"], True)
        self.assertEqual(cfg["api_keys"], {"token-1": "user-a"})


if __name__ == "__main__":
    unittest.main()
