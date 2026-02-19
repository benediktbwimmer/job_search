#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from job_search.auth import generate_api_key, normalize_auth_config, validate_auth_config
from job_search.json_io import load_json, save_json
from job_search.paths import CONFIG


def main():
    parser = argparse.ArgumentParser(description="Generate API keys for one or more users")
    parser.add_argument("--user", action="append", required=True, help="User ID (repeat for multiple users)")
    parser.add_argument("--prefix", default="js", help="Key prefix (default: js)")
    parser.add_argument(
        "--replace-user-keys",
        action="store_true",
        help="Remove existing keys for each provided user before generating new ones",
    )
    parser.add_argument(
        "--auth-path",
        default=str(CONFIG / "auth.json"),
        help="Path to auth config JSON (default: config/auth.json)",
    )
    parser.add_argument(
        "--leave-disabled",
        action="store_true",
        help="Do not auto-enable auth after seeding keys",
    )
    args = parser.parse_args()

    auth_path = Path(args.auth_path).expanduser().resolve()
    cfg_raw = load_json(auth_path, default={"enabled": False, "api_keys": {}})
    cfg = normalize_auth_config(cfg_raw)
    if args.replace_user_keys:
        users_set = {str(u).strip() for u in args.user if str(u).strip()}
        cfg["api_keys"] = {k: v for k, v in cfg.get("api_keys", {}).items() if v not in users_set}

    generated = {}
    for user_id in args.user:
        user = str(user_id or "").strip()
        if not user:
            continue
        token = generate_api_key(prefix=args.prefix)
        while token in cfg["api_keys"]:
            token = generate_api_key(prefix=args.prefix)
        cfg["api_keys"][token] = user
        generated[user] = token

    if not args.leave_disabled:
        cfg["enabled"] = True

    errors = validate_auth_config(cfg)
    if errors:
        joined = "; ".join(errors)
        raise SystemExit(f"auth config invalid after generation: {joined}")

    save_json(auth_path, cfg)
    print(f"Auth config updated: {auth_path}")
    for user, token in generated.items():
        print(f"{user}: {token}")


if __name__ == "__main__":
    main()
