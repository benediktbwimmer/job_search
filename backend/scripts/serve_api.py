#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from job_search.api_server import serve_api
from job_search.auth import normalize_auth_config, validate_auth_config
from job_search.json_io import load_json
from job_search.paths import CONFIG, DB
from job_search.storage.repository import JobSearchRepository


def main():
    parser = argparse.ArgumentParser(description="Serve a lightweight JSON API for job search data")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host")
    parser.add_argument("--port", type=int, default=8787, help="Bind port")
    parser.add_argument("--db-url", default="", help="Override DB URL")
    parser.add_argument(
        "--frontend-dist",
        default="",
        help="Optional path to frontend dist directory (defaults to ../frontend/dist).",
    )
    args = parser.parse_args()

    db_cfg = load_json(CONFIG / "database.json", default={})
    db_url = args.db_url.strip() or str(db_cfg.get("url") or "").strip()
    if not db_url:
        raise SystemExit("Database URL is missing. Configure config/database.json or pass --db-url.")

    repo = JobSearchRepository(
        db_url=db_url,
        migrations_dir=DB / "migrations",
        auto_migrate=bool(db_cfg.get("auto_migrate", False)),
    )
    repo.initialize()
    profile = load_json(CONFIG / "profile.json", default={})
    auth_cfg_raw = load_json(CONFIG / "auth.json", default={"enabled": False, "api_keys": {}})
    auth_errors = validate_auth_config(auth_cfg_raw)
    if auth_errors:
        joined = "; ".join(auth_errors)
        raise SystemExit(f"Invalid auth config in config/auth.json: {joined}")
    auth_cfg = normalize_auth_config(auth_cfg_raw)

    frontend_dist = args.frontend_dist.strip() or None
    server = serve_api(
        repo=repo,
        host=args.host,
        port=args.port,
        profile=profile,
        auth_config=auth_cfg,
        frontend_dist=frontend_dist,
    )
    print(f"API listening on http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
