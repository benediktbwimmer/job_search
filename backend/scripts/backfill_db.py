#!/usr/bin/env python3
import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from job_search.json_io import load_json
from job_search.models import ApplicationRecord, JobRankingRecord, JobRecord, PipelineRunRecord
from job_search.paths import CONFIG, DATA, DB, OUTPUT
from job_search.storage.repository import JobSearchRepository


def _slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_-]+", "-", value.strip())
    return cleaned.strip("-") or "latest"


def _build_run_record_from_report(report: dict, total_jobs: int) -> dict:
    generated_at = str(report.get("generated_at") or datetime.now(timezone.utc).isoformat())
    run_id = f"backfill-{_slugify(generated_at)}"
    tiers = report.get("tiers", {}) if isinstance(report, dict) else {}
    llm = report.get("llm", {}) if isinstance(report, dict) else {}

    return {
        "run_id": run_id,
        "started_at": generated_at,
        "ended_at": generated_at,
        "duration_ms": 0,
        "status": "success",
        "total_jobs": int(report.get("total", total_jobs)) if isinstance(report, dict) else total_jobs,
        "a_tier": int(tiers.get("A", 0)),
        "b_tier": int(tiers.get("B", 0)),
        "c_tier": int(tiers.get("C", 0)),
        "skipped_applied": int(report.get("skipped_applied", 0)) if isinstance(report, dict) else 0,
        "llm_enabled": bool(llm.get("enabled", False)),
        "llm_model": llm.get("model"),
        "llm_scored_live": int(llm.get("scored_live", 0)) if isinstance(llm, dict) else 0,
        "llm_cache_hits": int(llm.get("cache_hits", 0)) if isinstance(llm, dict) else 0,
        "llm_failed": int(llm.get("failed", 0)) if isinstance(llm, dict) else 0,
        "source_errors": len(report.get("errors", [])) if isinstance(report, dict) else 0,
        "error_message": None,
        "summary": report if isinstance(report, dict) else {},
    }


def main():
    parser = argparse.ArgumentParser(description="Backfill SQLite from existing JSON artifacts")
    parser.add_argument("--db-url", default="", help="Override DB URL (e.g., sqlite:///data/job_search.sqlite)")
    args = parser.parse_args()

    db_cfg = load_json(CONFIG / "database.json", default={})
    db_url = args.db_url.strip() or str(db_cfg.get("url") or "").strip() or "sqlite:///data/job_search.sqlite"

    repo = JobSearchRepository(
        db_url=db_url,
        migrations_dir=DB / "migrations",
        auto_migrate=True,
    )
    repo.initialize()

    applications_raw = load_json(DATA / "applied_jobs.json", default={"applied": []}).get("applied", [])
    applications = [ApplicationRecord.from_applied_dict(x, user_id="default") for x in applications_raw if x.get("url")]
    repo.upsert_applications(applications)

    log_backfilled = 0
    run_log_path = DATA / "pipeline_runs.jsonl"
    if run_log_path.exists():
        for line in run_log_path.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                run_obj = json.loads(line)
                repo.upsert_pipeline_run(PipelineRunRecord.from_run_record(run_obj))
                log_backfilled += 1
            except Exception:
                continue

    ranked_jobs = load_json(DATA / "jobs_normalized.json", default=[])
    report = load_json(OUTPUT / "latest_report.json", default={})

    job_records = [JobRecord.from_job(x) for x in ranked_jobs if isinstance(x, dict)]
    ranking_run_record = _build_run_record_from_report(report, total_jobs=len(ranked_jobs))
    ranking_records = [
        JobRankingRecord.from_ranked_job(ranking_run_record["run_id"], x)
        for x in ranked_jobs
        if isinstance(x, dict)
    ]

    repo.persist_pipeline_snapshot(
        run=PipelineRunRecord.from_run_record(ranking_run_record),
        jobs=job_records,
        rankings=ranking_records,
    )

    print(
        f"Backfill complete. db_url={db_url} | applications={len(applications)} | "
        f"jobs={len(job_records)} | rankings={len(ranking_records)} | runs_from_log={log_backfilled}"
    )


if __name__ == "__main__":
    main()
