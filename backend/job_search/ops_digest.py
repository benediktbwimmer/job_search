from datetime import datetime, timezone

from job_search.json_io import load_json
from job_search.paths import CONFIG, OUTPUT


def _job_key(job: dict) -> str:
    return str(job.get("url") or job.get("job_id") or "").strip().lower()


def build_weekly_digest(repo, user_id: str = "default", top_limit: int = 25) -> str:
    runs = repo.get_recent_runs(limit=2)
    latest_run_id = runs[0]["run_id"] if runs else None
    previous_run_id = runs[1]["run_id"] if len(runs) > 1 else None

    latest_jobs = repo.search_ranked_jobs(run_id=latest_run_id, limit=500, user_id=user_id)["jobs"] if latest_run_id else []
    prev_jobs = repo.search_ranked_jobs(run_id=previous_run_id, limit=500, user_id=user_id)["jobs"] if previous_run_id else []
    prev_keys = {_job_key(x) for x in prev_jobs}

    runtime_cfg = load_json(CONFIG / "runtime.json", default={})
    salary_cfg = runtime_cfg.get("salary_filter", {}) if isinstance(runtime_cfg, dict) else {}
    min_salary = int(salary_cfg.get("min_annual_eur", 0)) if salary_cfg.get("enabled", False) else 0

    new_jobs = [j for j in latest_jobs if _job_key(j) and _job_key(j) not in prev_keys]
    priority_new = []
    for job in new_jobs:
        reasons = [str(x).lower() for x in job.get("reasons", []) if str(x).strip()]
        watchlist_hit = any("company watchlist" in x for x in reasons)
        salary = job.get("salary") if isinstance(job.get("salary"), dict) else {}
        salary_ok = False
        if salary and min_salary > 0:
            annual_min = salary.get("annual_min_eur")
            salary_ok = bool(annual_min is not None and int(annual_min) >= min_salary)
        if watchlist_hit or salary_ok:
            priority_new.append(job)

    metrics = repo.get_application_metrics(user_id=user_id, days=14)
    source_health = repo.get_source_health(window_runs=12, stale_after_hours=72)
    stale_sources = [s for s in source_health if s.get("stale")]

    lines = []
    lines.append(f"# Weekly Ops Digest — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append("")
    lines.append(f"- Latest run: `{latest_run_id or 'n/a'}`")
    lines.append(f"- Previous run: `{previous_run_id or 'n/a'}`")
    lines.append(f"- New jobs since previous run: **{len(new_jobs)}**")
    lines.append(f"- Priority new jobs (watchlist/salary): **{len(priority_new)}**")
    lines.append("")

    lines.append("## Priority New Jobs")
    if not priority_new:
        lines.append("(none)")
    else:
        for job in priority_new[: max(1, int(top_limit))]:
            salary_text = ""
            salary = job.get("salary") if isinstance(job.get("salary"), dict) else {}
            if salary.get("annual_min_eur") is not None:
                salary_text = f" | salary~EUR {int(salary['annual_min_eur']):,}/y+"
            lines.append(
                f"- [{job.get('title')}]({job.get('url')}) "
                f"({job.get('company') or 'unknown'}, score={job.get('score')}, tier={job.get('tier')}{salary_text})"
            )
    lines.append("")

    lines.append("## Funnel Snapshot (14 days)")
    lines.append(f"- Total applications: **{metrics.get('total_applications', 0)}**")
    funnel = metrics.get("funnel", {}) if isinstance(metrics.get("funnel"), dict) else {}
    lines.append(
        f"- Applied+: **{funnel.get('applied_or_beyond', 0)}**, Interview+: **{funnel.get('interview_or_beyond', 0)}**, Offers: **{funnel.get('offers', 0)}**"
    )
    lines.append(
        f"- Rates: apply={funnel.get('apply_rate', 0.0):.2f}, interview={funnel.get('interview_rate', 0.0):.2f}, offer={funnel.get('offer_rate', 0.0):.2f}"
    )
    followups = metrics.get("followups", {}) if isinstance(metrics.get("followups"), dict) else {}
    lines.append(f"- Follow-ups due today: **{followups.get('due_today', 0)}** | overdue: **{followups.get('overdue', 0)}**")
    lines.append("")

    lines.append("## Source Health")
    if not source_health:
        lines.append("(no source health data)")
    else:
        for s in source_health:
            stale = "stale" if s.get("stale") else "fresh"
            lines.append(
                f"- {s.get('source_name')}: score={s.get('health_score')} success_rate={s.get('success_rate')} {stale}"
            )
    if stale_sources:
        lines.append("")
        lines.append("## Alerts")
        for s in stale_sources:
            lines.append(f"- Source appears stale: {s.get('source_name')} (last success: {s.get('last_success_at') or 'never'})")

    return "\n".join(lines) + "\n"


def write_weekly_digest(repo, output_path=OUTPUT / "weekly_digest.md", user_id: str = "default") -> str:
    content = build_weekly_digest(repo=repo, user_id=user_id)
    output_path.write_text(content)
    return str(output_path)
