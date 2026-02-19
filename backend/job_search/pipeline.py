import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import signal
import time
from urllib.error import HTTPError, URLError

from job_search.ingestion import (
    dedupe_jobs,
    enrich_job_detail,
    fetch_indeed_jobs,
    fetch_stepstone_jobs,
    fetch_stepstone_via_browser,
    fetch_url,
    greenhouse_jobs_url,
    lever_jobs_url,
    parse_greenhouse_jobs,
    parse_karriere_html,
    parse_lever_jobs,
    parse_rss,
)
from job_search.json_io import load_json, save_json
from job_search.llm_parsing import (
    llm_parse_cache_keys,
    llm_parse_job,
    load_llm_parse_cache,
    normalize_llm_parse_output,
    save_llm_parse_cache,
)
from job_search.models import (
    ApplicationRecord,
    JobRankingRecord,
    JobRecord,
    PipelineRunRecord,
    SourceFetchEventRecord,
)
from job_search.paths import CONFIG, DATA, DB, OUTPUT
from job_search.observability import emit_alert, emit_metric, log_event, write_runtime_metrics_snapshot
from job_search.reporting import markdown_report
from job_search.run_metadata import persist_run_metadata
from job_search.storage.repository import JobSearchRepository


def _build_repository(db_cfg: dict):
    if not db_cfg.get("enabled", False):
        return None

    db_url = str(db_cfg.get("url") or "").strip()
    if not db_url:
        raise RuntimeError("database is enabled but url is missing")

    repo = JobSearchRepository(
        db_url=db_url,
        migrations_dir=DB / "migrations",
        auto_migrate=bool(db_cfg.get("auto_migrate", False)),
    )
    repo.initialize()
    return repo


def _load_applied_urls(db_repo, applied_path):
    applied = load_json(applied_path, default={"applied": []}).get("applied", [])
    fallback_urls = {x.get("url", "").strip().lower() for x in applied if x.get("url")}

    if db_repo is None:
        return fallback_urls

    db_urls = set(db_repo.list_applied_urls(user_id="default"))
    if db_urls:
        return db_urls

    if applied:
        db_repo.upsert_applications(
            [ApplicationRecord.from_applied_dict(x, user_id="default") for x in applied if x.get("url")]
        )
    return fallback_urls


def _fetch_with_retry(fetch_fn, max_retries: int, backoff_seconds: float):
    attempts = 0
    last_error = None
    for attempt in range(max_retries + 1):
        attempts += 1
        try:
            return fetch_fn(), attempts
        except Exception as e:
            last_error = e
            if attempt < max_retries:
                if backoff_seconds > 0:
                    time.sleep(backoff_seconds * (2**attempt))
                continue
            try:
                setattr(last_error, "_attempts", attempts)
            except Exception:
                pass
            raise last_error


def _call_with_hard_timeout(timeout_sec: int, fn, *args, **kwargs):
    timeout = max(0, int(timeout_sec))
    if timeout <= 0 or not hasattr(signal, "SIGALRM"):
        return fn(*args, **kwargs)

    def _on_timeout(signum, frame):
        raise TimeoutError(f"operation timed out after {timeout}s")

    previous_handler = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, _on_timeout)
    signal.setitimer(signal.ITIMER_REAL, float(timeout))
    try:
        return fn(*args, **kwargs)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0.0)
        signal.signal(signal.SIGALRM, previous_handler)


def _save_llm_cache_snapshot(path, entries: dict, model: str, prompt_version: str):
    save_llm_parse_cache(
        path,
        {
            "meta": {
                "version": 1,
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "model": model,
                "prompt_version": prompt_version,
            },
            "entries": entries,
        },
    )


def _fetch_source_jobs(source: dict, source_kind: str):
    if source_kind == "rss":
        xml = fetch_url(source["url"])
        return parse_rss(xml, source["name"], source.get("type", "unknown"))
    if source_kind == "html":
        html = fetch_url(source["url"])
        return parse_karriere_html(html, source["name"], source.get("type", "unknown"))
    if source_kind == "browser":
        return fetch_stepstone_via_browser(
            source_name=source["name"],
            source_type=source.get("type", "unknown"),
            base_url=source["url"],
            pages=int(source.get("pages", 1)),
        )
    if source_kind == "stepstone":
        return fetch_stepstone_jobs(
            source_name=source["name"],
            source_type=source.get("type", "unknown"),
            base_url=source["url"],
            pages=int(source.get("pages", 1)),
            source_cfg=source,
        )
    if source_kind == "indeed":
        return fetch_indeed_jobs(
            source_name=source["name"],
            source_type=source.get("type", "unknown"),
            base_url=source["url"],
            pages=int(source.get("pages", 1)),
            source_cfg=source,
        )
    if source_kind == "greenhouse":
        url = str(source.get("url") or "").strip()
        if not url:
            url = greenhouse_jobs_url(source.get("board") or source.get("company") or source.get("token"))
            source["url"] = url
        raw = fetch_url(url)
        return parse_greenhouse_jobs(
            raw,
            source_name=source["name"],
            source_type=source.get("type", "remote"),
            company_hint=str(source.get("company_name") or ""),
        )
    if source_kind == "lever":
        url = str(source.get("url") or "").strip()
        if not url:
            url = lever_jobs_url(source.get("company") or source.get("team") or source.get("token"))
            source["url"] = url
        raw = fetch_url(url)
        return parse_lever_jobs(
            raw,
            source_name=source["name"],
            source_type=source.get("type", "remote"),
            company_hint=str(source.get("company_name") or ""),
        )
    raise ValueError(f"unknown source kind: {source_kind}")


def run_pipeline() -> dict:
    run_id = str(uuid.uuid4())
    started_at_dt = datetime.now(timezone.utc)

    summary = None
    errors = []
    enriched = []
    ranked = []
    source_events = []
    skipped_applied = 0
    llm_enabled = False
    llm_model = None
    llm_scored_count = 0
    llm_cache_hits = 0
    llm_failed_count = 0
    runtime_error = None
    runtime_cfg = {}
    operations_cfg = {}
    db_cfg = {}
    db_repo = None
    llm_filtered_invalid = 0
    llm_overflow_skipped = 0
    alerts = []

    try:
        log_event("pipeline_run_started", run_id=run_id)
        emit_metric("pipeline_run_started", tags={"run_id": run_id})
        profile = load_json(CONFIG / "profile.json")
        sources = load_json(CONFIG / "sources.json")
        constraints = load_json(CONFIG / "constraints.json", default={})
        scoring_cfg = load_json(CONFIG / "scoring.json", default={})
        runtime_cfg = load_json(CONFIG / "runtime.json", default={})
        operations_cfg = runtime_cfg.get("operations", {}) if isinstance(runtime_cfg, dict) else {}
        db_cfg = load_json(CONFIG / "database.json", default={})
        db_repo = _build_repository(db_cfg)
        source_retry_cfg = runtime_cfg.get("source_fetch", {}) if isinstance(runtime_cfg, dict) else {}
        max_retries = max(0, int(source_retry_cfg.get("max_retries", 0)))
        backoff_seconds = max(0.0, float(source_retry_cfg.get("backoff_seconds", 0.0)))
        source_health_cfg = runtime_cfg.get("source_health", {}) if isinstance(runtime_cfg, dict) else {}
        source_health_enabled = bool(source_health_cfg.get("enabled", False))
        source_health_window = max(1, int(source_health_cfg.get("window_runs", 12)))
        source_stale_after_hours = max(1, int(source_health_cfg.get("stale_after_hours", 72)))
        source_degraded_threshold = max(0, min(100, int(source_health_cfg.get("degraded_score_threshold", 25))))
        source_min_events_for_skip = max(1, int(source_health_cfg.get("min_events_for_skip", 4)))

        source_health_map = {}
        if db_repo and source_health_enabled:
            for h in db_repo.get_source_health(window_runs=source_health_window, stale_after_hours=source_stale_after_hours):
                source_health_map[str(h.get("source_name") or "")] = h

        applied_urls = _load_applied_urls(db_repo, DATA / "applied_jobs.json")
        fetched = []

        source_specs = []
        source_specs.extend([("rss", s) for s in sources.get("rss_sources", [])])
        source_specs.extend([("html", s) for s in sources.get("html_sources", [])])
        source_specs.extend([("stepstone", s) for s in sources.get("stepstone_sources", [])])
        source_specs.extend([("indeed", s) for s in sources.get("indeed_sources", [])])
        source_specs.extend([("browser", s) for s in sources.get("browser_sources", [])])
        source_specs.extend([("greenhouse", s) for s in sources.get("greenhouse_sources", [])])
        source_specs.extend([("lever", s) for s in sources.get("lever_sources", [])])

        for source_kind, s in source_specs:
            if s.get("enabled", True) is False:
                continue

            started = time.monotonic()
            attempts = 0
            jobs = []
            error_text = None
            success = False
            source_health = source_health_map.get(s.get("name", ""))
            local_max_retries = max_retries
            if source_health:
                if (
                    int(source_health.get("total_events", 0)) >= source_min_events_for_skip
                    and int(source_health.get("health_score", 0)) <= source_degraded_threshold
                ):
                    error_text = "skipped by source health policy"
                    alerts.append(f"source {s.get('name')} skipped due to low health score")
                    source_events.append(
                        {
                            "run_id": run_id,
                            "source_name": s.get("name", ""),
                            "source_kind": source_kind,
                            "source_type": s.get("type", "unknown"),
                            "source_url": s.get("url", ""),
                            "attempts": 0,
                            "success": False,
                            "jobs_fetched": 0,
                            "duration_ms": int((time.monotonic() - started) * 1000),
                            "error_message": error_text,
                        }
                    )
                    continue

                # Tune retries down for historically flaky sources so one feed does not dominate run time.
                if float(source_health.get("success_rate", 1.0)) < 0.35:
                    local_max_retries = min(local_max_retries, 1)
                if bool(source_health.get("stale", False)):
                    alerts.append(f"source {s.get('name')} appears stale")
            try:
                jobs, attempts = _fetch_with_retry(
                    lambda: _fetch_source_jobs(s, source_kind),
                    max_retries=local_max_retries,
                    backoff_seconds=backoff_seconds,
                )
                fetched.extend(jobs)
                success = True
            except (HTTPError, URLError, TimeoutError, ValueError) as e:
                attempts = int(getattr(e, "_attempts", attempts or 1))
                error_text = str(e)
                errors.append({"source": s.get("name"), "url": s.get("url"), "error": error_text})
            except Exception as e:
                attempts = int(getattr(e, "_attempts", attempts or 1))
                if source_kind == "browser":
                    error_text = f"browser source failed: {e}"
                else:
                    error_text = f"unexpected: {e}"
                errors.append({"source": s.get("name"), "url": s.get("url"), "error": error_text})
            finally:
                source_events.append(
                    {
                        "run_id": run_id,
                        "source_name": s.get("name", ""),
                        "source_kind": source_kind,
                        "source_type": s.get("type", "unknown"),
                        "source_url": s.get("url", ""),
                        "attempts": attempts,
                        "success": success,
                        "jobs_fetched": len(jobs),
                        "duration_ms": int((time.monotonic() - started) * 1000),
                        "error_message": error_text,
                    }
                )

        fetched = dedupe_jobs(fetched)
        enriched = [enrich_job_detail(j) for j in fetched]
        llm_cfg = scoring_cfg.get("llm_pipeline")
        if not isinstance(llm_cfg, dict):
            raise RuntimeError("config/scoring.json must define llm_pipeline settings")

        llm_enabled = bool(llm_cfg.get("enabled", True))
        llm_model = str(llm_cfg.get("model") or "gpt-5-mini")
        llm_max_jobs = max(1, int(llm_cfg.get("max_jobs_per_run", 300)))
        llm_drop_invalid = bool(llm_cfg.get("drop_invalid", True))
        prompt_version = str(llm_cfg.get("prompt_version", "v2"))
        llm_no_description_truncation = bool(llm_cfg.get("no_description_truncation", False))
        raw_description_limit = int(llm_cfg.get("description_max_chars", 2500))
        if llm_no_description_truncation or raw_description_limit <= 0:
            llm_description_max_chars = 0
        else:
            llm_description_max_chars = max(400, min(120000, raw_description_limit))
        raw_model_input_limit = int(
            llm_cfg.get("model_input_description_max_chars", 0 if llm_no_description_truncation else 20000)
        )
        if raw_model_input_limit <= 0:
            llm_input_description_chars = 0
        else:
            llm_input_description_chars = max(2000, min(120000, raw_model_input_limit))
        llm_job_timeout_sec = max(10, int(llm_cfg.get("per_job_timeout_sec", 75)))
        llm_progress_every = max(1, int(llm_cfg.get("progress_every", 10)))
        llm_parallel_initial = max(
            1,
            min(
                120,
                int(llm_cfg.get("parallel_workers_initial", llm_cfg.get("parallel_workers", 12))),
            ),
        )
        llm_parallel_min = max(1, min(llm_parallel_initial, int(llm_cfg.get("parallel_workers_min", 6))))
        llm_parallel_max = max(
            llm_parallel_initial,
            min(120, int(llm_cfg.get("parallel_workers_max", max(32, llm_parallel_initial)))),
        )
        llm_parallel_round_multiplier = max(1, min(6, int(llm_cfg.get("parallel_round_multiplier", 2))))

        if not llm_enabled:
            raise RuntimeError("llm_pipeline must be enabled for the current prototype pipeline")

        llm_cache_path = DATA / "llm_parse_cache.json"
        llm_cache = load_llm_parse_cache(llm_cache_path)
        llm_cache_entries = llm_cache.get("entries", {})

        candidates = []
        for j in enriched:
            u = (j.get("url") or "").strip().lower()
            if u and u in applied_urls:
                skipped_applied += 1
                continue
            candidates.append(j)
        candidate_target = min(len(candidates), llm_max_jobs)
        print(
            f"LLM evaluation started: {candidate_target} jobs (timeout/job={llm_job_timeout_sec}s, cache={len(llm_cache_entries)})"
        )

        completed_count = 0
        live_jobs: list[tuple[dict, list[str]]] = []

        def _emit_progress():
            if completed_count % llm_progress_every != 0 and completed_count != candidate_target:
                return
            _save_llm_cache_snapshot(
                path=llm_cache_path,
                entries=llm_cache_entries,
                model=llm_model,
                prompt_version=prompt_version,
            )
            print(
                f"LLM progress {completed_count}/{candidate_target} "
                f"(live={llm_scored_count}, cache={llm_cache_hits}, failed={llm_failed_count}, filtered={llm_filtered_invalid})"
            )

        def _ingest_llm_out(job: dict, llm_out: dict, scored_by: str):
            nonlocal llm_filtered_invalid
            is_job_posting = bool(llm_out.get("is_job_posting", True))
            if not is_job_posting and llm_drop_invalid:
                llm_filtered_invalid += 1
                return

            normalized = normalize_llm_parse_output(
                job=job,
                llm_out=llm_out,
                description_max_chars=llm_description_max_chars,
            )
            row = dict(job)
            for field in ("title", "company", "location", "description", "published"):
                value = str(normalized.get(field) or "").strip()
                if value:
                    row[field] = value

            row["remote_hint"] = bool(normalized.get("remote_hint", row.get("remote_hint", False)))
            row["score"] = max(0, min(100, int(normalized.get("score", 0))))
            row["tier"] = "A" if row["score"] >= 70 else ("B" if row["score"] >= 50 else "C")
            row["reasons"] = [str(x)[:120] for x in (normalized.get("reasons") or [])[:8]]
            row["llm_summary"] = str(normalized.get("summary") or "")[:180]
            row["quality_flags"] = [str(x)[:80] for x in (normalized.get("quality_flags") or [])[:8]]
            try:
                row["parse_confidence"] = float(normalized.get("confidence", 0.0))
            except (TypeError, ValueError):
                row["parse_confidence"] = 0.0
            row["scored_by"] = scored_by
            ranked.append(row)

        for idx, job in enumerate(candidates):
            if idx >= llm_max_jobs:
                llm_overflow_skipped += 1
                continue

            ckeys = llm_parse_cache_keys(
                job=job,
                model=llm_model,
                prompt_version=prompt_version,
                description_chars=llm_input_description_chars,
            )
            cached_key = next((k for k in ckeys if k in llm_cache_entries), None)
            cached = llm_cache_entries.get(cached_key) if cached_key else None
            if cached and isinstance(cached, dict):
                llm_cache_hits += 1
                _ingest_llm_out(job=job, llm_out=cached, scored_by=f"llm:{llm_model}:cache")
                completed_count += 1
                _emit_progress()
                continue

            live_jobs.append((job, ckeys))

        def _process_live_result(job: dict, ckeys: list[str], llm_out: dict):
            nonlocal llm_scored_count
            llm_scored_count += 1
            llm_cache_entries[ckeys[0]] = {
                **llm_out,
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "model": llm_model,
                "prompt_version": prompt_version,
            }
            _ingest_llm_out(job=job, llm_out=llm_out, scored_by=f"llm:{llm_model}:live")

        if llm_parallel_initial <= 1:
            for job, ckeys in live_jobs:
                try:
                    llm_out = _call_with_hard_timeout(
                        llm_job_timeout_sec,
                        llm_parse_job,
                        job=job,
                        profile=profile,
                        constraints=constraints,
                        model=llm_model,
                        description_max_chars=llm_description_max_chars,
                        input_description_max_chars=llm_input_description_chars,
                    )
                    _process_live_result(job=job, ckeys=ckeys, llm_out=llm_out)
                except Exception as e:
                    llm_failed_count += 1
                    errors.append(
                        {
                            "source": str(job.get("source") or ""),
                            "url": str(job.get("url") or ""),
                            "error": f"llm_evaluation_failed: {str(e)[:220]}",
                        }
                    )
                finally:
                    completed_count += 1
                    _emit_progress()
        else:
            worker_count = llm_parallel_initial
            cursor = 0
            live_total = len(live_jobs)
            print(
                "LLM adaptive concurrency enabled: "
                f"initial={llm_parallel_initial}, min={llm_parallel_min}, max={llm_parallel_max}"
            )
            while cursor < live_total:
                batch_limit = max(worker_count, worker_count * llm_parallel_round_multiplier)
                batch = live_jobs[cursor : cursor + batch_limit]
                cursor += len(batch)
                if not batch:
                    break

                round_success = 0
                round_failures = 0
                round_rate_limited = 0
                with ThreadPoolExecutor(max_workers=worker_count) as executor:
                    futures = {
                        executor.submit(
                            llm_parse_job,
                            job=job,
                            profile=profile,
                            constraints=constraints,
                            model=llm_model,
                            description_max_chars=llm_description_max_chars,
                            input_description_max_chars=llm_input_description_chars,
                        ): (job, ckeys)
                        for job, ckeys in batch
                    }

                    for future in as_completed(futures):
                        job, ckeys = futures[future]
                        try:
                            llm_out = future.result()
                            _process_live_result(job=job, ckeys=ckeys, llm_out=llm_out)
                            round_success += 1
                        except Exception as e:
                            round_failures += 1
                            error_text = str(e)[:220]
                            if "429" in error_text or "rate" in error_text.lower():
                                round_rate_limited += 1
                            llm_failed_count += 1
                            errors.append(
                                {
                                    "source": str(job.get("source") or ""),
                                    "url": str(job.get("url") or ""),
                                    "error": f"llm_evaluation_failed: {error_text}",
                                }
                            )
                        finally:
                            completed_count += 1
                            _emit_progress()

                if round_rate_limited > 0:
                    new_worker_count = max(llm_parallel_min, worker_count // 2)
                    if new_worker_count != worker_count:
                        print(
                            "LLM concurrency backoff: "
                            f"{worker_count} -> {new_worker_count} (rate_limited={round_rate_limited})"
                        )
                        worker_count = new_worker_count
                    continue

                if round_failures == 0 and round_success > 0 and worker_count < llm_parallel_max:
                    grow_by = max(1, worker_count // 3)
                    new_worker_count = min(llm_parallel_max, worker_count + grow_by)
                    if new_worker_count != worker_count:
                        print(
                            "LLM concurrency scale-up: "
                            f"{worker_count} -> {new_worker_count} (round_success={round_success})"
                        )
                        worker_count = new_worker_count

        _save_llm_cache_snapshot(
            path=llm_cache_path,
            entries=llm_cache_entries,
            model=llm_model,
            prompt_version=prompt_version,
        )

        ranked.sort(key=lambda x: x["score"], reverse=True)

        save_json(DATA / "jobs_normalized.json", ranked)
        save_json(DATA / "last_errors.json", errors)

        report_md = markdown_report(ranked, skipped_applied, errors)
        (OUTPUT / "latest_report.md").write_text(report_md)

        summary = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total": len(ranked),
            "tiers": {
                "A": len([x for x in ranked if x["tier"] == "A"]),
                "B": len([x for x in ranked if x["tier"] == "B"]),
                "C": len([x for x in ranked if x["tier"] == "C"]),
            },
            "skipped_applied": skipped_applied,
            "llm": {
                "enabled": llm_enabled,
                "model": llm_model,
                "scored_live": llm_scored_count,
                "cache_hits": llm_cache_hits,
                "failed": llm_failed_count,
                "filtered_invalid": llm_filtered_invalid,
                "overflow_skipped": llm_overflow_skipped,
            },
            "top": ranked[:25],
            "errors": errors,
        }
        if alerts:
            summary["alerts"] = alerts
        save_json(OUTPUT / "latest_report.json", summary)
        write_runtime_metrics_snapshot(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "run_id": run_id,
                "jobs_total": summary["total"],
                "tiers": summary["tiers"],
                "source_errors": len(errors),
                "llm_filtered_invalid": llm_filtered_invalid,
                "llm_overflow_skipped": llm_overflow_skipped,
                "alerts": alerts,
            }
        )
        if db_repo and source_health_enabled:
            current_health = db_repo.get_source_health(
                window_runs=source_health_window,
                stale_after_hours=source_stale_after_hours,
            )
            save_json(DATA / "source_health.json", {"generated_at": datetime.now(timezone.utc).isoformat(), "sources": current_health})

        emit_metric("pipeline_run_success", tags={"run_id": run_id})
        emit_metric("pipeline_source_errors", value=len(errors), tags={"run_id": run_id})
        log_event("pipeline_run_completed", run_id=run_id, status="success", total_jobs=summary["total"], source_errors=len(errors))

        print(
            f"Done. Jobs: {summary['total']} | A: {summary['tiers']['A']} | "
            f"B: {summary['tiers']['B']} | skipped_applied: {skipped_applied} | "
            f"llm_live: {llm_scored_count} | llm_cache: {llm_cache_hits} | "
            f"llm_failed: {llm_failed_count} | llm_filtered_invalid: {llm_filtered_invalid} | "
            f"llm_overflow_skipped: {llm_overflow_skipped} | errors: {len(errors)}"
        )

        return summary

    except BaseException as e:
        runtime_error = e
        emit_metric("pipeline_run_failed", tags={"run_id": run_id})
        log_event("pipeline_run_failed", level="error", run_id=run_id, message=str(e)[:240])
        raise

    finally:
        ended_at_dt = datetime.now(timezone.utc)
        duration_ms = int((ended_at_dt - started_at_dt).total_seconds() * 1000)

        run_record = {
            "run_id": run_id,
            "started_at": started_at_dt.isoformat(),
            "ended_at": ended_at_dt.isoformat(),
            "duration_ms": duration_ms,
            "status": "failed" if runtime_error else "success",
            "total_jobs": int(summary.get("total", 0)) if summary else 0,
            "a_tier": int(summary.get("tiers", {}).get("A", 0)) if summary else 0,
            "b_tier": int(summary.get("tiers", {}).get("B", 0)) if summary else 0,
            "c_tier": int(summary.get("tiers", {}).get("C", 0)) if summary else 0,
            "skipped_applied": skipped_applied,
            "llm_enabled": llm_enabled,
            "llm_model": llm_model,
            "llm_scored_live": llm_scored_count,
            "llm_cache_hits": llm_cache_hits,
            "llm_failed": llm_failed_count,
            "source_errors": len(errors),
            "error_message": str(runtime_error)[:400] if runtime_error else None,
            "summary": summary or {},
        }

        notices = persist_run_metadata(
            run_record=run_record,
            run_log_path=DATA / "pipeline_runs.jsonl",
            db_config=db_cfg,
            migrations_dir=DB / "migrations",
        )
        for n in notices:
            print(f"Metadata notice: {n}")

        if runtime_error is not None:
            alerts_cfg = operations_cfg.get("alerts", {}) if isinstance(operations_cfg, dict) else {}
            alert_enabled = bool(alerts_cfg.get("enabled", True))
            if alert_enabled:
                emit_alert(
                    kind="pipeline_failure",
                    message=f"pipeline run failed: {runtime_error}",
                    severity="error",
                    details={"run_id": run_id, "source_errors": len(errors)},
                    webhook_url=str(alerts_cfg.get("failure_webhook_url") or "").strip(),
                )

        if db_repo:
            run_model = PipelineRunRecord.from_run_record(run_record)

            def _job_identity(job: dict) -> tuple[str, str]:
                job_id = str(job.get("id") or "").strip()
                if job_id:
                    return ("id", job_id)
                job_url = str(job.get("url") or "").strip()
                if job_url:
                    return ("url", job_url)
                return ("raw", str(job))

            jobs_by_identity: dict[tuple[str, str], dict] = {}
            for raw_job in enriched:
                jobs_by_identity[_job_identity(raw_job)] = dict(raw_job)
            # Overlay LLM-cleaned rows so core search fields in jobs table stay normalized.
            for ranked_job in ranked:
                key = _job_identity(ranked_job)
                merged = dict(jobs_by_identity.get(key, {}))
                merged.update(ranked_job)
                jobs_by_identity[key] = merged

            jobs_for_db = [JobRecord.from_job(j) for j in jobs_by_identity.values()]
            rankings_for_db = [JobRankingRecord.from_ranked_job(run_id, j) for j in ranked]
            source_events_for_db = [SourceFetchEventRecord.from_dict(x) for x in source_events]
            try:
                db_repo.persist_pipeline_snapshot(
                    run=run_model,
                    jobs=jobs_for_db,
                    rankings=rankings_for_db,
                    source_events=source_events_for_db,
                )
            except Exception as e:
                if runtime_error is not None:
                    print(f"Metadata notice: database persistence failed after pipeline failure: {e}")
                else:
                    raise
