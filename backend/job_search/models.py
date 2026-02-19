import hashlib
import json
from dataclasses import dataclass


def _safe_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", errors="ignore")).hexdigest()


@dataclass(frozen=True)
class JobRecord:
    id: str
    source: str
    source_type: str
    title: str
    company: str
    location: str
    remote_hint: int
    url: str
    description: str
    published: str
    fetched_at: str
    normalized_json: str

    @classmethod
    def from_job(cls, job: dict):
        raw_id = str(job.get("id") or "").strip()
        url = str(job.get("url") or "").strip()
        if not raw_id:
            # Keep ID fallback aligned with JobRankingRecord.from_ranked_job to preserve joins.
            raw_id = url or f"generated:{_safe_hash(json.dumps(job, sort_keys=True, ensure_ascii=False))}"
        return cls(
            id=raw_id,
            source=str(job.get("source") or ""),
            source_type=str(job.get("source_type") or ""),
            title=str(job.get("title") or ""),
            company=str(job.get("company") or ""),
            location=str(job.get("location") or ""),
            remote_hint=1 if job.get("remote_hint") else 0,
            url=url,
            description=str(job.get("description") or ""),
            published=str(job.get("published") or ""),
            fetched_at=str(job.get("fetched_at") or ""),
            normalized_json=json.dumps(job, ensure_ascii=False),
        )


@dataclass(frozen=True)
class JobRankingRecord:
    run_id: str
    job_id: str
    score: int
    tier: str
    rule_score: int | None
    reasons_json: str
    skill_hits_json: str
    llm_summary: str
    llm_pros_json: str
    llm_risks_json: str
    scored_by: str

    @classmethod
    def from_ranked_job(cls, run_id: str, job: dict):
        job_id = str(job.get("id") or job.get("url") or "").strip()
        if not job_id:
            job_id = f"generated:{_safe_hash(json.dumps(job, sort_keys=True, ensure_ascii=False))}"

        return cls(
            run_id=run_id,
            job_id=job_id,
            score=int(job.get("score", 0)),
            tier=str(job.get("tier", "C")),
            rule_score=int(job["rule_score"]) if job.get("rule_score") is not None else None,
            reasons_json=json.dumps(job.get("reasons", []), ensure_ascii=False),
            skill_hits_json=json.dumps(job.get("skill_hits", []), ensure_ascii=False),
            llm_summary=str(job.get("llm_summary") or ""),
            llm_pros_json=json.dumps(job.get("llm_pros", []), ensure_ascii=False),
            llm_risks_json=json.dumps(job.get("llm_risks", []), ensure_ascii=False),
            scored_by=str(job.get("scored_by") or "rules"),
        )


@dataclass(frozen=True)
class ApplicationRecord:
    user_id: str
    job_url: str
    title: str
    company: str
    status: str
    applied_at: str
    notes: str
    next_action_at: str = ""
    next_action_type: str = ""

    @classmethod
    def from_applied_dict(cls, item: dict, user_id: str = "default"):
        return cls(
            user_id=user_id,
            job_url=str(item.get("url") or "").strip().lower(),
            title=str(item.get("title") or ""),
            company=str(item.get("company") or ""),
            status=str(item.get("status") or "applied"),
            applied_at=str(item.get("applied_at") or ""),
            notes=str(item.get("notes") or ""),
            next_action_at=str(item.get("next_action_at") or ""),
            next_action_type=str(item.get("next_action_type") or ""),
        )


@dataclass(frozen=True)
class PipelineRunRecord:
    run_id: str
    started_at: str
    ended_at: str
    status: str
    duration_ms: int
    total_jobs: int
    a_tier: int
    b_tier: int
    c_tier: int
    skipped_applied: int
    llm_enabled: int
    llm_model: str | None
    llm_scored_live: int
    llm_cache_hits: int
    llm_failed: int
    source_errors: int
    error_message: str | None
    summary_json: str

    @classmethod
    def from_run_record(cls, run_record: dict):
        return cls(
            run_id=str(run_record.get("run_id") or ""),
            started_at=str(run_record.get("started_at") or ""),
            ended_at=str(run_record.get("ended_at") or ""),
            status=str(run_record.get("status") or "unknown"),
            duration_ms=int(run_record.get("duration_ms", 0)),
            total_jobs=int(run_record.get("total_jobs", 0)),
            a_tier=int(run_record.get("a_tier", 0)),
            b_tier=int(run_record.get("b_tier", 0)),
            c_tier=int(run_record.get("c_tier", 0)),
            skipped_applied=int(run_record.get("skipped_applied", 0)),
            llm_enabled=1 if run_record.get("llm_enabled") else 0,
            llm_model=run_record.get("llm_model"),
            llm_scored_live=int(run_record.get("llm_scored_live", 0)),
            llm_cache_hits=int(run_record.get("llm_cache_hits", 0)),
            llm_failed=int(run_record.get("llm_failed", 0)),
            source_errors=int(run_record.get("source_errors", 0)),
            error_message=(str(run_record.get("error_message")) if run_record.get("error_message") is not None else None),
            summary_json=json.dumps(run_record.get("summary", {}), ensure_ascii=False),
        )


@dataclass(frozen=True)
class SourceFetchEventRecord:
    run_id: str
    source_name: str
    source_kind: str
    source_type: str
    source_url: str
    attempts: int
    success: int
    jobs_fetched: int
    duration_ms: int
    error_message: str | None

    @classmethod
    def from_dict(cls, obj: dict):
        return cls(
            run_id=str(obj.get("run_id") or ""),
            source_name=str(obj.get("source_name") or ""),
            source_kind=str(obj.get("source_kind") or ""),
            source_type=str(obj.get("source_type") or ""),
            source_url=str(obj.get("source_url") or ""),
            attempts=int(obj.get("attempts", 0)),
            success=1 if obj.get("success") else 0,
            jobs_fetched=int(obj.get("jobs_fetched", 0)),
            duration_ms=int(obj.get("duration_ms", 0)),
            error_message=(str(obj.get("error_message")) if obj.get("error_message") is not None else None),
        )


@dataclass(frozen=True)
class FeedbackEventRecord:
    user_id: str
    job_url: str
    action: str
    value: str
    source: str
    created_at: str

    @classmethod
    def from_dict(cls, obj: dict, user_id: str = "default"):
        return cls(
            user_id=user_id,
            job_url=str(obj.get("job_url") or "").strip().lower(),
            action=str(obj.get("action") or "").strip().lower(),
            value=str(obj.get("value") or "").strip(),
            source=str(obj.get("source") or "api").strip(),
            created_at=str(obj.get("created_at") or "").strip(),
        )


@dataclass(frozen=True)
class CoverLetterRecord:
    user_id: str
    job_url: str
    job_id: str
    run_id: str
    cv_variant: str
    language: str
    style: str
    company: str
    title: str
    body: str
    generated_at: str
