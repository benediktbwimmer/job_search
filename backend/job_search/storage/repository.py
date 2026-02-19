import json
import sqlite3
import re
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

from job_search.models import (
    ApplicationRecord,
    CoverLetterRecord,
    FeedbackEventRecord,
    JobRankingRecord,
    JobRecord,
    PipelineRunRecord,
    SourceFetchEventRecord,
)
from job_search.storage.db import apply_migrations, connect_sqlite


class JobSearchRepository:
    def __init__(self, db_url: str, migrations_dir: Path, auto_migrate: bool = False):
        self.db_url = db_url
        self.migrations_dir = migrations_dir
        self.auto_migrate = auto_migrate

    def initialize(self):
        if self.auto_migrate:
            apply_migrations(db_url=self.db_url, migrations_dir=self.migrations_dir)

    def list_applied_urls(self, user_id: str = "default") -> list[str]:
        conn = connect_sqlite(self.db_url)
        try:
            rows = conn.execute(
                """
                SELECT LOWER(job_url) AS job_url
                FROM applications
                WHERE user_id = ?
                  AND status NOT IN ('rejected', 'withdrawn')
                """,
                (user_id,),
            ).fetchall()
            return [str(row["job_url"]) for row in rows if row["job_url"]]
        finally:
            conn.close()

    def get_recent_runs(self, limit: int = 10) -> list[dict]:
        conn = connect_sqlite(self.db_url)
        try:
            rows = conn.execute(
                """
                SELECT run_id, started_at, ended_at, status, duration_ms,
                       total_jobs, a_tier, b_tier, c_tier, skipped_applied,
                       llm_enabled, llm_model, llm_scored_live, llm_cache_hits, llm_failed,
                       source_errors
                FROM pipeline_runs
                ORDER BY started_at DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def get_run(self, run_id: str) -> dict | None:
        run_id = str(run_id or "").strip()
        if not run_id:
            return None
        conn = connect_sqlite(self.db_url)
        try:
            row = conn.execute(
                """
                SELECT run_id, started_at, ended_at, status, duration_ms,
                       total_jobs, a_tier, b_tier, c_tier, skipped_applied,
                       llm_enabled, llm_model, llm_scored_live, llm_cache_hits, llm_failed,
                       source_errors, error_message, summary_json
                FROM pipeline_runs
                WHERE run_id = ?
                LIMIT 1
                """,
                (run_id,),
            ).fetchone()
            if not row:
                return None
            item = dict(row)
            try:
                summary = json.loads(str(item.get("summary_json") or "{}"))
            except Exception:
                summary = {}
            item["summary"] = summary if isinstance(summary, dict) else {}
            return item
        finally:
            conn.close()

    def get_latest_run_id(self) -> str | None:
        conn = connect_sqlite(self.db_url)
        try:
            row = conn.execute(
                """
                SELECT run_id
                FROM pipeline_runs
                ORDER BY started_at DESC
                LIMIT 1
                """
            ).fetchone()
            if not row:
                return None
            return str(row["run_id"])
        finally:
            conn.close()

    def get_run_source_events(self, run_id: str) -> list[dict]:
        conn = connect_sqlite(self.db_url)
        try:
            rows = conn.execute(
                """
                SELECT source_name, source_kind, source_type, source_url,
                       attempts, success, jobs_fetched, duration_ms, error_message, created_at
                FROM source_fetch_events
                WHERE run_id = ?
                ORDER BY source_name ASC
                """,
                (run_id,),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    @staticmethod
    def _parse_json_array(raw: str | None) -> list:
        if not raw:
            return []
        try:
            value = json.loads(raw)
        except Exception:
            return []
        return value if isinstance(value, list) else []

    @staticmethod
    def _parse_json_object(raw: str | None) -> dict:
        if not raw:
            return {}
        try:
            value = json.loads(raw)
        except Exception:
            return {}
        return value if isinstance(value, dict) else {}

    @staticmethod
    def _parse_sort_datetime(raw: str | None) -> datetime | None:
        value = str(raw or "").strip()
        if not value:
            return None
        lowered = value.lower()
        now = datetime.now(timezone.utc)

        if lowered in {"today", "heute"}:
            return now
        if lowered in {"yesterday", "gestern"}:
            return now.replace(microsecond=0) - timedelta(days=1)

        relative_match = re.match(r"^\s*(\d+)\s*([smhdw])\s*ago\s*$", lowered)
        if relative_match:
            qty = int(relative_match.group(1))
            unit = relative_match.group(2)
            delta = {
                "s": timedelta(seconds=qty),
                "m": timedelta(minutes=qty),
                "h": timedelta(hours=qty),
                "d": timedelta(days=qty),
                "w": timedelta(weeks=qty),
            }.get(unit, timedelta())
            return now - delta

        german_relative = re.match(r"^\s*vor\s+(\d+)\s+(tag|tagen|stunde|stunden|woche|wochen)\s*$", lowered)
        if german_relative:
            qty = int(german_relative.group(1))
            unit = german_relative.group(2)
            if unit in {"tag", "tagen"}:
                return now - timedelta(days=qty)
            if unit in {"stunde", "stunden"}:
                return now - timedelta(hours=qty)
            if unit in {"woche", "wochen"}:
                return now - timedelta(weeks=qty)

        try:
            iso_candidate = value.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(iso_candidate)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except Exception:
            pass

        try:
            parsed = parsedate_to_datetime(value)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except Exception:
            pass

        return None

    @classmethod
    def _sortable_timestamp(cls, published: str | None, fetched_at: str | None) -> float:
        published_dt = cls._parse_sort_datetime(published)
        if published_dt is not None:
            return published_dt.timestamp()
        fetched_dt = cls._parse_sort_datetime(fetched_at)
        if fetched_dt is not None:
            return fetched_dt.timestamp()
        return float("-inf")

    def _hydrate_ranked_job(self, row: sqlite3.Row, include_diagnostics: bool = False) -> dict:
        item = dict(row)
        reasons = self._parse_json_array(item.get("reasons_json"))
        skill_hits = self._parse_json_array(item.get("skill_hits_json"))
        llm_pros = self._parse_json_array(item.get("llm_pros_json"))
        llm_risks = self._parse_json_array(item.get("llm_risks_json"))
        normalized = self._parse_json_object(item.get("normalized_json"))

        adaptive_bonus = 0
        try:
            adaptive_bonus = int(normalized.get("adaptive_bonus") or 0)
        except (TypeError, ValueError):
            adaptive_bonus = 0
        adaptive_reasons = normalized.get("adaptive_reasons")
        if not isinstance(adaptive_reasons, list):
            adaptive_reasons = []

        final_score = int(item.get("score", 0))
        rule_score_raw = item.get("rule_score")
        rule_score = int(rule_score_raw) if rule_score_raw is not None else None
        base_score = (rule_score if rule_score is not None else final_score) - adaptive_bonus
        base_score = max(0, min(100, int(base_score)))

        hydrated = {
            "run_id": item.get("run_id"),
            "job_id": item.get("job_id"),
            "score": final_score,
            "tier": item.get("tier"),
            "rule_score": rule_score,
            "scored_by": item.get("scored_by"),
            "source": item.get("source"),
            "source_type": item.get("source_type"),
            "title": item.get("title"),
            "company": item.get("company"),
            "location": item.get("location"),
            "url": item.get("url"),
            "description": item.get("description"),
            "published": item.get("published"),
            "fetched_at": item.get("fetched_at"),
            "remote_hint": int(item.get("remote_hint") or 0),
            "application_status": item.get("application_status"),
            "reasons": reasons,
            "skill_hits": skill_hits,
            "llm_summary": item.get("llm_summary") or "",
            "llm_pros": llm_pros,
            "llm_risks": llm_risks,
            "salary": (normalized.get("salary") if isinstance(normalized.get("salary"), dict) else {}),
            "cv_variant": str(normalized.get("cv_variant") or "").strip(),
            "cv_recommendation_reasons": (
                normalized.get("cv_recommendation_reasons")
                if isinstance(normalized.get("cv_recommendation_reasons"), list)
                else []
            ),
        }

        if include_diagnostics:
            hydrated["diagnostics"] = {
                "base_score": base_score,
                "adaptive_bonus": adaptive_bonus,
                "adaptive_reasons": adaptive_reasons,
                "final_score": final_score,
                "rule_score": rule_score,
            }

        return hydrated

    def search_ranked_jobs(
        self,
        limit: int = 20,
        offset: int = 0,
        tier: str | None = None,
        run_id: str | None = None,
        query_text: str | None = None,
        company: str | None = None,
        source: str | None = None,
        source_type: str | None = None,
        location: str | None = None,
        remote: bool | None = None,
        min_score: int | None = None,
        max_score: int | None = None,
        application_status: str | None = None,
        sort: str = "score_desc",
        include_diagnostics: bool = False,
        user_id: str = "default",
    ) -> dict:
        resolved_run_id = run_id or self.get_latest_run_id()
        if not resolved_run_id:
            return {
                "run_id": None,
                "jobs": [],
                "limit": max(1, int(limit)),
                "offset": max(0, int(offset)),
                "total": 0,
                "has_more": False,
            }

        sort_key = (sort or "score_desc").strip().lower()
        order_by = {
            "score_desc": "jr.score DESC, jr.id ASC",
            "score_asc": "jr.score ASC, jr.id ASC",
            "company": "LOWER(COALESCE(j.company, '')) ASC, jr.score DESC, jr.id ASC",
            "title": "LOWER(COALESCE(j.title, '')) ASC, jr.score DESC, jr.id ASC",
        }.get(sort_key, "jr.score DESC, jr.id ASC")
        sort_in_python = sort_key in {"newest", "oldest"}

        base_from = """
            FROM job_rankings jr
            LEFT JOIN jobs j ON j.id = jr.job_id
            LEFT JOIN applications a
              ON a.user_id = ?
             AND LOWER(a.job_url) = LOWER(j.url)
        """
        where_clauses = ["jr.run_id = ?"]
        where_params = [resolved_run_id]

        if tier:
            where_clauses.append("jr.tier = ?")
            where_params.append(str(tier).upper())
        if query_text:
            needle = f"%{str(query_text).strip().lower()}%"
            where_clauses.append(
                "(LOWER(COALESCE(j.title, '')) LIKE ? "
                "OR LOWER(COALESCE(j.company, '')) LIKE ? "
                "OR LOWER(COALESCE(j.location, '')) LIKE ? "
                "OR LOWER(COALESCE(j.description, '')) LIKE ?)"
            )
            where_params.extend([needle, needle, needle, needle])
        if company:
            where_clauses.append("LOWER(COALESCE(j.company, '')) LIKE ?")
            where_params.append(f"%{str(company).strip().lower()}%")
        if source:
            where_clauses.append("LOWER(COALESCE(j.source, '')) = ?")
            where_params.append(str(source).strip().lower())
        if source_type:
            where_clauses.append("LOWER(COALESCE(j.source_type, '')) = ?")
            where_params.append(str(source_type).strip().lower())
        if location:
            where_clauses.append("LOWER(COALESCE(j.location, '')) LIKE ?")
            where_params.append(f"%{str(location).strip().lower()}%")
        if remote is True:
            where_clauses.append("COALESCE(j.remote_hint, 0) = 1")
        elif remote is False:
            where_clauses.append("COALESCE(j.remote_hint, 0) = 0")
        if min_score is not None:
            where_clauses.append("jr.score >= ?")
            where_params.append(int(min_score))
        if max_score is not None:
            where_clauses.append("jr.score <= ?")
            where_params.append(int(max_score))
        if application_status:
            where_clauses.append("LOWER(COALESCE(a.status, '')) = ?")
            where_params.append(str(application_status).strip().lower())

        where_sql = " AND ".join(where_clauses)
        limit_value = max(1, int(limit))
        offset_value = max(0, int(offset))

        conn = connect_sqlite(self.db_url)
        try:
            count_params = [user_id, *where_params]
            total_row = conn.execute(
                f"""
                SELECT COUNT(*) AS total
                {base_from}
                WHERE {where_sql}
                """,
                tuple(count_params),
            ).fetchone()
            total = int(total_row["total"]) if total_row else 0

            if sort_in_python:
                select_params = [user_id, *where_params]
                rows_all = conn.execute(
                    f"""
                    SELECT jr.run_id, jr.job_id, jr.score, jr.tier, jr.rule_score,
                           jr.reasons_json, jr.skill_hits_json, jr.llm_summary,
                           jr.llm_pros_json, jr.llm_risks_json, jr.scored_by,
                           j.source, j.source_type, j.title, j.company, j.location, j.url, j.description,
                           j.published, j.fetched_at, j.remote_hint, j.normalized_json,
                           a.status AS application_status
                    {base_from}
                    WHERE {where_sql}
                    ORDER BY jr.id ASC
                    """,
                    tuple(select_params),
                ).fetchall()
                rows_sorted = sorted(
                    rows_all,
                    key=lambda row: (
                        self._sortable_timestamp(row["published"], row["fetched_at"]),
                        str(row["job_id"] or ""),
                    ),
                    reverse=(sort_key == "newest"),
                )
                start = offset_value
                end = offset_value + limit_value + 1
                rows = rows_sorted[start:end]
                has_more = len(rows) > limit_value
                rows = rows[:limit_value]
            else:
                select_params = [user_id, *where_params, limit_value + 1, offset_value]
                rows = conn.execute(
                    f"""
                    SELECT jr.run_id, jr.job_id, jr.score, jr.tier, jr.rule_score,
                           jr.reasons_json, jr.skill_hits_json, jr.llm_summary,
                           jr.llm_pros_json, jr.llm_risks_json, jr.scored_by,
                           j.source, j.source_type, j.title, j.company, j.location, j.url, j.description,
                           j.published, j.fetched_at, j.remote_hint, j.normalized_json,
                           a.status AS application_status
                    {base_from}
                    WHERE {where_sql}
                    ORDER BY {order_by}
                    LIMIT ?
                    OFFSET ?
                    """,
                    tuple(select_params),
                ).fetchall()
                has_more = len(rows) > limit_value
                rows = rows[:limit_value]
            jobs = [self._hydrate_ranked_job(row, include_diagnostics=include_diagnostics) for row in rows]
            return {
                "run_id": resolved_run_id,
                "jobs": jobs,
                "limit": limit_value,
                "offset": offset_value,
                "total": total,
                "has_more": has_more,
            }
        finally:
            conn.close()

    def get_ranked_jobs(
        self,
        limit: int = 20,
        tier: str | None = None,
        run_id: str | None = None,
        include_diagnostics: bool = False,
    ) -> list[dict]:
        return self.search_ranked_jobs(
            limit=limit,
            tier=tier,
            run_id=run_id,
            include_diagnostics=include_diagnostics,
        )["jobs"]

    def list_applications(self, limit: int = 50, status: str | None = None, user_id: str = "default") -> list[dict]:
        conn = connect_sqlite(self.db_url)
        try:
            params = [user_id]
            status_clause = ""
            if status:
                status_clause = "AND status = ?"
                params.append(status)
            params.append(max(1, int(limit)))
            rows = conn.execute(
                f"""
                SELECT user_id, job_url, title, company, status, applied_at, notes,
                       next_action_at, next_action_type, created_at
                FROM applications
                WHERE user_id = ?
                  {status_clause}
                ORDER BY COALESCE(applied_at, created_at) DESC
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def get_application(self, job_url: str, user_id: str = "default") -> dict | None:
        normalized_url = str(job_url or "").strip().lower()
        if not normalized_url:
            return None
        conn = connect_sqlite(self.db_url)
        try:
            row = conn.execute(
                """
                SELECT user_id, job_url, title, company, status, applied_at, notes,
                       next_action_at, next_action_type, created_at
                FROM applications
                WHERE user_id = ? AND LOWER(job_url) = ?
                LIMIT 1
                """,
                (user_id, normalized_url),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def upsert_applications(self, applications: list[ApplicationRecord]):
        if not applications:
            return
        conn = connect_sqlite(self.db_url)
        try:
            conn.executemany(
                """
                INSERT INTO applications (
                    user_id, job_url, title, company, status, applied_at, notes, next_action_at, next_action_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id, job_url) DO UPDATE SET
                    title = excluded.title,
                    company = excluded.company,
                    status = excluded.status,
                    applied_at = CASE
                        WHEN excluded.applied_at IS NULL OR excluded.applied_at = '' THEN applications.applied_at
                        ELSE excluded.applied_at
                    END,
                    notes = excluded.notes,
                    next_action_at = CASE
                        WHEN excluded.next_action_at IS NULL OR excluded.next_action_at = '' THEN applications.next_action_at
                        ELSE excluded.next_action_at
                    END,
                    next_action_type = CASE
                        WHEN excluded.next_action_type IS NULL OR excluded.next_action_type = '' THEN applications.next_action_type
                        ELSE excluded.next_action_type
                    END
                """,
                [
                    (
                        a.user_id,
                        a.job_url,
                        a.title,
                        a.company,
                        a.status,
                        a.applied_at,
                        a.notes,
                        a.next_action_at,
                        a.next_action_type,
                    )
                    for a in applications
                ],
            )
            conn.commit()
        finally:
            conn.close()

    def set_application_status(
        self,
        job_url: str,
        status: str,
        title: str = "",
        company: str = "",
        notes: str = "",
        user_id: str = "default",
        applied_at: str | None = None,
        next_action_at: str | None = None,
        next_action_type: str | None = None,
    ) -> dict:
        normalized_url = str(job_url or "").strip().lower()
        normalized_status = str(status or "").strip().lower()
        if not normalized_url:
            raise ValueError("job_url is required")
        if not normalized_status:
            raise ValueError("status is required")

        existing = self.get_application(normalized_url, user_id=user_id)
        keep_applied_at = existing.get("applied_at") if existing else ""
        if applied_at is not None:
            effective_applied_at = applied_at
        elif normalized_status == "applied":
            effective_applied_at = keep_applied_at if keep_applied_at else datetime.now(timezone.utc).isoformat()
        else:
            effective_applied_at = keep_applied_at or ""
        final_title = title if title else (existing.get("title", "") if existing else "")
        final_company = company if company else (existing.get("company", "") if existing else "")
        final_notes = notes if notes else (existing.get("notes", "") if existing else "")
        final_next_action_at = (
            next_action_at
            if next_action_at is not None
            else ((existing.get("next_action_at") or "") if existing else "")
        )
        final_next_action_type = (
            next_action_type
            if next_action_type is not None
            else ((existing.get("next_action_type") or "") if existing else "")
        )

        self.upsert_applications(
            [
                ApplicationRecord(
                    user_id=user_id,
                    job_url=normalized_url,
                    title=final_title,
                    company=final_company,
                    status=normalized_status,
                    applied_at=effective_applied_at,
                    notes=final_notes,
                    next_action_at=final_next_action_at,
                    next_action_type=final_next_action_type,
                )
            ]
        )
        updated = self.get_application(normalized_url, user_id=user_id)
        if not updated:
            raise RuntimeError("failed to persist application status")
        return updated

    def set_application_followup(
        self,
        job_url: str,
        next_action_at: str,
        next_action_type: str,
        user_id: str = "default",
    ) -> dict:
        existing = self.get_application(job_url, user_id=user_id)
        if not existing:
            raise ValueError("application not found")
        return self.set_application_status(
            job_url=job_url,
            status=existing.get("status") or "saved",
            title=existing.get("title") or "",
            company=existing.get("company") or "",
            notes=existing.get("notes") or "",
            user_id=user_id,
            applied_at=existing.get("applied_at"),
            next_action_at=str(next_action_at or "").strip(),
            next_action_type=str(next_action_type or "").strip(),
        )

    def list_due_followups(self, user_id: str = "default", due_before: str | None = None, limit: int = 100) -> list[dict]:
        cutoff = str(due_before or datetime.now(timezone.utc).isoformat()).strip()
        conn = connect_sqlite(self.db_url)
        try:
            rows = conn.execute(
                """
                SELECT user_id, job_url, title, company, status, applied_at, notes,
                       next_action_at, next_action_type, created_at
                FROM applications
                WHERE user_id = ?
                  AND next_action_at IS NOT NULL
                  AND next_action_at != ''
                  AND DATETIME(next_action_at) <= DATETIME(?)
                  AND status NOT IN ('rejected', 'withdrawn')
                ORDER BY DATETIME(next_action_at) ASC
                LIMIT ?
                """,
                (user_id, cutoff, max(1, int(limit))),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def get_job_by_url(self, job_url: str) -> dict | None:
        normalized = str(job_url or "").strip().lower()
        if not normalized:
            return None
        conn = connect_sqlite(self.db_url)
        try:
            row = conn.execute(
                """
                SELECT id, source, source_type, title, company, location, remote_hint,
                       url, description, published, fetched_at, normalized_json
                FROM jobs
                WHERE LOWER(url) = ?
                ORDER BY fetched_at DESC
                LIMIT 1
                """,
                (normalized,),
            ).fetchone()
            if not row:
                return None
            item = dict(row)
            normalized_json = self._parse_json_object(item.get("normalized_json"))
            if normalized_json:
                item.update(normalized_json)
            return item
        finally:
            conn.close()

    def list_cover_letters(self, user_id: str = "default", job_url: str | None = None, limit: int = 30) -> list[dict]:
        conn = connect_sqlite(self.db_url)
        try:
            params = [user_id]
            job_clause = ""
            if job_url:
                job_clause = "AND LOWER(job_url) = ?"
                params.append(str(job_url).strip().lower())
            params.append(max(1, int(limit)))
            rows = conn.execute(
                f"""
                SELECT id, user_id, job_url, job_id, run_id, cv_variant, language, style,
                       company, title, body, generated_at, version
                FROM cover_letters
                WHERE user_id = ?
                  {job_clause}
                ORDER BY generated_at DESC, id DESC
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def get_latest_cover_letter(self, user_id: str, job_url: str) -> dict | None:
        rows = self.list_cover_letters(user_id=user_id, job_url=job_url, limit=1)
        return rows[0] if rows else None

    def save_cover_letter(self, item: CoverLetterRecord) -> dict:
        existing = self.get_latest_cover_letter(user_id=item.user_id, job_url=item.job_url)
        next_version = int(existing["version"]) + 1 if existing else 1
        conn = connect_sqlite(self.db_url)
        try:
            conn.execute(
                """
                INSERT INTO cover_letters (
                    user_id, job_url, job_id, run_id, cv_variant, language, style,
                    company, title, body, generated_at, version
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item.user_id,
                    item.job_url,
                    item.job_id,
                    item.run_id,
                    item.cv_variant,
                    item.language,
                    item.style,
                    item.company,
                    item.title,
                    item.body,
                    item.generated_at if item.generated_at else datetime.now(timezone.utc).isoformat(),
                    next_version,
                ),
            )
            conn.commit()
        finally:
            conn.close()
        latest = self.get_latest_cover_letter(user_id=item.user_id, job_url=item.job_url)
        if not latest:
            raise RuntimeError("failed to persist cover letter")
        return latest

    def list_feedback_events(
        self,
        limit: int = 100,
        action: str | None = None,
        job_url: str | None = None,
        user_id: str = "default",
    ) -> list[dict]:
        conn = connect_sqlite(self.db_url)
        try:
            params = [user_id]
            action_clause = ""
            job_clause = ""
            if action:
                action_clause = "AND action = ?"
                params.append(str(action).strip().lower())
            if job_url:
                job_clause = "AND LOWER(job_url) = ?"
                params.append(str(job_url).strip().lower())
            params.append(max(1, int(limit)))
            rows = conn.execute(
                f"""
                SELECT id, user_id, job_url, action, value, source, created_at
                FROM feedback_events
                WHERE user_id = ?
                  {action_clause}
                  {job_clause}
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def get_feedback_signal_data(self, user_id: str = "default", limit: int = 2000) -> dict:
        conn = connect_sqlite(self.db_url)
        try:
            applications = conn.execute(
                """
                SELECT a.job_url, a.status, a.title AS app_title, a.company AS app_company,
                       a.applied_at, a.created_at,
                       j.source, j.source_type, j.title AS job_title, j.company AS job_company
                FROM applications a
                LEFT JOIN jobs j
                  ON LOWER(j.url) = LOWER(a.job_url)
                WHERE a.user_id = ?
                ORDER BY COALESCE(a.applied_at, a.created_at) DESC
                LIMIT ?
                """,
                (user_id, max(1, int(limit))),
            ).fetchall()

            feedback = conn.execute(
                """
                SELECT f.job_url, f.action, f.value, f.source AS feedback_source, f.created_at,
                       j.source, j.source_type, j.title AS job_title, j.company AS job_company
                FROM feedback_events f
                LEFT JOIN jobs j
                  ON LOWER(j.url) = LOWER(f.job_url)
                WHERE f.user_id = ?
                ORDER BY f.created_at DESC, f.id DESC
                LIMIT ?
                """,
                (user_id, max(1, int(limit))),
            ).fetchall()

            return {
                "applications": [dict(row) for row in applications],
                "feedback": [dict(row) for row in feedback],
            }
        finally:
            conn.close()

    def get_application_metrics(self, user_id: str = "default", days: int = 30) -> dict:
        lookback_days = max(1, min(365, int(days)))
        conn = connect_sqlite(self.db_url)
        try:
            status_rows = conn.execute(
                """
                SELECT status, COUNT(*) AS count
                FROM applications
                WHERE user_id = ?
                GROUP BY status
                ORDER BY count DESC, status ASC
                """,
                (user_id,),
            ).fetchall()
            status_counts = {str(row["status"]): int(row["count"]) for row in status_rows}
            total = int(sum(status_counts.values()))

            progress_statuses = {"applied", "interview", "offer", "rejected", "withdrawn"}
            interview_statuses = {"interview", "offer"}
            applied_count = int(sum(v for k, v in status_counts.items() if k in progress_statuses))
            interview_count = int(sum(v for k, v in status_counts.items() if k in interview_statuses))
            offer_count = int(status_counts.get("offer", 0))

            cutoff_expr = f"-{lookback_days} days"
            recent_rows = conn.execute(
                """
                SELECT DATE(COALESCE(applied_at, created_at)) AS day, status, COUNT(*) AS count
                FROM applications
                WHERE user_id = ?
                  AND DATETIME(COALESCE(applied_at, created_at)) >= DATETIME('now', ?)
                GROUP BY day, status
                ORDER BY day DESC, status ASC
                """,
                (user_id, cutoff_expr),
            ).fetchall()
            activity = [
                {"day": row["day"], "status": row["status"], "count": int(row["count"])}
                for row in recent_rows
                if row["day"]
            ]

            feedback_rows = conn.execute(
                """
                SELECT action, COUNT(*) AS count
                FROM feedback_events
                WHERE user_id = ?
                  AND DATETIME(created_at) >= DATETIME('now', ?)
                GROUP BY action
                ORDER BY count DESC, action ASC
                """,
                (user_id, cutoff_expr),
            ).fetchall()
            feedback_counts = {str(row["action"]): int(row["count"]) for row in feedback_rows}

            due_today_row = conn.execute(
                """
                SELECT COUNT(*) AS due_today
                FROM applications
                WHERE user_id = ?
                  AND next_action_at IS NOT NULL
                  AND next_action_at != ''
                  AND DATE(next_action_at) = DATE('now')
                  AND status NOT IN ('rejected', 'withdrawn')
                """,
                (user_id,),
            ).fetchone()
            overdue_row = conn.execute(
                """
                SELECT COUNT(*) AS overdue
                FROM applications
                WHERE user_id = ?
                  AND next_action_at IS NOT NULL
                  AND next_action_at != ''
                  AND DATETIME(next_action_at) < DATETIME('now')
                  AND status NOT IN ('rejected', 'withdrawn')
                """,
                (user_id,),
            ).fetchone()

            def _ratio(numerator: int, denominator: int) -> float:
                if denominator <= 0:
                    return 0.0
                return round(float(numerator) / float(denominator), 4)

            return {
                "days": lookback_days,
                "total_applications": total,
                "status_counts": status_counts,
                "funnel": {
                    "saved_total": int(status_counts.get("saved", 0)),
                    "applied_or_beyond": applied_count,
                    "interview_or_beyond": interview_count,
                    "offers": offer_count,
                    "apply_rate": _ratio(applied_count, total),
                    "interview_rate": _ratio(interview_count, applied_count),
                    "offer_rate": _ratio(offer_count, interview_count),
                },
                "recent_activity": activity,
                "feedback_counts": feedback_counts,
                "followups": {
                    "due_today": int(due_today_row["due_today"]) if due_today_row else 0,
                    "overdue": int(overdue_row["overdue"]) if overdue_row else 0,
                },
            }
        finally:
            conn.close()

    def get_source_health(
        self,
        window_runs: int = 20,
        stale_after_hours: int = 72,
    ) -> list[dict]:
        run_limit = max(1, int(window_runs))
        stale_hours = max(1, int(stale_after_hours))
        conn = connect_sqlite(self.db_url)
        try:
            rows = conn.execute(
                """
                WITH recent_runs AS (
                    SELECT run_id
                    FROM pipeline_runs
                    ORDER BY started_at DESC
                    LIMIT ?
                )
                SELECT s.source_name,
                       COUNT(*) AS total_events,
                       SUM(CASE WHEN s.success = 1 THEN 1 ELSE 0 END) AS success_events,
                       SUM(CASE WHEN s.success = 0 THEN 1 ELSE 0 END) AS failed_events,
                       AVG(CASE WHEN s.success = 1 THEN s.jobs_fetched ELSE NULL END) AS avg_jobs_on_success,
                       MAX(s.created_at) AS last_seen_at,
                       MAX(CASE WHEN s.success = 1 THEN s.created_at ELSE NULL END) AS last_success_at
                FROM source_fetch_events s
                JOIN recent_runs r
                  ON r.run_id = s.run_id
                GROUP BY s.source_name
                ORDER BY s.source_name ASC
                """,
                (run_limit,),
            ).fetchall()

            health = []
            for row in rows:
                total = int(row["total_events"] or 0)
                success = int(row["success_events"] or 0)
                failed = int(row["failed_events"] or 0)
                success_rate = round(float(success) / float(total), 4) if total > 0 else 0.0
                stale = True
                if row["last_success_at"]:
                    stale_probe = conn.execute(
                        """
                        SELECT CASE
                          WHEN DATETIME(?) < DATETIME('now', ?) THEN 1 ELSE 0
                        END AS is_stale
                        """,
                        (row["last_success_at"], f"-{stale_hours} hours"),
                    ).fetchone()
                    stale = bool(int(stale_probe["is_stale"])) if stale_probe else True

                score = int(round((success_rate * 70.0) + (0 if stale else 20) + min(10, max(0, total - failed))))
                health.append(
                    {
                        "source_name": row["source_name"],
                        "window_runs": run_limit,
                        "total_events": total,
                        "success_events": success,
                        "failed_events": failed,
                        "success_rate": success_rate,
                        "avg_jobs_on_success": float(row["avg_jobs_on_success"] or 0.0),
                        "last_seen_at": row["last_seen_at"],
                        "last_success_at": row["last_success_at"],
                        "stale": stale,
                        "health_score": max(0, min(100, score)),
                    }
                )
            return health
        finally:
            conn.close()

    def add_feedback_events(self, events: list[FeedbackEventRecord]):
        if not events:
            return
        conn = connect_sqlite(self.db_url)
        try:
            conn.executemany(
                """
                INSERT INTO feedback_events (
                    user_id, job_url, action, value, source, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        e.user_id,
                        e.job_url,
                        e.action,
                        e.value,
                        e.source,
                        e.created_at if e.created_at else datetime.now(timezone.utc).isoformat(),
                    )
                    for e in events
                ],
            )
            conn.commit()
        finally:
            conn.close()

    def upsert_pipeline_run(self, run: PipelineRunRecord):
        conn = connect_sqlite(self.db_url)
        try:
            self._upsert_pipeline_run_conn(conn, run)
            conn.commit()
        finally:
            conn.close()

    def persist_pipeline_snapshot(
        self,
        run: PipelineRunRecord,
        jobs: list[JobRecord],
        rankings: list[JobRankingRecord],
        source_events: list[SourceFetchEventRecord] | None = None,
    ):
        conn = connect_sqlite(self.db_url)
        try:
            conn.execute("BEGIN")
            self._upsert_pipeline_run_conn(conn, run)
            self._upsert_jobs_conn(conn, jobs)
            self._replace_run_rankings_conn(conn, run.run_id, rankings)
            self._replace_run_source_events_conn(conn, run.run_id, source_events or [])
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _upsert_pipeline_run_conn(self, conn: sqlite3.Connection, run: PipelineRunRecord):
        conn.execute(
            """
            INSERT INTO pipeline_runs (
                run_id, started_at, ended_at, status, duration_ms,
                total_jobs, a_tier, b_tier, c_tier, skipped_applied,
                llm_enabled, llm_model, llm_scored_live, llm_cache_hits, llm_failed,
                source_errors, error_message, summary_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id) DO UPDATE SET
                started_at = excluded.started_at,
                ended_at = excluded.ended_at,
                status = excluded.status,
                duration_ms = excluded.duration_ms,
                total_jobs = excluded.total_jobs,
                a_tier = excluded.a_tier,
                b_tier = excluded.b_tier,
                c_tier = excluded.c_tier,
                skipped_applied = excluded.skipped_applied,
                llm_enabled = excluded.llm_enabled,
                llm_model = excluded.llm_model,
                llm_scored_live = excluded.llm_scored_live,
                llm_cache_hits = excluded.llm_cache_hits,
                llm_failed = excluded.llm_failed,
                source_errors = excluded.source_errors,
                error_message = excluded.error_message,
                summary_json = excluded.summary_json
            """,
            (
                run.run_id,
                run.started_at,
                run.ended_at,
                run.status,
                run.duration_ms,
                run.total_jobs,
                run.a_tier,
                run.b_tier,
                run.c_tier,
                run.skipped_applied,
                run.llm_enabled,
                run.llm_model,
                run.llm_scored_live,
                run.llm_cache_hits,
                run.llm_failed,
                run.source_errors,
                run.error_message,
                run.summary_json,
            ),
        )

    def _upsert_jobs_conn(self, conn: sqlite3.Connection, jobs: list[JobRecord]):
        if not jobs:
            return
        conn.executemany(
            """
            INSERT INTO jobs (
                id, source, source_type, title, company, location, remote_hint,
                url, description, published, fetched_at, normalized_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                source = excluded.source,
                source_type = excluded.source_type,
                title = excluded.title,
                company = excluded.company,
                location = excluded.location,
                remote_hint = excluded.remote_hint,
                url = excluded.url,
                description = excluded.description,
                published = excluded.published,
                fetched_at = excluded.fetched_at,
                normalized_json = excluded.normalized_json
            """,
            [
                (
                    j.id,
                    j.source,
                    j.source_type,
                    j.title,
                    j.company,
                    j.location,
                    j.remote_hint,
                    j.url,
                    j.description,
                    j.published,
                    j.fetched_at,
                    j.normalized_json,
                )
                for j in jobs
            ],
        )

    def _replace_run_rankings_conn(
        self,
        conn: sqlite3.Connection,
        run_id: str,
        rankings: list[JobRankingRecord],
    ):
        conn.execute("DELETE FROM job_rankings WHERE run_id = ?", (run_id,))
        if not rankings:
            return
        conn.executemany(
            """
            INSERT INTO job_rankings (
                run_id, job_id, score, tier, rule_score,
                reasons_json, skill_hits_json, llm_summary,
                llm_pros_json, llm_risks_json, scored_by
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id, job_id) DO UPDATE SET
                score = excluded.score,
                tier = excluded.tier,
                rule_score = excluded.rule_score,
                reasons_json = excluded.reasons_json,
                skill_hits_json = excluded.skill_hits_json,
                llm_summary = excluded.llm_summary,
                llm_pros_json = excluded.llm_pros_json,
                llm_risks_json = excluded.llm_risks_json,
                scored_by = excluded.scored_by
            """,
            [
                (
                    r.run_id,
                    r.job_id,
                    r.score,
                    r.tier,
                    r.rule_score,
                    r.reasons_json,
                    r.skill_hits_json,
                    r.llm_summary,
                    r.llm_pros_json,
                    r.llm_risks_json,
                    r.scored_by,
                )
                for r in rankings
            ],
        )

    def _replace_run_source_events_conn(
        self,
        conn: sqlite3.Connection,
        run_id: str,
        source_events: list[SourceFetchEventRecord],
    ):
        conn.execute("DELETE FROM source_fetch_events WHERE run_id = ?", (run_id,))
        if not source_events:
            return
        conn.executemany(
            """
            INSERT INTO source_fetch_events (
                run_id, source_name, source_kind, source_type, source_url,
                attempts, success, jobs_fetched, duration_ms, error_message
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    e.run_id,
                    e.source_name,
                    e.source_kind,
                    e.source_type,
                    e.source_url,
                    e.attempts,
                    e.success,
                    e.jobs_fetched,
                    e.duration_ms,
                    e.error_message,
                )
                for e in source_events
            ],
        )
