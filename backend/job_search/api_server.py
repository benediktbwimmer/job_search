import json
import mimetypes
import os
import re
import subprocess
import sys
import threading
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from job_search.auth import normalize_auth_config, validate_auth_config
from job_search.cover_letter import generate_cover_letter
from job_search.models import CoverLetterRecord
from job_search.models import FeedbackEventRecord
from job_search.observability import emit_metric, log_event
from job_search.ui_pages import board_html as _board_page_html
from job_search.ui_pages import dashboard_html as _dashboard_page_html
from job_search.ui_pages import workspace_html as _workspace_page_html

_ALLOWED_APPLICATION_STATUSES = {
    "saved",
    "applied",
    "interview",
    "offer",
    "rejected",
    "withdrawn",
    "dismissed",
}

_PROGRESS_RE = re.compile(
    r"LLM progress\s+(\d+)/(\d+)\s+\(live=(\d+),\s*cache=(\d+),\s*failed=(\d+),\s*filtered=(\d+)\)",
    flags=re.IGNORECASE,
)


class PipelineRunController:
    def __init__(self, backend_root: Path):
        self.backend_root = Path(backend_root)
        self._lock = threading.Lock()
        self._proc: subprocess.Popen | None = None
        self._state = self._empty_state()

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def _empty_state(self) -> dict:
        return {
            "running": False,
            "status": "idle",
            "run_id": "",
            "started_at": "",
            "ended_at": "",
            "elapsed_seconds": 0,
            "pid": None,
            "progress": {
                "processed": 0,
                "total": 0,
                "live": 0,
                "cache": 0,
                "failed": 0,
                "filtered": 0,
            },
            "logs": [],
            "exit_code": None,
        }

    def _snapshot_locked(self) -> dict:
        out = dict(self._state)
        out["progress"] = dict(self._state.get("progress") or {})
        out["logs"] = list(self._state.get("logs") or [])
        started = str(out.get("started_at") or "")
        if started:
            try:
                started_dt = datetime.fromisoformat(started)
                end_dt = datetime.now(timezone.utc) if out.get("running") else datetime.fromisoformat(
                    str(out.get("ended_at") or self._utc_now())
                )
                out["elapsed_seconds"] = max(0, int((end_dt - started_dt).total_seconds()))
            except Exception:
                out["elapsed_seconds"] = 0
        return out

    def get_active(self) -> dict:
        with self._lock:
            if self._proc and self._proc.poll() is not None and self._state.get("running"):
                self._finalize_locked(int(self._proc.poll() or 0))
            return self._snapshot_locked()

    def start(self) -> tuple[bool, dict]:
        with self._lock:
            if self._proc and self._proc.poll() is None:
                return False, self._snapshot_locked()

            cmd = [sys.executable, "-u", "scripts/run_pipeline.py"]
            env = os.environ.copy()
            current_pythonpath = env.get("PYTHONPATH", "")
            env["PYTHONPATH"] = str(self.backend_root) + (":" + current_pythonpath if current_pythonpath else "")

            proc = subprocess.Popen(
                cmd,
                cwd=str(self.backend_root),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                env=env,
            )
            self._proc = proc
            self._state = self._empty_state()
            self._state.update(
                {
                    "running": True,
                    "status": "running",
                    "started_at": self._utc_now(),
                    "pid": int(proc.pid),
                    "logs": deque(maxlen=220),
                }
            )

            watcher = threading.Thread(target=self._watch_process, args=(proc,), daemon=True)
            watcher.start()
            return True, self._snapshot_locked()

    def _append_log_locked(self, line: str):
        logs = self._state.get("logs")
        if not isinstance(logs, deque):
            logs = deque(logs or [], maxlen=220)
        logs.append(line)
        self._state["logs"] = logs

    def _parse_line_locked(self, line: str):
        self._append_log_locked(line)
        msg = str(line or "").strip()
        if not msg:
            return

        m = _PROGRESS_RE.search(msg)
        if m:
            self._state["progress"] = {
                "processed": int(m.group(1)),
                "total": int(m.group(2)),
                "live": int(m.group(3)),
                "cache": int(m.group(4)),
                "failed": int(m.group(5)),
                "filtered": int(m.group(6)),
            }
            return

        if not msg.startswith("{"):
            return

        try:
            payload = json.loads(msg)
        except Exception:
            return
        event = str(payload.get("event") or "")
        if event == "pipeline_run_started":
            self._state["run_id"] = str(payload.get("run_id") or "")
            self._state["status"] = "running"
        elif event == "pipeline_run_completed":
            self._state["run_id"] = str(payload.get("run_id") or self._state.get("run_id") or "")
            self._state["status"] = str(payload.get("status") or "success")
            self._state["running"] = False
            self._state["ended_at"] = self._utc_now()
        elif event == "pipeline_run_failed":
            self._state["run_id"] = str(payload.get("run_id") or self._state.get("run_id") or "")
            self._state["status"] = "failed"
            self._state["running"] = False
            self._state["ended_at"] = self._utc_now()

    def _finalize_locked(self, code: int):
        self._state["running"] = False
        if not self._state.get("ended_at"):
            self._state["ended_at"] = self._utc_now()
        if self._state.get("status") in {"idle", "running"}:
            self._state["status"] = "success" if int(code) == 0 else "failed"
        self._state["exit_code"] = int(code)

    def _watch_process(self, proc: subprocess.Popen):
        if proc.stdout is None:
            with self._lock:
                self._finalize_locked(1)
            return

        for raw in proc.stdout:
            line = str(raw).rstrip("\n")
            with self._lock:
                self._parse_line_locked(line)

        code = int(proc.wait() or 0)
        with self._lock:
            self._finalize_locked(code)


def _int_param(query: dict, key: str, default: int, minimum: int = 1, maximum: int = 500) -> int:
    raw = query.get(key, [default])[0]
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


def _optional_int_param(query: dict, key: str, minimum: int = 0, maximum: int = 100) -> int | None:
    if key not in query:
        return None
    raw = query.get(key, [None])[0]
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None
    return max(minimum, min(maximum, value))


def _str_param(query: dict, key: str) -> str | None:
    raw = query.get(key, [None])[0]
    if raw is None:
        return None
    text = str(raw).strip()
    return text if text else None


def _bool_param(query: dict, key: str) -> bool | None:
    raw = query.get(key, [None])[0]
    if raw is None:
        return None
    value = str(raw).strip().lower()
    if value in {"1", "true", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _resolve_api_key(headers, query: dict) -> str:
    auth = headers.get("Authorization", "").strip()
    if auth.lower().startswith("bearer "):
        token = auth[7:].strip()
        if token:
            return token
    header_key = headers.get("X-API-Key", "").strip()
    if header_key:
        return header_key
    query_key = _str_param(query, "api_key")
    return query_key or ""


def _is_api_get_path(path: str) -> bool:
    exact = {
        "/",
        "/health",
        "/runs",
        "/runs/active",
        "/jobs",
        "/applications",
        "/applications/metrics",
        "/applications/followups",
        "/applications/workspace",
        "/cover-letters",
        "/sources/health",
        "/metrics",
        "/feedback",
    }
    if path in exact:
        return True
    if path.startswith("/runs/") and (path.endswith("/sources") or len(path.split("/")) == 3):
        return True
    return False


def _is_api_post_path(path: str) -> bool:
    return path in {
        "/applications",
        "/applications/bulk",
        "/applications/followup",
        "/feedback",
        "/cover-letters/generate",
        "/runs/start",
    }

_LEGACY_GET_API_PATHS = {
    "/",
    "/health",
    "/jobs",
    "/applications",
    "/applications/metrics",
    "/applications/followups",
    "/applications/workspace",
    "/cover-letters",
    "/sources/health",
    "/metrics",
    "/feedback",
}

_LEGACY_POST_API_PATHS = {
    "/applications",
    "/applications/bulk",
    "/applications/followup",
    "/feedback",
    "/cover-letters/generate",
}


def _resolve_user_id(path: str, headers, query: dict, auth_config: dict, is_api_request: bool) -> str:
    auth_cfg = auth_config if isinstance(auth_config, dict) else {}
    if not bool(auth_cfg.get("enabled", False)):
        return "default"
    if not is_api_request:
        return "default"
    if path in {"/", "/health"}:
        return "default"

    token = _resolve_api_key(headers, query)
    if not token:
        raise PermissionError("missing api key")
    api_keys = auth_cfg.get("api_keys", {}) if isinstance(auth_cfg.get("api_keys"), dict) else {}
    user_id = str(api_keys.get(token) or "").strip()
    if not user_id:
        raise PermissionError("invalid api key")
    return user_id


def _json_response(handler, status: int, payload: dict):
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Cache-Control", "no-store")
    handler.end_headers()
    handler.wfile.write(body)
    handler._last_status_code = status


def _html_response(handler, status: int, html_text: str):
    body = html_text.encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "text/html; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)
    handler._last_status_code = status


def _file_response(handler, status: int, content: bytes, content_type: str):
    handler.send_response(status)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Content-Length", str(len(content)))
    handler.end_headers()
    handler.wfile.write(content)
    handler._last_status_code = status


def _sanitize_path(path: str) -> str:
    value = str(path or "").split("?", 1)[0].split("#", 1)[0]
    return value.rstrip("/") or "/"


def build_handler(repo, profile: dict | None = None, auth_config: dict | None = None, frontend_dist: str | None = None):
    profile = profile or {}
    auth_config = auth_config or {"enabled": False, "api_keys": {}}
    api_metrics = {
        "requests_total": 0,
        "errors_total": 0,
        "by_method": {},
        "by_path": {},
        "by_status": {},
    }

    backend_root = Path(__file__).resolve().parents[1]
    frontend_path = Path(frontend_dist).resolve() if frontend_dist else (backend_root.parent / "frontend" / "dist").resolve()
    index_path = frontend_path / "index.html"
    run_controller = PipelineRunController(backend_root=backend_root)

    class ApiHandler(BaseHTTPRequestHandler):
        def _write_json(self, status: int, payload: dict):
            _json_response(self, status, payload)

        def _write_html(self, status: int, html_text: str):
            _html_response(self, status, html_text)

        def _write_file(self, status: int, content: bytes, content_type: str):
            _file_response(self, status, content, content_type)

        def _not_found(self):
            self._write_json(404, {"error": "not_found"})

        def _read_json_body(self) -> dict:
            length_raw = self.headers.get("Content-Length", "0")
            try:
                length = int(length_raw)
            except (TypeError, ValueError):
                raise ValueError("invalid_content_length")
            if length <= 0:
                raise ValueError("empty_body")
            if length > 1_000_000:
                raise ValueError("body_too_large")
            raw = self.rfile.read(length)
            try:
                payload = json.loads(raw.decode("utf-8"))
            except Exception:
                raise ValueError("invalid_json")
            if not isinstance(payload, dict):
                raise ValueError("invalid_json_object")
            return payload

        def _record_api_stats(self, method: str, path: str, status_code: int):
            status_key = str(int(status_code))
            api_metrics["requests_total"] = int(api_metrics.get("requests_total", 0)) + 1
            api_metrics["by_method"][method] = int(api_metrics["by_method"].get(method, 0)) + 1
            api_metrics["by_path"][path] = int(api_metrics["by_path"].get(path, 0)) + 1
            api_metrics["by_status"][status_key] = int(api_metrics["by_status"].get(status_key, 0)) + 1
            if int(status_code) >= 500:
                api_metrics["errors_total"] = int(api_metrics.get("errors_total", 0)) + 1

        def _try_serve_frontend_asset(self, raw_path: str) -> bool:
            if not index_path.exists():
                return False
            path = _sanitize_path(raw_path)
            if path == "/":
                content = index_path.read_bytes()
                self._write_file(200, content, "text/html; charset=utf-8")
                return True

            rel = path.lstrip("/")
            candidate = (frontend_path / rel).resolve()
            try:
                candidate.relative_to(frontend_path)
            except Exception:
                return False
            if candidate.is_file():
                ctype = mimetypes.guess_type(str(candidate))[0] or "application/octet-stream"
                if ctype.startswith("text/"):
                    ctype = ctype + "; charset=utf-8"
                self._write_file(200, candidate.read_bytes(), ctype)
                return True

            if path in {"/dashboard", "/workspace", "/board", "/runs"} or not path.startswith("/api"):
                content = index_path.read_bytes()
                self._write_file(200, content, "text/html; charset=utf-8")
                return True
            return False

        def _handle_get_api(self, path: str, query: dict, user_id: str) -> bool:
            if path in {"/", "/health"}:
                self._write_json(200, {"ok": True})
                return True

            if path == "/runs":
                limit = _int_param(query, "limit", 10)
                runs = repo.get_recent_runs(limit=limit)
                self._write_json(200, {"runs": runs})
                return True

            if path == "/runs/active":
                self._write_json(200, {"run": run_controller.get_active()})
                return True

            if path == "/jobs":
                limit = _int_param(query, "limit", 20, minimum=1, maximum=100)
                offset = _int_param(query, "offset", 0, minimum=0, maximum=5000)
                result = repo.search_ranked_jobs(
                    limit=limit,
                    offset=offset,
                    tier=_str_param(query, "tier"),
                    run_id=_str_param(query, "run_id"),
                    query_text=_str_param(query, "q"),
                    company=_str_param(query, "company"),
                    source=_str_param(query, "source"),
                    source_type=_str_param(query, "source_type"),
                    location=_str_param(query, "location"),
                    remote=_bool_param(query, "remote"),
                    min_score=_optional_int_param(query, "min_score", minimum=0, maximum=100),
                    max_score=_optional_int_param(query, "max_score", minimum=0, maximum=100),
                    application_status=_str_param(query, "application_status"),
                    sort=_str_param(query, "sort") or "score_desc",
                    include_diagnostics=bool(_bool_param(query, "include_diagnostics")),
                    user_id=user_id,
                )
                self._write_json(200, result)
                return True

            if path == "/applications":
                limit = _int_param(query, "limit", 50)
                status = query.get("status", [None])[0]
                apps = repo.list_applications(limit=limit, status=status, user_id=user_id)
                self._write_json(200, {"applications": apps})
                return True

            if path == "/applications/metrics":
                days = _int_param(query, "days", 30, minimum=1, maximum=365)
                metrics = repo.get_application_metrics(user_id=user_id, days=days)
                self._write_json(200, {"metrics": metrics})
                return True

            if path == "/applications/followups":
                limit = _int_param(query, "limit", 100, minimum=1, maximum=500)
                due_before = _str_param(query, "due_before")
                items = repo.list_due_followups(user_id=user_id, due_before=due_before, limit=limit)
                self._write_json(200, {"followups": items})
                return True

            if path == "/applications/workspace":
                job_url = _str_param(query, "job_url")
                if not job_url:
                    apps = repo.list_applications(limit=1, user_id=user_id)
                    if not apps:
                        self._write_json(200, {"workspace": None})
                        return True
                    job_url = str(apps[0].get("job_url") or "")
                application = repo.get_application(job_url=job_url, user_id=user_id)
                job = repo.get_job_by_url(job_url)
                feedback = repo.list_feedback_events(limit=200, user_id=user_id, job_url=job_url)
                cover_letters = repo.list_cover_letters(user_id=user_id, job_url=job_url, limit=20)
                self._write_json(
                    200,
                    {
                        "workspace": {
                            "application": application,
                            "job": job,
                            "feedback": feedback,
                            "cover_letters": cover_letters,
                        }
                    },
                )
                return True

            if path == "/cover-letters":
                limit = _int_param(query, "limit", 30, minimum=1, maximum=200)
                job_url = _str_param(query, "job_url")
                rows = repo.list_cover_letters(user_id=user_id, job_url=job_url, limit=limit)
                self._write_json(200, {"cover_letters": rows})
                return True

            if path == "/sources/health":
                window_runs = _int_param(query, "window_runs", 12, minimum=1, maximum=200)
                stale_after_hours = _int_param(query, "stale_after_hours", 72, minimum=1, maximum=24 * 90)
                rows = repo.get_source_health(window_runs=window_runs, stale_after_hours=stale_after_hours)
                self._write_json(200, {"sources": rows})
                return True

            if path == "/metrics":
                self._write_json(200, {"api_metrics": api_metrics})
                return True

            if path == "/feedback":
                limit = _int_param(query, "limit", 100)
                action = query.get("action", [None])[0]
                job_url = _str_param(query, "job_url")
                events = repo.list_feedback_events(limit=limit, action=action, job_url=job_url, user_id=user_id)
                self._write_json(200, {"feedback": events})
                return True

            if path.startswith("/runs/") and path.endswith("/sources"):
                run_id = path[len("/runs/") : -len("/sources")]
                run_id = run_id[:-1] if run_id.endswith("/") else run_id
                if not run_id:
                    self._not_found()
                    return True
                events = repo.get_run_source_events(run_id)
                self._write_json(200, {"run_id": run_id, "source_events": events})
                return True

            if path.startswith("/runs/"):
                run_id = path[len("/runs/") :]
                run_id = run_id[:-1] if run_id.endswith("/") else run_id
                if not run_id:
                    self._not_found()
                    return True
                run = repo.get_run(run_id)
                if not run:
                    self._not_found()
                    return True
                self._write_json(200, {"run": run})
                return True

            return False

        def _handle_post_api(self, path: str, user_id: str) -> bool:
            if path == "/runs/start":
                started, run_state = run_controller.start()
                if started:
                    self._write_json(202, {"started": True, "run": run_state})
                else:
                    self._write_json(409, {"started": False, "message": "pipeline already running", "run": run_state})
                return True

            if path == "/applications":
                payload = self._read_json_body()
                job_url = str(payload.get("job_url") or "").strip()
                status = str(payload.get("status") or "").strip().lower()
                title = str(payload.get("title") or "")
                company = str(payload.get("company") or "")
                notes = str(payload.get("notes") or "")
                applied_at = payload.get("applied_at")
                applied_at = str(applied_at) if applied_at is not None else None
                next_action_at = payload.get("next_action_at")
                next_action_at = str(next_action_at) if next_action_at is not None else None
                next_action_type = payload.get("next_action_type")
                next_action_type = str(next_action_type) if next_action_type is not None else None
                if not job_url:
                    raise ValueError("job_url is required")
                if status not in _ALLOWED_APPLICATION_STATUSES:
                    raise ValueError("invalid status")
                updated = repo.set_application_status(
                    job_url=job_url,
                    status=status,
                    title=title,
                    company=company,
                    notes=notes,
                    user_id=user_id,
                    applied_at=applied_at,
                    next_action_at=next_action_at,
                    next_action_type=next_action_type,
                )
                self._write_json(200, {"application": updated})
                return True

            if path == "/applications/bulk":
                payload = self._read_json_body()
                items = payload.get("items")
                if not isinstance(items, list):
                    raise ValueError("items must be a list")
                updated_rows = []
                for idx, item in enumerate(items):
                    if not isinstance(item, dict):
                        raise ValueError(f"items[{idx}] must be an object")
                    job_url = str(item.get("job_url") or "").strip()
                    status = str(item.get("status") or "").strip().lower()
                    title = str(item.get("title") or "")
                    company = str(item.get("company") or "")
                    notes = str(item.get("notes") or "")
                    applied_at = item.get("applied_at")
                    applied_at = str(applied_at) if applied_at is not None else None
                    next_action_at = item.get("next_action_at")
                    next_action_at = str(next_action_at) if next_action_at is not None else None
                    next_action_type = item.get("next_action_type")
                    next_action_type = str(next_action_type) if next_action_type is not None else None
                    if not job_url:
                        raise ValueError(f"items[{idx}].job_url is required")
                    if status not in _ALLOWED_APPLICATION_STATUSES:
                        raise ValueError(f"items[{idx}].status is invalid")
                    updated_rows.append(
                        repo.set_application_status(
                            job_url=job_url,
                            status=status,
                            title=title,
                            company=company,
                            notes=notes,
                            user_id=user_id,
                            applied_at=applied_at,
                            next_action_at=next_action_at,
                            next_action_type=next_action_type,
                        )
                    )
                self._write_json(200, {"updated": len(updated_rows), "applications": updated_rows})
                return True

            if path == "/applications/followup":
                payload = self._read_json_body()
                job_url = str(payload.get("job_url") or "").strip()
                next_action_at = str(payload.get("next_action_at") or "").strip()
                next_action_type = str(payload.get("next_action_type") or "").strip()
                if not job_url:
                    raise ValueError("job_url is required")
                if not next_action_at:
                    raise ValueError("next_action_at is required")
                if not next_action_type:
                    raise ValueError("next_action_type is required")
                updated = repo.set_application_followup(
                    job_url=job_url,
                    next_action_at=next_action_at,
                    next_action_type=next_action_type,
                    user_id=user_id,
                )
                self._write_json(200, {"application": updated})
                return True

            if path == "/feedback":
                payload = self._read_json_body()
                job_url = str(payload.get("job_url") or "").strip()
                action = str(payload.get("action") or "").strip().lower()
                value = str(payload.get("value") or "")
                source = str(payload.get("source") or "api")
                created_at = payload.get("created_at")
                created_at = str(created_at) if created_at is not None else ""
                allowed_actions = {
                    "viewed",
                    "saved",
                    "dismissed",
                    "applied",
                    "interview",
                    "offer",
                    "rejected",
                    "clicked",
                }
                if not job_url:
                    raise ValueError("job_url is required")
                if action not in allowed_actions:
                    raise ValueError("invalid action")
                repo.add_feedback_events(
                    [
                        FeedbackEventRecord.from_dict(
                            {
                                "job_url": job_url,
                                "action": action,
                                "value": value,
                                "source": source,
                                "created_at": created_at,
                            },
                            user_id=user_id,
                        )
                    ]
                )
                self._write_json(201, {"ok": True})
                return True

            if path == "/cover-letters/generate":
                payload = self._read_json_body()
                job_url = str(payload.get("job_url") or "").strip().lower()
                cv_variant = str(payload.get("cv_variant") or "en_short").strip().lower()
                style = str(payload.get("style") or "concise").strip().lower()
                additional_context = str(payload.get("additional_context") or "").strip()
                regenerate_raw = payload.get("regenerate", False)
                regenerate = (
                    regenerate_raw.strip().lower() in {"1", "true", "yes", "y", "on"}
                    if isinstance(regenerate_raw, str)
                    else bool(regenerate_raw)
                )
                if not job_url:
                    raise ValueError("job_url is required")
                if not regenerate:
                    existing = repo.get_latest_cover_letter(user_id=user_id, job_url=job_url)
                    if existing:
                        self._write_json(200, {"cover_letter": existing, "cached": True})
                        return True
                job = repo.get_job_by_url(job_url)
                if not job:
                    raise ValueError("job not found")
                generated = generate_cover_letter(
                    profile=profile,
                    job=job,
                    cv_variant=cv_variant,
                    style=style,
                    additional_context=additional_context,
                    model="gpt-5.2",
                )
                saved = repo.save_cover_letter(
                    CoverLetterRecord(
                        user_id=user_id,
                        job_url=job_url,
                        job_id=str(job.get("id") or ""),
                        run_id="",
                        cv_variant=generated["cv_variant"],
                        language=generated["language"],
                        style=generated["style"],
                        company=str(job.get("company") or ""),
                        title=str(job.get("title") or ""),
                        body=generated["body"],
                        generated_at=datetime.now(timezone.utc).isoformat(),
                    )
                )
                self._write_json(201, {"cover_letter": saved, "cached": False, "model": generated.get("model")})
                return True

            return False

        def _serve_legacy_page(self, raw_path: str):
            path = _sanitize_path(raw_path)
            if path in {"/", "/dashboard"}:
                self._write_html(200, _dashboard_page_html())
                return True
            if path == "/workspace":
                self._write_html(200, _workspace_page_html())
                return True
            if path == "/board":
                self._write_html(200, _board_page_html())
                return True
            if path == "/runs":
                self._write_html(
                    200,
                    """<!doctype html><html><head><meta charset='utf-8'><title>Runs</title></head>
<body><h1>Runs</h1><p>Frontend dist missing. Build frontend first.</p></body></html>""",
                )
                return True
            return False

        def do_GET(self):
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            raw_path = _sanitize_path(parsed.path)
            api_path = raw_path
            if raw_path == "/api":
                api_path = "/"
            elif raw_path.startswith("/api/"):
                api_path = _sanitize_path(raw_path[4:])

            is_api_request = raw_path.startswith("/api") or raw_path in _LEGACY_GET_API_PATHS
            status_code = 500
            try:
                user_id = _resolve_user_id(
                    path=api_path,
                    headers=self.headers,
                    query=query,
                    auth_config=auth_config,
                    is_api_request=is_api_request,
                )

                handled_api = self._handle_get_api(api_path, query, user_id=user_id) if is_api_request else False
                if handled_api:
                    status_code = int(getattr(self, "_last_status_code", 200))
                else:
                    if self._try_serve_frontend_asset(raw_path) or self._serve_legacy_page(raw_path):
                        status_code = int(getattr(self, "_last_status_code", 200))
                    else:
                        self._not_found()
                        status_code = 404

                log_event("api_request", method="GET", path=raw_path, status=status_code, user_id=user_id)
                emit_metric("api_request", tags={"method": "GET", "path": raw_path, "status": str(status_code)})
            except PermissionError as e:
                self._write_json(401, {"error": "unauthorized", "message": str(e)})
                status_code = 401
            except Exception as e:
                self._write_json(500, {"error": "internal_error", "message": str(e)[:220]})
                status_code = 500
            finally:
                self._record_api_stats(method="GET", path=raw_path, status_code=status_code)

        def do_POST(self):
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            raw_path = _sanitize_path(parsed.path)
            api_path = raw_path
            if raw_path == "/api":
                api_path = "/"
            elif raw_path.startswith("/api/"):
                api_path = _sanitize_path(raw_path[4:])

            is_api_request = raw_path.startswith("/api") or raw_path in _LEGACY_POST_API_PATHS
            status_code = 500
            try:
                user_id = _resolve_user_id(
                    path=api_path,
                    headers=self.headers,
                    query=query,
                    auth_config=auth_config,
                    is_api_request=is_api_request,
                )
                if not is_api_request or not self._handle_post_api(api_path, user_id=user_id):
                    self._not_found()
                status_code = int(getattr(self, "_last_status_code", 200))
                log_event("api_request", method="POST", path=raw_path, status=status_code, user_id=user_id)
                emit_metric("api_request", tags={"method": "POST", "path": raw_path, "status": str(status_code)})
            except PermissionError as e:
                self._write_json(401, {"error": "unauthorized", "message": str(e)})
                status_code = 401
            except ValueError as e:
                self._write_json(400, {"error": "bad_request", "message": str(e)})
                status_code = 400
            except Exception as e:
                self._write_json(500, {"error": "internal_error", "message": str(e)[:220]})
                status_code = 500
            finally:
                self._record_api_stats(method="POST", path=raw_path, status_code=status_code)

        def log_message(self, format, *args):
            return

    return ApiHandler


def serve_api(
    repo,
    host: str = "127.0.0.1",
    port: int = 8787,
    profile: dict | None = None,
    auth_config: dict | None = None,
    frontend_dist: str | None = None,
):
    auth_input = auth_config if auth_config is not None else {"enabled": False, "api_keys": {}}
    auth_errors = validate_auth_config(auth_input)
    if auth_errors:
        raise ValueError(f"invalid auth config: {'; '.join(auth_errors)}")
    normalized_auth = normalize_auth_config(auth_input)

    handler = build_handler(repo, profile=profile, auth_config=normalized_auth, frontend_dist=frontend_dist)
    server = ThreadingHTTPServer((host, int(port)), handler)
    log_event("api_server_started", host=host, port=int(port), frontend_dist=str(frontend_dist or ""))
    return server
