import json
from datetime import datetime, timezone
from urllib.request import Request, urlopen

from job_search.json_io import save_json
from job_search.paths import DATA


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _append_jsonl(path, payload: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def log_event(event: str, level: str = "info", **fields):
    row = {"ts": _utc_now(), "event": event, "level": level, **fields}
    print(json.dumps(row, ensure_ascii=False))


def emit_metric(name: str, value: float = 1.0, tags: dict | None = None):
    row = {
        "ts": _utc_now(),
        "name": str(name),
        "value": float(value),
        "tags": tags or {},
    }
    _append_jsonl(DATA / "metrics.jsonl", row)


def emit_alert(kind: str, message: str, severity: str = "error", details: dict | None = None, webhook_url: str = ""):
    row = {
        "ts": _utc_now(),
        "kind": str(kind),
        "severity": str(severity),
        "message": str(message),
        "details": details or {},
    }
    _append_jsonl(DATA / "alerts.jsonl", row)

    url = str(webhook_url or "").strip()
    if url:
        try:
            body = json.dumps(row, ensure_ascii=False).encode("utf-8")
            req = Request(url, data=body, headers={"Content-Type": "application/json"})
            with urlopen(req, timeout=5):
                pass
        except Exception:
            # Keep alerting best-effort and never fail the caller.
            pass


def write_runtime_metrics_snapshot(snapshot: dict):
    save_json(DATA / "runtime_metrics.json", snapshot or {})
