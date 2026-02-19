import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from urllib.request import Request, urlopen

from job_search.paths import BASE


class FetchBackendError(RuntimeError):
    pass


@dataclass
class FetchResult:
    text: str
    backend: str
    url: str
    status_code: int | None = None
    content_type: str = "text/plain"


DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def _coerce_headers(headers: dict | None) -> dict:
    out = {
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "de-AT,de;q=0.9,en;q=0.8",
    }
    if isinstance(headers, dict):
        for key, value in headers.items():
            k = str(key or "").strip()
            v = str(value or "").strip()
            if k and v:
                out[k] = v
    return out


def _fetch_http(url: str, timeout_sec: int, headers: dict | None) -> FetchResult:
    req = Request(url, headers=_coerce_headers(headers))
    with urlopen(req, timeout=max(1, int(timeout_sec))) as resp:
        text = resp.read().decode("utf-8", errors="ignore")
        status = int(getattr(resp, "status", 200))
        ctype = str(resp.headers.get("content-type") or "text/html")
        return FetchResult(text=text, backend="http", url=str(resp.geturl() or url), status_code=status, content_type=ctype)


def _fetch_curl_cffi(url: str, timeout_sec: int, headers: dict | None) -> FetchResult:
    try:
        from curl_cffi import requests as curl_requests  # type: ignore
    except Exception as e:
        raise FetchBackendError(f"curl_cffi unavailable: {e}") from e

    resp = curl_requests.get(
        url,
        impersonate="chrome124",
        timeout=max(1, int(timeout_sec)),
        headers=_coerce_headers(headers),
    )
    status = int(getattr(resp, "status_code", 0) or 0)
    if status < 200 or status >= 400:
        raise FetchBackendError(f"curl_cffi returned status {status} for {url}")
    ctype = str(resp.headers.get("content-type") or "text/html")
    return FetchResult(text=str(resp.text or ""), backend="curl_cffi", url=str(getattr(resp, "url", url)), status_code=status, content_type=ctype)


def _fetch_playwright_cli(url: str, timeout_sec: int, headers: dict | None) -> FetchResult:
    script_path = BASE / "scripts" / "playwright_fetch.mjs"
    if not script_path.exists():
        raise FetchBackendError(f"missing Playwright script: {script_path}")

    args = [
        "node",
        str(script_path),
        "--url",
        str(url),
        "--timeout-ms",
        str(max(1000, int(timeout_sec) * 1000)),
    ]
    ua = _coerce_headers(headers).get("User-Agent")
    if ua:
        args.extend(["--user-agent", ua])

    cp = subprocess.run(args, capture_output=True, text=True, timeout=max(5, int(timeout_sec) + 10))
    if cp.returncode != 0:
        stderr = (cp.stderr or cp.stdout or "").strip()
        raise FetchBackendError(f"playwright_cli failed: {stderr[:500]}")

    try:
        payload = json.loads(cp.stdout)
    except Exception as e:
        raise FetchBackendError(f"playwright_cli invalid JSON: {e}") from e

    if not isinstance(payload, dict):
        raise FetchBackendError("playwright_cli returned unexpected payload")
    if not bool(payload.get("ok")):
        raise FetchBackendError(str(payload.get("error") or "playwright_cli returned ok=false"))

    html = str(payload.get("html") or "")
    if not html:
        raise FetchBackendError("playwright_cli returned empty html")
    status = int(payload.get("status") or 200)
    resolved_url = str(payload.get("url") or url)
    return FetchResult(text=html, backend="playwright_cli", url=resolved_url, status_code=status, content_type="text/html")


def _fetch_openclaw_snapshot(url: str, timeout_sec: int, headers: dict | None) -> FetchResult:
    timeout_ms = max(1000, int(timeout_sec) * 1000)
    start_cp = subprocess.run(
        ["openclaw", "browser", "start", "--browser-profile", "openclaw", "--json"],
        capture_output=True,
        text=True,
        timeout=max(5, timeout_sec + 5),
    )
    if start_cp.returncode != 0:
        raise FetchBackendError((start_cp.stderr or start_cp.stdout or "openclaw start failed").strip())

    open_cp = subprocess.run(
        ["openclaw", "browser", "open", url, "--browser-profile", "openclaw", "--json"],
        capture_output=True,
        text=True,
        timeout=max(5, timeout_sec + 5),
    )
    if open_cp.returncode != 0:
        raise FetchBackendError((open_cp.stderr or open_cp.stdout or "openclaw open failed").strip())

    snap_cp = subprocess.run(
        [
            "openclaw",
            "browser",
            "snapshot",
            "--browser-profile",
            "openclaw",
            "--json",
            "--limit",
            "18000",
        ],
        capture_output=True,
        text=True,
        timeout=max(5, timeout_sec + 10),
    )
    if snap_cp.returncode != 0:
        raise FetchBackendError((snap_cp.stderr or snap_cp.stdout or "openclaw snapshot failed").strip())
    try:
        payload = json.loads(snap_cp.stdout)
    except Exception as e:
        raise FetchBackendError(f"openclaw snapshot invalid json: {e}") from e
    snap = str(payload.get("snapshot") or "")
    if not snap:
        raise FetchBackendError("openclaw snapshot was empty")
    return FetchResult(text=snap, backend="openclaw_snapshot", url=str(payload.get("url") or url), status_code=200, content_type="text/openclaw-snapshot")


_BACKEND_IMPL = {
    "http": _fetch_http,
    "curl_cffi": _fetch_curl_cffi,
    "playwright_cli": _fetch_playwright_cli,
    "openclaw_snapshot": _fetch_openclaw_snapshot,
}


def fetch_with_backends(url: str, backends: list[str], timeout_sec: int = 30, headers: dict | None = None) -> FetchResult:
    candidates = [str(x or "").strip() for x in (backends or []) if str(x or "").strip()]
    if not candidates:
        candidates = ["http"]

    last_error: Exception | None = None
    for backend in candidates:
        impl = _BACKEND_IMPL.get(backend)
        if impl is None:
            last_error = FetchBackendError(f"unknown backend: {backend}")
            continue
        try:
            return impl(url=url, timeout_sec=timeout_sec, headers=headers)
        except Exception as e:
            last_error = e
            continue
    if last_error is None:
        raise FetchBackendError(f"no backends configured for {url}")
    raise FetchBackendError(str(last_error))

