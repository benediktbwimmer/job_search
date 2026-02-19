import hashlib
import json
import os
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from job_search.json_io import save_json


def _hash_text(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8", errors="ignore")).hexdigest()


def load_llm_cache(path: Path):
    if not path.exists():
        return {"meta": {"version": 1}, "entries": {}}
    try:
        obj = json.loads(path.read_text())
        if "entries" not in obj:
            obj = {"meta": {"version": 1}, "entries": {}}
        return obj
    except Exception:
        return {"meta": {"version": 1}, "entries": {}}


def save_llm_cache(path: Path, cache_obj):
    save_json(path, cache_obj)


def llm_cache_key(job: dict, profile: dict, constraints: dict, model: str, prompt_version: str) -> str:
    stable_blob = json.dumps(
        {
            "url": job.get("url", ""),
            "title": job.get("title", ""),
            "company": job.get("company", ""),
            "location": job.get("location", ""),
            "description": (job.get("description", "") or "")[:4000],
            "profile": {
                "target_titles": profile.get("target_titles", []),
                "skills": profile.get("skills", []),
                "preferred_keywords": profile.get("preferred_keywords", []),
                "location": profile.get("location", ""),
                "local_first": profile.get("local_first", True),
            },
            "constraints": constraints,
            "model": model,
            "prompt_version": prompt_version,
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    return _hash_text(stable_blob)


def _resolve_openai_api_key() -> str:
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if api_key:
        return api_key

    env_path = Path.home() / ".openclaw" / ".env"
    if not env_path.exists():
        return ""

    try:
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line.startswith("OPENAI_API_KEY="):
                return line.split("=", 1)[1].strip()
    except Exception:
        return ""
    return ""


def call_openai_json(model: str, system_prompt: str, user_prompt: str, timeout_sec: int = 45, max_retries: int = 2):
    api_key = _resolve_openai_api_key()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is missing")

    payload = {
        "model": model,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }

    req = Request(
        "https://api.openai.com/v1/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    data = None
    for attempt in range(max(0, int(max_retries)) + 1):
        try:
            with urlopen(req, timeout=timeout_sec) as resp:
                data = json.loads(resp.read().decode("utf-8", errors="ignore"))
            break
        except HTTPError as e:
            retriable = int(getattr(e, "code", 0)) in {429, 500, 502, 503, 504}
            if not retriable or attempt >= int(max_retries):
                raise
            retry_after = str(e.headers.get("Retry-After") or "").strip() if getattr(e, "headers", None) else ""
            try:
                delay = float(retry_after) if retry_after else 1.5 * (2**attempt)
            except ValueError:
                delay = 1.5 * (2**attempt)
            time.sleep(min(12.0, max(0.25, delay)))
        except Exception:
            if attempt >= int(max_retries):
                raise
            time.sleep(min(12.0, 1.5 * (2**attempt)))

    if not isinstance(data, dict):
        raise RuntimeError("invalid openai response payload")
    content = data["choices"][0]["message"]["content"]
    return json.loads(content)


def llm_score_job(job: dict, profile: dict, constraints: dict, model: str):
    system_prompt = (
        "You are a strict job-fit scoring engine. "
        "Return ONLY valid JSON with keys: score (0-100 integer), tier (A|B|C), summary (max 180 chars), "
        "pros (array of short strings), risks (array of short strings). "
        "Scoring intent: user prefers local Innsbruck/Tirol/Austria roles and is open to non-senior local jobs. "
        "Remote roles are fine but lower priority than good local roles. Penalize non-engineering roles."
    )

    user_prompt = json.dumps(
        {
            "candidate_profile": {
                "location": profile.get("location"),
                "target_titles": profile.get("target_titles", []),
                "skills": profile.get("skills", []),
                "preferred_keywords": profile.get("preferred_keywords", []),
                "local_first": profile.get("local_first", True),
                "must_have_any": profile.get("must_have_any", []),
            },
            "constraints": constraints,
            "job": {
                "title": job.get("title", ""),
                "company": job.get("company", ""),
                "location": job.get("location", ""),
                "source": job.get("source", ""),
                "url": job.get("url", ""),
                "description": (job.get("description", "") or "")[:5000],
            },
            "scoring_policy": {
                "A": "strong fit and worth applying now",
                "B": "decent fit, review",
                "C": "weak fit or skip",
            },
        },
        ensure_ascii=False,
    )

    out = call_openai_json(model=model, system_prompt=system_prompt, user_prompt=user_prompt)
    score = int(out.get("score", 0))
    score = max(0, min(100, score))
    tier = str(out.get("tier", "C")).strip().upper()
    if tier not in {"A", "B", "C"}:
        tier = "A" if score >= 70 else ("B" if score >= 50 else "C")
    return {
        "score": score,
        "tier": tier,
        "summary": str(out.get("summary", "")).strip()[:180],
        "pros": [str(x)[:120] for x in out.get("pros", [])[:5]],
        "risks": [str(x)[:120] for x in out.get("risks", [])[:5]],
    }
