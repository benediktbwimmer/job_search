import math
import re
from collections import defaultdict

STATUS_WEIGHTS = {
    "saved": 1.0,
    "applied": 3.0,
    "interview": 6.0,
    "offer": 9.0,
    "dismissed": -2.0,
    "rejected": -4.0,
    "withdrawn": -3.0,
}

ACTION_WEIGHTS = {
    "viewed": 0.2,
    "clicked": 0.8,
    "saved": 1.5,
    "applied": 3.0,
    "interview": 5.0,
    "offer": 8.0,
    "dismissed": -2.0,
    "rejected": -4.0,
}

STOPWORDS = {
    "and",
    "the",
    "for",
    "with",
    "from",
    "this",
    "that",
    "you",
    "your",
    "our",
    "role",
    "engineer",
    "developer",
    "software",
    "senior",
    "junior",
}


def _normalize(value: str) -> str:
    return str(value or "").strip().lower()


def _tokenize(value: str) -> list[str]:
    tokens = re.findall(r"[a-z0-9][a-z0-9+.#-]{2,}", _normalize(value))
    out = []
    for token in tokens:
        if token in STOPWORDS:
            continue
        out.append(token)
    return out


def _squash(raw: float, limit: int) -> int:
    if raw == 0:
        return 0
    return int(round(limit * math.tanh(raw / max(1.0, limit))))


def build_adaptive_profile(signal_data: dict) -> dict:
    source_scores = defaultdict(float)
    source_type_scores = defaultdict(float)
    company_scores = defaultdict(float)
    token_scores = defaultdict(float)

    samples = 0

    for row in signal_data.get("applications", []):
        weight = STATUS_WEIGHTS.get(_normalize(row.get("status")), 0.0)
        if weight == 0:
            continue
        samples += 1

        source = _normalize(row.get("source"))
        source_type = _normalize(row.get("source_type"))
        company = _normalize(row.get("job_company") or row.get("app_company"))
        title = row.get("job_title") or row.get("app_title") or ""

        if source:
            source_scores[source] += weight
        if source_type:
            source_type_scores[source_type] += weight * 0.7
        if company:
            company_scores[company] += weight
        for token in _tokenize(title):
            token_scores[token] += weight * 0.8

    for row in signal_data.get("feedback", []):
        weight = ACTION_WEIGHTS.get(_normalize(row.get("action")), 0.0)
        if weight == 0:
            continue
        samples += 1

        source = _normalize(row.get("source"))
        source_type = _normalize(row.get("source_type"))
        company = _normalize(row.get("job_company"))
        title = row.get("job_title") or ""

        if source:
            source_scores[source] += weight
        if source_type:
            source_type_scores[source_type] += weight * 0.7
        if company:
            company_scores[company] += weight
        for token in _tokenize(title):
            token_scores[token] += weight * 0.7

    return {
        "samples": samples,
        "source_scores": dict(source_scores),
        "source_type_scores": dict(source_type_scores),
        "company_scores": dict(company_scores),
        "token_scores": dict(token_scores),
    }


def adaptive_bonus_for_job(job: dict, profile: dict) -> tuple[int, list[str]]:
    if not profile or int(profile.get("samples", 0)) <= 0:
        return 0, []

    source = _normalize(job.get("source"))
    source_type = _normalize(job.get("source_type"))
    company = _normalize(job.get("company"))
    title_tokens = _tokenize(job.get("title", ""))

    source_raw = float(profile.get("source_scores", {}).get(source, 0.0)) if source else 0.0
    source_type_raw = float(profile.get("source_type_scores", {}).get(source_type, 0.0)) if source_type else 0.0
    company_raw = float(profile.get("company_scores", {}).get(company, 0.0)) if company else 0.0
    token_raw = 0.0
    for token in title_tokens[:8]:
        token_raw += float(profile.get("token_scores", {}).get(token, 0.0))

    source_bonus = _squash(source_raw, 6)
    source_type_bonus = _squash(source_type_raw, 4)
    company_bonus = _squash(company_raw, 5)
    token_bonus = _squash(token_raw, 8)

    total = source_bonus + source_type_bonus + company_bonus + token_bonus
    total = max(-15, min(15, total))

    reasons = []
    if source_bonus:
        reasons.append(f"source history {source_bonus:+d}")
    if source_type_bonus:
        reasons.append(f"source type history {source_type_bonus:+d}")
    if company_bonus:
        reasons.append(f"company history {company_bonus:+d}")
    if token_bonus:
        reasons.append(f"title history {token_bonus:+d}")

    return total, reasons
