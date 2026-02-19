import hashlib
import html
import json
import re
from pathlib import Path
from urllib.parse import urlparse

from job_search.json_io import save_json
from job_search.llm_scoring import call_openai_json


def _hash_text(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8", errors="ignore")).hexdigest()


_ROLE_WORDS = {
    "engineer",
    "developer",
    "architect",
    "manager",
    "director",
    "specialist",
    "lead",
    "senior",
    "staff",
    "principal",
    "role",
    "position",
    "job",
    "team",
}
_URL_ROLE_MARKERS = _ROLE_WORDS | {
    "frontend",
    "backend",
    "fullstack",
    "full",
    "stack",
    "software",
    "qa",
    "sre",
    "site",
    "reliability",
    "support",
    "technical",
    "project",
    "product",
    "marketing",
    "sales",
    "design",
    "research",
    "analyst",
    "ops",
    "devops",
    "data",
    "security",
    "manager",
    "head",
    "principal",
    "staff",
    "senior",
    "junior",
    "intern",
    "freelance",
    "contract",
    "consultant",
    "specialist",
    "advocate",
    "platform",
    "cloud",
    "java",
    "python",
    "ruby",
    "rails",
    "golang",
    "typescript",
    "react",
    "angular",
    "dotnet",
    "net",
    "ai",
    "ml",
    "of",
    "on",
}
_COMPANY_BAD_PHRASES = (
    "we are",
    "about us",
    "about the role",
    "the role",
    "responsibilities",
    "this role",
    "join us",
    "job description",
    "that partners with",
    "founded in",
    "description",
    "culture that",
    "in this",
    "base salary range",
    "our formula",
    "forums and events",
    "hands-on technical role",
)
_COMPANY_BAD_EXACT = {"us", "overview", "description", "company", "hiring team"}
_STEPSTONE_STOP_TOKENS = {
    "w",
    "m",
    "d",
    "hall",
    "in",
    "tirol",
    "tirolo",
    "innsbruck",
    "remote",
    "austria",
    "deutschland",
}


def _normalized_space(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip())


def _trim_text(text: str, max_chars: int | None) -> str:
    raw = str(text or "")
    if max_chars is None:
        return raw
    try:
        limit = int(max_chars)
    except (TypeError, ValueError):
        return raw
    if limit <= 0:
        return raw
    return raw[:limit]


def _looks_like_snapshot_dump(text: str) -> bool:
    value = str(text or "").lower()
    if len(value) < 120:
        return False
    markers = (
        "[ref=e",
        "[cursor=pointer]",
        "- generic [ref=",
        "- heading \"",
        "- link \"",
        "- /url:",
    )
    return sum(1 for marker in markers if marker in value) >= 2


def _cleanup_snapshot_dump(text: str) -> str:
    value = str(text or "")
    if not _looks_like_snapshot_dump(value):
        return value.strip()
    value = re.sub(r"\[ref=e\d+\]", " ", value)
    value = value.replace("[cursor=pointer]", " ")
    value = re.sub(
        r"-\s*(?:generic|heading|link|button|article|status|time)\s*(?:\[ref=e\d+\])?\s*:\s*",
        " ",
        value,
        flags=re.IGNORECASE,
    )
    value = re.sub(r"-\s*/url:\s*\S+", " ", value, flags=re.IGNORECASE)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def _looks_like_company(text: str) -> bool:
    value = _normalized_space(html.unescape(text))
    if not value:
        return False
    if "<" in value or ">" in value:
        return False
    if len(value) < 3 and not any(ch.isdigit() for ch in value):
        return False
    if len(value) > 120:
        return False
    lower = value.lower()
    if lower in _COMPANY_BAD_EXACT:
        return False
    if any(x in lower for x in _COMPANY_BAD_PHRASES):
        return False
    if lower.startswith(("about ", "role ", "position ", "job ", "your mission")):
        return False
    if lower.startswith(("in ", "our ", "this ", "that ", "with ", "for ")):
        return False
    if re.search(r"\b(is|are|was|were)\b", lower):
        return False
    if "http://" in lower or "https://" in lower:
        return False
    if value.count(".") > 3:
        return False
    if "." in value and " " in value and re.search(r"\.\s+[A-Z]", value):
        return False
    if "." in value and re.search(r"[A-Za-z]\.[A-Za-z]", value):
        return False
    if value.count(",") >= 2:
        return False
    # Filter out obvious sentences/paragraph fragments.
    if sum(value.count(ch) for ch in (".", "!", "?", ";")) >= 2:
        return False

    tokens = re.findall(r"[A-Za-z0-9&'.-]+", value)
    if not tokens or len(tokens) > 8:
        return False
    sentence_stopwords = {"in", "this", "and", "for", "our", "with", "at", "to", "the", "of", "on", "your", "we", "you"}
    if len(tokens) >= 5 and sum(1 for t in tokens if t.lower() in sentence_stopwords) >= 2:
        return False

    role_hits = sum(1 for t in tokens if t.lower() in _ROLE_WORDS)
    if role_hits >= 3:
        return False

    # If all-lowercase and not a domain-like company token, likely description fragment.
    if value[0].islower() and "." not in value and not any(ch.isupper() for ch in value):
        return False
    return True


def _clean_company_candidate(raw: str) -> str:
    value = _normalized_space(html.unescape(raw))
    value = re.sub(r"^(company|employer|about us|about)\s*:\s*", "", value, flags=re.IGNORECASE).strip()
    value = re.sub(r"\s+(is|are|was|were)$", "", value, flags=re.IGNORECASE)
    value = re.sub(
        r"\s+(?:is hiring|is looking for|is seeking|seeks|hiring)\b.*$",
        "",
        value,
        flags=re.IGNORECASE,
    )
    value = re.sub(r"\s+(?:that|which)\b.*$", "", value, flags=re.IGNORECASE)
    value = re.sub(
        r"'s\s+(?:technology|engineering|product|platform|developer|development)\s+team$",
        "",
        value,
        flags=re.IGNORECASE,
    )
    value = re.sub(r"\s+team$", "", value, flags=re.IGNORECASE)
    value = value.strip(" -|,;")
    # If model returns "Company: Role title", keep company part.
    if ":" in value:
        left, right = value.split(":", 1)
        right_words = {x.lower() for x in re.findall(r"[A-Za-z0-9]+", right)}
        if _looks_like_company(left) and (right_words & _ROLE_WORDS):
            value = left.strip()
    return value


def _canonicalize_company(value: str) -> str:
    text = _normalized_space(value)
    if not text:
        return ""

    # Canonicalize host-like companies, e.g. "comparis.ch" -> "Comparis".
    hostish = re.sub(r"^https?://", "", text, flags=re.IGNORECASE).strip("/")
    hostish = hostish.split("/", 1)[0].strip().lower()
    domain_match = re.fullmatch(r"(?:www\.)?([a-z0-9][a-z0-9-]{0,62})(?:\.[a-z0-9-]{2,63})+", hostish)
    if domain_match:
        stem = domain_match.group(1).replace("-", " ").strip()
        if stem:
            return " ".join(part.capitalize() for part in stem.split())

    # Keep existing mixed-case brands; otherwise normalize simple lowercase names.
    if text.lower() == text and " " in text:
        return " ".join(part.capitalize() for part in text.split())
    return text


def _extract_company_from_title(title: str) -> str:
    clean = _normalized_space(html.unescape(title))
    if not clean:
        return ""
    for sep in (":", " - ", " | ", " @ "):
        if sep not in clean:
            continue
        left = clean.split(sep, 1)[0].strip()
        cand = _clean_company_candidate(left)
        if _looks_like_company(cand):
            return cand
    m = re.search(r"\bat\s+([A-Z][A-Za-z0-9&'().,\- ]{1,80})$", clean)
    if m:
        cand = _clean_company_candidate(m.group(1))
        if _looks_like_company(cand):
            return cand
    return ""


def _extract_company_from_description(description: str) -> str:
    text = _normalized_space(html.unescape(description))
    if not text:
        return ""

    patterns = [
        r"\bCompany\s*:\s*([A-Z][A-Za-z0-9&'().,\- ]{1,80})\b",
        r"\bAbout Us:\s*([A-Z][A-Za-z0-9&'().,\-]{1,80})\s+(?:is|are|partners|builds|helps|develops)\b",
        r"\bA Career with\s+([A-Z][A-Za-z0-9&'().,\-]{1,80})(?:'s|\b)",
        r"\b([A-Z][A-Za-z0-9&'().,\- ]{1,80}?)\s+(?:is hiring|is looking for|seeks|is seeking|hiring)\b",
        r"\b([A-Z][A-Za-z0-9&'().,\-]{1,80})\s+is\s+(?:a|an|the)\b",
        r"\bAt\s+([A-Z][A-Za-z0-9&'().,\- ]{1,80}?),\s+(?:we|our|you)\b",
        r"\bJoin\s+([A-Z][A-Za-z0-9&'().,\-]{1,80})\s+(?:as|to|for)\b",
        r"\bAbout\s+([A-Z][A-Za-z0-9&'().\-]{1,40}(?:\s+[A-Z][A-Za-z0-9&'().\-]{1,40}){0,3})\b",
    ]
    for pattern in patterns:
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if not m:
            continue
        cand = _clean_company_candidate(m.group(1))
        if _looks_like_company(cand):
            return cand

    url_match = re.search(
        r"\bURL\s*:\s*https?://(?:www\.)?([A-Za-z0-9-]{2,40})\.(?:com|io|ai|dev|co|org|net)\b",
        text,
        flags=re.IGNORECASE,
    )
    if url_match:
        label = url_match.group(1).replace("-", " ").strip().title()
        cand = _clean_company_candidate(label)
        if _looks_like_company(cand):
            return cand
    return ""


def _extract_company_from_url(url: str, title: str) -> str:
    raw_url = str(url or "").strip()
    if not raw_url:
        return ""
    parsed = urlparse(raw_url)
    host = (parsed.netloc or "").lower()
    slug = (parsed.path or "").strip("/").split("/")[-1]
    if not slug:
        return ""

    title_tokens = {x.lower() for x in re.findall(r"[A-Za-z0-9]+", str(title or ""))}
    slug_tokens = [t for t in slug.lower().split("-") if t]

    # WeWorkRemotely-style slug: <company>-<role...>
    if "weworkremotely.com" in host:
        if slug.startswith("details-"):
            slug_tokens = [t for t in slug_tokens[1:] if t]
        company_tokens = []
        for tok in slug_tokens:
            if tok in _URL_ROLE_MARKERS:
                break
            company_tokens.append(tok)
            if len(company_tokens) >= 4:
                break
        candidate = " ".join(company_tokens).strip()
        candidate = _clean_company_candidate(candidate.title())
        if _looks_like_company(candidate):
            return candidate

    # RemoteOK-style slug: remote-<role...>-<company...>-<id>
    if "remoteok.com" in host and slug_tokens:
        if re.fullmatch(r"\d{4,}", slug_tokens[-1] or ""):
            slug_tokens = slug_tokens[:-1]
        if slug_tokens and slug_tokens[0] == "remote":
            slug_tokens = slug_tokens[1:]
        tail = slug_tokens[-3:] if len(slug_tokens) >= 3 else slug_tokens
        while tail and tail[0] in title_tokens:
            tail = tail[1:]
        if not tail and slug_tokens:
            tail = slug_tokens[-1:]
        candidate = _clean_company_candidate(" ".join(tail).title())
        if _looks_like_company(candidate):
            return candidate

    if "stepstone." in host:
        segments = (parsed.path or "").split("--")
        if len(segments) >= 3:
            middle = str(segments[-2] or "")
            words = [w for w in middle.split("-") if w]
            company_tokens = []
            for token in reversed(words):
                low = token.lower()
                if low in _STEPSTONE_STOP_TOKENS and company_tokens:
                    break
                if re.fullmatch(r"\d+", token):
                    continue
                company_tokens.append(token)
                if len(company_tokens) >= 4:
                    break
            if company_tokens:
                candidate = _clean_company_candidate(" ".join(reversed(company_tokens)))
                if _looks_like_company(candidate):
                    return candidate

    return ""


def _resolve_company(job: dict, llm_company: str, llm_description: str) -> str:
    candidates = [
        _clean_company_candidate(str(job.get("company") or "")),
        _extract_company_from_title(str(job.get("title") or "")),
        _extract_company_from_description(llm_description),
        _extract_company_from_description(str(job.get("description") or "")),
        _extract_company_from_url(str(job.get("url") or ""), str(job.get("title") or "")),
        _clean_company_candidate(llm_company),
    ]
    for cand in candidates:
        normalized = _canonicalize_company(cand)
        if _looks_like_company(normalized):
            return normalized
    return ""


def normalize_llm_parse_output(job: dict, llm_out: dict, description_max_chars: int) -> dict:
    normalized = dict(llm_out or {})
    normalized["title"] = str(normalized.get("title") or "").strip()[:220]
    normalized["company"] = _resolve_company(
        job=job,
        llm_company=str(normalized.get("company") or ""),
        llm_description=str(normalized.get("description") or ""),
    )
    normalized["location"] = str(normalized.get("location") or "").strip()[:180]

    llm_description = str(normalized.get("description") or "").strip()
    raw_description = str(job.get("description") or "").strip()
    if not llm_description:
        llm_description = raw_description
    llm_description = _cleanup_snapshot_dump(llm_description)
    raw_description = _cleanup_snapshot_dump(raw_description)
    raw_noisy = _looks_like_snapshot_dump(str(job.get("description") or ""))
    llm_noisy = _looks_like_snapshot_dump(str(normalized.get("description") or ""))
    # Prefer fuller source text only when the source text is not noisy.
    if len(raw_description) > len(llm_description) and not raw_noisy:
        llm_description = raw_description
    if llm_noisy and raw_description and not raw_noisy:
        llm_description = raw_description
    normalized["description"] = _trim_text(llm_description, description_max_chars)
    normalized["published"] = str(normalized.get("published") or "").strip()[:64]
    normalized["summary"] = str(normalized.get("summary") or "").strip()[:180]
    return normalized


def load_llm_parse_cache(path: Path):
    if not path.exists():
        return {"meta": {"version": 1}, "entries": {}}
    try:
        obj = json.loads(path.read_text())
        if "entries" not in obj:
            return {"meta": {"version": 1}, "entries": {}}
        return obj
    except Exception:
        return {"meta": {"version": 1}, "entries": {}}


def save_llm_parse_cache(path: Path, cache_obj):
    save_json(path, cache_obj)


def llm_parse_cache_key(job: dict, model: str, prompt_version: str, description_chars: int = 6000) -> str:
    description = str(job.get("description") or "")
    clipped_description = _trim_text(description, description_chars)
    stable_blob = json.dumps(
        {
            "source": str(job.get("source") or ""),
            "source_type": str(job.get("source_type") or ""),
            "url": str(job.get("url") or ""),
            "title": str(job.get("title") or ""),
            "company": str(job.get("company") or ""),
            "location": str(job.get("location") or ""),
            "description_hash": _hash_text(clipped_description),
            "description_len": len(clipped_description),
            "published": str(job.get("published") or ""),
            "model": str(model or ""),
            "prompt_version": str(prompt_version or ""),
            "description_chars": int(description_chars) if str(description_chars or "").strip() else 0,
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    return _hash_text(stable_blob)


def _llm_parse_legacy_cache_key(job: dict, model: str, prompt_version: str, description_chars: int = 6000) -> str:
    try:
        input_desc_chars = max(600, min(8000, int(description_chars)))
    except (TypeError, ValueError):
        input_desc_chars = 6000
    stable_blob = json.dumps(
        {
            "source": str(job.get("source") or ""),
            "source_type": str(job.get("source_type") or ""),
            "url": str(job.get("url") or ""),
            "title": str(job.get("title") or ""),
            "company": str(job.get("company") or ""),
            "location": str(job.get("location") or ""),
            "description": str(job.get("description") or "")[:input_desc_chars],
            "published": str(job.get("published") or ""),
            "model": str(model or ""),
            "prompt_version": str(prompt_version or ""),
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    return _hash_text(stable_blob)


def llm_parse_cache_keys(job: dict, model: str, prompt_version: str, description_chars: int = 6000) -> list[str]:
    primary = llm_parse_cache_key(job=job, model=model, prompt_version=prompt_version, description_chars=description_chars)
    legacy = _llm_parse_legacy_cache_key(
        job=job,
        model=model,
        prompt_version=prompt_version,
        description_chars=description_chars,
    )
    if legacy == primary:
        return [primary]
    return [primary, legacy]


def llm_parse_job(
    job: dict,
    profile: dict,
    constraints: dict,
    model: str,
    description_max_chars: int = 2500,
    input_description_max_chars: int = 20000,
) -> dict:
    raw_description = str(job.get("description") or "")
    input_description = _trim_text(raw_description, input_description_max_chars)
    system_prompt = (
        "You are a strict job posting evaluator. "
        "Return ONLY valid JSON with keys: "
        "is_job_posting (boolean), title (string), company (string), location (string), "
        "remote_hint (boolean), description (string), published (string), "
        "score (0-100 integer), tier (A|B|C), reasons (array of short strings), "
        "summary (string max 180 chars), quality_flags (array of short strings), confidence (number 0..1). "
        "Company must be only the company name, never a sentence, role title, or description fragment. "
        "If unknown, return an empty string. "
        "If this item looks like navigation text, recommendation widgets, or mixed/ambiguous listing content, "
        "set is_job_posting=false."
    )

    user_prompt = json.dumps(
        {
            "candidate_profile": {
                "location": profile.get("location"),
                "target_titles": profile.get("target_titles", []),
                "must_have_any": profile.get("must_have_any", []),
                "skills": profile.get("skills", []),
                "preferred_keywords": profile.get("preferred_keywords", []),
                "exclude_keywords": profile.get("exclude_keywords", []),
                "local_first": bool(profile.get("local_first", True)),
            },
            "constraints": constraints,
            "raw_item": {
                "source": str(job.get("source") or ""),
                "source_type": str(job.get("source_type") or ""),
                "url": str(job.get("url") or ""),
                "title": str(job.get("title") or ""),
                "company": str(job.get("company") or ""),
                "location": str(job.get("location") or ""),
                "description": input_description,
                "published": str(job.get("published") or ""),
            },
            "rules": {
                "preserve_truthful_fields": True,
                "avoid_inventing": True,
                "description_max_chars": (int(description_max_chars) if int(description_max_chars) > 0 else "no_limit"),
                "input_description_max_chars": (
                    int(input_description_max_chars) if int(input_description_max_chars) > 0 else "no_limit"
                ),
                "company_rules": {
                    "max_words": 8,
                    "must_not_include_role_words": True,
                    "must_not_be_sentence": True,
                    "if_unsure_return_empty": True,
                },
                "score_policy": {
                    "A": "strong fit and worth applying now",
                    "B": "decent fit, review",
                    "C": "weak fit or skip",
                },
            },
        },
        ensure_ascii=False,
    )

    out = call_openai_json(model=model, system_prompt=system_prompt, user_prompt=user_prompt)

    title = str(out.get("title", "")).strip()[:220]
    company = _resolve_company(job=job, llm_company=str(out.get("company", "")), llm_description=input_description)
    location = str(out.get("location", "")).strip()[:180]
    model_description = str(out.get("description", "")).strip()
    raw_description_clean = str(job.get("description") or "").strip()
    model_description = _cleanup_snapshot_dump(model_description)
    raw_description_clean = _cleanup_snapshot_dump(raw_description_clean)
    description_candidate = model_description or raw_description_clean
    raw_noisy = _looks_like_snapshot_dump(str(job.get("description") or ""))
    model_noisy = _looks_like_snapshot_dump(str(out.get("description") or ""))
    if len(raw_description_clean) > len(description_candidate) and not raw_noisy:
        description_candidate = raw_description_clean
    if model_noisy and raw_description_clean and not raw_noisy:
        description_candidate = raw_description_clean
    description = _trim_text(description_candidate, description_max_chars)
    published = str(out.get("published", "")).strip()[:64]
    quality_flags = [str(x)[:80] for x in out.get("quality_flags", [])[:8]]
    reasons = [str(x)[:120] for x in out.get("reasons", [])[:8]]
    summary = str(out.get("summary", "")).strip()[:180]
    is_job_posting = bool(out.get("is_job_posting", True))
    confidence_raw = out.get("confidence", 0.0)
    try:
        confidence = float(confidence_raw)
    except (TypeError, ValueError):
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))
    try:
        score = int(out.get("score", 0))
    except (TypeError, ValueError):
        score = 0
    score = max(0, min(100, score))
    tier = str(out.get("tier", "")).strip().upper()
    if tier not in {"A", "B", "C"}:
        tier = "A" if score >= 70 else ("B" if score >= 50 else "C")

    if not title:
        title = str(job.get("title") or "").strip()[:220]
    if not description:
        description = _trim_text(raw_description_clean, description_max_chars)

    return {
        "is_job_posting": is_job_posting,
        "title": title,
        "company": company,
        "location": location,
        "remote_hint": bool(out.get("remote_hint", False)),
        "description": description,
        "published": published,
        "score": score,
        "tier": tier,
        "reasons": reasons,
        "summary": summary,
        "quality_flags": quality_flags,
        "confidence": confidence,
    }
