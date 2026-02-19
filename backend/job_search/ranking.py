import re


def _normalize_company_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").strip().lower())


def _watchlist_match(job: dict, watchlist_cfg: dict) -> tuple[bool, str]:
    if not isinstance(watchlist_cfg, dict) or not watchlist_cfg.get("enabled", False):
        return False, ""

    company = str(job.get("company") or "")
    title = str(job.get("title") or "")
    url = str(job.get("url") or "").lower()
    source = str(job.get("source") or "").lower()

    company_token = _normalize_company_token(company)
    text_token = _normalize_company_token(f"{company} {title} {source}")
    companies = [_normalize_company_token(x) for x in watchlist_cfg.get("companies", []) if str(x).strip()]
    for token in companies:
        if token and (token in company_token or token in text_token):
            return True, f"company:{token}"

    domains = [str(x).strip().lower() for x in watchlist_cfg.get("domains", []) if str(x).strip()]
    for domain in domains:
        if domain and domain in url:
            return True, f"domain:{domain}"

    return False, ""


def is_geo_compatible(text: str, source_type: str, constraints: dict):
    text_l = text.lower()
    target_loc = any(k in text_l for k in constraints.get("target_location_keywords", []))
    has_remote = "remote" in text_l or source_type == "remote"

    disallowed = [m for m in constraints.get("disallowed_remote_markers", []) if m in text_l]
    if disallowed:
        return False, "geo restricted remote"

    if any(x in text_l for x in constraints.get("exclude_if_contains", [])):
        return False, "explicit exclusion marker"

    if target_loc:
        return True, "target location"

    if has_remote:
        return True, "remote"

    if constraints.get("require_remote_or_target_location", True):
        return False, "not remote and outside target location"

    return True, "allowed"


def skill_in_text(skill: str, text: str) -> bool:
    s = (skill or "").strip().lower()
    if not s:
        return False

    if s == "c++":
        return bool(re.search(r"(?<![a-z0-9])(c\+\+|cpp)(?![a-z0-9])", text))
    if s == "c#":
        return bool(re.search(r"(?<![a-z0-9])(c#|csharp)(?![a-z0-9])", text))
    if s == "go":
        if re.search(r"\bgolang\b", text):
            return True
        if re.search(r"\bgo(?:\s|-)?to(?:\s|-)?market\b", text):
            return False
        return bool(re.search(r"(?<![a-z0-9])go(?![a-z0-9])", text))

    pattern = r"(?<![a-z0-9])" + re.escape(s) + r"(?![a-z0-9])"
    return bool(re.search(pattern, text))


def score_job(job, profile, constraints, watchlist_cfg: dict | None = None):
    title = (job.get("title") or "").lower()
    desc = (job.get("description") or "").lower()
    loc = (job.get("location") or "").lower()
    source_type = (job.get("source_type") or "").lower()
    text = " ".join([title, desc, loc])

    score = 0
    reasons = []

    ok_geo, geo_reason = is_geo_compatible(text, source_type, constraints)
    if not ok_geo:
        return 0, "C", [geo_reason], []

    reasons.append(geo_reason)

    seniority_hit = any(k in title for k in profile["must_have_any"])
    if seniority_hit:
        score += 8
        reasons.append("seniority match")

    target_role_hit = any(t.lower().split()[0] in title for t in profile["target_titles"])
    if target_role_hit:
        score += 8
        reasons.append("target role")

    skill_hits = [s for s in profile["skills"] if skill_in_text(s, text)]
    if skill_hits:
        score += min(40, len(skill_hits) * 3)
        reasons.append(f"skills ({len(skill_hits)})")

    pref_hits = [k for k in profile["preferred_keywords"] if k in text]
    if pref_hits:
        score += min(15, len(pref_hits) * 3)
        reasons.append("domain fit")

    is_local = any(k in text for k in constraints.get("target_location_keywords", []))
    if is_local:
        score += 35 if constraints.get("prefer_local_strong", False) else 20
        reasons.append("target geography")
        if not seniority_hit:
            score += 10
            reasons.append("local non-senior accepted")
    elif "remote" in text or source_type == "remote" or job.get("remote_hint"):
        score += 12
        reasons.append("remote fit")
        if any(k in text for k in constraints.get("preferred_remote_regions", [])):
            score += 5
            reasons.append("timezone/region fit")
        else:
            score -= 8
            reasons.append("remote geography unclear")

    excluded_level = any(x in text for x in profile["exclude_keywords"])
    if excluded_level:
        score -= 50
        reasons.append("excluded level")

    watchlist_hit, watchlist_reason = _watchlist_match(job, watchlist_cfg or {})
    if watchlist_hit:
        boost = int((watchlist_cfg or {}).get("score_boost", 10))
        boost = max(0, min(30, boost))
        if boost > 0:
            score += boost
            reasons.append("company watchlist")
            if watchlist_reason:
                reasons.append(watchlist_reason)

    if "onsite" in text and not any(k in text for k in constraints.get("target_location_keywords", [])):
        score -= 25
        reasons.append("onsite outside target area")

    if is_local and not seniority_hit and not excluded_level:
        if re.search(r"\b(developer|engineer|software|backend|frontend|fullstack|full-stack)\b", title):
            if score < 55:
                score = 55
                reasons.append("local-first role floor")

    score = max(0, min(100, score))
    tier = "A" if score >= 70 else ("B" if score >= 50 else "C")
    return score, tier, reasons, skill_hits[:8]
