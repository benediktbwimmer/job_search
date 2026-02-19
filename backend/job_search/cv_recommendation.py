import re


def _has_any(text: str, tokens: list[str]) -> bool:
    for token in tokens:
        if token and token in text:
            return True
    return False


def recommend_cv_variant(job: dict, cfg: dict | None = None) -> tuple[str, list[str]]:
    cfg = cfg or {}
    title = str(job.get("title") or "")
    description = str(job.get("description") or "")
    location = str(job.get("location") or "")
    source_type = str(job.get("source_type") or "")
    text = f"{title} {description} {location} {source_type}".lower()

    default_language = str(cfg.get("default_language") or "en").strip().lower()
    if default_language not in {"en", "de"}:
        default_language = "en"

    german_signals = [
        "deutsch",
        "german",
        "österreich",
        "austria",
        "innsbruck",
        "tirol",
        "wien",
        "dach",
    ]
    english_only_signals = ["english only", "fluent english", "business english"]

    language = default_language
    reasons = []
    if _has_any(text, german_signals) and not _has_any(text, english_only_signals):
        language = "de"
        reasons.append("german market signals")
    elif language == "en":
        reasons.append("default to english")
    else:
        reasons.append("default to german")

    seniority_markers = ["senior", "staff", "lead", "principal", "architect"]
    long_score = 0
    if _has_any(title.lower(), seniority_markers):
        long_score += 1
        reasons.append("seniority scope")
    if len(description) >= int(cfg.get("long_description_threshold", 1600)):
        long_score += 1
        reasons.append("long description complexity")
    if len(re.findall(r"\b(kubernetes|terraform|distributed|microservices|platform|architecture)\b", text)) >= 2:
        long_score += 1
        reasons.append("multi-skill emphasis")

    prefer_long_for_senior = bool(cfg.get("prefer_long_for_senior", True))
    if prefer_long_for_senior and _has_any(title.lower(), seniority_markers):
        long_score += 1

    length_variant = "long" if long_score >= 2 else "short"
    if length_variant == "short":
        reasons.append("concise profile sufficient")

    return f"{language}_{length_variant}", reasons[:4]
