import json
from datetime import datetime, timezone

from job_search.llm_scoring import call_openai_json


DEFAULT_COVER_LETTER_MODEL = "gpt-5.2"


def _first_name(full_name: str) -> str:
    parts = [p for p in str(full_name or "").strip().split(" ") if p]
    return parts[0] if parts else "Candidate"


def _language_from_variant(cv_variant: str) -> str:
    token = str(cv_variant or "").strip().lower()
    if token.startswith("de_"):
        return "de"
    return "en"


def _string_list(items, limit: int = 50, item_max_len: int = 120) -> list[str]:
    out: list[str] = []
    for item in items or []:
        text = str(item or "").strip()
        if not text:
            continue
        out.append(text[:item_max_len])
        if len(out) >= limit:
            break
    return out


def _sanitize_experience_highlights(profile: dict) -> list[dict]:
    raw = profile.get("experience_highlights", [])
    if not isinstance(raw, list):
        return []

    highlights: list[dict] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        company = str(item.get("company") or "").strip()[:160]
        role = str(item.get("role") or "").strip()[:180]
        summary = str(item.get("summary") or "").strip()[:500]
        impact = str(item.get("impact") or "").strip()[:350]
        technologies = _string_list(item.get("technologies", []), limit=20, item_max_len=60)
        keywords = _string_list(item.get("keywords", []), limit=20, item_max_len=60)

        if not any([company, role, summary, impact, technologies, keywords]):
            continue

        highlights.append(
            {
                "company": company,
                "role": role,
                "summary": summary,
                "impact": impact,
                "technologies": technologies,
                "keywords": keywords,
            }
        )
        if len(highlights) >= 20:
            break

    return highlights


def _match_experience_highlights(job: dict, highlights: list[dict]) -> list[dict]:
    if not highlights:
        return []

    job_company = str(job.get("company") or "").strip().lower()
    job_text = " ".join(
        [
            str(job.get("title") or ""),
            str(job.get("company") or ""),
            str(job.get("description") or "")[:6000],
        ]
    ).lower()

    matched: list[dict] = []
    for h in highlights:
        company = str(h.get("company") or "").strip().lower()
        technologies = [str(x).strip().lower() for x in h.get("technologies", []) if str(x).strip()]
        keywords = [str(x).strip().lower() for x in h.get("keywords", []) if str(x).strip()]

        company_match = bool(company and job_company and (company in job_company or job_company in company))
        tech_match = any(token and token in job_text for token in technologies)
        keyword_match = any(token and token in job_text for token in keywords)
        if company_match or tech_match or keyword_match:
            matched.append(h)
        if len(matched) >= 6:
            break
    return matched


def _build_auto_context(matched_highlights: list[dict]) -> str:
    if not matched_highlights:
        return ""
    lines = []
    for h in matched_highlights[:4]:
        parts = []
        company = str(h.get("company") or "").strip()
        role = str(h.get("role") or "").strip()
        summary = str(h.get("summary") or "").strip()
        impact = str(h.get("impact") or "").strip()
        technologies = ", ".join([str(x) for x in h.get("technologies", [])[:8]])
        if company:
            parts.append(f"company: {company}")
        if role:
            parts.append(f"role: {role}")
        if summary:
            parts.append(f"summary: {summary}")
        if impact:
            parts.append(f"impact: {impact}")
        if technologies:
            parts.append(f"technologies: {technologies}")
        if parts:
            lines.append("- " + "; ".join(parts))
    if not lines:
        return ""
    return "Relevant prior experience to emphasize:\n" + "\n".join(lines)


def _template_cover_letter(profile: dict, job: dict, cv_variant: str = "en_short", style: str = "concise") -> dict:
    language = _language_from_variant(cv_variant)
    name = str(profile.get("name") or "Candidate")
    location = str(profile.get("location") or "")
    title = str(job.get("title") or "Software Engineer")
    company = str(job.get("company") or "Hiring Team")
    skills = [str(x).strip() for x in profile.get("skills", []) if str(x).strip()][:6]
    keywords = [str(x).strip() for x in profile.get("preferred_keywords", []) if str(x).strip()][:3]
    skill_line = ", ".join(skills) if skills else "software engineering"
    keyword_line = ", ".join(keywords) if keywords else "platform work"

    if language == "de":
        body = (
            f"Sehr geehrtes {company}-Team,\n\n"
            f"ich bewerbe mich für die Position \"{title}\". "
            f"Ich bringe Erfahrung in {skill_line} mit und habe in den letzten Jahren "
            f"skalierbare Systeme mit Fokus auf {keyword_line} umgesetzt.\n\n"
            f"Besonders relevant für diese Rolle ist meine Fähigkeit, komplexe Anforderungen schnell "
            f"in robuste, wartbare Lösungen zu übersetzen und dabei eng mit Produkt und Betrieb zusammenzuarbeiten. "
            f"Mit Standort {location} kann ich effizient im europäischen Umfeld zusammenarbeiten.\n\n"
            f"Ich freue mich auf ein Gespräch über Ihren Bedarf und darüber, wie ich Ihr Team unterstützen kann.\n\n"
            f"Mit freundlichen Grüßen\n{name}"
        )
    else:
        body = (
            f"Dear {company} team,\n\n"
            f"I am applying for the \"{title}\" role. "
            f"I bring hands-on experience across {skill_line}, and I have delivered production systems "
            f"with a strong focus on {keyword_line}.\n\n"
            f"For this role, my main value is turning complex requirements into reliable and maintainable solutions "
            f"while collaborating closely with product and operations. "
            f"Based in {location}, I can work effectively across European time zones.\n\n"
            f"I would value the chance to discuss your needs and how I can contribute.\n\n"
            f"Best regards,\n{name}"
        )

    if style == "detailed":
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if language == "de":
            body += f"\n\nKurzprofil ({timestamp}): Schwerpunkt auf Backend/Plattform, Cloud-Infrastruktur und Teamarbeit."
        else:
            body += f"\n\nProfile note ({timestamp}): Focused on backend/platform delivery, cloud infrastructure, and team execution."

    return {"language": language, "style": style, "cv_variant": cv_variant, "body": body}


def generate_cover_letter(
    profile: dict,
    job: dict,
    cv_variant: str = "en_short",
    style: str = "concise",
    additional_context: str = "",
    model: str = DEFAULT_COVER_LETTER_MODEL,
) -> dict:
    template = _template_cover_letter(profile=profile, job=job, cv_variant=cv_variant, style=style)
    language = template["language"]
    style_token = str(style or "concise").strip().lower() or "concise"
    if style_token not in {"concise", "detailed"}:
        style_token = "concise"
    model_name = str(model or DEFAULT_COVER_LETTER_MODEL).strip() or DEFAULT_COVER_LETTER_MODEL

    highlights = _sanitize_experience_highlights(profile=profile)
    matched_highlights = _match_experience_highlights(job=job, highlights=highlights)
    auto_context = _build_auto_context(matched_highlights=matched_highlights)
    extra_context = str(additional_context or "").strip()[:2000]
    combined_context = extra_context
    if auto_context:
        combined_context = f"{combined_context}\n\n{auto_context}".strip() if combined_context else auto_context

    system_prompt = (
        "You write high-quality job application cover letters. "
        "Return ONLY valid JSON with keys: body (string), language (en|de), style (concise|detailed). "
        "Do not invent facts, years, employers, or achievements that are not provided. "
        "Use concrete but truthful wording and keep a professional tone. "
        "If extra applicant context or matched prior experience is provided, prioritize it when relevant."
    )
    user_prompt = json.dumps(
        {
            "target_language": language,
            "style": style_token,
            "candidate_profile": {
                "name": str(profile.get("name") or "Candidate"),
                "first_name": _first_name(profile.get("name") or "Candidate"),
                "location": str(profile.get("location") or ""),
                "target_titles": _string_list(profile.get("target_titles", []), limit=15, item_max_len=90),
                "skills": _string_list(profile.get("skills", []), limit=60, item_max_len=80),
                "preferred_keywords": [
                    str(x).strip() for x in profile.get("preferred_keywords", []) if str(x).strip()
                ][:20],
                "experience_highlights": highlights[:12],
            },
            "job": {
                "title": str(job.get("title") or "Software Engineer"),
                "company": str(job.get("company") or "Hiring Team"),
                "location": str(job.get("location") or ""),
                "url": str(job.get("url") or ""),
                "description": str(job.get("description") or "")[:5000],
            },
            "requirements": {
                "max_words_concise": 180,
                "max_words_detailed": 280,
                "structure": ["greeting", "fit_summary", "role_relevance", "close"],
                "close_with_name": True,
                "must_prioritize_matched_experience": bool(matched_highlights),
            },
            "matched_experience_highlights": matched_highlights[:6],
            "additional_context": combined_context[:4000],
            "fallback_template": template["body"],
        },
        ensure_ascii=False,
    )

    out = call_openai_json(model=model_name, system_prompt=system_prompt, user_prompt=user_prompt, timeout_sec=60)
    body = str(out.get("body") or "").strip().replace("\r\n", "\n")
    if not body:
        raise ValueError("cover letter generation returned an empty body")

    out_language = str(out.get("language") or language).strip().lower()
    out_style = str(out.get("style") or style_token).strip().lower()
    if out_language not in {"en", "de"}:
        out_language = language
    if out_style not in {"concise", "detailed"}:
        out_style = style_token

    return {
        "language": out_language,
        "style": out_style,
        "cv_variant": cv_variant,
        "body": body[:7000],
        "model": model_name,
    }
