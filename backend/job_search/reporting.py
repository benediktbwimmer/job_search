from datetime import datetime


def _format_salary(salary: dict) -> str:
    if not isinstance(salary, dict) or not salary:
        return ""
    currency = salary.get("currency") or ""
    period = salary.get("period") or ""
    min_amount = salary.get("min_amount")
    max_amount = salary.get("max_amount")
    annual_min_eur = salary.get("annual_min_eur")
    annual_max_eur = salary.get("annual_max_eur")
    raw_text = salary.get("raw_text")

    if min_amount is not None:
        if max_amount is not None:
            label = f"{currency} {int(min_amount):,} - {int(max_amount):,}".strip()
        else:
            label = f"{currency} {int(min_amount):,}".strip()
        if period:
            label += f" per {period}"
    elif raw_text:
        label = str(raw_text)
    else:
        label = ""

    if annual_min_eur is not None:
        if annual_max_eur is not None:
            label += f" (~EUR {int(annual_min_eur):,} - {int(annual_max_eur):,}/year)"
        else:
            label += f" (~EUR {int(annual_min_eur):,}/year)"
    return label.strip()


def markdown_report(jobs_ranked, skipped_applied, errors, now: datetime | None = None):
    now = now or datetime.now()
    now_label = now.strftime("%Y-%m-%d %H:%M")
    a = [j for j in jobs_ranked if j["tier"] == "A"]
    b = [j for j in jobs_ranked if j["tier"] == "B"]
    c = [j for j in jobs_ranked if j["tier"] == "C"]

    lines = []
    lines.append(f"# Job Shortlist — {now_label}")
    lines.append("")
    lines.append(f"Total jobs scanned: **{len(jobs_ranked)}**")
    lines.append(f"- A-tier: **{len(a)}**")
    lines.append(f"- B-tier: **{len(b)}**")
    lines.append(f"- C-tier: **{len(c)}**")
    lines.append(f"- Skipped (already applied): **{skipped_applied}**")
    lines.append(f"- Source errors: **{len(errors)}**")
    lines.append("")

    def emit(section, jobs, limit=20):
        lines.append(f"## {section}")
        if not jobs:
            lines.append("(none)")
            lines.append("")
            return
        for j in jobs[:limit]:
            lines.append(f"### [{j['title']}]({j['url']})")
            lines.append(f"- Score: **{j['score']}** | Tier: **{j['tier']}**")
            if j.get("company"):
                lines.append(f"- Company: {j['company']}")
            lines.append(f"- Source: {j.get('source','')}")
            if j.get("location"):
                lines.append(f"- Location hint: {j['location']}")
            if j.get("llm_summary"):
                lines.append(f"- Why: {j['llm_summary']}")
            else:
                lines.append(f"- Why: {', '.join(j['reasons']) if j['reasons'] else 'general fit'}")
            if j.get("llm_pros"):
                lines.append(f"- LLM pros: {', '.join(j['llm_pros'])}")
            if j.get("llm_risks"):
                lines.append(f"- LLM risks: {', '.join(j['llm_risks'])}")
            if j.get("adaptive_bonus"):
                lines.append(f"- Adaptive: {j['adaptive_bonus']:+d} ({', '.join(j.get('adaptive_reasons', []))})")
            if j.get("salary"):
                formatted_salary = _format_salary(j.get("salary"))
                if formatted_salary:
                    lines.append(f"- Salary: {formatted_salary}")
            if j.get("cv_variant"):
                cv_reasons = ", ".join(j.get("cv_recommendation_reasons", [])[:3])
                if cv_reasons:
                    lines.append(f"- CV variant: {j['cv_variant']} ({cv_reasons})")
                else:
                    lines.append(f"- CV variant: {j['cv_variant']}")
            if j.get("skill_hits"):
                lines.append(f"- Skill hits: {', '.join(j['skill_hits'])}")
            lines.append("")

    emit("A-tier (apply now)", a, 15)
    emit("B-tier (review)", b, 20)
    emit("C-tier (skip for now)", c, 10)

    if errors:
        lines.append("## Source errors")
        for e in errors[:20]:
            lines.append(f"- {e.get('source')}: {e.get('error')}")

    return "\n".join(lines)
