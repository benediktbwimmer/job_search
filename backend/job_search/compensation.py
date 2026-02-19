import re


_CURRENCY_BY_TOKEN = {
    "€": "EUR",
    "eur": "EUR",
    "$": "USD",
    "usd": "USD",
    "£": "GBP",
    "gbp": "GBP",
    "chf": "CHF",
}

_FX_TO_EUR = {
    "EUR": 1.0,
    "USD": 0.92,
    "GBP": 1.17,
    "CHF": 1.03,
}

_PERIOD_TO_YEAR = {
    "year": 1.0,
    "month": 12.0,
    "week": 52.0,
    "day": 260.0,
    "hour": 2080.0,
}

_SALARY_PATTERN = re.compile(
    r"(?P<currency>€|\$|£|eur|usd|gbp|chf)?\s*"
    r"(?P<min>\d{2,3}(?:[,\s]\d{3})+|\d{2,6})\s*(?P<k1>[kK])?"
    r"(?:\s*(?:-|to|–|—)\s*(?P<currency2>€|\$|£|eur|usd|gbp|chf)?\s*(?P<max>\d{2,3}(?:[,\s]\d{3})+|\d{2,6})\s*(?P<k2>[kK])?)?"
    r"\s*(?P<period>per\s+year|/year|annually|annual|per\s+month|/month|monthly|per\s+week|/week|weekly|per\s+day|/day|daily|per\s+hour|/hour|hourly)?",
    flags=re.IGNORECASE,
)


def _clean_amount(raw: str, has_k: bool) -> float | None:
    value = str(raw or "").strip().replace(",", "").replace(" ", "")
    if not value:
        return None
    try:
        amount = float(value)
    except ValueError:
        return None
    if has_k:
        amount *= 1000.0
    return amount


def _normalize_period(raw: str) -> str | None:
    token = str(raw or "").strip().lower()
    if not token:
        return None
    if "year" in token or "annual" in token:
        return "year"
    if "month" in token:
        return "month"
    if "week" in token:
        return "week"
    if "day" in token:
        return "day"
    if "hour" in token:
        return "hour"
    return None


def _annualized_eur(amount: float, currency: str | None, period: str | None) -> float | None:
    if amount is None:
        return None
    fx = _FX_TO_EUR.get((currency or "").upper())
    period_factor = _PERIOD_TO_YEAR.get(period or "")
    if fx is None or period_factor is None:
        return None
    return round(amount * fx * period_factor, 2)


def _extract_currency(group_value: str | None, text: str) -> str | None:
    token = (group_value or "").strip().lower()
    if token:
        return _CURRENCY_BY_TOKEN.get(token)
    text_l = text.lower()
    for key, curr in _CURRENCY_BY_TOKEN.items():
        if key.isalpha() and re.search(rf"(?<![a-z0-9]){re.escape(key)}(?![a-z0-9])", text_l):
            return curr
    return None


def extract_salary_info(job: dict) -> dict:
    title = str(job.get("title") or "")
    description = str(job.get("description") or "")
    text = f"{title} {description}".strip()
    if not text:
        return {}

    best = None
    for match in _SALARY_PATTERN.finditer(text):
        period = _normalize_period(match.group("period"))
        currency_token = match.group("currency") or match.group("currency2")
        currency = _extract_currency(currency_token, text)
        raw_min = _clean_amount(match.group("min"), bool(match.group("k1")))
        raw_max = _clean_amount(match.group("max"), bool(match.group("k2")))
        if raw_min is None:
            continue
        if raw_max is not None and raw_max < raw_min:
            raw_min, raw_max = raw_max, raw_min

        # Ignore likely non-salary numbers unless period/currency makes intent explicit.
        if period is None and currency is None and raw_min < 1000:
            continue

        annual_min = _annualized_eur(raw_min, currency, period)
        annual_max = _annualized_eur(raw_max, currency, period) if raw_max is not None else None
        candidate = {
            "currency": currency,
            "period": period,
            "min_amount": int(raw_min),
            "max_amount": (int(raw_max) if raw_max is not None else None),
            "annual_min_eur": (int(annual_min) if annual_min is not None else None),
            "annual_max_eur": (int(annual_max) if annual_max is not None else None),
            "raw_text": match.group(0).strip(),
        }

        if not best:
            best = candidate
            continue
        best_annual = best.get("annual_max_eur") or best.get("annual_min_eur") or 0
        cand_annual = candidate.get("annual_max_eur") or candidate.get("annual_min_eur") or 0
        if cand_annual > best_annual:
            best = candidate

    return best or {}


def salary_meets_threshold(salary_info: dict, min_annual_eur: int) -> bool | None:
    if not salary_info:
        return None
    annual = salary_info.get("annual_min_eur")
    if annual is None:
        return None
    return int(annual) >= int(min_annual_eur)
