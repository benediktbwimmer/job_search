import json
import html as html_lib
import random
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

from job_search.fetch_backends import FetchBackendError, fetch_with_backends


def fetch_url(url: str, timeout: int = 20) -> str:
    result = fetch_with_backends(url=url, backends=["http"], timeout_sec=max(1, int(timeout)))
    return str(result.text or "")


def strip_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text or "")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def guess_company(title: str, desc: str) -> str:
    m = re.search(r" at ([A-Za-z0-9&.,\- ]+)", title, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()
    m2 = re.search(r"company[:\s]+([A-Za-z0-9&.,\- ]+)", desc, flags=re.IGNORECASE)
    return m2.group(1).strip() if m2 else ""


def guess_location(text: str) -> str:
    pats = [
        r"\b(Innsbruck|Tyrol|Tirol|Austria|Österreich|Vienna|Wien|Europe|EU|Germany|Deutschland|CET|CEST|Hall in Tirol|Kufstein|Wattens|Schwaz)\b",
        r"\b(remote|Home-Office|home office)\b",
    ]
    hits = []
    for p in pats:
        hits.extend(re.findall(p, text, flags=re.IGNORECASE))
    return ", ".join(sorted(set(h.strip() for h in hits if h)))


def guess_remote(text: str) -> bool:
    return bool(
        re.search(
            r"\b(remote|fully remote|work from anywhere|distributed|home office|home-office)\b",
            text,
            flags=re.IGNORECASE,
        )
    )


def parse_rss(xml_text: str, source_name: str, source_type: str):
    out = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return out

    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        desc = strip_html(item.findtext("description") or "")
        pub = (item.findtext("pubDate") or "").strip()
        guid = (item.findtext("guid") or link or title).strip()
        text = f"{title} {desc}"

        out.append(
            {
                "id": f"{source_name}:{guid}"[:500],
                "source": source_name,
                "source_type": source_type,
                "title": title,
                "company": guess_company(title, desc),
                "location": guess_location(text),
                "remote_hint": guess_remote(text),
                "url": link,
                "description": desc,
                "published": pub,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
        )
    return out


def parse_karriere_html(html_text: str, source_name: str, source_type: str):
    out = []
    hrefs = re.findall(r'href="([^"]+)"', html_text)

    def _extract_karriere_job_id(href: str) -> str:
        target = str(href or "").strip()
        patterns = [
            r"^https?://(?:www\.)?karriere\.at/jobs/(\d{6,8})(?:[/?#]|$)",
            r"^/jobs/(\d{6,8})(?:[/?#]|$)",
            r"^https?://(?:www\.)?karriere\.at/jobs/[^/?#]+/(\d{6,8})(?:[/?#]|$)",
            r"^/jobs/[^/?#]+/(\d{6,8})(?:[/?#]|$)",
        ]
        for pattern in patterns:
            m = re.match(pattern, target)
            if m:
                return str(m.group(1))
        return ""

    seen = set()
    for href in hrefs:
        job_id = _extract_karriere_job_id(href)
        if not job_id:
            continue
        if job_id in seen:
            continue
        seen.add(job_id)
        url = f"https://www.karriere.at/jobs/{job_id}"
        out.append(
            {
                "id": f"{source_name}:{job_id}",
                "source": source_name,
                "source_type": source_type,
                "title": f"Karriere.at listing {job_id}",
                "company": "",
                "location": source_type,
                "remote_hint": False,
                "url": url,
                "description": "",
                "published": "",
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
        )
    return out


def greenhouse_jobs_url(board: str) -> str:
    token = str(board or "").strip().lower()
    if not token:
        raise ValueError("greenhouse board is required")
    return f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true"


def lever_jobs_url(company: str) -> str:
    token = str(company or "").strip().lower()
    if not token:
        raise ValueError("lever company is required")
    return f"https://api.lever.co/v0/postings/{token}?mode=json"


def _parse_epoch_millis(value) -> str:
    try:
        millis = int(value)
    except (TypeError, ValueError):
        return ""
    if millis <= 0:
        return ""
    return datetime.fromtimestamp(millis / 1000.0, tz=timezone.utc).isoformat()


def parse_greenhouse_jobs(json_text: str, source_name: str, source_type: str, company_hint: str = ""):
    out = []
    try:
        payload = json.loads(json_text)
    except Exception:
        return out
    jobs = payload.get("jobs", []) if isinstance(payload, dict) else []
    if not isinstance(jobs, list):
        return out

    for row in jobs:
        if not isinstance(row, dict):
            continue
        title = str(row.get("title") or "").strip()
        link = str(row.get("absolute_url") or row.get("url") or "").strip()
        content = strip_html(str(row.get("content") or ""))
        location_obj = row.get("location") if isinstance(row.get("location"), dict) else {}
        location = str(location_obj.get("name") or "").strip()
        updated_at = str(row.get("updated_at") or row.get("created_at") or "").strip()
        company = str(company_hint or row.get("company_name") or "").strip()
        if not company:
            company = guess_company(title, content)

        text = " ".join([title, content, location, source_name, company])
        job_id = str(row.get("id") or link or title).strip()
        if not job_id or not link:
            continue

        out.append(
            {
                "id": f"{source_name}:{job_id}"[:500],
                "source": source_name,
                "source_type": source_type,
                "title": title,
                "company": company,
                "location": location or guess_location(text),
                "remote_hint": guess_remote(text),
                "url": link,
                "description": content,
                "published": updated_at,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
        )
    return out


def parse_lever_jobs(json_text: str, source_name: str, source_type: str, company_hint: str = ""):
    out = []
    try:
        payload = json.loads(json_text)
    except Exception:
        return out
    jobs = payload if isinstance(payload, list) else []

    for row in jobs:
        if not isinstance(row, dict):
            continue
        title = str(row.get("text") or "").strip()
        link = str(row.get("hostedUrl") or row.get("applyUrl") or "").strip()
        desc_plain = str(row.get("descriptionPlain") or row.get("description") or "")
        description = strip_html(desc_plain)
        categories = row.get("categories") if isinstance(row.get("categories"), dict) else {}
        location = str(categories.get("location") or "").strip()
        team = str(categories.get("team") or "").strip()
        commitment = str(categories.get("commitment") or "").strip()
        workplace_type = str(row.get("workplaceType") or "")
        company = str(company_hint or "").strip()
        if not company:
            company = guess_company(title, description)

        text = " ".join([title, description, location, team, commitment, workplace_type, source_name, company])
        job_id = str(row.get("id") or link or title).strip()
        if not job_id or not link:
            continue

        out.append(
            {
                "id": f"{source_name}:{job_id}"[:500],
                "source": source_name,
                "source_type": source_type,
                "title": title,
                "company": company,
                "location": location or guess_location(text),
                "remote_hint": guess_remote(text),
                "url": link,
                "description": description,
                "published": _parse_epoch_millis(row.get("createdAt")) or _parse_epoch_millis(row.get("updatedAt")),
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
        )
    return out


_STEPSTONE_JOB_URL_RE = re.compile(r"/url: (/stellenangebote--[^\s]+)")
_STEPSTONE_TITLE_RE = re.compile(r'- heading "([^"]+)" \[level=2\]')
_STEPSTONE_COMPANY_IMG_RE = re.compile(r'- img "([^"]+)" \[ref=[^\]]+\]')
_STEPSTONE_TIME_RE = re.compile(r"- time \[ref=[^\]]+\]:\s*([^\n]+)")
_STEPSTONE_DESC_QUOTED_RE = re.compile(r'- generic \[ref=[^\]]+\]:\s*"([^"\n]{80,2400})"')
_STEPSTONE_DESC_TEXT_RE = re.compile(r"- text: ([^\n]{80,2400})")
_STEPSTONE_NOISE_TITLE_RE = re.compile(r"^(erscheinungsdatum|jobs finden)$", flags=re.IGNORECASE)
_STEPSTONE_LOCATION_STOPWORDS = {"schnelle bewerbung", "show more"}
_STEPSTONE_DETAIL_HEADING_RE = re.compile(r'- heading "([^"]+)" \[level=(3|4)\]')
_STEPSTONE_DETAIL_PARAGRAPH_RE = re.compile(r"- paragraph(?: \[ref=[^\]]+\])?:\s*(.+)")
_STEPSTONE_DETAIL_LISTITEM_RE = re.compile(r"- listitem \[ref=[^\]]+\]:\s*(.+)")
_STEPSTONE_DETAIL_PUBLISHED_RE = re.compile(r"Erschienen:\s*([^\"]+)")
_STEPSTONE_DETAIL_MIN_DESC_CHARS = 220
_STEPSTONE_DETAIL_MAX_JOBS = 80
_STEPSTONE_PRELOADED_ANCHOR = 'window.__PRELOADED_STATE__["app-unifiedResultlist"] = '
_STEPSTONE_DEFAULT_LISTING_BACKENDS = ["http", "curl_cffi", "playwright_cli", "openclaw_snapshot"]
_STEPSTONE_DEFAULT_DETAIL_BACKENDS = ["curl_cffi", "playwright_cli", "http", "openclaw_snapshot"]
_STEPSTONE_DETAIL_DELAY_MIN_MS = 500
_STEPSTONE_DETAIL_DELAY_MAX_MS = 1300
_INDEED_PRELOADED_ANCHORS = [
    'window.mosaic.providerData["mosaic-provider-jobcards"] = ',
    'window.mosaic.providerData["mosaic-provider-jobcards"]=',
]
_INDEED_DEFAULT_LISTING_BACKENDS = ["playwright_cli"]
_INDEED_DEFAULT_DETAIL_BACKENDS = ["playwright_cli"]
_INDEED_DETAIL_MIN_DESC_CHARS = 700
_INDEED_DETAIL_MAX_JOBS = 20
_INDEED_DETAIL_DELAY_MIN_MS = 150
_INDEED_DETAIL_DELAY_MAX_MS = 450


def _clean_snapshot_value(raw: str) -> str:
    value = str(raw or "")
    value = re.sub(r"\[ref=e\d+\]", " ", value)
    value = value.replace("[cursor=pointer]", " ")
    value = value.replace('"', " ")
    value = re.sub(r"\s+", " ", value).strip()
    return value.strip(" -|")


def _clean_stepstone_description(raw: str) -> str:
    value = _clean_snapshot_value(raw)
    if not value:
        return ""
    value = re.sub(r"\blink\s+[^:]{1,160}:\s*", " ", value, flags=re.IGNORECASE)
    value = re.sub(
        r"\b(?:generic|link|img|heading|button|article|status|time|strong|text)\s*:\s*",
        " ",
        value,
        flags=re.IGNORECASE,
    )
    value = re.sub(r"\bshow more\b", " ", value, flags=re.IGNORECASE)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def _normalize_relative_published(raw: str, now: datetime | None = None) -> str:
    value = _clean_snapshot_value(raw)
    if not value:
        return ""
    now_dt = now if now is not None else datetime.now(timezone.utc)
    lowered = value.lower()

    if lowered in {"today", "heute"}:
        return now_dt.isoformat()
    if lowered in {"yesterday", "gestern"}:
        return (now_dt - timedelta(days=1)).isoformat()

    german = re.match(
        r"^vor\s+(\d+)\+?\s+(sekunde|sekunden|minute|minuten|stunde|stunden|tag|tagen|woche|wochen|monat|monaten|jahr|jahren)$",
        lowered,
    )
    if german:
        qty = int(german.group(1))
        unit = german.group(2)
        if unit in {"sekunde", "sekunden"}:
            return (now_dt - timedelta(seconds=qty)).isoformat()
        if unit in {"minute", "minuten"}:
            return (now_dt - timedelta(minutes=qty)).isoformat()
        if unit in {"stunde", "stunden"}:
            return (now_dt - timedelta(hours=qty)).isoformat()
        if unit in {"tag", "tagen"}:
            return (now_dt - timedelta(days=qty)).isoformat()
        if unit in {"woche", "wochen"}:
            return (now_dt - timedelta(weeks=qty)).isoformat()
        if unit in {"monat", "monaten"}:
            return (now_dt - timedelta(days=30 * qty)).isoformat()
        if unit in {"jahr", "jahren"}:
            return (now_dt - timedelta(days=365 * qty)).isoformat()

    english = re.match(
        r"^(\d+)\+?\s+(second|seconds|minute|minutes|hour|hours|day|days|week|weeks|month|months|year|years)\s+ago$",
        lowered,
    )
    if english:
        qty = int(english.group(1))
        unit = english.group(2)
        if unit in {"second", "seconds"}:
            return (now_dt - timedelta(seconds=qty)).isoformat()
        if unit in {"minute", "minutes"}:
            return (now_dt - timedelta(minutes=qty)).isoformat()
        if unit in {"hour", "hours"}:
            return (now_dt - timedelta(hours=qty)).isoformat()
        if unit in {"day", "days"}:
            return (now_dt - timedelta(days=qty)).isoformat()
        if unit in {"week", "weeks"}:
            return (now_dt - timedelta(weeks=qty)).isoformat()
        if unit in {"month", "months"}:
            return (now_dt - timedelta(days=30 * qty)).isoformat()
        if unit in {"year", "years"}:
            return (now_dt - timedelta(days=365 * qty)).isoformat()

    m_date = re.match(r"^(\d{1,2})\.(\d{1,2})\.(\d{4})$", value)
    if m_date:
        day = int(m_date.group(1))
        month = int(m_date.group(2))
        year = int(m_date.group(3))
        try:
            return datetime(year, month, day, tzinfo=timezone.utc).isoformat()
        except ValueError:
            return value

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat()
    except Exception:
        return value


def _extract_assigned_json_object(text: str, anchor: str) -> dict:
    raw = str(text or "")
    idx = raw.find(anchor)
    if idx < 0:
        return {}
    s = raw[idx + len(anchor) :]
    brace_depth = 0
    in_string = False
    escape_next = False
    end_idx = None
    for pos, ch in enumerate(s):
        if in_string:
            if escape_next:
                escape_next = False
            elif ch == "\\":
                escape_next = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
            continue
        if ch == "{":
            brace_depth += 1
            continue
        if ch == "}":
            brace_depth -= 1
            if brace_depth == 0:
                end_idx = pos + 1
                break
    if end_idx is None:
        return {}
    blob = s[:end_idx]
    try:
        payload = json.loads(blob)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _strip_html_preserve_blocks(raw: str) -> str:
    text = str(raw or "")
    if not text:
        return ""
    text = re.sub(r"(?i)<\s*br\s*/?\s*>", "\n", text)
    text = re.sub(r"(?i)</\s*(p|div|section|article|h[1-6])\s*>", "\n", text)
    text = re.sub(r"(?i)<\s*li[^>]*>", "\n- ", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html_lib.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _extract_karriere_jobposting_from_html(html_text: str) -> dict:
    html = str(html_text or "")
    if not html:
        return {}

    scripts = re.findall(r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>', html, flags=re.IGNORECASE | re.DOTALL)
    for script in scripts:
        try:
            obj = json.loads(script)
        except Exception:
            continue
        candidates = obj if isinstance(obj, list) else [obj]
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            if str(candidate.get("@type") or "").strip().lower() != "jobposting":
                continue

            title = str(candidate.get("title") or "").strip()
            date_posted = str(candidate.get("datePosted") or "").strip()
            published = _normalize_relative_published(date_posted)
            description = _strip_html_preserve_blocks(str(candidate.get("description") or ""))

            org = candidate.get("hiringOrganization") if isinstance(candidate.get("hiringOrganization"), dict) else {}
            company = str(org.get("name") or "").strip()

            location = ""
            job_location = candidate.get("jobLocation")
            loc_candidates = job_location if isinstance(job_location, list) else [job_location]
            for loc in loc_candidates:
                if not isinstance(loc, dict):
                    continue
                address_obj = loc.get("address") if isinstance(loc.get("address"), dict) else {}
                locality = str(address_obj.get("addressLocality") or "").strip()
                region = str(address_obj.get("addressRegion") or "").strip()
                country = str(address_obj.get("addressCountry") or "").strip()
                location = locality or region or country
                if location:
                    break

            return {
                "title": title,
                "company": company,
                "location": location,
                "published": published,
                "description": description,
            }
    return {}


def _extract_stepstone_jobposting_from_html(html_text: str) -> dict:
    html = str(html_text or "")
    if not html:
        return {}
    scripts = re.findall(r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>', html, flags=re.IGNORECASE | re.DOTALL)
    for script in scripts:
        try:
            obj = json.loads(script)
        except Exception:
            continue
        candidates = obj if isinstance(obj, list) else [obj]
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            if str(candidate.get("@type") or "").strip().lower() != "jobposting":
                continue
            title = str(candidate.get("title") or "").strip()
            date_posted = str(candidate.get("datePosted") or "").strip()
            description = _strip_html_preserve_blocks(str(candidate.get("description") or ""))
            org = candidate.get("hiringOrganization") if isinstance(candidate.get("hiringOrganization"), dict) else {}
            company = str(org.get("name") or "").strip()
            loc_obj = candidate.get("jobLocation") if isinstance(candidate.get("jobLocation"), dict) else {}
            address_obj = loc_obj.get("address") if isinstance(loc_obj.get("address"), dict) else {}
            locality = str(address_obj.get("addressLocality") or "").strip()
            return {
                "title": title,
                "company": company,
                "location": locality,
                "published": _normalize_relative_published(date_posted),
                "description": description,
            }
    return {}


def _stepstone_page_url(base_url: str, page: int) -> str:
    if page <= 1:
        return base_url
    sep = "&" if "?" in base_url else "?"
    return f"{base_url}{sep}page={page}"


def _normalize_stepstone_strategy(source: dict) -> dict:
    strategy = source.get("fetch_strategy") if isinstance(source.get("fetch_strategy"), dict) else {}
    listing_backends = [str(x).strip() for x in strategy.get("listing_backends", _STEPSTONE_DEFAULT_LISTING_BACKENDS) if str(x).strip()]
    detail_backends = [str(x).strip() for x in strategy.get("detail_backends", _STEPSTONE_DEFAULT_DETAIL_BACKENDS) if str(x).strip()]
    timeout_sec = max(5, int(strategy.get("timeout_sec", 35)))
    detail_min_chars = max(120, int(strategy.get("detail_min_chars", _STEPSTONE_DETAIL_MIN_DESC_CHARS)))
    detail_max_jobs = max(0, int(strategy.get("detail_max_jobs", _STEPSTONE_DETAIL_MAX_JOBS)))
    delay_min_ms = max(0, int(strategy.get("detail_delay_min_ms", _STEPSTONE_DETAIL_DELAY_MIN_MS)))
    delay_max_ms = max(delay_min_ms, int(strategy.get("detail_delay_max_ms", _STEPSTONE_DETAIL_DELAY_MAX_MS)))
    enrich_details = bool(strategy.get("detail_enrich", True))
    return {
        "listing_backends": listing_backends,
        "detail_backends": detail_backends,
        "timeout_sec": timeout_sec,
        "detail_min_chars": detail_min_chars,
        "detail_max_jobs": detail_max_jobs,
        "detail_delay_min_ms": delay_min_ms,
        "detail_delay_max_ms": delay_max_ms,
        "detail_enrich": enrich_details,
    }


def _normalize_indeed_strategy(source: dict) -> dict:
    strategy = source.get("fetch_strategy") if isinstance(source.get("fetch_strategy"), dict) else {}
    listing_backends = [
        str(x).strip() for x in strategy.get("listing_backends", _INDEED_DEFAULT_LISTING_BACKENDS) if str(x).strip()
    ]
    detail_backends = [
        str(x).strip() for x in strategy.get("detail_backends", _INDEED_DEFAULT_DETAIL_BACKENDS) if str(x).strip()
    ]
    timeout_sec = max(8, int(strategy.get("timeout_sec", 45)))
    detail_min_chars = max(120, int(strategy.get("detail_min_chars", _INDEED_DETAIL_MIN_DESC_CHARS)))
    detail_max_jobs = max(0, int(strategy.get("detail_max_jobs", _INDEED_DETAIL_MAX_JOBS)))
    delay_min_ms = max(0, int(strategy.get("detail_delay_min_ms", _INDEED_DETAIL_DELAY_MIN_MS)))
    delay_max_ms = max(delay_min_ms, int(strategy.get("detail_delay_max_ms", _INDEED_DETAIL_DELAY_MAX_MS)))
    enrich_details = bool(strategy.get("detail_enrich", True))
    return {
        "listing_backends": listing_backends,
        "detail_backends": detail_backends,
        "timeout_sec": timeout_sec,
        "detail_min_chars": detail_min_chars,
        "detail_max_jobs": detail_max_jobs,
        "detail_delay_min_ms": delay_min_ms,
        "detail_delay_max_ms": delay_max_ms,
        "detail_enrich": enrich_details,
    }


def _indeed_origin(source_url: str) -> str:
    parsed = urlparse(str(source_url or "").strip())
    if parsed.scheme and parsed.netloc and "indeed." in parsed.netloc.lower():
        return f"{parsed.scheme}://{parsed.netloc}"
    return "https://at.indeed.com"


def _indeed_page_url(base_url: str, page: int) -> str:
    parsed = urlparse(str(base_url or "").strip())
    query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    query_pairs = [(k, v) for (k, v) in query_pairs if str(k).lower() != "start"]
    lower_keys = {str(k).lower() for (k, _) in query_pairs}
    if "fromage" not in lower_keys:
        query_pairs.append(("fromage", "14"))
    if "sort" not in lower_keys:
        query_pairs.append(("sort", "date"))
    if page > 1:
        query_pairs.append(("start", str((page - 1) * 10)))
    query = urlencode(query_pairs)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, query, parsed.fragment))


def _extract_indeed_jobcards_payload(html_text: str) -> dict:
    raw = str(html_text or "")
    if not raw:
        return {}
    for anchor in _INDEED_PRELOADED_ANCHORS:
        payload = _extract_assigned_json_object(raw, anchor)
        if payload:
            return payload
    return {}


def _looks_like_indeed_block_page(html_text: str) -> bool:
    text = str(html_text or "").lower()
    if not text:
        return False
    markers = [
        "additional verification required",
        "our systems have detected unusual traffic",
        "verify you are human",
        "access denied",
        "<title>just a moment",
        "enable javascript and cookies to continue",
    ]
    return any(m in text for m in markers)


def parse_indeed_listing_html(
    html_text: str,
    source_name: str,
    source_type: str,
    fetched_at: str | None = None,
    source_url: str = "",
) -> list[dict]:
    out: list[dict] = []
    payload = _extract_indeed_jobcards_payload(html_text)
    model = payload.get("metaData", {}).get("mosaicProviderJobCardsModel", {}) if isinstance(payload.get("metaData"), dict) else {}
    items = model.get("results", []) if isinstance(model, dict) else []
    if not isinstance(items, list):
        return out

    origin = _indeed_origin(source_url)
    fetched_iso = str(fetched_at or datetime.now(timezone.utc).isoformat())
    seen = set()
    for row in items:
        if not isinstance(row, dict):
            continue

        job_key = str(row.get("jobkey") or row.get("jk") or "").strip()
        title = str(row.get("displayTitle") or row.get("title") or "").strip()
        if not job_key or not title:
            continue
        if job_key in seen:
            continue
        seen.add(job_key)

        company = str(row.get("company") or row.get("companyName") or "").strip()
        location = str(row.get("formattedLocation") or "").strip()
        if not location:
            location = ", ".join(
                x
                for x in [
                    str(row.get("jobLocationCity") or "").strip(),
                    str(row.get("jobLocationRegion") or "").strip(),
                    str(row.get("jobLocationCountry") or "").strip(),
                ]
                if x
            )
        relative = str(row.get("formattedRelativeTime") or "").strip()
        published = _parse_epoch_millis(row.get("pubDate")) or _normalize_relative_published(relative)
        snippet = _strip_html_preserve_blocks(str(row.get("snippet") or row.get("snippetText") or ""))
        url = f"{origin}/viewjob?jk={job_key}"

        text = " ".join([title, company, location, snippet, relative])
        out.append(
            {
                "id": f"{source_name}:{job_key}"[:500],
                "source": source_name,
                "source_type": source_type,
                "title": title[:220],
                "company": company[:180],
                "location": (location or guess_location(text))[:180],
                "remote_hint": guess_remote(text),
                "url": url,
                "description": (snippet or title)[:3200],
                "published": str(published or "")[:64],
                "fetched_at": fetched_iso,
            }
        )
    return out


def _extract_indeed_jobposting_from_html(html_text: str) -> dict:
    html = str(html_text or "")
    if not html:
        return {}
    scripts = re.findall(r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>', html, flags=re.IGNORECASE | re.DOTALL)
    for script in scripts:
        try:
            obj = json.loads(script)
        except Exception:
            continue
        candidates = obj if isinstance(obj, list) else [obj]
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            if str(candidate.get("@type") or "").strip().lower() != "jobposting":
                continue

            title = str(candidate.get("title") or "").strip()
            date_posted = str(candidate.get("datePosted") or "").strip()
            published = _normalize_relative_published(date_posted)
            description = _strip_html_preserve_blocks(str(candidate.get("description") or ""))
            org = candidate.get("hiringOrganization") if isinstance(candidate.get("hiringOrganization"), dict) else {}
            company = str(org.get("name") or "").strip()

            location = ""
            job_location = candidate.get("jobLocation")
            loc_candidates = job_location if isinstance(job_location, list) else [job_location]
            for loc in loc_candidates:
                if not isinstance(loc, dict):
                    continue
                address_obj = loc.get("address") if isinstance(loc.get("address"), dict) else {}
                locality = str(address_obj.get("addressLocality") or "").strip()
                region = str(address_obj.get("addressRegion") or "").strip()
                country = str(address_obj.get("addressCountry") or "").strip()
                location = locality or region or country
                if location:
                    break

            job_location_type = str(candidate.get("jobLocationType") or "").strip().lower()
            remote_hint = bool(job_location_type and "telecommute" in job_location_type)
            if not remote_hint:
                remote_hint = guess_remote(" ".join([title, description, location]))

            return {
                "title": title,
                "company": company,
                "location": location,
                "published": published,
                "description": description,
                "remote_hint": remote_hint,
            }
    return {}


def _merge_indeed_detail(job: dict, detail: dict):
    if not isinstance(detail, dict):
        return
    title = str(detail.get("title") or "").strip()
    company = str(detail.get("company") or "").strip()
    location = str(detail.get("location") or "").strip()
    published = str(detail.get("published") or "").strip()
    description = str(detail.get("description") or "").strip()
    remote_hint = bool(detail.get("remote_hint", False))

    if title and len(title) >= len(str(job.get("title") or "")):
        job["title"] = title[:220]
    if company and not str(job.get("company") or "").strip():
        job["company"] = company[:180]
    if location and not str(job.get("location") or "").strip():
        job["location"] = location[:180]
    if published:
        job["published"] = published[:64]
    if description and len(description) >= len(str(job.get("description") or "").strip()):
        job["description"] = description[:8000]
    if remote_hint:
        job["remote_hint"] = True


def _enrich_indeed_with_detail_pages(jobs: list[dict], strategy: dict):
    if not jobs:
        return jobs
    if not bool(strategy.get("detail_enrich", True)):
        return jobs

    detail_min_chars = max(120, int(strategy.get("detail_min_chars", _INDEED_DETAIL_MIN_DESC_CHARS)))
    detail_max_jobs = max(0, int(strategy.get("detail_max_jobs", _INDEED_DETAIL_MAX_JOBS)))
    if detail_max_jobs <= 0:
        return jobs

    detail_backends = [str(x) for x in strategy.get("detail_backends", _INDEED_DEFAULT_DETAIL_BACKENDS)]
    timeout_sec = max(8, int(strategy.get("timeout_sec", 45)))
    delay_min_ms = max(0, int(strategy.get("detail_delay_min_ms", _INDEED_DETAIL_DELAY_MIN_MS)))
    delay_max_ms = max(delay_min_ms, int(strategy.get("detail_delay_max_ms", _INDEED_DETAIL_DELAY_MAX_MS)))

    enrichable = [
        j
        for j in jobs
        if len(str(j.get("description") or "").strip()) < detail_min_chars
        or not str(j.get("company") or "").strip()
        or not str(j.get("published") or "").strip()
    ]
    if not enrichable:
        return jobs

    processed = 0
    seen_urls = set()
    for job in enrichable:
        if processed >= detail_max_jobs:
            break
        url = str(job.get("url") or "").strip()
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        processed += 1

        try:
            fetched = fetch_with_backends(url=url, backends=detail_backends, timeout_sec=timeout_sec)
            detail = _extract_indeed_jobposting_from_html(fetched.text)
            _merge_indeed_detail(job, detail)
        except Exception:
            continue

        if delay_max_ms > 0:
            sleep_ms = random.randint(delay_min_ms, delay_max_ms)
            time.sleep(sleep_ms / 1000.0)

    return jobs


def fetch_indeed_jobs(source_name: str, source_type: str, base_url: str, pages: int = 1, source_cfg: dict | None = None):
    source_cfg = source_cfg or {}
    strategy = _normalize_indeed_strategy(source_cfg)
    timeout_sec = int(strategy.get("timeout_sec", 45))
    listing_backends = [str(x) for x in strategy.get("listing_backends", _INDEED_DEFAULT_LISTING_BACKENDS)]
    fetched_at = datetime.now(timezone.utc).isoformat()

    out = []
    blocked_errors: list[str] = []
    for page in range(1, max(1, pages) + 1):
        page_url = _indeed_page_url(base_url, page)
        try:
            fetched = fetch_with_backends(url=page_url, backends=listing_backends, timeout_sec=timeout_sec)
            rows = parse_indeed_listing_html(
                html_text=fetched.text,
                source_name=source_name,
                source_type=source_type,
                fetched_at=fetched_at,
                source_url=page_url,
            )
            out.extend(rows)
            if not rows:
                status_code = int(fetched.status_code or 0)
                if status_code >= 400 or _looks_like_indeed_block_page(fetched.text):
                    blocked_errors.append(f"listing blocked (status={status_code}) for {page_url}")
        except FetchBackendError:
            continue

    if not out and blocked_errors:
        raise FetchBackendError("; ".join(blocked_errors)[:400])

    deduped = []
    seen = set()
    for row in out:
        key = str(row.get("id") or row.get("url") or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    return _enrich_indeed_with_detail_pages(deduped, strategy=strategy)


def parse_stepstone_listing_html(html_text: str, source_name: str, source_type: str, fetched_at: str | None = None) -> list[dict]:
    out: list[dict] = []
    preloaded = _extract_assigned_json_object(html_text, _STEPSTONE_PRELOADED_ANCHOR)
    items = preloaded.get("searchResults", {}).get("items", []) if isinstance(preloaded.get("searchResults"), dict) else []
    if not isinstance(items, list):
        return out

    fetched_iso = str(fetched_at or datetime.now(timezone.utc).isoformat())
    for row in items:
        if not isinstance(row, dict):
            continue
        title = str(row.get("title") or "").strip()
        company = str(row.get("companyName") or "").strip()
        location = str(row.get("location") or "").strip()
        rel_url = str(row.get("url") or "").strip()
        if not title or not rel_url:
            continue
        abs_url = urljoin("https://www.stepstone.at", rel_url.split("?", 1)[0].strip())
        if "stellenangebote--" not in abs_url:
            continue
        snippet = _strip_html_preserve_blocks(str(row.get("textSnippet") or ""))
        published = _normalize_relative_published(
            str(row.get("datePosted") or row.get("publishFromDate") or row.get("periodPostedDate") or "")
        )
        work_from_home = row.get("workFromHome")
        remote_hint = False
        try:
            remote_hint = int(work_from_home or 0) > 0
        except Exception:
            remote_hint = guess_remote(" ".join([title, snippet, location]))

        desc = snippet if snippet else title
        out.append(
            {
                "id": f"{source_name}:{str(row.get('id') or abs_url)}"[:500],
                "source": source_name,
                "source_type": source_type,
                "title": title,
                "company": company,
                "location": location or guess_location(" ".join([title, company, desc])),
                "remote_hint": remote_hint,
                "url": abs_url,
                "description": desc[:2400],
                "published": str(published or "")[:64],
                "fetched_at": fetched_iso,
            }
        )
    return out


def _extract_stepstone_detail_from_snapshot(snapshot: str) -> dict:
    snap = str(snapshot or "")
    if not snap:
        return {}

    lines = snap.splitlines()
    start_idx = 0
    for idx, line in enumerate(lines):
        if '[level=1]' in line and '- heading "' in line:
            start_idx = idx
            break

    end_idx = len(lines)
    for idx in range(start_idx, len(lines)):
        line = lines[idx]
        if "- contentinfo" in line or "The Stepstone Group GmbH" in line:
            end_idx = idx
            break

    segment_lines = []
    for line in lines[start_idx:end_idx]:
        low = line.lower()
        if "diese jobs waren bei anderen jobsuchenden beliebt" in low:
            break
        segment_lines.append(line)
    segment = "\n".join(segment_lines)
    parts: list[str] = []

    for heading, lvl in _STEPSTONE_DETAIL_HEADING_RE.findall(segment):
        h = _clean_stepstone_description(heading)
        if h and h.lower() not in {"profil", "aufgaben", "wir bieten"}:
            parts.append(h)
        elif h:
            parts.append(h.upper())

    for text in _STEPSTONE_DETAIL_PARAGRAPH_RE.findall(segment):
        clean = _clean_stepstone_description(text)
        if clean and len(clean) >= 30:
            parts.append(clean)

    for text in _STEPSTONE_DETAIL_LISTITEM_RE.findall(segment):
        clean = _clean_stepstone_description(text)
        if clean and len(clean) >= 8:
            parts.append(f"- {clean}")

    deduped: list[str] = []
    seen = set()
    for p in parts:
        key = p.strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(p.strip())

    description = "\n".join(deduped).strip()
    if len(description) > 8000:
        description = description[:8000]

    published = ""
    m_pub = _STEPSTONE_DETAIL_PUBLISHED_RE.search(segment)
    if m_pub:
        published = _normalize_relative_published(m_pub.group(1))

    return {"description": description, "published": published}


def _merge_stepstone_detail(job: dict, detail: dict):
    if not isinstance(detail, dict):
        return
    title = str(detail.get("title") or "").strip()
    company = str(detail.get("company") or "").strip()
    location = str(detail.get("location") or "").strip()
    published = str(detail.get("published") or "").strip()
    description = str(detail.get("description") or "").strip()

    if title and len(title) >= len(str(job.get("title") or "")):
        job["title"] = title[:220]
    if company and not str(job.get("company") or "").strip():
        job["company"] = company[:180]
    if location and not str(job.get("location") or "").strip():
        job["location"] = location[:180]
    if published:
        job["published"] = published[:64]
    if description and len(description) >= len(str(job.get("description") or "").strip()):
        job["description"] = description[:8000]


def _enrich_stepstone_with_detail_pages(jobs: list[dict], strategy: dict):
    if not jobs:
        return jobs
    if not bool(strategy.get("detail_enrich", True)):
        return jobs

    detail_min_chars = max(120, int(strategy.get("detail_min_chars", _STEPSTONE_DETAIL_MIN_DESC_CHARS)))
    detail_max_jobs = max(0, int(strategy.get("detail_max_jobs", _STEPSTONE_DETAIL_MAX_JOBS)))
    if detail_max_jobs <= 0:
        return jobs

    detail_backends = [str(x) for x in strategy.get("detail_backends", _STEPSTONE_DEFAULT_DETAIL_BACKENDS)]
    timeout_sec = max(5, int(strategy.get("timeout_sec", 35)))
    delay_min_ms = max(0, int(strategy.get("detail_delay_min_ms", _STEPSTONE_DETAIL_DELAY_MIN_MS)))
    delay_max_ms = max(delay_min_ms, int(strategy.get("detail_delay_max_ms", _STEPSTONE_DETAIL_DELAY_MAX_MS)))

    enrichable = [j for j in jobs if len(str(j.get("description") or "").strip()) < detail_min_chars]
    if not enrichable:
        return jobs

    processed = 0
    seen_urls = set()
    for job in enrichable:
        if processed >= detail_max_jobs:
            break
        url = str(job.get("url") or "").strip()
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        processed += 1

        try:
            fetched = fetch_with_backends(
                url=url,
                backends=detail_backends,
                timeout_sec=timeout_sec,
            )
            if fetched.backend == "openclaw_snapshot":
                detail = _extract_stepstone_detail_from_snapshot(fetched.text)
            else:
                detail = _extract_stepstone_jobposting_from_html(fetched.text)
            _merge_stepstone_detail(job, detail)
        except Exception:
            continue

        if delay_max_ms > 0:
            sleep_ms = random.randint(delay_min_ms, delay_max_ms)
            time.sleep(sleep_ms / 1000.0)

    return jobs


def parse_stepstone_snapshot(snapshot: str, source_name: str, source_type: str, fetched_at: str | None = None):
    out = []
    snap = str(snapshot or "")
    if not snap:
        return out

    url_matches = list(_STEPSTONE_JOB_URL_RE.finditer(snap))
    if not url_matches:
        return out

    fetched_iso = str(fetched_at or datetime.now(timezone.utc).isoformat())

    for idx, match in enumerate(url_matches):
        rel = match.group(1).strip()
        abs_url = f"https://www.stepstone.at{rel}"
        if "stellenangebote--" not in abs_url:
            continue

        prev_start = url_matches[idx - 1].start() if idx > 0 else 0
        next_start = url_matches[idx + 1].start() if idx + 1 < len(url_matches) else len(snap)
        before = snap[max(prev_start, match.start() - 2200) : match.start()]
        after = snap[match.end() : next_start]

        title_matches = list(_STEPSTONE_TITLE_RE.finditer(before))
        if not title_matches:
            continue
        title = _clean_snapshot_value(title_matches[-1].group(1))
        if not title:
            continue
        if _STEPSTONE_NOISE_TITLE_RE.match(title) or title.lower().startswith("noch nichts dabei"):
            continue

        company_matches = list(_STEPSTONE_COMPANY_IMG_RE.finditer(before))
        company = _clean_snapshot_value(company_matches[-1].group(1)) if company_matches else ""
        if not company:
            after_company_matches = list(_STEPSTONE_COMPANY_IMG_RE.finditer(after))
            if after_company_matches:
                company = _clean_snapshot_value(after_company_matches[0].group(1))
        if company.lower() == title.lower():
            company = ""

        time_match = _STEPSTONE_TIME_RE.search(after)
        if not time_match:
            time_match = re.search(r"\bshow more\s+([^\"\n]{2,50})\"", after, flags=re.IGNORECASE)
        published_raw = _clean_snapshot_value(time_match.group(1) if time_match else "")
        published = _normalize_relative_published(published_raw)

        description = ""
        desc_match = _STEPSTONE_DESC_QUOTED_RE.search(after)
        if desc_match:
            description = _clean_stepstone_description(desc_match.group(1))
        if not description:
            desc_text_match = _STEPSTONE_DESC_TEXT_RE.search(after)
            if desc_text_match:
                description = _clean_stepstone_description(desc_text_match.group(1))
        description = re.sub(r"\bshow more\b.*$", "", description, flags=re.IGNORECASE).strip()
        if "[ref=e" in description.lower() or "cursor=pointer" in description.lower():
            description = ""

        generic_values = [_clean_snapshot_value(x) for x in re.findall(r"- generic \[ref=[^\]]+\]:\s*([^\n]+)", after[:1400])]
        generic_values = [x for x in generic_values if x and x.lower() not in _STEPSTONE_LOCATION_STOPWORDS and not x.startswith("/url:")]
        context_parts = [title, company, description] + generic_values[:8]
        context_text = " ".join(x for x in context_parts if x)
        location = guess_location(context_text)
        remote_hint = guess_remote(context_text)

        if not description:
            fallback_bits = [x for x in generic_values if len(x) >= 40 and x != company and x != title]
            description = _clean_stepstone_description(" ".join(fallback_bits[:2]).strip())
        if not description:
            description = title

        out.append(
            {
                "id": f"{source_name}:{abs_url}"[:500],
                "source": source_name,
                "source_type": source_type,
                "title": title,
                "company": company,
                "location": location,
                "remote_hint": remote_hint,
                "url": abs_url,
                "description": description[:2400],
                "published": published[:64],
                "fetched_at": fetched_iso,
            }
        )
    return out


def fetch_stepstone_jobs(source_name: str, source_type: str, base_url: str, pages: int = 1, source_cfg: dict | None = None):
    source_cfg = source_cfg or {}
    strategy = _normalize_stepstone_strategy(source_cfg)
    timeout_sec = int(strategy.get("timeout_sec", 35))
    listing_backends = [str(x) for x in strategy.get("listing_backends", _STEPSTONE_DEFAULT_LISTING_BACKENDS)]
    fetched_at = datetime.now(timezone.utc).isoformat()

    out = []
    for page in range(1, max(1, pages) + 1):
        page_url = _stepstone_page_url(base_url, page)
        try:
            fetched = fetch_with_backends(url=page_url, backends=listing_backends, timeout_sec=timeout_sec)
            if fetched.backend == "openclaw_snapshot":
                rows = parse_stepstone_snapshot(
                    snapshot=fetched.text,
                    source_name=source_name,
                    source_type=source_type,
                    fetched_at=fetched_at,
                )
            else:
                rows = parse_stepstone_listing_html(
                    html_text=fetched.text,
                    source_name=source_name,
                    source_type=source_type,
                    fetched_at=fetched_at,
                )
            out.extend(rows)
        except FetchBackendError:
            continue

    # Keep first occurrence order by URL.
    deduped = []
    seen = set()
    for row in out:
        key = str(row.get("url") or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    return _enrich_stepstone_with_detail_pages(deduped, strategy=strategy)


def fetch_stepstone_via_browser(source_name: str, source_type: str, base_url: str, pages: int = 1):
    """Legacy wrapper kept for compatibility. Uses only OpenClaw snapshot backend."""
    source_cfg = {
        "fetch_strategy": {
            "listing_backends": ["openclaw_snapshot"],
            "detail_backends": ["openclaw_snapshot"],
            "detail_enrich": True,
            "detail_max_jobs": _STEPSTONE_DETAIL_MAX_JOBS,
            "detail_min_chars": _STEPSTONE_DETAIL_MIN_DESC_CHARS,
            "timeout_sec": 90,
            "detail_delay_min_ms": 0,
            "detail_delay_max_ms": 0,
        }
    }
    return fetch_stepstone_jobs(
        source_name=source_name,
        source_type=source_type,
        base_url=base_url,
        pages=pages,
        source_cfg=source_cfg,
    )


def enrich_job_detail(job):
    url = job.get("url", "")
    if "karriere.at/jobs/" not in url:
        return job
    try:
        html = fetch_url(url, timeout=20)
    except Exception:
        return job

    title = job.get("title", "")
    company = job.get("company", "")
    published = str(job.get("published") or "").strip()

    detail = _extract_karriere_jobposting_from_html(html)
    detail_title = str(detail.get("title") or "").strip()
    detail_company = str(detail.get("company") or "").strip()
    detail_location = str(detail.get("location") or "").strip()
    detail_published = str(detail.get("published") or "").strip()
    detail_description = str(detail.get("description") or "").strip()
    if detail_title:
        title = detail_title
    if detail_company:
        company = detail_company
    if detail_published:
        published = detail_published

    m_title = re.search(r"<title>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
    if m_title:
        raw = strip_html(m_title.group(1))
        raw = raw.replace("- karriere.at", "").strip()
        if raw:
            title = raw

    if " - " in title and not company:
        parts = [p.strip() for p in title.split(" - ") if p.strip()]
        if len(parts) >= 2:
            company = parts[1]

    text_sample = detail_description if detail_description else strip_html(html[:30000])
    location = detail_location or guess_location(f"{title} {text_sample}")

    return {
        **job,
        "title": title,
        "company": company,
        "location": location or job.get("location", ""),
        "published": published,
        "remote_hint": guess_remote(f"{title} {text_sample}"),
        "description": text_sample[:8000],
    }


def dedupe_jobs(jobs):
    seen = set()
    out = []
    for j in jobs:
        key = (j.get("url") or j.get("id") or j.get("title", "")).strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(j)
    return out
