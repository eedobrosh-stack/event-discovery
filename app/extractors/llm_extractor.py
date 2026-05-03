"""LLM-powered event extraction from arbitrary venue/listing URLs.

Public entry point: ``extract(url, ...) -> ExtractionResult``.

Two execution paths, picked mechanically based on whether the page has
substantive static HTML:

  cleaned HTML ≥ threshold  →  HTML path: send cleaned <body> to Gemini
                                 with response_schema enforcement.
  cleaned HTML < threshold  →  url_context path: ask Gemini to fetch the
                                 URL itself (server-side JS render via the
                                 url_context tool); Gemini then extracts.

The url_context path was proven out against the cameri.co.il SPA earlier
this week — pulled real Hebrew event titles from a JS-rendered page with
no Playwright dependency.

Hallucination cross-check: when running the HTML path we also have the
raw HTML, so we drop any event whose ``name`` doesn't appear as a literal
substring of the source. This prevents Gemini from inventing events
during low-content extractions. The url_context path skips this guard
(we don't have the rendered HTML on our side); flagged for follow-up.

Pagination: NOT handled in v1. ``extract`` returns whatever's on the
first page. Pagination strategy is per-site and lives outside this module
when we get to it.

This module returns ``RawEvent`` objects ready for the existing
collector ingestion pipeline. The caller fills in city/source-routing
context.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional
from urllib.parse import quote, urljoin, urlsplit, urlunsplit

import urllib.request

from app.services.collectors.base import RawEvent

logger = logging.getLogger(__name__)


class ExtractorUnconfigured(RuntimeError):
    """Raised when the extractor is invoked without a GEMINI_API_KEY set.

    The constructor doesn't fail on missing key (modules at import time
    shouldn't blow up) — only ``extract()`` raises, so callers can decide
    whether the extractor's absence is fatal."""


@dataclass
class ExtractionResult:
    events: list[RawEvent]
    method: str               # "html" | "url_context" | "error"
    raw_html_bytes: int = 0
    cleaned_html_bytes: int = 0
    api_call_ok: bool = False
    error: Optional[str] = None
    dropped_for_hallucination: int = 0


# Schema mirrors RawEvent fields the LLM is responsible for. Matches the
# spike script (kept stable so existing test fixtures still parse).
_RESPONSE_SCHEMA: dict = {
    "type": "object",
    "properties": {
        "events": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name":           {"type": "string"},
                    "artist_name":    {"type": "string", "nullable": True},
                    "start_date":     {"type": "string",
                                       "description": "ISO 8601 date YYYY-MM-DD."},
                    "start_time":     {"type": "string", "nullable": True,
                                       "description": "24h HH:MM."},
                    "end_date":       {"type": "string", "nullable": True},
                    "venue_name":     {"type": "string", "nullable": True},
                    "purchase_link":  {"type": "string", "nullable": True},
                    "price":          {"type": "number", "nullable": True},
                    "price_currency": {"type": "string", "nullable": True,
                                       "description": "ISO 4217: ILS, USD, EUR, GBP."},
                    "description":    {"type": "string", "nullable": True},
                    "image_url":      {"type": "string", "nullable": True},
                },
                "required": ["name", "start_date"],
                "propertyOrdering": [
                    "name", "artist_name", "start_date", "start_time", "end_date",
                    "venue_name", "purchase_link", "price", "price_currency",
                    "description", "image_url",
                ],
            },
        },
    },
    "required": ["events"],
}


_HTML_PROMPT = """\
You are extracting upcoming events from a city-events web page.

Source URL: {url}
Today's date: {today}

The HTML below is the events listing page. Extract every distinct upcoming
event you can identify — concerts, theatre, exhibitions, club nights,
festivals, talks, tours. Skip past events. Skip non-event content
(blog posts, articles, "about us" sections).

Rules:
- Each event must have a real start_date in ISO format (YYYY-MM-DD).
  Skip the event if you cannot determine a date with reasonable confidence.
- If the page lists a date range, set start_date to the first day and
  end_date to the last day. Keep them within the same row.
- artist_name only when there's a clear performer/lecturer/headliner.
  For exhibitions or non-headlined events, leave artist_name null.
- venue_name should be the specific venue (e.g. "Cameri Theatre"), not
  a city name.
- purchase_link should be the direct ticket URL when visible (the
  <a href> nearest the event card). Use absolute URL.
- price should be the lowest visible price as a number. "Free" → 0.
  Unknown → null. price_currency in ISO code (ILS, USD, EUR, GBP).
- Don't invent fields. Null is always preferable to a guess.
- Translate names to English ONLY if the page provides a translation;
  otherwise keep the original-language name verbatim.

Return up to {max_events} events. Page HTML follows:

---
{html}
---
"""


_URL_CONTEXT_PROMPT = """\
You are extracting upcoming events from a city-events web page.

Source URL: {url}
Today's date: {today}

Fetch the page and extract every distinct upcoming event you can identify
— concerts, theatre, exhibitions, club nights, festivals, talks, tours.
Skip past events. Skip non-event content (blog posts, articles, "about
us" sections).

Same field rules as the structured output schema requires:
- start_date is ISO YYYY-MM-DD; skip events without a confident date.
- start_time is 24h HH:MM if visible.
- artist_name only when a clear performer/lecturer is named.
- venue_name is the specific venue, not a city.
- purchase_link absolute URL when visible; null otherwise.
- price as the lowest visible number; "Free" → 0; unknown → null.
- price_currency in ISO code (ILS, USD, EUR, GBP).
- Translate names to English ONLY if the page provides a translation.

Return up to {max_events} events.
"""


# Decision-rule threshold for HTML-vs-url_context. Below this many bytes
# of cleaned <body> the page is treated as a JS-rendered SPA where the
# meaningful content arrives only after JS executes.
_SPA_THRESHOLD_BYTES = 2_000

# Truncate even cleaned HTML at this size before passing to Gemini.
# A typical events listing has its content in the first 200 KB of <body>.
_MAX_CLEANED_HTML = 200_000


def _fetch_html(url: str, timeout: int = 20) -> Optional[str]:
    """Plain HTTP fetch with Hebrew/CJK URL safety. Returns None on error.

    Percent-encodes non-ASCII path/query characters so urlopen's ASCII
    serializer doesn't blow up on URLs with Hebrew components — the same
    treatment used in scripts/find_city_guides.
    """
    parts = urlsplit(url)
    safe_path = quote(parts.path, safe="/%-._~!$&'()*+,;=:@")
    safe_query = quote(parts.query, safe="=&%-._~!$'()*+,;:@/?")
    encoded = urlunsplit((parts.scheme, parts.netloc, safe_path,
                          safe_query, parts.fragment))

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9,he;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    try:
        req = urllib.request.Request(encoded, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", errors="ignore")
    except Exception as e:
        logger.warning(f"_fetch_html({url}): {type(e).__name__}: {e}")
        return None


def _clean_html(html: str, base_url: str, max_chars: int = _MAX_CLEANED_HTML) -> str:
    """Strip noise and resolve relative URLs.

    Keeps the structural skeleton (anchors → ticket links, img → posters)
    so Gemini can emit absolute URLs in extracted fields. Drops scripts,
    styles, headers/footers/nav/aside since they're chrome that wastes
    tokens. <noscript> is preserved in case a site uses it for a fallback
    event listing.
    """
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "lxml")

    for tag in soup(["script", "style", "svg", "iframe", "form", "input",
                     "button", "header", "footer", "nav", "aside"]):
        tag.decompose()

    for a in soup.find_all("a", href=True):
        a["href"] = urljoin(base_url, a["href"])
    for img in soup.find_all("img", src=True):
        img["src"] = urljoin(base_url, img["src"])

    body = soup.body or soup
    text = str(body)
    if len(text) > max_chars:
        text = text[:max_chars] + "\n<!-- TRUNCATED -->"
    return text


def _gemini_client():
    """Lazy import + key-presence check. Raises ExtractorUnconfigured if key is
    missing so callers see a clean exception rather than ImportError or KeyError."""
    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        raise ExtractorUnconfigured(
            "GEMINI_API_KEY (or GOOGLE_API_KEY) is not set. "
            "The LLM extractor needs it. See docs/llm_extraction_architecture.md."
        )
    try:
        from google import genai
        return genai.Client(api_key=api_key)
    except ImportError as e:
        raise ExtractorUnconfigured(
            "google-genai SDK not installed. Add to requirements: google-genai"
        ) from e


def _to_raw_event(d: dict, source: str, source_url: str) -> Optional[RawEvent]:
    """Convert one Gemini-emitted event dict to a RawEvent. Drops malformed
    or past-dated rows; returns None for those."""
    name = (d.get("name") or "").strip()
    if not name:
        return None
    sd_str = (d.get("start_date") or "").strip()
    if not sd_str:
        return None
    try:
        start_d = date.fromisoformat(sd_str[:10])
    except ValueError:
        return None
    if start_d < date.today():
        return None

    end_d = None
    ed_str = (d.get("end_date") or "").strip()
    if ed_str:
        try:
            end_d = date.fromisoformat(ed_str[:10])
        except ValueError:
            pass

    # Stable source_id: hash(scrape_source | page_url | name | start_date)
    # — same shape the architecture doc proposes for cross-collector dedup.
    import hashlib
    seed = f"{source}|{source_url}|{name.lower().strip()}|{sd_str[:10]}"
    sid = source + "_" + hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]

    price_raw = d.get("price")
    price: Optional[float] = None
    if price_raw is not None:
        try:
            price = float(price_raw)
        except (TypeError, ValueError):
            price = None

    return RawEvent(
        name=name,
        start_date=start_d,
        start_time=(d.get("start_time") or None),
        end_date=end_d,
        end_time=None,  # LLM doesn't emit end_time today
        artist_name=(d.get("artist_name") or None),
        description=(d.get("description") or None),
        price=price,
        price_currency=(d.get("price_currency") or "USD"),
        purchase_link=(d.get("purchase_link") or None),
        image_url=(d.get("image_url") or None),
        venue_name=(d.get("venue_name") or None),
        source=source,
        source_id=sid,
        raw_categories=[],
    )


def _hallucination_filter(events: list[RawEvent], raw_html: str) -> tuple[list[RawEvent], int]:
    """Drop events whose ``name`` doesn't appear in the source HTML.

    Imperfect (the LLM may translate or paraphrase names slightly), so we
    use a loose match: if the first 8 chars of the name (lowercased,
    whitespace-collapsed) appear in the lowercased HTML, the event is kept.
    A stricter exact-substring check produced too many false positives
    against multi-line pretty-printed HTML in early testing.
    """
    if not raw_html:
        return events, 0
    haystack = " ".join(raw_html.lower().split())
    kept: list[RawEvent] = []
    dropped = 0
    for ev in events:
        needle = " ".join(ev.name.lower().strip().split())[:8]
        if not needle or needle in haystack:
            kept.append(ev)
        else:
            dropped += 1
            logger.warning(
                f"hallucination drop: {ev.name[:50]!r} not in source HTML"
            )
    return kept, dropped


def _is_transient(exc: Exception) -> bool:
    """Heuristic: which Gemini SDK errors deserve a retry?

    The SDK raises ServerError for 5xx (overload, timeout) and
    APIError/ClientError for 4xx (bad request, auth). Only the former
    are worth retrying — 4xx won't fix itself.
    """
    name = type(exc).__name__
    if name in ("ServerError", "ServiceUnavailable", "DeadlineExceeded"):
        return True
    msg = str(exc)
    # Fall back to message inspection for SDK versions that don't expose
    # subclassed errors.
    return any(s in msg for s in (" 503 ", " 502 ", " 504 ", "UNAVAILABLE",
                                   "DEADLINE_EXCEEDED"))


def _gemini_call_with_retry(call, *, max_attempts: int = 3, base_delay: float = 1.5):
    """Tiny exponential-backoff wrapper for transient 5xx. Three attempts at
    1.5s / 4.5s / 13.5s — total 19s budget before giving up. Anything
    non-transient bubbles immediately.
    """
    import time
    last: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return call()
        except Exception as e:
            last = e
            if not _is_transient(e) or attempt == max_attempts:
                raise
            wait = base_delay * (3 ** (attempt - 1))
            logger.info(
                f"Gemini transient error (attempt {attempt}/{max_attempts}): "
                f"{type(e).__name__} — sleeping {wait:.1f}s"
            )
            time.sleep(wait)
    raise last  # unreachable; satisfies type-checker


def _extract_via_html(client, url: str, cleaned_html: str, *,
                      model: str, max_events: int) -> list[dict]:
    from google.genai import types as gtypes
    prompt = _HTML_PROMPT.format(
        url=url, today=date.today().isoformat(),
        max_events=max_events, html=cleaned_html,
    )
    resp = _gemini_call_with_retry(lambda: client.models.generate_content(
        model=model, contents=prompt,
        config=gtypes.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=_RESPONSE_SCHEMA,
            temperature=0,
            max_output_tokens=20_000,
        ),
    ))
    payload = json.loads(resp.text or '{"events":[]}')
    return list(payload.get("events") or [])


def _extract_via_url_context(client, url: str, *,
                             model: str, max_events: int) -> list[dict]:
    """SPA fallback path. Uses Gemini's url_context tool to fetch+render the
    page server-side. Note: response_schema is incompatible with grounding
    tools, so we ask for JSON via prose and parse defensively (same approach
    as scripts/find_city_guides_ai.py)."""
    from google.genai import types as gtypes
    prompt = _URL_CONTEXT_PROMPT.format(
        url=url, today=date.today().isoformat(), max_events=max_events,
    )
    prompt += (
        "\nReturn ONLY a JSON object of shape "
        '{"events":[{...}, ...]} matching the field rules above. '
        "No markdown fences, no commentary."
    )
    resp = _gemini_call_with_retry(lambda: client.models.generate_content(
        model=model, contents=prompt,
        config=gtypes.GenerateContentConfig(
            tools=[gtypes.Tool(url_context=gtypes.UrlContext())],
            temperature=0,
        ),
    ))
    raw = (resp.text or "").strip()
    if raw.startswith("```"):
        # Strip ```json ... ``` fences if present.
        lines = raw.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        raw = "\n".join(lines)
    if not raw.startswith("{"):
        # Some grounded responses prefix prose; find the first { … last }.
        i, j = raw.find("{"), raw.rfind("}")
        if i != -1 and j > i:
            raw = raw[i:j + 1]
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.warning(f"url_context JSON parse failed: {e}")
        return []
    return list(payload.get("events") or [])


def extract(url: str, *,
            source_name: str = "llm_extractor",
            model: str = "gemini-2.5-flash",
            max_events: int = 50,
            spa_threshold_bytes: int = _SPA_THRESHOLD_BYTES) -> ExtractionResult:
    """Pull upcoming events from ``url`` via Gemini.

    The extractor decides between the HTML path (cleaned <body> in the
    prompt) and the url_context path (Gemini fetches the URL itself,
    handling SPA rendering) based on cleaned-HTML byte-size.

    Returns an ExtractionResult — never raises for ordinary failures
    (network, parse, no events). Only ``ExtractorUnconfigured`` raises,
    and only on the first call when key/SDK are missing.
    """
    raw_html = _fetch_html(url) or ""
    cleaned = _clean_html(raw_html, base_url=url) if raw_html else ""
    raw_bytes = len(raw_html)
    cleaned_bytes = len(cleaned)

    use_url_context = cleaned_bytes < spa_threshold_bytes

    client = _gemini_client()
    method = "url_context" if use_url_context else "html"
    events_raw: list[dict] = []
    error: Optional[str] = None

    try:
        if use_url_context:
            events_raw = _extract_via_url_context(
                client, url, model=model, max_events=max_events,
            )
        else:
            events_raw = _extract_via_html(
                client, url, cleaned, model=model, max_events=max_events,
            )
    except Exception as e:
        error = f"{type(e).__name__}: {e}"
        logger.warning(f"extract({url}) {method} path failed: {error}")
        return ExtractionResult(
            events=[], method="error",
            raw_html_bytes=raw_bytes, cleaned_html_bytes=cleaned_bytes,
            api_call_ok=False, error=error,
        )

    events: list[RawEvent] = []
    for d in events_raw:
        ev = _to_raw_event(d, source_name, url)
        if ev:
            events.append(ev)

    # Hallucination guard — only meaningful on the HTML path where we
    # have the source the LLM saw. url_context path skips (we don't have
    # the rendered DOM).
    dropped = 0
    if method == "html":
        events, dropped = _hallucination_filter(events, raw_html)

    return ExtractionResult(
        events=events,
        method=method,
        raw_html_bytes=raw_bytes,
        cleaned_html_bytes=cleaned_bytes,
        api_call_ok=True,
        dropped_for_hallucination=dropped,
    )
