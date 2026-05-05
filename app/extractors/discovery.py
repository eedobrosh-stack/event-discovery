"""Gemini-grounded discovery — find candidate event-listing URLs per city.

Cadence B of Route 1: ask Gemini (with google_search grounding) to surface
local-language event sources for a city we want to cover. Returns a list
of candidate dicts; the caller (the discovery scheduler job) is
responsible for probing each candidate and registering the winners.

Public surface:
    discover_via_gemini(city, n=15, ..., excluded_domains, excluded_urls)
        -> list[dict]
    looks_like_event_listing(html, url) -> tuple[bool, str]

Each candidate dict has shape:
    {"url": str, "source_type": str, "language": str, "why_relevant": str}

Cost: one grounded Gemini call per city (~$0.005 on flash). At 50
priority cities × 1/month, well under $1/month.

Honest limitations to remember:
  • Gemini sometimes hallucinates URLs that 404 or never existed.
    The probe step downstream is the natural guard — bad URLs return
    nothing and don't get registered.
  • Local-language search quality depends on Gemini's training; works
    well for major languages, weaker for smaller markets.
  • Grounding tools and response_schema are mutually exclusive in the
    Gemini API, so we ask for JSON via prose and parse defensively.
"""
from __future__ import annotations

import json
import logging
import os
import re
from typing import Optional
from urllib.parse import urlsplit

logger = logging.getLogger(__name__)


_PROMPT_TEMPLATE = """\
You are an event-discovery research assistant. Find websites that publish
current event listings for a given city. We will scrape these sites for
event listings, so we want sites that publish structured calendars
(ideally schema.org/Event JSON-LD, but visible HTML lists are fine too).

City: {city}

Use Google Search to find:
- Official tourism boards / convention & visitors bureaus
- Local lifestyle / city-magazine sites with event calendars
- Aggregators specific to that city or region (Eventbrite-like local platforms)
- Local-language sources if the city's primary language is not English
- Major venues' event pages (concert halls, performance centers) ONLY if they
  publish structured listings

Avoid:
- Pure ticketing sites (Ticketmaster, StubHub) — too generic
- Generic global aggregators (Eventful, AllEvents.in) unless city-specific
- Forum/Reddit/social-media discussions about events
- News articles about events (we want listing pages, not articles)
{exclusion_block}
Return ONLY a JSON array of {n} candidates, no markdown fences, no commentary.
Each entry MUST have these fields:

  url           — full URL to the events listing page (e.g. /events/, /calendar/)
  source_type   — one of: "tourism_board" | "city_magazine" | "aggregator"
                          | "venue" | "newspaper" | "lifestyle"
  language      — ISO-639-1 code, e.g. "en", "pt", "he", "de"
  why_relevant  — one short sentence on why this site is worth scraping

Example:
[
  {{"url": "https://www.timeout.com/london/events", "source_type": "city_magazine",
    "language": "en", "why_relevant": "Time Out London publishes daily-curated event listings."}}
]
"""


def _build_exclusion_block(
    excluded_domains: Optional[list[str]],
    excluded_urls: Optional[list[str]],
) -> str:
    """Render a hard-negative constraint block for the prompt.

    Two lists, both optional:
      • excluded_domains — hosts we already cover with hand-coded collectors
      • excluded_urls    — full URLs already in our LLMSource registry

    Returns an empty string when both inputs are empty (or None) so the
    prompt template stays clean. When non-empty, returns a block that
    instructs Gemini explicitly to skip them. We cap the URL list at 60
    entries to keep the prompt under control as the registry grows.
    """
    blocks: list[str] = []
    if excluded_domains:
        cleaned_d = sorted({d.strip().lower() for d in excluded_domains if d and d.strip()})
        if cleaned_d:
            domain_lines = "\n".join(f"  - {d}" for d in cleaned_d)
            blocks.append(
                "Do NOT suggest any URL hosted on any of these domains "
                "(or their subdomains) — we already cover them with "
                "purpose-built collectors and re-discovery would be wasted "
                "work:\n" + domain_lines
            )
    if excluded_urls:
        cleaned_u = sorted({u.strip() for u in excluded_urls if u and u.strip()})
        if cleaned_u:
            # Cap to keep prompt size sane. The post-filter is the safety net
            # for any that slip through; this list is just to nudge Gemini
            # toward fresh ideas.
            head = cleaned_u[:60]
            url_lines = "\n".join(f"  - {u}" for u in head)
            tail = (f"\n  ...and {len(cleaned_u) - len(head)} more (pattern is clear)"
                    if len(cleaned_u) > len(head) else "")
            blocks.append(
                "Do NOT suggest any of these specific URLs — they are "
                "already in our registry:\n" + url_lines + tail
            )
    if not blocks:
        return ""
    return "\nHARD CONSTRAINTS — these take priority over the lists above:\n\n" + "\n\n".join(blocks) + "\n"


# ── Visible-content heuristic (lever 1: register candidates without JSON-LD) ──
#
# Gemini surfaces lots of legitimate event-listing pages that don't expose
# schema.org JSON-LD — JS-rendered tourism boards, Wordpress calendars,
# region-specific magazines. Today they get rejected at the probe step
# because count_events(html) returns 0. Lever 1: also accept candidates
# that LOOK like event-listing pages by visible signals, then let the LLM
# extractor handle the actual extraction. Drift detection prunes any
# speculative registrations that consistently extract 0 events.

_LISTING_PATH_RE = re.compile(
    r"/(events?|whats[-_]?on|what'?s[-_]?on|calendar|agenda|programme|program"
    r"|listings?|happenings?|things[-_]?to[-_]?do|veranstaltungen|eventos"
    r"|événements|évenements|evenementen|wydarzenia)(?:/|$)",
    re.I,
)

# Visible date strings in many forms. Errs toward catching real dates over
# avoiding false positives — false positives just mean we trial-onboard a
# slightly noisier source, which drift detection cleans up.
_DATE_STRING_RE = re.compile(
    r"(?:"
    # ISO yyyy-mm-dd
    r"\b\d{4}-\d{2}-\d{2}\b"
    # Month-name + day, both orders, en/de/es-light
    r"|\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\.?\s+\d{1,2}\b"
    r"|\b\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\.?\b"
    r"|\b(?:January|February|March|April|May|June|July|August|September"
    r"|October|November|December)\s+\d{1,2}\b"
    r"|\b\d{1,2}\s+(?:January|February|March|April|May|June|July|August"
    r"|September|October|November|December)\b"
    # Today / Tomorrow / Tonight in several languages
    r"|\b(?:Today|Tomorrow|Tonight|Heute|Morgen|Hoy|Mañana|Aujourd'?hui|Demain)\b"
    r")",
    re.I,
)

# Anchors that point at an event-detail URL. Matches /event/... or /events/...
# with an additional path component (so we don't match the index page itself).
_EVENT_HREF_RE = re.compile(r'href=["\'][^"\']*/events?/[^"\']+["\']', re.I)

_TAG_STRIP_RE = re.compile(r"<(script|style)\b[^>]*>.*?</\1>", re.DOTALL | re.I)
_ANY_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html_for_dates(html: str) -> str:
    """Remove <script>/<style> blocks and HTML tags so date counts reflect
    visible content, not embedded JS or CSS. Cheap and approximate — full
    HTML parsing would be overkill for a heuristic."""
    no_scripts = _TAG_STRIP_RE.sub(" ", html)
    return _ANY_TAG_RE.sub(" ", no_scripts)


def looks_like_event_listing(html: str, url: str) -> tuple[bool, str]:
    """Heuristic: does this page LOOK like an event-listing page?

    Used to register discovery candidates that lack JSON-LD events but
    still publish events as visible HTML. Two independent gates — a
    page passes if either fires:

      • path-and-dates    URL path strongly suggests a calendar
                          AND ≥3 visible date-shaped strings
      • anchors-and-dates ≥10 event-detail anchors
                          AND ≥5 visible date-shaped strings

    The thresholds are deliberately permissive — drift detection is the
    backstop for false positives (3 consecutive empty extractions blocks
    the source).

    Returns (passes, reason) so callers can record which signal fired in
    LLMSource.notes for later debugging.
    """
    if not html:
        return False, "empty html"

    path = urlsplit(url).path or ""
    path_match = bool(_LISTING_PATH_RE.search(path))
    visible_text = _strip_html_for_dates(html)
    date_count = len(_DATE_STRING_RE.findall(visible_text))
    anchor_count = len(_EVENT_HREF_RE.findall(html))

    if path_match and date_count >= 3:
        return True, f"path+dates (dates={date_count})"
    if anchor_count >= 10 and date_count >= 5:
        return True, f"anchors+dates (anchors={anchor_count}, dates={date_count})"
    return (
        False,
        f"no-signal (path={path_match}, dates={date_count}, anchors={anchor_count})",
    )


class DiscoveryError(RuntimeError):
    """Raised on unrecoverable discovery failures (no key, malformed response,
    network exhaustion). Caller should log and continue with the next city
    rather than abort the whole sweep."""


def discover_via_gemini(
    city: str,
    n: int = 15,
    model: str = "gemini-2.5-flash",
    excluded_domains: Optional[list[str]] = None,
    excluded_urls: Optional[list[str]] = None,
) -> list[dict]:
    """Ask Gemini for ``n`` event-listing candidates for ``city``.

    Returns the parsed candidate list (may be shorter than n if Gemini's
    response had fewer entries). Empty list on parse failure or empty
    response — does not raise to keep the discovery sweep moving past a
    single broken city.

    Raises DiscoveryError if the API key / SDK is missing — that's an
    operator-config issue worth surfacing, not a per-city problem.

    ``excluded_domains`` and ``excluded_urls`` are passed to Gemini as a
    hard-negative constraint in the prompt. Used to keep Gemini from
    spending its candidate budget re-suggesting sites we already cover
    (hand-coded collectors) or already have in our LLMSource registry.
    LLM compliance with negative lists is imperfect, so the caller still
    needs the post-filter as a safety net — but the in-prompt list shifts
    the candidate distribution toward genuinely fresh sources.
    """
    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        raise DiscoveryError(
            "GEMINI_API_KEY (or GOOGLE_API_KEY) is not set; discovery cannot run."
        )
    try:
        from google import genai
        from google.genai import types as gtypes
    except ImportError as e:
        raise DiscoveryError(
            "google-genai SDK not installed (add to requirements.txt)."
        ) from e

    client = genai.Client(api_key=api_key)
    exclusion_block = _build_exclusion_block(excluded_domains, excluded_urls)
    prompt = _PROMPT_TEMPLATE.format(city=city, n=n, exclusion_block=exclusion_block)

    logger.info(f"discover_via_gemini: asking Gemini ({model}) for {n} candidates for {city!r}")
    try:
        resp = client.models.generate_content(
            model=model,
            contents=prompt,
            config=gtypes.GenerateContentConfig(
                tools=[gtypes.Tool(google_search=gtypes.GoogleSearch())],
                # Low but not zero — Gemini benefits from a sliver of
                # exploration when picking which search results to follow.
                temperature=0.2,
            ),
        )
    except Exception as e:
        logger.warning(f"discover_via_gemini({city!r}): API call failed: {type(e).__name__}: {e}")
        return []

    raw = (resp.text or "").strip()
    if not raw:
        return []

    # Defensive parse — grounding tools + response_schema are mutually
    # exclusive so we get prose+JSON. Strip optional ```json fences and
    # bracket-extract on prose prefixes.
    if raw.startswith("```"):
        lines = raw.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        raw = "\n".join(lines)
    if not raw.startswith("["):
        i, j = raw.find("["), raw.rfind("]")
        if i != -1 and j > i:
            raw = raw[i:j + 1]

    try:
        candidates = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.warning(
            f"discover_via_gemini({city!r}): JSON parse failed: {e}; "
            f"first 200 chars: {raw[:200]!r}"
        )
        return []
    if not isinstance(candidates, list):
        logger.warning(
            f"discover_via_gemini({city!r}): expected JSON array, "
            f"got {type(candidates).__name__}"
        )
        return []

    # Audit: log what Gemini actually searched for, useful when investigating
    # poor-quality results for a given city.
    try:
        gm = getattr(resp.candidates[0], "grounding_metadata", None)
        if gm and getattr(gm, "web_search_queries", None):
            logger.info(
                f"discover_via_gemini({city!r}): search queries: "
                f"{list(gm.web_search_queries)}"
            )
    except Exception:
        pass

    # Drop entries missing the only required field — url. Anything else
    # missing is recoverable; the URL is essential.
    filtered = [c for c in candidates if isinstance(c, dict) and c.get("url")]
    logger.info(
        f"discover_via_gemini({city!r}): "
        f"{len(filtered)}/{len(candidates)} candidates after url filter"
    )
    return filtered
