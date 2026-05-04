"""Gemini-grounded discovery — find candidate event-listing URLs per city.

Cadence B of Route 1: ask Gemini (with google_search grounding) to surface
local-language event sources for a city we want to cover. Returns a list
of candidate dicts; the caller (the discovery scheduler job) is
responsible for probing each candidate and registering the winners.

Public surface:
    discover_via_gemini(city, n=15, model='gemini-2.5-flash') -> list[dict]

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

logger = logging.getLogger(__name__)


_PROMPT_TEMPLATE = """\
You are an event-discovery research assistant. Find websites that publish
current event listings for a given city. We will scrape these sites for
schema.org/Event JSON-LD blocks, so we want sites that are likely to
expose structured event data on individual events pages.

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


class DiscoveryError(RuntimeError):
    """Raised on unrecoverable discovery failures (no key, malformed response,
    network exhaustion). Caller should log and continue with the next city
    rather than abort the whole sweep."""


def discover_via_gemini(
    city: str,
    n: int = 15,
    model: str = "gemini-2.5-flash",
) -> list[dict]:
    """Ask Gemini for ``n`` event-listing candidates for ``city``.

    Returns the parsed candidate list (may be shorter than n if Gemini's
    response had fewer entries). Empty list on parse failure or empty
    response — does not raise to keep the discovery sweep moving past a
    single broken city.

    Raises DiscoveryError if the API key / SDK is missing — that's an
    operator-config issue worth surfacing, not a per-city problem.
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
    prompt = _PROMPT_TEMPLATE.format(city=city, n=n)

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
