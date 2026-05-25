"""Hybrid Brave Search + LLM-classifier discovery — no hallucinated URLs.

Cadence B alternative to ``app.extractors.discovery``: instead of asking
Gemini to *generate* candidate URLs (which it sometimes hallucinates),
we (a) fire themed Brave Search queries to get *real* indexed URLs and
(b) ask Gemini to *classify* which results are event-listing pages.

Public surface:
    discover_via_search_pipeline(city, ..., excluded_domains, excluded_urls)
        -> list[dict]    # same shape as discover_via_gemini()

    brave_search(query, n=10)              -> list[SearchHit]
    discover_via_search(city, ...)         -> list[SearchHit]
    filter_candidates_via_llm(hits, city)  -> list[dict]

Each candidate dict (final output) has the same shape used by callers of
the original discover_via_gemini:
    {"url": str, "source_type": str, "language": str, "why_relevant": str}

Why Brave (and not Google CSE):
    Google deprecated whole-web Programmable Search Engines for new
    accounts in 2024 — the "Search the entire web" toggle is permanently
    disabled in their modern UI, and we hit that wall during setup. Brave
    runs its own crawler and exposes whole-web results via a clean API
    with no console gauntlet (one API key, no project linkage).

Cost outline (10 cities × 4 queries × 30 days):
    Brave : 1,200 queries × $3/1000 ≈ $3.60/month (free up to 2k/mo)
    Gemini: 30 classification calls × ~$0.005 ≈ $0.15/month

Setup requirement (operator, one-time):
    1. Sign up at https://api.search.brave.com/, copy the API key.
    2. Set ``BRAVE_API_KEY=<key>`` in .env. Free tier (2k queries/mo) is
       enough for our scale; bump to a paid plan if you scale beyond
       ~70 queries/day.

If Brave isn't configured, ``discover_via_search_pipeline`` raises
DiscoveryError so the scheduler can fall back to the original
Gemini-grounded path without aborting the whole sweep.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Optional

import urllib.parse
import urllib.request

from app.extractors.discovery import DiscoveryError

logger = logging.getLogger(__name__)


# ── Brave Search API call ──────────────────────────────────────────────────

_BRAVE_ENDPOINT = "https://api.search.brave.com/res/v1/web/search"


@dataclass
class SearchHit:
    """One result from Brave Search.

    Three fields are all we need for downstream classification:
      url      : canonical link to the page
      title    : page title from Brave
      snippet  : Brave's short text excerpt (their "description" field)
    """
    url: str
    title: str
    snippet: str


def _brave_credentials() -> str:
    """Resolve API key from env, or raise DiscoveryError."""
    api_key = os.environ.get("BRAVE_API_KEY")
    if not api_key:
        raise DiscoveryError(
            "BRAVE_API_KEY is not set. Sign up at "
            "https://api.search.brave.com/ and put the key in your .env."
        )
    return api_key


def brave_search(query: str, n: int = 10) -> list[SearchHit]:
    """Fire one Brave Search query and return up to ``n`` parsed hits.

    Empty list on quota exhaustion or transient errors — we don't want a
    single bad query to abort a full city sweep.

    Free tier is 2,000 queries/month at 1 query/second; "Data for AI" paid
    plans start at $3/1k. We don't add explicit rate limiting here because
    Cadence B's per-city sequential loop naturally stays under 1 q/s.
    """
    api_key = _brave_credentials()
    n = max(1, min(n, 20))  # Brave's max page size is 20.
    params = {
        "q": query,
        "count": n,
        "safesearch": "off",
    }
    url = f"{_BRAVE_ENDPOINT}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "X-Subscription-Token": api_key,
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        # 429 = quota exhausted. 401 = bad/missing key. 422 = malformed
        # query. All operator-config issues — surface them to the log
        # but don't abort the sweep; the caller sees an empty list.
        body = ""
        try:
            body = e.read().decode("utf-8")[:300]
        except Exception:
            pass
        logger.warning(
            f"brave_search({query!r}): HTTP {e.code}: {e.reason} — {body}"
        )
        return []
    except Exception as e:
        logger.warning(f"brave_search({query!r}): {type(e).__name__}: {e}")
        return []

    web = data.get("web") or {}
    items = web.get("results") or []
    hits: list[SearchHit] = []
    for it in items:
        link = (it.get("url") or "").strip()
        if not link:
            continue
        hits.append(SearchHit(
            url=link,
            title=(it.get("title") or "").strip(),
            # Brave names this 'description'; we keep the more conventional
            # 'snippet' on the dataclass for cross-engine consistency.
            snippet=(it.get("description") or "").strip(),
        ))
    return hits


# ── Per-city query templates ───────────────────────────────────────────────
#
# Themed to maximise event-listing surface area while minimising overlap:
# calendar/what's-on hits the listing pages directly, "things to do this
# month" finds curated weekly digests, the venue/arts queries pick up
# performing-arts complexes that magazines miss. The "2026" hint on the
# first query nudges Brave to favour recent pages.
#
# Local-language coverage comes from Brave's regional ranking — for
# Berlin, even the English query "Berlin events calendar" tends to
# surface visitberlin.de in the results. If yield is poor for non-English
# cities we'll add per-city local-language templates.

_QUERY_TEMPLATES: list[str] = [
    "{city} events calendar 2026",
    "{city} what's on this month",
    "{city} live music venues calendar",
    "{city} arts and culture events",
]


def discover_via_search(
    city: str,
    n_queries: Optional[int] = None,
    n_per_query: int = 10,
) -> list[SearchHit]:
    """Fire all query templates for ``city``, dedupe results by URL.

    Returns the union (deduped) of hits across queries. Order is roughly
    "first-seen wins" so the highest-ranked results from the first query
    dominate when we cap downstream.
    """
    templates = _QUERY_TEMPLATES if n_queries is None else _QUERY_TEMPLATES[:n_queries]

    seen: set[str] = set()
    out: list[SearchHit] = []
    for tpl in templates:
        query = tpl.format(city=city)
        logger.info(f"brave: query {query!r}")
        for hit in brave_search(query, n=n_per_query):
            if hit.url in seen:
                continue
            seen.add(hit.url)
            out.append(hit)
    logger.info(
        f"brave({city!r}): {len(out)} unique hits across "
        f"{len(templates)} queries"
    )
    return out


# ── LLM-classifier filter ──────────────────────────────────────────────────

_FILTER_PROMPT = """\
You are reviewing search results for {city} event-listing pages.

For each numbered result, decide if the page is likely a CALENDAR or
LISTING of upcoming events in {city} (or covers {city} as part of a
regional events listing).

ACCEPT if:
  - URL path / title suggests a calendar, "what's on", or events listing
  - Snippet describes upcoming events at this site
  - It is a city tourism board, arts venue, or local magazine event hub

REJECT if:
  - It is a single article ABOUT events, not a listing
  - It is a forum / Reddit / social-media post
  - It is a single-event ticket booking page (we want listings, not one event)
  - It clearly does not cover {city}
  - It is a generic global aggregator we already cover (Eventbrite,
    Meetup, Ticketmaster, Lu.ma, Resident Advisor, AllEvents, etc.)

Results to review:
{results_block}

Return ONLY a JSON array describing accepted results, no markdown fences,
no commentary. Each entry MUST have these fields:

  index         — the 1-based number of the accepted result
  source_type   — one of: "tourism_board" | "city_magazine" | "aggregator"
                          | "venue" | "newspaper" | "lifestyle"
  language      — ISO-639-1 code, e.g. "en", "de", "es"
  why_relevant  — one short sentence on why this site is worth scraping

Example:
[
  {{"index": 1, "source_type": "tourism_board", "language": "de",
    "why_relevant": "Official visitBerlin events portal."}}
]
"""


def _format_results_block(hits: list[SearchHit]) -> str:
    """Render the numbered hit list for the LLM prompt."""
    lines: list[str] = []
    for i, h in enumerate(hits, start=1):
        # Bound snippet length to keep the prompt size predictable.
        snippet = (h.snippet or "")[:280].replace("\n", " ").strip()
        lines.append(f"[{i}] URL: {h.url}\n    Title: {h.title}\n    Snippet: {snippet}")
    return "\n\n".join(lines)


def filter_candidates_via_llm(
    hits: list[SearchHit],
    city: str,
    model: str = "gemini-2.5-flash",
) -> list[dict]:
    """Classify which search hits are real event-listing pages for ``city``.

    Returns candidate dicts in the same shape as discover_via_gemini():
        {"url", "source_type", "language", "why_relevant"}

    The LLM only returns indices + classification metadata — we look up
    the URL ourselves so the model can't hallucinate a slightly-different
    URL than what the search engine indexed.

    Empty list on parse failure or empty input — keeps the city loop moving.
    """
    if not hits:
        return []

    # GEMINI_API_KEY first (it's the dedicated Gemini key); GOOGLE_API_KEY
    # is a fallback for setups that share one Google Cloud key across
    # services. Reversing this caused 403s when GOOGLE_API_KEY was a
    # leftover key without generativelanguage permission.
    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        raise DiscoveryError(
            "Neither GEMINI_API_KEY nor GOOGLE_API_KEY is set; "
            "LLM classifier cannot run."
        )
    try:
        from google import genai
        from google.genai import types as gtypes
    except ImportError as e:
        raise DiscoveryError(
            "google-genai SDK not installed (add to requirements.txt)."
        ) from e

    client = genai.Client(api_key=api_key)
    prompt = _FILTER_PROMPT.format(
        city=city,
        results_block=_format_results_block(hits),
    )
    logger.info(
        f"llm-filter({city!r}): classifying {len(hits)} search hits via {model}"
    )

    # Gemini Flash returns frequent transient 503 ("high demand") errors —
    # we observed ~75% of cities hitting one in a single sweep during
    # peak hours. A short retry with backoff turns those from yield-loss
    # into a tiny added latency (each retry adds at most ~10s).
    import time
    resp = None
    last_err: Optional[Exception] = None
    for attempt in range(3):
        try:
            resp = client.models.generate_content(
                model=model,
                contents=prompt,
                config=gtypes.GenerateContentConfig(
                    temperature=0.0,
                    response_mime_type="application/json",
                ),
            )
            break
        except Exception as e:
            last_err = e
            # Only retry on transient classes — 503 / quota / timeout.
            # Bad-request / unauthorized errors won't recover.
            msg = str(e).lower()
            transient = ("503" in msg or "unavailable" in msg
                         or "timeout" in msg or "deadline" in msg
                         or "resource_exhausted" in msg)
            if not transient or attempt == 2:
                logger.warning(
                    f"filter_candidates_via_llm({city!r}): "
                    f"API call failed (attempt {attempt + 1}/3): "
                    f"{type(e).__name__}: {e}"
                )
                return []
            backoff = 2 ** attempt * 2  # 2s, 4s
            logger.info(
                f"filter_candidates_via_llm({city!r}): "
                f"transient {type(e).__name__} on attempt {attempt + 1}, "
                f"retrying in {backoff}s"
            )
            time.sleep(backoff)
    if resp is None:
        # All retries exhausted — last_err already logged above.
        return []

    raw = (resp.text or "").strip()
    if not raw:
        return []
    try:
        decisions = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.warning(
            f"filter_candidates_via_llm({city!r}): JSON parse failed: {e}; "
            f"first 200 chars: {raw[:200]!r}"
        )
        return []
    if not isinstance(decisions, list):
        logger.warning(
            f"filter_candidates_via_llm({city!r}): "
            f"expected JSON array, got {type(decisions).__name__}"
        )
        return []

    out: list[dict] = []
    for d in decisions:
        if not isinstance(d, dict):
            continue
        try:
            idx = int(d.get("index", 0))
        except (TypeError, ValueError):
            continue
        # 1-based; outside-range indices = LLM mis-numbered → skip
        if not (1 <= idx <= len(hits)):
            continue
        hit = hits[idx - 1]
        out.append({
            "url": hit.url,
            "source_type": d.get("source_type") or "unknown",
            "language": d.get("language") or "en",
            "why_relevant": (d.get("why_relevant") or "")[:300],
            # Stash the original search title/snippet under a private key
            # so callers (and the audit dashboard) can show provenance.
            "_search_title": hit.title,
            "_search_snippet": hit.snippet,
        })
    logger.info(
        f"llm-filter({city!r}): "
        f"{len(out)}/{len(hits)} hits accepted as event-listing pages"
    )
    return out


# ── Orchestrator (drop-in for discover_via_gemini) ─────────────────────────

def discover_via_search_pipeline(
    city: str,
    n: int = 15,                         # noqa: ARG001 — kept for signature parity
    model: str = "gemini-2.5-flash",
    excluded_domains: Optional[list[str]] = None,
    excluded_urls: Optional[list[str]] = None,
    n_per_query: int = 10,
) -> list[dict]:
    """End-to-end: search queries → dedupe → reserved/url filter → LLM classify.

    Returns candidate dicts in the same shape as discover_via_gemini, so
    the scheduler job can use either path with no further branching.

    The exclusion lists are applied locally (not in the LLM prompt) —
    classification only sees URLs that haven't already been pruned, which
    keeps the prompt tight and the per-call cost predictable.

    ``n`` is accepted for signature compatibility with the Gemini path
    but ignored — search volume is governed by ``n_per_query`` × len(query
    templates), which we calibrate elsewhere.
    """
    excl_d = {d.lower() for d in (excluded_domains or []) if d}
    excl_u = set(excluded_urls or [])

    hits = discover_via_search(city, n_per_query=n_per_query)

    def _host_excluded(u: str) -> bool:
        try:
            host = (urllib.parse.urlsplit(u).hostname or "").lower()
        except Exception:
            return False
        if not host:
            return False
        if host.startswith("www."):
            host = host[4:]
        if host in excl_d:
            return True
        parts = host.split(".")
        for i in range(1, len(parts) - 1):
            if ".".join(parts[i:]) in excl_d:
                return True
        return False

    pruned: list[SearchHit] = []
    n_excl_d = n_excl_u = 0
    for h in hits:
        if h.url in excl_u:
            n_excl_u += 1
            continue
        if _host_excluded(h.url):
            n_excl_d += 1
            continue
        pruned.append(h)
    if n_excl_d or n_excl_u:
        logger.info(
            f"brave({city!r}): pruned {n_excl_d} reserved-domain "
            f"+ {n_excl_u} already-registered hits before LLM"
        )

    if not pruned:
        return []

    return filter_candidates_via_llm(pruned, city, model=model)


_ARTIST_TOUR_FILTER_PROMPT = """\
You are reviewing search results triggered by the query "{artist} shows" /
"{artist} upcoming performances". The goal is to find pages that LIST
events — not biographical pages, social-media posts, or streaming links.

ACCEPT if:
  - URL path / title looks like a tour-date listing, calendar, or
    upcoming-events grid (artist OR multi-artist).
  - Snippet mentions concrete event/tour dates, venues, or "buy tickets".
  - It is an indie ticketing site, venue calendar, or city events hub
    that surfaced because it lists this artist.

REJECT if:
  - It is a Wikipedia / Bandcamp / Spotify / YouTube / Genius / Apple
    Music page (we already have those signals).
  - It is a single article ABOUT the artist (news, profile, review).
  - It is a fan forum / Reddit / Twitter / Facebook / Instagram page.
  - It is a generic global aggregator we already cover (Ticketmaster,
    Bandsintown, Eventbrite, ResidentAdvisor, Meetup, Lu.ma, Songkick,
    Dice, Skiddle, AllEvents, Xceed).
  - It clearly does not list events (lyric site, merch store).

Results to review:
{results_block}

Return ONLY a JSON array describing accepted results, no markdown fences,
no commentary. Each entry MUST have:

  index         — 1-based number of the accepted result
  source_type   — one of: "tour_listing" | "venue" | "ticketing"
                          | "city_magazine" | "tourism_board" | "other"
  language      — ISO-639-1 code
  why_relevant  — one short sentence

Example:
[
  {{"index": 2, "source_type": "venue", "language": "en",
    "why_relevant": "Local venue's upcoming-shows calendar."}}
]
"""


def filter_artist_tour_pages_via_llm(
    hits: list[SearchHit],
    artist: str,
    model: str = "gemini-2.5-flash",
) -> list[dict]:
    """Same shape as ``filter_candidates_via_llm`` but framed for the
    artist-driven Brave query in spotify_brave_query_job.

    Accepts tour-date pages, venue calendars, ticketing sites, and city
    event hubs that surfaced for the artist query. Rejects single-artist
    bio/news/social pages and aggregators we already cover.
    """
    if not hits:
        return []

    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        raise DiscoveryError(
            "Neither GEMINI_API_KEY nor GOOGLE_API_KEY is set; "
            "LLM classifier cannot run."
        )
    try:
        from google import genai
        from google.genai import types as gtypes
    except ImportError as e:
        raise DiscoveryError(
            "google-genai SDK not installed (add to requirements.txt)."
        ) from e

    client = genai.Client(api_key=api_key)
    prompt = _ARTIST_TOUR_FILTER_PROMPT.format(
        artist=artist,
        results_block=_format_results_block(hits),
    )
    logger.info(
        f"filter_artist_tour_pages_via_llm({artist!r}): "
        f"classifying {len(hits)} search hits via {model}"
    )

    # Same transient-503 retry pattern as the city classifier.
    import time
    resp = None
    for attempt in range(3):
        try:
            resp = client.models.generate_content(
                model=model,
                contents=prompt,
                config=gtypes.GenerateContentConfig(
                    temperature=0.0,
                    response_mime_type="application/json",
                ),
            )
            break
        except Exception as e:
            msg = str(e).lower()
            transient = ("503" in msg or "unavailable" in msg
                         or "timeout" in msg or "deadline" in msg
                         or "resource_exhausted" in msg)
            if not transient or attempt == 2:
                logger.warning(
                    f"filter_artist_tour_pages_via_llm({artist!r}): "
                    f"API call failed (attempt {attempt + 1}/3): "
                    f"{type(e).__name__}: {e}"
                )
                return []
            time.sleep(2 ** attempt * 2)
    if resp is None:
        return []

    raw = (resp.text or "").strip()
    if not raw:
        return []
    try:
        decisions = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.warning(
            f"filter_artist_tour_pages_via_llm({artist!r}): JSON parse failed: {e}; "
            f"first 200 chars: {raw[:200]!r}"
        )
        return []
    if not isinstance(decisions, list):
        return []

    out: list[dict] = []
    for d in decisions:
        if not isinstance(d, dict):
            continue
        try:
            idx = int(d.get("index", 0))
        except (TypeError, ValueError):
            continue
        if not (1 <= idx <= len(hits)):
            continue
        out.append({
            "url": hits[idx - 1].url,
            "source_type": d.get("source_type") or "other",
            "language": d.get("language") or "en",
            "why_relevant": d.get("why_relevant") or "",
        })
    return out


__all__ = [
    "SearchHit",
    "brave_search",
    "discover_via_search",
    "filter_candidates_via_llm",
    "filter_artist_tour_pages_via_llm",
    "discover_via_search_pipeline",
]
