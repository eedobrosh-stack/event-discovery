"""Hybrid Google CSE + LLM-classifier discovery — no hallucinated URLs.

Cadence B alternative to ``app.extractors.discovery``: instead of asking
Gemini to *generate* candidate URLs (which it sometimes hallucinates),
we (a) fire themed Google Custom Search queries to get *real* indexed
URLs and (b) ask Gemini to *classify* which results are event-listing
pages.

Public surface:
    discover_via_cse_pipeline(city, ..., excluded_domains, excluded_urls)
        -> list[dict]    # same shape as discover_via_gemini()

    cse_search(query, n=10)                -> list[CseHit]
    discover_via_cse(city, ...)            -> list[CseHit]
    filter_candidates_via_llm(hits, city)  -> list[dict]

Each candidate dict (final output) has the same shape used by callers of
the original discover_via_gemini:
    {"url": str, "source_type": str, "language": str, "why_relevant": str}

Cost outline (10 cities × 4 queries × 30 days):
    CSE   : 1,200 queries × $5/1000 ≈ $6/month
    Gemini: 30 classification calls × ~$0.005 ≈ $0.15/month

Setup requirement (operator, one-time):
    1. Create a Programmable Search Engine at
       https://programmablesearchengine.google.com/, set to "search the
       entire web". Copy the Search Engine ID.
    2. Enable the "Custom Search API" in your Google Cloud project.
    3. Set ``GOOGLE_CSE_ID=<id>`` in .env. Reuse the existing
       ``GEMINI_API_KEY`` (or set ``GOOGLE_API_KEY`` if your CSE key
       differs from your Gemini key).

If CSE isn't configured, ``discover_via_cse_pipeline`` raises
DiscoveryError so the scheduler can fall back to the original
Gemini-grounded path without aborting the whole sweep.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, asdict
from typing import Optional

import urllib.parse
import urllib.request

from app.extractors.discovery import DiscoveryError

logger = logging.getLogger(__name__)


# ── CSE call ────────────────────────────────────────────────────────────────

_CSE_ENDPOINT = "https://www.googleapis.com/customsearch/v1"


@dataclass
class CseHit:
    """One result from Google Programmable Search.

    Three fields are all we need for downstream classification:
      url      : canonical link to the page
      title    : page title from CSE
      snippet  : Google's short text excerpt
    """
    url: str
    title: str
    snippet: str


def _cse_credentials() -> tuple[str, str]:
    """Resolve API key + CSE ID from env, or raise DiscoveryError.

    Reuses GEMINI_API_KEY by default. Operators with separate keys can
    set GOOGLE_API_KEY to override. Either path works because both endpoints
    are gated by the same Cloud project's API key restrictions.
    """
    api_key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise DiscoveryError(
            "Neither GOOGLE_API_KEY nor GEMINI_API_KEY is set; CSE cannot run."
        )
    cse_id = os.environ.get("GOOGLE_CSE_ID")
    if not cse_id:
        raise DiscoveryError(
            "GOOGLE_CSE_ID is not set. Create a Programmable Search Engine "
            "at https://programmablesearchengine.google.com/ and put the "
            "ID in your .env."
        )
    return api_key, cse_id


def cse_search(query: str, n: int = 10) -> list[CseHit]:
    """Fire one Google CSE query and return up to ``n`` parsed hits.

    Empty list on quota exhaustion or transient errors — we don't want a
    single bad query to abort a full city sweep.

    Free tier is 100 queries/day; paid tier is $5/1000. Operators using
    discovery at scale need the paid tier enabled in Google Cloud.
    """
    api_key, cse_id = _cse_credentials()
    # CSE caps at 10 results per page. Caller can pass higher n; we cap.
    n = max(1, min(n, 10))
    params = {
        "key": api_key,
        "cx": cse_id,
        "q": query,
        "num": n,
        "safe": "off",
    }
    url = f"{_CSE_ENDPOINT}?{urllib.parse.urlencode(params)}"

    try:
        with urllib.request.urlopen(url, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        # 429 = quota exhausted. 403 = API not enabled / wrong key.
        # Both are operator-config issues — surface them to the log but
        # don't abort the sweep; the caller sees an empty list.
        body = ""
        try:
            body = e.read().decode("utf-8")[:300]
        except Exception:
            pass
        logger.warning(
            f"cse_search({query!r}): HTTP {e.code}: {e.reason} — {body}"
        )
        return []
    except Exception as e:
        logger.warning(f"cse_search({query!r}): {type(e).__name__}: {e}")
        return []

    items = data.get("items") or []
    hits: list[CseHit] = []
    for it in items:
        link = (it.get("link") or "").strip()
        if not link:
            continue
        hits.append(CseHit(
            url=link,
            title=(it.get("title") or "").strip(),
            snippet=(it.get("snippet") or "").strip(),
        ))
    return hits


# ── Per-city query templates ───────────────────────────────────────────────
#
# Themed to maximise event-listing surface area while minimising overlap:
# calendar/what's-on hits the listing pages directly, "things to do this
# month" finds curated weekly digests, the venue/arts queries pick up
# performing-arts complexes that magazines miss. The "2026" hint on the
# first query nudges Google to favour recent pages.
#
# Local-language coverage comes from Google's regional ranking — for
# Berlin, even the English query "Berlin events calendar" tends to
# surface visitberlin.de in the results. If yield is poor for non-English
# cities we'll add per-city local-language templates.

_QUERY_TEMPLATES: list[str] = [
    "{city} events calendar 2026",
    "{city} what's on this month",
    "{city} live music venues calendar",
    "{city} arts and culture events",
]


def discover_via_cse(
    city: str,
    n_queries: Optional[int] = None,
    n_per_query: int = 10,
) -> list[CseHit]:
    """Fire all query templates for ``city``, dedupe results by URL.

    Returns the union (deduped) of hits across queries. Order is roughly
    "first-seen wins" so the highest-ranked results from the first query
    dominate when we cap downstream.
    """
    templates = _QUERY_TEMPLATES if n_queries is None else _QUERY_TEMPLATES[:n_queries]

    seen: set[str] = set()
    out: list[CseHit] = []
    for tpl in templates:
        query = tpl.format(city=city)
        logger.info(f"cse: query {query!r}")
        for hit in cse_search(query, n=n_per_query):
            if hit.url in seen:
                continue
            seen.add(hit.url)
            out.append(hit)
    logger.info(
        f"cse({city!r}): {len(out)} unique hits across "
        f"{len(templates)} queries"
    )
    return out


# ── LLM-classifier filter ──────────────────────────────────────────────────

_FILTER_PROMPT = """\
You are reviewing Google search results for {city} event-listing pages.

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


def _format_results_block(hits: list[CseHit]) -> str:
    """Render the numbered hit list for the LLM prompt."""
    lines: list[str] = []
    for i, h in enumerate(hits, start=1):
        # Bound snippet length to keep the prompt size predictable.
        snippet = (h.snippet or "")[:280].replace("\n", " ").strip()
        lines.append(f"[{i}] URL: {h.url}\n    Title: {h.title}\n    Snippet: {snippet}")
    return "\n\n".join(lines)


def filter_candidates_via_llm(
    hits: list[CseHit],
    city: str,
    model: str = "gemini-2.5-flash",
) -> list[dict]:
    """Classify which CSE hits are real event-listing pages for ``city``.

    Returns candidate dicts in the same shape as discover_via_gemini():
        {"url", "source_type", "language", "why_relevant"}

    The LLM only returns indices + classification metadata — we look up
    the URL ourselves so the model can't hallucinate a slightly-different
    URL than what Google indexed.

    Empty list on parse failure or empty input — keeps the city loop moving.
    """
    if not hits:
        return []

    api_key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise DiscoveryError(
            "Neither GOOGLE_API_KEY nor GEMINI_API_KEY is set; "
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
        f"llm-filter({city!r}): classifying {len(hits)} CSE hits via {model}"
    )
    try:
        # No grounding tool here — we have the URLs already, we just need
        # the classifier. Strict JSON via response_schema is preferred
        # (and unlike the search-grounded call, response_schema is allowed
        # in this no-tools mode).
        resp = client.models.generate_content(
            model=model,
            contents=prompt,
            config=gtypes.GenerateContentConfig(
                temperature=0.0,
                response_mime_type="application/json",
            ),
        )
    except Exception as e:
        logger.warning(
            f"filter_candidates_via_llm({city!r}): "
            f"API call failed: {type(e).__name__}: {e}"
        )
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
            # Stash the original CSE title/snippet under a private key
            # so callers (and the audit dashboard) can show provenance.
            "_cse_title": hit.title,
            "_cse_snippet": hit.snippet,
        })
    logger.info(
        f"llm-filter({city!r}): "
        f"{len(out)}/{len(hits)} hits accepted as event-listing pages"
    )
    return out


# ── Orchestrator (drop-in for discover_via_gemini) ─────────────────────────

def discover_via_cse_pipeline(
    city: str,
    n: int = 15,                         # noqa: ARG001 — kept for signature parity
    model: str = "gemini-2.5-flash",
    excluded_domains: Optional[list[str]] = None,
    excluded_urls: Optional[list[str]] = None,
    n_per_query: int = 10,
) -> list[dict]:
    """End-to-end: CSE queries → dedupe → reserved/url filter → LLM classify.

    Returns candidate dicts in the same shape as discover_via_gemini, so
    the scheduler job can use either path with no further branching.

    The exclusion lists are applied locally (not in the LLM prompt) —
    classification only sees URLs that haven't already been pruned, which
    keeps the prompt tight and the per-call cost predictable.

    ``n`` is accepted for signature compatibility with the Gemini path
    but ignored — CSE is governed by ``n_per_query`` × len(query templates),
    which we calibrate elsewhere.
    """
    excl_d = {d.lower() for d in (excluded_domains or []) if d}
    excl_u = set(excluded_urls or [])

    hits = discover_via_cse(city, n_per_query=n_per_query)

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

    pruned: list[CseHit] = []
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
            f"cse({city!r}): pruned {n_excl_d} reserved-domain "
            f"+ {n_excl_u} already-registered hits before LLM"
        )

    if not pruned:
        return []

    return filter_candidates_via_llm(pruned, city, model=model)


__all__ = [
    "CseHit",
    "cse_search",
    "discover_via_cse",
    "filter_candidates_via_llm",
    "discover_via_cse_pipeline",
]
