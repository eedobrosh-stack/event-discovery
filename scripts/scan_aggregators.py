"""
scan_aggregators.py — triage candidate event-aggregator sites

Pipeline:
    Gemini "events in {city}" thread  →  CSV  →  this script  →
    candidates.md + candidates.csv  →  human review  →  parser builds

Reads a CSV of search results (one row per result), aggregates by registrable
domain, filters out already-scraped + known-noise domains, fetches each
candidate's homepage to detect JSON-LD `*Event` types and event-shaped sitemap
URLs, scores, and outputs a ranked candidate list.

USAGE
    pip install tldextract
    python3 scripts/scan_aggregators.py scripts/scan_input.csv

    # outputs:
    #   scripts/candidates.md   — markdown table, sorted by score
    #   scripts/candidates.csv  — same data, for Google Sheets import

INPUT CSV FORMAT
    city,country,lang,query,rank,url,title,snippet

    Tel Aviv,Israel,he,אירועים בתל אביב,1,https://www.mevalim.co.il/...,מבלים,...
    Tel Aviv,Israel,he,אירועים בתל אביב,2,https://www.timeout.com/...,Time Out,...
    ...

GEMINI PROMPT TEMPLATE
    Use a Gemini auto-thread (or any search tool) with this prompt:

    > For each (city, country, language) pair below, run the search query and
    > return the top 10 organic Google web results. Skip sponsored/ad results,
    > image/video carousels, and news boxes. For each result output one CSV row:
    >     city,country,lang,query,rank,url,title,snippet
    >
    > Cities (use the local-language query):
    > 1.  Tel Aviv, Israel, he   — אירועים בתל אביב
    > 2.  Tel Aviv, Israel, en   — events in Tel Aviv
    > 3.  Jerusalem, Israel, he  — אירועים בירושלים
    > 4.  Jerusalem, Israel, en  — events in Jerusalem
    > 5.  Berlin, Germany, de    — Veranstaltungen in Berlin
    > 6.  Munich, Germany, de    — Veranstaltungen in München
    > 7.  Paris, France, fr      — événements à Paris
    > 8.  Madrid, Spain, es      — eventos en Madrid
    > 9.  Barcelona, Spain, es   — eventos en Barcelona
    > 10. São Paulo, Brazil, pt  — eventos em São Paulo
    > 11. Mexico City, Mexico, es — eventos en Ciudad de México
    > 12. Tokyo, Japan, ja       — 東京 イベント
    > 13. Tokyo, Japan, en       — events in Tokyo
    > 14. Mumbai, India, en      — events in Mumbai
    > 15. Istanbul, Turkey, tr   — İstanbul etkinlikleri
    > 16. Seoul, South Korea, ko — 서울 이벤트
    > 17. Warsaw, Poland, pl     — wydarzenia w Warszawie
    > 18. Stockholm, Sweden, sv  — evenemang i Stockholm
    >
    > Output a single CSV (with header row) — no commentary.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

try:
    import tldextract
except ImportError:
    sys.exit("scan_aggregators.py requires `tldextract` — run: pip install tldextract")

import httpx
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)
logger = logging.getLogger("scan_aggregators")


# ── Brand allowlist — global ticketing platforms that operate one brand
# across many ccTLDs (eventbrite.com / .de / .fr / …, ticketmaster.com / .de /
# .co.uk / .es / …). Match by `tldextract.domain` (the second-level label),
# so any TLD variant of the brand is excluded.
BRAND_ALLOWLIST: set[str] = {
    "eventbrite", "ticketmaster", "bandsintown", "songkick", "meetup",
    "stubhub", "axs", "ticketweb", "vivaticket", "ticketone",
}

# ── Allowlist (already-scraped) — exact registrable domains we have collectors
# for. Grow this as new collectors land. Use the canonical registrable domain
# (e.g. `mevalim.co.il`, not `tickets.mevalim.co.il`) — subdomains are
# normalised away during ingest.
ALLOWLIST: set[str] = {
    "setlist.fm", "lu.ma", "dice.fm", "skiddle.com",
    "allevents.in", "ra.co",                    # Resident Advisor
    # Israel
    "mevalim.co.il", "smarticket.co.il", "barby.co.il", "cameri.co.il",
    "leaan.co.il", "hatarbut.org.il", "goshow.co.il",
    # US venues / city guides covered by city_guides + venue_websites
    "choosechicago.com",
    # Tech conferences
    "techconf.directory",
    # Sports (ESPN + friends — handled by sports/* scrapers)
    "espn.com", "nba.com", "nhl.com", "mlb.com", "nfl.com",
}

# ── Blocklist — known noise. Tourism/listicle/news/social/forum sites that
# rank highly for "events in {city}" but never expose machine-readable feeds.
# Grow this aggressively — false positives here are cheap (we just lose a
# candidate that wasn't viable anyway).
BLOCKLIST: set[str] = {
    # Tourism aggregators (no machine-readable data, mostly tour sales)
    "tripadvisor.com", "viator.com", "getyourguide.com", "expedia.com",
    "booking.com", "airbnb.com", "klook.com", "civitatis.com",
    # News / mags
    "timeout.com",  # actually has events but in human-curated lists, not parseable
    "cnn.com", "bbc.com", "nytimes.com", "theguardian.com",
    # Social / forums
    "facebook.com", "instagram.com", "twitter.com", "x.com", "tiktok.com",
    "reddit.com", "quora.com", "linkedin.com", "youtube.com",
    # Blog platforms
    "medium.com", "substack.com", "wordpress.com", "blogspot.com",
    # Wikis / dictionaries
    "wikipedia.org", "wikitravel.org", "wikivoyage.org",
    # Search engines / portals
    "google.com", "bing.com", "yahoo.com", "duckduckgo.com",
    # Tourism boards (often human-curated lists, low-signal)
    "lonelyplanet.com",
}

# Fetch tuning — polite. We hit each candidate once so this is well within
# any reasonable rate limit; concurrency mostly speeds up the overall script.
CONCURRENCY = 6
TIMEOUT = 10
DELAY = 0.3
HEADERS = {
    "User-Agent": (
        "Supercaly-Scanner/1.0 "
        "(+https://event-discovery.onrender.com; aggregator discovery bot)"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

JSONLD_EVENT_TYPES = {
    "Event", "MusicEvent", "TheaterEvent", "ComedyEvent", "ChildrensEvent",
    "DanceEvent", "SocialEvent", "SportsEvent", "EducationEvent",
    "BusinessEvent", "FoodEvent", "Festival", "ScreeningEvent",
    "VisualArtsEvent", "ExhibitionEvent",
}

# Sitemap heuristics — URL substrings strongly correlated with event pages.
# Covers the languages our city sweep targets, plus a few generic English
# patterns that show up cross-locale (`/whats-on/`, `/programme/`, `/tickets/`).
# We keep this permissive: false positives here only inflate scores a bit;
# false negatives kill genuine aggregators (the previous version missed
# `eventi/`, `akce/`, `agenda/`, `tapahtumat/`, etc., which auto-skipped real
# regional aggregators in the 17-city sweep).
EVENT_URL_PATTERNS = re.compile(
    # English / generic
    r"/(events?|shows?|concerts?|gigs?|performances?|"
    r"whats[-_]?on|what'?s[-_]?on|programme|calendar|tickets?|"
    # Romance
    r"eventos?|eventi|evenement|evenements|événement|événements|"
    r"espet[áa]culos|spectacles?|conciertos|concerti|concertos|"
    # Germanic
    r"veranstaltung|veranstaltungen|programm|konzerte?|"
    r"auff[uü]hrungen?|evenementen|voorstellingen|agenda|uit|"
    # Slavic
    r"wydarzenia|koncerty|akce|"
    # Uralic / Finno-Ugric
    r"rendezv[ée]ny|rendezv[ée]nyek|programok|koncertek|tapahtumat|"
    # Turkic
    r"etkinlik|etkinlikler|"
    # Greek
    r"εκδηλώσεις|συναυλίες|"
    # Hebrew
    r"ארועים|אירועים|הופעות|"
    # Arabic
    r"فعاليات|حفلات|أحداث|"
    # Indonesian / Malay
    r"acara|konser|"
    # Thai
    r"งาน|อีเวนต์|คอนเสิร์ต|"
    # CJK
    r"イベント|アクティビティ|公演|"
    r"活動|活动|演唱會|演唱会|表演|展演|"
    r"이벤트|공연|"
    # Nordic
    r"evenemang|konserter|arrangementer|koncerter"
    r")(/|$|\?)",
    re.IGNORECASE,
)

# When walking a sitemap-of-sitemaps, prefer child sitemaps whose URL
# suggests they contain event listings — falls back to first-N if no match.
EVENT_SITEMAP_HINT = re.compile(
    r"event|show|concert|gig|veranstaltung|evento|eventi|akce|"
    r"agenda|programme|programm|tapahtum|rendezv|etkinlik|"
    r"performance|spectacle|whats[-_]?on|tickets?",
    re.IGNORECASE,
)

# Discard hallucinated Gemini placeholder URLs (e.g. `/list-1`, `/list-2`).
# These slipped through in the 17-city sweep and polluted rank metadata.
HALLUCINATED_PATH_RE = re.compile(r"/list-\d+/?$")

CACHE_PATH = Path(__file__).parent / ".aggregator_scan_cache.json"
CACHE_TTL_DAYS = 7


# ─────────────────────────────────────────────────────────────────────────────
# Data model
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Candidate:
    domain: str
    cities: set[str] = field(default_factory=set)
    queries: set[str] = field(default_factory=set)
    ranks: list[int] = field(default_factory=list)
    sample_urls: list[str] = field(default_factory=list)
    titles: list[str] = field(default_factory=list)
    # Filled in by probe phase
    has_jsonld_event: Optional[bool] = None
    sitemap_event_count: Optional[int] = None
    fetch_status: Optional[str] = None  # "ok", "404", "timeout", …
    # Computed
    score: float = 0.0
    verdict: str = ""

    @property
    def avg_rank(self) -> float:
        return sum(self.ranks) / len(self.ranks) if self.ranks else 99.0


# ─────────────────────────────────────────────────────────────────────────────
# Domain extraction
# ─────────────────────────────────────────────────────────────────────────────

_extractor = tldextract.TLDExtract(suffix_list_urls=())  # use bundled snapshot


def registrable_domain(url: str) -> str:
    ext = _extractor(url)
    if not ext.domain or not ext.suffix:
        return ""
    return f"{ext.domain}.{ext.suffix}".lower()


def brand_label(url: str) -> str:
    """Second-level domain label, e.g. 'eventbrite' for eventbrite.de."""
    return _extractor(url).domain.lower()


# ─────────────────────────────────────────────────────────────────────────────
# CSV ingest + aggregation
# ─────────────────────────────────────────────────────────────────────────────

def ingest_csv(path: Path) -> dict[str, Candidate]:
    """Read the Gemini search-results CSV and group by registrable domain."""
    candidates: dict[str, Candidate] = {}
    skipped_allow = 0
    skipped_block = 0
    skipped_invalid = 0
    skipped_hallucinated = 0

    # `utf-8-sig` strips a leading BOM if present — Gemini-exported CSVs
    # commonly carry one, which would otherwise turn the first column header
    # `city` into `\ufeffcity` and silently null out every row's city value
    # (collapsing multi-city signal).
    with open(path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = (row.get("url") or "").strip()
            domain = registrable_domain(url)
            if not domain:
                skipped_invalid += 1
                continue
            # Drop Gemini's placeholder `/list-N` URLs so they don't pollute
            # rank/title aggregates. The bare domain still gets surfaced from
            # other (real) rows for the same site if any exist.
            if HALLUCINATED_PATH_RE.search(url):
                skipped_hallucinated += 1
                continue
            if domain in ALLOWLIST or brand_label(url) in BRAND_ALLOWLIST:
                skipped_allow += 1
                continue
            if domain in BLOCKLIST:
                skipped_block += 1
                continue

            c = candidates.setdefault(domain, Candidate(domain=domain))
            c.cities.add(row.get("city", ""))
            c.queries.add(row.get("query", ""))
            try:
                c.ranks.append(int(row.get("rank") or 99))
            except ValueError:
                c.ranks.append(99)
            if len(c.sample_urls) < 3:
                c.sample_urls.append(url)
            title = (row.get("title") or "").strip()
            if title and title not in c.titles and len(c.titles) < 3:
                c.titles.append(title)

    logger.info(
        f"ingested rows → {len(candidates)} unique candidate domains  "
        f"(skipped: allowlist={skipped_allow}, blocklist={skipped_block}, "
        f"invalid_url={skipped_invalid}, hallucinated={skipped_hallucinated})"
    )
    return candidates


# ─────────────────────────────────────────────────────────────────────────────
# Probe — fetch homepage + sitemap for each candidate
# ─────────────────────────────────────────────────────────────────────────────

def _detect_jsonld_events(html: str) -> bool:
    """True if the page contains at least one JSON-LD `*Event` item."""
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        return False
    for script in soup.find_all("script", type="application/ld+json"):
        raw = script.string or ""
        if not raw.strip():
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        items = data if isinstance(data, list) else data.get("@graph", [data])
        for item in items:
            if not isinstance(item, dict):
                continue
            t = item.get("@type")
            if isinstance(t, list):
                if any(x in JSONLD_EVENT_TYPES for x in t):
                    return True
            elif isinstance(t, str) and t in JSONLD_EVENT_TYPES:
                return True
    return False


async def _fetch(client: httpx.AsyncClient, url: str) -> tuple[Optional[str], int]:
    """Return (text, status_code). Text is None on failure."""
    try:
        r = await client.get(url, timeout=TIMEOUT, follow_redirects=True)
        return (r.text, r.status_code) if r.status_code == 200 else (None, r.status_code)
    except Exception:
        return None, 0


_SITEMAP_URL_RE = re.compile(r"<loc>\s*([^<\s]+)\s*</loc>")
_ROBOTS_SITEMAP_RE = re.compile(r"^\s*sitemap:\s*(\S+)", re.IGNORECASE | re.MULTILINE)

# Hardcoded fallbacks — used only when robots.txt has no Sitemap: lines.
SITEMAP_FALLBACK_PATHS = (
    "/sitemap.xml",
    "/sitemap_index.xml",
    "/sitemap-events.xml",
    "/sitemap-event.xml",
    "/event-sitemap.xml",
    "/sitemaps.xml",
    "/wp-sitemap.xml",
)

# Hard cap on total sitemap URLs we'll scan per domain — prevents pathological
# cases (a giant retailer with 1M product URLs) from blowing up the run.
SITEMAP_URL_CAP = 50_000
SITEMAP_INDEX_CHILDREN_CAP = 8


async def _discover_sitemap_urls(client: httpx.AsyncClient, domain: str) -> list[str]:
    """Return a prioritised list of sitemap URLs to probe for `domain`.

    Order: robots.txt-declared sitemaps first (most authoritative), then the
    hardcoded fallbacks. Deduped while preserving order.
    """
    seen: set[str] = set()
    out: list[str] = []

    text, _ = await _fetch(client, f"https://{domain}/robots.txt")
    if text:
        for m in _ROBOTS_SITEMAP_RE.findall(text):
            url = m.strip()
            if url and url not in seen:
                seen.add(url)
                out.append(url)

    for path in SITEMAP_FALLBACK_PATHS:
        url = f"https://{domain}{path}"
        if url not in seen:
            seen.add(url)
            out.append(url)
    return out


async def _count_sitemap_events(client: httpx.AsyncClient, domain: str) -> int:
    """Walk sitemaps and count URLs matching the event-URL heuristic.

    Strategy:
      1. Discover sitemap URLs via robots.txt + hardcoded fallbacks.
      2. For each reachable sitemap, parse <loc> entries.
      3. If entries are themselves sitemaps (sitemap-of-sitemaps), prioritise
         children whose name suggests events; sample up to 8 children deep.
      4. Sum URLs across all parsed sitemaps that match EVENT_URL_PATTERNS.

    Returns -1 if no sitemap is reachable, otherwise the count (≥ 0).
    """
    sitemap_urls = await _discover_sitemap_urls(client, domain)
    any_reachable = False
    event_urls = 0
    total_urls = 0

    for sm_url in sitemap_urls:
        text, _ = await _fetch(client, sm_url)
        if not text:
            continue
        any_reachable = True
        urls = _SITEMAP_URL_RE.findall(text)
        if not urls:
            continue

        # Split into child-sitemaps vs regular URLs. Some sites mix both in
        # one file — handle that.
        child_sitemaps = [u for u in urls if u.endswith(".xml") or u.endswith(".xml.gz")]
        page_urls = [u for u in urls if u not in child_sitemaps]

        # Prefer event-named child sitemaps; fall back to first N otherwise.
        if child_sitemaps:
            event_named = [u for u in child_sitemaps if EVENT_SITEMAP_HINT.search(u)]
            ordered = event_named + [u for u in child_sitemaps if u not in event_named]
            for child in ordered[:SITEMAP_INDEX_CHILDREN_CAP]:
                if total_urls >= SITEMAP_URL_CAP:
                    break
                t2, _ = await _fetch(client, child)
                if not t2:
                    continue
                child_urls = _SITEMAP_URL_RE.findall(t2)
                page_urls.extend(child_urls)

        # Score this sitemap's pages and short-circuit if we've found plenty.
        for u in page_urls:
            total_urls += 1
            if EVENT_URL_PATTERNS.search(u):
                event_urls += 1
            if total_urls >= SITEMAP_URL_CAP:
                break

        # Stop walking further sitemap entrypoints once we have a clear signal.
        if event_urls >= 50 or total_urls >= SITEMAP_URL_CAP:
            break

    return event_urls if any_reachable else -1


async def probe_one(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    cand: Candidate,
    cache: dict,
) -> None:
    """Fill in has_jsonld_event / sitemap_event_count / fetch_status on `cand`,
    using `cache` to skip recently-probed domains."""
    domain = cand.domain
    cached = cache.get(domain)
    if cached and (
        datetime.fromisoformat(cached["fetched_at"]).date()
        >= datetime.utcnow().date().replace(day=max(1, datetime.utcnow().day - CACHE_TTL_DAYS))
    ):
        cand.has_jsonld_event = cached.get("has_jsonld_event")
        cand.sitemap_event_count = cached.get("sitemap_event_count", -1)
        cand.fetch_status = cached.get("fetch_status", "cached")
        return

    async with sem:
        # Homepage probe — JSON-LD detection + sitemap walk both rooted here.
        text, status = await _fetch(client, f"https://{domain}/")
        if text is None:
            cand.fetch_status = f"http_{status}"
            cand.has_jsonld_event = False
            cand.sitemap_event_count = -1
        else:
            cand.fetch_status = "ok"
            cand.has_jsonld_event = _detect_jsonld_events(text)
            cand.sitemap_event_count = await _count_sitemap_events(client, domain)

            # Many regional aggregators put JSON-LD only on listing pages, not
            # the homepage. If the homepage is dry, also probe the first
            # Gemini-surfaced URL — that's the page Google indexed for the
            # local-language event query, so it's the most likely listing.
            if not cand.has_jsonld_event and cand.sample_urls:
                listing_url = cand.sample_urls[0]
                # Only probe if it points at the same registrable domain
                # (no off-site redirects), and is non-trivial.
                if (
                    registrable_domain(listing_url) == domain
                    and listing_url.rstrip("/") != f"https://{domain}"
                ):
                    text2, _ = await _fetch(client, listing_url)
                    if text2 and _detect_jsonld_events(text2):
                        cand.has_jsonld_event = True

        await asyncio.sleep(DELAY)

    cache[domain] = {
        "fetched_at": datetime.utcnow().isoformat(),
        "has_jsonld_event": cand.has_jsonld_event,
        "sitemap_event_count": cand.sitemap_event_count,
        "fetch_status": cand.fetch_status,
    }


async def probe_all(candidates: dict[str, Candidate], use_cache: bool = True) -> None:
    cache = _load_cache() if use_cache else {}
    sem = asyncio.Semaphore(CONCURRENCY)
    async with httpx.AsyncClient(headers=HEADERS, timeout=TIMEOUT) as client:
        tasks = [probe_one(client, sem, c, cache) for c in candidates.values()]
        # Progress: log every 10 completions
        for i, t in enumerate(asyncio.as_completed(tasks), 1):
            await t
            if i % 10 == 0 or i == len(tasks):
                logger.info(f"probed {i}/{len(tasks)}")
    _save_cache(cache)


def _load_cache() -> dict:
    if CACHE_PATH.exists():
        try:
            return json.loads(CACHE_PATH.read_text())
        except Exception:
            return {}
    return {}


def _save_cache(cache: dict) -> None:
    CACHE_PATH.write_text(json.dumps(cache, indent=2, ensure_ascii=False))


# ─────────────────────────────────────────────────────────────────────────────
# Score + verdict
# ─────────────────────────────────────────────────────────────────────────────

# Sitemap-event count threshold for promoting a candidate to "review". Lowered
# from 10 → 5 after the 17-city sweep showed real regional aggregators (port.hu,
# wantedinrome.com, athinorama.gr) only surface ~5–10 event-pattern URLs in
# their top-level sitemap because most events live in deeper sub-sitemaps the
# walk doesn't always reach.
REVIEW_SITEMAP_THRESHOLD = 5
STRONG_SITEMAP_THRESHOLD = 20


def score_candidates(candidates: dict[str, Candidate]) -> None:
    """Assign `score` and `verdict` to each candidate.

    Score components (higher = more interesting):
      + 10 per city it appears in (multi-city = global aggregator)
      +  5 if has JSON-LD Event (homepage or first sample URL)
      +  3 per 10 event-URLs in sitemap (capped at 9)
      -  0.5 per rank (lower rank = better; rank 1 → -0.5, rank 10 → -5)

    Verdicts:
      "strong"     — JSON-LD Event AND sitemap_events ≥ STRONG_SITEMAP_THRESHOLD
      "review"     — JSON-LD Event OR  sitemap_events ≥ REVIEW_SITEMAP_THRESHOLD
                                    OR cities ≥ 2
                     (multi-city = aggregator, not a single-city tourism portal)
      "auto-skip"  — no signal at all
    """
    for c in candidates.values():
        s = 0.0
        s += 10 * len(c.cities)
        if c.has_jsonld_event:
            s += 5
        if c.sitemap_event_count and c.sitemap_event_count > 0:
            s += min(9, c.sitemap_event_count // 10) * 3
        s -= 0.5 * c.avg_rank
        c.score = round(s, 1)

        has_jsonld = bool(c.has_jsonld_event)
        sitemap_count = c.sitemap_event_count or 0
        if has_jsonld and sitemap_count >= STRONG_SITEMAP_THRESHOLD:
            c.verdict = "strong"
        elif (
            has_jsonld
            or sitemap_count >= REVIEW_SITEMAP_THRESHOLD
            or len(c.cities) >= 2
        ):
            c.verdict = "review"
        else:
            c.verdict = "auto-skip"


# ─────────────────────────────────────────────────────────────────────────────
# Output
# ─────────────────────────────────────────────────────────────────────────────

def write_outputs(candidates: dict[str, Candidate], out_dir: Path) -> None:
    ranked = sorted(candidates.values(), key=lambda c: -c.score)
    md_path = out_dir / "candidates.md"
    csv_path = out_dir / "candidates.csv"

    # ── Markdown ────────────────────────────────────────────────────────────
    lines = [
        "# Aggregator candidates",
        "",
        f"_Generated {datetime.utcnow().isoformat(timespec='seconds')}Z_  ",
        f"_Total candidates: {len(ranked)}  ·  "
        f"strong: {sum(1 for c in ranked if c.verdict == 'strong')}  ·  "
        f"review: {sum(1 for c in ranked if c.verdict == 'review')}  ·  "
        f"auto-skip: {sum(1 for c in ranked if c.verdict == 'auto-skip')}_",
        "",
        "| # | Domain | Verdict | Score | Cities | AvgRank | JSON-LD | SitemapEvts | Sample URL | Titles |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for i, c in enumerate(ranked, 1):
        sample = c.sample_urls[0] if c.sample_urls else ""
        titles = " · ".join(c.titles[:2]) if c.titles else ""
        lines.append(
            f"| {i} | `{c.domain}` | **{c.verdict}** | {c.score} | "
            f"{len(c.cities)} ({', '.join(sorted(c.cities)[:3])}{'…' if len(c.cities) > 3 else ''}) | "
            f"{c.avg_rank:.1f} | "
            f"{'✓' if c.has_jsonld_event else '✗' if c.has_jsonld_event is False else '?'} | "
            f"{c.sitemap_event_count if c.sitemap_event_count is not None else '?'} | "
            f"<{sample}> | {titles} |"
        )
    md_path.write_text("\n".join(lines), encoding="utf-8")
    logger.info(f"wrote {md_path}")

    # ── CSV (for Sheets import) ─────────────────────────────────────────────
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "rank", "domain", "verdict", "score",
            "city_count", "cities", "avg_rank",
            "has_jsonld_event", "sitemap_event_count", "fetch_status",
            "sample_url", "titles", "queries",
        ])
        for i, c in enumerate(ranked, 1):
            w.writerow([
                i, c.domain, c.verdict, c.score,
                len(c.cities), "; ".join(sorted(c.cities)), round(c.avg_rank, 1),
                c.has_jsonld_event, c.sitemap_event_count, c.fetch_status,
                c.sample_urls[0] if c.sample_urls else "",
                " · ".join(c.titles[:2]),
                " · ".join(sorted(c.queries)[:2]),
            ])
    logger.info(f"wrote {csv_path}")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("input_csv", help="Path to the Gemini-output CSV.")
    ap.add_argument(
        "--out-dir", default=str(Path(__file__).parent),
        help="Directory for candidates.md / candidates.csv (default: scripts/)",
    )
    ap.add_argument(
        "--no-probe", action="store_true",
        help="Skip the homepage/sitemap probe (faster, less signal).",
    )
    ap.add_argument(
        "--no-cache", action="store_true",
        help="Ignore the on-disk probe cache and refetch every domain.",
    )
    args = ap.parse_args()

    in_path = Path(args.input_csv)
    if not in_path.exists():
        sys.exit(f"input CSV not found: {in_path}")

    candidates = ingest_csv(in_path)
    if not args.no_probe and candidates:
        asyncio.run(probe_all(candidates, use_cache=not args.no_cache))
    score_candidates(candidates)
    write_outputs(candidates, Path(args.out_dir))


if __name__ == "__main__":
    main()
