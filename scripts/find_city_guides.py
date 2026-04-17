#!/usr/bin/env python3
"""
City event-guide scanner.

Finds local websites that publish schema.org JSON-LD Event data for a given
city.  Combines two discovery strategies:
  1. Pattern library — common URL templates for CVBs, local papers, tourism
     boards (e.g. visit{city}.com/events/, timeout.com/{city}/events).
  2. DuckDuckGo HTML search — top organic results for "{city} events calendar"
     surface real sites we don't know about yet.

Usage:
    python scripts/find_city_guides.py "Chicago"
    python scripts/find_city_guides.py "Seattle" --pages 3 --top 20

Outputs a ranked table of working sources with event counts.  Paste any
winner into city_guides.py CITY_GUIDES to start collecting.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date
from typing import Optional


# ── HTTP ─────────────────────────────────────────────────────────────────────

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
_HEADERS = {
    "User-Agent": _UA,
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
_TIMEOUT = 12


def _fetch(url: str) -> Optional[str]:
    try:
        req = urllib.request.Request(url, headers=_HEADERS)
        resp = urllib.request.urlopen(req, timeout=_TIMEOUT)
        return resp.read().decode("utf-8", errors="ignore")
    except Exception:
        return None


# ── JSON-LD event counting ────────────────────────────────────────────────────

def _count_events(html: str, future_only: bool = True) -> tuple[int, list[str]]:
    """Return (event_count, [sample_names]) from a page's JSON-LD blocks."""
    today = str(date.today())
    ld_blocks = re.findall(
        r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
        html, re.DOTALL,
    )
    events = []
    for block in ld_blocks:
        try:
            data = json.loads(block)
            items = data if isinstance(data, list) else [data]
            for item in items:
                if not isinstance(item, dict):
                    continue
                if item.get("@type") not in ("Event", "MusicEvent", "EventSeries"):
                    continue
                if future_only:
                    sd = (item.get("startDate") or "")[:10]
                    if sd and sd < today:
                        continue
                events.append(item.get("name", "Untitled"))
        except (json.JSONDecodeError, TypeError):
            continue
    return len(events), events[:3]


# ── URL candidate generation ─────────────────────────────────────────────────

def _slug(city: str) -> str:
    return city.lower().replace(" ", "").replace("-", "")


def _hyphen(city: str) -> str:
    return city.lower().replace(" ", "-")


def _plus(city: str) -> str:
    return city.lower().replace(" ", "+")


def _generate_candidates(city: str) -> list[str]:
    sl = _slug(city)
    hy = _hyphen(city)
    candidates = [
        # Official tourism / CVB boards
        f"https://visit{sl}.com/events/",
        f"https://www.visit{sl}.com/events/",
        f"https://www.visit{sl}.org/events/",
        f"https://discover{sl}.com/events/",
        f"https://www.discover{sl}.com/events/",
        f"https://www.{sl}.com/events/",
        f"https://www.{hy}.com/events/",
        # Tourism board variations
        f"https://www.visit{sl}.com/things-to-do/events/",
        f"https://www.visit{sl}.org/things-to-do/events/",
        f"https://visit{sl}.org/events/",
        f"https://www.{sl}tourism.com/events/",
        f"https://www.tourism{sl}.com/events/",
        # Local lifestyle / alternative press
        f"https://www.timeout.com/{hy}/events",
        f"https://now{sl}.com/events/",
        f"https://www.{sl}mag.com/events/",
        f"https://www.{sl}magazine.com/events/",
        f"https://www.{sl}observer.com/events/",
        f"https://www.{sl}weekly.com/events/",
        f"https://www.{sl}scene.com/events/",
        f"https://www.{sl}lifestyles.com/events/",
        f"https://www.{sl}chronicle.com/events/",
        f"https://www.{sl}reader.com/events/",
        # City-specific patterns we've found to work
        f"https://www.choose{sl}.com/events/",
        f"https://www.go{sl}.com/events/",
        f"https://www.hello{sl}.com/events/",
        f"https://www.{sl}.gov/events/",
        f"https://events.{sl}.gov/",
        f"https://www.{sl}arts.com/events/",
        # Regional / neighbourhood guides
        f"https://www.{sl}agenda.com/events/",
        f"https://www.{sl}calendar.com/events/",
        f"https://www.{sl}guide.com/events/",
        f"https://www.the{sl}guide.com/events/",
        # Common lifestyle brands
        f"https://www.blogto.com/events/",           # Toronto
        f"https://www.{sl}.curbed.com/events/",
        f"https://sfstation.com/events/",             # SF
        f"https://concreteplayground.com/{hy}/",      # AU cities
        f"https://www.broadsheet.com.au/{hy}/",       # AU cities
    ]

    # For every base domain already in the list, also try alternate paths
    # (some tourism boards use /calendar/ or /whats-on/ instead of /events/)
    extra: list[str] = []
    bases_seen: set[str] = set()
    for c in candidates:
        m = re.match(r'(https?://[^/]+)', c)
        if m:
            b = m.group(1)
            if b not in bases_seen:
                bases_seen.add(b)
                for alt in ("/calendar/", "/whats-on/", "/things-to-do/",
                            "/agenda/", "/what-to-do/", "/see-and-do/events/",
                            "/events-calendar/"):
                    extra.append(b + alt)
    candidates = candidates + extra

    # Deduplicate preserving order
    seen: set[str] = set()
    out: list[str] = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


# ── DuckDuckGo search ─────────────────────────────────────────────────────────

def _ddg_search(city: str, query_suffix: str = "events calendar things to do") -> list[str]:
    """Return up to 30 URLs from multiple DuckDuckGo HTML searches."""
    queries = [
        f"{city} events calendar",
        f"{city} things to do events",
        f"visit {city} events",
        f"{city} local events guide",
    ]
    seen: set[str] = set()
    out: list[str] = []

    for q_text in queries:
        q = urllib.parse.quote_plus(q_text)
        html = _fetch(f"https://html.duckduckgo.com/html/?q={q}")
        if not html:
            continue

        # DDG wraps result URLs in redirect links; extract via uddg param
        urls = re.findall(r'uddg=([^&"]+)', html)
        decoded = []
        for u in urls:
            try:
                decoded.append(urllib.parse.unquote(u))
            except Exception:
                pass
        # Also grab plain hrefs pointing to external sites
        hrefs = re.findall(r'href="(https?://(?!duckduckgo)[^"]+)"', html)
        combined = decoded + hrefs

        for u in combined:
            # Accept any domain-root or event/calendar/guide path
            u_lower = u.lower()
            if any(kw in u_lower for kw in (
                "/event", "/calendar", "/whats-on", "/things-to-do",
                "/agenda", "/what-to-do", "/see-do", "/guide",
            )) or re.search(r'https?://(?:visit|discover|choose|go|hello|now)[a-z]+\.(?:com|org)/', u_lower):
                base = u.split("?")[0].rstrip("/") + "/"
                if base not in seen and "duckduckgo" not in base:
                    seen.add(base)
                    out.append(base)

        if len(out) >= 30:
            break

    return out[:30]


# ── Probe a single URL ────────────────────────────────────────────────────────

@dataclass
class ProbeResult:
    url: str
    status: str = "error"   # "ok" | "no_events" | "error"
    event_count: int = 0
    samples: list[str] = field(default_factory=list)
    size_kb: int = 0
    has_pagination: bool = False


def _probe(url: str) -> ProbeResult:
    html = _fetch(url)
    if html is None:
        return ProbeResult(url=url, status="error")

    size_kb = len(html) // 1024
    count, samples = _count_events(html)

    has_pagination = bool(
        re.search(r'href="[^"]*(?:page/\d+|[?&]page=\d+)[^"]*"', html)
        or re.search(r'rel=["\']next["\']', html)
    )

    if count > 0:
        return ProbeResult(url=url, status="ok", event_count=count,
                           samples=samples, size_kb=size_kb,
                           has_pagination=has_pagination)
    return ProbeResult(url=url, status="no_events", size_kb=size_kb)


# ── Main ──────────────────────────────────────────────────────────────────────

def scan(city: str, max_pages_check: int = 2, top_n: int = 10,
         workers: int = 12) -> list[ProbeResult]:
    print(f"\n🔍  Scanning for event guides: {city!r}")

    # Collect candidates
    pattern_candidates = _generate_candidates(city)
    print(f"   Pattern library: {len(pattern_candidates)} candidates")

    ddg_candidates = _ddg_search(city)
    print(f"   DuckDuckGo search: {len(ddg_candidates)} additional URLs")

    all_candidates = list(dict.fromkeys(pattern_candidates + ddg_candidates))
    print(f"   Total to probe: {len(all_candidates)} unique URLs\n")

    # Probe in parallel
    results: list[ProbeResult] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_probe, url): url for url in all_candidates}
        done = 0
        for future in as_completed(futures):
            done += 1
            result = future.result()
            results.append(result)
            if result.status == "ok":
                print(f"   ✅  [{done}/{len(all_candidates)}] {result.url}")
                print(f"       {result.event_count} future events | paginated: {result.has_pagination}")
                for s in result.samples:
                    print(f"       · {s[:70]}")
            # else: silent

    # Sort: working first, then by event count desc
    winners = [r for r in results if r.status == "ok"]
    winners.sort(key=lambda r: r.event_count, reverse=True)

    print(f"\n{'─'*70}")
    print(f"  RESULTS for {city!r}  ({len(winners)} working sources found)")
    print(f"{'─'*70}")
    if not winners:
        print("  No JSON-LD event sources found.")
    else:
        for i, r in enumerate(winners[:top_n], 1):
            pag = "paginated" if r.has_pagination else "single page"
            print(f"  {i:2}. {r.url}")
            print(f"      {r.event_count} events · {pag} · {r.size_kb}KB")
            for s in r.samples:
                print(f"      · {s[:65]}")
            print()

    print("  To add a winner to city_guides.py:")
    print('  CITY_GUIDES["CITY"] = CityGuideConfig(')
    print('      base_url="URL_HERE",')
    print('      max_pages=5,')
    print('      source_tag="SITE_TAG",')
    print('  )')
    print()

    return winners


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Find JSON-LD event sources for a city")
    parser.add_argument("city", help='City name e.g. "Seattle"')
    parser.add_argument("--pages", type=int, default=2,
                        help="Pages to check per site (default 2)")
    parser.add_argument("--top", type=int, default=10,
                        help="Show top N results (default 10)")
    parser.add_argument("--workers", type=int, default=12,
                        help="Parallel workers (default 12)")
    args = parser.parse_args()

    winners = scan(args.city, max_pages_check=args.pages,
                   top_n=args.top, workers=args.workers)
    sys.exit(0 if winners else 1)
