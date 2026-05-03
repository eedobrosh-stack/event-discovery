"""
City event-guide scanner — Gemini-grounded variant.

Same goal as find_city_guides.py: find websites that publish schema.org
JSON-LD Event data for a given city. Different discovery mechanism:

    find_city_guides.py:
        Pattern library (≈100 URL templates)  +  DuckDuckGo HTML search
        ↓
        _probe() each URL  →  count <script type="application/ld+json"> Events

    find_city_guides_ai.py (this file):
        ONE Gemini API call with google_search tool grounding
        ↓
        _probe() each URL  →  count Events  (same downstream logic)

Why bother: pattern matching is brittle for non-English / non-US cities.
Asking a model with live search "what are the top 20 event-listing sites
for São Paulo?" surfaces local-language sources (Sympla, Eventim BR,
hyper-local lifestyle sites) that no pattern library will ever guess.

Cost: ~3-5k input + ~2k output tokens per city ≈ $0.005/city on flash.

Requires GEMINI_API_KEY in /Users/eedo.b/supercaly/.env (same key the
classifier uses).

Usage:
    python3 scripts/find_city_guides_ai.py "São Paulo"
    python3 scripts/find_city_guides_ai.py "Tel Aviv" --candidates 30
    python3 scripts/find_city_guides_ai.py "Berlin" --top 15

The probe stage reuses find_city_guides._probe / _count_events so we
get identical scoring / sample output. Pass --no-probe to skip probing
and just print Gemini's raw candidate list (useful for debugging
discovery quality).
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Reuse .env loader pattern from classify_via_api.py — same approach.
ENV_PATH = ROOT / ".env"
if ENV_PATH.is_file():
    for line in ENV_PATH.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

# Reuse the existing probe machinery — single source of truth for "is this
# URL actually a schema.org event source?"
sys.path.insert(0, str(ROOT / "scripts"))
from find_city_guides import _probe, ProbeResult  # noqa: E402

try:
    from google import genai
    from google.genai import types as gtypes
except ImportError:
    sys.exit("google-genai not installed. Run: pip3 install --user google-genai")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("find_city_guides_ai")


# Note on output format: when a Gemini call uses tools (google_search), the
# response_schema constraint isn't supported — schema-mode and tool-mode are
# mutually exclusive in the API. So we ask for JSON via prose instructions
# and parse defensively. The model is reliable enough at JSON when the
# instructions are precise; we strip markdown fences before parsing.
PROMPT_TEMPLATE = """\
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

Example output:
[
  {{"url": "https://www.timeout.com/london/events", "source_type": "city_magazine",
    "language": "en", "why_relevant": "Time Out London publishes daily-curated event listings."}},
  ...
]
"""


def discover_via_gemini(city: str, n: int, model: str) -> list[dict]:
    """One grounded Gemini call → list of candidate dicts."""
    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        sys.exit("GEMINI_API_KEY not set in env or /Users/eedo.b/supercaly/.env")

    client = genai.Client(api_key=api_key)
    prompt = PROMPT_TEMPLATE.format(city=city, n=n)

    log.info(f"asking Gemini ({model}) for {n} candidates for {city!r}…")
    resp = client.models.generate_content(
        model=model,
        contents=prompt,
        config=gtypes.GenerateContentConfig(
            tools=[gtypes.Tool(google_search=gtypes.GoogleSearch())],
            temperature=0.2,  # low but not zero — let it explore search results
        ),
    )

    raw = (resp.text or "").strip()
    # Strip ```json … ``` fence if Gemini decided to add one anyway.
    if raw.startswith("```"):
        # Remove first fence line and trailing fence
        lines = raw.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        raw = "\n".join(lines)

    # Some grounded responses prefix prose like "Here are…" then the array.
    # Find the first '[' and the matching last ']' as a fallback.
    if not raw.startswith("["):
        i = raw.find("[")
        j = raw.rfind("]")
        if i != -1 and j != -1 and j > i:
            raw = raw[i:j + 1]

    try:
        candidates = json.loads(raw)
    except json.JSONDecodeError as e:
        debug_path = Path("/tmp/gemini_discover_raw.txt")
        debug_path.write_text(resp.text or "", encoding="utf-8")
        sys.exit(f"Gemini output was not valid JSON: {e}\nraw → {debug_path}")

    if not isinstance(candidates, list):
        sys.exit(f"Expected JSON array, got {type(candidates).__name__}")

    # Surface any grounding metadata so we can audit what Gemini searched.
    try:
        gm = getattr(resp.candidates[0], "grounding_metadata", None)
        if gm and getattr(gm, "web_search_queries", None):
            log.info(f"  search queries used: {list(gm.web_search_queries)}")
    except Exception:
        pass

    return candidates


def probe_candidates(
    candidates: list[dict], workers: int = 12,
) -> list[tuple[dict, ProbeResult]]:
    """Reuse find_city_guides._probe for each candidate URL."""
    out: list[tuple[dict, ProbeResult]] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_probe, c["url"]): c for c in candidates if c.get("url")}
        done = 0
        for fut in as_completed(futures):
            cand = futures[fut]
            res = fut.result()
            done += 1
            tag = "✅" if res.status == "ok" else ("⚪" if res.status == "no_events" else "❌")
            print(f"   {tag}  [{done}/{len(futures)}] {cand['url']}")
            if res.status == "ok":
                print(f"       {res.event_count} events · paginated={res.has_pagination}")
                for s in res.samples:
                    print(f"       · {s[:70]}")
            out.append((cand, res))
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("city", help='City name e.g. "São Paulo"')
    ap.add_argument("--candidates", type=int, default=20,
                    help="How many candidates to ask Gemini for (default 20)")
    ap.add_argument("--top", type=int, default=10,
                    help="Show top N working winners after probing (default 10)")
    ap.add_argument("--model", default="gemini-2.5-flash",
                    help="Gemini model id (default: gemini-2.5-flash)")
    ap.add_argument("--workers", type=int, default=12,
                    help="Parallel probe workers (default 12)")
    ap.add_argument("--no-probe", action="store_true",
                    help="Skip probe stage; print Gemini's candidate list and exit")
    args = ap.parse_args()

    print(f"\n🤖  Gemini-grounded scan for: {args.city!r}\n")

    candidates = discover_via_gemini(args.city, args.candidates, args.model)
    print(f"   Gemini returned {len(candidates)} candidates:\n")
    for i, c in enumerate(candidates, 1):
        url = c.get("url", "?")
        st = c.get("source_type", "?")
        lang = c.get("language", "?")
        why = c.get("why_relevant", "")
        print(f"   {i:2}. [{st}/{lang}] {url}")
        if why:
            print(f"       — {why}")
    print()

    if args.no_probe:
        return 0

    print(f"   Probing {len(candidates)} URLs for JSON-LD events…\n")
    results = probe_candidates(candidates, workers=args.workers)

    winners = [(c, r) for c, r in results if r.status == "ok"]
    winners.sort(key=lambda pair: pair[1].event_count, reverse=True)

    print(f"\n{'─'*70}")
    print(f"  RESULTS for {args.city!r}  ({len(winners)} JSON-LD sources found "
          f"out of {len(results)} probed)")
    print(f"{'─'*70}")
    if not winners:
        print("  No JSON-LD event sources found.")
        print("  (Gemini's candidates may still be useful — many sites publish")
        print("   events as plain HTML. Consider workflow B: extract via LLM.)")
        return 1

    for i, (cand, res) in enumerate(winners[:args.top], 1):
        pag = "paginated" if res.has_pagination else "single page"
        st = cand.get("source_type", "?")
        lang = cand.get("language", "?")
        print(f"  {i:2}. [{st}/{lang}] {res.url}")
        print(f"      {res.event_count} events · {pag} · {res.size_kb}KB")
        for s in res.samples:
            print(f"      · {s[:65]}")
        print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
