"""
Extract events from any HTML via Gemini.

The (B) workflow we sketched: instead of requiring schema.org JSON-LD,
let an LLM read raw HTML and emit structured events. Unlocks the long
tail of city sources (Hebrew lifestyle sites, niche venue pages, local
papers without JSON-LD) that the existing collectors can't parse.

Usage:
    python3 scripts/extract_events_via_ai.py URL
    python3 scripts/extract_events_via_ai.py URL --max-events 30
    python3 scripts/extract_events_via_ai.py URL --raw-html      # save HTML for debug

Cost note: per-page input is the entire HTML body (we strip script/style
and trim to a sane limit). On gemini-2.5-flash that's ~$0.04 per page
input + ~$0.05 per 50-event output ≈ $0.10/page. At scale we'd batch
multiple pages per call or use prompt caching.

This is a SPIKE — not wired into the collector pipeline yet. Goal here
is to prove Gemini can reliably extract from a Hebrew/non-schema page
and emit clean structured data.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Reuse .env loader.
ENV_PATH = ROOT / ".env"
if ENV_PATH.is_file():
    for line in ENV_PATH.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

try:
    from google import genai
    from google.genai import types as gtypes
except ImportError:
    sys.exit("google-genai not installed. Run: pip3 install --user google-genai")

try:
    from bs4 import BeautifulSoup
except ImportError:
    sys.exit("beautifulsoup4 not installed. Run: pip3 install --user beautifulsoup4")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("extract_events_via_ai")


# Match the columns in app.models.event.Event so this output can flow
# into the existing collector ingestion pipeline later.
RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "events": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "artist_name": {"type": "string", "nullable": True},
                    "start_date": {"type": "string",
                                   "description": "ISO 8601 date YYYY-MM-DD. If only a year/month known, use first of month."},
                    "start_time": {"type": "string", "nullable": True,
                                   "description": "24h HH:MM, e.g. '21:00'. Null if unknown."},
                    "end_date": {"type": "string", "nullable": True},
                    "venue_name": {"type": "string", "nullable": True},
                    "purchase_link": {"type": "string", "nullable": True,
                                      "description": "Direct ticket URL if visible. Otherwise null."},
                    "price": {"type": "number", "nullable": True,
                              "description": "Lowest price as a number. Null if unknown or 'free'."},
                    "price_currency": {"type": "string", "nullable": True,
                                       "description": "ISO 4217 code, e.g. 'ILS','USD','EUR'. Null if unknown."},
                    "description": {"type": "string", "nullable": True},
                    "image_url": {"type": "string", "nullable": True},
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


def _fetch(url: str, timeout: int = 20) -> str | None:
    """Plain HTTP fetch — same UA strategy as find_city_guides.

    Percent-encode non-ASCII path chars (Hebrew/CJK URLs) so urlopen's ASCII
    serializer doesn't blow up. We only touch the path, not the host or query
    parsing, so already-encoded URLs round-trip unchanged.
    """
    from urllib.parse import quote, urlsplit, urlunsplit
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
        log.error(f"fetch failed: {e}")
        return None


def clean_html(html: str, base_url: str, max_chars: int = 200_000) -> str:
    """Strip noise and return a token-efficient HTML representation.

    We keep the structural skeleton (so Gemini can see anchors → ticket links
    and img→ poster URLs) but drop scripts, styles, and obviously irrelevant
    chrome. Truncate aggressively if still too big — a typical events listing
    page has its content in the first 200 KB after stripping.
    """
    soup = BeautifulSoup(html, "lxml")

    # Drop noise elements outright. Keep <noscript> in case it has a fallback
    # event listing — some sites do that.
    for tag in soup(["script", "style", "svg", "iframe", "form", "input",
                     "button", "header", "footer", "nav", "aside"]):
        tag.decompose()

    # Resolve relative URLs in <a href> and <img src> so Gemini emits absolute
    # purchase_link / image_url — saves us a post-processing step.
    from urllib.parse import urljoin
    for a in soup.find_all("a", href=True):
        a["href"] = urljoin(base_url, a["href"])
    for img in soup.find_all("img", src=True):
        img["src"] = urljoin(base_url, img["src"])

    body = soup.body or soup
    text = str(body)
    if len(text) > max_chars:
        text = text[:max_chars] + "\n<!-- TRUNCATED -->"
    return text


PROMPT_TEMPLATE = """\
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


def extract_events(url: str, model: str, max_events: int) -> dict:
    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        sys.exit("GEMINI_API_KEY not set")

    log.info(f"fetching {url}…")
    html = _fetch(url)
    if not html:
        sys.exit("fetch failed")
    log.info(f"  raw HTML: {len(html):,} bytes")

    cleaned = clean_html(html, base_url=url)
    log.info(f"  cleaned:  {len(cleaned):,} bytes (≈{len(cleaned)//4:,} tokens)")

    from datetime import date
    prompt = PROMPT_TEMPLATE.format(
        url=url,
        today=date.today().isoformat(),
        max_events=max_events,
        html=cleaned,
    )

    log.info(f"calling Gemini ({model})…")
    client = genai.Client(api_key=api_key)
    resp = client.models.generate_content(
        model=model,
        contents=prompt,
        config=gtypes.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=RESPONSE_SCHEMA,
            temperature=0,
            max_output_tokens=20000,
        ),
    )

    try:
        return json.loads(resp.text or "")
    except json.JSONDecodeError as e:
        debug = Path("/tmp/gemini_extract_raw.txt")
        debug.write_text(resp.text or "", encoding="utf-8")
        sys.exit(f"JSON parse failed: {e}\nraw → {debug}")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("url", help="URL of an events listing page")
    ap.add_argument("--model", default="gemini-2.5-flash")
    ap.add_argument("--max-events", type=int, default=50,
                    help="Tell Gemini to return up to N events (default 50)")
    ap.add_argument("--raw-html", action="store_true",
                    help="Also save raw + cleaned HTML to /tmp for debugging")
    args = ap.parse_args()

    if args.raw_html:
        # Re-fetch for debug copies; cheap.
        html = _fetch(args.url) or ""
        Path("/tmp/extract_raw.html").write_text(html, encoding="utf-8")
        Path("/tmp/extract_clean.html").write_text(
            clean_html(html, args.url), encoding="utf-8")
        log.info("debug → /tmp/extract_raw.html, /tmp/extract_clean.html")

    result = extract_events(args.url, args.model, args.max_events)
    events = result.get("events", [])

    print(f"\n📋  Extracted {len(events)} events from {args.url}\n")
    for i, ev in enumerate(events, 1):
        date_part = ev.get("start_date", "?")
        if ev.get("start_time"):
            date_part += f" {ev['start_time']}"
        if ev.get("end_date") and ev["end_date"] != ev.get("start_date"):
            date_part += f" → {ev['end_date']}"
        venue = ev.get("venue_name") or ""
        artist = ev.get("artist_name") or ""
        price = ""
        if ev.get("price") is not None:
            cur = ev.get("price_currency") or ""
            price = f" · {ev['price']}{cur}"
        print(f"  {i:2}. [{date_part}] {ev['name']}")
        if artist:
            print(f"      artist: {artist}")
        if venue:
            print(f"      venue:  {venue}")
        if price.strip():
            print(f"     {price}")
        if ev.get("purchase_link"):
            print(f"      ticket: {ev['purchase_link']}")

    # Dump full JSON for downstream inspection
    out_path = Path("/tmp/extracted_events.json")
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n→ full JSON: {out_path}")


if __name__ == "__main__":
    main()
