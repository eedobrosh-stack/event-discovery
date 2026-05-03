"""
Spike: does Gemini's url_context tool render JS for SPA sites?

If yes, we get SPA support "for free" via the API and skip Playwright.
If no, we know we need a headless browser for venue-site coverage.

Test: ask Gemini to read cameri.co.il (a known SPA) and report what
events it can see. Compare against:
  - HTTP-only fetch:    static HTML has no event content (we proved this)
  - Browser at runtime: the page DOES show events to a human

If url_context surfaces real event names → it's executing JS server-side.
If it returns "no events visible" → it's only fetching static HTML and
we need Playwright.
"""
from __future__ import annotations
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = ROOT / ".env"
if ENV_PATH.is_file():
    for line in ENV_PATH.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

from google import genai
from google.genai import types as gtypes

api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
if not api_key:
    sys.exit("GEMINI_API_KEY not set")

client = genai.Client(api_key=api_key)

# Test: SPA-or-not detection.
# Cameri URL is pre-encoded — earlier the tool rejected the literal Hebrew form.
# We use ALSO a second SPA candidate (a major US theatre) where the SPA-ness is
# unambiguous and the URL is plain ASCII — eliminates the URL-encoding variable.
TARGETS = [
    ("Cameri SPA (pre-encoded)",
     "https://www.cameri.co.il/%D7%9C%D7%95%D7%97-%D7%94%D7%95%D7%A4%D7%A2%D7%95%D7%AA/"),
    ("Habima (Hebrew theatre, ASCII URL)",
     "https://www.habima.co.il/eng/calendar"),
    ("control: server-rendered aggregator",
     "https://allevents.in/tel%20aviv"),
]

PROMPT = """\
Fetch the URL provided and tell me what UPCOMING EVENTS you can see on the
page. Be specific: list the first 5 event names you find with their dates
if visible. If the page appears empty or you only see navigation chrome,
say so explicitly — I am testing whether your fetcher can render JavaScript.

URL: {url}
"""

for label, url in TARGETS:
    print(f"\n{'='*70}\n{label}: {url}\n{'='*70}")
    try:
        resp = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=PROMPT.format(url=url),
            config=gtypes.GenerateContentConfig(
                tools=[gtypes.Tool(url_context=gtypes.UrlContext())],
                temperature=0,
            ),
        )
    except Exception as e:
        print(f"ERROR: {e}")
        continue

    print(resp.text or "(empty)")
    # Also surface what URLs the tool actually fetched, if metadata exposes it.
    try:
        cand = resp.candidates[0]
        meta = getattr(cand, "url_context_metadata", None)
        if meta:
            print(f"\n[url_context metadata] {meta}")
    except Exception:
        pass
