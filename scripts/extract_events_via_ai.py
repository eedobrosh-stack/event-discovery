"""Extract events from any URL via Gemini — CLI front-end.

Thin wrapper around app.extractors.llm_extractor.extract(). All the
fetch / clean / Gemini / hallucination-guard logic lives in the module
so collectors and other tooling can import the same code path.

Usage:
    python3 scripts/extract_events_via_ai.py URL
    python3 scripts/extract_events_via_ai.py URL --max-events 30
    python3 scripts/extract_events_via_ai.py URL --raw-html       # save HTML for debug

Cost note: per-page input is the entire <body> after stripping (capped at
200 KB). On gemini-2.5-flash that's ~$0.04 per page input + ~$0.05 per
50-event output ≈ $0.10/page. At scale we'd batch multiple pages per
call or use prompt caching.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Reuse .env loader pattern used elsewhere — no python-dotenv dependency.
ENV_PATH = ROOT / ".env"
if ENV_PATH.is_file():
    for line in ENV_PATH.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

from app.extractors.llm_extractor import extract, ExtractorUnconfigured  # noqa: E402
from app.extractors.llm_extractor import _fetch_html, _clean_html        # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("extract_events_via_ai")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("url", help="URL of an events listing page")
    ap.add_argument("--model", default="gemini-2.5-flash")
    ap.add_argument("--max-events", type=int, default=50,
                    help="Tell Gemini to return up to N events (default 50)")
    ap.add_argument("--raw-html", action="store_true",
                    help="Also save raw + cleaned HTML to /tmp for debugging")
    ap.add_argument("--source-name", default="llm_extractor",
                    help="Source label written into RawEvent.source")
    args = ap.parse_args()

    if args.raw_html:
        html = _fetch_html(args.url) or ""
        Path("/tmp/extract_raw.html").write_text(html, encoding="utf-8")
        Path("/tmp/extract_clean.html").write_text(
            _clean_html(html, args.url), encoding="utf-8")
        log.info("debug → /tmp/extract_raw.html, /tmp/extract_clean.html")

    log.info(f"extracting from {args.url}…")
    try:
        result = extract(
            args.url,
            source_name=args.source_name,
            model=args.model,
            max_events=args.max_events,
        )
    except ExtractorUnconfigured as e:
        sys.exit(f"extractor unconfigured: {e}")

    log.info(
        f"  method={result.method}  raw={result.raw_html_bytes:,}B  "
        f"cleaned={result.cleaned_html_bytes:,}B  "
        f"events={len(result.events)}  "
        f"hallucination_dropped={result.dropped_for_hallucination}"
    )
    if result.error:
        log.warning(f"  error: {result.error}")

    print(f"\n📋  Extracted {len(result.events)} events from {args.url} "
          f"(via {result.method})\n")
    for i, ev in enumerate(result.events, 1):
        date_part = str(ev.start_date)
        if ev.start_time:
            date_part += f" {ev.start_time}"
        if ev.end_date and ev.end_date != ev.start_date:
            date_part += f" → {ev.end_date}"
        print(f"  {i:2}. [{date_part}] {ev.name}")
        if ev.artist_name:
            print(f"      artist: {ev.artist_name}")
        if ev.venue_name:
            print(f"      venue:  {ev.venue_name}")
        if ev.price is not None:
            print(f"      price:  {ev.price} {ev.price_currency or ''}")
        if ev.purchase_link:
            print(f"      ticket: {ev.purchase_link}")

    # Dump full JSON for downstream inspection (lossy: dataclass → dict).
    out_path = Path("/tmp/extracted_events.json")
    payload = {
        "method": result.method,
        "raw_html_bytes": result.raw_html_bytes,
        "cleaned_html_bytes": result.cleaned_html_bytes,
        "api_call_ok": result.api_call_ok,
        "dropped_for_hallucination": result.dropped_for_hallucination,
        "error": result.error,
        "events": [
            {
                "name": ev.name,
                "start_date": str(ev.start_date),
                "start_time": ev.start_time,
                "end_date": str(ev.end_date) if ev.end_date else None,
                "artist_name": ev.artist_name,
                "venue_name": ev.venue_name,
                "purchase_link": ev.purchase_link,
                "price": ev.price,
                "price_currency": ev.price_currency,
                "image_url": ev.image_url,
                "source": ev.source,
                "source_id": ev.source_id,
            }
            for ev in result.events
        ],
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n→ full JSON: {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
