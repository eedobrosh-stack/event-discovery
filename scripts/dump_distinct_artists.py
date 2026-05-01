"""Paginate through /api/events on prod and dump distinct artist_name values.

Output: scripts/distinct_artists.txt — one artist per line, sorted alphabetically.

Why this script: we want to feed the artist list into a Gemini classification
prompt and the local dev DB may be stale. Hitting prod via the public API is
the simplest way to get a current, accurate dump without DB access.
"""
import sys
import time
import urllib.request
import json

BASE = "https://superca.ly"
LIMIT = 500
OUT_PATH = "scripts/distinct_artists.txt"

def fetch(offset: int) -> list:
    url = f"{BASE}/api/events?limit={LIMIT}&offset={offset}"
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.load(resp)

def total_count() -> int:
    with urllib.request.urlopen(f"{BASE}/api/events/count", timeout=30) as resp:
        return json.load(resp).get("total", 0)

def main():
    total = total_count()
    print(f"Total events: {total:,}")
    artists: dict[str, str] = {}  # lower → canonical, dedupe case-insensitive
    offset = 0
    pages = 0
    last_log = time.time()
    while offset < total:
        try:
            page = fetch(offset)
        except Exception as exc:
            print(f"  retry @{offset}: {exc}", file=sys.stderr)
            time.sleep(2)
            continue
        if not page:
            break
        for ev in page:
            name = (ev.get("artist_name") or "").strip()
            if name:
                artists.setdefault(name.lower(), name)
        offset += LIMIT
        pages += 1
        # Progress every ~5s
        if time.time() - last_log > 5:
            print(f"  page {pages}, offset {offset:,} / {total:,} — distinct artists so far: {len(artists):,}")
            last_log = time.time()
    sorted_names = sorted(artists.values(), key=lambda s: s.lower())
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        for n in sorted_names:
            f.write(n + "\n")
    print(f"Done. {len(sorted_names):,} distinct artists → {OUT_PATH}")

if __name__ == "__main__":
    main()
