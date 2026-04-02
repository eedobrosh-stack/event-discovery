"""
Two-phase genre enrichment:
  Phase 1 – Re-map performers that already have genres but are stuck on "Concert"
             (fixes the substring-matching bug).
  Phase 2 – Look up Wikipedia for performers with no genre data at all.

Usage:
    python3 scripts/enrich_genres_wikipedia.py            # both phases
    python3 scripts/enrich_genres_wikipedia.py --phase 1  # re-map only
    python3 scripts/enrich_genres_wikipedia.py --phase 2  # wikipedia only
"""
import argparse
import asyncio
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx
from sqlalchemy import text
from app.database import engine
from app.services.performer_lookup import tags_to_type, TYPE_TO_CATEGORY, normalize

WIKI_API = "https://en.wikipedia.org/w/api.php"
WIKI_HEADERS = {"User-Agent": "Supercaly/1.0 (event-discovery; contact@supercaly.app)"}
CONCURRENCY = 3
DELAY = 0.5


# ── Wikipedia helpers ────────────────────────────────────────────────────────

def extract_genres_from_wikitext(text_content: str) -> list[str]:
    """Extract genre values from Wikipedia infobox wikitext."""
    genres = []
    # Match | genre = ... or | genres = ...
    m = re.search(r'\|\s*genres?\s*=\s*(.*?)(?=\n\s*\||\n\s*\}\})', text_content, re.IGNORECASE | re.DOTALL)
    if not m:
        return genres
    raw = m.group(1)
    # Strip wikilinks [[...]] → inner text
    raw = re.sub(r'\[\[([^\|\]]+\|)?([^\]]+)\]\]', r'\2', raw)
    # Strip templates {{...}}
    raw = re.sub(r'\{\{[^}]+\}\}', '', raw)
    # Strip HTML tags
    raw = re.sub(r'<[^>]+>', '', raw)
    # Split on commas, line breaks, bullets
    for part in re.split(r'[,\n*•·]', raw):
        g = part.strip().lower()
        if g and len(g) > 2 and len(g) < 60:
            genres.append(g)
    return genres


async def fetch_wikipedia_genres(client: httpx.AsyncClient, sem: asyncio.Semaphore, name: str) -> list[str]:
    async with sem:
        try:
            # Search for the artist page
            search_resp = await client.get(
                WIKI_API,
                params={"action": "query", "list": "search", "srsearch": name,
                        "srnamespace": 0, "srlimit": 3, "format": "json"},
                headers=WIKI_HEADERS, timeout=8,
            )
            await asyncio.sleep(DELAY)
            if search_resp.status_code != 200:
                return []

            results = search_resp.json().get("query", {}).get("search", [])
            if not results:
                return []

            # Pick the best title match
            title = None
            name_lower = name.lower()
            for r in results:
                if name_lower in r["title"].lower():
                    title = r["title"]
                    break
            if not title:
                title = results[0]["title"]

            # Fetch the wikitext of the page
            wt_resp = await client.get(
                WIKI_API,
                params={"action": "query", "titles": title, "prop": "revisions",
                        "rvprop": "content", "rvslots": "main", "format": "json",
                        "rvsection": 0},
                headers=WIKI_HEADERS, timeout=8,
            )
            await asyncio.sleep(DELAY)
            if wt_resp.status_code != 200:
                return []

            pages = wt_resp.json().get("query", {}).get("pages", {})
            for page in pages.values():
                wikitext = (page.get("revisions") or [{}])[0].get("slots", {}).get("main", {}).get("*", "")
                if wikitext:
                    return extract_genres_from_wikitext(wikitext)
        except Exception:
            pass
        return []


# ── Phase 1: re-map performers with existing genres ──────────────────────────

def phase1_remap():
    print("=== Phase 1: Re-mapping performers with existing genres ===")
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, name, genres FROM performers
            WHERE event_type_name = 'Concert'
            AND genres IS NOT NULL AND genres != '[]' AND genres != ''
        """)).fetchall()
        print(f"  {len(rows)} performers to re-evaluate...")

        updated = 0
        for pid, name, genres_json in rows:
            try:
                genres = json.loads(genres_json) if genres_json else []
            except Exception:
                continue
            if not genres:
                continue
            event_type = tags_to_type(genres)
            if event_type and event_type != "Concert":
                category = TYPE_TO_CATEGORY.get(event_type, "Music")
                conn.execute(text("""
                    UPDATE performers SET event_type_name=:et, category=:cat WHERE id=:id
                """), {"et": event_type, "cat": category, "id": pid})
                updated += 1

        conn.commit()
        print(f"  ✓ Re-mapped {updated} performers\n")
    return updated


# ── Phase 2: Wikipedia lookup for empty genres ────────────────────────────────

async def phase2_wikipedia():
    print("=== Phase 2: Wikipedia lookup for performers with no genres ===")
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, name FROM performers
            WHERE (genres IS NULL OR genres = '[]' OR genres = '')
            ORDER BY id
        """)).fetchall()
        print(f"  {len(rows)} performers to look up on Wikipedia...")

        sem = asyncio.Semaphore(CONCURRENCY)
        updated = 0
        found_genre = 0
        save_every = 50

        async with httpx.AsyncClient() as client:
            for i, (pid, name) in enumerate(rows):
                genres = await fetch_wikipedia_genres(client, sem, name)
                event_type = tags_to_type(genres) if genres else None
                category = TYPE_TO_CATEGORY.get(event_type, "Music") if event_type else "Music"

                conn.execute(text("""
                    UPDATE performers
                    SET genres=:g, event_type_name=:et, category=:cat, source='wikipedia'
                    WHERE id=:id
                """), {
                    "g": json.dumps(genres),
                    "et": event_type or "Concert",
                    "cat": category,
                    "id": pid,
                })

                if genres:
                    found_genre += 1
                if event_type and event_type != "Concert":
                    updated += 1

                if (i + 1) % save_every == 0:
                    conn.commit()
                    print(f"  {i+1}/{len(rows)} — genres found: {found_genre}, re-typed: {updated}", flush=True)

        conn.commit()
        print(f"  ✓ Wikipedia phase done: {found_genre} genres found, {updated} re-typed\n")
    return updated


# ── Main ──────────────────────────────────────────────────────────────────────

async def main(phase: int):
    total = 0
    if phase in (0, 1):
        total += phase1_remap()
    if phase in (0, 2):
        total += await phase2_wikipedia()

    if total > 0:
        print(f"Running recategorize_from_performers to apply changes to events...")
        import subprocess
        subprocess.run([sys.executable, "scripts/recategorize_from_performers.py"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", type=int, default=0, help="1=remap only, 2=wikipedia only, 0=both")
    args = parser.parse_args()
    asyncio.run(main(args.phase))
