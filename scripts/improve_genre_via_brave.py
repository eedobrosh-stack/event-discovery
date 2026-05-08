"""Brave-augmented re-classify of UNKNOWN/unmatched upcoming artists (Lever C).

After Phase A (tag bridge) and Phase B (pure-Gemini classifier) in
scripts/improve_genre_coverage.py, the residual gap is artists that
Gemini doesn't recognise from its training data alone. This script
closes that gap by feeding Gemini real search-engine context for each
artist before asking for a classification — same retrieval-augmented
pattern we use for Cadence B discovery, applied to artist-genre
classification.

Pipeline:
  1. Pick targets — distinct upcoming-event artists that are NOT in
     the "known" set (no row, OR primary_genre IS NULL/'UNKNOWN').
     Ranked by upcoming-event count desc, so a partial run still
     covers the highest-impact artists first.
  2. For each target: Brave search with the artist name. Take the
     top 3 organic results' (url, title, snippet). Cache to a JSONL
     file so re-runs (after failures, partial caps, etc.) don't
     double-spend on the same artist.
  3. Gemini batch-classify with the snippets as context. Same
     taxonomy + validator the manual ingest workflow uses, but the
     prompt frames the snippets as primary signal — long-tail
     artists that didn't ring a bell from name alone often do once
     the Wikipedia/AllMusic blurb is in context.
  4. Write ArtistGenre rows with source='brave-bridge', overwriting
     any existing UNKNOWN rows (matches the seed-loader policy:
     known always wins over UNKNOWN).

Cost estimate:
  Brave: ~$5 / 1,000 queries (Search plan). Free $5 monthly credit.
  Gemini: ~$0.005 / 50-batch.
  Full pass at ~2,200 artists ≈ $11.

Runtime: dominated by Brave at ~1 qps → ~40 min for a full sweep.

Usage:
  python3 scripts/improve_genre_via_brave.py                # full live pass
  python3 scripts/improve_genre_via_brave.py --max 50       # cap (testing)
  python3 scripts/improve_genre_via_brave.py --dry-run      # report, no writes
  python3 scripts/improve_genre_via_brave.py --no-fetch     # use cache only
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(str(ROOT / ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("brave_reclassify")

from sqlalchemy import func, select, distinct, not_  # noqa: E402

from app.database import SessionLocal  # noqa: E402
from app.models import Event  # noqa: E402
from app.models.genre import ArtistGenre, GenreTaxonomy  # noqa: E402
from app.extractors.discovery_search import brave_search  # noqa: E402

# Re-use functions from the pre-existing script so the two scripts share
# the same taxonomy index, the same artist-name canonicalisation, and the
# same Gemini call wrapper. Keeps Phase B and Phase C operationally
# consistent.
from scripts.improve_genre_coverage import (  # noqa: E402
    build_taxonomy_index,
    normalize_artist_name,
    upsert_artist_genre,
    _gemini_call,
    render_taxonomy_block,
)
from scripts.ingest_gemini_classifications import (  # noqa: E402
    validate_classification,
)


# ── Cache ──────────────────────────────────────────────────────────────────

CACHE_PATH = ROOT / "scripts" / "_brave_cache.jsonl"


def load_cache() -> dict[str, list[dict]]:
    """Read the JSONL cache into {normalized_artist → results list}.

    JSONL is append-only and durable across interrupts. If the same
    artist appears multiple times (re-queried during dev), the LAST
    entry wins.
    """
    cache: dict[str, list[dict]] = {}
    if not CACHE_PATH.exists():
        return cache
    with CACHE_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            artist = rec.get("artist")
            if artist:
                cache[artist] = rec.get("results") or []
    return cache


def append_cache(artist: str, results: list[dict]) -> None:
    """Append one entry to the cache file. fsync to survive a crash."""
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    rec = {
        "artist": artist,
        "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "results": results,
    }
    with CACHE_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())


# ── Target selection ──────────────────────────────────────────────────────

def fetch_unmatched_with_event_counts(db) -> list[tuple[str, int]]:
    """Distinct lowercased upcoming-event artist names that aren't in
    the 'known' set, ordered by upcoming-event count descending.

    Returns list of (normalized_name, event_count). Highest-impact
    artists first so a partial run still covers them.
    """
    today = date.today()
    norm_artist = func.lower(func.trim(Event.artist_name))

    known_subq = (
        db.query(ArtistGenre.normalized_name)
        .filter(
            ArtistGenre.primary_genre.isnot(None),
            ArtistGenre.primary_genre != "UNKNOWN",
        )
        .subquery()
    )

    rows = (
        db.query(norm_artist.label("name"), func.count(Event.id).label("n"))
        .filter(
            Event.start_date >= today,
            Event.artist_name.isnot(None),
            Event.artist_name != "",
            not_(norm_artist.in_(select(known_subq.c.normalized_name))),
        )
        .group_by(norm_artist)
        .order_by(func.count(Event.id).desc())
        .all()
    )
    return [(r[0], int(r[1])) for r in rows if r[0]]


# ── Brave ──────────────────────────────────────────────────────────────────

# Conservative pacing — Brave's free tier is 1 qps; the Search ($5/1k)
# plan should accept higher, but a 1.1s spacing keeps us comfortably
# below any per-second cap and produces deterministic timing for the
# user's monthly budget calculation.
BRAVE_QPS_DELAY = 1.1


def fetch_artist_context(artist: str) -> list[dict]:
    """Brave search for an artist, return top 3 (url, title, snippet) dicts.

    Brave returns SearchHit objects from our shared discovery_search
    module. We unpack into plain dicts so they survive the JSONL cache.
    Empty list on Brave failure (gets logged inside brave_search).
    """
    hits = brave_search(artist, n=3)
    return [
        {"url": h.url, "title": h.title, "snippet": h.snippet}
        for h in hits[:3]
    ]


# ── Gemini classifier with snippet context ────────────────────────────────

_PROMPT_TEMPLATE = """\
You are classifying performing artists, bands and acts into our two-level
genre taxonomy. For each artist below you have search-engine snippets
(URL + title + first ~250 chars of the page description). Use the
SNIPPETS as your primary signal — your training data may not include
these long-tail artists, but the snippets are real and current.

For each artist, return ONE object with these EXACT keys:
  - artist     : the artist's name, copied verbatim from the input
  - primary    : EXACTLY one sub-genre from the list below, OR "UNKNOWN"
  - secondary_1: another sub-genre from the list, or null
  - secondary_2: another sub-genre, or null
  - confidence : "high" | "medium" | "low"

Rules:
  R1. The "artist" key is REQUIRED on every entry — copy verbatim.
  R2. primary MUST be one of the listed sub-genres OR "UNKNOWN".
      Never use a parent name as primary. Never invent labels.
  R3. If primary is "UNKNOWN", set confidence=low and both secondaries=null.
  R4. Use UNKNOWN only when the snippets are clearly NOT about a
      performing artist (a venue, a city event, a non-music brand, a
      person with the same name in a different field). When the snippets
      identify a real artist, classify them — even a hesitant guess at
      "medium" or "low" is more useful than UNKNOWN.
  R5. Return EXACTLY one entry per input artist, in the same order.
      Do not skip artists.
  R6. Output STRICTLY JSON: {{"classifications":[{{...}}, ...]}}.
      No prose, no markdown fences.

Allowed sub-genres (canonical case — copy exactly):
{taxonomy}

Artists to classify (with search context):
{artists_block}
"""


def render_artists_block(items: list[tuple[str, list[dict]]]) -> str:
    """Render numbered ARTIST + snippets blocks for the prompt."""
    blocks: list[str] = []
    for name, hits in items:
        if not hits:
            blocks.append(f"ARTIST: {name}\n  (no search results)")
            continue
        lines = [f"ARTIST: {name}"]
        for i, h in enumerate(hits[:3], start=1):
            snippet = (h.get("snippet") or "").replace("\n", " ").strip()[:280]
            title = (h.get("title") or "").strip()[:140]
            url = (h.get("url") or "").strip()
            lines.append(f"  [{i}] {title}\n      {url}\n      {snippet}")
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks)


# ── Main ──────────────────────────────────────────────────────────────────

@dataclass
class Stats:
    targeted: int = 0
    cached_hits: int = 0
    brave_calls: int = 0
    brave_empty: int = 0
    classified: int = 0
    skipped_no_match: int = 0
    by_confidence: dict[str, int] = field(default_factory=dict)


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--max", type=int, default=None,
                        help="Cap on artists processed (testing).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report only — no DB writes (Brave still queried).")
    parser.add_argument("--no-fetch", action="store_true",
                        help="Use cache only, skip Brave calls. Useful for "
                             "re-classifying after a prompt tweak.")
    parser.add_argument("--batch-size", type=int, default=30,
                        help="Gemini batch size (default 30; lower than "
                             "Phase B's 80 because each entry carries ~3 "
                             "snippets of context).")
    args = parser.parse_args()

    db = SessionLocal()
    try:
        idx = build_taxonomy_index(db)
        valid_subs = {sg for sg, _ in idx.sub_lookup.values()}
        valid_parents = set(idx.parent_lookup.values())
        taxonomy_block = render_taxonomy_block(idx)

        log.info("Fetching unmatched upcoming-event artists ranked by event count …")
        targets = fetch_unmatched_with_event_counts(db)
        log.info(f"Pool: {len(targets)} unmatched upcoming-event artists")
        if args.max:
            targets = targets[:args.max]
            log.info(f"Capped to top {len(targets)} by event count")

        cache = load_cache()
        log.info(f"Cache: {len(cache)} previously-fetched artists on disk")

        stats = Stats()

        # ── Phase: gather context ─────────────────────────────────────
        artist_context: list[tuple[str, list[dict]]] = []
        log.info("Phase 1/2: gathering search context …")
        for i, (name, ev_count) in enumerate(targets, start=1):
            stats.targeted += 1
            if name in cache:
                stats.cached_hits += 1
                artist_context.append((name, cache[name]))
                continue
            if args.no_fetch:
                # Cache miss in cache-only mode — skip; can't classify
                # without context.
                continue
            results = fetch_artist_context(name)
            stats.brave_calls += 1
            if not results:
                stats.brave_empty += 1
            append_cache(name, results)
            artist_context.append((name, results))
            if i % 25 == 0:
                log.info(f"  Brave: {i}/{len(targets)} "
                         f"(cache_hits={stats.cached_hits}, "
                         f"empty_responses={stats.brave_empty})")
            time.sleep(BRAVE_QPS_DELAY)

        log.info(f"Context gathered: {len(artist_context)} artists with "
                 f"snippets ({stats.cached_hits} from cache, "
                 f"{stats.brave_calls} new Brave calls)")

        # ── Phase: classify in batches ────────────────────────────────
        log.info("Phase 2/2: Gemini classification …")
        n_batches = (len(artist_context) + args.batch_size - 1) // args.batch_size
        for bi, start in enumerate(range(0, len(artist_context), args.batch_size), start=1):
            batch = artist_context[start:start + args.batch_size]
            log.info(f"  Batch {bi}/{n_batches}: {len(batch)} artists")
            prompt = _PROMPT_TEMPLATE.format(
                taxonomy=taxonomy_block,
                artists_block=render_artists_block(batch),
            )
            data = _gemini_call(prompt)
            if not data:
                log.warning(f"  Batch {bi}: no Gemini response, skipping")
                continue
            classifications = data.get("classifications") or []
            if not isinstance(classifications, list):
                log.warning(f"  Batch {bi}: bad shape under 'classifications'")
                continue

            # Positional fallback if Gemini drops `artist` from entries
            # (same pattern as Phase B).
            missing_artist = sum(
                1 for e in classifications
                if isinstance(e, dict) and not (e.get("artist") or e.get("artist_name") or e.get("name"))
            )
            if (missing_artist == len(classifications)
                    and len(classifications) == len(batch)):
                log.info(f"  Batch {bi}: response had no 'artist' fields; "
                         f"recovering by input order.")
                for ent, (name, _) in zip(classifications, batch):
                    if isinstance(ent, dict):
                        ent["artist"] = name

            for entry in classifications:
                cleaned, reject_reason, _fixups = validate_classification(
                    entry, valid_subs=valid_subs, valid_parents=valid_parents
                )
                if reject_reason or not cleaned:
                    stats.skipped_no_match += 1
                    if reject_reason:
                        log.debug(f"  reject {entry.get('artist')!r}: {reject_reason}")
                    continue
                artist_raw = cleaned["artist"]
                normalized = normalize_artist_name(artist_raw)
                if not args.dry_run:
                    upsert_artist_genre(
                        db,
                        artist_name=artist_raw,
                        normalized_name=normalized,
                        primary=cleaned["primary"],
                        secondary_1=cleaned["secondary_1"],
                        secondary_2=cleaned["secondary_2"],
                        confidence=cleaned["confidence"],
                        source="brave-bridge",
                    )
                stats.classified += 1
                conf = cleaned["confidence"]
                stats.by_confidence[conf] = stats.by_confidence.get(conf, 0) + 1
            if not args.dry_run:
                db.commit()

        log.info(
            f"Done. targeted={stats.targeted} brave_calls={stats.brave_calls} "
            f"brave_empty={stats.brave_empty} cached_hits={stats.cached_hits} "
            f"classified={stats.classified} skipped_no_match={stats.skipped_no_match} "
            f"by_confidence={stats.by_confidence}"
        )
    finally:
        db.close()


if __name__ == "__main__":
    main()
