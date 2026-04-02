#!/usr/bin/env python3
"""
Option A artist cleanup:
  1. Clear when artist_name == event_name (exact, case-insensitive)
  2. Clear when SequenceMatcher similarity > 0.85 (artist ≈ event name)
  3. Clear known promoter / junk phrases
  4. Multi-artist lineups → keep only the first artist
  5. Clear when artist_name == venue_name (exact, case-insensitive)
  6. Clear very long names (>120 chars) that are unlikely to be a real artist
"""
import sys
import os
import re
import logging
from difflib import SequenceMatcher

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database import SessionLocal
from app.models import Event, Venue

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# ── Promoter / junk keywords ────────────────────────────────────────────────
JUNK_PATTERNS = [
    re.compile(r"\bpresents\b", re.I),
    re.compile(r"\bproductions?\b", re.I),
    re.compile(r"\baeg\b", re.I),
    re.compile(r"\blive nation\b", re.I),
    re.compile(r"\bvarious artists?\b", re.I),
    re.compile(r"\bvarious performers?\b", re.I),
    re.compile(r"\btba\b", re.I),
    re.compile(r"\btbd\b", re.I),
    re.compile(r"\bmultiple artists?\b", re.I),
    re.compile(r"\bopen mic\b", re.I),
    re.compile(r"\bguest artists?\b", re.I),
    re.compile(r"\bdepartment of\b", re.I),
    re.compile(r"\buniversity\b", re.I),
    re.compile(r"\bcollege\b", re.I),
    re.compile(r"\borgchestra\b", re.I),  # typo variant
    re.compile(r"\binc\.?\b", re.I),
    re.compile(r"\bllc\.?\b", re.I),
    re.compile(r"\bent(ertainment)?\b", re.I),
    re.compile(r"\bfeaturing\b", re.I),
]

# ── Multi-artist separators ──────────────────────────────────────────────────
# Split on these; keep only the first token
MULTI_ARTIST_SEPS = [
    r" b2b ",
    r" vs\.? ",
    r" × ",
    r" x ",  # intentional word boundary to avoid "Alex"
    r"\(live\)",   # trailing tag, not really a separator but treat it
]

def is_junk(name: str) -> bool:
    return any(p.search(name) for p in JUNK_PATTERNS)


def similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()


def extract_first_artist(name: str):
    """
    If the name looks like a multi-artist string, return the first artist.
    Returns None if the result is empty or looks like junk.
    """
    # Split on b2b / × / vs first (these are unambiguous)
    for sep in [r" b2b ", r" × ", r" vs\.? ", r" x "]:
        parts = re.split(sep, name, flags=re.I)
        if len(parts) >= 2:
            first = parts[0].strip().strip(",").strip()
            return first if first else None

    # Split on comma — only if there are 2+ commas (3+ artists)
    comma_parts = [p.strip() for p in name.split(",") if p.strip()]
    if len(comma_parts) >= 3:
        return comma_parts[0]

    return name  # unchanged


def clean_events(dry_run: bool = False) -> None:
    db = SessionLocal()
    try:
        # Pre-load venue names for fast lookup
        venue_names = {
            v.name.lower().strip()
            for v in db.query(Venue.name).all()
            if v.name
        }

        total = cleared = kept_first = unchanged = 0
        batch = []
        BATCH_SIZE = 500

        for event in db.query(Event).filter(Event.artist_name.isnot(None)).yield_per(500):
            total += 1
            artist = (event.artist_name or "").strip()
            if not artist:
                continue

            new_artist = artist
            reason = None

            # 1. Exact match with event name (case-insensitive)
            if artist.lower() == event.name.lower().strip():
                new_artist = None
                reason = "exact_match_event_name"

            # 2. Fuzzy match with event name
            elif similarity(artist, event.name) > 0.85:
                new_artist = None
                reason = "fuzzy_match_event_name"

            # 3. Very long → likely not a real artist name
            elif len(artist) > 120:
                new_artist = None
                reason = "too_long"

            # 4. Matches venue name
            elif artist.lower() in venue_names:
                new_artist = None
                reason = "matches_venue_name"

            # 5. Junk / promoter keywords
            elif is_junk(artist):
                new_artist = None
                reason = "junk_keyword"

            # 6. Multi-artist — keep only the first
            else:
                first = extract_first_artist(artist)
                if first and first != artist:
                    # Sanity check the extracted first name
                    if len(first) >= 2 and not is_junk(first):
                        new_artist = first
                        reason = "kept_first_of_multi"

            if new_artist != artist:
                if reason == "kept_first_of_multi":
                    kept_first += 1
                else:
                    cleared += 1

                logger.debug(
                    "[%s] id=%d  %r  →  %r",
                    reason, event.id,
                    artist[:80],
                    new_artist,
                )

                if not dry_run:
                    event.artist_name = new_artist
                    batch.append(event)
                    if len(batch) >= BATCH_SIZE:
                        db.commit()
                        batch.clear()
            else:
                unchanged += 1

        if not dry_run and batch:
            db.commit()

        mode = "DRY RUN — " if dry_run else ""
        logger.info(
            "%sResults: %d total | %d cleared | %d kept-first | %d unchanged",
            mode, total, cleared, kept_first, unchanged,
        )

    finally:
        db.close()


if __name__ == "__main__":
    dry = "--dry-run" in sys.argv
    if dry:
        logger.info("=== DRY RUN MODE ===")
    clean_events(dry_run=dry)
