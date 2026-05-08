"""Improve artist→genre coverage in two phases.

Closes the gap between distinct upcoming-event artists and the
ArtistGenre table. Run idempotently — re-running only acts on still-
unmatched artists, never touches existing high-confidence rows.

PHASE A — Performer.genres bridge (free):
    For each unmatched upcoming artist that has Spotify/MusicBrainz
    tags sitting in Performer.genres, derive a primary_genre from the
    tags by:
        1. Direct match against an existing sub-genre name (high conf)
        2. Match against a parent name → pick a representative sub
           for that parent (medium conf)
        3. Alias map for common Spotify variants (edm, hip-hop, r&b)
    Writes ArtistGenre with source='spotify-bridge' — distinguishable
    from gemini-sourced rows so a later Brave-augmented re-classify
    pass can overwrite without ambiguity.

PHASE B — Gemini classification (~$0.30 for ~3K artists at flash
rates):
    For artists still unmatched after Phase A, call Gemini Flash in
    batches of 80 with the same prompt format the manual ingestion
    workflow expects. Validates responses against the existing
    taxonomy and writes ArtistGenre rows with source='gemini'.

Usage:
    python3 scripts/improve_genre_coverage.py                # both phases live
    python3 scripts/improve_genre_coverage.py --phase a      # bridge only
    python3 scripts/improve_genre_coverage.py --phase b      # classifier only
    python3 scripts/improve_genre_coverage.py --dry-run      # report, no writes
    python3 scripts/improve_genre_coverage.py --max 100      # cap per phase (test)
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
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
log = logging.getLogger("improve_genre_coverage")

from sqlalchemy import func, select, distinct, not_  # noqa: E402

from app.database import SessionLocal  # noqa: E402
from app.models import Event  # noqa: E402
from app.models.genre import ArtistGenre, GenreTaxonomy  # noqa: E402
from app.models.performer import Performer  # noqa: E402

from datetime import date  # noqa: E402


# ── Tag normalisation + lookup ─────────────────────────────────────────────

# Common Spotify tags that aren't sub-genre names but map cleanly to one
# of our parent genres. Values point at parent_genre names; the lookup
# layer then picks a representative sub-genre for that parent.
_PARENT_ALIASES: dict[str, str] = {
    # Hip-Hop family
    "edm": "Electronic",
    "hip hop": "Hip-Hop",
    "hip-hop": "Hip-Hop",
    "rap": "Hip-Hop",
    "r&b": "R&B",
    "rnb": "R&B",
    "soul": "R&B",
    "disco": "R&B",
    # Rock family
    "pop/rock": "Rock",
    "rock and roll": "Rock",
    "alt": "Rock",
    "alt-rock": "Rock",
    "alternative": "Rock",
    "metal": "Rock",
    "punk": "Rock",
    "indie": "Rock",
    "noise rock": "Rock",
    "experimental rock": "Rock",
    "psychobilly": "Rock",
    "noise": "Rock",
    # Pop family — Spotify often labels regional pop variants
    "j pop": "Pop",
    "arabic pop": "Pop",
    "bedroom pop": "Pop",
    "alternative pop": "Pop",
    "art pop": "Pop",
    # Electronic family
    "house": "Electronic",
    "electro house": "Electronic",
    "deep house": "Electronic",
    "electronic dance music": "Electronic",
    "electronica": "Electronic",
    "dance": "Electronic",
    "ambient": "Electronic",
    # Country/Folk
    "singer-songwriter": "Country",
    "folk-rock": "Country",
    "indie folk": "Country",
    # Comedy
    "observational comedy": "Comedy",
    "comedy": "Comedy",
    # World — keep mapped to Other if your taxonomy has an Other parent;
    # otherwise these'll fall through to Phase B where Gemini can decide.
    "world": "Other",
    "world music": "Other",
}


def normalize_tag(s: str) -> str:
    """Normalise a Spotify/MusicBrainz tag for taxonomy comparison only.

    Lowercase, collapse whitespace, AND replace hyphens with spaces —
    the last bit lets us match Spotify "hip-hop" against our "Hip-Hop"
    parent without per-tag aliasing. This function is for taxonomy
    lookup only; do NOT use it to canonicalise artist names for the
    ArtistGenre.normalized_name primary key — that has its own
    convention via normalize_artist_name() below.
    """
    s = (s or "").strip().lower()
    s = s.replace("-", " ")
    s = re.sub(r"\s+", " ", s)
    return s


def normalize_artist_name(s: str) -> str:
    """Mirror the existing Performer.normalized_name convention used
    across the codebase: lowercase + collapse internal whitespace, but
    KEEP hyphens. Matches the join behaviour SQLAlchemy uses for
    Event.artist_name → ArtistGenre.normalized_name (lower + trim).
    Don't substitute normalize_tag here — that would break joins on
    hyphenated names like "Hip-Hop Group".
    """
    return " ".join((s or "").lower().split())


@dataclass
class TaxonomyIndex:
    """Lookup tables built once from GenreTaxonomy.

    sub_lookup        normalised sub-genre  → (sub_genre, parent_genre)
    parent_lookup     normalised parent      → parent_genre
    parent_default_sub  parent_genre         → representative sub-genre
                       (the one with the most upcoming events at build
                        time, falling back to the first alphabetical sub
                        when there's no event signal)
    """
    sub_lookup: dict[str, tuple[str, str]]
    parent_lookup: dict[str, str]
    parent_default_sub: dict[str, str]


def build_taxonomy_index(db) -> TaxonomyIndex:
    rows = (
        db.query(GenreTaxonomy.sub_genre, GenreTaxonomy.parent_genre)
        .order_by(GenreTaxonomy.parent_genre, GenreTaxonomy.sub_genre)
        .all()
    )
    sub_lookup: dict[str, tuple[str, str]] = {}
    parent_lookup: dict[str, str] = {}
    by_parent: dict[str, list[str]] = {}
    for sg, pg in rows:
        sub_lookup[normalize_tag(sg)] = (sg, pg)
        parent_lookup[normalize_tag(pg)] = pg
        by_parent.setdefault(pg, []).append(sg)

    # Pick a representative sub per parent: the one with the highest
    # upcoming-event count via existing ArtistGenre+Event joins. Falls
    # back to alphabetical first sub when no events use the parent yet.
    today = date.today()
    norm_artist = func.lower(func.trim(Event.artist_name))
    sub_event_counts = dict(
        db.query(ArtistGenre.primary_genre, func.count(func.distinct(Event.id)))
        .select_from(Event)
        .join(ArtistGenre, ArtistGenre.normalized_name == norm_artist)
        .filter(
            Event.start_date >= today,
            Event.artist_name.isnot(None),
            Event.artist_name != "",
            ArtistGenre.primary_genre.isnot(None),
            ArtistGenre.primary_genre != "UNKNOWN",
        )
        .group_by(ArtistGenre.primary_genre)
        .all()
    )
    parent_default_sub: dict[str, str] = {}
    for parent, subs in by_parent.items():
        ranked = sorted(subs, key=lambda s: (-int(sub_event_counts.get(s, 0) or 0), s))
        parent_default_sub[parent] = ranked[0]

    return TaxonomyIndex(
        sub_lookup=sub_lookup,
        parent_lookup=parent_lookup,
        parent_default_sub=parent_default_sub,
    )


@dataclass
class BridgeMatch:
    primary: str         # sub-genre name to write
    parent: str          # parent_genre (informational; not stored)
    confidence: str      # 'high' | 'medium'
    via: str             # 'sub-direct' | 'parent-direct' | 'alias-parent'


def map_tags_to_genre(
    tags: list[str], idx: TaxonomyIndex
) -> Optional[BridgeMatch]:
    """Pick the best (primary, parent, confidence) for a tag list.

    Walks tags in order. Direct sub-genre match wins (high conf). Falls
    back to direct parent match (medium conf, representative sub
    chosen). Last resort is the alias map for common variants.
    Returns None if nothing maps — caller handles that as Phase B input.
    """
    # Pass 1: direct sub-genre match — strongest signal.
    for raw in tags:
        nt = normalize_tag(raw)
        if nt in idx.sub_lookup:
            sg, pg = idx.sub_lookup[nt]
            return BridgeMatch(primary=sg, parent=pg, confidence="high",
                               via="sub-direct")

    # Pass 2: direct parent name as tag (Spotify says "rock", we have a
    # "Rock" parent with multiple subs — pick the representative sub).
    for raw in tags:
        nt = normalize_tag(raw)
        if nt in idx.parent_lookup:
            pg = idx.parent_lookup[nt]
            sub = idx.parent_default_sub.get(pg)
            if sub:
                return BridgeMatch(primary=sub, parent=pg, confidence="medium",
                                   via="parent-direct")

    # Pass 3: alias map for common Spotify variants.
    for raw in tags:
        nt = normalize_tag(raw)
        target = _PARENT_ALIASES.get(nt)
        if not target:
            continue
        # The alias points at a parent name. Resolve via parent_lookup.
        target_norm = normalize_tag(target)
        if target_norm in idx.parent_lookup:
            pg = idx.parent_lookup[target_norm]
            sub = idx.parent_default_sub.get(pg)
            if sub:
                return BridgeMatch(primary=sub, parent=pg, confidence="medium",
                                   via="alias-parent")

    return None


# ── Shared: find unmatched artists ─────────────────────────────────────────

def fetch_unmatched_artists(db) -> list[str]:
    """Distinct lowercased artist_name values for upcoming events whose
    artists do NOT have a known (non-UNKNOWN) ArtistGenre row."""
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
        db.query(distinct(norm_artist))
        .filter(
            Event.start_date >= today,
            Event.artist_name.isnot(None),
            Event.artist_name != "",
            not_(norm_artist.in_(select(known_subq.c.normalized_name))),
        )
        .all()
    )
    return sorted({r[0] for r in rows if r[0]})


def fetch_performer_tags(
    db, normalized_names: list[str]
) -> dict[str, list[str]]:
    """Map normalized_name → list of Spotify/MusicBrainz tags from
    Performer.genres. Skips rows whose `genres` is null/empty/malformed.
    """
    if not normalized_names:
        return {}
    out: dict[str, list[str]] = {}
    rows = (
        db.query(Performer.normalized_name, Performer.genres)
        .filter(Performer.normalized_name.in_(normalized_names))
        .all()
    )
    for name, raw in rows:
        if not raw:
            continue
        try:
            tags = json.loads(raw) if isinstance(raw, str) else raw
        except (json.JSONDecodeError, TypeError):
            continue
        if isinstance(tags, list):
            cleaned = [str(t).strip() for t in tags if t and isinstance(t, (str,))]
            if cleaned:
                out[name] = cleaned
    return out


# ── Persistence ────────────────────────────────────────────────────────────

def upsert_artist_genre(
    db,
    *,
    artist_name: str,
    normalized_name: str,
    primary: str,
    secondary_1: Optional[str],
    secondary_2: Optional[str],
    confidence: str,
    source: str,
) -> tuple[ArtistGenre, bool]:
    """Insert or refresh a row keyed on normalized_name. Returns (row,
    created)."""
    existing = (
        db.query(ArtistGenre)
        .filter(ArtistGenre.normalized_name == normalized_name)
        .first()
    )
    if existing:
        existing.primary_genre = primary
        existing.secondary_1 = secondary_1
        existing.secondary_2 = secondary_2
        existing.confidence = confidence
        existing.source = source
        existing.classified_at = datetime.utcnow()
        return existing, False
    row = ArtistGenre(
        artist_name=artist_name,
        normalized_name=normalized_name,
        primary_genre=primary,
        secondary_1=secondary_1,
        secondary_2=secondary_2,
        confidence=confidence,
        source=source,
    )
    db.add(row)
    return row, True


# ── Phase A ────────────────────────────────────────────────────────────────

@dataclass
class PhaseStats:
    candidates: int = 0
    written: int = 0
    skipped_no_match: int = 0
    by_confidence: dict[str, int] = field(default_factory=dict)
    by_via: dict[str, int] = field(default_factory=dict)


def run_phase_a(db, idx: TaxonomyIndex, dry_run: bool, cap: Optional[int]) -> PhaseStats:
    log.info("=== Phase A: Performer.genres bridge ===")
    unmatched = fetch_unmatched_artists(db)
    tag_map = fetch_performer_tags(db, unmatched)
    log.info(
        f"Phase A pool: {len(unmatched)} unmatched upcoming artists, "
        f"{len(tag_map)} have Performer.genres tags"
    )

    stats = PhaseStats()
    processed = 0
    for normalized_name, tags in tag_map.items():
        if cap is not None and processed >= cap:
            break
        processed += 1
        stats.candidates += 1

        match = map_tags_to_genre(tags, idx)
        if not match:
            stats.skipped_no_match += 1
            continue

        # We don't have the original-cased artist name from the Performer
        # row (Performer.name is stored separately). Fetch it once for
        # display value; we re-use the normalized_name as the primary key.
        original = (
            db.query(Performer.name)
            .filter(Performer.normalized_name == normalized_name)
            .scalar()
        ) or normalized_name

        if not dry_run:
            upsert_artist_genre(
                db,
                artist_name=original,
                normalized_name=normalized_name,
                primary=match.primary,
                secondary_1=None,
                secondary_2=None,
                confidence=match.confidence,
                source="spotify-bridge",
            )
        stats.written += 1
        stats.by_confidence[match.confidence] = stats.by_confidence.get(match.confidence, 0) + 1
        stats.by_via[match.via] = stats.by_via.get(match.via, 0) + 1

    if not dry_run:
        db.commit()

    log.info(
        f"Phase A done: written={stats.written} "
        f"skipped_no_match={stats.skipped_no_match} "
        f"by_via={stats.by_via} by_confidence={stats.by_confidence}"
    )
    return stats


# ── Phase B ────────────────────────────────────────────────────────────────

# Reuse the validator from the manual ingest pipeline so we treat
# programmatic Gemini responses with the same lenience and fix-ups.
from scripts.ingest_gemini_classifications import (  # noqa: E402
    validate_classification,
    UNKNOWN,
)

# Mirrors the prompt the manual workflow has been using — see the
# `ingest_gemini_classifications.py` docstring for the JSON shape it
# expects back. We list the taxonomy in the prompt explicitly so Gemini
# can't drift onto unknown sub-genre labels (validator would reject and
# we'd waste tokens).
_PROMPT_TEMPLATE = """\
You are classifying performing artists, bands and acts into our two-level
genre taxonomy. For each artist, return ONE object with these EXACT keys:
  - artist     : the artist's name, copied verbatim from the input list
  - primary    : EXACTLY one sub-genre from the list below, OR the string "UNKNOWN"
  - secondary_1: another sub-genre from the list, or null
  - secondary_2: another sub-genre, or null
  - confidence : "high" | "medium" | "low"

Rules:
  R1. The "artist" key is REQUIRED on every entry — copy the input name
      exactly so we can match the response back to the request.
  R2. primary MUST be one of the listed sub-genres OR exactly "UNKNOWN".
      Never use a parent name as primary. Never invent labels.
  R3. If primary is "UNKNOWN", set confidence to "low" and both
      secondaries to null.
  R4. Pick "low" confidence when you genuinely don't recognise the artist
      — better to say UNKNOWN than guess.
  R5. Return EXACTLY one entry per input artist, in the same order.
      Do NOT skip artists — emit UNKNOWN with confidence=low if needed.
  R6. Output STRICTLY JSON: {{"classifications":[{{...}}, ...]}}.
      No prose, no markdown fences.

Example one-entry shape:
  {{"artist": "Foo Bar", "primary": "Hard Rock", "secondary_1": "Heavy Metal",
    "secondary_2": null, "confidence": "high"}}

Allowed sub-genres (canonical case — copy exactly):
{taxonomy}

Artists to classify:
{artists}
"""


def render_taxonomy_block(idx: TaxonomyIndex) -> str:
    """Render '<Parent>: sub, sub, …' lines for the prompt."""
    by_parent: dict[str, list[str]] = {}
    for sub, parent in idx.sub_lookup.values():
        by_parent.setdefault(parent, []).append(sub)
    lines = []
    for parent in sorted(by_parent.keys()):
        subs = sorted(by_parent[parent])
        lines.append(f"  {parent}: " + ", ".join(subs))
    return "\n".join(lines)


def _gemini_call(prompt: str, model: str = "gemini-2.5-flash") -> Optional[dict]:
    """One Gemini call with 3-attempt retry on transient failures."""
    import os
    from google import genai
    from google.genai import types as gtypes

    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        log.error("GEMINI_API_KEY not set — Phase B cannot run.")
        return None
    client = genai.Client(api_key=api_key)

    last_err: Optional[Exception] = None
    for attempt in range(3):
        try:
            resp = client.models.generate_content(
                model=model,
                contents=prompt,
                config=gtypes.GenerateContentConfig(
                    temperature=0.1,
                    response_mime_type="application/json",
                ),
            )
            raw = (resp.text or "").strip()
            if not raw:
                last_err = RuntimeError("empty response")
            else:
                try:
                    return json.loads(raw)
                except json.JSONDecodeError as e:
                    last_err = e
                    log.warning(f"  parse failure (attempt {attempt + 1}): {e}; "
                                f"first 200 chars: {raw[:200]!r}")
        except Exception as e:
            last_err = e
            msg = str(e).lower()
            transient = ("503" in msg or "unavailable" in msg
                         or "timeout" in msg or "deadline" in msg
                         or "resource_exhausted" in msg)
            if not transient or attempt == 2:
                log.warning(
                    f"  Gemini call failed (attempt {attempt + 1}/3): "
                    f"{type(e).__name__}: {e}"
                )
                return None
        time.sleep(2 ** attempt * 2)
    log.warning(f"  Gemini call exhausted retries: {last_err}")
    return None


def run_phase_b(
    db,
    idx: TaxonomyIndex,
    dry_run: bool,
    cap: Optional[int],
    batch_size: int = 80,
) -> PhaseStats:
    log.info("=== Phase B: Gemini classification ===")
    unmatched = fetch_unmatched_artists(db)
    if cap is not None:
        unmatched = unmatched[:cap]
    log.info(f"Phase B pool: {len(unmatched)} unmatched artists "
             f"(after Phase A)")

    valid_subs = {sg for sg, _ in idx.sub_lookup.values()}
    valid_parents = set(idx.parent_lookup.values())
    taxonomy_block = render_taxonomy_block(idx)

    stats = PhaseStats()
    n_batches = (len(unmatched) + batch_size - 1) // batch_size
    for bi, start in enumerate(range(0, len(unmatched), batch_size), start=1):
        batch = unmatched[start:start + batch_size]
        log.info(f"  Batch {bi}/{n_batches}: {len(batch)} artists")

        artist_block = "\n".join(f"  - {n}" for n in batch)
        prompt = _PROMPT_TEMPLATE.format(taxonomy=taxonomy_block, artists=artist_block)

        data = _gemini_call(prompt)
        if not data:
            log.warning(f"  Batch {bi} returned no data, skipping")
            continue

        classifications = data.get("classifications") or []
        if not isinstance(classifications, list):
            log.warning(f"  Batch {bi}: expected list under 'classifications', "
                        f"got {type(classifications).__name__}")
            continue

        # Positional fallback: if Gemini drops the `artist` field but
        # returns the same number of entries we sent, fill it in from
        # the input list in order. The prompt asks for matched ordering
        # and "no skips", so this is a reasonable recovery path. We
        # only apply it when ALL entries are missing the field — partial
        # gaps suggest a different problem.
        missing_artist = sum(
            1 for e in classifications
            if isinstance(e, dict) and not (e.get("artist") or e.get("artist_name") or e.get("name"))
        )
        if missing_artist == len(classifications) and len(classifications) == len(batch):
            log.info(f"  Batch {bi}: response had no 'artist' fields; "
                     f"recovering by input order ({len(batch)} entries match).")
            for ent, name in zip(classifications, batch):
                if isinstance(ent, dict):
                    ent["artist"] = name

        # Map original (lowercased) names → entries by normalised compare,
        # so out-of-order responses still write to the right artist.
        for entry in classifications:
            stats.candidates += 1
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
            if not dry_run:
                upsert_artist_genre(
                    db,
                    artist_name=artist_raw,
                    normalized_name=normalized,
                    primary=cleaned["primary"],
                    secondary_1=cleaned["secondary_1"],
                    secondary_2=cleaned["secondary_2"],
                    confidence=cleaned["confidence"],
                    source="gemini",
                )
            stats.written += 1
            stats.by_confidence[cleaned["confidence"]] = stats.by_confidence.get(cleaned["confidence"], 0) + 1
        if not dry_run:
            db.commit()

    log.info(
        f"Phase B done: written={stats.written} "
        f"skipped_no_match={stats.skipped_no_match} "
        f"by_confidence={stats.by_confidence}"
    )
    return stats


# ── Entry point ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--phase", choices=["a", "b", "both"], default="both")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report what would be written, no DB writes.")
    parser.add_argument("--max", type=int, default=None,
                        help="Per-phase artist cap (testing).")
    parser.add_argument("--batch-size", type=int, default=80,
                        help="Phase B Gemini batch size (default: 80).")
    args = parser.parse_args()

    db = SessionLocal()
    try:
        idx = build_taxonomy_index(db)
        log.info(
            f"Taxonomy: {len(set(p for _, p in idx.sub_lookup.values()))} parents / "
            f"{len(idx.sub_lookup)} sub-genres"
        )

        if args.phase in ("a", "both"):
            run_phase_a(db, idx, dry_run=args.dry_run, cap=args.max)
        if args.phase in ("b", "both"):
            run_phase_b(db, idx, dry_run=args.dry_run, cap=args.max,
                        batch_size=args.batch_size)
    finally:
        db.close()


if __name__ == "__main__":
    main()
