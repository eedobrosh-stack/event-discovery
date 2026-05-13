"""In-memory autocomplete candidate index.

Powers /api/suggestions without round-tripping the DB on every
keystroke. Loaded once at app startup, refreshed every 30 minutes.
Each match query becomes a list scan in Python (low-tens-of-ms in
the worst case), eliminating the 7+ sequential SQL queries the
naive implementation issued per request.

What's indexed (per Route 1 priority order — see suggestions.py):
  artists           every distinct Event.artist_name (upcoming + past;
                    artist popularity tends to span event windows)
  sport_teams       distinct home_team / away_team (sports rows only)
  sport_event_names "League - Home vs Away" patterns for the
                    league-prefix early exit
  event_types       (EventType.name, EventType.category) pairs
  categories        distinct EventType.category
  parent_genres     GenreTaxonomy.parent_genre
  sub_genres        (sub_genre, parent_genre) pairs
  venues            (Venue.name, Venue.physical_city) pairs
  event_names       Event.name for upcoming events (mevalim
                    comedians, techconf conferences, etc.)

Each list pre-stores the lowercased form alongside the original so
the matcher only does string ops, never re-lowercases per scan.

Match semantics mirror app.api._search_filters.name_match_ilike
exactly so the user-facing behaviour is identical to the DB-backed
version. See ``name_matches()``.
"""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Refresh cadence — events.db churns slowly enough that 30 min staleness
# is invisible to users (artists rarely appear/vanish faster). The
# rebuild scans cheap indexed columns, takes < 2s on the prod data set.
INDEX_TTL_SECONDS = 1800

# Allowlist for the Tournament chip kind. The `events.tournament` column
# is populated broadly by the sport collectors + the backfill script,
# but only labels in this set become AC suggestions. v1 ships with just
# the FIFA World Cup so the surface starts narrow; add more here
# (e.g. "NBA", "Wimbledon", "Roland Garros", "US Open", "Premier League"…)
# to widen — no recompute needed, the index rebuild picks it up.
TOURNAMENT_ALLOWLIST: frozenset[str] = frozenset({
    "FIFA World Cup",
})

# Threshold below which we require a strict whole-word match (matches
# _search_filters._WHOLE_WORD_BELOW exactly).
_WHOLE_WORD_BELOW = 4


# ── Match semantics (Python mirror of name_match_ilike) ────────────────────

def _word_start_matches(text_lower: str, q: str) -> bool:
    """``q`` matches the start of any word in ``text_lower``.

    A "word start" is column start OR right after a space.
    Mirrors word_start_ilike's two ILIKE patterns.
    """
    return text_lower.startswith(q) or (" " + q) in text_lower


def _whole_word_matches(text_lower: str, q: str) -> bool:
    """``q`` matches as a complete word in ``text_lower``."""
    if text_lower == q:
        return True
    if text_lower.startswith(q + " "):
        return True
    if text_lower.endswith(" " + q):
        return True
    return f" {q} " in text_lower


def name_matches(text_lower: str, q: str) -> bool:
    """Length-aware, word-aware, multi-word match.

    Mirrors app.api._search_filters.name_match_ilike. ``text_lower``
    must already be lowercased by caller (avoids re-lowercasing per
    candidate — the index pre-stores the lowercased form).
    """
    parts = q.split()
    if not parts:
        return True
    if len(parts) == 1:
        p = parts[0]
        if len(p) >= _WHOLE_WORD_BELOW:
            return _word_start_matches(text_lower, p)
        return _whole_word_matches(text_lower, p)
    # Multi-word: every token must match as a word-start, in any order.
    # Per-token whole-word rule relaxed for short tokens here — the
    # other tokens act as the noise filter. Identical to the multi-word
    # branch in _search_filters.name_match_ilike.
    for p in parts:
        if not _word_start_matches(text_lower, p):
            return False
    return True


# ── Prefix bucket (3-char first-pass narrowing) ────────────────────────────
#
# For lists that are large enough that linear scan dominates ("sting"
# against 30K artists used to take ~53 ms), we bucket every candidate
# by the first 3 chars of every word it contains. A query that's >=3
# chars then only scans the relevant bucket, dropping the candidate
# count from ~30K to typically ~100-300. For shorter queries we fall
# back to flat scan; the index is too small at that point for bucket
# narrowing to matter.

class _PrefixBucket:
    """Map of 3-char prefix → list of candidates whose any-word starts
    with that prefix. Each candidate is a tuple whose [0] is the
    pre-lowered text — same shape the matchers downstream expect.
    A single candidate may live in multiple buckets (one per word).
    """
    __slots__ = ("by_prefix", "all_items")

    def __init__(self):
        # Stable order preserved within each bucket so downstream
        # ranking stays deterministic.
        self.by_prefix: dict[str, list[tuple]] = {}
        self.all_items: list[tuple] = []

    def add(self, item: tuple) -> None:
        self.all_items.append(item)
        text_lower = item[0]
        # De-dupe per item: a candidate "Sting Stings" shouldn't show
        # up twice in the same "sti" bucket.
        seen_prefixes: set[str] = set()
        for word in text_lower.split():
            if not word:
                continue
            key = word[:3] if len(word) >= 3 else word
            if key not in seen_prefixes:
                seen_prefixes.add(key)
                self.by_prefix.setdefault(key, []).append(item)

    def candidates_for(self, q: str) -> list[tuple]:
        """Return likely candidates for a single-token query.

        For queries 3+ chars: returns only items in the matching
        bucket. For shorter queries: returns the whole list (the
        ``name_matches`` path will still apply whole-word semantics).
        """
        if not q:
            return self.all_items
        if len(q) >= 3:
            return self.by_prefix.get(q[:3], [])
        # Short queries fall back to flat scan — the bucket might exist
        # under the literal short word, but we'd miss whole-word hits
        # like " jam " in "Pearl Jam" if we only checked one bucket.
        return self.all_items


# ── Index data ─────────────────────────────────────────────────────────────

@dataclass
class SuggestionsIndex:
    """Pre-loaded candidate strings for fast autocomplete matching.

    All `.lower` fields hold the lowercased form so callers don't
    re-lowercase per scan. The original-cased string is preserved
    alongside for display.

    The two largest lists (artists, event_names) also expose a
    PrefixBucket for first-pass candidate narrowing on >=3-char
    queries — turns the worst-case 30K-row scan into a typical
    100-300 row scan.
    """
    # (lower, original)
    artists: list[tuple[str, str]] = field(default_factory=list)
    artists_bucket: _PrefixBucket = field(default_factory=_PrefixBucket)
    sport_teams: list[tuple[str, str]] = field(default_factory=list)
    # (event_name_lower, event_name, sport_value)  — for league early-exit
    sport_event_names: list[tuple[str, str, str]] = field(default_factory=list)
    # (tournament_lower, tournament) — populated from distinct
    # events.tournament gated by TOURNAMENT_ALLOWLIST below. Top-priority
    # autocomplete chip kind. v1 allowlist is just "FIFA World Cup"; the
    # underlying column is populated more broadly (see
    # scripts/backfill_event_tournament.py) so widening the chip surface
    # is a one-line allowlist edit, no recompute.
    tournaments: list[tuple[str, str]] = field(default_factory=list)
    # (name_lower, name, category)
    event_types: list[tuple[str, str, str]] = field(default_factory=list)
    categories: list[tuple[str, str]] = field(default_factory=list)
    parent_genres: list[tuple[str, str]] = field(default_factory=list)
    # (sub_lower, sub, parent)
    sub_genres: list[tuple[str, str, str]] = field(default_factory=list)
    # (name_lower, name, physical_city)
    venues: list[tuple[str, str, Optional[str]]] = field(default_factory=list)
    event_names: list[tuple[str, str]] = field(default_factory=list)
    event_names_bucket: _PrefixBucket = field(default_factory=_PrefixBucket)
    built_at: datetime = field(default_factory=datetime.utcnow)

    def is_stale(self) -> bool:
        return (datetime.utcnow() - self.built_at).total_seconds() > INDEX_TTL_SECONDS


# Singleton + lock to prevent multiple concurrent rebuilds.
_index: Optional[SuggestionsIndex] = None
_index_lock = threading.Lock()


# ── Build ──────────────────────────────────────────────────────────────────

def build_index(db: Session) -> SuggestionsIndex:
    """Populate a fresh SuggestionsIndex from the DB. ~2s on prod data."""
    started = datetime.utcnow()
    idx = SuggestionsIndex()

    # Artists — distinct values from events. Past + future, since artist
    # popularity isn't time-bound for autocomplete suggestion purposes.
    #
    # Two-source UNION:
    #   1. Event.artist_name (the canonical column).
    #   2. Event.name from sources known to put the performer's name
    #      there with an empty artist_name — chiefly mevalim (Hebrew
    #      stand-up / shows) and techconf (conference speakers). Acts
    #      as a safety net when the collector hasn't populated
    #      artist_name yet (older rows pre-backfill, future collectors
    #      that miss the convention). De-duplicated case-insensitively
    #      with the canonical set so a name appearing in both columns
    #      shows up exactly once.
    seen_lower: set[str] = set()
    rows = db.execute(text("""
        SELECT DISTINCT artist_name FROM events
        WHERE artist_name IS NOT NULL AND artist_name != ''
    """)).fetchall()
    for r in rows:
        if r[0]:
            low = r[0].lower()
            if low in seen_lower:
                continue
            seen_lower.add(low)
            item = (low, r[0])
            idx.artists.append(item)
            idx.artists_bucket.add(item)

    # Safety-net branch — keep restricted to known performer-named
    # sources so we don't accidentally promote conference-event
    # titles or generic listing rows into the Artist surface.
    rows = db.execute(text("""
        SELECT DISTINCT name FROM events
        WHERE name IS NOT NULL AND name != ''
          AND (artist_name IS NULL OR artist_name = '')
          AND scrape_source IN ('mevalim', 'techconf')
    """)).fetchall()
    for r in rows:
        if r[0]:
            low = r[0].lower()
            if low in seen_lower:
                continue
            seen_lower.add(low)
            item = (low, r[0])
            idx.artists.append(item)
            idx.artists_bucket.add(item)

    # Sport teams — combine home_team + away_team, dedupe case-insensitively.
    seen_teams: dict[str, str] = {}
    for col in ("home_team", "away_team"):
        rows = db.execute(text(f"""
            SELECT DISTINCT {col} FROM events
            WHERE sport IS NOT NULL AND {col} IS NOT NULL AND {col} != ''
        """)).fetchall()
        for (t,) in rows:
            if t:
                seen_teams.setdefault(t.lower(), t)
    idx.sport_teams = [(k, v) for k, v in seen_teams.items()]

    # Sport event names — for the league-prefix early-exit.
    rows = db.execute(text("""
        SELECT DISTINCT name, sport FROM events
        WHERE sport IS NOT NULL
        LIMIT 5000
    """)).fetchall()
    idx.sport_event_names = [
        (r[0].lower(), r[0], r[1]) for r in rows if r[0] and r[1]
    ]

    # EventType.name + category, deduped to the (name, category) pair.
    rows = db.execute(text("""
        SELECT DISTINCT name, category FROM event_types
        WHERE name IS NOT NULL AND name != ''
    """)).fetchall()
    idx.event_types = [(r[0].lower(), r[0], r[1] or "") for r in rows]

    rows = db.execute(text("""
        SELECT DISTINCT category FROM event_types
        WHERE category IS NOT NULL AND category != ''
    """)).fetchall()
    idx.categories = [(r[0].lower(), r[0]) for r in rows]

    # Genres — split parent + sub.
    rows = db.execute(text("""
        SELECT DISTINCT parent_genre FROM genre_taxonomy
        WHERE parent_genre IS NOT NULL
    """)).fetchall()
    idx.parent_genres = [(r[0].lower(), r[0]) for r in rows]

    rows = db.execute(text("""
        SELECT sub_genre, parent_genre FROM genre_taxonomy
        WHERE sub_genre IS NOT NULL AND parent_genre IS NOT NULL
    """)).fetchall()
    idx.sub_genres = [(r[0].lower(), r[0], r[1]) for r in rows]

    # Venues — name + physical_city for label hint.
    rows = db.execute(text("""
        SELECT DISTINCT name, physical_city FROM venues
        WHERE name IS NOT NULL AND name != ''
    """)).fetchall()
    idx.venues = [(r[0].lower(), r[0], r[1]) for r in rows]

    # Event names — only upcoming, dedupe filter applied at match time.
    # Filter out the "League - Home vs Away" sport pattern early (it's
    # surfaced via teams/sports already, would just clutter the Event slot).
    rows = db.execute(text("""
        SELECT DISTINCT name FROM events
        WHERE name IS NOT NULL AND name != ''
          AND start_date >= DATE('now')
    """)).fetchall()
    for r in rows:
        if r[0] and " - " not in r[0]:
            item = (r[0].lower(), r[0])
            idx.event_names.append(item)
            idx.event_names_bucket.add(item)

    # Tournaments — distinct events.tournament values, gated by an
    # allowlist so the chip surface ships incrementally. The DB column
    # is populated for all ESPN team-sport + tennis rows (see
    # scripts/backfill_event_tournament.py and the espn/tennis
    # collectors), but only allowlisted labels become AC suggestions.
    # Widening the chip surface is a one-line edit to TOURNAMENT_ALLOWLIST.
    rows = db.execute(text("""
        SELECT DISTINCT tournament FROM events
        WHERE tournament IS NOT NULL AND tournament != ''
    """)).fetchall()
    idx.tournaments = [
        (r[0].lower(), r[0]) for r in rows
        if r[0] and r[0] in TOURNAMENT_ALLOWLIST
    ]

    elapsed_ms = (datetime.utcnow() - started).total_seconds() * 1000
    logger.info(
        f"_suggestions_index built: artists={len(idx.artists)} "
        f"teams={len(idx.sport_teams)} types={len(idx.event_types)} "
        f"venues={len(idx.venues)} events={len(idx.event_names)} "
        f"tournaments={len(idx.tournaments)} "
        f"elapsed_ms={elapsed_ms:.0f}"
    )
    return idx


def get_index(db: Session) -> SuggestionsIndex:
    """Return the live index, rebuilding if stale or never built.

    Lock-protected so concurrent first-callers don't double-build.
    """
    global _index
    if _index is not None and not _index.is_stale():
        return _index
    with _index_lock:
        if _index is None or _index.is_stale():
            _index = build_index(db)
    return _index


def warm_index(db: Session) -> None:
    """Trigger a build so the first user request lands on a hot index.

    Called once from app startup. Failures are logged and swallowed —
    the lazy path in get_index will retry on the first real request.
    """
    try:
        get_index(db)
    except Exception as e:
        logger.warning(f"warm_index failed: {type(e).__name__}: {e}")


def invalidate_index() -> None:
    """Force a rebuild on the next get_index call. Public hook for any
    future write path that wants to reflect changes immediately."""
    global _index
    _index = None


# ── Per-category matchers ──────────────────────────────────────────────────
#
# Each takes the index, the lowercased query string, and a result limit.
# Returns a list of plain dicts in the suggestion API output shape.
# Filtering uses name_matches() so behaviour is identical to the DB
# version — no semantic drift between the two paths.

def _take_matches(items, q: str, limit: int, projector):
    """Linear-scan ``items`` (each item: tuple whose [0] is the lowered
    string) and apply ``projector(item)`` to every match. Returns up
    to ``limit`` results. Early-terminates once limit reached.
    """
    out: list = []
    for item in items:
        if name_matches(item[0], q):
            out.append(projector(item))
            if len(out) >= limit:
                break
    return out


def _bucket_candidates_for_query(bucket: _PrefixBucket, q: str) -> list[tuple]:
    """Pick candidates for the matcher.

    Single-token queries with a token >=3 chars hit the bucket; shorter
    or empty tokens fall back to the full list. Multi-token queries
    use the bucket of the FIRST token whose length >= 3 — every other
    token still gets verified by name_matches, but narrowing on one is
    enough to dominate the cost.
    """
    parts = q.split()
    if not parts:
        return bucket.all_items
    for p in parts:
        if len(p) >= 3:
            return bucket.candidates_for(p)
    # All tokens short — flat scan.
    return bucket.all_items


def filter_artists(idx: SuggestionsIndex, q: str, limit: int) -> list[dict]:
    """Artists ranked by exact > prefix > word-start (mirrors original
    SQL relevance CASE). All matches are collected, then sorted, then
    capped — the limit is applied AFTER ranking, not during scan."""
    q_lower = q.lower()
    matches: list[tuple[int, str]] = []  # (relevance, original)
    candidates = _bucket_candidates_for_query(idx.artists_bucket, q)
    for low, original in candidates:
        if not name_matches(low, q):
            continue
        if low == q_lower:
            rank = 0
        elif low.startswith(q_lower):
            rank = 1
        else:
            rank = 2
        matches.append((rank, original))
    matches.sort(key=lambda x: (x[0], x[1]))
    return [
        {"kind": "performer", "value": v, "label": v, "badge": "Artist"}
        for _, v in matches[:limit]
    ]


def filter_sport_teams(idx: SuggestionsIndex, q: str, limit: int) -> list[dict]:
    return _take_matches(
        idx.sport_teams, q, limit,
        lambda it: {"kind": "sport_team", "value": it[1], "label": it[1],
                    "badge": "Team"},
    )


def filter_tournaments(idx: SuggestionsIndex, q: str, limit: int) -> list[dict]:
    """Tournaments — top-priority chip kind, prepended above sub-genre
    in the final ordering (see suggestions.py). Source is the
    TOURNAMENT_ALLOWLIST gate over distinct events.tournament; v1 ships
    with "FIFA World Cup" only."""
    return _take_matches(
        idx.tournaments, q, limit,
        lambda it: {"kind": "tournament", "value": it[1], "label": it[1],
                    "badge": "Tournament"},
    )


def filter_event_types(idx: SuggestionsIndex, q: str, limit: int) -> list[dict]:
    return _take_matches(
        idx.event_types, q, limit,
        lambda it: {"kind": "event_type", "value": it[1], "label": it[1],
                    "badge": "Format"},
    )


def filter_categories(idx: SuggestionsIndex, q: str, limit: int) -> list[dict]:
    return _take_matches(
        idx.categories, q, limit,
        lambda it: {"kind": "category", "value": it[1], "label": it[1],
                    "badge": "Category"},
    )


def filter_parent_genres(idx: SuggestionsIndex, q: str, limit: int) -> list[dict]:
    return _take_matches(
        idx.parent_genres, q, limit,
        lambda it: {"kind": "genre", "value": it[1], "label": it[1],
                    "badge": "Genre"},
    )


def filter_sub_genres(idx: SuggestionsIndex, q: str, limit: int) -> list[dict]:
    """Sub-genre chip — value is the parent (Flavor 1)."""
    seen: set[str] = set()
    out: list[dict] = []
    for low, sub, parent in idx.sub_genres:
        if sub in seen:
            continue
        if name_matches(low, q):
            seen.add(sub)
            out.append({
                "kind": "genre",
                "value": parent,
                "label": sub,
                "badge": "Sub-genre",
            })
            if len(out) >= limit:
                break
    return out


def filter_venues(idx: SuggestionsIndex, q: str, limit: int) -> list[dict]:
    """Venues match against name OR physical_city — same OR-logic as
    the original SQL filter."""
    out: list[dict] = []
    for name_lower, name, city in idx.venues:
        match = name_matches(name_lower, q)
        if not match and city:
            match = name_matches(city.lower(), q)
        if match:
            label = f"{name} — {city}" if city else name
            out.append({"kind": "venue", "value": name, "label": label,
                        "badge": "Venue"})
            if len(out) >= limit:
                break
    return out


def filter_event_names(idx: SuggestionsIndex, q: str, limit: int,
                       exclude_lower: set[str]) -> list[dict]:
    """Event names ranked by exact > prefix > substring; excludes
    anything already surfaced as an artist (caller passes the
    lowercased artist values it already used)."""
    q_lower = q.lower()
    matches: list[tuple[int, str]] = []
    candidates = _bucket_candidates_for_query(idx.event_names_bucket, q)
    for low, original in candidates:
        if low in exclude_lower:
            continue
        if not name_matches(low, q):
            continue
        if low == q_lower:
            rank = 0
        elif low.startswith(q_lower):
            rank = 1
        else:
            rank = 2
        matches.append((rank, original))
    matches.sort(key=lambda x: (x[0], x[1]))
    return [
        {"kind": "event", "value": v, "label": v, "badge": "Event"}
        for _, v in matches[:limit]
    ]


def filter_sport_league_early_exit(
    idx: SuggestionsIndex, q: str, min_len: int
) -> Optional[list[dict]]:
    """If the query matches a league prefix on a sport-event name
    (NBA, EuroLeague, …), return the league chip(s) only — same
    early-exit semantics as the SQL version. None means "no match,
    continue with the regular pipeline".
    """
    if len(q) < min_len:
        return None
    q_lower = q.lower()
    leagues: dict[str, str] = {}
    for low, name, _sport in idx.sport_event_names:
        if not low.startswith(q_lower):
            continue
        if " - " not in name:
            continue
        label = name.split(" - ")[0].strip()
        if label.lower().startswith(q_lower):
            leagues[label] = label
    if not leagues:
        return None
    return [
        {"kind": "sport", "value": label, "label": label, "badge": "Sport"}
        for label in sorted(leagues)
    ]
