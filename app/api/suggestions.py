"""Autocomplete suggestions endpoint.

Powered by an in-memory candidate index (app.api._suggestions_index)
so the request path is pure-Python list scanning — no DB round-trips
on the hot path. Average response time on cache miss: ~10-30ms vs
~200-500ms for the DB-backed predecessor.

The index refreshes every 30 minutes; warmed at app startup so the
first user request hits a hot index.

Two layers of caching together keep the perceived latency low:
  • In-memory candidate index (this module's source of truth)
  • Per-query response cache (5-min TTL, keyed by query string)

The frontend additionally caches the response client-side keyed
by the query string, so re-typed queries return instantly with no
fetch.
"""
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.api import _suggestions_index as idx_mod

# Sports event names follow "League - Home vs Away". When the query
# matches a league prefix exactly, we return only that sport so users
# can build a clean "NBA calendar" without other completions mixing in.
_MIN_SPORT_QUERY_LEN = 2

router = APIRouter(prefix="/api/suggestions", tags=["suggestions"])

# ── Per-query response cache (5-min TTL) ──────────────────────────────────
# Decoupled from the candidate index — same q from N users hits the cache,
# so even on a fresh deploy the first repeat saves an index scan.
_cache: dict = {}
_CACHE_TTL = 300  # seconds


def _cache_get(q: str) -> Optional[list]:
    entry = _cache.get(q)
    if entry and (datetime.utcnow() - entry["ts"]).total_seconds() < _CACHE_TTL:
        return entry["data"]
    return None


def _cache_set(q: str, data: list) -> None:
    _cache[q] = {"data": data, "ts": datetime.utcnow()}
    if len(_cache) > 500:
        cutoff = datetime.utcnow()
        stale = [k for k, v in _cache.items()
                 if (cutoff - v["ts"]).total_seconds() >= _CACHE_TTL]
        for k in stale:
            _cache.pop(k, None)


@router.get("")
def get_suggestions(
    q: str = Query(..., min_length=1),
    limit: int = Query(12, le=30),
    db: Session = Depends(get_db),
):
    """
    Returns autocomplete suggestions in priority order:
      0. Tournament (Tournament badge) — top priority; allowlisted set
         of named competitions (FIFA World Cup, …)
      1. Sub-genre  (Sub-genre badge)  — chip filters parent genre
      2. Genre      (Genre badge)      — typed parent name
      3. Artist     (Artist badge)
      3b. Sport team (Team badge)      — tied with Artist
      4. Format     (Format badge)     — EventType.name
      5. Category   (Category badge)
      6. Venue      (Venue badge)
      7. Event name (Event badge)

    See app.api._suggestions_index for the candidate-source-of-truth.
    """
    # Lowercase BEFORE the cache lookup too — the index entries are
    # pre-lowercased (`text_lower` in _suggestions_index), so uppercase
    # queries previously returned [] across the board (no chips, no
    # artists, no venues). 2026-05-12 user report: typing "Psychedelic
    # Rock" returned no chip even though the sub-genre + 92 tagged
    # artists exist. Caching the lowercase form also dedupes
    # case-variant requests (jazz vs Jazz vs JAZZ → one cache entry).
    q_stripped = q.strip().lower()
    cached = _cache_get(q_stripped)
    if cached is not None:
        return cached[:limit]
    PER_TYPE = 3
    idx = idx_mod.get_index(db)

    # ── Tournaments (top priority) ──────────────────────────────
    # Slot 0 — slim allowlisted set (FIFA World Cup, …). Computed
    # BEFORE the sport-league early-exit so a "world cup" query that
    # would have early-exited into "sport" chips gets the Tournament
    # chip prepended instead. Same data, stronger chip kind (strict
    # equality on Event.tournament vs the legacy freetext "sport"
    # chip which falls into the loose type_search bucket).
    tournaments = idx_mod.filter_tournaments(idx, q_stripped, PER_TYPE)

    # ── Sports league early-exit ──────────────────────────────────
    # Skipped entirely when a Tournament chip is already on the table
    # for the same query — the Tournament chip is the better surface
    # (single click → strict equality on the indexed tournament
    # column, vs. the early-exit's per-match-label noise where event
    # names like "World Cup: Match 1 - Group A - …" produce a dozen
    # near-identical "sport" chips that all map to the same loose
    # type_search filter).
    if tournaments:
        results = tournaments[:limit]
        _cache_set(q_stripped, results)
        return results
    league_chips = idx_mod.filter_sport_league_early_exit(
        idx, q_stripped, _MIN_SPORT_QUERY_LEN
    )
    if league_chips:
        results = league_chips[:limit]
        _cache_set(q_stripped, results)
        return results

    # ── Categories / Formats / Sport-teams ──────────────────────
    categories  = idx_mod.filter_categories(idx, q_stripped, PER_TYPE)
    event_types = idx_mod.filter_event_types(idx, q_stripped, PER_TYPE)
    sport_teams = (
        idx_mod.filter_sport_teams(idx, q_stripped, PER_TYPE)
        if len(q_stripped) >= _MIN_SPORT_QUERY_LEN else []
    )

    # ── Genres (parent + sub-genre) ─────────────────────────────
    # When the query matches a parent name directly, surface the
    # parent. Otherwise fall back to sub-genre matches.
    genres_results = idx_mod.filter_parent_genres(idx, q_stripped, PER_TYPE)
    sub_genres_results: list = []
    if not genres_results:
        sub_genres_results = idx_mod.filter_sub_genres(idx, q_stripped, PER_TYPE)

    # ── Themes ──────────────────────────────────────────────────
    # Themes are flat (no parent/sub distinction); for non-music
    # content (conferences, eventually workshops/festivals). Slot
    # them just below sub-genres in the cascade — they carry the
    # same "topic" semantics as genres but bind to events directly.
    themes_results = idx_mod.filter_themes(idx, q_stripped, PER_TYPE)

    # ── Artists / Venues / Event names ──────────────────────────
    artists = idx_mod.filter_artists(idx, q_stripped, PER_TYPE + 2)

    venue_results = idx_mod.filter_venues(idx, q_stripped, PER_TYPE)

    artist_names_seen = {
        (a.get("value") or "").lower() for a in artists if a.get("value")
    }
    event_results = idx_mod.filter_event_names(
        idx, q_stripped, PER_TYPE, artist_names_seen
    )

    # ── Final ordering — tournament / sub-genre / genre / artist &
    #    team / format / category / venue / event-name. See doc-comment
    #    for the rationale (Tournament is the highest-signal user
    #    intent when present; taxonomy chips dominate over specific
    #    artists named after genre words).
    results = (
        tournaments               # 0 (top)
        + sub_genres_results      # 1
        + genres_results          # 2
        + themes_results          # 2.5 — same "topic" semantics, non-music
        + artists                 # 3a
        + sport_teams             # 3b
        + event_types             # 4 (Format)
        + categories              # 5 (Category)
        + venue_results           # 6
        + event_results           # 7
    )[:limit]

    _cache_set(q_stripped, results)
    return results
