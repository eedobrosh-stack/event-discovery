"""Normalize sport-tournament events that landed via Ticketmaster.

The Ticketmaster collector ingests FIFA World Cup 2026 fixtures with
notably messy strings:

  - Literal "???" sequences inside the event name where some non-ASCII
    separator got mojibake-replaced upstream (e.g. "World Cup: Match
    30 ??? Group C - Scotland vs Morocco").
  - ``home_team`` / ``away_team`` parsed by splitting the full name on
    " vs " without trimming the surrounding prefix/suffix — so
    home_team ends up as ``"World Cup: Match 1 Group A - Mexico"`` and
    away_team as ``"South Africa - 2026 World Cup"``.
  - ``artist_name`` set to the tournament name itself
    (``"2026 Soccer World Cup"``) which leaks into the Artist column
    on the results page.

This script normalizes those fields for **every event with a non-NULL
``tournament``**, idempotently:

  1. ``name`` — collapse `` ??? `` (with surrounding spaces) to a
     single space; also strip stray double-spaces left behind.
  2. ``home_team`` / ``away_team`` — re-derive from the cleaned name
     using a small set of patterns. When the name shape can't be
     parsed cleanly, leave the existing values alone (don't write
     degenerate strings).
  3. ``artist_name`` — for tournament rows, NULL it out so the Artist
     column on the frontend reads "-" (the frontend renders Teams
     instead for tournament rows; see app.js).
  4. **event_type m2m** — remove any non-Sports event_type
     associations (Ticketmaster often mis-tags WC fixtures as
     "Concert / Music") and ensure at least one Sports association
     exists, preferring "Sports Event" if present in the catalog,
     otherwise "Soccer Match" as a fallback for soccer rows.

Re-running after a clean pass is a no-op — every step is checked
before writing.

Naming patterns currently in the wild for FIFA World Cup 2026:

  - "World Cup: Match N Group X - HOME vs AWAY - 2026 World Cup"
  - "World Cup: Match N Group X - HOME vs AWAY - 2026 Soccer World Cup"
  - "World Cup Round of N: HOME vs. AWAY (Match N) - 2026 World Cup"

The parser handles all three; anything else falls through to "skip".

Usage:
    PYTHONPATH=. python3 scripts/normalize_tournament_events.py
    PYTHONPATH=. python3 scripts/normalize_tournament_events.py --apply
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
from collections import Counter
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(str(ROOT / ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("normalize_tournament_events")

from sqlalchemy import text  # noqa: E402

from app.database import SessionLocal  # noqa: E402
from app.models import Event, EventType, event_event_types  # noqa: E402

# ── Name cleanup ──────────────────────────────────────────────────────
# Collapse the literal "???" sequence (with one or more surrounding
# spaces) into a single space. Don't touch standalone single-? marks
# that might legitimately appear in event names.
_QMARK_RUN = re.compile(r"\s+\?{2,}\s+")
_MULTI_SPACE = re.compile(r"\s{2,}")
# Trailing " - 2026 World Cup" / " - 2026 Soccer World Cup" suffix —
# noise from Ticketmaster, drop it during team parsing only (we keep
# the original suffix in the event name itself since users may search
# for "2026 World Cup" as free text).
_WC_TRAILING_SUFFIX = re.compile(
    r"\s*-\s*2026\s+(soccer\s+)?world\s+cup\s*$",
    flags=re.IGNORECASE,
)


def clean_name(name: str | None) -> str | None:
    if not name:
        return name
    n = _QMARK_RUN.sub(" ", name)
    n = _MULTI_SPACE.sub(" ", n).strip()
    return n or None


# ── Team parsing ──────────────────────────────────────────────────────
# After name cleanup, the relevant team-bearing patterns look like:
#   "World Cup: Match 1 Group A - Mexico vs South Africa"
#   "World Cup Round of 32: 1A vs. TBD (Match 79)"
#   "World Cup: Match 78 Group E runners up v Group I runners-up"
# We strip the trailing "(Match N)" / suffix tag, then split on the
# separator (case-insensitive): " v ", " vs ", or " vs. ". Ticketmaster
# uses all three depending on the round/group placeholder shape.
_VS_SPLIT = re.compile(r"\s+v(?:s\.?)?\s+", flags=re.IGNORECASE)
_TRAILING_MATCH_TAG = re.compile(r"\s*\(Match\s+\d+\)\s*$", flags=re.IGNORECASE)

# Tournament-context prefix patterns that appear before the home-team
# label in Ticketmaster WC names. Tried in order; first match wins.
# Anchored with ^ — these strip the leading tournament context cleanly
# regardless of whether the home label is a real country name
# ("Mexico") or a bracket placeholder ("Group E runners up", "1A",
# "W79"). Ordering matters: longer-prefix patterns first so
# "Match N Group X - " takes precedence over "Match N ".
_HOME_PREFIX_PATTERNS = [
    # "World Cup: Match N Group X - " (full group-stage form)
    re.compile(r"^World Cup:\s+Match\s+\d+\s+Group\s+[A-Z0-9]+\s*-\s*",
               flags=re.IGNORECASE),
    # "World Cup: Match N Group X- " (broken dash, e.g. "Group B- Qatar")
    re.compile(r"^World Cup:\s+Match\s+\d+\s+Group\s+[A-Z0-9]+-\s*",
               flags=re.IGNORECASE),
    # "World Cup: Match N Group X " (placeholder bracket form — home
    # label starts inline after the group designator)
    re.compile(r"^World Cup:\s+Match\s+\d+\s+Group\s+[A-Z0-9]+\s+",
               flags=re.IGNORECASE),
    # "World Cup: Match N " (no group designator)
    re.compile(r"^World Cup:\s+Match\s+\d+\s+", flags=re.IGNORECASE),
    # "World Cup Round of N: " (knockout)
    re.compile(r"^World Cup Round of\s+\d+:\s+", flags=re.IGNORECASE),
]


def _strip_home_prefix(home_side: str) -> str:
    """Strip a tournament-context prefix from the home-side substring.
    Falls back to the original string when no prefix matches — caller
    should still apply ` - ` / `: ` fallback parsing for that case."""
    for pat in _HOME_PREFIX_PATTERNS:
        m = pat.match(home_side)
        if m:
            return home_side[m.end():].strip()
    return home_side


def parse_teams(cleaned_name: str | None) -> tuple[str | None, str | None]:
    """Return (home_team, away_team) extracted from a cleaned name,
    or (None, None) if the shape isn't recognised."""
    if not cleaned_name:
        return (None, None)
    # Strip the trailing " - 2026 [Soccer] World Cup" suffix first so it
    # doesn't end up glued to the away-team value.
    body = _WC_TRAILING_SUFFIX.sub("", cleaned_name).strip()
    # Drop any trailing "(Match N)" so it doesn't bleed into away_team.
    body = _TRAILING_MATCH_TAG.sub("", body).strip()

    parts = _VS_SPLIT.split(body, maxsplit=1)
    if len(parts) != 2:
        return (None, None)
    home_side, away_side = parts

    # Strip the tournament-context prefix from home_side. Try the
    # explicit patterns first; fall back to the " - " / ": " rsplit
    # heuristic for any shape the patterns don't cover.
    stripped = _strip_home_prefix(home_side)
    if stripped == home_side:
        # No prefix pattern matched — fall back to the legacy
        # heuristic (rsplit on " - " or ": ").
        if " - " in home_side:
            home = home_side.rsplit(" - ", 1)[-1].strip()
        elif ": " in home_side:
            home = home_side.rsplit(": ", 1)[-1].strip()
        else:
            home = home_side.strip()
    else:
        home = stripped

    away = away_side.strip()
    # Drop trailing ":" or "-" leftover characters defensively.
    home = home.rstrip(" -:")
    away = away.rstrip(" -:")
    if not home or not away:
        return (None, None)
    # Reject only when home still carries an obvious match-number
    # prefix ("Match N", "Round of N") — those indicate the parser
    # failed to strip the tournament context. Bracket placeholders
    # like "Group E runners up" are valid team-slot labels for
    # not-yet-determined matches, so they pass through.
    bad_carriers = ("Match ", "Round of ")
    if any(s in home for s in bad_carriers) or any(s in away for s in bad_carriers):
        return (None, None)
    return (home, away)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--apply", action="store_true",
                   help="Write changes. Default: dry-run.")
    args = p.parse_args()

    db = SessionLocal()
    try:
        rows = (
            db.query(Event.id, Event.name, Event.home_team, Event.away_team,
                     Event.artist_name, Event.tournament, Event.sport)
            .filter(Event.tournament.isnot(None))
            .all()
        )
        log.info("Tournament rows: %d", len(rows))

        # Resolve the EventType ids we want to ensure tournaments have.
        # Preferred: "Sports Event" (generic). Fallback: "Soccer Match"
        # when an event is sport=Soccer. Both must exist in the catalog
        # for the re-tag to land; we fall back to no-op if neither does
        # (the catalog is seeded at boot so this should be rare).
        sports_event_id = db.execute(text(
            "SELECT id FROM event_types WHERE name = 'Sports Event' LIMIT 1"
        )).scalar()
        soccer_match_id = db.execute(text(
            "SELECT id FROM event_types WHERE name = 'Soccer Match' LIMIT 1"
        )).scalar()
        log.info("Catalog: Sports Event id=%s, Soccer Match id=%s",
                 sports_event_id, soccer_match_id)

        # Existing (event_id, type_id, type_name, type_category) m2m
        # rows for our tournament events. Pull in one shot to avoid
        # per-row queries.
        tournament_ids = [r[0] for r in rows]
        if tournament_ids:
            CHUNK = 500
            existing_m2m: dict[int, list[tuple[int, str, str]]] = {}
            for i in range(0, len(tournament_ids), CHUNK):
                chunk = tournament_ids[i:i + CHUNK]
                m2m_rows = db.execute(text(
                    "SELECT eet.event_id, et.id, et.name, et.category "
                    "FROM event_event_types eet "
                    "JOIN event_types et ON et.id = eet.event_type_id "
                    "WHERE eet.event_id IN ("
                    + ",".join(f":k{j}" for j in range(len(chunk)))
                    + ")"
                ), {f"k{j}": k for j, k in enumerate(chunk)}).fetchall()
                for ev_id, type_id, type_name, type_category in m2m_rows:
                    existing_m2m.setdefault(ev_id, []).append((type_id, type_name, type_category))
        else:
            existing_m2m = {}

        name_updates: list[tuple[int, str]] = []
        team_updates: list[tuple[int, str, str]] = []
        artist_clears: list[int] = []
        m2m_remove: list[tuple[int, int]] = []      # (event_id, type_id) to delete
        m2m_add: list[tuple[int, int]] = []         # (event_id, type_id) to insert
        unparsed: list[str] = []

        for rid, name, home, away, artist, tour, sport in rows:
            cleaned = clean_name(name)
            if cleaned != name and cleaned is not None:
                name_updates.append((rid, cleaned))

            new_home, new_away = parse_teams(cleaned)
            if new_home and new_away and (new_home != home or new_away != away):
                team_updates.append((rid, new_home, new_away))
            elif not new_home:
                unparsed.append(name or "")

            # Clear artist_name for tournament rows where it leaks the
            # tournament name itself ("2026 Soccer World Cup", etc.).
            if artist:
                artist_clears.append(rid)

            # event_type m2m fixup. Goal: tournament events are tagged
            # under category="Sports" and nothing else. Drop any
            # non-Sports type associations; ensure at least one Sports
            # type is present (Soccer Match if soccer + available,
            # otherwise Sports Event).
            current = existing_m2m.get(rid, [])
            has_sports_type = any(
                (cat or "").lower() == "sports" for (_id, _n, cat) in current
            )
            for type_id, type_name, type_category in current:
                if (type_category or "").lower() != "sports":
                    m2m_remove.append((rid, type_id))
            if not has_sports_type:
                # Prefer Soccer Match for sport=Soccer, otherwise the
                # generic Sports Event tag. Skip when neither is in
                # the catalog (no harm; the row still has tournament
                # set and the frontend can still render it as Sports).
                preferred_id = None
                if (sport or "").lower() == "soccer" and soccer_match_id:
                    preferred_id = soccer_match_id
                elif sports_event_id:
                    preferred_id = sports_event_id
                elif soccer_match_id:
                    preferred_id = soccer_match_id
                if preferred_id:
                    m2m_add.append((rid, preferred_id))

        log.info("Name updates (??? cleanup): %d", len(name_updates))
        log.info("Team re-parses: %d", len(team_updates))
        log.info("artist_name clears: %d", len(artist_clears))
        log.info("event_type associations to REMOVE (non-Sports): %d", len(m2m_remove))
        log.info("event_type associations to ADD (Sports tag): %d", len(m2m_add))
        log.info("Unparseable names (left as-is): %d", len(unparsed))
        for n in unparsed[:5]:
            log.info("  e.g. %s", n[:80])

        # Show a sample of the team re-parses so the dry-run preview is
        # informative — the user gets to eyeball whether the parser is
        # producing sensible values before committing.
        log.info("Sample team parses:")
        for rid, h, a in team_updates[:6]:
            log.info("  id=%d  home=%-25s  away=%s", rid, h, a)

        if args.apply:
            log.info("APPLYING …")
            for rid, new_name in name_updates:
                db.execute(text("UPDATE events SET name = :n WHERE id = :id"),
                           {"id": rid, "n": new_name})
            for rid, h, a in team_updates:
                db.execute(text(
                    "UPDATE events SET home_team = :h, away_team = :a WHERE id = :id"
                ), {"id": rid, "h": h, "a": a})
            if artist_clears:
                # Single statement, IN-clause chunked at 500.
                CHUNK = 500
                for i in range(0, len(artist_clears), CHUNK):
                    chunk = artist_clears[i:i + CHUNK]
                    db.execute(text(
                        "UPDATE events SET artist_name = NULL "
                        "WHERE id IN ("
                        + ",".join(f":k{j}" for j in range(len(chunk)))
                        + ")"
                    ), {f"k{j}": k for j, k in enumerate(chunk)})
            for ev_id, type_id in m2m_remove:
                db.execute(text(
                    "DELETE FROM event_event_types "
                    "WHERE event_id = :ev AND event_type_id = :et"
                ), {"ev": ev_id, "et": type_id})
            for ev_id, type_id in m2m_add:
                # SQLite-compatible idempotent insert: INSERT OR IGNORE
                # guards against re-runs where the association already
                # exists (the m2m table has a unique constraint on
                # (event_id, event_type_id)).
                db.execute(text(
                    "INSERT OR IGNORE INTO event_event_types "
                    "(event_id, event_type_id) VALUES (:ev, :et)"
                ), {"ev": ev_id, "et": type_id})
            db.commit()
            log.info("Done.")
        else:
            log.info("Dry-run. Re-run with --apply.")

        return 0
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
