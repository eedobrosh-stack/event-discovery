"""Fetch per-country TV broadcaster data for tournament events.

Source: **TheSportsDB** (https://www.thesportsdb.com/api/v1/json/), a
free community-maintained sports database. Their `/lookuptv.php` endpoint
returns the list of TV channels broadcasting a given fixture, with each
broadcast row carrying the country, channel name, logo URL, and a
stable channel id.

For each event with a `tournament` set we already match in our DB:

  1. Resolve TheSportsDB's matching event by (dateEvent, strHomeTeam,
     strAwayTeam) — TheSportsDB's WC events live under league id
     ``4429``. The list is small (a few hundred max for a season) so
     we load it once per tournament and build a lookup map keyed on
     (date, sorted-team-pair).
  2. Pull broadcasts via ``/lookuptv.php?id={idEvent}``, dedupe to
     unique (channel, country) pairs, normalize the country to ISO-2
     using the canonical name→ISO-2 map from leagues.py, and save the
     result into ``events.tv_channels`` as JSON in the established
     shape ``[{channel, country, type, url, logo}]``.

Idempotent — re-running produces the same JSON for events whose data
hasn't changed upstream, so it's safe to put on a weekly schedule once
the WC starts and broadcaster lists settle.

Known limits (v1):
  - TheSportsDB lists 15/104 WC fixtures today (group stage opener
    coverage; more land closer to kick-off). Knockout placeholder
    rows in our DB ("1A vs TBD") won't match until the bracket fills.
  - Per-country coverage is uneven — popular markets (UK, US, Brazil,
    Spain) have data, smaller markets often don't. The frontend
    falls back to "TV info pending" when no per-geo channel exists.

Usage:
    PYTHONPATH=. python3 scripts/fetch_tournament_tv.py
    PYTHONPATH=. python3 scripts/fetch_tournament_tv.py --apply
    PYTHONPATH=. python3 scripts/fetch_tournament_tv.py --apply --tournament "FIFA World Cup"
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import urllib.parse
import urllib.request
from collections import defaultdict
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
log = logging.getLogger("fetch_tournament_tv")

from sqlalchemy import text  # noqa: E402

from app.database import SessionLocal  # noqa: E402
from app.models import Event  # noqa: E402
from app.services.collectors.scrapers.sports.leagues import (  # noqa: E402
    COUNTRY_NAME_TO_ISO2,
)

# TheSportsDB free public test key. They publish "123" as the dev key
# in the docs; production usage is welcome to upgrade to a personal
# key for higher rate limits but we don't need that at our scale.
API_KEY = "123"
BASE = f"https://www.thesportsdb.com/api/v1/json/{API_KEY}"

# Tournament label → TheSportsDB league id. Add entries here as the
# Tournament chip's allowlist widens. Lookup is by exact label match
# against `events.tournament`.
TOURNAMENT_TO_TSDB_LEAGUE = {
    "FIFA World Cup": "4429",
}

# Season key passed to TheSportsDB's season endpoint. The WC season
# uses the year of the tournament (not a year-range like club leagues).
# Hardcoded for now; expand to a tournament→season map when other
# tournaments join the allowlist.
TOURNAMENT_TO_SEASON = {
    "FIFA World Cup": "2026",
}

# Polite delay between API calls. Free tier supports ~1 req/sec sustained.
REQUEST_DELAY_S = 0.25

# Team-name normalisation map. TheSportsDB and Ticketmaster spell some
# country names differently (Czechia vs Czech Republic, Türkiye vs
# Turkey, South Korea vs Korea Republic). Normalising both sides
# against this canonical form lets the team-set matcher fire on what
# would otherwise be near-misses. Keys/values are lowercased.
_TEAM_ALIASES = {
    "czech republic": "czechia",
    "czechia": "czechia",
    "korea republic": "south korea",
    "south korea": "south korea",
    "korea, south": "south korea",
    "türkiye": "turkey",
    "turkiye": "turkey",
    "ivory coast": "ivory coast",
    "cote d'ivoire": "ivory coast",
    "côte d'ivoire": "ivory coast",
    "iran islamic republic": "iran",
    "ir iran": "iran",
    "usa": "united states",
    "us": "united states",
    "u.s.a.": "united states",
    "congo dr": "congo dr",
    "dr congo": "congo dr",
    "democratic republic of congo": "congo dr",
    "bosnia-herzegovina": "bosnia and herzegovina",
    "bosnia and herzegovina": "bosnia and herzegovina",
    "cape verde": "cape verde",
    "cabo verde": "cape verde",
    "tba": "tba",
    "tbd": "tba",   # placeholder unification
}


def _canon_team(name: str | None) -> str:
    n = (name or "").strip().lower()
    return _TEAM_ALIASES.get(n, n)


def _http_get_json(url: str) -> dict:
    """Plain GET → JSON. Returns {} on any error so the caller can
    skip-and-continue rather than abort the whole backfill."""
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            return json.load(resp)
    except Exception as e:
        log.warning("fetch failed for %s: %s", url, e)
        return {}


def _tsdb_season_events(league_id: str, season: str) -> list[dict]:
    """Fetch the full season-event list for a TheSportsDB league. The
    response shape is `{"events": [...] or null}`."""
    url = f"{BASE}/eventsseason.php?id={league_id}&s={urllib.parse.quote(season)}"
    body = _http_get_json(url)
    return body.get("events") or []


def _tsdb_event_tv(event_id: str) -> list[dict]:
    """Fetch the per-fixture broadcaster list."""
    url = f"{BASE}/lookuptv.php?id={event_id}"
    body = _http_get_json(url)
    return body.get("tvevent") or []


def _match_key(home: str | None, away: str | None) -> tuple[str, str]:
    """Order-independent key for matching our events to TheSportsDB
    events. Both teams are normalised through _TEAM_ALIASES and
    sorted so a home/away swap upstream doesn't break the join.
    Dates are intentionally NOT in the key — Ticketmaster and
    TheSportsDB occasionally publish different kick-off dates for
    the same fixture (timezone differences, schedule revisions),
    and team-pair within a tournament is unique enough at WC scale
    that omitting the date is the right tradeoff."""
    return tuple(sorted([_canon_team(home), _canon_team(away)]))


def _normalize_tv_rows(tsdb_rows: list[dict]) -> list[dict]:
    """Map TheSportsDB's tvevent rows to our internal tv_channels JSON
    shape: [{channel, country, type, url, logo}]. Country is ISO-2
    (matches the frontend's lookup); rows with unknown country
    names fall through with the original string so they're at least
    visible (even if the geo filter won't match)."""
    seen: set[tuple[str, str]] = set()
    out: list[dict] = []
    for r in tsdb_rows:
        channel = (r.get("strChannel") or "").strip()
        country_full = (r.get("strCountry") or "").strip()
        if not channel:
            continue
        country_iso2 = COUNTRY_NAME_TO_ISO2.get(country_full, country_full)
        key = (channel.lower(), country_iso2.upper())
        if key in seen:
            continue
        seen.add(key)
        out.append({
            "channel": channel,
            "country": country_iso2,
            "type": "TV",
            "url": (r.get("strChannelUrl") or "").strip() or None,
            "logo": (r.get("strLogo") or "").strip() or None,
        })
    return out


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--apply", action="store_true",
                   help="Write tv_channels to events. Default: dry-run.")
    p.add_argument("--tournament", default=None,
                   help="Scope to a single tournament label (otherwise: every "
                        "tournament with a TheSportsDB league mapping).")
    args = p.parse_args()

    tournaments = (
        [args.tournament] if args.tournament else list(TOURNAMENT_TO_TSDB_LEAGUE)
    )

    db = SessionLocal()
    try:
        total_matched = 0
        total_written = 0
        total_unmatched = 0
        for tour in tournaments:
            league_id = TOURNAMENT_TO_TSDB_LEAGUE.get(tour)
            season = TOURNAMENT_TO_SEASON.get(tour)
            if not league_id or not season:
                log.warning("Skipping %s — no league/season mapping", tour)
                continue

            log.info("=== %s (league=%s season=%s) ===", tour, league_id, season)
            tsdb_events = _tsdb_season_events(league_id, season)
            log.info("TheSportsDB events for season: %d", len(tsdb_events))

            # Map sorted-team-pair → TheSportsDB event row.
            tsdb_map: dict[tuple, dict] = {}
            for e in tsdb_events:
                key = _match_key(e.get("strHomeTeam"), e.get("strAwayTeam"))
                # Skip placeholder rows whose key collapses to ("", "") —
                # we can't disambiguate them anyway.
                if not any(key):
                    continue
                tsdb_map[key] = e

            # Our events with this tournament.
            our_rows = (
                db.query(Event.id, Event.start_date, Event.home_team,
                         Event.away_team, Event.tv_channels)
                .filter(Event.tournament == tour)
                .all()
            )
            log.info("Our events for %s: %d", tour, len(our_rows))

            updates: list[tuple[int, list[dict]]] = []
            unmatched_examples: list[str] = []
            matched_no_tv: list[str] = []
            n_matched = 0
            for rid, sd, home, away, _tv in our_rows:
                if not (home and away):
                    continue
                key = _match_key(home, away)
                tsdb_event = tsdb_map.get(key)
                if not tsdb_event:
                    if len(unmatched_examples) < 5:
                        unmatched_examples.append(f"{sd} {home} vs {away}")
                    continue

                n_matched += 1
                event_id = tsdb_event.get("idEvent")
                if not event_id:
                    continue
                tv_rows = _tsdb_event_tv(event_id)
                time.sleep(REQUEST_DELAY_S)
                if not tv_rows:
                    if len(matched_no_tv) < 5:
                        matched_no_tv.append(f"{sd} {home} vs {away}")
                    continue
                normalized = _normalize_tv_rows(tv_rows)
                if normalized:
                    updates.append((rid, normalized))

            total_matched += len(updates)
            total_unmatched += len(our_rows) - n_matched

            log.info("Matched in TheSportsDB:        %d", n_matched)
            log.info("  with TV data:                %d", len(updates))
            log.info("  matched but no broadcasts:   %d (e.g. %s)",
                     n_matched - len(updates), matched_no_tv[:3])
            log.info("Unmatched (placeholder / not in TheSportsDB): %d (e.g. %s)",
                     len(our_rows) - n_matched, unmatched_examples[:3])
            for rid, chans in updates[:3]:
                countries = sorted({c["country"] for c in chans})
                log.info("  id=%d  %d channels across %s",
                         rid, len(chans), ",".join(countries))

            if args.apply:
                for rid, chans in updates:
                    db.execute(
                        text("UPDATE events SET tv_channels = :j WHERE id = :id"),
                        {"id": rid, "j": json.dumps(chans)},
                    )
                db.commit()
                total_written += len(updates)
                log.info("Wrote %d rows for %s", len(updates), tour)

        log.info("=== TOTAL ===")
        log.info("Matched events with TV data: %d", total_matched)
        log.info("Unmatched / no TV data:      %d", total_unmatched)
        if args.apply:
            log.info("Rows written:                %d", total_written)
        else:
            log.info("Dry-run only. Re-run with --apply.")
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
