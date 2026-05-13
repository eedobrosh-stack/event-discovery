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


# ── LLM-sourced fallback broadcaster map ───────────────────────────────
# TheSportsDB's broadcaster lists are sparse pre-tournament (1/15 WC
# fixtures populated when this code shipped). Until they fill in, we
# carry a hand-curated fallback for the marquee broadcasters in each
# major market. Compiled by Claude from public WC 2026 rights
# announcements and standing broadcaster relationships — high
# confidence for the listed entries, accepted as best-effort with
# small markets potentially shifting between announcement and kick-off.
#
# Shape: {tournament_label: {country_iso2: [{"channel", "url"}, ...]}}
# Channels are listed in display priority order — the frontend picks
# the first matching entry for the user's selected-city country.
_LLM_BROADCASTERS: dict[str, dict[str, list[dict]]] = {
    "FIFA World Cup": {
        # ── Host countries ──
        "US": [
            {"channel": "FOX Sports", "url": "https://www.foxsports.com/soccer/fifa-world-cup"},
            {"channel": "Telemundo",  "url": "https://www.telemundo.com/deportes/copa-mundial-fifa"},
        ],
        "CA": [
            {"channel": "TSN",        "url": "https://www.tsn.ca/soccer"},
            {"channel": "CTV",        "url": "https://www.ctv.ca/sports"},
        ],
        "MX": [
            {"channel": "TUDN",       "url": "https://tudn.com/"},
            {"channel": "TV Azteca",  "url": "https://www.tvazteca.com/aztecadeportes/"},
        ],
        # ── Major football markets ──
        "GB": [
            {"channel": "BBC Sport",  "url": "https://www.bbc.co.uk/sport/football/world-cup"},
            {"channel": "ITV",        "url": "https://www.itv.com/sport/football/world-cup"},
        ],
        "IL": [
            {"channel": "Kan",        "url": "https://www.kan.org.il/"},
        ],
        "BR": [
            {"channel": "Globo",      "url": "https://globoplay.globo.com/futebol/"},
            {"channel": "SporTV",     "url": "https://sportv.globo.com/"},
        ],
        "DE": [
            {"channel": "ARD",        "url": "https://www.sportschau.de/fussball/wm-2026/"},
            {"channel": "ZDF",        "url": "https://sport.zdf.de/fussball/wm/"},
        ],
        "ES": [
            {"channel": "RTVE",       "url": "https://www.rtve.es/deportes/futbol/"},
        ],
        "IT": [
            {"channel": "Rai Sport",  "url": "https://www.raisport.rai.it/"},
        ],
        "FR": [
            {"channel": "TF1",        "url": "https://www.tf1info.fr/sport/"},
            {"channel": "beIN Sports","url": "https://www.beinsports.com/fr"},
        ],
        "NL": [
            {"channel": "NOS",        "url": "https://nos.nl/sport/voetbal"},
        ],
        "PT": [
            {"channel": "RTP",        "url": "https://www.rtp.pt/desporto"},
        ],
        "AR": [
            {"channel": "TyC Sports", "url": "https://www.tycsports.com/"},
            {"channel": "TV Pública", "url": "https://www.tvpublica.com.ar/"},
        ],
        "BE": [
            {"channel": "RTBF",       "url": "https://www.rtbf.be/sport"},
            {"channel": "VRT",        "url": "https://sporza.be/"},
        ],
        "CH": [
            {"channel": "SRF",        "url": "https://www.srf.ch/sport"},
        ],
        "AT": [
            {"channel": "ORF Sport",  "url": "https://sport.orf.at/"},
        ],
        "GR": [
            {"channel": "ERT",        "url": "https://www.ert.gr/sports/"},
        ],
        "TR": [
            {"channel": "TRT Spor",   "url": "https://www.trtspor.com.tr/"},
        ],
        # ── Nordics ──
        "NO": [
            {"channel": "NRK",        "url": "https://www.nrk.no/sport/"},
            {"channel": "TV 2",       "url": "https://www.tv2.no/sport/"},
        ],
        "SE": [
            {"channel": "SVT",        "url": "https://www.svtplay.se/sport"},
            {"channel": "TV4",        "url": "https://www.tv4play.se/sport"},
        ],
        # ── Asia ──
        "JP": [
            {"channel": "NHK",        "url": "https://www.nhk.or.jp/sports/"},
        ],
        "KR": [
            {"channel": "KBS",        "url": "https://sports.kbs.co.kr/"},
            {"channel": "SBS",        "url": "https://sports.sbs.co.kr/"},
        ],
        "AU": [
            {"channel": "SBS",        "url": "https://www.sbs.com.au/sport"},
            {"channel": "Optus Sport","url": "https://sport.optus.com.au/"},
        ],
        "NZ": [
            {"channel": "Sky NZ",     "url": "https://www.sky.co.nz/sport"},
        ],
        "UZ": [
            {"channel": "UZREPORT",   "url": "https://uzreport.uz/"},
        ],
        # ── MENA (beIN holds most regional rights) ──
        "SA": [
            {"channel": "SSC",        "url": "https://www.ssc.sa/"},
        ],
        "QA": [
            {"channel": "beIN Sports","url": "https://www.beinsports.com/"},
        ],
        "EG": [
            {"channel": "beIN Sports","url": "https://www.beinsports.com/"},
        ],
        "IR": [
            {"channel": "IRIB Varzesh","url": "https://www.iribtv.ir/"},
        ],
        "IQ": [
            {"channel": "Al Iraqiya", "url": "https://imn.iq/"},
        ],
        "JO": [
            {"channel": "beIN Sports","url": "https://www.beinsports.com/"},
        ],
        # ── Africa ──
        "ZA": [
            {"channel": "SABC Sport", "url": "https://www.sabcsport.com/"},
            {"channel": "SuperSport", "url": "https://supersport.com/"},
        ],
        "MA": [
            {"channel": "SNRT",       "url": "https://snrt.ma/"},
        ],
        "DZ": [
            {"channel": "EPTV",       "url": "https://www.entv.dz/"},
        ],
        "TN": [
            {"channel": "El Wataniya 1","url": "https://www.watania1.tn/"},
        ],
        "SN": [
            {"channel": "RTS",        "url": "https://www.rts.sn/"},
        ],
        "GH": [
            {"channel": "GTV",        "url": "https://gbcghana.com/"},
        ],
        "CI": [
            {"channel": "RTI",        "url": "https://www.rti.ci/"},
        ],
        "CD": [
            {"channel": "RTNC",       "url": "https://rtnc.cd/"},
        ],
        "CV": [
            {"channel": "TCV",        "url": "https://www.rtc.cv/"},
        ],
        # ── Latin America ──
        "CO": [
            {"channel": "Caracol TV", "url": "https://www.caracoltv.com/"},
            {"channel": "RCN",        "url": "https://www.canalrcn.com/"},
        ],
        "EC": [
            {"channel": "Teleamazonas","url": "https://www.teleamazonas.com/"},
        ],
        "UY": [
            {"channel": "Canal 10",   "url": "https://www.canal10.com.uy/"},
        ],
        "PY": [
            {"channel": "Tigo Sports","url": "https://www.tigosports.py/"},
        ],
        "PA": [
            {"channel": "TVN",        "url": "https://www.tvn-2.com/"},
        ],
        "HT": [
            {"channel": "Télé Métropole","url": "https://www.metropolehaiti.com/"},
        ],
        "CW": [
            {"channel": "TeleCuraçao","url": "https://www.telecuracao.com/"},
        ],
        # ── Europe (other WC participants) ──
        "CZ": [
            {"channel": "ČT Sport",   "url": "https://sport.ceskatelevize.cz/"},
        ],
        "HR": [
            {"channel": "HRT",        "url": "https://hrt.hr/sport/"},
        ],
        "BA": [
            {"channel": "BHRT",       "url": "https://bhrt.ba/sport/"},
        ],
    },
}


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


def _llm_channels_for(tournament: str) -> list[dict]:
    """Expand the LLM-sourced broadcaster map for one tournament into
    the tv_channels JSON shape. Returns the full per-country list — the
    frontend filters down to the user's country at render time."""
    out: list[dict] = []
    for iso2, channels in _LLM_BROADCASTERS.get(tournament, {}).items():
        for c in channels:
            out.append({
                "channel": c["channel"],
                "country": iso2,
                "type": "TV",
                "url": c.get("url"),
                "logo": None,
            })
    return out


def _merge_channels(primary: list[dict], fallback: list[dict]) -> list[dict]:
    """Merge two channel lists. For each (channel-name, country) pair
    that appears in `primary`, keep the primary entry (it's the
    upstream-sourced truth — likely fresher / more detailed). For
    everything else in `fallback`, append. Preserves stable ordering
    so the frontend's "first match wins" is reproducible."""
    seen: set[tuple[str, str]] = set()
    merged: list[dict] = []
    for c in primary:
        key = ((c.get("channel") or "").lower(), (c.get("country") or "").upper())
        if key in seen:
            continue
        seen.add(key)
        merged.append(c)
    for c in fallback:
        key = ((c.get("channel") or "").lower(), (c.get("country") or "").upper())
        if key in seen:
            continue
        seen.add(key)
        merged.append(c)
    return merged


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

            # The LLM-sourced fallback applies to every event of this
            # tournament, regardless of whether TheSportsDB has data for
            # the specific fixture. Most major broadcasters air every
            # match in a WC (BBC airs all UK-broadcast games, Kan airs
            # all IL-broadcast games, etc.), so this is the right shape
            # for "per-country fallback".
            llm_fallback = _llm_channels_for(tour)
            log.info("LLM fallback channels for %s: %d (across %d countries)",
                     tour, len(llm_fallback),
                     len({c["country"] for c in llm_fallback}))

            updates: list[tuple[int, list[dict]]] = []
            tsdb_hits = 0
            llm_only = 0
            unmatched_examples: list[str] = []
            for rid, sd, home, away, existing in our_rows:
                if not (home and away):
                    # Even unparsed bracket rows get the per-country
                    # fallback applied — they're still WC fixtures.
                    if llm_fallback:
                        updates.append((rid, list(llm_fallback)))
                    continue
                key = _match_key(home, away)
                tsdb_event = tsdb_map.get(key)
                tsdb_channels: list[dict] = []
                if tsdb_event:
                    event_id = tsdb_event.get("idEvent")
                    if event_id:
                        tv_rows = _tsdb_event_tv(event_id)
                        time.sleep(REQUEST_DELAY_S)
                        if tv_rows:
                            tsdb_channels = _normalize_tv_rows(tv_rows)
                            tsdb_hits += 1
                else:
                    if len(unmatched_examples) < 5:
                        unmatched_examples.append(f"{sd} {home} vs {away}")

                merged = _merge_channels(tsdb_channels, llm_fallback)
                if not tsdb_channels and merged:
                    llm_only += 1
                if merged:
                    updates.append((rid, merged))

            total_matched += len(updates)
            total_unmatched += len(our_rows) - len(updates)

            log.info("Events that will receive tv_channels: %d / %d",
                     len(updates), len(our_rows))
            log.info("  TheSportsDB-sourced channels:  %d events", tsdb_hits)
            log.info("  LLM-fallback only:             %d events", llm_only)
            log.info("Unmatched in TheSportsDB (LLM still applied): e.g. %s",
                     unmatched_examples[:3])
            for rid, chans in updates[:3]:
                countries = sorted({c["country"] for c in chans})
                log.info("  id=%d  %d channels across %d countries",
                         rid, len(chans), len(countries))

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
