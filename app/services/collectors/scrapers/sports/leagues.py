"""
League configuration: maps sport/league slugs to countries.

ESPN base:  https://site.api.espn.com/apis/site/v2/sports/{sport}/{league}/scoreboard
ESPN web:   https://site.web.api.espn.com/apis/site/v2/sports/{sport}/{league}/scoreboard
            (required for NRL which uses a numeric league ID)
"""
from __future__ import annotations
from dataclasses import dataclass, field


@dataclass
class LeagueConfig:
    sport: str          # ESPN sport segment, e.g. "football", "soccer"
    league: str         # ESPN league segment, e.g. "nfl", "eng.1"
    country: str        # ISO-2 country code for city matching
    label: str          # Human-readable name, e.g. "NFL"
    category: str       # raw_category, e.g. "American Football"
    use_web_domain: bool = False   # True for leagues that need site.web.api.espn.com


# All confirmed working against live ESPN API (April 2026).
ESPN_LEAGUES: list[LeagueConfig] = [
    # ── United States ──────────────────────────────────────────────────────
    LeagueConfig("football",            "nfl",   "US", "NFL",              "American Football"),
    LeagueConfig("basketball",          "nba",   "US", "NBA",              "Basketball"),
    LeagueConfig("hockey",              "nhl",   "US", "NHL",              "Ice Hockey"),
    LeagueConfig("soccer",              "usa.1", "US", "MLS",              "Soccer"),
    # ── United Kingdom ─────────────────────────────────────────────────────
    LeagueConfig("soccer",              "eng.1", "GB", "Premier League",   "Soccer"),
    LeagueConfig("soccer",              "eng.2", "GB", "Championship",     "Soccer"),
    # ── Europe ─────────────────────────────────────────────────────────────
    LeagueConfig("soccer",              "ger.1", "DE", "Bundesliga",       "Soccer"),
    LeagueConfig("soccer",              "esp.1", "ES", "La Liga",          "Soccer"),
    LeagueConfig("soccer",              "fra.1", "FR", "Ligue 1",          "Soccer"),
    LeagueConfig("soccer",              "ita.1", "IT", "Serie A",          "Soccer"),
    # ── Australia ──────────────────────────────────────────────────────────
    LeagueConfig("australian-football", "afl",   "AU", "AFL",              "Australian Football"),
    LeagueConfig("soccer",              "aus.1", "AU", "A-League",         "Soccer"),
    LeagueConfig("rugby-league",        "3",     "AU", "NRL",              "Rugby League",
                 use_web_domain=True),
    # ── Canada ─────────────────────────────────────────────────────────────
    LeagueConfig("football",            "cfl",   "CA", "CFL",              "Canadian Football"),
    # ── Israel ─────────────────────────────────────────────────────────────
    LeagueConfig("soccer",              "isr.1", "IL", "Israeli Premier League", "Soccer"),
    # ── Brazil ─────────────────────────────────────────────────────────────
    LeagueConfig("soccer",              "bra.1", "BR", "Brasileirao",      "Soccer"),
]

# Country codes that appear in multiple leagues → used for city lookup filtering
LEAGUE_COUNTRIES: set[str] = {lg.country for lg in ESPN_LEAGUES}
