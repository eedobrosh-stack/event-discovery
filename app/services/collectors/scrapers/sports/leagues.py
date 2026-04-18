"""
League configuration: maps sport/league slugs to countries.

ESPN base:  https://site.api.espn.com/apis/site/v2/sports/{sport}/{league}/scoreboard
ESPN web:   https://site.web.api.espn.com/apis/site/v2/sports/{sport}/{league}/scoreboard
            (required for NRL which uses a numeric league ID)

IMPORTANT: LeagueConfig.country stores ISO-2 codes ("US", "GB", …).
The collect() methods receive city.country as the *full* country name
("United States", "United Kingdom", …). Use COUNTRY_NAME_TO_ISO2 to bridge them.

For multi-country competitions (EuroLeague, Champions League, etc.) set
`extra_countries` to the frozenset of all ISO-2 codes whose clubs participate.
The ESPN collector includes a league whenever the requested country appears in
either `country` OR `extra_countries`.
"""
from __future__ import annotations
from dataclasses import dataclass, field


@dataclass
class LeagueConfig:
    sport: str              # ESPN sport segment, e.g. "football", "soccer"
    league: str             # ESPN league segment, e.g. "nfl", "eng.1"
    country: str            # primary ISO-2 country code, e.g. "US", "GB"
    label: str              # Human-readable name, e.g. "NFL"
    category: str           # raw_category, e.g. "American Football"
    use_web_domain: bool = False           # True for leagues needing site.web.api.espn.com
    extra_countries: frozenset = field(    # additional ISO-2 codes (multi-country leagues)
        default_factory=frozenset
    )

    def matches_country(self, iso2: str) -> bool:
        """Return True if this league should be collected for the given ISO-2 country."""
        return self.country == iso2 or iso2 in self.extra_countries


# Maps City.country (full name from our DB) → ISO-2 code used in LeagueConfig.country
COUNTRY_NAME_TO_ISO2: dict[str, str] = {
    "United States":        "US",
    "United Kingdom":       "GB",
    "Germany":              "DE",
    "Spain":                "ES",
    "France":               "FR",
    "Italy":                "IT",
    "Australia":            "AU",
    "Canada":               "CA",
    "Israel":               "IL",
    "Brazil":               "BR",
    "Netherlands":          "NL",
    "Portugal":             "PT",
    "Turkey":               "TR",
    "Belgium":              "BE",
    "Greece":               "GR",
    "Lithuania":            "LT",
    "Serbia":               "RS",
    "Monaco":               "MC",
    "New Zealand":          "NZ",
    "Japan":                "JP",
    "Mexico":               "MX",
    "Argentina":            "AR",
    "South Africa":         "ZA",
    "Singapore":            "SG",
    "United Arab Emirates": "AE",
    "Saudi Arabia":         "SA",
    "Bahrain":              "BH",
    "China":                "CN",
    "Azerbaijan":           "AZ",
    "Hungary":              "HU",
    "Austria":              "AT",
}

# ── UEFA Champions League / Europa League ────────────────────────────────────
# Broadest multi-country competition — essentially every European footballing nation.
# We include our priority-city countries to avoid collecting for non-priority nations.
_UEFA_COUNTRIES = frozenset({
    "GB", "DE", "ES", "FR", "IT", "NL", "PT", "BE", "TR",
    "GR", "RS", "NL", "AT", "CH",
})

# All confirmed working against live ESPN API (April 2026).
ESPN_LEAGUES: list[LeagueConfig] = [
    # ── United States ──────────────────────────────────────────────────────
    LeagueConfig("football",            "nfl",           "US", "NFL",                       "American Football"),
    LeagueConfig("basketball",          "nba",           "US", "NBA",                       "Basketball"),
    LeagueConfig("hockey",              "nhl",           "US", "NHL",                       "Ice Hockey"),
    LeagueConfig("soccer",              "usa.1",         "US", "MLS",                       "Soccer"),
    # ── United Kingdom ─────────────────────────────────────────────────────
    LeagueConfig("soccer",              "eng.1",         "GB", "Premier League",             "Soccer"),
    LeagueConfig("soccer",              "eng.2",         "GB", "Championship",               "Soccer"),
    LeagueConfig("soccer",              "sco.1",         "GB", "Scottish Premiership",       "Soccer"),
    LeagueConfig("rugby-union",         "premiership",   "GB", "Premiership Rugby",          "Rugby Union"),
    LeagueConfig("rugby-league",        "super.league",  "GB", "Super League",               "Rugby League"),
    # ── Germany ────────────────────────────────────────────────────────────
    LeagueConfig("soccer",              "ger.1",         "DE", "Bundesliga",                 "Soccer"),
    LeagueConfig("soccer",              "ger.2",         "DE", "2. Bundesliga",              "Soccer"),
    # ── Spain ──────────────────────────────────────────────────────────────
    LeagueConfig("soccer",              "esp.1",         "ES", "La Liga",                    "Soccer"),
    # ── France ─────────────────────────────────────────────────────────────
    LeagueConfig("soccer",              "fra.1",         "FR", "Ligue 1",                    "Soccer"),
    LeagueConfig("rugby-union",         "top.14",        "FR", "Top 14",                     "Rugby Union"),
    # ── Italy ──────────────────────────────────────────────────────────────
    LeagueConfig("soccer",              "ita.1",         "IT", "Serie A",                    "Soccer"),
    # ── Netherlands ────────────────────────────────────────────────────────
    LeagueConfig("soccer",              "ned.1",         "NL", "Eredivisie",                 "Soccer"),
    # ── Portugal ───────────────────────────────────────────────────────────
    LeagueConfig("soccer",              "por.1",         "PT", "Primeira Liga",              "Soccer"),
    # ── Turkey ─────────────────────────────────────────────────────────────
    LeagueConfig("soccer",              "tur.1",         "TR", "Türkiye Süper Ligi",         "Soccer"),
    # ── Belgium ────────────────────────────────────────────────────────────
    LeagueConfig("soccer",              "bel.1",         "BE", "Belgian First Division A",   "Soccer"),
    # ── Australia ──────────────────────────────────────────────────────────
    LeagueConfig("australian-football", "afl",           "AU", "AFL",                        "Australian Football"),
    LeagueConfig("soccer",              "aus.1",         "AU", "A-League",                   "Soccer"),
    LeagueConfig("rugby-league",        "3",             "AU", "NRL",                        "Rugby League",
                 use_web_domain=True),
    LeagueConfig("rugby-union",         "aus.super.rugby", "AU", "Super Rugby Pacific",     "Rugby Union"),
    LeagueConfig("basketball",          "nbl",           "AU", "NBL",                        "Basketball"),
    LeagueConfig("cricket",             "bbl",           "AU", "Big Bash League",            "Cricket"),
    # ── Canada ─────────────────────────────────────────────────────────────
    LeagueConfig("football",            "cfl",           "CA", "CFL",                        "Canadian Football"),
    # ── Israel ─────────────────────────────────────────────────────────────
    LeagueConfig("soccer",              "isr.1",         "IL", "Israeli Premier League",     "Soccer"),
    LeagueConfig("basketball",          "isr.1",         "IL", "Israeli Basketball Premier League", "Basketball"),
    # ── Brazil ─────────────────────────────────────────────────────────────
    LeagueConfig("soccer",              "bra.1",         "BR", "Brasileirao",                "Soccer"),
    # ── Argentina ──────────────────────────────────────────────────────────
    LeagueConfig("soccer",              "arg.1",         "AR", "Liga Argentina",             "Soccer"),
    # ── Mexico ─────────────────────────────────────────────────────────────
    LeagueConfig("soccer",              "mex.1",         "MX", "Liga MX",                    "Soccer"),

    # NOTE: EuroLeague and EuroCup are NOT on ESPN's API.
    # They are collected by EuroLeagueCollector (euroleague.py) using the
    # official Euroleague Basketball API (api-live.euroleague.net/v2).

    # ── Multi-country: UEFA Club Competitions ─────────────────────────────
    # Collected for any European country with clubs in the competition.
    # Individual match venues determine the physical city — the dedup index
    # (scrape_source + source_id) prevents saving the same fixture twice.
    LeagueConfig("soccer",              "uefa.champions", "ES", "UEFA Champions League",    "Soccer",
                 extra_countries=_UEFA_COUNTRIES),
    LeagueConfig("soccer",              "uefa.europa",    "ES", "UEFA Europa League",       "Soccer",
                 extra_countries=_UEFA_COUNTRIES),
]

# Country codes that appear in at least one league (union of primary + extras)
LEAGUE_COUNTRIES: set[str] = set()
for _lg in ESPN_LEAGUES:
    LEAGUE_COUNTRIES.add(_lg.country)
    LEAGUE_COUNTRIES.update(_lg.extra_countries)
