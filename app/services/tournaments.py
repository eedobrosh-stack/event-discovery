"""Tournament catalog — single source of truth for everything tournament-related.

Every named competition that should surface as a Tournament chip in
autocomplete has one entry here. The dict packs:

  * display_label   — the friendly name used in calendar titles and
                      anywhere we render the tournament to a user
                      ("FIFA World Cup 2026" vs. the bare key
                      "FIFA World Cup" stored in events.tournament).
  * tsdb_league     — TheSportsDB league id for fetch_tournament_tv.py.
                      Optional — set to None when TheSportsDB doesn't
                      carry the tournament cleanly (tennis Grand Slams
                      model the whole event as a single multi-day row in
                      our DB, but TheSportsDB serves per-match data).
  * tsdb_season     — TheSportsDB season key. Year for one-off
                      tournaments (WC, Wimbledon), year-range for
                      league seasons ("2025-2026").
  * broadcasters    — {country_iso2: [{"channel", "url"}, ...]}.
                      LLM-sourced fallback applied to every event of
                      the tournament. Most major broadcasters air every
                      match in their market, so a per-country fallback
                      is the right shape regardless of which match a
                      viewer is checking.

To add a tournament:

  1. Add an entry to TOURNAMENTS below. The key MUST match the value
     written into ``events.tournament`` by the source collector.
  2. Ensure collector tags the rows (see ESPN espn.py / tennis.py
     for examples — `tournament=cfg.label`). If existing data needs
     a backfill, ``scripts/backfill_event_tournament.py`` handles
     the common ESPN/tennis/Ticketmaster shapes.
  3. Run ``scripts/fetch_tournament_tv.py --apply --tournament "<key>"``
     on the Render shell to populate broadcaster data.

The frontend (app.js) doesn't need to know about new tournaments —
its tournament-row affordances kick in for any event whose
``tournament`` field is set.
"""
from __future__ import annotations

from typing import TypedDict, Optional


class _Broadcaster(TypedDict):
    channel: str
    url: str


class _TournamentConfig(TypedDict):
    display_label: str
    tsdb_league: Optional[str]
    tsdb_season: Optional[str]
    broadcasters: dict[str, list[_Broadcaster]]


# ── Shared regional channel groups ────────────────────────────────────
# Many tournaments share the same regional sub-licensee in a given
# market (beIN Sports across the MENA region, Eurosport across most
# of Europe for tennis Grand Slams other than the one played there).
# Defining the groups once keeps the per-tournament entries focused on
# what's actually unique.

_BEIN_MENA = [
    {"channel": "beIN Sports", "url": "https://www.beinsports.com/"},
]

_EUROSPORT_EU_TENNIS = [
    {"channel": "Eurosport", "url": "https://www.eurosport.com/tennis/"},
]


TOURNAMENTS: dict[str, _TournamentConfig] = {
    # ════════════════════════════════════════════════════════════════
    # FIFA World Cup 2026 — hosted by US/CA/MX, kick-off June 11 2026
    # ════════════════════════════════════════════════════════════════
    "FIFA World Cup": {
        "display_label": "FIFA World Cup 2026",
        "tsdb_league": "4429",
        "tsdb_season": "2026",
        "broadcasters": {
            # Hosts
            "US": [
                {"channel": "FOX Sports",  "url": "https://www.foxsports.com/soccer/fifa-world-cup"},
                {"channel": "Telemundo",   "url": "https://www.telemundo.com/deportes/copa-mundial-fifa"},
            ],
            "CA": [
                {"channel": "TSN",         "url": "https://www.tsn.ca/soccer"},
                {"channel": "CTV",         "url": "https://www.ctv.ca/sports"},
            ],
            "MX": [
                {"channel": "TUDN",        "url": "https://tudn.com/"},
                {"channel": "TV Azteca",   "url": "https://www.tvazteca.com/aztecadeportes/"},
            ],
            # Europe — major football markets
            "GB": [
                {"channel": "BBC Sport",   "url": "https://www.bbc.co.uk/sport/football/world-cup"},
                {"channel": "ITV",         "url": "https://www.itv.com/sport/football/world-cup"},
            ],
            "IL": [{"channel": "Kan",      "url": "https://www.kan.org.il/"}],
            "BR": [
                {"channel": "Globo",       "url": "https://globoplay.globo.com/futebol/"},
                {"channel": "SporTV",      "url": "https://sportv.globo.com/"},
            ],
            "DE": [
                {"channel": "ARD",         "url": "https://www.sportschau.de/fussball/wm-2026/"},
                {"channel": "ZDF",         "url": "https://sport.zdf.de/fussball/wm/"},
            ],
            "ES": [{"channel": "RTVE",     "url": "https://www.rtve.es/deportes/futbol/"}],
            "IT": [{"channel": "Rai Sport","url": "https://www.raisport.rai.it/"}],
            "FR": [
                {"channel": "TF1",         "url": "https://www.tf1info.fr/sport/"},
                {"channel": "beIN Sports", "url": "https://www.beinsports.com/fr"},
            ],
            "NL": [{"channel": "NOS",      "url": "https://nos.nl/sport/voetbal"}],
            "PT": [{"channel": "RTP",      "url": "https://www.rtp.pt/desporto"}],
            "AR": [
                {"channel": "TyC Sports",  "url": "https://www.tycsports.com/"},
                {"channel": "TV Pública",  "url": "https://www.tvpublica.com.ar/"},
            ],
            "BE": [
                {"channel": "RTBF",        "url": "https://www.rtbf.be/sport"},
                {"channel": "VRT",         "url": "https://sporza.be/"},
            ],
            "CH": [{"channel": "SRF",      "url": "https://www.srf.ch/sport"}],
            "AT": [{"channel": "ORF Sport","url": "https://sport.orf.at/"}],
            "GR": [{"channel": "ERT",      "url": "https://www.ert.gr/sports/"}],
            "TR": [{"channel": "TRT Spor", "url": "https://www.trtspor.com.tr/"}],
            "NO": [
                {"channel": "NRK",         "url": "https://www.nrk.no/sport/"},
                {"channel": "TV 2",        "url": "https://www.tv2.no/sport/"},
            ],
            "SE": [
                {"channel": "SVT",         "url": "https://www.svtplay.se/sport"},
                {"channel": "TV4",         "url": "https://www.tv4play.se/sport"},
            ],
            # Asia
            "JP": [{"channel": "NHK",      "url": "https://www.nhk.or.jp/sports/"}],
            "KR": [
                {"channel": "KBS",         "url": "https://sports.kbs.co.kr/"},
                {"channel": "SBS",         "url": "https://sports.sbs.co.kr/"},
            ],
            "AU": [
                {"channel": "SBS",         "url": "https://www.sbs.com.au/sport"},
                {"channel": "Optus Sport", "url": "https://sport.optus.com.au/"},
            ],
            "NZ": [{"channel": "Sky NZ",   "url": "https://www.sky.co.nz/sport"}],
            "UZ": [{"channel": "UZREPORT", "url": "https://uzreport.uz/"}],
            # MENA — beIN holds most regional rights
            "SA": [{"channel": "SSC",      "url": "https://www.ssc.sa/"}],
            "QA": _BEIN_MENA,
            "EG": _BEIN_MENA,
            "IR": [{"channel": "IRIB Varzesh","url": "https://www.iribtv.ir/"}],
            "IQ": [{"channel": "Al Iraqiya","url": "https://imn.iq/"}],
            "JO": _BEIN_MENA,
            # Africa
            "ZA": [
                {"channel": "SABC Sport",  "url": "https://www.sabcsport.com/"},
                {"channel": "SuperSport",  "url": "https://supersport.com/"},
            ],
            "MA": [{"channel": "SNRT",     "url": "https://snrt.ma/"}],
            "DZ": [{"channel": "EPTV",     "url": "https://www.entv.dz/"}],
            "TN": [{"channel": "El Wataniya 1","url": "https://www.watania1.tn/"}],
            "SN": [{"channel": "RTS",      "url": "https://www.rts.sn/"}],
            "GH": [{"channel": "GTV",      "url": "https://gbcghana.com/"}],
            "CI": [{"channel": "RTI",      "url": "https://www.rti.ci/"}],
            "CD": [{"channel": "RTNC",     "url": "https://rtnc.cd/"}],
            "CV": [{"channel": "TCV",      "url": "https://www.rtc.cv/"}],
            # Latin America
            "CO": [
                {"channel": "Caracol TV",  "url": "https://www.caracoltv.com/"},
                {"channel": "RCN",         "url": "https://www.canalrcn.com/"},
            ],
            "EC": [{"channel": "Teleamazonas","url": "https://www.teleamazonas.com/"}],
            "UY": [{"channel": "Canal 10", "url": "https://www.canal10.com.uy/"}],
            "PY": [{"channel": "Tigo Sports","url": "https://www.tigosports.py/"}],
            "PA": [{"channel": "TVN",      "url": "https://www.tvn-2.com/"}],
            "HT": [{"channel": "Télé Métropole","url": "https://www.metropolehaiti.com/"}],
            "CW": [{"channel": "TeleCuraçao","url": "https://www.telecuracao.com/"}],
            # Europe (other WC participants)
            "CZ": [{"channel": "ČT Sport", "url": "https://sport.ceskatelevize.cz/"}],
            "HR": [{"channel": "HRT",      "url": "https://hrt.hr/sport/"}],
            "BA": [{"channel": "BHRT",     "url": "https://bhrt.ba/sport/"}],
        },
    },

    # ════════════════════════════════════════════════════════════════
    # NBA 2025-26
    # ════════════════════════════════════════════════════════════════
    "NBA": {
        "display_label": "NBA 2025–26",
        "tsdb_league": "4387",
        "tsdb_season": "2025-2026",
        "broadcasters": {
            "US": [
                {"channel": "ESPN",          "url": "https://www.espn.com/nba/"},
                {"channel": "NBA on TNT",    "url": "https://www.nba.com/watch"},
                {"channel": "NBA League Pass","url": "https://www.nba.com/leaguepass"},
            ],
            "CA": [
                {"channel": "TSN",           "url": "https://www.tsn.ca/nba"},
                {"channel": "Sportsnet",     "url": "https://www.sportsnet.ca/basketball/"},
            ],
            "MX": [{"channel": "ESPN México","url": "https://www.espn.com.mx/nba/"}],
            "GB": [{"channel": "Sky Sports", "url": "https://www.skysports.com/nba"}],
            "IL": [{"channel": "Sport 5",    "url": "https://www.sport5.co.il/"}],
            "ES": [{"channel": "Movistar+",  "url": "https://ver.movistarplus.es/deportes"}],
            "DE": [
                {"channel": "DAZN",          "url": "https://www.dazn.com/de-DE/basketball"},
                {"channel": "MagentaSport",  "url": "https://www.magentasport.de/"},
            ],
            "FR": [{"channel": "beIN Sports","url": "https://www.beinsports.com/fr/basket-nba"}],
            "IT": [{"channel": "Sky Sport NBA","url": "https://sport.sky.it/nba"}],
            "BR": [
                {"channel": "ESPN Brasil",   "url": "https://www.espn.com.br/nba/"},
                {"channel": "NBA League Pass","url": "https://www.nba.com/leaguepass"},
            ],
            "AR": [{"channel": "ESPN",       "url": "https://www.espn.com.ar/nba/"}],
            "AU": [{"channel": "ESPN",       "url": "https://www.espn.com.au/nba/"}],
            "JP": [{"channel": "Rakuten",    "url": "https://nba.rakuten.co.jp/"}],
            "KR": [{"channel": "SPOTV",      "url": "https://www.spotv.net/"}],
            "CN": [{"channel": "Tencent Sports","url": "https://nba.qq.com/"}],
            "PH": [{"channel": "NBA TV PH",  "url": "https://www.nba.com/"}],
            "IN": [{"channel": "Sony Sports","url": "https://www.sonyliv.com/sports"}],
            "TR": [{"channel": "S Sport",    "url": "https://www.s-sport.tv/"}],
            "GR": [{"channel": "ERT",        "url": "https://www.ert.gr/sports/"}],
        },
    },

    # ════════════════════════════════════════════════════════════════
    # UEFA Champions League 2025-26
    # ════════════════════════════════════════════════════════════════
    "UEFA Champions League": {
        "display_label": "UEFA Champions League 2025–26",
        "tsdb_league": "4480",
        "tsdb_season": "2025-2026",
        "broadcasters": {
            "GB": [{"channel": "TNT Sports",  "url": "https://www.tntsports.co.uk/football/champions-league"}],
            "US": [
                {"channel": "Paramount+",     "url": "https://www.paramountplus.com/sports/"},
                {"channel": "CBS Sports",     "url": "https://www.cbssports.com/soccer/champions-league/"},
            ],
            "IL": [{"channel": "Sport 5",     "url": "https://www.sport5.co.il/"}],
            "ES": [{"channel": "Movistar+",   "url": "https://ver.movistarplus.es/deportes/"}],
            "DE": [
                {"channel": "DAZN",           "url": "https://www.dazn.com/de-DE/home"},
                {"channel": "Amazon Prime",   "url": "https://www.primevideo.com/"},
            ],
            "FR": [
                {"channel": "Canal+",         "url": "https://www.canalplus.com/sport/"},
                {"channel": "beIN Sports",    "url": "https://www.beinsports.com/fr"},
            ],
            "IT": [
                {"channel": "Sky Sport",      "url": "https://sport.sky.it/calcio/champions-league"},
                {"channel": "Mediaset",       "url": "https://www.mediasetinfinity.it/"},
            ],
            "NL": [{"channel": "Ziggo Sport", "url": "https://www.ziggo.nl/televisie/zenders/sport/"}],
            "PT": [{"channel": "Eleven Sports","url": "https://elevensports.pt/"}],
            "BR": [
                {"channel": "SBT",            "url": "https://www.sbt.com.br/esportes"},
                {"channel": "Space",          "url": "https://www.warnermediabrasil.com/"},
            ],
            "MX": [
                {"channel": "TUDN",           "url": "https://tudn.com/"},
                {"channel": "Caliente TV",    "url": "https://www.caliente.tv/"},
            ],
            "AR": [{"channel": "ESPN",        "url": "https://www.espn.com.ar/futbol/uefa-champions-league/"}],
            "CA": [{"channel": "DAZN",        "url": "https://www.dazn.com/en-CA/"}],
            "AU": [{"channel": "Stan Sport",  "url": "https://www.stan.com.au/sport"}],
            "JP": [{"channel": "WOWOW",       "url": "https://www.wowow.co.jp/sports/"}],
            "TR": [{"channel": "TV8.5",       "url": "https://www.tv8bucukspor.com/"}],
            "GR": [{"channel": "Cosmote TV",  "url": "https://www.cosmotetv.gr/"}],
            "BE": [{"channel": "Play Sports", "url": "https://www.playsports.be/"}],
            "CH": [{"channel": "blue Sport",  "url": "https://www.blue.ch/sport"}],
            "AT": [{"channel": "ServusTV",    "url": "https://www.servustv.com/sport/"}],
            "QA": _BEIN_MENA,
            "EG": _BEIN_MENA,
        },
    },

    # ════════════════════════════════════════════════════════════════
    # Premier League 2025-26
    # ════════════════════════════════════════════════════════════════
    "Premier League": {
        "display_label": "Premier League 2025–26",
        "tsdb_league": "4328",
        "tsdb_season": "2025-2026",
        "broadcasters": {
            "GB": [
                {"channel": "Sky Sports",   "url": "https://www.skysports.com/premier-league"},
                {"channel": "TNT Sports",   "url": "https://www.tntsports.co.uk/football/premier-league"},
                {"channel": "Amazon Prime", "url": "https://www.primevideo.com/sports"},
            ],
            "US": [
                {"channel": "NBC Sports",   "url": "https://www.nbcsports.com/soccer/premier-league"},
                {"channel": "Peacock",      "url": "https://www.peacocktv.com/sports/premier-league"},
            ],
            "IL": [{"channel": "Sport 5",   "url": "https://www.sport5.co.il/"}],
            "ES": [{"channel": "DAZN",      "url": "https://www.dazn.com/es-ES/home"}],
            "DE": [{"channel": "Sky",       "url": "https://www.sky.de/sport"}],
            "FR": [
                {"channel": "Free",         "url": "https://www.free.fr/freebox/free-ligue-1/"},
                {"channel": "beIN Sports",  "url": "https://www.beinsports.com/fr"},
            ],
            "IT": [
                {"channel": "DAZN",         "url": "https://www.dazn.com/it-IT/home"},
                {"channel": "Sky Sport",    "url": "https://sport.sky.it/calcio/premier-league"},
            ],
            "BR": [{"channel": "ESPN Brasil","url": "https://www.espn.com.br/futebol/liga-inglesa/"}],
            "MX": [{"channel": "Caliente TV","url": "https://www.caliente.tv/"}],
            "AU": [{"channel": "Optus Sport","url": "https://sport.optus.com.au/"}],
            "CA": [{"channel": "FuboTV",    "url": "https://www.fubo.tv/canada/"}],
            "JP": [{"channel": "SPOTV NOW", "url": "https://www.spotvnow.jp/"}],
            "KR": [{"channel": "SPOTV",     "url": "https://www.spotv.net/"}],
            "IN": [{"channel": "Star Sports","url": "https://www.hotstar.com/in/sports"}],
            "TR": [{"channel": "S Sport",   "url": "https://www.s-sport.tv/"}],
            "NL": [{"channel": "Viaplay",   "url": "https://viaplay.nl/"}],
            "PT": [{"channel": "Eleven Sports","url": "https://elevensports.pt/"}],
            "QA": _BEIN_MENA,
            "EG": _BEIN_MENA,
        },
    },

    # ════════════════════════════════════════════════════════════════
    # Tennis Grand Slams — single-event-per-tournament in our DB
    # ════════════════════════════════════════════════════════════════
    "Wimbledon": {
        "display_label": "Wimbledon 2026",
        "tsdb_league": None,  # TheSportsDB models tennis per-match; our row is the whole tournament.
        "tsdb_season": None,
        "broadcasters": {
            "GB": [{"channel": "BBC Sport",   "url": "https://www.bbc.co.uk/sport/tennis"}],
            "US": [
                {"channel": "ESPN",           "url": "https://www.espn.com/tennis/"},
                {"channel": "Tennis Channel", "url": "https://www.tennischannel.com/"},
            ],
            "IL": [{"channel": "Sport 5",     "url": "https://www.sport5.co.il/"}],
            "ES": _EUROSPORT_EU_TENNIS,
            "DE": [
                {"channel": "ARD",            "url": "https://www.sportschau.de/tennis/"},
                {"channel": "ZDF",            "url": "https://sport.zdf.de/tennis/"},
            ],
            "FR": _EUROSPORT_EU_TENNIS,
            "IT": [
                {"channel": "Sky Sport",      "url": "https://sport.sky.it/tennis"},
                {"channel": "SuperTennis",    "url": "https://www.supertennis.tv/"},
            ],
            "BR": [
                {"channel": "SporTV",         "url": "https://sportv.globo.com/"},
                {"channel": "ESPN Brasil",    "url": "https://www.espn.com.br/tenis/"},
            ],
            "AU": [
                {"channel": "9Now",           "url": "https://www.9now.com.au/"},
                {"channel": "Stan Sport",     "url": "https://www.stan.com.au/sport"},
            ],
            "CA": [{"channel": "TSN",         "url": "https://www.tsn.ca/tennis"}],
            "JP": [{"channel": "WOWOW",       "url": "https://www.wowow.co.jp/sports/tennis/"}],
            "AR": [{"channel": "ESPN",        "url": "https://www.espn.com.ar/tenis/"}],
            "MX": [{"channel": "ESPN México", "url": "https://www.espn.com.mx/tenis/"}],
            "NL": _EUROSPORT_EU_TENNIS,
            "BE": _EUROSPORT_EU_TENNIS,
            "GR": _EUROSPORT_EU_TENNIS,
            "TR": _EUROSPORT_EU_TENNIS,
            "PL": _EUROSPORT_EU_TENNIS,
            "AT": [{"channel": "ServusTV",    "url": "https://www.servustv.com/sport/tennis/"}],
            "QA": _BEIN_MENA,
        },
    },

    "Roland Garros": {
        "display_label": "Roland-Garros 2026",
        "tsdb_league": None,
        "tsdb_season": None,
        "broadcasters": {
            "FR": [
                {"channel": "France Télévisions","url": "https://www.france.tv/sport/roland-garros/"},
                {"channel": "Amazon Prime",   "url": "https://www.primevideo.com/region/eu/"},
            ],
            "US": [
                {"channel": "Tennis Channel", "url": "https://www.tennischannel.com/"},
                {"channel": "NBC",            "url": "https://www.nbcsports.com/tennis/french-open"},
            ],
            "IL": [{"channel": "Sport 5",     "url": "https://www.sport5.co.il/"}],
            "GB": [
                {"channel": "TNT Sports",     "url": "https://www.tntsports.co.uk/tennis/"},
                {"channel": "Eurosport",      "url": "https://www.eurosport.com/tennis/"},
            ],
            "ES": _EUROSPORT_EU_TENNIS,
            "DE": [
                {"channel": "Eurosport",      "url": "https://www.eurosport.de/tennis/"},
                {"channel": "ServusTV",       "url": "https://www.servustv.com/sport/tennis/"},
            ],
            "IT": _EUROSPORT_EU_TENNIS,
            "NL": _EUROSPORT_EU_TENNIS,
            "BE": _EUROSPORT_EU_TENNIS,
            "AT": [{"channel": "ServusTV",    "url": "https://www.servustv.com/sport/tennis/"}],
            "GR": _EUROSPORT_EU_TENNIS,
            "TR": _EUROSPORT_EU_TENNIS,
            "PL": _EUROSPORT_EU_TENNIS,
            "BR": [{"channel": "SporTV",      "url": "https://sportv.globo.com/"}],
            "AR": [{"channel": "ESPN",        "url": "https://www.espn.com.ar/tenis/"}],
            "MX": [{"channel": "ESPN México", "url": "https://www.espn.com.mx/tenis/"}],
            "AU": [
                {"channel": "9Now",           "url": "https://www.9now.com.au/"},
                {"channel": "Stan Sport",     "url": "https://www.stan.com.au/sport"},
            ],
            "CA": [{"channel": "TSN",         "url": "https://www.tsn.ca/tennis"}],
            "JP": [{"channel": "WOWOW",       "url": "https://www.wowow.co.jp/sports/tennis/"}],
            "QA": _BEIN_MENA,
        },
    },

    "US Open": {
        "display_label": "US Open 2026",
        "tsdb_league": None,
        "tsdb_season": None,
        "broadcasters": {
            "US": [
                {"channel": "ESPN",           "url": "https://www.espn.com/tennis/usopen/"},
                {"channel": "ESPN+",          "url": "https://plus.espn.com/"},
            ],
            "IL": [{"channel": "Sport 5",     "url": "https://www.sport5.co.il/"}],
            "GB": [{"channel": "Sky Sports",  "url": "https://www.skysports.com/tennis"}],
            "ES": _EUROSPORT_EU_TENNIS,
            "DE": [
                {"channel": "ServusTV",       "url": "https://www.servustv.com/sport/tennis/"},
                {"channel": "Sky",            "url": "https://www.sky.de/sport"},
            ],
            "FR": [
                {"channel": "Eurosport",      "url": "https://www.eurosport.fr/tennis/"},
                {"channel": "Amazon Prime",   "url": "https://www.primevideo.com/region/eu/"},
            ],
            "IT": _EUROSPORT_EU_TENNIS,
            "NL": _EUROSPORT_EU_TENNIS,
            "BE": _EUROSPORT_EU_TENNIS,
            "AT": [{"channel": "ServusTV",    "url": "https://www.servustv.com/sport/tennis/"}],
            "GR": _EUROSPORT_EU_TENNIS,
            "TR": _EUROSPORT_EU_TENNIS,
            "PL": _EUROSPORT_EU_TENNIS,
            "BR": [{"channel": "SporTV",      "url": "https://sportv.globo.com/"}],
            "AR": [{"channel": "ESPN",        "url": "https://www.espn.com.ar/tenis/"}],
            "MX": [{"channel": "ESPN México", "url": "https://www.espn.com.mx/tenis/"}],
            "AU": [
                {"channel": "9Now",           "url": "https://www.9now.com.au/"},
                {"channel": "Stan Sport",     "url": "https://www.stan.com.au/sport"},
            ],
            "CA": [{"channel": "TSN",         "url": "https://www.tsn.ca/tennis"}],
            "JP": [{"channel": "WOWOW",       "url": "https://www.wowow.co.jp/sports/tennis/"}],
            "QA": _BEIN_MENA,
        },
    },
}


# ── Derived collections (callers import these) ────────────────────────

# Set of tournament keys allowed as AC chips. Imported by
# _suggestions_index.py to gate the chip surface.
TOURNAMENT_ALLOWLIST: frozenset[str] = frozenset(TOURNAMENTS.keys())


def display_label(tournament: str) -> str:
    """Friendly name for a tournament. Falls back to the bare key when
    the tournament isn't in the catalog."""
    cfg = TOURNAMENTS.get(tournament)
    return cfg["display_label"] if cfg else tournament


def tsdb_mapping(tournament: str) -> tuple[Optional[str], Optional[str]]:
    """(league_id, season) for TheSportsDB lookup, or (None, None)
    when the tournament isn't sourced from TheSportsDB."""
    cfg = TOURNAMENTS.get(tournament)
    if not cfg:
        return (None, None)
    return (cfg.get("tsdb_league"), cfg.get("tsdb_season"))


def broadcasters_for(tournament: str) -> dict[str, list[dict]]:
    """Per-country broadcaster map for a tournament. Returns an empty
    dict for tournaments not in the catalog."""
    cfg = TOURNAMENTS.get(tournament)
    return cfg.get("broadcasters", {}) if cfg else {}
