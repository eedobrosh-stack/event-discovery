"""Vertical × geo query taxonomy for Cadence-B Brave discovery.

Source: operator-curated spreadsheet (May 2026) — two seed lists plus
a target-city list. Imported here as Python literals so Cadence B can
rotate through (vertical, geo) pairs deterministically. To update,
edit this file; the next Cadence-B fire reads the new lists.

Query shapes (see also: scheduler.jobs.llm_discover_sources_job):

  category × city     → "{category} events in {city}"
                        e.g. "Art events in Detroit"
  conference × city   → "{vertical} conferences in {city}"
                        e.g. "AI/ML conferences in Tel Aviv"
  city × template     → "things to do in {city}" / "concerts in {city}"
                        / "live shows in {city}" / "{city} event calendar"
                        / "{city} events this weekend"
  category × country  → "{category} events in {country}"
                        e.g. "Music events in Norway"

Country list comes from the live ``cities`` table at runtime (distinct
``country`` values), not hardcoded here — that keeps the geography
axis aligned with whatever cities we're actually collecting events
for.

The ``conference_country`` shape was retired on 2026-05-18: a QA
review of the LLM-extracted event pool showed that 59% of upcoming
LLM events came from 9 conference-aggregator hosts, all reached
predominantly through country-level conference queries (which return
templated aggregator landing pages). Those domains are now in
``_RESERVED_DISCOVERY_DOMAINS`` and the kind is filtered out of the
coverage-row picker so legacy rows stay inert.
"""
from __future__ import annotations


# ── Event categories (general bucket) ─────────────────────────────────
# 23 entries. Cover the breadth of consumer events Supercaly aggregates.
# Used for the "{category} events in {city}" shape.
EVENT_CATEGORIES: list[str] = [
    "Music",
    "Art",
    "Comedy",
    "Sports",
    "Business",
    "Film",
    "Fitness",
    "Gaming",
    "Festival",
    "Dance",
    "Career",
    "Food & Drink",
    "Workshop",
    "Charity",
    "Outdoor",
    "Literature",
    "Parade",
    "Theme Park",
    "Craft",
    "Religious",
    "Home & Garden",
    "Medical",
    "Real Estate",
]


# ── Professional conference verticals ─────────────────────────────────
# Trimmed 2026-05-18 from 28 → 8 high-signal B2B verticals. The dropped
# entries (Psychology, Leadership & Culture, EdTech, PropTech, Supply
# Chain, Aerospace, AgTech, Sustainability/ESG, etc.) were the ones
# driving most of the conference-aggregator junk surfaced in the
# 2026-05-18 QA review. Used only for the city-level shape — the
# country-level conference shape is retired.
CONFERENCE_VERTICALS: list[str] = [
    "Artificial Intelligence & Machine Learning",
    "Cybersecurity & Data Privacy",
    "MarTech & Marketing Automation",
    "Digital Banking & FinTech",
    "Digital Health & Telemedicine",
    "ClimateTech & Decarbonization",
    "SaaS & Enterprise Operations",
    "Gaming & Esports Business",
]


# ── Consumer-event city query templates ───────────────────────────────
# Added 2026-05-18 to rebalance the query mix away from B2B conferences.
# These shapes hit consumer-aggregator pages (TimeOut, SecretNYC, Do312,
# tourism boards) rather than conference indices. Rendered with
# .format(city=…) so the template stores the entire phrase.
CITY_QUERY_TEMPLATES: list[str] = [
    "things to do in {city}",
    "concerts in {city}",
    "live shows in {city}",
    "{city} event calendar",
    "{city} events this weekend",
]


# Cities are NOT hardcoded — the phase pulls all cities (~8K) from
# the live `cities` table so geo coverage tracks whatever rows we
# already collect events for, not a frozen curated list. Same goes
# for the country axis (distinct cities.country values).


# ── Priority-tier picks ──────────────────────────────────────────────
# Used by the vertical-geo phase to compute coverage-row priority:
#
#   * Wave 1 (priority=2): top 100 cities by event count × ALL
#                          city-axis shapes (category × city,
#                          conference × city, and the 5 city query
#                          templates).
#   * Wave 2 (priority=1): OECD-38 × first 10 categories
#                          (country-level conference shape retired).
#   * Wave 0 (priority=0): every other (kind, vertical, geo_name)
#                          combo in the matrix.
#
# Cities for Wave 1 are computed at runtime from the events table
# (popularity is a moving target). OECD list + category "top N" pick
# live here as named constants.

# OECD member states ordered by population (2024 estimates). 38
# countries total. Names match the canonical `cities.country` values
# we see in the DB; "Korea Republic" is the WC-style spelling, but
# our cities table uses "South Korea" — matching that here.
OECD_COUNTRIES_BY_POPULATION: list[str] = [
    "United States",        # ~332M
    "Mexico",               # ~128M
    "Japan",                # ~125M
    "Turkey",               # ~85M
    "Germany",              # ~84M
    "France",               # ~68M
    "United Kingdom",       # ~67M
    "Italy",                # ~59M
    "South Korea",          # ~52M
    "Colombia",             # ~52M
    "Spain",                # ~48M
    "Canada",               # ~40M
    "Poland",               # ~38M
    "Australia",            # ~26M
    "Chile",                # ~19M
    "Netherlands",          # ~17M
    "Belgium",              # ~12M
    "Sweden",               # ~10.5M
    "Czech Republic",       # ~10.5M
    "Greece",               # ~10.4M
    "Portugal",             # ~10.3M
    "Hungary",              # ~9.7M
    "Israel",               # ~9.6M
    "Austria",              # ~9.1M
    "Switzerland",          # ~8.8M
    "Denmark",              # ~5.9M
    "Norway",               # ~5.5M
    "Finland",              # ~5.5M
    "Slovakia",             # ~5.4M
    "Ireland",              # ~5.2M
    "New Zealand",          # ~5.2M
    "Costa Rica",           # ~5.2M
    "Lithuania",            # ~2.8M
    "Slovenia",             # ~2.1M
    "Latvia",               # ~1.9M
    "Estonia",              # ~1.3M
    "Luxembourg",           # ~0.66M
    "Iceland",              # ~0.39M
]


def wave2_categories() -> list[str]:
    """Top 10 categories from the spreadsheet — i.e. the first 10
    entries of EVENT_CATEGORIES in the operator-defined order."""
    return EVENT_CATEGORIES[:10]


# ── Query template helpers ───────────────────────────────────────────
# Centralised so the registered coverage rows agree with the actually-
# fired Brave queries (no skew between "what we said we'd fire" vs
# "what we fired").

def category_city_query(category: str, city: str) -> str:
    return f"{category} events in {city}"


def category_country_query(category: str, country: str) -> str:
    return f"{category} events in {country}"


def conference_city_query(vertical: str, city: str) -> str:
    return f"{vertical} conferences in {city}"


def city_template_query(template: str, city: str) -> str:
    """Render a CITY_QUERY_TEMPLATES entry against a city."""
    return template.format(city=city)


def enumerate_pairs(cities: list[str],
                    countries: list[str]) -> list[tuple[str, str, str, str]]:
    """All (kind, vertical, geo_type, geo_name) combos this taxonomy
    can produce given a city list + country list. Caller is
    responsible for mixing with a coverage log to pick which ones
    to fire next.

    Matrix size at prod scale (~8K cities, ~38 OECD countries):
      category_city      : 23 × 8K   = 184,000
      conference_city    : 8  × 8K   =  64,000
      city_query         : 5  × 8K   =  40,000
      category_country   : 23 × 38   =     874
      Total                          ≈ 288,874

    For `city_query`, the `vertical` field stores the template string
    (e.g. "things to do in {city}"); `render_query` substitutes the
    geo_name in. The `conference_country` kind was retired 2026-05-18
    after a QA review showed it drove most aggregator-junk discovery.

    `kind`      : "category_city" | "category_country" |
                  "conference_city" | "city_query"
    `vertical`  : the topical term (event category, conference vertical,
                  or city-query template string)
    `geo_type`  : "city" | "country"
    `geo_name`  : the resolved place name
    """
    pairs: list[tuple[str, str, str, str]] = []
    for cat in EVENT_CATEGORIES:
        for city in cities:
            pairs.append(("category_city", cat, "city", city))
        for country in countries:
            pairs.append(("category_country", cat, "country", country))
    for vert in CONFERENCE_VERTICALS:
        for city in cities:
            pairs.append(("conference_city", vert, "city", city))
    for template in CITY_QUERY_TEMPLATES:
        for city in cities:
            pairs.append(("city_query", template, "city", city))
    return pairs


def render_query(kind: str, vertical: str, geo_name: str) -> str:
    """Return the Brave query string for one row.

    The retired ``conference_country`` kind is still rendered for
    back-compat with any legacy brave_query_coverage rows that happen
    to slip through the picker filter — but those rows are excluded
    by ``llm_discover_sources_job``'s ``WHERE kind != 'conference_country'``
    clause, so this branch is essentially unreachable in practice.
    """
    if kind == "category_city":
        return category_city_query(vertical, geo_name)
    if kind == "category_country":
        return category_country_query(vertical, geo_name)
    if kind == "conference_city":
        return conference_city_query(vertical, geo_name)
    if kind == "city_query":
        return city_template_query(vertical, geo_name)
    if kind == "conference_country":
        # retired 2026-05-18; legacy rows return the old shape if
        # anything still references them
        return f"{vertical} conferences in {geo_name}"
    raise ValueError(f"unknown kind: {kind!r}")


def compute_priority(kind: str, vertical: str, geo_name: str,
                     top_cities: set[str]) -> int:
    """Return the priority tier for a coverage row.

    Priority semantics (used by _run_vertical_geo_brave_phase's
    picker as the primary ORDER BY):

      2 — Wave 1: top-100 cities × any city-axis kind
                  (category_city, conference_city, city_query).
      1 — Wave 2: country kinds, OECD-only, top-10 categories
                  (conference_country tier was retired 2026-05-18).
      0 — everything else (the long-tail rotation).

    Note: when a Wave 2 country has a city in the top-100, that
    city's city-level queries are Wave 1 and the country-level
    queries are Wave 2. Both fire ahead of long-tail; the city
    queries fire first because Wave 1 > Wave 2.
    """
    # Wave 1: any city-axis kind, when city is in the top-100 set.
    if geo_name in top_cities and kind in (
        "category_city", "conference_city", "city_query"
    ):
        return 2

    # Wave 2: category × OECD country (conference_country retired).
    if kind == "category_country" and geo_name in OECD_COUNTRIES_BY_POPULATION:
        if vertical in wave2_categories():
            return 1

    return 0
