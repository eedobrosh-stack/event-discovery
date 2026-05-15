"""Vertical × geo query taxonomy for Cadence-B Brave discovery.

Source: operator-curated spreadsheet (May 2026) — two seed lists plus
a target-city list. Imported here as Python literals so Cadence B can
rotate through (vertical, geo) pairs deterministically. To update,
edit this file; the next Cadence-B fire reads the new lists.

Query shapes (see also: scheduler.jobs.llm_discover_sources_job):

  category × city     → "{category} events in {city}"
                        e.g. "Art events in Detroit"
  vertical × city     → "{vertical} conferences in {city}"
                        e.g. "AI/ML conferences in Tel Aviv"
  vertical × country  → "{vertical} conferences in {country}"
                        e.g. "MarTech conferences in Norway"

Country list comes from the live ``cities`` table at runtime (distinct
``country`` values), not hardcoded here — that keeps the geography
axis aligned with whatever cities we're actually collecting events
for.
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
# 28 entries. Used for both:
#   "{vertical} conferences in {city}"
#   "{vertical} conferences in {country}"
#
# Format choice (with the "&" separator) preserves the operator's
# spelling so the query reads natural in Brave's index.
CONFERENCE_VERTICALS: list[str] = [
    "Artificial Intelligence & Machine Learning",
    "Cybersecurity & Data Privacy",
    "Cloud Computing & DevOps",
    "Mobile & Telecommunications",
    "MarTech & Marketing Automation",
    "Content Strategy & Creator Economy",
    "Social Media & Influencer Marketing",
    "Data Analytics & Personalization",
    "Digital Banking & FinTech",
    "E-commerce & Retail Innovation",
    "RegTech & Compliance",
    "Web3 & Decentralized Finance",
    "Digital Health & Telemedicine",
    "Biotechnology & Therapeutics",
    "Healthcare Administration & Policy",
    "Renewable Energy Infrastructure",
    "ClimateTech & Decarbonization",
    "Corporate Sustainability & ESG",
    "SaaS & Enterprise Operations",
    "Future of Work & Talent Acquisition",
    "Leadership & Culture",
    "EdTech & Higher Education Innovation",
    "PropTech & Real Estate Innovation",
    "Supply Chain & Logistics Automation",
    "Gaming & Esports Business",
    "Aerospace & Defense Technology",
    "AgTech & Sustainable Farming",
    "Psychology",
]


# Cities are NOT hardcoded — the phase pulls all cities (~8K) from
# the live `cities` table so geo coverage tracks whatever rows we
# already collect events for, not a frozen curated list. Same goes
# for the country axis (distinct cities.country values).


# ── Priority-tier picks ──────────────────────────────────────────────
# Used by the vertical-geo phase to compute coverage-row priority:
#
#   * Wave 1 (priority=2): top 100 cities by event count × ALL
#                          categories + ALL conference verticals.
#   * Wave 2 (priority=1): OECD-38 (ordered by population) × first
#                          10 categories + first 6 conference
#                          verticals.
#   * Wave 0 (priority=0): every other (kind, vertical, geo_name)
#                          combo in the matrix.
#
# Cities for Wave 1 are computed at runtime from the events table
# (popularity is a moving target). OECD list + category/vertical
# "top N" picks live here as named constants.

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


def wave2_conferences() -> list[str]:
    """Top 6 conference verticals from the spreadsheet — i.e. the
    first 6 entries of CONFERENCE_VERTICALS in the operator-defined
    order."""
    return CONFERENCE_VERTICALS[:6]


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


def conference_country_query(vertical: str, country: str) -> str:
    return f"{vertical} conferences in {country}"


def enumerate_pairs(cities: list[str],
                    countries: list[str]) -> list[tuple[str, str, str, str]]:
    """All (kind, vertical, geo_type, geo_name) combos this taxonomy
    can produce given a city list + country list. Caller is
    responsible for mixing with a coverage log to pick which ones
    to fire next.

    Matrix size at prod scale (~8K cities, ~50 countries):
      category_city      : 23 × 8K   = 184,000
      conference_city    : 28 × 8K   = 224,000
      category_country   : 23 × 50   =   1,150  (new — added for Wave 2)
      conference_country : 28 × 50   =   1,400
      Total                          ≈ 410,550

    `kind`      : "category_city" | "category_country" |
                  "conference_city" | "conference_country"
    `vertical`  : the topical term (event category or conference vertical)
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
        for country in countries:
            pairs.append(("conference_country", vert, "country", country))
    return pairs


def render_query(kind: str, vertical: str, geo_name: str) -> str:
    """Return the Brave query string for one row."""
    if kind == "category_city":
        return category_city_query(vertical, geo_name)
    if kind == "category_country":
        return category_country_query(vertical, geo_name)
    if kind == "conference_city":
        return conference_city_query(vertical, geo_name)
    if kind == "conference_country":
        return conference_country_query(vertical, geo_name)
    raise ValueError(f"unknown kind: {kind!r}")


def compute_priority(kind: str, vertical: str, geo_name: str,
                     top_cities: set[str]) -> int:
    """Return the priority tier for a coverage row.

    Priority semantics (used by _run_vertical_geo_brave_phase's
    picker as the primary ORDER BY):

      2 — Wave 1: top-100 cities × ALL categories + ALL verticals
      1 — Wave 2: OECD × first-10 cats + first-6 verticals
                 (city kinds NOT included here; OECD cities tend to
                 already sit in the top-100 by event count, so the
                 city-level coverage is handled by Wave 1)
      0 — everything else (the long-tail rotation)

    Note: when a Wave 2 country has a city in the top-100, that
    city's city-level queries are Wave 1 and the country-level
    queries are Wave 2. Both fire ahead of long-tail; the city
    queries fire first because Wave 1 > Wave 2.
    """
    # Wave 1: city kinds × any vertical/category, when city is in
    # the top-100 set.
    if geo_name in top_cities and kind in ("category_city", "conference_city"):
        return 2

    # Wave 2: country kinds, OECD-only, top-10 cats / top-6 verticals.
    if kind == "category_country" and geo_name in OECD_COUNTRIES_BY_POPULATION:
        if vertical in wave2_categories():
            return 1
    if kind == "conference_country" and geo_name in OECD_COUNTRIES_BY_POPULATION:
        if vertical in wave2_conferences():
            return 1

    return 0
