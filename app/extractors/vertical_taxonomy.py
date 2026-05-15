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


# ── Query template helpers ───────────────────────────────────────────
# Centralised so the registered coverage rows agree with the actually-
# fired Brave queries (no skew between "what we said we'd fire" vs
# "what we fired").

def category_city_query(category: str, city: str) -> str:
    return f"{category} events in {city}"


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

    Matrix size grows linearly with city count — at ~8K cities and
    51 verticals (23 categories + 28 conference verticals), this
    yields ~408K (vertical, city) combos plus 28 × |countries| for
    conference-at-country queries. ~410K total. Plan accordingly.

    `kind`      : "category_city" | "conference_city" | "conference_country"
    `vertical`  : the topical term (event category or conference vertical)
    `geo_type`  : "city" | "country"
    `geo_name`  : the resolved place name
    """
    pairs: list[tuple[str, str, str, str]] = []
    for cat in EVENT_CATEGORIES:
        for city in cities:
            pairs.append(("category_city", cat, "city", city))
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
    if kind == "conference_city":
        return conference_city_query(vertical, geo_name)
    if kind == "conference_country":
        return conference_country_query(vertical, geo_name)
    raise ValueError(f"unknown kind: {kind!r}")
