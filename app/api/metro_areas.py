"""
Metropolitan / county area definitions and API endpoint.

Each metro entry maps a human-friendly area name to the list of city names
that belong to it. The endpoint resolves those names against the DB so only
cities that actually have events are returned.

GET /api/metro-areas  →  list of MetroAreaOut
"""
from __future__ import annotations

import time
from typing import List, Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database import get_db, SessionLocal

router = APIRouter(prefix="/api/metro-areas", tags=["metro-areas"])

# ── Static metro-area definitions ──────────────────────────────────────────────
# City names must match the `name` column in the cities table (case-insensitive).
# Only cities that exist AND have events will be included in the response.

METRO_AREAS: list[dict] = [
    {
        "id": "bay-area",
        "name": "Bay Area",
        "country": "United States",
        "cities": [
            "San Francisco", "Oakland", "San Jose", "Berkeley",
            "Fremont", "Hayward", "Sunnyvale", "Santa Clara", "Concord",
            "Vallejo", "Richmond", "Antioch", "San Mateo", "Daly City",
            "San Leandro", "South San Francisco", "Alameda", "Walnut Creek",
            "Livermore", "Napa", "Santa Rosa", "Petaluma", "Novato",
            "San Rafael", "Mill Valley", "Sausalito", "Palo Alto",
            "Mountain View", "Menlo Park", "Redwood City", "San Bruno",
            "Burlingame", "Foster City", "Millbrae", "San Francisco Bay Area",
        ],
    },
    {
        "id": "greater-nyc",
        "name": "Greater New York",
        "country": "United States",
        "cities": [
            "New York", "Brooklyn", "Queens", "Bronx", "Staten Island",
            "Newark", "Jersey City", "Hoboken", "Yonkers", "New Rochelle",
            "White Plains", "Stamford", "Bridgeport", "Long Island City",
            "Astoria", "Flushing", "Jamaica", "Harlem",
        ],
    },
    {
        "id": "greater-la",
        "name": "Greater Los Angeles",
        "country": "United States",
        "cities": [
            "Los Angeles", "Long Beach", "Anaheim", "Santa Ana", "Riverside",
            "Irvine", "Glendale", "Burbank", "Pasadena", "Santa Monica",
            "Culver City", "Beverly Hills", "Hollywood", "Venice",
            "Inglewood", "Compton", "Torrance", "El Monte", "Pomona",
            "Thousand Oaks", "Ontario", "Rancho Cucamonga",
        ],
    },
    {
        "id": "greater-chicago",
        "name": "Greater Chicago",
        "country": "United States",
        "cities": [
            "Chicago", "Aurora", "Joliet", "Naperville", "Elgin",
            "Waukegan", "Cicero", "Evanston", "Schaumburg", "Bolingbrook",
            "Arlington Heights", "Peoria", "Rockford",
        ],
    },
    {
        "id": "greater-miami",
        "name": "Greater Miami",
        "country": "United States",
        "cities": [
            "Miami", "Miami Beach", "Hialeah", "Fort Lauderdale",
            "Boca Raton", "West Palm Beach", "Coral Gables", "Aventura",
            "Hollywood", "Pompano Beach", "Deerfield Beach", "Delray Beach",
            "Hallandale Beach", "North Miami", "Coral Springs",
        ],
    },
    {
        "id": "greater-boston",
        "name": "Greater Boston",
        "country": "United States",
        "cities": [
            "Boston", "Cambridge", "Somerville", "Quincy", "Newton",
            "Brookline", "Waltham", "Medford", "Malden", "Lowell",
            "Lynn", "Worcester", "Providence",
        ],
    },
    {
        "id": "greater-seattle",
        "name": "Greater Seattle",
        "country": "United States",
        "cities": [
            "Seattle", "Bellevue", "Tacoma", "Redmond", "Kirkland",
            "Renton", "Everett", "Sammamish", "Shoreline",
        ],
    },
    {
        "id": "greater-london",
        "name": "Greater London",
        "country": "United Kingdom",
        "cities": [
            "London", "Westminster", "Camden", "Islington", "Hackney",
            "Southwark", "Lambeth", "Greenwich", "Lewisham", "Bromley",
            "Croydon", "Kingston upon Thames", "Richmond upon Thames",
            "Wandsworth", "Hammersmith", "Kensington", "Chelsea",
            "Tower Hamlets", "Newham", "Barking", "Havering",
            "Waltham Forest", "Haringey", "Enfield", "Barnet",
        ],
    },
    {
        "id": "greater-paris",
        "name": "Greater Paris",
        "country": "France",
        "cities": [
            "Paris", "Boulogne-Billancourt", "Saint-Denis", "Argenteuil",
            "Montreuil", "Nanterre", "Créteil", "Versailles",
            "Colombes", "Asnières-sur-Seine", "Courbevoie",
        ],
    },
    {
        "id": "greater-berlin",
        "name": "Greater Berlin",
        "country": "Germany",
        "cities": [
            "Berlin", "Potsdam", "Brandenburg an der Havel",
            "Frankfurt (Oder)", "Cottbus",
        ],
    },
    {
        "id": "greater-toronto",
        "name": "Greater Toronto",
        "country": "Canada",
        "cities": [
            "Toronto", "Mississauga", "Brampton", "Markham", "Vaughan",
            "Richmond Hill", "Oakville", "Burlington", "Ajax", "Pickering",
            "Whitby", "Oshawa", "Hamilton",
        ],
    },
    {
        "id": "gush-dan",
        "name": "Gush Dan (Tel Aviv Metro)",
        "country": "Israel",
        "cities": [
            "Tel Aviv", "Ramat Gan", "Givatayim", "Petah Tikva", "Bnei Brak",
            "Bat Yam", "Holon", "Rishon LeZion", "Or Yehuda", "Kiryat Ono",
            "Herzliya", "Netanya", "Rehovot", "Kfar Saba", "Ra'anana",
        ],
    },
    {
        "id": "greater-amsterdam",
        "name": "Greater Amsterdam",
        "country": "Netherlands",
        "cities": [
            "Amsterdam", "Haarlem", "Amstelveen", "Zaandam",
            "Almere", "Hoofddorp",
        ],
    },
    {
        "id": "greater-sydney",
        "name": "Greater Sydney",
        "country": "Australia",
        "cities": [
            "Sydney", "Parramatta", "Liverpool", "Penrith",
            "Wollongong", "Newcastle", "Manly", "Bondi",
        ],
    },
]

# ── Schema ─────────────────────────────────────────────────────────────────────

class MetroAreaOut(BaseModel):
    id: str
    name: str
    country: str
    city_ids: str          # comma-separated city IDs that exist in the DB
    city_names: List[str]  # matched city names (for display)
    city_count: int


# ── Cache ─────────────────────────────────────────────────────────────────────

_cache: List[MetroAreaOut] = []
_cache_ts: float = 0.0
_TTL = 3600  # 1 hour — changes only when new cities are scraped


def _build_metro_list(db: Session) -> List[MetroAreaOut]:
    result = []
    for metro in METRO_AREAS:
        # Fetch all cities that match any name in this metro's city list
        # Use case-insensitive matching
        placeholders = ", ".join([f":n{i}" for i in range(len(metro["cities"]))])
        params = {f"n{i}": name for i, name in enumerate(metro["cities"])}

        rows = db.execute(text(f"""
            SELECT MIN(c.id) as city_id, c.name
            FROM cities c
            WHERE LOWER(c.name) IN ({placeholders})
              AND c.id IN (
                  SELECT DISTINCT v.city_id
                  FROM venues v
                  WHERE v.city_id IS NOT NULL
                    AND v.id IN (
                        SELECT DISTINCT e.venue_id
                        FROM events e
                        WHERE e.venue_id IS NOT NULL
                    )
              )
            GROUP BY LOWER(c.name)
            ORDER BY c.name
        """), {k: v.lower() for k, v in params.items()}).fetchall()

        if not rows:
            continue  # skip metros with no matching cities in DB

        city_ids = ",".join(str(r[0]) for r in rows)
        city_names = [r[1] for r in rows]

        result.append(MetroAreaOut(
            id=metro["id"],
            name=metro["name"],
            country=metro["country"],
            city_ids=city_ids,
            city_names=city_names,
            city_count=len(rows),
        ))

    return result


def warm_metro_cache():
    """Call once at startup alongside warm_cities_cache."""
    global _cache, _cache_ts
    db = SessionLocal()
    try:
        _cache = _build_metro_list(db)
        _cache_ts = time.time()
    finally:
        db.close()


@router.get("", response_model=List[MetroAreaOut])
def list_metro_areas(db: Session = Depends(get_db)):
    global _cache, _cache_ts
    if _cache and (time.time() - _cache_ts) < _TTL:
        return _cache
    _cache = _build_metro_list(db)
    _cache_ts = time.time()
    return _cache
