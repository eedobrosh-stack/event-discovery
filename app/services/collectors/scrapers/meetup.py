"""
Meetup.com event scraper via GraphQL (GQL2 endpoint — no auth required).

Fetches locally-recommended events for each city using Meetup's internal
GraphQL endpoint. Covers tech meetups, networking events, workshops, fitness,
community events, and more.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import date, datetime, timezone
from typing import Optional

import httpx

from app.services.collectors.base import BaseCollector, RawEvent, safe_time, default_end_time

logger = logging.getLogger(__name__)

# City name → (latitude, longitude, radius_miles)
CITY_COORDS: dict[str, tuple[float, float, int]] = {
    "New York":      (40.7128,  -74.0060, 25),
    "Tel Aviv":      (32.0853,   34.7818, 20),
    "London":        (51.5074,   -0.1278, 25),
    "Los Angeles":   (34.0522, -118.2437, 25),
    "Chicago":       (41.8781,  -87.6298, 25),
    "San Francisco": (37.7749, -122.4194, 20),
    "Berkeley":      (37.8716, -122.2727, 15),
    "Berlin":        (52.5200,   13.4050, 25),
    "Paris":         (48.8566,    2.3522, 25),
    "Toronto":       (43.6532,  -79.3832, 25),
    "Sydney":        (-33.8688, 151.2093, 25),
    "Austin":        (30.2672,  -97.7431, 20),
    "Boston":        (42.3601,  -71.0589, 20),
    "Seattle":       (47.6062, -122.3321, 20),
    "Miami":         (25.7617,  -80.1918, 25),
    "Amsterdam":     (52.3676,    4.9041, 20),
    "Singapore":     ( 1.3521,  103.8198, 20),
    "Tokyo":         (35.6762,  139.6503, 25),
}

GQL2_URL = "https://www.meetup.com/gql2"

GQL_QUERY = """
query RecommendedEvents($lat: Float!, $lon: Float!, $radius: Float!, $first: Int!) {
  recommendedEvents(filter: { lat: $lat, lon: $lon, radius: $radius }, first: $first) {
    edges {
      node {
        id
        title
        dateTime
        endTime
        description
        eventUrl
        venue {
          name
          address
          city
          state
          country
          lat
          lon
        }
        group {
          name
          urlname
        }
      }
    }
  }
}
"""

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Origin": "https://www.meetup.com",
    "Referer": "https://www.meetup.com/find/",
}


def _sid(event_id: str) -> str:
    return hashlib.md5(f"meetup:{event_id}".encode()).hexdigest()[:16]


def _parse_meetup_dt(iso_str: str) -> Optional[datetime]:
    """Parse Meetup ISO datetime which includes timezone offset."""
    if not iso_str:
        return None
    try:
        return datetime.fromisoformat(iso_str)
    except (ValueError, AttributeError):
        return None


# ---------------------------------------------------------------------------
# Category inference
# ---------------------------------------------------------------------------

_CATEGORY_RULES: list[tuple[list[str], str]] = [
    (["hackathon", "hack ", "buildathon", "coding", "code ", "developer", "devops",
      "kubernetes", "docker", "cloud native", "aws ", "gcp ", "azure ",
      "python ", "javascript", "react ", "node.js", "typescript",
      "machine learning", "deep learning", "llm", "generative ai",
      "data science", "data engineering", "blockchain", "web3",
      "startup", "saas", "open source", "github", "api "], "Technology"),

    (["conference", "summit", "congress", "symposium", "expo ", "tech talk",
      "product launch", "demo day", "innovation"], "Technology"),

    (["meetup", "networking", "happy hour", "mixer", "social hour",
      "after work", "professional network", "connect & grow"], "Career"),

    (["workshop", "bootcamp", "training course", "masterclass",
      "class ", "certification", "learn to", "how to "], "Workshop"),

    (["yoga", "pilates", "meditation", "mindfulness", "wellness",
      "health & wellness", "mental health"], "Fitness"),

    (["run ", "running", "marathon", "5k", "10k", "cycling", "bike ride",
      "fitness", "crossfit", "hiit", "workout", "gym ", "trail", "hike",
      "hiking", "climb"], "Fitness"),

    (["music", "concert", "jazz", "dj set", "live music", "band ",
      "classical", "symphony", "orchestra", "choir", "open mic",
      "jam session", "acoustic"], "Music"),

    (["art ", "gallery", "exhibit", "museum", "photography", "painting",
      "drawing", "sculpture", "illustration", "design ", "creative"], "Art"),

    (["improv", "comedy", "stand-up", "stand up", "open mic comedy"], "Comedy"),

    (["food", "dinner", "brunch", "tasting", "wine", "beer", "cocktail",
      "cuisine", "restaurant", "cooking", "baking", "culinary"], "Food & Drink"),

    (["board game", "tabletop", "dungeon", "d&d", "role play", "gaming",
      "video game", "esport", "lan party"], "Gaming"),

    (["dance", "salsa", "tango", "swing ", "bachata", "kizomba",
      "ballroom", "latin dance", "west coast swing"], "Dance"),

    (["book club", "reading", "author talk", "literary", "poetry",
      "writing workshop", "storytelling"], "Literature"),

    (["outdoor", "nature", "park ", "garden", "kayak", "canoe",
      "paddleboard", "camping", "birdwatch", "wildlife"], "Outdoor"),

    (["charity", "volunteer", "fundrais", "community service",
      "nonprofit", "give back", "donation"], "Charity"),

    (["language exchange", "language learning", "spanish", "french",
      "english conversation", "japanese", "mandarin"], "Workshop"),

    (["career", "job fair", "interview prep", "resume", "linkedin"],
     "Career"),

    (["entrepreneur", "business", "startup pitch", "venture", "investor",
      "founder"], "Career"),
]


def _infer_categories(title: str, group_name: str, description: str) -> list[str]:
    """
    Return up to 2 internal category names inferred from the event text.
    Defaults to ["Technology"] since Meetup skews heavily tech.
    """
    text = f"{title} {group_name} {description[:300] if description else ''}".lower()
    matched: list[str] = []
    for keywords, category in _CATEGORY_RULES:
        if any(kw in text for kw in keywords):
            if category not in matched:
                matched.append(category)
        if len(matched) >= 2:
            break
    return matched if matched else ["Technology"]


# ---------------------------------------------------------------------------
# Collector
# ---------------------------------------------------------------------------

class MeetupCollector(BaseCollector):
    """Fetches events from Meetup.com via its internal GQL2 endpoint."""

    @property
    def source_name(self) -> str:
        return "meetup"

    def is_configured(self) -> bool:
        return True  # No API key needed

    async def collect(self, city_name: str, country_code: str = "US", **kwargs) -> list[RawEvent]:
        coords = CITY_COORDS.get(city_name)
        if not coords:
            return []

        lat, lon, radius = coords
        payload = {
            "query": GQL_QUERY,
            "variables": {"lat": lat, "lon": lon, "radius": float(radius), "first": 60},
        }

        try:
            async with httpx.AsyncClient(timeout=25, follow_redirects=True, headers=HEADERS) as client:
                resp = await client.post(GQL2_URL, json=payload)
                resp.raise_for_status()
                body = resp.json()
        except Exception as e:
            logger.debug(f"Meetup fetch failed for {city_name}: {type(e).__name__}: {e}")
            return []

        if "errors" in body:
            logger.debug(f"Meetup GQL errors for {city_name}: {body['errors']}")
            return []

        edges = (
            body.get("data", {})
            .get("recommendedEvents", {})
            .get("edges", [])
        )

        events: list[RawEvent] = []
        for edge in edges:
            node = edge.get("node") or {}
            raw = self._transform(node, city_name)
            if raw:
                events.append(raw)

        logger.info(f"Meetup {city_name}: {len(events)} events")
        return events

    def _transform(self, node: dict, city_name: str) -> Optional[RawEvent]:
        try:
            title = (node.get("title") or "").strip()
            if not title:
                return None

            start_str = node.get("dateTime")
            if not start_str:
                return None

            start_dt = _parse_meetup_dt(start_str)
            if not start_dt:
                return None

            # Normalize to UTC-aware then compare as date only
            today = date.today()
            if start_dt.date() < today:
                return None

            end_dt = _parse_meetup_dt(node.get("endTime") or "")

            # Venue — skip events with no physical venue (online-only)
            venue = node.get("venue") or {}
            venue_name = venue.get("name") or ""
            venue_address = venue.get("address") or ""
            venue_city = venue.get("city") or city_name
            venue_country = venue.get("country") or None
            venue_lat = venue.get("lat") or None
            venue_lon = venue.get("lon") or None

            # If the venue has no city at all it is likely an online event
            if not venue and not venue_city:
                return None

            # Group / organizer
            group = node.get("group") or {}
            group_name = group.get("name") or ""
            group_slug = group.get("urlname") or ""

            # Purchase / detail link
            event_url = node.get("eventUrl") or ""
            if not event_url and group_slug:
                event_id = node.get("id", "")
                event_url = f"https://www.meetup.com/{group_slug}/events/{event_id}/"

            # Description (truncated for storage)
            description = (node.get("description") or "")[:500] or None

            # Categories from keyword inference
            raw_cats = _infer_categories(title, group_name, description or "")

            # End time defaults
            end_date = end_dt.date() if end_dt else None
            end_time = safe_time(end_dt) if end_dt else None
            if end_time is None:
                end_date, end_time = default_end_time(
                    safe_time(start_dt), start_dt.date(), None
                )

            return RawEvent(
                source="meetup",
                source_id=_sid(str(node.get("id", title))),
                name=title,
                description=description,
                start_date=start_dt.date(),
                start_time=safe_time(start_dt),
                end_date=end_date,
                end_time=end_time,
                venue_name=venue_name or None,
                venue_address=venue_address or None,
                venue_city=venue_city,
                venue_country=venue_country,
                venue_lat=float(venue_lat) if venue_lat else None,
                venue_lon=float(venue_lon) if venue_lon else None,
                purchase_link=event_url or None,
                image_url=None,  # Meetup image field TBD — omitted for now
                price=None,
                price_currency="USD",
                raw_categories=raw_cats,
            )
        except Exception as e:
            logger.debug(f"Meetup _transform error: {e}")
            return None
