"""
Bandsintown API collector — artist-centric event source.

Unlike other collectors (which are city-based), Bandsintown is queried
per artist name. This module provides a thin async wrapper used by
scripts/scan_bandsintown.py.

API docs: https://bandsintown.com/for/developers
Endpoint: GET https://rest.bandsintown.com/artists/{artist}/events?app_id=APP_ID
"""
from __future__ import annotations

import logging
import urllib.parse
from datetime import date

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

BANDSINTOWN_BASE = "https://rest.bandsintown.com"


class BandsintownClient:
    """Lightweight async HTTP client for the Bandsintown v3 REST API."""

    def __init__(self, app_id: str | None = None):
        self.app_id = app_id or settings.BANDSINTOWN_APP_ID

    def is_configured(self) -> bool:
        return bool(self.app_id)

    async def get_artist_events(self, artist_name: str) -> list[dict]:
        """
        Return upcoming events for an artist.

        Returns empty list on 404 (artist not found) or any HTTP error.
        Each dict has the raw Bandsintown event structure:
          id, datetime, venue {name, city, region, country, latitude, longitude},
          offers [{type, url, status}], lineup [str], url, description
        """
        if not self.is_configured():
            raise RuntimeError("BANDSINTOWN_APP_ID not set in .env")

        encoded = urllib.parse.quote(artist_name, safe="")
        url = f"{BANDSINTOWN_BASE}/artists/{encoded}/events"

        async with httpx.AsyncClient(timeout=15) as client:
            try:
                resp = await client.get(url, params={"app_id": self.app_id})
                if resp.status_code == 404:
                    logger.debug(f"[Bandsintown] Artist not found: {artist_name!r}")
                    return []
                resp.raise_for_status()
                data = resp.json()
                # API returns [] or a list of event dicts
                if not isinstance(data, list):
                    return []
                return data
            except httpx.HTTPStatusError as e:
                logger.warning(f"[Bandsintown] HTTP {e.response.status_code} for {artist_name!r}")
                return []
            except Exception as e:
                logger.warning(f"[Bandsintown] Error fetching {artist_name!r}: {e}")
                return []

    def parse_event(self, raw: dict, artist_name: str) -> dict | None:
        """
        Convert a raw Bandsintown event dict into a normalised dict
        compatible with our RawEvent dataclass.
        Returns None if the event is in the past or missing required fields.
        """
        dt_str = raw.get("datetime", "")
        if not dt_str:
            return None

        try:
            # Format: "2026-05-15T20:00:00"
            from datetime import datetime
            dt = datetime.fromisoformat(dt_str.rstrip("Z"))
        except ValueError:
            return None

        if dt.date() < date.today():
            return None

        venue = raw.get("venue") or {}
        venue_name    = venue.get("name") or None
        venue_city    = venue.get("city") or None
        venue_country = venue.get("country") or None
        venue_region  = venue.get("region") or None
        try:
            venue_lat = float(venue.get("latitude") or 0) or None
        except (TypeError, ValueError):
            venue_lat = None
        try:
            venue_lon = float(venue.get("longitude") or 0) or None
        except (TypeError, ValueError):
            venue_lon = None

        # Ticket URL — prefer "Tickets" offer, fall back to event URL
        ticket_url = raw.get("url")
        for offer in raw.get("offers") or []:
            if offer.get("type") == "Tickets" and offer.get("url"):
                ticket_url = offer["url"]
                break

        lineup = raw.get("lineup") or [artist_name]
        event_name = lineup[0] if lineup else artist_name

        return {
            "source_id":      str(raw.get("id", "")),
            "name":           event_name,
            "artist_name":    artist_name,
            "start_date":     dt.date(),
            "start_time":     dt.strftime("%H:%M") if dt.hour or dt.minute else None,
            "description":    raw.get("description") or None,
            "purchase_link":  ticket_url,
            "venue_name":     venue_name,
            "venue_city":     venue_city,
            "venue_country":  venue_country,
            "venue_region":   venue_region,
            "venue_lat":      venue_lat,
            "venue_lon":      venue_lon,
        }
