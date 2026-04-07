"""Platform venue management API.

Endpoints:
  POST   /api/platform-venues/detect        — detect platform from a URL
  GET    /api/platform-venues/              — list all platform venues
  POST   /api/platform-venues/              — add a new platform venue
  PATCH  /api/platform-venues/{id}          — update venue settings
  DELETE /api/platform-venues/{id}          — remove a platform venue
  POST   /api/platform-venues/{id}/scrape   — manually scrape one venue now
"""
from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import City
from app.models.platform_venue import PlatformVenue
from app.services.platform_registry import detect_platform, fetch_platform_venue_events

router = APIRouter(prefix="/api/platform-venues", tags=["platform-venues"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _serialize(pv: PlatformVenue, city_name: str | None = None) -> dict:
    return {
        "id": pv.id,
        "name": pv.name,
        "city_id": pv.city_id,
        "city_name": city_name,
        "platform": pv.platform,
        "platform_id": pv.platform_id,
        "website_url": pv.website_url,
        "address": pv.address,
        "active": pv.active,
        "default_event_type_id": pv.default_event_type_id,
        "last_scraped_at": str(pv.last_scraped_at) if pv.last_scraped_at else None,
        "created_at": str(pv.created_at) if pv.created_at else None,
    }


async def _scrape_one(pv: PlatformVenue, db: Session) -> int:
    """Fetch + save events for a single PlatformVenue. Returns events saved count."""
    from app.scheduler.jobs import registry

    city = db.query(City).filter(City.id == pv.city_id).first()
    if not city:
        return 0

    raw_events = await fetch_platform_venue_events(pv, city.name, city.country)
    saved = registry._save_events(raw_events, city, db)

    pv.last_scraped_at = datetime.utcnow()
    db.commit()

    return saved


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.post("/detect")
async def detect_venue_platform(payload: dict):
    """
    Identify the ticketing platform for a venue URL without saving anything.

    Body: { "url": "https://..." }
    Returns: { detected, platform, platform_id, name, confidence, url }
    """
    url = (payload.get("url") or "").strip()
    if not url:
        raise HTTPException(400, "url is required")

    result = await detect_platform(url)
    if not result:
        return {"detected": False, "url": url}
    return {"detected": True, "url": url, **result}


@router.get("/")
def list_platform_venues(db: Session = Depends(get_db)):
    """Return all platform venues ordered by platform then name."""
    pvs = db.query(PlatformVenue).order_by(PlatformVenue.platform, PlatformVenue.name).all()
    city_map = {c.id: c.name for c in db.query(City).all()}
    return [_serialize(pv, city_map.get(pv.city_id)) for pv in pvs]


@router.post("/")
async def add_platform_venue(payload: dict, db: Session = Depends(get_db)):
    """
    Add a new platform venue and optionally scrape it immediately.

    Body:
      name              str   (required)
      platform          str   (required) — e.g. "venuepilot"
      platform_id       str   — venue ID on the platform
      city_id           int   — links to a City record
      website_url       str
      address           str
      active            bool  (default true)
      default_event_type_id  int
      scrape_now        bool  — if true, fetch events before returning
    """
    name = (payload.get("name") or "").strip()
    platform = (payload.get("platform") or "").strip()
    if not name or not platform:
        raise HTTPException(400, "name and platform are required")

    pv = PlatformVenue(
        name=name,
        city_id=payload.get("city_id"),
        platform=platform,
        platform_id=(payload.get("platform_id") or "").strip() or None,
        website_url=(payload.get("website_url") or "").strip() or None,
        address=(payload.get("address") or "").strip() or None,
        active=bool(payload.get("active", True)),
        default_event_type_id=payload.get("default_event_type_id"),
    )
    db.add(pv)
    db.commit()
    db.refresh(pv)

    city = db.query(City).filter(City.id == pv.city_id).first()
    result = _serialize(pv, city.name if city else None)

    if payload.get("scrape_now"):
        saved = await _scrape_one(pv, db)
        result["events_saved"] = saved

    return result


@router.patch("/{venue_id}")
def update_platform_venue(venue_id: int, payload: dict, db: Session = Depends(get_db)):
    """Update settings for an existing platform venue."""
    pv = db.query(PlatformVenue).filter(PlatformVenue.id == venue_id).first()
    if not pv:
        raise HTTPException(404, "Platform venue not found")

    for field in ["name", "city_id", "platform", "platform_id",
                  "website_url", "address", "active", "default_event_type_id"]:
        if field in payload:
            setattr(pv, field, payload[field])

    db.commit()
    city = db.query(City).filter(City.id == pv.city_id).first()
    return _serialize(pv, city.name if city else None)


@router.delete("/{venue_id}")
def delete_platform_venue(venue_id: int, db: Session = Depends(get_db)):
    """Remove a platform venue (does not delete already-scraped events)."""
    pv = db.query(PlatformVenue).filter(PlatformVenue.id == venue_id).first()
    if not pv:
        raise HTTPException(404, "Platform venue not found")
    db.delete(pv)
    db.commit()
    return {"ok": True, "id": venue_id}


@router.post("/{venue_id}/scrape")
async def scrape_platform_venue(venue_id: int, db: Session = Depends(get_db)):
    """Manually trigger event scraping for a specific platform venue."""
    pv = db.query(PlatformVenue).filter(PlatformVenue.id == venue_id).first()
    if not pv:
        raise HTTPException(404, "Platform venue not found")

    saved = await _scrape_one(pv, db)
    return {
        "venue": pv.name,
        "platform": pv.platform,
        "events_saved": saved,
        "last_scraped_at": str(pv.last_scraped_at),
    }
