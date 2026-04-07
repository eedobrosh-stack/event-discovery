from __future__ import annotations
from datetime import datetime, timedelta
from typing import Optional
from icalendar import Calendar, Event as ICSEvent

try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
except ImportError:  # Python < 3.9
    from backports.zoneinfo import ZoneInfo, ZoneInfoNotFoundError  # type: ignore


def _event_tz(event) -> Optional[ZoneInfo]:
    """Return a ZoneInfo for the event's venue timezone, or None (floating)."""
    tz_name = None
    venue = getattr(event, "venue", None)
    if venue:
        tz_name = venue.timezone
        if not tz_name:
            city = getattr(venue, "city", None)
            if city:
                tz_name = city.timezone
    if tz_name:
        try:
            return ZoneInfo(tz_name)
        except (ZoneInfoNotFoundError, Exception):
            pass
    return None


def _build_cal(name: Optional[str] = None, refresh_hours: Optional[int] = None) -> Calendar:
    cal = Calendar()
    cal.add("prodid", "-//Supercaly//EN")
    cal.add("version", "2.0")
    cal.add("calscale", "GREGORIAN")
    cal.add("method", "PUBLISH")
    if name:
        cal.add("x-wr-calname", name)
    if refresh_hours:
        cal.add("x-published-ttl", f"PT{refresh_hours}H")
        cal.add("refresh-interval;value=duration", f"PT{refresh_hours}H")
    return cal


def _add_events(cal: Calendar, events) -> None:
    for event in events:
        ics_event = ICSEvent()
        # Use DB primary key for UID — source_id can be None, causing collisions
        ics_event.add("uid", f"supercaly-{event.id}@supercaly.ly")
        ics_event.add("summary", event.name)

        tz = _event_tz(event)  # ZoneInfo or None (floating)

        # Build start datetime
        if event.start_time:
            parts = event.start_time.split(":")
            h, m = int(parts[0]), int(parts[1]) if len(parts) > 1 else 0
            dt_start = datetime(
                event.start_date.year, event.start_date.month, event.start_date.day,
                h, m,
                tzinfo=tz,
            )
            ics_event.add("dtstart", dt_start)
        else:
            dt_start = None
            ics_event.add("dtstart", event.start_date)

        # Build end datetime
        if event.end_date:
            if event.end_time:
                parts = event.end_time.split(":")
                h, m = int(parts[0]), int(parts[1]) if len(parts) > 1 else 0
                dt_end = datetime(
                    event.end_date.year, event.end_date.month, event.end_date.day,
                    h, m,
                    tzinfo=tz,
                )
                ics_event.add("dtend", dt_end)
            else:
                ics_event.add("dtend", event.end_date)
        elif dt_start:
            # Default 2-hour duration
            ics_event.add("dtend", dt_start + timedelta(hours=2))

        if event.venue_name:
            ics_event.add("location", event.venue_name)
        if event.purchase_link:
            ics_event.add("url", event.purchase_link)
        description_parts = []
        if event.artist_name:
            description_parts.append(f"Artist: {event.artist_name}")
        description_parts.append("Build your events calendar with Supercaly: https://superca.ly")
        ics_event.add("description", "\n\n".join(description_parts))

        cal.add_component(ics_event)


def generate_ics(events) -> bytes:
    cal = _build_cal()
    _add_events(cal, events)
    return cal.to_ical()


def generate_subscription_ics(events, name: str = "Supercaly Events") -> bytes:
    """Generate an ICS with subscription headers so calendar apps auto-refresh."""
    cal = _build_cal(name=name, refresh_hours=6)
    _add_events(cal, events)
    return cal.to_ical()
