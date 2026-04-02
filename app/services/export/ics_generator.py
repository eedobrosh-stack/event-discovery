from __future__ import annotations
from datetime import datetime, timedelta
from typing import Optional
from icalendar import Calendar, Event as ICSEvent


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
        ics_event.add("uid", f"{event.scrape_source}-{event.source_id}@supercaly")
        ics_event.add("summary", event.name)

        # Build start datetime
        if event.start_time:
            parts = event.start_time.split(":")
            h, m = int(parts[0]), int(parts[1]) if len(parts) > 1 else 0
            dt_start = datetime(
                event.start_date.year, event.start_date.month, event.start_date.day,
                h, m,
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
        if event.artist_name:
            ics_event.add("description", f"Artist: {event.artist_name}")

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
