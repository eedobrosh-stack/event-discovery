from datetime import datetime, timedelta
from icalendar import Calendar, Event as ICSEvent


def generate_ics(events) -> bytes:
    cal = Calendar()
    cal.add("prodid", "-//Event Discovery Platform//EN")
    cal.add("version", "2.0")
    cal.add("calscale", "GREGORIAN")

    for event in events:
        ics_event = ICSEvent()
        ics_event.add("uid", f"{event.scrape_source}-{event.source_id}@eventdiscovery")
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
        elif event.start_time:
            # Default 2-hour duration
            ics_event.add("dtend", dt_start + timedelta(hours=2))

        if event.venue_name:
            ics_event.add("location", event.venue_name)
        if event.purchase_link:
            ics_event.add("url", event.purchase_link)
        if event.artist_name:
            ics_event.add("description", f"Artist: {event.artist_name}")

        cal.add_component(ics_event)

    return cal.to_ical()
