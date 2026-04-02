"""
Cameri Theatre (הקאמרי) collector — Israeli theatre (WordPress + FullCalendar).
Scrapes the shows calendar page which embeds all upcoming events in a JS variable.
No API key needed.
"""
from __future__ import annotations
import httpx
import re
import json
import logging
from datetime import date, datetime

from app.services.collectors.base import BaseCollector, RawEvent, safe_time, default_end_time

logger = logging.getLogger(__name__)

CALENDAR_URL = "https://www.cameri.co.il/%d7%9c%d7%95%d7%97-%d7%94%d7%95%d7%a4%d7%a2%d7%95%d7%aa/"
AJAX_URL = "https://www.cameri.co.il/wp-admin/admin-ajax.php"
TICKETS_BASE = "https://tickets.cameri.co.il/order/"


class CameriCollector(BaseCollector):

    @property
    def source_name(self) -> str:
        return "cameri"

    def is_configured(self) -> bool:
        return True  # No API key required

    async def collect(self, city_name: str, country_code: str = "IL", **kwargs) -> list[RawEvent]:
        # Cameri is in Tel Aviv only
        if city_name not in ("Tel Aviv", "Tel Aviv-Yafo"):
            return []

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8",
        }

        async with httpx.AsyncClient(timeout=30, follow_redirects=True, headers=headers) as client:
            resp = await client.get(CALENDAR_URL)
            if resp.status_code != 200:
                logger.warning(f"Cameri: calendar page returned {resp.status_code}")
                return []
            html = resp.text

        # Extract nonce for potential AJAX refetch (we may not need it since data is embedded)
        # Extract calendarEvents JS variable embedded in the page
        m = re.search(r'calendarEvents\s*=\s*(\[.+?\]);', html, re.DOTALL)
        if not m:
            logger.warning("Cameri: calendarEvents not found in page")
            return []

        try:
            raw_events = json.loads(m.group(1))
        except json.JSONDecodeError as e:
            logger.warning(f"Cameri: JSON parse error: {e}")
            return []

        today = date.today()
        events = []

        for ev in raw_events:
            start_str = ev.get("start", "")
            if not start_str:
                continue

            try:
                start_date = date.fromisoformat(start_str)
            except ValueError:
                continue

            if start_date < today:
                continue

            props = ev.get("extendedProps", {})
            show_name = props.get("show_name", "") or ""
            if not show_name or len(show_name.strip()) < 2:
                continue

            image_url = props.get("image_horizontal") or props.get("image")
            show_permalink = props.get("show_permalink", "")
            press_global_id = props.get("press_global_id", "")

            # Build ticket purchase link using press_global_id
            if press_global_id:
                purchase_link = f"{TICKETS_BASE}{press_global_id}"
            elif show_permalink:
                purchase_link = show_permalink
            else:
                purchase_link = "https://www.cameri.co.il"

            # Each event may have multiple time slots
            times = props.get("times", [])
            if times:
                for time_entry in times:
                    # time_entry: [show_id, "HH:MM", ""]
                    show_id = time_entry[0] if len(time_entry) > 0 else None
                    time_str = time_entry[1] if len(time_entry) > 1 else ""

                    start_time = None
                    if time_str and ":" in time_str:
                        t_parts = time_str.strip().split(":")
                        if len(t_parts) >= 2 and t_parts[0].isdigit() and t_parts[1].isdigit():
                            start_time = f"{int(t_parts[0]):02d}:{int(t_parts[1]):02d}"

                    end_date_val, end_time = default_end_time(start_time, start_date, None)

                    # Use individual show_id for purchase link if available
                    if show_id:
                        show_purchase_link = f"{TICKETS_BASE}{show_id}"
                    else:
                        show_purchase_link = purchase_link

                    source_id = f"cameri_{show_id}" if show_id else f"cameri_{press_global_id}_{start_str}"

                    events.append(RawEvent(
                        name=show_name,
                        start_date=start_date,
                        start_time=start_time,
                        end_date=end_date_val,
                        end_time=end_time,
                        price=None,
                        price_currency="ILS",
                        image_url=image_url,
                        purchase_link=show_purchase_link,
                        venue_name="הקאמרי - תיאטרון תל אביב",
                        venue_address="קפלן 2, תל אביב",
                        venue_city="Tel Aviv",
                        venue_country="Israel",
                        source="cameri",
                        source_id=source_id,
                        raw_categories=["Art"],
                    ))
            else:
                # No time entries — add one event with no time
                source_id = f"cameri_{press_global_id}_{start_str}" if press_global_id else f"cameri_{start_str}_{show_name[:20]}"
                events.append(RawEvent(
                    name=show_name,
                    start_date=start_date,
                    start_time=None,
                    end_date=None,
                    end_time=None,
                    price=None,
                    price_currency="ILS",
                    image_url=image_url,
                    purchase_link=purchase_link,
                    venue_name="הקאמרי - תיאטרון תל אביב",
                    venue_address="קפלן 2, תל אביב",
                    venue_city="Tel Aviv",
                    venue_country="Israel",
                    source="cameri",
                    source_id=source_id,
                    raw_categories=["Art"],
                ))

        logger.info(f"Cameri: found {len(events)} events")
        return events
