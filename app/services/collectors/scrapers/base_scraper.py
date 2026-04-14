from __future__ import annotations

import re
from datetime import date, datetime
from typing import Optional, Tuple
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

try:
    from curl_cffi.requests import AsyncSession as _CffiSession
    _CFFI_OK = True
except ImportError:
    _CFFI_OK = False


USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

HEBREW_MONTHS = {
    "ינואר": 1, "פברואר": 2, "מרץ": 3, "מרס": 3, "אפריל": 4, "מאי": 5,
    "יוני": 6, "יולי": 7, "אוגוסט": 8, "ספטמבר": 9, "אוקטובר": 10,
    "נובמבר": 11, "דצמבר": 12,
}

ENGLISH_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}

ABBR_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

GENERIC_EXACT = {
    "various artists", "various", "untitled", "event", "show", "performance",
    "concert", "gig", "tickets", "details", "info", "more", "more info",
    "read more", "buy tickets", "get tickets", "book now", "reserve",
    "click here", "learn more", "see more", "view details", "view more",
    "coming soon", "to be announced", "tba", "tbd", "sold out",
    "home", "about", "contact", "menu", "search", "login", "register",
    "back", "next", "previous", "loading", "error", "close", "open",
    "narrow search", "all upcoming events", "all events", "filter",
    "see all", "view all", "show all", "load more", "reset",
    "subscribe", "sign up", "newsletter", "follow us",
}


BROWSER_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}


async def fetch_html(url: str, timeout: int = 30) -> Optional[str]:
    """Fetch HTML content from a URL.

    Uses curl_cffi (Chrome TLS impersonation) when available to bypass
    Cloudflare and WAF protection; falls back to httpx with full browser headers.
    """
    if _CFFI_OK:
        try:
            async with _CffiSession(impersonate="chrome120") as session:
                resp = await session.get(url, timeout=timeout)
                resp.raise_for_status()
                return resp.text
        except Exception:
            pass  # fall through to httpx
    try:
        async with httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            headers=BROWSER_HEADERS,
        ) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.text
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None


def is_generic_name(name: str) -> bool:
    """Check if an event name is generic/UI text that should be skipped."""
    if not name:
        return True
    name_lower = name.lower().strip()
    if len(name_lower) < 3:
        return True
    if name_lower in GENERIC_EXACT:
        return True
    # All words are generic
    words = re.split(r"[\s\-–—|]+", name_lower)
    if words and all(w.strip() in GENERIC_EXACT or len(w.strip()) < 2 for w in words if w.strip()):
        return True
    # URL patterns
    if "http" in name_lower or "www." in name_lower:
        return True
    # Pure numbers or dates
    if re.match(r"^\d+$", name_lower) or re.match(r"^\d{1,2}/\d{1,2}/\d{2,4}$", name_lower):
        return True
    return False


def extract_artist_from_name(full_name: str) -> Tuple[str, Optional[str]]:
    """Extract artist name from event name. Returns (event_name, artist_name)."""
    if not full_name:
        return full_name, None

    cleaned = full_name.strip()
    # Remove CTA suffixes
    cleaned = re.sub(r"\s*(buy tickets|get tickets)\s*$", "", cleaned, flags=re.IGNORECASE)

    patterns = [
        (r"^(.+?)\s*[-–—]\s*(.+)$", 1, 2),   # "Artist - Event"
        (r"^(.+?)\s*\|\s*(.+)$", 1, 2),        # "Artist | Event"
        (r"^(.+?)\s+(?:featuring|with)\s+(.+)$", 2, 1),  # "Event featuring Artist"
    ]

    for pattern, artist_idx, event_idx in patterns:
        m = re.match(pattern, cleaned, re.IGNORECASE)
        if m:
            artist = m.group(artist_idx).strip()
            event = m.group(event_idx).strip()
            if 2 < len(artist) < 100 and len(event) > 2 and not is_generic_name(artist):
                return event, artist

    return cleaned, None


def parse_datetime(date_text: str, time_text: str = "") -> Tuple[Optional[str], str, str]:
    """Parse date/time strings. Returns (start_date, start_time, end_time) or (None, ...)."""
    now = datetime.now()
    start_date = None
    start_time = "20:00"
    end_time = "22:30"

    if date_text:
        # ISO format: 2026-03-19
        iso_m = re.search(r"(\d{4}-\d{2}-\d{2})", date_text)
        if iso_m:
            start_date = iso_m.group(1)
        else:
            # US format: "Mar 7", "March 3", "Sat Mar 7, 2026"
            us_m = re.search(
                r"(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)?\s*"
                r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*"
                r"\s+(\d{1,2})(?:\s*,?\s*(\d{4}))?",
                date_text, re.IGNORECASE,
            )
            if us_m:
                month_key = us_m.group(1).lower()[:3]
                day = int(us_m.group(2))
                year = int(us_m.group(3)) if us_m.group(3) else now.year
                month = ABBR_MONTHS.get(month_key)
                if month:
                    start_date = f"{year}-{month:02d}-{day:02d}"
            else:
                # DD/MM/YYYY or DD.MM.YYYY
                num_m = re.search(r"(\d{1,2})[/\-.](\d{1,2})[/\-.](\d{2,4})", date_text)
                if num_m:
                    day, month, year = num_m.group(1), num_m.group(2), num_m.group(3)
                    if len(year) == 2:
                        year = f"20{year}"
                    start_date = f"{year}-{int(month):02d}-{int(day):02d}"

    # Parse time
    if time_text:
        time_m = re.search(r"(\d{1,2}):(\d{2})\s*(am|pm)?", time_text, re.IGNORECASE)
        if time_m:
            hours = int(time_m.group(1))
            minutes = time_m.group(2)
            ampm = time_m.group(3)
            if ampm and ampm.lower() == "pm" and hours < 12:
                hours += 12
            if ampm and ampm.lower() == "am" and hours == 12:
                hours = 0
            if 0 <= hours <= 23:
                start_time = f"{hours:02d}:{minutes}"

    # Filter past dates
    if start_date:
        try:
            event_date = date.fromisoformat(start_date)
            if event_date < date.today():
                return None, start_time, end_time
        except ValueError:
            return None, start_time, end_time

    return start_date, start_time, end_time


def parse_price(price_text: str) -> Optional[float]:
    """Extract price from text like '$25', '25.00', etc."""
    if not price_text:
        return None
    m = re.search(r"[\d,]+(?:\.\d{2})?", price_text)
    if m:
        try:
            return float(m.group(0).replace(",", ""))
        except ValueError:
            pass
    return None
