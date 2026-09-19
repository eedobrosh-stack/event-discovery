"""Ticketmaster "add-on" listing filter.

Ticketmaster's Discovery API returns upsell SKUs as first-class events:
"Zac Brown Band Premium Seating", "Zac Brown Band Bar Rails", "Parking
permit Masego", "Metallica - Suite Reservation", "Hieroglyphics - VIP
M&G Add-On - NOT A CONCERT TICKET". They share date + venue with the
real show, so every one of them lands in the catalog as a duplicate-
looking row with a nonsense title (2026-09-18 QA report: ~1,441
duplicate groups were exactly this class).

`ticket_addon_reason(name)` returns a short class label when a title is
an upsell, else None. Patterns are deliberately conservative — each one
was checked against the Ticketmaster titles in the prod snapshot — and
biased toward *missing* an add-on rather than dropping a real show:

  * bare words that are also show titles / band names / venue names are
    NOT matched on their own ("Hospitality" is a band, "Suite" is a
    classical form, "Lounge" and "Experience" name venues and
    attractions, "Tailgate" parties are real events);
  * "Parking Lot Party"-style titles are explicitly allow-listed.

Used by the ticketmaster collector (`_transform` drops the row) and as a
belt-and-braces guard in `CollectorRegistry._save_events` for
source == "ticketmaster" so admin/scrape and any future TM entry point
get the same treatment. `scripts/remove_ticketmaster_addons.py` applies
the same predicate retroactively.
"""
from __future__ import annotations

import re

# Titles that look like an add-on keyword but are real shows. Checked
# first; a hit here short-circuits every ADDON_PATTERNS check.
SAFE_PATTERNS: tuple[re.Pattern, ...] = (
    re.compile(r"\bparking lot (party|concert|series|show|sessions?|live)\b", re.I),
    # "X Tour | VENUE UPGRADE - NOW AT FESTIVAL HALL": a real show whose
    # room got upsized, not an upsell.
    re.compile(r"\bvenue upgrade\b", re.I),
)

# (class label, compiled regex). Order matters only for the label
# reported — the first hit wins.
ADDON_PATTERNS: tuple[tuple[str, re.Pattern], ...] = tuple(
    (label, re.compile(rx, re.I)) for label, rx in (
        ("premium_seating",
         r"\bpremium (seating|perch|packages?|tickets?|upgrades?|suites?|viewing)\b"),
        ("bar_rails", r"\bbar rails?\b"),
        ("parking",
         r"\bparking\b|\bparkeer|\bparkeren\b|\bparkplatz|\bparkticket|\bparkschein"),
        ("vip_addon",
         r"\bvip (packages?|upgrades?|m&g|m & g|meet|photo|tailgate|pre-?show|add-?ons?|upsell|q&a|hang)\b"),
        ("meet_greet", r"\bmeet (&|and) greet\b|\bm&g\b|\bm & g\b"),
        ("upgrade", r"\bupgrades?\b"),
        ("lounge_access", r"\b(lounge|club) access\b|\bvinyl room (access|upgrades?)\b"),
        ("suite",
         r"\bsuite (reservation|admission|packages?|tickets?)\b|\bshare a suite\b|"
         r"^suites? - |\bticketmaster suite\b|\bclub level suite\b|\bsuites at\b"),
        ("fast_lane", r"\bfast lane\b|\bearly entry\b"),
        ("hospitality", r"\bhospitality (experience|packages?|tickets?|suite|lounge)\b"),
        ("package",
         r"\b(dinner|hotel|venue premium) packages?\b|\bticket (\+|&|and) hotel\b"),
        ("not_a_ticket",
         r"\bnot a (concert|show|event) ticket\b|\bticket not included\b|"
         r"\bno ticket included\b|\bdoes not include (a |an |the )?(\w+ )?ticket\b|"
         r"\bupsell\b|\bboleto[^|]{0,30}no incl"),
        ("add_on", r"\badd-?ons?\b"),
        ("season_ticket",
         r"\bseason tickets?\b|\bticket deposit\b|\bnotification list\b|"
         r"\bflex pack\b|\bmini plan\b"),
        ("misc_pass",
         r"\bphoto opp?\b|\btow pass\b|\b(un)?signed poster\b|\bdinner credit\b|"
         r"\bcamping (pass|permit|ticket)\b|\bshuttle (pass|bus)\b"),
    )
)


def ticket_addon_reason(name: str | None) -> str | None:
    """Return the add-on class label when ``name`` is an upsell listing,
    otherwise None. Case-insensitive; whitespace-tolerant."""
    if not name:
        return None
    text = " ".join(name.split())
    for safe in SAFE_PATTERNS:
        if safe.search(text):
            return None
    for label, rx in ADDON_PATTERNS:
        if rx.search(text):
            return label
    return None


def is_ticket_addon(name: str | None) -> bool:
    return ticket_addon_reason(name) is not None
