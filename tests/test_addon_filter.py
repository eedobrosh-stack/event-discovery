"""Ticketmaster add-on listing filter (Premium Seating / Parking / Suite
Reservation / VIP M&G Add-On ...) — predicate, collector drop, save-path
guard."""
from __future__ import annotations

from datetime import date, timedelta

import pytest

from app.models import Event
from app.services.collectors.addon_filter import is_ticket_addon, ticket_addon_reason
from app.services.collectors.api.ticketmaster import TicketmasterCollector
from app.services.collectors.base import RawEvent
from app.services.collectors.registry import CollectorRegistry

TOMORROW = date.today() + timedelta(days=1)

# Real snapshot titles, one per class.
ADDONS = [
    ("Zac Brown Band Premium Seating", "premium_seating"),
    ("Zac Brown Band Bar Rails", "bar_rails"),
    ("Zac Brown Band Premium Perch", "premium_seating"),
    ("Atlanta Braves v. Philadelphia Phillies * Premium Seating *", "premium_seating"),
    ("Suites - JOURNEY - Final Frontier Tour - Xfinity Mobile Arena Premium Seating", "premium_seating"),
    ("Kehlani | Venue Premium Packages", "premium_seating"),
    ("Bill Bailey - Venue Premium Tickets", "premium_seating"),
    ("Parking permit Masego - Parkeerkaarten Arenapoort", "parking"),
    ("Garage - 1180 Peachtree St: Celtic Woman - WAC Parking", "parking"),
    ("PSS Parking - Pitbull", "parking"),
    ("C_Parkeerkaart TAFKAL 2026 - Ahoy Parkeren", "parking"),
    ("Deep Purple | VIP Package", "vip_addon"),
    ("VIP Tailgate: Broncos v Chargers", "vip_addon"),
    ("Michael Blaustein VIP M&G Add-on", "vip_addon"),
    ("Ich Troje - Meet&Greet UPGRADE", "upgrade"),
    ("Marti Pellow - M&G Add-On MATINEE *DOES NOT INCLUDE EVENT TICKET*", "meet_greet"),
    ("KALEO | Vinyl Room Upgrade (TICKET NOT INCLUDED) - Vinyl Room Access", "upgrade"),
    ("Eiffel Tower Bridge Lock Upgrade", "upgrade"),
    ("The Place To Be prior to Dusty Slay - The Place to Be Lounge Access", "lounge_access"),
    ("Vinyl Room Access – Deep Purple (concert ticket not included)", "lounge_access"),
    ("Metallica - Suite Reservation", "suite"),
    ("Share a Suite - Belfast Giants vs Sheffield Steelers", "suite"),
    ("ICON Suites at Spark Arena - Noah Kahan", "suite"),
    ("2027 Rolex 24 Friday Suite Admission", "suite"),
    ("Westlife: Venue Premium Tickets - Utilita Hospitality Experience", "premium_seating"),
    ("Dinner Package - Buzzcocks - Not a Concert Ticket - Brooklyn Bowl Dinner Package", "package"),
    ("Jack White | Official Fontainebleau Ticket + Hotel Packages", "package"),
    ("Foreigner Ticket + Hotel Deals", "package"),
    ("Reserved Lawn- BABYMETAL - NOT A CONCERT TICKET - Live Nation Reserved Lawn", "not_a_ticket"),
    ("Countess Luann de Lesseps VIP Photo Opp - LN Artist Upsell", "vip_addon"),
    ("Bryson Tiller | Wine & Dine (TICKET NOT INCLUDED)", "not_a_ticket"),
    ("Mt. Joy - Unsigned Poster Add-on", "add_on"),
    ("2026 Premium Season Tickets", "season_ticket"),
    ("Seattle Seahawks Season Ticket Notification List", "season_ticket"),
    ("2026 Inter Miami Season Ticket Deposit", "season_ticket"),
    ("2026 Tiger-Cats 10-Ticket Flex Pack", "season_ticket"),
    ("2026 NASCAR Tailgate Tow Pass", "misc_pass"),
    ("President | Early entry and merchandise experience", "fast_lane"),
]

# Real shows that contain a tempting keyword — must NOT be dropped.
REAL_SHOWS = [
    "Zac Brown Band w/ Brothers Osborne",
    "Parking Lot Party with The Lone Bellow",
    "Hospitality",                                   # the band
    "Tchaikovsky: The Nutcracker Suite",             # classical form
    "Chinese Laundry Invites HEADHUNTERS [GUY CONTACT & SOLAR SUITE] + AMOTIK",
    "Clock-Out Lounge Presents: Jeffrey Silverstein w/ Forest Ray",
    "The Paddington Bear Experience",
    "Courtside Cardshow Miami Three Day VIP Pass 10am Entry",
    "Champions Tailgate: 2027 Rose Bowl Game",
    "Sleep Theory Australian Tour | Melbourne | VENUE UPGRADE - NOW AT FESTIVAL HALL",
    "1970 International Theater VIP Tour",
    "Sabaton",
    "Ultimate 4D Experience",
    "Concert at Parken - Coldplay",                  # Copenhagen stadium, not parking
    "The Suite Life Reunion Panel",
    "Upgraded Platinum Alpha",
    "Premium",                                       # bare word
    "Package Tour: Stevie Wonder & Friends",
]


@pytest.mark.parametrize("title,label", ADDONS)
def test_addon_titles_are_flagged(title, label):
    assert ticket_addon_reason(title) == label
    assert is_ticket_addon(title)


@pytest.mark.parametrize("title", REAL_SHOWS)
def test_real_shows_are_not_flagged(title):
    assert ticket_addon_reason(title) is None, title


def test_empty_and_none_are_safe():
    assert ticket_addon_reason(None) is None
    assert ticket_addon_reason("") is None
    assert ticket_addon_reason("   ") is None


def _tm_payload(name: str, attraction: str | None = None) -> dict:
    ev = {
        "id": "tm-1",
        "name": name,
        "url": "https://tm.example/1",
        "dates": {"start": {"localDate": TOMORROW.isoformat(), "localTime": "20:00:00"}},
        "_embedded": {"venues": [{"name": "Denny Sanford PREMIER Center"}]},
        "classifications": [],
    }
    if attraction:
        ev["_embedded"]["attractions"] = [{"name": attraction}]
    return ev


def test_ticketmaster_transform_drops_addons_and_keeps_shows():
    tm = TicketmasterCollector.__new__(TicketmasterCollector)
    assert tm._transform(_tm_payload("Zac Brown Band Premium Seating")) is None
    assert tm._transform(_tm_payload("Zac Brown Band Bar Rails")) is None
    # The upsell attraction name is appended to the title — must still drop.
    assert tm._transform(_tm_payload("Signed Poster - Brett Goldstein", "LN Artist Upsell")) is None
    real = tm._transform(_tm_payload("Zac Brown Band w/ Brothers Osborne"))
    assert real is not None and real.name == "Zac Brown Band w/ Brothers Osborne"


def test_save_events_guard_skips_ticketmaster_addons_only(db, city):
    reg = CollectorRegistry()
    raws = [
        RawEvent(name="Zac Brown Band w/ Brothers Osborne", start_date=TOMORROW,
                 venue_name="Denny Sanford PREMIER Center", source="ticketmaster", source_id="a"),
        RawEvent(name="Zac Brown Band Premium Seating", start_date=TOMORROW,
                 venue_name="Denny Sanford PREMIER Center", source="ticketmaster", source_id="b"),
        RawEvent(name="Zac Brown Band Bar Rails", start_date=TOMORROW,
                 venue_name="Denny Sanford PREMIER Center", source="ticketmaster", source_id="c"),
        # Same keyword from a non-Ticketmaster source is left alone: the
        # filter is scoped to the source whose API produces the SKUs.
        RawEvent(name="Parking Day Block Party", start_date=TOMORROW,
                 venue_name="Main St", source="meetup", source_id="d"),
    ]
    saved = reg._save_events(raws, city, db)
    names = sorted(n for (n,) in db.query(Event.name).all())
    assert saved == 2
    assert names == ["Parking Day Block Party", "Zac Brown Band w/ Brothers Osborne"]
