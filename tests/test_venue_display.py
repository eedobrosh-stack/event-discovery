"""One venue, one name: display reads Venue.name, not the scraped string."""
from types import SimpleNamespace as NS

from app.services.venue_display import display_venue_city, display_venue_name


def test_display_prefers_linked_venue_name():
    ev = NS(venue_name="מועדון שבלול תל אביב", venue=NS(name="שבלול ג'אז"))
    assert display_venue_name(ev) == "שבלול ג'אז"
    assert display_venue_name(NS(venue_name="Online", venue=None)) == "Online"


def test_israeli_city_is_canonical():
    v = NS(physical_city="תל אביב-יפו", physical_country="Israel", city=NS(name="Tel Aviv", country="Israel"))
    assert display_venue_city(v) == "Tel Aviv"
    v = NS(physical_city="Brooklyn", physical_country="United States", city=NS(name="New York", country="United States"))
    assert display_venue_city(v) == "Brooklyn"
