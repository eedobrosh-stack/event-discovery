"""
Re-categorize all events using a two-pass strategy:

  Pass 1 – Performer lookup (high confidence)
    If event.artist_name is set and the artist is in the performers table,
    use their known category + event_type.

  Pass 2 – Keyword matching (fallback)
    For events with no artist name (or artist not in performers table),
    scan event name + description against KEYWORD_INDEX.

Run:
    python3 scripts/categorize_events.py
    python3 scripts/categorize_events.py --dry-run   # show stats, don't commit
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database import SessionLocal
from app.models import Event, EventType, Performer
from app.services.performer_lookup import normalize

# ─────────────────────────────────────────────────────────────────────────────
# Keyword fallback map  (event_type_name → list of trigger phrases)
# Only used when there is NO artist name (or artist not yet looked up).
# ─────────────────────────────────────────────────────────────────────────────
KEYWORD_INDEX: dict[str, list[str]] = {
    # ── Music ──
    "Jazz Concert":                 ["jazz", "blues", "bebop", "swing", "coltrane"],
    "Hip-Hop / Rap Concert":        ["hip-hop", "hip hop", "rap", "trap", "drill"],
    "Rock Concert":                 ["rock concert", "punk", "metal", "grunge"],
    "Pop Concert":                  ["pop concert"],
    "Electronic / DJ Set":          ["dj set", "dj ", " dj", "electronic", "edm", "techno",
                                     "house music", "rave", "club night", "dnb",
                                     "drum and bass", "trance", "psytrance", "b2b",
                                     "all-night long", "warehouse party"],
    "R&B / Soul Concert":           ["r&b", " soul ", "neo-soul", "motown"],
    "Country Concert":              ["country music", "bluegrass", "americana"],
    "Latin Concert":                ["salsa", "reggaeton", "cumbia", "bachata", "latin music"],
    "Reggae / Calypso Concert":     ["reggae", "calypso", "dancehall", "ska"],
    "Gospel Concert":               ["gospel", "christian music", "worship"],
    "Symphony Orchestral Performances": ["symphony", "philharmonic", "orchestral"],
    "Fully Staged Opera":           ["opera ", " opera"],
    "String Quartets":              ["string quartet", "chamber music"],
    "Baroque Orchestras":           ["baroque", "early music"],
    # ── Comedy ──
    "Open Mic Nights":              ["open mic", "open-mic"],
    "Comedy Club Headliners":       ["stand-up", "standup", "stand up comedy", "comedy club"],
    "Short-Form Improv":            ["improv", "short-form improv"],
    "Sketch Comedy Performances":   ["sketch comedy", "sketch show"],
    "One-Person Shows":             ["one-person show", "one-man show", "one-woman show"],
    # ── Dance ──
    "Classical Ballet":             ["ballet", "nutcracker", "swan lake"],
    "Contemporary Ballet":          ["contemporary ballet"],
    "Modern Dance":                 ["modern dance", "contemporary dance"],
    "Flamenco":                     ["flamenco"],
    "Irish Step Dance":             ["irish dance", "riverdance"],
    # ── Theatre / Art ──
    "Broadway Show":                ["broadway", "off-broadway", "west end", "musical theatre",
                                     "musical theater"],
    "Play / Drama":                 [" play ", "theatre", "theater", "staged reading", "drama"],
    "Special Museum Exhibitions":   ["museum exhibition", "museum exhibit"],
    "Interactive Art Installations":["installation", "immersive art"],
    "Art Fairs":                    ["art fair"],
    # ── Film ──
    "Red Carpet Premieres":         ["premiere", "red carpet"],
    "International Film Festivals": ["film festival"],
    "Art House Cinema Screenings":  ["arthouse", "art house cinema", "indie film"],
    "Community Film Screenings":    ["film screening", "outdoor film", "movie screening"],
    # ── Food & Drink ──
    "Wine Tastings":                ["wine tasting", "wine pairing"],
    "Craft Beer Events":            ["craft beer", "beer festival", "brewery event"],
    "Street Food Fairs":            ["street food", "food fair", "food market"],
    "Farmers Markets":              ["farmers market", "farmer's market"],
    # ── Fitness ──
    "Marathons":                    ["marathon", "half marathon", " 5k", " 10k", "fun run"],
    "Yoga Retreats":                ["yoga class", "pilates", "meditation retreat"],
    "Cycling Races":                ["cycling race", "bike race"],
    # ── Technology ──
    "AI Tech Conferences":          ["ai conference", "machine learning", "artificial intelligence"],
    "Startup Showcases":            ["startup", "demo day", "pitch night", "hackathon"],
    # ── Literature ──
    "Author Talks":                 ["author talk", "book talk", "author reading"],
    "Poetry Slams":                 ["poetry slam", "spoken word"],
    "Book Launches":                ["book launch", "book signing"],
    # ── Charity ──
    "Formal Fundraising Galas":     ["fundraising gala", "charity gala", "benefit gala"],
    "Benefit Concerts":             ["benefit concert", "charity concert"],
    # ── Gaming ──
    "eSports Tournaments":          ["esports", "e-sports", "gaming tournament"],
    # ── Outdoor ──
    "Hiking Meetups":               ["hiking", " hike ", "trail walk", "nature walk"],
    # ── Festival ──
    "Genre-Specific Music Festivals":["music festival"],
    "Pride Parades":                ["pride parade", "lgbtq parade"],
    "Holiday Parades":              ["holiday parade", "thanksgiving parade"],
    "Cultural Celebrations":        ["cultural festival", "cultural celebration"],
}

# Sort by longest keyword first (prevents "rock" beating "rock climbing")
_sorted_kw_index = sorted(
    KEYWORD_INDEX.items(),
    key=lambda x: -max(len(k) for k in x[1]),
)


def keyword_match(text: str):
    """Return first matching EventType name or None."""
    tl = text.lower()
    for type_name, kws in _sorted_kw_index:
        if any(kw in tl for kw in kws):
            return type_name
    return None


def run(dry_run: bool = False):
    db = SessionLocal()
    try:
        # Pre-load EventType map: name → EventType object
        et_by_name: dict[str, EventType] = {
            et.name: et for et in db.query(EventType).all()
        }

        # Pre-load performers: normalized_name → (category, event_type_name)
        performer_map = {
            p.normalized_name: (p.category, p.event_type_name)
            for p in db.query(Performer).all()
            if p.event_type_name
        }

        print(f"Loaded {len(et_by_name)} event types, {len(performer_map)} performers")

        events = db.query(Event).all()

        stats = {
            "performer_hit":     0,
            "keyword_hit":       0,
            "music_default":     0,  # has artist but no performer data → safe "Music" default
            "no_match":          0,
        }

        for event in events:
            assigned_type = None

            # ── Pass 1: performer lookup ──────────────────────────────────
            if event.artist_name and event.artist_name.strip():
                norm = normalize(event.artist_name.strip())
                if norm in performer_map:
                    _, type_name = performer_map[norm]
                    et = et_by_name.get(type_name)
                    if et:
                        assigned_type = et
                        stats["performer_hit"] += 1

                if assigned_type is None:
                    # Artist exists but not yet in performers table.
                    # Default to "Concert" (generic Music) — infinitely better than "Art".
                    et = et_by_name.get("Concert") or et_by_name.get("Pop Concert")
                    if et:
                        assigned_type = et
                        stats["music_default"] += 1

            # ── Pass 2: keyword matching ─────────────────────────────────
            if assigned_type is None:
                search_text = " ".join(filter(None, [
                    event.name or "",
                    event.venue_name or "",
                    event.description or "",
                ]))
                type_name = keyword_match(search_text)
                if type_name:
                    et = et_by_name.get(type_name)
                    if et:
                        assigned_type = et
                        stats["keyword_hit"] += 1

            # ── Apply ────────────────────────────────────────────────────
            if assigned_type:
                if not dry_run:
                    event.event_types = [assigned_type]
            else:
                stats["no_match"] += 1
                # Leave existing assignment untouched rather than wiping to Art

        if not dry_run:
            db.commit()

        total = sum(stats.values())
        print("\n── Categorization results ──────────────────────────────")
        print(f"  Performer lookup (DB match):  {stats['performer_hit']:>6,}")
        print(f"  Artist → Music default:       {stats['music_default']:>6,}")
        print(f"  Keyword match (no artist):    {stats['keyword_hit']:>6,}")
        print(f"  No match (unchanged):         {stats['no_match']:>6,}")
        print(f"  Total events:                 {total:>6,}")
        if dry_run:
            print("\n  [DRY RUN — nothing committed]")

    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Show statistics without writing to the database")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
