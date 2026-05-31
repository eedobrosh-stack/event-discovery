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
from app.models import Event, EventType, Performer, EventTheme
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
    # ── Conference (the format) ──
    # Renamed from 'Tech Conference' on 2026-05-31 — the type name
    # should describe FORMAT, not TOPIC (the 5-conference-types-bug
    # we'd just finished unwinding). Tech / Psychology / Medicine /
    # etc. now live entirely in the theme dimension. Keywords here
    # drive only the event_type assignment; theme_match (below)
    # catches the topic separately.
    "Conference":                   ["tech conference", "ai conference", "ai summit",
                                     "cybersecurity conference", "security summit",
                                     "machine learning", "artificial intelligence",
                                     "startup conf", "startup summit", "demo day",
                                     "pitch night", "hackathon",
                                     "consumer electronics",
                                     # Generic conference-shape signals — last so
                                     # specific patterns above win first.
                                     "conference", "summit", "symposium", "congress",
                                     "convention"],
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


# ─────────────────────────────────────────────────────────────────────────────
# Theme classifier (orthogonal to event_type — see app/models/theme.py).
#
# Themes are CONTENT classification; an event can carry multiple themes
# (e.g. an "AI in FinTech" conference would match both AI and FinTech).
# Distinct from event_type assignment, which picks exactly one type per
# event.
#
# Keyword sets are conservative — broad terms like "ai" alone would
# trigger on every "Saturday at AI Bar" or "AI Brothers" event; the
# keys here require disambiguating context ("ai conference", "ai
# summit", "machine learning") to avoid false positives.
# ─────────────────────────────────────────────────────────────────────────────
THEME_KEYWORDS: dict[str, list[str]] = {
    "AI":                   ["ai conference", "ai summit", "artificial intelligence",
                             "machine learning", "deep learning", "genai", "generative ai",
                             "neural network", "data science", "llm summit", "ml summit"],
    "Cybersecurity":        ["cybersecurity", "cyber security", "infosec", "info-sec",
                             "ethical hacking", "pentest", "threat intel", "zero trust",
                             "security conference", "security summit"],
    "Startup":              ["startup conf", "startup summit", "founders conf",
                             "founders summit", "demo day", "pitch night", "venture capital",
                             "vc summit", "accelerator demo", "y combinator",
                             "tech startup", "startup pitch"],
    "Crypto":               ["crypto", "blockchain", "web3", "web 3.0", "nft summit",
                             "bitcoin conference", "ethereum conference", "defi summit",
                             "consensus 2", "token2049"],
    "Consumer Electronics": ["consumer electronics", "ces 20", "ces 21", "ces 22", "ces 23",
                             "ces 24", "ces 25", "ces 26",
                             "gadget expo", "iot expo", "electronics show"],
    "DevOps":               ["devops", "kubecon", "kubernetes conference",
                             "site reliability", "ci/cd conf", "platform engineering",
                             "cloud native conference", "sre conference"],
    "FinTech":              ["fintech", "fin-tech", "payments summit", "banking summit",
                             "open banking", "regtech", "neobank", "money 20/20"],
    "Healthcare":           ["healthtech", "health-tech", "medtech", "med-tech",
                             "medical conference", "biotech conference", "life sciences summit",
                             "pharma summit", "digital health"],
    "Marketing":            ["marketing summit", "marketing conference", "growth hacking",
                             "growth marketing", "seo conference", "advertising week",
                             "cmo summit", "content marketing world"],
    "Career":               ["career fair", "job fair", "career expo", "recruiting expo",
                             "professional networking", "hiring summit"],
    # 2026-05-31 — broader topical themes covering non-tech conferences.
    # Each keyword set requires disambiguating context (e.g. "psychology
    # conference", "psychology summit") rather than the bare topic
    # word, to avoid false positives on event names that happen to
    # contain a topic substring.
    "Psychology":           ["psychology conference", "psychology summit",
                             "psychology congress", "psychological association",
                             "occupational health psychology", "clinical psychology",
                             "positive psychology", "neuropsychology"],
    "Education":            ["education conference", "education summit",
                             "edtech conference", "edtech summit", "k12 conference",
                             "k-12 education", "higher education conference",
                             "teaching conference", "pedagogical congress",
                             "academic conference"],
    "Mental Health":        ["mental health conference", "mental health summit",
                             "psychiatry conference", "psychiatry summit",
                             "psychotherapy conference", "wellbeing conference",
                             "well-being forum"],
    "Medicine":             ["medicine conference", "medical conference",
                             "medical congress", "clinical conference",
                             "clinical congress", "cardiology conference",
                             "oncology conference", "pediatrics conference",
                             "surgery conference", "radiology conference"],
    "Pharmaceutical":       ["pharmaceutical conference", "pharma conference",
                             "pharma summit", "drug development summit",
                             "clinical trials summit"],
    "Sustainability":       ["sustainability conference", "sustainability summit",
                             "esg conference", "esg summit", "circular economy"],
    "Climate":              ["climate conference", "climate summit", "cop summit",
                             "climate tech conference", "climate action forum"],
    "Energy":               ["energy conference", "energy summit",
                             "renewable energy summit", "solar conference",
                             "wind energy summit", "oil & gas conference",
                             "utilities conference"],
    "Real Estate":          ["real estate conference", "real estate summit",
                             "proptech conference", "proptech summit",
                             "commercial real estate forum"],
    "Legal":                ["legal conference", "law conference", "legaltech",
                             "law firm summit", "in-house counsel forum",
                             "litigation summit"],
    "Compliance":           ["compliance conference", "compliance summit",
                             "risk & compliance", "aml conference", "gdpr summit",
                             "audit conference"],
    "Manufacturing":        ["manufacturing conference", "manufacturing summit",
                             "industry 4.0 conference", "smart manufacturing",
                             "factory automation summit"],
    "Retail":               ["retail conference", "retail summit", "shoptalk",
                             "ecommerce summit", "nrf big show", "retail innovation"],
    "Media":                ["media conference", "media summit",
                             "broadcasting conference", "journalism conference",
                             "advertising week", "creator economy summit"],
    "Hospitality":          ["hospitality conference", "hospitality summit",
                             "travel conference", "travel summit", "hotel investment",
                             "tourism conference"],
    "Logistics":            ["logistics conference", "logistics summit",
                             "supply chain conference", "supply chain summit",
                             "freight conference", "warehouse conference"],
    "Government":           ["government conference", "government summit",
                             "public sector conference", "policy summit",
                             "gov tech summit", "civic tech conference"],
    "Architecture":         ["architecture conference", "architecture summit",
                             "design conference", "design summit", "aia conference",
                             "urban planning conference"],
    "Agriculture":          ["agriculture conference", "agriculture summit",
                             "agtech conference", "agtech summit", "farming conference",
                             "food systems conference"],
    "Insurance":            ["insurance conference", "insurance summit",
                             "insurtech conference", "insurtech summit",
                             "underwriting summit"],
}

_sorted_theme_kw = sorted(
    THEME_KEYWORDS.items(),
    key=lambda x: -max(len(k) for k in x[1]),
)


def theme_match(text: str) -> list[str]:
    """Return the list of themes whose keywords appear in `text`.

    Unlike keyword_match, returns ALL matches (events can carry multiple
    themes — an "AI in FinTech" conference matches both). Order is by
    longest-keyword-first sort, which corresponds roughly to "most
    specific theme first" — not load-bearing for correctness but nice
    for the audit log.
    """
    tl = text.lower()
    out: list[str] = []
    for theme, kws in _sorted_theme_kw:
        if any(kw in tl for kw in kws):
            out.append(theme)
    return out


def _classify(event, performer_map, et_by_name):
    """Pick the best EventType for `event`. Returns (et_or_None, stats_key).

    Two-pass, identical to the original `run()` body — factored out so
    both the manual full-table script AND the nightly incremental cron
    use the exact same classifier.
    """
    # ── Pass 1: performer lookup ──────────────────────────────────
    if event.artist_name and event.artist_name.strip():
        norm = normalize(event.artist_name.strip())
        if norm in performer_map:
            _, type_name = performer_map[norm]
            et = et_by_name.get(type_name)
            if et:
                return et, "performer_hit"

    # ── Pass 2: keyword matching on event-side text ───────────────
    # MUST come before the music_default fallback below — the 2026-05-31
    # bug: LLM-extracted conference events like "European Academy of
    # Occupational Health Psychology Conference" carry artist_name='Jari
    # Hakanen, Sabine Sonnentag' (speaker names from JSON-LD), which
    # pre-fix immediately routed them to music_default ('Concert') and
    # skipped keyword classification entirely. Surfacing keyword_match
    # first lets the 'conference' / 'summit' / 'symposium' patterns
    # win over the artist-implies-music heuristic.
    search_text = " ".join(filter(None, [
        event.name or "",
        event.venue_name or "",
        event.description or "",
    ]))
    type_name = keyword_match(search_text)
    if type_name:
        et = et_by_name.get(type_name)
        if et:
            return et, "keyword_hit"

    # ── Pass 3: artist exists but unknown → safe Music default ────
    # Only fires when no keyword matched, so non-music events with
    # speaker-names-as-artist_name (conferences, lectures, workshops)
    # are no longer caught here.
    if event.artist_name and event.artist_name.strip():
        et = et_by_name.get("Concert") or et_by_name.get("Pop Concert")
        if et:
            return et, "music_default"

    return None, "no_match"


def run_incremental(*, hours_back: int = 48, dry_run: bool = False) -> dict:
    """Non-destructive cron version: only fill gaps on recently-created
    events that have NO event_types assignment yet.

    Why this exists separately from `run()`
    ---------------------------------------
    The full-table `run()` does `event.event_types = [assigned_type]`,
    which wipes any pre-existing assignment (manual fixes, collector
    overrides, etc.). That's fine as a one-off but lethal as a cron.

    This entry point:
      * Filters to events created in the last `hours_back` (default 48h
        — enough buffer that a missed schedule tick on a Render restart
        doesn't leave events orphaned).
      * Skips any event that already has ≥1 event_type association.
      * Only writes when the classifier returns a non-None EventType,
        so "no_match" events stay genuinely empty (for keyword-hit
        retries on a future run after richer data lands).

    Designed to be called from app.scheduler.jobs at hourly cadence.
    Returns the stats dict so the caller can log into ScanLog.notes.
    """
    from datetime import datetime, timedelta
    from sqlalchemy import not_, exists, select
    from app.models import event_event_types as _eet

    db = SessionLocal()
    try:
        et_by_name = {et.name: et for et in db.query(EventType).all()}
        performer_map = {
            p.normalized_name: (p.category, p.event_type_name)
            for p in db.query(Performer).all()
            if p.event_type_name
        }

        cutoff = datetime.utcnow() - timedelta(hours=hours_back)
        # Events with NO event_type association, created within the window.
        # NOT EXISTS over the m2m is cleaner than outerjoin + NULL filter
        # for SQLite + avoids dragging duplicate rows into Python.
        no_et = ~exists(select(1).where(_eet.c.event_id == Event.id))
        pending = (
            db.query(Event)
            .filter(Event.created_at > cutoff, no_et)
            .all()
        )

        stats = {"performer_hit": 0, "music_default": 0, "keyword_hit": 0,
                 "no_match": 0, "themes_assigned": 0}
        applied = 0
        for ev in pending:
            assigned, key = _classify(ev, performer_map, et_by_name)
            stats[key] += 1
            if assigned and not dry_run:
                ev.event_types = [assigned]
                applied += 1
                # Speaker names extracted into artist_name by the
                # JSON-LD performer-field pass are misleading for
                # conferences — the "artist" of a conference is the
                # conference itself, not its keynote(s). Clear the
                # field at categorize-time so the column in the
                # results table renders empty for conferences.
                # (2026-05-31 — EAOHP case: 'Jari Hakanen, Sabine
                # Sonnentag' shouldn't render as the event's artist.)
                if assigned.name == "Conference" and ev.artist_name:
                    ev.artist_name = None

            # Theme assignment is orthogonal to event_type — runs on
            # every pending event regardless of whether _classify
            # picked a type. Themes are sparse (most events match 0);
            # idempotent because we only insert (event_id, theme) pairs
            # that don't already exist.
            if not dry_run:
                themes = theme_match(" ".join([ev.name or "", ev.description or ""]))
                if themes:
                    existing = {
                        et.theme_name for et in
                        db.query(EventTheme).filter(EventTheme.event_id == ev.id).all()
                    }
                    for theme in themes:
                        if theme not in existing:
                            db.add(EventTheme(event_id=ev.id, theme_name=theme))
                            stats["themes_assigned"] += 1

        if not dry_run:
            db.commit()
        stats["scanned"] = len(pending)
        stats["applied"] = applied
        return stats
    finally:
        db.close()


def run(dry_run: bool = False):
    """Manual full-table re-categorize. Destructive (overwrites existing
    event_types on every event). Kept for the one-off CLI workflow —
    use `run_incremental` from any automated path."""
    db = SessionLocal()
    try:
        et_by_name: dict[str, EventType] = {
            et.name: et for et in db.query(EventType).all()
        }
        performer_map = {
            p.normalized_name: (p.category, p.event_type_name)
            for p in db.query(Performer).all()
            if p.event_type_name
        }
        print(f"Loaded {len(et_by_name)} event types, {len(performer_map)} performers")

        events = db.query(Event).all()
        stats = {"performer_hit": 0, "keyword_hit": 0, "music_default": 0,
                 "no_match": 0, "themes_assigned": 0}

        for event in events:
            assigned, key = _classify(event, performer_map, et_by_name)
            stats[key] += 1
            if assigned and not dry_run:
                event.event_types = [assigned]
                if assigned.name == "Conference" and event.artist_name:
                    # Same rationale as run_incremental — see comment there.
                    event.artist_name = None
            # no_match: leave existing assignment untouched

            # Theme assignment (parallel to event_type — see THEME_KEYWORDS
            # and the matching block in run_incremental for rationale).
            if not dry_run:
                themes = theme_match(" ".join([event.name or "", event.description or ""]))
                if themes:
                    existing = {
                        et.theme_name for et in
                        db.query(EventTheme).filter(EventTheme.event_id == event.id).all()
                    }
                    for theme in themes:
                        if theme not in existing:
                            db.add(EventTheme(event_id=event.id, theme_name=theme))
                            stats["themes_assigned"] += 1

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
    parser.add_argument("--incremental", type=int, metavar="HOURS",
                        help="Run incremental mode: only events created in the "
                             "last N hours that have no event_types yet. "
                             "Non-destructive — never overwrites existing "
                             "assignments. Use this from automation.")
    args = parser.parse_args()
    if args.incremental is not None:
        stats = run_incremental(hours_back=args.incremental, dry_run=args.dry_run)
        print(f"incremental({args.incremental}h): {stats}")
    else:
        run(dry_run=args.dry_run)
