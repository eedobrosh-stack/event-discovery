"""De-duplicate venue rows that represent the same physical place.

Symptom (observed 2026-05-09): the venues table holds multiple rows
for one real venue, typically because two collectors scrape the same
place under different display names (e.g. ``"Shablul Jazz Club"`` from
an English source and ``"מועדון שבלול תל אביב"`` from a Hebrew one).
Each row gets independent metadata — different ``default_event_type_id``,
different ``street_address`` populations, different events parented
under it. Result in search: events at the "same" venue split across
rows, and one row's ``Jazz Concert`` default doesn't reach events
that landed under the other row.

This script clusters venue rows that look like the same place, picks a
canonical row per cluster, repoints all child events, and deletes the
non-canonical rows. Idempotent — re-running after a successful pass
finds nothing.

Detection — within a single ``city_id``, two venues are flagged as a
duplicate pair when **at least 2** of these signals agree:
  1. ``geo`` — both have ``(latitude, longitude)`` within 50 m.
  2. ``events`` — ≥3 events share the same ``(start_date, artist_name)``
     across the two venues (artist_name non-null). Strong because
     two real venues will not coincidentally book the same artist on
     the same date 3+ times.
  3. ``phone`` — normalised phone string identical (digits only).
  4. ``url`` — normalised ``website_url`` host identical.
  5. ``address`` — normalised ``street_address`` identical.

Clusters are formed by union-find over flagged pairs, so A↔B and B↔C
collapse into {A,B,C}. The 2-signal threshold prevents false merges
from coincidental address sharing or shared phone-line numbers.

Canonical pick (highest first):
  1. Has ``default_event_type_id`` set (preserves the override that
     drives event_type tagging at typed venues like Shablul Jazz Club).
  2. More child events.
  3. More populated columns (non-null count across optional fields).
  4. Lower ``id`` (stable tie-break).

Apply phase, in one transaction per cluster:
  - Backfill canonical's null columns from any duplicate that has them
    (lat/lon/phone/website_url/street_address/timezone/venue_type).
  - ``UPDATE events SET venue_id = canonical WHERE venue_id IN (dups)``.
  - ``DELETE FROM venues WHERE id IN (dups)``.

The events table's unique index is ``(scrape_source, source_id)`` —
NOT involving ``venue_id`` — so repointing cannot cause a collision.
Cross-source event duplicates that surface after this run are #2's
problem (event-level dedupe).

Safety:
  - ``--apply`` is required to write. Default mode is dry-run (prints
    plan, exits clean).
  - ``--city-id N`` scopes to a single city — prefer this on the first
    real run to validate the heuristic against a known case before
    going global.
  - Audit log written to ``data/dedupe_venues_<ts>.json`` listing
    every cluster, signals, canonical pick, and event count moved.

Usage:
    PYTHONPATH=. python3 scripts/dedupe_venues.py --city-id 239
    PYTHONPATH=. python3 scripts/dedupe_venues.py --city-id 239 --apply
    PYTHONPATH=. python3 scripts/dedupe_venues.py --apply
    # Force-merge known same-venue clusters the heuristic can't reach:
    PYTHONPATH=. python3 scripts/dedupe_venues.py --city-id 239 \\
        --merge-pair 36663:68595:69237:71859:72896
"""
from __future__ import annotations

import argparse
import difflib
import json
import logging
import math
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(str(ROOT / ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("dedupe_venues")

from sqlalchemy import text  # noqa: E402

from app.database import SessionLocal  # noqa: E402

# Distance threshold for geo signal. Venues this close are virtually
# always the same building; complexes with multiple stages still
# legitimately collide here, but their event lineups will too.
GEO_THRESHOLD_M = 50.0

# Minimum (start_date, artist_name) co-occurrences to flag the events
# signal. 3 is conservative — incidental overlaps are rare; tour
# routings typically don't put the same artist at two different
# real venues in one city on the same date.
MIN_COOCCUR_DEFAULT = 3

# Optional columns we backfill from duplicates onto the canonical
# row. Order doesn't matter — the first non-null wins.
BACKFILL_COLS = [
    "latitude", "longitude", "phone", "website_url",
    "street_address", "physical_city", "physical_country",
    "timezone", "venue_type",
]


# ──────────────────────────────────────────────────────────────────────
# Normalisation helpers
# ──────────────────────────────────────────────────────────────────────
def _norm_phone(s: str | None) -> str:
    """Digits only, leading zero stripped, country prefix kept.
    "+972 (3) 546-1891" → "972354618 91"-style → "97235461891"."""
    return re.sub(r"\D", "", s or "")


def _norm_url(s: str | None) -> str:
    """Normalised host. ``"https://www.shablul.co.il/events"`` →
    ``"shablul.co.il"``. Empty string when the URL doesn't parse to a
    netloc (so two empty/garbage URLs don't falsely match)."""
    if not s:
        return ""
    s = s.strip()
    if "://" not in s:
        s = "http://" + s
    try:
        host = urlparse(s).netloc.lower()
    except Exception:
        return ""
    return host[4:] if host.startswith("www.") else host


def _norm_address(s: str | None) -> str:
    """Lowercase, collapse whitespace, drop trailing punctuation. Two
    venues with addresses that differ only in spacing or comma
    placement collapse to the same key."""
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return re.sub(r"[,.;]+$", "", s).strip()


def _norm_venue_name(s: str | None) -> str:
    """Name key used by the production QA check.

    Unlike the old deduper, this folds punctuation, accents, HTML-ish
    separators, and whitespace before comparing names.  It intentionally
    keeps non-Latin letters so Hebrew venue names remain searchable.
    """
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    s = s.replace("&", " and ")
    s = re.sub(r"[^\w\s]+", " ", s, flags=re.UNICODE)
    return re.sub(r"\s+", " ", s).strip()


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distance in metres between two lat/lon points."""
    R = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def _populated_count(v: dict) -> int:
    return sum(1 for c in BACKFILL_COLS if v.get(c) not in (None, ""))


# ──────────────────────────────────────────────────────────────────────
# Sub-venue (hall) detection
# ──────────────────────────────────────────────────────────────────────
# Two venue names that look almost identical but differ in a hall
# qualifier represent distinct rooms inside one building, NOT a
# duplicate. Examples we observed in Tel Aviv:
#   "בית ציוני אמריקה תל אביב" (the building, 140 events) vs
#   "בית ציוני אמריקה (אולם מרתה), תל אביב" (Marta Hall, 14 events)
#   "תיאטרון הקאמרי תל אביב" vs "תיאטרון הקאמרי - אולם 1 תל אביב"
#   "היכל התרבות, אולם צוקר" (Tzucker Hall) vs
#   "היכל התרבות (אולם ע"ש לאוי)" (Lewy Hall — DIFFERENT room)
#
# The guard extracts a "hall identifier" set from each name. Two
# venues are considered different sub-rooms when:
#   * one has any identifier and the other has none, or
#   * both have identifiers and the sets don't overlap.
# In either case we VETO the heuristic merge regardless of other
# signals. Forced merges via --merge-pair bypass this guard.
HALL_KEYWORDS = [
    "אולם", "אודיטוריום", "במה", "אמפי", "מרפסת",   # he: hall, auditorium, stage, amphi, terrace
    "Hall", "Auditorium", "Stage", "Studio", "Room", "Saal",
    "Warehouse", "Factory", "Upstairs", "Downstairs", "Night",
]
_HALL_KW_RE = re.compile(
    r"(?:" + "|".join(re.escape(k) for k in HALL_KEYWORDS) + r")"
    r"(?!\w)"
    r"[\s\-]*(?:ע\"ש\s+|של\s+)?"
    r"([\w\"'\-]+)?",
    re.IGNORECASE,
)
_PRE_HALL_RE = re.compile(
    r"([\w\"'\-]+)\s+(?:Hall|Room|Saal|Warehouse|Factory|Arena)(?!\w)",
    re.IGNORECASE,
)
_PARENS_RE = re.compile(r"\(([^)]+)\)")
_HALL_PREFIX_STRIP = re.compile(r'^(?:אולם|Hall|Auditorium|Studio|Stage|ע\"ש|של)\s+', re.IGNORECASE)


def _norm_hall_token(s: str) -> str:
    s = s.strip()
    # Strip leading hall-prefix words (so "אולם מרתה" inside parens
    # collapses to "מרתה" — matches against bare "מרתה" outside parens).
    s = _HALL_PREFIX_STRIP.sub("", s).strip()
    s = re.sub(r"[^\w]+", "", s, flags=re.UNICODE)
    return s.lower()


def _extract_hall_ids(name: str | None) -> set[str]:
    """Tokens that distinguish a sub-room within a building. Empty
    set ⇒ no hall qualification (refers to the whole venue)."""
    if not name:
        return set()
    out: set[str] = set()
    for inside in _PARENS_RE.findall(name):
        tok = _norm_hall_token(inside)
        if tok:
            out.add(tok)
    for m in _HALL_KW_RE.finditer(name):
        tok = _norm_hall_token(m.group(1) or m.group(0))
        if tok:
            out.add(tok)
    preceding_ids = set()
    for m in _PRE_HALL_RE.finditer(name):
        tok = _norm_hall_token(m.group(1))
        if tok:
            preceding_ids.add(tok)
    if preceding_ids:
        out.difference_update({"hall", "room", "saal", "arena"})
        out.update(preceding_ids)
    return out


def _is_sub_venue_distinction(a: str | None, b: str | None) -> bool:
    """True when ``a`` and ``b`` look like different sub-rooms of one
    building (or one is the building, the other is a sub-room)."""
    ha, hb = _extract_hall_ids(a), _extract_hall_ids(b)
    if ha and hb:
        return not (ha & hb)         # disjoint sets ⇒ different rooms
    return bool(ha) != bool(hb)      # exactly one has a hall id ⇒ split


# ──────────────────────────────────────────────────────────────────────
# Detection
# ──────────────────────────────────────────────────────────────────────
def _load_venues(db, city_id: int | None, country: str | None = None) -> list[dict]:
    sql = """
        SELECT v.id, v.name, v.city_id, v.latitude, v.longitude, v.phone,
               v.website_url, v.street_address, v.physical_city,
               v.physical_country, v.timezone, v.venue_type,
               v.default_event_type_id, c.name AS city_name,
               (SELECT COUNT(*) FROM events e WHERE e.venue_id = v.id) AS event_count
        FROM venues v JOIN cities c ON c.id = v.city_id
    """
    where, params = [], {}
    if city_id is not None:
        where.append("v.city_id = :cid")
        params["cid"] = city_id
    if country:
        where.append("c.country = :country")
        params["country"] = country
    if where:
        sql += " WHERE " + " AND ".join(where)
    rows = db.execute(text(sql), params).fetchall()
    return [dict(r._mapping) for r in rows]


def _cooccurrence_pairs(db, city_id: int | None, min_n: int) -> dict[tuple[int, int], int]:
    """For every ordered pair of venue ids in the same city, count
    events that share ``(start_date, artist_name)`` (artist_name not
    null/empty). Returns {(a, b): n} with a < b and n ≥ min_n.

    The ``e1.venue_id < e2.venue_id`` JOIN predicate guarantees the
    smaller id appears as ``a`` — no need for LEAST/GREATEST (which
    SQLite lacks anyway)."""
    sql = """
        SELECT e1.venue_id AS a,
               e2.venue_id AS b,
               COUNT(*) AS n
        FROM events e1
        JOIN events e2 ON e1.start_date = e2.start_date
                       AND LOWER(e1.artist_name) = LOWER(e2.artist_name)
                       AND e1.venue_id < e2.venue_id
        JOIN venues v1 ON v1.id = e1.venue_id
        JOIN venues v2 ON v2.id = e2.venue_id
        WHERE e1.artist_name IS NOT NULL AND e1.artist_name <> ''
          AND v1.city_id = v2.city_id
    """
    params: dict = {"min_n": min_n}
    if city_id is not None:
        sql += " AND v1.city_id = :cid"
        params["cid"] = city_id
    sql += " GROUP BY a, b HAVING COUNT(*) >= :min_n"
    out: dict[tuple[int, int], int] = {}
    for row in db.execute(text(sql), params).fetchall():
        out[(int(row[0]), int(row[1]))] = int(row[2])
    return out


def _signals(a: dict, b: dict, cooccur_n: int) -> dict[str, bool | int]:
    """Compute signal vector for one pair. Both venues already share
    city_id."""
    sig: dict[str, bool | int] = {
        "geo": False, "events": False,
        "phone": False, "url": False, "address": False,
    }
    if all(a[c] is not None for c in ("latitude", "longitude")) and \
       all(b[c] is not None for c in ("latitude", "longitude")):
        d = _haversine_m(a["latitude"], a["longitude"], b["latitude"], b["longitude"])
        sig["geo"] = d <= GEO_THRESHOLD_M
    if cooccur_n:
        sig["events"] = True
        sig["events_n"] = cooccur_n
    pa, pb = _norm_phone(a["phone"]), _norm_phone(b["phone"])
    if pa and pb and pa == pb:
        sig["phone"] = True
    ua, ub = _norm_url(a["website_url"]), _norm_url(b["website_url"])
    if ua and ub and ua == ub:
        sig["url"] = True
    aa, ab = _norm_address(a["street_address"]), _norm_address(b["street_address"])
    if aa and ab and aa == ab:
        sig["address"] = True
    return sig


def _signal_count(sig: dict) -> int:
    return sum(1 for k, v in sig.items() if k != "events_n" and bool(v))


def _build_clusters(
    venues: list[dict],
    cooccur: dict[tuple[int, int], int],
    forced_groups: list[list[int]] | None = None,
) -> list[tuple[list[dict], list[dict]]]:
    """Return list of (canonical, [duplicates]) tuples. Pairs are
    flagged when ≥2 signals agree; clusters formed by union-find.
    ``forced_groups`` is a list of venue-id groups that get merged
    unconditionally — used for known cases the heuristic can't reach
    (e.g. Hebrew/English name pairs with no shared metadata)."""
    by_id = {v["id"]: v for v in venues}
    by_city: dict[int, list[dict]] = {}
    for v in venues:
        by_city.setdefault(v["city_id"], []).append(v)

    parent: dict[int, int] = {v["id"]: v["id"] for v in venues}
    pair_signals: dict[tuple[int, int], dict] = {}

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for city_id, group in by_city.items():
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                a, b = group[i], group[j]
                pair = (a["id"], b["id"]) if a["id"] < b["id"] else (b["id"], a["id"])
                # Sub-venue veto — see _is_sub_venue_distinction.
                # Suppresses cases like "Cameri" vs "Cameri Hall 1" or
                # "(אולם מרתה)" vs "(אולם מאירהוף)" where a metadata
                # match is structural (same building) but the names
                # disambiguate distinct rooms.
                if _is_sub_venue_distinction(a["name"], b["name"]):
                    continue
                cn = cooccur.get(pair, 0)
                sig = _signals(a, b, cn)
                if _signal_count(sig) >= 2:
                    pair_signals[pair] = sig
                    union(a["id"], b["id"])

    # Forced groups bypass the signal heuristic. Validate every id
    # exists and shares a city before merging.
    for spec in forced_groups or []:
        missing = [i for i in spec if i not in by_id]
        if missing:
            raise SystemExit(f"--merge-pair: unknown venue ids: {missing}")
        cities = {by_id[i]["city_id"] for i in spec}
        if len(cities) > 1:
            raise SystemExit(
                f"--merge-pair {spec}: ids span multiple city_ids {cities} — refusing."
            )
        first = spec[0]
        for other in spec[1:]:
            pair = (first, other) if first < other else (other, first)
            pair_signals[pair] = {"forced": True}
            union(first, other)

    # Group by root; singletons get filtered below by the len < 2 check
    clusters: dict[int, list[dict]] = {}
    for v in venues:
        clusters.setdefault(find(v["id"]), []).append(v)

    out: list[tuple[list[dict], list[dict]]] = []
    for members in clusters.values():
        if len(members) < 2:
            continue
        members.sort(key=lambda v: (
            0 if v["default_event_type_id"] is not None else 1,
            -(v["event_count"] or 0),
            -_populated_count(v),
            v["id"],
        ))
        canonical, dups = members[0], members[1:]
        # Attach signals for audit
        for d in dups:
            key = (canonical["id"], d["id"]) if canonical["id"] < d["id"] else (d["id"], canonical["id"])
            d["_signals_to_canonical"] = pair_signals.get(key, {})
        out.append(([canonical], dups))
    return out


# ──────────────────────────────────────────────────────────────────────
# Website pass
# ──────────────────────────────────────────────────────────────────────
# Hosts that sell tickets, map, or list MANY unrelated venues. A shared
# host on one of these says nothing about two rows being the same place
# (leaan.co.il alone covers Beit Zioni America, the Eri Geller Museum and
# Heichal HaTarbut), so the website pass ignores them outright. The
# name-relatedness requirement below would already block those merges;
# this list is the second belt.
AGGREGATOR_HOSTS = {
    # ticketing / listing platforms
    "leaan.co.il", "mevalim.co.il", "habama.co.il", "smarticket.co.il",
    "live.tickchak.co.il", "tickchak.co.il", "kupatbravo.co.il",
    "ticketmaster.co.il", "ticketmaster.com", "eventbuzz.co.il",
    "ontopo.com", "secrettelaviv.com", "concerts50.com", "haifa.events",
    "bandsintown.com", "songkick.com", "eventbrite.com", "dice.fm",
    "ra.co", "residentadvisor.net", "meetup.com", "lu.ma", "luma.com",
    # maps / social / media — never identify a venue on their own
    "waze.com", "maps.apple.com", "maps.google.com", "google.com",
    "youtube.com", "shazam.com", "facebook.com", "instagram.com",
    "spotify.com", "wikipedia.org",
}
# Suffixes whose hosts are shared by many municipal venues.
AGGREGATOR_SUFFIXES = ("gov.il", "muni.il")

# Tokens that carry no venue identity — region names, country names.
# Stripped before two names are compared so that "גריי, יהוד-מונוסון"
# and "מועדון הגריי יהוד, אזור המרכז" both reduce to the brand core.
# The city's OWN name is stripped per city instead (see
# ``_city_noise_tokens``) because ``cities.name`` is English while the
# venue names are usually Hebrew.
GEO_NOISE_TOKENS = {
    "אזור", "המרכז", "הצפון", "הדרום", "מחוז", "ישראל", "השרון",
    "israel", "il", "district",
}

# Venue-kind words that may prefix a name without changing which place
# it is ("תיאטרון בית ליסין" is "בית ליסין"). Only stripped as a LEADING
# token: mid-name they distinguish rooms inside a complex, which is why
# "תיאטרון הפארק, פארק הירקון" must not collapse into "פארק הירקון".
GENERIC_PREFIXES = {
    "תיאטרון", "תאטרון", "מועדון", "היכל", "מוזיאון", "בית", "מרכז",
    "theatre", "theater", "club", "museum", "the", "cafe", "bar", "pub",
}

# Hebrew has no case, so the definite article is the main spelling
# wobble between collectors: "מדיטק" / "המדיטק", "גריי" / "הגריי".
_HE_RE = re.compile(r"^[֐-׿]+$")


def _is_aggregator_host(host: str) -> bool:
    return bool(host) and (host in AGGREGATOR_HOSTS
                           or host.endswith(AGGREGATOR_SUFFIXES))


def _host_brand(host: str) -> str:
    """Registrable label of a host, letters/digits only. ``grayclub.co.il``
    → ``grayclub``; used to recognise a venue named after its own site."""
    if not host:
        return ""
    parts = [p for p in host.split(".") if p]
    # Drop public-suffix-ish tail labels (co/org/ac/com/net/il/us/…).
    while len(parts) > 1 and len(parts[-1]) <= 3:
        parts.pop()
    return re.sub(r"[^a-z0-9]", "", parts[-1].lower()) if parts else ""


def _he_fold(tok: str) -> str:
    """Drop a leading Hebrew definite article so "הגריי" keys as "גריי"."""
    if len(tok) >= 4 and tok.startswith("ה") and _HE_RE.match(tok):
        return tok[1:]
    return tok


def _city_noise_tokens(venues: list[dict]) -> dict[int, set[str]]:
    """Per city, the tokens that are city qualifiers rather than venue
    identity.

    ``cities.name`` is English ("Yehud", "Jerusalem") while most venue
    names are Hebrew, so the city's own spelling can't be read off the
    city row. It can be read off the data: a token carried by a quarter
    of a city's venue names is the city, its district or its region —
    "יהוד", "מונוסון", "אביב", "יפו" — never a venue's identity. Only
    computed for cities with enough rows for the ratio to mean anything.
    """
    by_city: dict[int, list[dict]] = defaultdict(list)
    for v in venues:
        by_city[v["city_id"]].append(v)
    out: dict[int, set[str]] = {}
    for city_id, group in by_city.items():
        noise: set[str] = set()
        for src in (group[0].get("city_name"),):
            noise.update(t for t in _norm_venue_name(src).split() if len(t) >= 3)
        if len(group) >= 8:
            counts: Counter[str] = Counter()
            for v in group:
                counts.update(set(_norm_venue_name(v["name"]).split()))
            for tok, n in counts.items():
                if n >= 3 and n / len(group) >= 0.25 and tok not in GENERIC_PREFIXES:
                    noise.add(tok)
        out[city_id] = noise
    return out


def _name_tokens(venue: dict, city_noise: set[str]) -> list[str]:
    """Identity tokens of a venue name: city/region qualifiers removed,
    definite articles folded, order preserved."""
    toks = [_he_fold(t) for t in _norm_venue_name(venue["name"]).split()]
    drop = {_he_fold(t) for t in city_noise} | GEO_NOISE_TOKENS
    for src in (venue.get("physical_city"),):
        drop.update(_he_fold(t) for t in _norm_venue_name(src).split() if len(t) >= 3)
    kept = [t for t in toks if t not in drop]
    return kept or toks


def _strip_generic_prefix(toks: list[str]) -> list[str]:
    while len(toks) > 1 and toks[0] in GENERIC_PREFIXES:
        toks = toks[1:]
    return toks


def _name_core(venue: dict, city_noise: set[str] | None = None) -> str:
    """Identity tokens as one string, with a generic leading venue-kind
    word ("תיאטרון", "מועדון") dropped and the rest sorted — collectors
    disagree on word order ("הקאמרי - תיאטרון" vs "תיאטרון הקאמרי")
    far more often than on the words themselves."""
    toks = _strip_generic_prefix(_name_tokens(venue, city_noise or set()))
    return " ".join(sorted(toks))


def _brand_match(core: str, brand: str) -> bool:
    """True when a name core looks like the host's own brand label."""
    if not core or not brand or len(brand) < 4:
        return False
    flat = re.sub(r"[^a-z0-9]", "", core)
    return bool(flat) and (flat in brand or brand in flat)


def _digit_tokens(toks: list[str]) -> set[str]:
    return {t for t in toks if any(ch.isdigit() for ch in t)}


def _website_relation(a: dict, b: dict, brand: str, shared_events: int,
                      city_noise: set[str]) -> str | None:
    """Why ``a`` and ``b`` (same city, same non-aggregator host) are the
    same venue — or None when the shared host is not enough.

    Sharing a website makes two rows *candidates*; the name has to agree
    before they are folded, because one venue's site legitimately hosts
    several rooms. The ladder, strictest first:

    ``core_equal``   identical once city qualifiers, definite articles
                     and a leading venue-kind word are removed.
    ``core_ratio``   near-identical spelling ("פקטורי" / "פאקטורי").
    ``host_brand``   both names are the site's own brand — the
                     English/Hebrew pair "Gray Club" / "מועדון Gray".
    ``shared_events`` names share no token at all (cross-script rows
                     like "GRAY מודיעין" / "גריי מודיעין") but the two
                     rows already hold the same show on the same date.

    A number token present on one side only ("הבימה 4") means a numbered
    hall, never a spelling variant, and vetoes the pair outright.
    """
    ta, tb = _name_tokens(a, city_noise), _name_tokens(b, city_noise)
    if _digit_tokens(ta) != _digit_tokens(tb):
        return None
    ca, cb = _name_core(a, city_noise), _name_core(b, city_noise)
    if ca and cb:
        if sorted(ta) == sorted(tb) or ca == cb:
            return "core_equal"
        ratio = difflib.SequenceMatcher(None, ca, cb).ratio()
        if ratio >= 0.86:
            return f"core_ratio={ratio:.2f}"
        # Brand match reads the tokens in their written order: it is the
        # concatenation ("Gray" + "Club") that has to equal the host label.
        if (_brand_match(" ".join(_strip_generic_prefix(ta)), brand)
                and _brand_match(" ".join(_strip_generic_prefix(tb)), brand)):
            return "host_brand"
        # Any shared identity token means these are two names for places
        # in the same complex — and the part that differs is what tells
        # the rooms apart. Only wholly disjoint names fall through to the
        # event bridge.
        if set(ta) & set(tb):
            return None
    if shared_events >= 2:
        return f"shared_events={shared_events}"
    return None


def _build_website_clusters(db, venues: list[dict]) -> list[tuple[list[dict], list[dict]]]:
    """Consolidate venues that share a website AND a city.

    The shared host is the entry ticket, not the verdict: rows only merge
    when their names also agree (equal / near / contained core, or both
    matching the host's own brand) or when they already hold the same
    event on the same date. Without that second condition a ticketing
    host would collapse every venue it sells for into one row.

    Sub-room names (``אולם ע"ש לאוי`` vs ``אולם צוקר``) and rows whose
    ``physical_city`` disagrees are vetoed, exactly as in the other passes.
    """
    by_id = {v["id"]: v for v in venues}
    parent = {v["id"]: v["id"] for v in venues}
    pair_signals: dict[tuple[int, int], dict] = {}

    signatures: dict[int, set[tuple[str, str]]] = {v["id"]: set() for v in venues}
    rows = db.execute(text(
        "SELECT venue_id, start_date, LOWER(COALESCE(NULLIF(artist_name, ''), name)) "
        "FROM events WHERE venue_id IS NOT NULL"
    )).fetchall()
    for venue_id, start_date, ident in rows:
        if venue_id in signatures and ident:
            signatures[venue_id].add((str(start_date), str(ident).strip()))

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    city_noise = _city_noise_tokens(venues)
    buckets: dict[tuple[int, str], list[dict]] = defaultdict(list)
    skipped_aggregator: set[str] = set()
    for v in venues:
        host = _norm_url(v["website_url"])
        if not host:
            continue
        if _is_aggregator_host(host):
            skipped_aggregator.add(host)
            continue
        buckets[(v["city_id"], host)].append(v)

    for (city_id, host), group in buckets.items():
        if len(group) < 2:
            continue
        brand = _host_brand(host)
        noise = city_noise.get(city_id, set())
        for i, a in enumerate(group):
            for b in group[i + 1:]:
                if _is_sub_venue_distinction(a["name"], b["name"]):
                    continue
                pa, pb = _norm_venue_name(a["physical_city"]), _norm_venue_name(b["physical_city"])
                if pa and pb and pa != pb:
                    continue
                shared = len(signatures[a["id"]] & signatures[b["id"]])
                why = _website_relation(a, b, brand, shared, noise)
                if not why:
                    continue
                pair = (a["id"], b["id"]) if a["id"] < b["id"] else (b["id"], a["id"])
                pair_signals[pair] = {"url": True, "host": host, "match": why,
                                      "shared_events": shared}
                union(a["id"], b["id"])

    if skipped_aggregator:
        log.info(f"aggregator hosts skipped: {len(skipped_aggregator)} "
                 f"({', '.join(sorted(skipped_aggregator)[:8])}…)")

    groups: dict[int, list[dict]] = {}
    for v in venues:
        groups.setdefault(find(v["id"]), []).append(v)
    out = []
    for members in groups.values():
        if len(members) < 2:
            continue
        members.sort(key=lambda v: (
            0 if v["default_event_type_id"] is not None else 1,
            -(v["event_count"] or 0),
            -_populated_count(v),
            v["id"],
        ))
        canonical, dups = members[0], members[1:]
        for d in dups:
            key = (canonical["id"], d["id"]) if canonical["id"] < d["id"] else (d["id"], canonical["id"])
            d["_signals_to_canonical"] = pair_signals.get(key, {"url": True, "match": "transitive"})
        out.append(([canonical], dups))
    return out


# ──────────────────────────────────────────────────────────────────────
# Site pass (2026-09-24)
# ──────────────────────────────────────────────────────────────────────
# The product rule, from reviewing the Israel venue report: two rows on
# the same site ARE the same venue unless their names differ by a
# location (another city / branch). Halls, word order, Hebrew vs English,
# "היכל התרבות" vs "היכל הפיס לתרבות" — all the same place. The website
# pass above is stricter (names must agree) and so left pairs like
# betshemesh.smarticket.co.il / d-one.co.il / hotcinema.co.il/theater/2
# unmerged.
#
# The site key depends on who owns the site:
#   own site       (d-one.co.il, habima.co.il)  → the host
#   shared site    (ticketing, municipal, cinema chains, listing sites,
#                   and any *.smarticket.co.il-style tenant host that also
#                   serves other venues) → host + path, the venue's own
#                   page; a shared site's homepage identifies nothing.
# On a shared site the names must also share one identity word, because
# one listing page can carry several unrelated venues (govextra's
# Rishon page lists three).

# Maps, video and link-in-bio hosts: a collector put the wrong link in
# website_url. Cleared, never used as a site key.
JUNK_URL_RE = re.compile(
    r"(^|\.)(waze\.com|youtube\.com|youtu\.be|google\.[a-z.]+|goo\.gl|"
    r"maps\.apple\.com|linktr\.ee|lnk\.bio|facebook\.com|fb\.com|instagram\.com|"
    r"wikipedia\.org|tripadvisor\.[a-z.]+|bit\.ly|tinyurl\.com)$"
)
SHARED_SITE_HOSTS = AGGREGATOR_HOSTS | {
    "hotcinema.co.il", "cinema-city.co.il", "yesplanet.co.il", "lev.co.il",
    "rav-hen.co.il", "makore.co.il", "itraveljerusalem.com", "hilton.com",
    "marriott.com", "hamatnas.co.il", "kehilatayim.org.il", "park.co.il",
    "makefet.com", "modiinapp.com", "imj.org.il", "ethos.co.il",
    # one operator, several venues on one site
    "sportpalace.co.il", "atlas.co.il", "zofim.org.il", "eilatport.co.il",
    "lunapark.co.il", "fattal.co.il", "isrotel.com", "ihg.com",
}
# A venue that moved its site: rows still carrying the old host are
# re-pointed at the new URL (Shablul moved its box office to smarticket).
SITE_MOVES = {
    "shabluljazz.com": "https://shablul.smarticket.co.il/",
}
# Display name for a merged venue when the vote below would pick a
# descriptive spelling over the brand ("מועדון שבלול" vs "שבלול ג'אז").
# Keyed by the site key of the cluster.
DISPLAY_NAMES = {
    "shablul.smarticket.co.il": "שבלול ג'אז",
}
_HALL_WORD_RE = re.compile(r"(^|[\s,\-–])(אולם|במה|ביתן|hall|stage|studio|pavilion)(\s|$|\d)", re.IGNORECASE)
# "אוניברסיטת חיפה", "נמל אילת": the city is part of a compound name.
_CONSTRUCT_BEFORE_PLACE = {"אוניברסיטת", "עיריית", "נמל", "מכללת", "מוזיאון", "university", "port"}
_HOMEPAGE_PATH_RE = re.compile(
    r"^(/(he|en|ar|ru|heb|eng))?(/(pages/)?(home|homepage|index|default|main)(\.aspx|\.php|\.html?)?)?$"
)
# Words that say what kind of place a row is, not which one. Dropped
# before two shared-site names are compared for a common identity word.
SITE_GENERIC_WORDS = GENERIC_PREFIXES | {
    "היכל", "התרבות", "תרבות", "לתרבות", "אולם", "האולם", "אודיטוריום",
    "משכן", "המשכן", "לאמנויות", "אמנויות", "אומנויות", "הבמה", "במה",
    "העירוני", "עירוני", "המופעים", "מופעים", "קהילתי", "מתנ", "ס", "ע", "ש",
    "hall", "center", "centre", "auditorium", "arts", "of", "and",
}


def _is_junk_url(url: str | None) -> bool:
    host = _norm_url(url)
    return bool(host) and bool(JUNK_URL_RE.search(host))


def _site_key(url: str | None) -> tuple[str, bool]:
    """(key, shared). Empty key = the URL identifies no venue."""
    host = _norm_url(url)
    if not host or JUNK_URL_RE.search(host):
        return "", False
    s = url.strip() if "://" in url else "http://" + url.strip()
    try:
        p = urlparse(s)
    except Exception:
        return "", False
    path = unquote(p.path).rstrip("/").lower()
    shared = _is_aggregator_host(host) or host in SHARED_SITE_HOSTS
    if not shared:
        return host, False
    if _HOMEPAGE_PATH_RE.match(path) and not p.query:
        return "", True
    return host + path + ("?" + p.query if p.query else ""), True


_PLACE_TOKS: set[str] | None = None


def _identity_words(name: str | None) -> set[str]:
    """Words that say WHICH place a row is: venue-kind words, place names
    and region noise dropped, Hebrew definite article folded on both
    sides of the comparison ("היכל" → "יכל" is still a kind word)."""
    global _PLACE_TOKS
    if _PLACE_TOKS is None:
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        from app.services.il_places import PLACES  # noqa: E402
        _PLACE_TOKS = {_he_fold(t) for k in (*PLACES, *PLACES.values())
                       if not k.startswith(("Outside Israel", "Online", "Israel - Other"))
                       for t in _norm_venue_name(k).split()}
    drop = {_he_fold(w) for w in SITE_GENERIC_WORDS | GEO_NOISE_TOKENS} | _PLACE_TOKS
    toks = {_he_fold(t) for t in _norm_venue_name(name).split()}
    return {t for t in toks if t not in drop and len(t) > 1}


def _page_key(url: str | None) -> str:
    """host + path with language / homepage / about segments folded, so
    habima.co.il/en/homepage and habima.co.il/ are one page."""
    host = _norm_url(url)
    if not host:
        return ""
    s = url.strip() if "://" in url else "http://" + url.strip()
    path = unquote(urlparse(s).path).rstrip("/").lower()
    path = re.sub(r"^/(he|en|ar|ru|heb|eng)(?=/|$)", "", path)
    if re.fullmatch(r"(/(pages/)?(home|homepage|index|default|main|about)(\.aspx|\.php|\.html?)?)?", path):
        path = ""
    return host + path


def _script(words: set[str]) -> str:
    heb = any(_HE_RE.match(w) for w in words)
    lat = any(re.match(r"^[a-z0-9]+$", w) for w in words)
    return "mixed" if heb and lat else "he" if heb else "latin"


def _build_site_clusters(db, venues: list[dict], *, apply: bool) -> tuple[list, dict]:
    """Clusters by the site rule above, after two clean-ups:

    1. junk website_url (waze / youtube / maps …) is cleared;
    2. a row with no website whose exact normalised name matches a
       website-bearing row in the same real city inherits that website
       (only when every such match agrees on one URL).

    Both clean-ups are written only with ``apply``. Returns
    (clusters, stats).
    """
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from app.services.il_places import canon_place, name_places  # noqa: E402

    stats = {"junk_urls_cleared": [], "websites_backfilled": [], "site_moves": [],
             "renamed": []}
    for v in venues:
        v["_place"] = canon_place(v.get("physical_city"), v.get("city_name"))
        moved = SITE_MOVES.get(_norm_url(v["website_url"]))
        if moved and v["website_url"] != moved:
            stats["site_moves"].append({"id": v["id"], "name": v["name"],
                                        "from": v["website_url"], "to": moved})
            v["website_url"] = moved
        if _is_junk_url(v["website_url"]):
            stats["junk_urls_cleared"].append({"id": v["id"], "name": v["name"],
                                               "url": v["website_url"]})
            v["website_url"] = None

    # Twins: a website-less row inherits the site of the website-bearing
    # row(s) it names — by exact name or recorded alias first, else by
    # identity words (one row's words contained in the other's, sharing a
    # real word: "שבלול ג'אז" ⊂ "Shablul Jazz- שבלול ג'אז" / "מועדון
    # שבלול"). Only when every candidate agrees on ONE site.
    aliases_by_venue: dict[int, list[str]] = defaultdict(list)
    for vid, alias in db.execute(text("SELECT venue_id, alias FROM venue_aliases")).fetchall():
        aliases_by_venue[vid].append(alias)
    urls_by_name: dict[tuple[str, str], set[str]] = defaultdict(set)
    words_by_place: dict[str, list[tuple[set[str], str]]] = defaultdict(list)
    for v in venues:
        if v["website_url"] and _site_key(v["website_url"])[0]:
            url = v["website_url"].strip()
            for nm in [v["name"], *aliases_by_venue.get(v["id"], [])]:
                urls_by_name[(v["_place"], _norm_venue_name(nm))].add(url)
                w = _identity_words(nm)
                if w:
                    words_by_place[v["_place"]].append((w, url))
    for v in venues:
        if v["website_url"]:
            continue
        urls = urls_by_name.get((v["_place"], _norm_venue_name(v["name"])))
        if not urls:
            wv = _identity_words(v["name"])
            if wv:
                # The smaller side must carry two identity words, so a
                # lone "סינמה" / "port" / "hotel" never picks a twin.
                urls = {url for w, url in words_by_place.get(v["_place"], [])
                        if (wv <= w or w <= wv) and len(wv & w) >= 2
                        and any(len(t) >= 3 for t in wv & w)}
        if urls and len(urls) == 1:
            v["website_url"] = next(iter(urls))
            stats["websites_backfilled"].append({"id": v["id"], "name": v["name"],
                                                 "url": v["website_url"]})

    if apply:
        for row in stats["site_moves"]:
            db.execute(text("UPDATE venues SET website_url = :u WHERE id = :id"),
                       {"u": row["to"], "id": row["id"]})
        for row in stats["junk_urls_cleared"]:
            db.execute(text("UPDATE venues SET website_url = NULL WHERE id = :id"), {"id": row["id"]})
        for row in stats["websites_backfilled"]:
            db.execute(text("UPDATE venues SET website_url = :u WHERE id = :id"),
                       {"u": row["url"], "id": row["id"]})
        db.commit()

    parent = {v["id"]: v["id"] for v in venues}

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    buckets: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for v in venues:
        key, shared = _site_key(v["website_url"])
        if key:
            v["_shared_site"] = shared
            v["_page"] = _page_key(v["website_url"])
            buckets[(v["_place"], key)].append(v)

    pair_signals: dict[tuple[int, int], dict] = {}
    for (place, key), group in buckets.items():
        if len(group) < 2 or place.startswith(("Outside Israel", "Online")):
            continue
        for i, a in enumerate(group):
            pa = name_places(a["name"])
            wa = _identity_words(a["name"])
            for b in group[i + 1:]:
                pb = name_places(b["name"])
                if pa and pb and not (pa & pb):
                    continue                      # names name different cities
                if (pa ^ pb) - {place}:
                    continue                      # one name names another city
                wb = _identity_words(b["name"])
                if wa and wb and not (wa & wb):
                    # Nothing in common but the site. On a shared page that
                    # is two venues; on an own site it is the same place when
                    # the names are in two scripts (Hebrew/English can share
                    # no word) or both rows point at the same page of it
                    # ("מנחם עינן" / "היכל התרבות מודיעין" on shows.org.il).
                    cross = _script(wa) != _script(wb) and "mixed" not in (_script(wa), _script(wb))
                    same_page = a["_page"] == b["_page"]
                    if a["_shared_site"]:
                        if not (cross and same_page):
                            continue
                    elif not (cross or same_page):
                        continue
                why = "same_page" if a["_shared_site"] else "same_site"
                pair = (min(a["id"], b["id"]), max(a["id"], b["id"]))
                pair_signals[pair] = {"url": True, "site": key, "place": place, "match": why}
                ra, rb = find(a["id"]), find(b["id"])
                if ra != rb:
                    parent[ra] = rb

    groups: dict[int, list[dict]] = defaultdict(list)
    for v in venues:
        groups[find(v["id"])].append(v)
    out = []
    for members in groups.values():
        if len(members) < 2:
            continue
        # Prefer a row whose City row IS its real city, so the survivor is
        # not one of the default-city (Tel Aviv) fallbacks.
        members.sort(key=lambda v: (
            0 if _norm_venue_name(v.get("city_name")) == _norm_venue_name(v["_place"]) else 1,
            0 if v["default_event_type_id"] is not None else 1,
            -(v["event_count"] or 0),
            -_populated_count(v),
            v["id"],
        ))
        canonical, dups = members[0], members[1:]
        for d in dups:
            key = (min(canonical["id"], d["id"]), max(canonical["id"], d["id"]))
            d["_signals_to_canonical"] = pair_signals.get(key, {"url": True, "match": "transitive"})
        display = _display_name(members, canonical["_place"],
                                _site_key(canonical["website_url"])[0])
        if display and display != canonical["name"]:
            canonical["_display_name"] = display
            stats["renamed"].append({"id": canonical["id"], "from": canonical["name"],
                                     "to": display})
        out.append(([canonical], dups))
    return out, stats


def _strip_place_suffix(name: str, place: str) -> str:
    """Drop a trailing qualifier naming the venue's own city — but only
    when the rest still says which venue it is: "תיאטרון הקאמרי, תל
    אביב-יפו" → "תיאטרון הקאמרי", "תיאטרון חיפה חיפה" → "תיאטרון חיפה",
    while "תיאטרון חיפה" and "היכל התרבות נתניה" keep their city (it IS
    their name)."""
    from app.services.il_places import PLACES  # noqa: E402
    spellings = sorted({k for k, v in PLACES.items() if v == place} | {place},
                       key=len, reverse=True)
    out = (name or "").replace("&quot;", '"').replace("&#039;", "'").strip()
    while True:
        for sp in spellings:
            m = re.search(r"[\s,\-–|(]*\(?" + re.escape(sp) + r"\)?[\s.]*$", out, re.IGNORECASE)
            if m and m.start() > 0:
                rest = out[:m.start()].rstrip(" ,-–|(.")
                prev = rest.split()[-1].lower() if rest.split() else ""
                if prev in _CONSTRUCT_BEFORE_PLACE and out[m.start():m.start() + 1].isspace():
                    continue
                if _identity_words(rest) or any(
                        re.search(r"(^|[\s,\-–])" + re.escape(x) + r"($|[\s,\-–])", rest)
                        for x in spellings):
                    out = rest
                    break
        else:
            return out or name


def _display_name(members: list[dict], place: str, site_key: str) -> str | None:
    """The one name a merged venue is shown under, on every surface.

    Conservative: the survivor keeps its own name, minus a redundant
    own-city suffix. Only when that name is Latin / mixed-script (and the
    cluster has Hebrew spellings) or is a hall's name, is another member's
    spelling chosen — the one whose identity words most members share,
    and only if that is a majority. DISPLAY_NAMES overrides everything."""
    if site_key in DISPLAY_NAMES:
        return DISPLAY_NAMES[site_key]
    canonical = members[0]["name"]

    def is_hebrew(c: str) -> bool:
        return bool(re.search("[\u0590-\u05ff]", c)) and not re.search("[A-Za-z]", c)

    base = canonical
    if not is_hebrew(canonical) or _HALL_WORD_RE.search(canonical):
        any_hebrew = any(is_hebrew(m["name"]) for m in members)
        cands: Counter[str] = Counter(
            _strip_place_suffix(m["name"], place) for m in members
            if (is_hebrew(m["name"]) or not any_hebrew) and not _HALL_WORD_RE.search(m["name"]))
        if cands:
            words = [_identity_words(m["name"]) for m in members]

            def coverage(c: str) -> int:
                w = _identity_words(c)
                return sum(1 for mw in words if w and w <= mw)

            best = min(cands, key=lambda c: (-coverage(c), -cands[c], -len(c), c))
            # Switch only when the spelling speaks for most of the cluster
            # ("בארבי" in 4 of 5 Barby rows), never on a 1-of-2 guess.
            if coverage(best) * 2 > len(members):
                base = best
    return _strip_place_suffix(base, place)


def _build_name_clusters(db, venues: list[dict]) -> list[tuple[list[dict], list[dict]]]:
    """Find same-city venue name variants with indexed candidate blocking.

    Exact/near-identical normalized names are sufficient on their own.
    Short-name containment (e.g. ``AMAMA`` vs ``AMAMA Jazz Room``) also
    requires corroboration: shared event identity, URL, address, or geo.
    This catches the AMAMA family without collapsing unrelated venues such
    as ``Sphere`` and ``Life Burns Faster at Sphere``.
    """
    by_id = {v["id"]: v for v in venues}
    parent = {v["id"]: v["id"] for v in venues}
    pair_signals: dict[tuple[int, int], dict] = {}
    signatures: dict[int, set[tuple[str, str]]] = {v["id"]: set() for v in venues}
    rows = db.execute(text(
        "SELECT venue_id, start_date, LOWER(COALESCE(NULLIF(artist_name, ''), name)) "
        "FROM events WHERE venue_id IS NOT NULL"
    )).fetchall()
    for venue_id, start_date, ident in rows:
        if venue_id in signatures and ident:
            signatures[venue_id].add((str(start_date), str(ident).strip()))

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    by_city: dict[int, list[dict]] = {}
    for v in venues:
        by_city.setdefault(v["city_id"], []).append(v)

    for group in by_city.values():
        keyed = [(v, _norm_venue_name(v["name"])) for v in group]
        blocks: dict[str, list[tuple[dict, str]]] = {}
        for v, name in keyed:
            if name:
                blocks.setdefault(name[:5], []).append((v, name))
        for block in blocks.values():
            if len(block) < 2 or len(block) > 500:
                continue
            for i, (a, na) in enumerate(block):
                for b, nb in block[i + 1:]:
                    if _is_sub_venue_distinction(a["name"], b["name"]):
                        continue
                    exact = na == nb
                    ratio = difflib.SequenceMatcher(None, na, nb).ratio()
                    near = ratio >= 0.92
                    contained = (na in nb or nb in na) and min(len(na), len(nb)) >= 4
                    shared_events = signatures[a["id"]] & signatures[b["id"]]
                    sig = {
                        "name": exact or near,
                        "name_ratio": round(ratio, 3),
                        "shared_events": len(shared_events),
                        "url": bool(_norm_url(a["website_url"]) and _norm_url(a["website_url"]) == _norm_url(b["website_url"])),
                        "address": bool(_norm_address(a["street_address"]) and _norm_address(a["street_address"]) == _norm_address(b["street_address"])),
                        "geo": False,
                    }
                    if all(a[c] is not None for c in ("latitude", "longitude")) and all(b[c] is not None for c in ("latitude", "longitude")):
                        sig["geo"] = _haversine_m(a["latitude"], a["longitude"], b["latitude"], b["longitude"]) <= 60
                    supported = bool(shared_events) or sig["url"] or sig["address"] or sig["geo"]
                    if not (exact or near or (contained and supported)):
                        continue
                    pair = (a["id"], b["id"]) if a["id"] < b["id"] else (b["id"], a["id"])
                    pair_signals[pair] = sig
                    union(*pair)

    groups: dict[int, list[dict]] = {}
    for v in venues:
        root = find(v["id"])
        groups.setdefault(root, []).append(v)
    out = []
    for members in groups.values():
        if len(members) < 2:
            continue
        members.sort(key=lambda v: (
            0 if v["default_event_type_id"] is not None else 1,
            -(v["event_count"] or 0),
            -_populated_count(v),
            v["id"],
        ))
        canonical, dups = members[0], members[1:]
        for d in dups:
            key = (canonical["id"], d["id"]) if canonical["id"] < d["id"] else (d["id"], canonical["id"])
            d["_signals_to_canonical"] = pair_signals.get(key, {})
        out.append(([canonical], dups))
    return out


def _build_cross_language_clusters(db, venues: list[dict]) -> list[tuple[list[dict], list[dict]]]:
    """Find same-city venues whose names use different scripts.

    Names are not transliterated. Instead, a cross-script pair must share
    at least three ``(date, event identifier)`` fingerprints, or share a
    normalized website host. This handles English/Hebrew aliases such as
    Shablul Jazz Club / מועדון שבלול while avoiding empty venue rows and
    unrelated same-city venues.
    """
    by_id = {v["id"]: v for v in venues}
    parent = {v["id"]: v["id"] for v in venues}
    signatures: dict[int, set[tuple[str, str]]] = {v["id"]: set() for v in venues}
    event_venues: dict[tuple[str, str], set[int]] = defaultdict(set)
    rows = db.execute(text(
        "SELECT venue_id, start_date, LOWER(COALESCE(NULLIF(artist_name, ''), name)) "
        "FROM events WHERE venue_id IS NOT NULL"
    )).fetchall()
    for venue_id, start_date, ident in rows:
        if venue_id in signatures and ident:
            key = (str(start_date), str(ident).strip())
            signatures[venue_id].add(key)
            event_venues[key].add(venue_id)

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    def scripts(name: str | None) -> tuple[bool, bool]:
        s = name or ""
        return bool(re.search(r"[A-Za-z]", s)), bool(re.search(r"[\u0590-\u05ff]", s))

    by_city = {v["city_id"]: [] for v in venues}
    for v in venues:
        by_city[v["city_id"]].append(v["id"])
    city_of = {v["id"]: v["city_id"] for v in venues}
    pair_counts: dict[tuple[int, int], int] = defaultdict(int)
    for ids in event_venues.values():
        ids = list(ids)
        for i, a in enumerate(ids):
            for b in ids[i + 1:]:
                if city_of[a] == city_of[b]:
                    pair = (a, b) if a < b else (b, a)
                    pair_counts[pair] += 1

    for (a_id, b_id), shared_n in pair_counts.items():
        a, b = by_id[a_id], by_id[b_id]
        al, ah = scripts(a["name"])
        bl, bh = scripts(b["name"])
        cross_script = (al and bh and not ah and not bl) or (bl and ah and not bh and not al)
        pa, pb = _norm_venue_name(a["physical_city"]), _norm_venue_name(b["physical_city"])
        if not cross_script or _is_sub_venue_distinction(a["name"], b["name"]):
            continue
        if pa and pb and pa != pb:
            continue
        if shared_n < 3:
            continue
        union(a_id, b_id)

    groups: dict[int, list[dict]] = {}
    for v in venues:
        groups.setdefault(find(v["id"]), []).append(v)
    out = []
    for members in groups.values():
        if len(members) < 2:
            continue
        members.sort(key=lambda v: (
            0 if v["default_event_type_id"] is not None else 1,
            -(v["event_count"] or 0),
            -_populated_count(v),
            v["id"],
        ))
        canonical, dups = members[0], members[1:]
        for d in dups:
            shared = len(signatures[canonical["id"]] & signatures[d["id"]])
            d["_signals_to_canonical"] = {"cross_script": True, "shared_events": shared}
        out.append(([canonical], dups))
    return out


# ──────────────────────────────────────────────────────────────────────
# Apply
# ──────────────────────────────────────────────────────────────────────
def _alias_plan(db, canonical: dict, dups: list[dict]) -> list[dict]:
    """The name permutations to record against the canonical venue.

    Every dropped row's display name becomes an alias, so the ingest path
    (``find_venue_alias``) routes the next scrape of "גריי, יהוד-מונוסון"
    onto the surviving Gray row instead of re-creating the duplicate.
    The canonical's own name is included so the table holds the whole
    cluster.

    Aliases the dropped rows already own are NOT listed here — they are
    re-pointed in place by ``fold_cluster``, which keeps their key and so
    cannot collide. Listed names are skipped when the city already has
    that key, because it may belong to a venue outside this cluster.
    """
    city_id = canonical["city_id"]
    # ids come straight from the venues table, so inlining them is safe
    # and avoids an expanding bindparam for a list that is always short.
    dup_list = ", ".join(str(int(d["id"])) for d in dups) or "0"
    taken = {
        row[0] for row in db.execute(
            text(f"SELECT normalized_alias FROM venue_aliases "
                 f"WHERE city_id = :cid AND venue_id NOT IN ({dup_list})"),
            {"cid": city_id},
        ).fetchall()
    }
    out: list[dict] = []
    seen: set[str] = set()

    def add(alias: str | None, source: str) -> None:
        alias = (alias or "").strip()
        key = _norm_venue_name(alias)
        if not key or key in seen or key in taken:
            return
        seen.add(key)
        out.append({"alias": alias, "normalized_alias": key, "source": source})

    add(canonical["name"], "dedupe_venues:canonical")
    for d in dups:
        add(d["name"], "dedupe_venues:merged")
    return out


def fold_cluster(db, canonical: dict, dups: list[dict], *, apply: bool,
                 write_aliases: bool = True) -> dict:
    """Reassign events from each dup to canonical and delete the dup
    venue row. Single transaction across the cluster."""
    aliases = _alias_plan(db, canonical, dups) if write_aliases else []
    aliases_repointed = 0
    backfill = {}
    for col in BACKFILL_COLS:
        if canonical[col] in (None, ""):
            for d in dups:
                if d[col] not in (None, ""):
                    backfill[col] = d[col]
                    break
    moved_events = 0
    if apply:
        if backfill:
            sets = ", ".join(f"{c} = :{c}" for c in backfill)
            params = dict(backfill)
            params["id"] = canonical["id"]
            db.execute(text(f"UPDATE venues SET {sets} WHERE id = :id"), params)
        # Aliases first. The dropped rows' existing aliases have to be
        # re-pointed before the DELETE: they would otherwise be left
        # dangling (SQLite enforces ON DELETE CASCADE only when
        # foreign_keys is ON, and either way the spellings would stop
        # resolving — which is how the duplicates came back last time).
        # The update keeps (city_id, normalized_alias), so it cannot
        # collide with the unique index.
        display = canonical.get("_display_name")
        if display:
            # The old canonical spelling is already in ``aliases`` (the
            # canonical row's own name is always recorded), so search and
            # re-ingest keep resolving it after the rename.
            db.execute(text("UPDATE venues SET name = :n WHERE id = :id"),
                       {"n": display, "id": canonical["id"]})
            key = _norm_venue_name(display)
            if key and key not in {a["normalized_alias"] for a in aliases}:
                aliases.append({"alias": display, "normalized_alias": key,
                                "source": "dedupe_venues:display"})
        dup_list = ", ".join(str(int(d["id"])) for d in dups)
        res = db.execute(
            text(f"UPDATE venue_aliases SET venue_id = :new WHERE venue_id IN ({dup_list})"),
            {"new": canonical["id"]},
        )
        aliases_repointed = res.rowcount or 0
        for a in aliases:
            db.execute(text("""
                INSERT INTO venue_aliases (venue_id, city_id, alias, normalized_alias,
                                           source, confidence, created_at)
                VALUES (:vid, :cid, :alias, :key, :source, 1.0, CURRENT_TIMESTAMP)
                ON CONFLICT (city_id, normalized_alias) DO NOTHING
            """), {"vid": canonical["id"], "cid": canonical["city_id"],
                   "alias": a["alias"], "key": a["normalized_alias"], "source": a["source"]})
        for d in dups:
            res = db.execute(
                text("UPDATE events SET venue_id = :new WHERE venue_id = :old"),
                {"new": canonical["id"], "old": d["id"]},
            )
            moved_events += res.rowcount or 0
            db.execute(text("DELETE FROM venues WHERE id = :id"), {"id": d["id"]})
        db.commit()
    return {
        "canonical_id": canonical["id"],
        "canonical_name": canonical["name"],
        "display_name": canonical.get("_display_name") or canonical["name"],
        "city_id": canonical["city_id"],
        "city": canonical.get("city_name"),
        "duplicates": [
            {
                "id": d["id"],
                "name": d["name"],
                "events_under_dup": d["event_count"],
                "signals": d.get("_signals_to_canonical", {}),
            }
            for d in dups
        ],
        "aliases_written": [a["alias"] for a in aliases],
        "aliases_repointed": aliases_repointed,
        "backfilled_columns": list(backfill.keys()),
        "events_moved": moved_events,
    }


# ──────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--apply", action="store_true",
                        help="Write changes. Without this, runs as a dry-run.")
    parser.add_argument("--city-id", type=int, default=None,
                        help="Restrict to a single city (recommended for first run).")
    parser.add_argument("--min-cooccur", type=int, default=MIN_COOCCUR_DEFAULT,
                        help=f"Min same-(date, artist) events to flag the 'events' signal (default {MIN_COOCCUR_DEFAULT}).")
    parser.add_argument("--merge-pair", action="append", default=[],
                        metavar="A:B[:C…]",
                        help="Force-merge specific venue ids (colon-separated). Repeat for multiple groups. "
                             "Bypasses signal heuristic — use for known same-venue clusters the heuristic can't catch "
                             "(e.g. Hebrew/English name pairs).")
    parser.add_argument("--name-pass", action="store_true",
                        help="Use the indexed normalized-name/variant pass across all cities. "
                             "Containment variants require corroborating metadata or shared events.")
    parser.add_argument("--cross-language-pass", action="store_true",
                        help="Merge cross-script venue aliases using shared events or website host.")
    parser.add_argument("--website-pass", action="store_true",
                        help="Consolidate same-city venues that share a website host (ticketing/map "
                             "aggregators excluded) and whose names or events agree. Records every "
                             "merged spelling in venue_aliases.")
    parser.add_argument("--site-pass", action="store_true",
                        help="Same-site rule: rows sharing a venue's own site (or the same page on a "
                             "ticketing/municipal/chain site) in the same real city merge unless their "
                             "names name different cities. Also clears junk website_url values "
                             "(waze/youtube/maps) and gives website-less rows the site of an "
                             "exact-name twin. Use with --country.")
    parser.add_argument("--country", default=None,
                        help="Restrict to one country, e.g. --country Israel.")
    args = parser.parse_args()

    forced_groups: list[list[int]] = []
    for spec in args.merge_pair:
        try:
            ids = [int(x) for x in spec.split(":") if x]
        except ValueError:
            raise SystemExit(f"--merge-pair {spec!r}: ids must be integers.")
        if len(ids) < 2:
            raise SystemExit(f"--merge-pair {spec!r}: need at least 2 ids.")
        forced_groups.append(ids)

    mode = "APPLY" if args.apply else "DRY-RUN"
    scope = f"city_id={args.city_id}" if args.city_id else (args.country or "ALL CITIES")
    if (args.name_pass or args.cross_language_pass) and args.city_id is not None:
        parser.error("name/cross-language passes are all-city passes; omit --city-id")
    log.info(f"mode={mode} scope={scope} min_cooccur={args.min_cooccur} forced_groups={len(forced_groups)}")

    db = SessionLocal()
    try:
        venues = _load_venues(db, args.city_id, args.country)
        log.info(f"loaded {len(venues)} venue rows")
        site_stats = None
        if args.website_pass or args.site_pass:
            cooccur = {}
        else:
            cooccur = _cooccurrence_pairs(db, args.city_id, args.min_cooccur)
            log.info(f"co-occurrence pairs (≥{args.min_cooccur}): {len(cooccur)}")
        if args.site_pass:
            clusters, site_stats = _build_site_clusters(db, venues, apply=args.apply)
            log.info(f"site moves: {len(site_stats['site_moves'])}, "
                     f"junk website_url cleared: {len(site_stats['junk_urls_cleared'])}, "
                     f"websites backfilled from twins: {len(site_stats['websites_backfilled'])}, "
                     f"renamed to one display name: {len(site_stats['renamed'])}")
        elif args.website_pass:
            clusters = _build_website_clusters(db, venues)
        elif args.cross_language_pass:
            clusters = _build_cross_language_clusters(db, venues)
        elif args.name_pass:
            clusters = _build_name_clusters(db, venues)
        else:
            clusters = _build_clusters(venues, cooccur, forced_groups=forced_groups)
        log.info(f"duplicate clusters: {len(clusters)}")
        if not clusters:
            log.info("nothing to do.")
            return

        plan = []
        for canon_list, dups in clusters:
            canonical = canon_list[0]
            log.info(
                f"cluster keep id={canonical['id']} {canonical['name']!r} "
                f"(events={canonical['event_count']}, "
                f"default_type_id={canonical['default_event_type_id']})"
            )
            for d in dups:
                sig = d.get("_signals_to_canonical", {})
                log.info(
                    f"  drop id={d['id']} {d['name']!r} "
                    f"(events={d['event_count']}) signals={sig}"
                )
            res = fold_cluster(db, canonical, dups, apply=args.apply)
            plan.append(res)

        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        suffix = (("site_" if args.site_pass else
                   "website_" if args.website_pass else
                   "cross_language_" if args.cross_language_pass else
                   "name_" if args.name_pass else "")
                  + ("apply" if args.apply else "dryrun"))
        audit_path = ROOT / "data" / f"dedupe_venues_{ts}_{suffix}.json"
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        audit = {"site_cleanup": site_stats, "clusters": plan} if site_stats is not None else plan
        audit_path.write_text(json.dumps(audit, ensure_ascii=False, indent=2))
        log.info(f"audit written: {audit_path}")
        if not args.apply:
            log.info("DRY-RUN — re-run with --apply to write.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
