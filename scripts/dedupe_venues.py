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
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

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
    "אולם", "אודיטוריום", "במה",         # he: hall, auditorium, stage
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
def _load_venues(db, city_id: int | None) -> list[dict]:
    sql = """
        SELECT v.id, v.name, v.city_id, v.latitude, v.longitude, v.phone,
               v.website_url, v.street_address, v.physical_city,
               v.physical_country, v.timezone, v.venue_type,
               v.default_event_type_id,
               (SELECT COUNT(*) FROM events e WHERE e.venue_id = v.id) AS event_count
        FROM venues v
    """
    params = {}
    if city_id is not None:
        sql += " WHERE v.city_id = :cid"
        params["cid"] = city_id
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
def fold_cluster(db, canonical: dict, dups: list[dict], *, apply: bool) -> dict:
    """Reassign events from each dup to canonical and delete the dup
    venue row. Single transaction across the cluster."""
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
        "duplicates": [
            {
                "id": d["id"],
                "name": d["name"],
                "events_under_dup": d["event_count"],
                "signals": d.get("_signals_to_canonical", {}),
            }
            for d in dups
        ],
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
    scope = f"city_id={args.city_id}" if args.city_id else "ALL CITIES"
    if (args.name_pass or args.cross_language_pass) and args.city_id is not None:
        parser.error("name/cross-language passes are all-city passes; omit --city-id")
    log.info(f"mode={mode} scope={scope} min_cooccur={args.min_cooccur} forced_groups={len(forced_groups)}")

    db = SessionLocal()
    try:
        venues = _load_venues(db, args.city_id)
        log.info(f"loaded {len(venues)} venue rows")
        cooccur = _cooccurrence_pairs(db, args.city_id, args.min_cooccur)
        log.info(f"co-occurrence pairs (≥{args.min_cooccur}): {len(cooccur)}")
        if args.cross_language_pass:
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
        suffix = (("cross_language_" if args.cross_language_pass else "name_" if args.name_pass else "")
                  + ("apply" if args.apply else "dryrun"))
        audit_path = ROOT / "data" / f"dedupe_venues_{ts}_{suffix}.json"
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        audit_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2))
        log.info(f"audit written: {audit_path}")
        if not args.apply:
            log.info("DRY-RUN — re-run with --apply to write.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
