"""Detect duplicate / nested city rows for consolidation review.

Outputs a CSV of candidate pairs that look like the same physical city
under different labels (aliases) or sub-areas of a larger city (nested).
The user reviews + edits the CSV; scripts/apply_city_consolidation.py
reads the approved rows and writes canonical_city_id / parent_city_id
links on the cities table.

Detection heuristics (all gated to same-country pairs — different
countries with similar names are real separate cities, e.g. London UK
vs London Canada):

  Alias candidates (same physical city, different label):
    - Geo near-match: lat/lon within ALIAS_GEO_KM AND name root matches
      AND no nested-direction prefix.
    - Pattern match: "City Of X" ≡ "X" (London ≡ City Of London),
      "X-Yafo" ≡ "X" (Tel Aviv-Yafo ≡ Tel Aviv), "X City" ≡ "X",
      "Tower Of X" ≡ "X" (landmark rows; technically delete-candidates
      but alias to the proper city is safer).
    - Bare-form vs labeled: same name + same country, one has a state
      and the other doesn't (e.g. "New York City, NY" ≡ "New York
      City, <no state>"). Prefer the row WITH state metadata as
      canonical.

  Nested candidates (sub-area of a larger city):
    - Direction-prefixed: "North London" → "London", "East New York"
      → "New York". Requires the parent name to exist as a separate
      row in the same country AND geo within NESTED_GEO_KM of the
      parent.
    - "Greater X" pattern is treated as a metro grouping (different
      mechanism via the existing metro_areas table) — skipped here.

Canonical choice when a pair is detected:
  1. Prefer the row with the higher upcoming-event count (where users
     have actually been booking — minimizes downstream migration).
  2. Tie-break: prefer the row WITH a state field set over one without.
  3. Tie-break: prefer the row whose name is the BARE form (no
     "City Of" / "-Yafo" / "City" prefix-suffix).
  4. Tie-break: lower id (older row, more established).

Confidence score in [0, 1]:
  1.0     same-coords + alias-pattern match
  0.9     geo < 2km + name root match
  0.8     alias-pattern match (no geo cross-check needed)
  0.7     geo < 5km + name root match
  0.6     direction-prefix nested + geo within metro
  0.4     plausible but weak — needs human review
  <0.4    not output

Usage:
    PYTHONPATH=. python3 scripts/detect_city_duplicates.py
    PYTHONPATH=. python3 scripts/detect_city_duplicates.py --min-confidence 0.6
    PYTHONPATH=. python3 scripts/detect_city_duplicates.py -o /tmp/dupes.csv
"""
from __future__ import annotations

import argparse
import csv
import logging
import math
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(str(ROOT / ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("detect_city_duplicates")

from app.database import SessionLocal  # noqa: E402

# ── Tunables ───────────────────────────────────────────────────────
ALIAS_GEO_KM = 5.0       # same physical city when this close
NESTED_GEO_KM = 30.0     # plausible sub-area when within this radius
MIN_CONFIDENCE_DEFAULT = 0.5

# ── Patterns ───────────────────────────────────────────────────────
# Cardinal-direction prefixes that signal a sub-area of a larger city.
# Order matters for some compound directions ("South West" before "South").
_NESTED_PREFIXES = (
    "north west", "north east", "south west", "south east",
    "north", "south", "east", "west",
    "central", "downtown", "old", "outer", "inner", "upper", "lower",
)

# Alias patterns: regex → replacement. First match wins. The replacement
# is the BARE-form name we expect to find as the canonical row.
_ALIAS_PATTERNS = [
    (re.compile(r"^city of (.+)$", re.I),          r"\1"),
    (re.compile(r"^(.+) city$",     re.I),         r"\1"),
    (re.compile(r"^(.+)-yafo$",     re.I),         r"\1"),
    (re.compile(r"^(.+) area$",     re.I),         r"\1"),
    (re.compile(r"^tower of (.+)$", re.I),         r"\1"),
    (re.compile(r"^old (.+)$",      re.I),         r"\1"),
    (re.compile(r"^the (.+)$",      re.I),         r"\1"),
]

# "Greater X" looks alias-y but in this codebase it's already covered
# by the metro_areas table — treat as a metro grouping, not a city
# alias. Skip so we don't fight the existing metro layer.
_GREATER_RE = re.compile(r"^greater\s+", re.I)


@dataclass
class CityRow:
    id: int
    name: str
    country: str
    state: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    upcoming_events: int = 0

    @property
    def label(self) -> str:
        s = f"{self.name}, {self.country}"
        if self.state:
            s += f" ({self.state})"
        return s

    @property
    def has_geo(self) -> bool:
        return self.latitude is not None and self.longitude is not None


def haversine_km(a: CityRow, b: CityRow) -> Optional[float]:
    """Great-circle distance in km, or None if either side lacks coords."""
    if not (a.has_geo and b.has_geo):
        return None
    lat1, lon1, lat2, lon2 = (
        math.radians(a.latitude), math.radians(a.longitude),
        math.radians(b.latitude), math.radians(b.longitude),
    )
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    h = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * 6371.0 * math.asin(math.sqrt(h))


def normalize_name(s: str) -> str:
    """Lowercase + strip + collapse whitespace. No diacritic stripping yet."""
    return re.sub(r"\s+", " ", (s or "").lower().strip())


def strip_alias_decoration(name: str) -> Optional[str]:
    """Return the bare name if `name` matches a known alias pattern, else None."""
    n = normalize_name(name)
    for pat, repl in _ALIAS_PATTERNS:
        m = pat.match(n)
        if m:
            return normalize_name(pat.sub(repl, n))
    return None


def strip_nested_prefix(name: str) -> Optional[str]:
    """Return the bare name if `name` starts with a cardinal direction
    prefix (suggesting a sub-area), else None. Tries longest prefix first.
    """
    n = normalize_name(name)
    for prefix in _NESTED_PREFIXES:
        if n.startswith(prefix + " "):
            return n[len(prefix) + 1:]
    return None


def load_cities(db) -> list[CityRow]:
    """Pull all cities + their upcoming event counts in one go."""
    log.info("Loading cities + event counts …")
    from sqlalchemy import text
    rows = db.execute(text("""
        SELECT c.id, c.name, c.country, c.state, c.latitude, c.longitude,
               COUNT(DISTINCT CASE WHEN e.start_date >= date('now') THEN e.id END) AS upcoming
        FROM cities c
        LEFT JOIN venues v ON v.city_id = c.id
        LEFT JOIN events e ON e.venue_id = v.id
        GROUP BY c.id
    """)).fetchall()
    cities = [
        CityRow(id=r[0], name=r[1], country=r[2], state=r[3],
                latitude=r[4], longitude=r[5], upcoming_events=r[6] or 0)
        for r in rows
    ]
    log.info(f"  loaded {len(cities):,} city rows")
    return cities


def pick_canonical(a: CityRow, b: CityRow) -> tuple[CityRow, CityRow]:
    """Return (canonical, alias) — the row to KEEP vs the one to point at it.

    Order of tie-breakers documented in the module docstring."""
    # 1. Higher upcoming event count wins.
    if a.upcoming_events != b.upcoming_events:
        return (a, b) if a.upcoming_events > b.upcoming_events else (b, a)
    # 2. With-state wins (more metadata).
    if bool(a.state) != bool(b.state):
        return (a, b) if a.state else (b, a)
    # 3. Bare-form wins (no "City Of X" / "X-Yafo" decoration).
    a_decorated = strip_alias_decoration(a.name) is not None
    b_decorated = strip_alias_decoration(b.name) is not None
    if a_decorated != b_decorated:
        return (b, a) if a_decorated else (a, b)
    # 4. Lower id wins (older row).
    return (a, b) if a.id < b.id else (b, a)


def detect_pairs(cities: list[CityRow]) -> list[dict]:
    """Run all detection passes and return a list of candidate dicts."""
    # Bucket by country — same-name cities in different countries are
    # never the same physical city.
    by_country: dict[str, list[CityRow]] = defaultdict(list)
    for c in cities:
        by_country[c.country].append(c)

    # Index each country's cities by normalized bare name for O(1)
    # lookups during the alias / nested passes.
    candidates: list[dict] = []
    for country, group in by_country.items():
        by_name: dict[str, list[CityRow]] = defaultdict(list)
        for c in group:
            by_name[normalize_name(c.name)].append(c)

        seen_pairs: set[tuple[int, int]] = set()

        def emit(a: CityRow, b: CityRow, rel: str, score: float, signal: str):
            canonical, alias = pick_canonical(a, b)
            key = (min(canonical.id, alias.id), max(canonical.id, alias.id))
            if key in seen_pairs:
                return
            seen_pairs.add(key)
            dist = haversine_km(canonical, alias)
            candidates.append({
                "relationship": rel,
                "confidence": round(score, 2),
                "canonical_id": canonical.id,
                "canonical_label": canonical.label,
                "canonical_events": canonical.upcoming_events,
                "alias_id": alias.id,
                "alias_label": alias.label,
                "alias_events": alias.upcoming_events,
                "distance_km": round(dist, 2) if dist is not None else None,
                "signal": signal,
            })

        # ── Pass 1: alias-pattern match (City Of X ≡ X, etc.) ──────
        for c in group:
            bare = strip_alias_decoration(c.name)
            if not bare:
                continue
            for target in by_name.get(bare, []):
                if target.id == c.id:
                    continue
                # Geo cross-check when both have coords — if they're
                # 50km+ apart, the pattern is misleading (two unrelated
                # cities that happen to share a stem). Skip.
                dist = haversine_km(c, target)
                if dist is not None and dist > 50:
                    continue
                score = 1.0 if (dist is not None and dist < 1.0) else 0.8
                emit(c, target, "alias", score, signal=f"pattern-match ({c.name} ↔ {target.name})")

        # ── Pass 2: bare-name + same country, one with state one without ──
        # e.g. "New York City, NY" ≡ "New York City, <no state>"
        for nm, rows_with_name in by_name.items():
            if len(rows_with_name) < 2:
                continue
            with_state = [r for r in rows_with_name if r.state]
            without_state = [r for r in rows_with_name if not r.state]
            for ws in with_state:
                for nos in without_state:
                    dist = haversine_km(ws, nos)
                    if dist is None or dist < 5.0:
                        score = 0.95
                        emit(ws, nos, "alias", score,
                             signal=f"bare-vs-state ({ws.name}/{ws.state} ↔ {nos.name})")

        # ── Pass 3: geo near-duplicate even with different names ──
        # Same country, lat/lon within ALIAS_GEO_KM, and name root
        # matches by simple equality of normalized forms (catches
        # things alias-patterns missed: misspellings, accent variants).
        # Skip explicit "Greater X" rows here — they're metro groupings.
        geo_cities = [c for c in group if c.has_geo and not _GREATER_RE.match(c.name or "")]
        for i, a in enumerate(geo_cities):
            for b in geo_cities[i + 1:]:
                dist = haversine_km(a, b)
                if dist is None or dist > ALIAS_GEO_KM:
                    continue
                # Same coords + names share a long common prefix
                # (handles "Tel Aviv" / "Tel Aviv-Yafo" if pattern
                # pass missed it).
                na, nb = normalize_name(a.name), normalize_name(b.name)
                if na == nb:
                    continue  # Already covered by pass 2 if applicable.
                # Common prefix or substring of length ≥4 — heuristic.
                prefix_len = 0
                for ca, cb in zip(na, nb):
                    if ca == cb:
                        prefix_len += 1
                    else:
                        break
                if prefix_len < 4:
                    continue
                score = 0.9 if dist < 2.0 else 0.7
                emit(a, b, "alias", score,
                     signal=f"geo<{ALIAS_GEO_KM}km + prefix match ({a.name} ↔ {b.name})")

        # ── Pass 4: nested (direction-prefixed sub-areas) ──────────
        for c in group:
            bare = strip_nested_prefix(c.name)
            if not bare:
                continue
            for parent in by_name.get(bare, []):
                if parent.id == c.id:
                    continue
                # Geo cross-check — sub-area should be within the
                # parent's metro radius.
                dist = haversine_km(c, parent)
                if dist is not None and dist > NESTED_GEO_KM:
                    continue
                score = 0.7 if (dist is not None and dist < 15.0) else 0.6
                # In nested relationships, the parent is the "canonical"
                # in our emit-record sense (the bigger surface that
                # absorbs descendants in events queries). The sub-area
                # row keeps its own id and stays selectable — the
                # apply script will set parent_city_id (not
                # canonical_city_id) for nested rows.
                emit(parent, c, "nested", score,
                     signal=f"direction-prefix ({c.name} → {parent.name})")

    candidates.sort(key=lambda d: (-d["confidence"], d["relationship"]))
    return candidates


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--min-confidence", type=float, default=MIN_CONFIDENCE_DEFAULT,
                    help=f"Suppress candidates below this score (default {MIN_CONFIDENCE_DEFAULT}).")
    ap.add_argument("-o", "--output", default="data/city_duplicates.csv",
                    help="Output CSV path.")
    args = ap.parse_args()

    db = SessionLocal()
    try:
        cities = load_cities(db)
        log.info("Detecting candidate pairs …")
        candidates = detect_pairs(cities)
        kept = [c for c in candidates if c["confidence"] >= args.min_confidence]
        log.info(f"  {len(candidates):,} raw candidates / {len(kept):,} above confidence ≥ {args.min_confidence}")

        # Breakdown
        from collections import Counter
        rel_counts = Counter(c["relationship"] for c in kept)
        log.info(f"  relationships: {dict(rel_counts)}")

        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        with out.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=[
                "relationship", "confidence", "approved",
                "canonical_id", "canonical_label", "canonical_events",
                "alias_id", "alias_label", "alias_events",
                "distance_km", "signal",
            ])
            w.writeheader()
            for c in kept:
                row = dict(c)
                # "approved" column starts empty — the operator marks 'y'
                # to confirm before apply_city_consolidation.py acts on it.
                row["approved"] = ""
                w.writerow(row)

        log.info(f"  wrote {out}")
        log.info("Review the CSV, mark `approved` column 'y' on rows to apply,")
        log.info("then run: PYTHONPATH=. python3 scripts/apply_city_consolidation.py")
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
