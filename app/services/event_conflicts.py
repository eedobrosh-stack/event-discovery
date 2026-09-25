"""Same show, several rows — propagate what one row knows, fold the rest.

Two product rules (Eedo, 2026-09-25, from "ג׳ימבו ג׳יי ותזמורת הרחוב" at
Barby: four rows a night — makore 20:30 at an address-venue, barby and
tickchak 21:00, muzi 22:00 — only two carried the artist, one had no
category / format):

A. Propagation. When events with the same title in the same city carry
   exactly one artist between them, every row of that title without an
   artist gets it, plus the siblings' YouTube link, and a row with no
   category / format gets the siblings' event types.

B. Time conflicts. Two rows on the same day in the same city are one
   show when they share a title, or share an artist (then also the same
   venue or the exact same time), and they are no more than
   ``WINDOW_MIN`` apart. A time difference between sources is a parsing
   artefact (doors vs show, time-zone slips), so the EARLIEST time is
   kept; the other rows' artist, links, price, image and event types are
   folded into it and the row moves to the cluster's best-established
   venue (most events), then the others are deleted.

   Guard: rows from ONE source at different times or venues are real
   separate performances (matinee + evening, a tour's meeting points)
   and are never folded; one source only folds with itself at the exact
   same time and venue.

Where it runs:
  * ingest — ``find_time_conflict`` in CollectorRegistry._save_events:
    an incoming row that conflicts with an existing one enriches it
    (and moves it to the earlier time) instead of being created, so a
    folded row is not re-created by the next scrape;
  * ``resolve_event_conflicts_job`` every 6 h (propagate, then fold);
  * scripts/resolve_event_conflicts.py for dry runs / one-offs;
  * the supercaly-qa checks report what is left.
"""
from __future__ import annotations

import logging
from collections import Counter, defaultdict
from datetime import date

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.models import Event
from app.services.dedup import normalize_title

log = logging.getLogger(__name__)

WINDOW_MIN = 180

FOLD_COLS = [
    "artist_name", "artist_youtube_channel", "artist_popularity", "artist_spotify_url",
    "purchase_link", "price", "price_currency", "description", "image_url",
]


def _minutes(t: str | None) -> int | None:
    if not t:
        return None
    try:
        h, m = t.strip()[:5].split(":")
        return int(h) * 60 + int(m)
    except (ValueError, AttributeError):
        return None


def _artist_key(a: str | None) -> str:
    return normalize_title(a)


def _city_keys(db: Session) -> dict[int, int]:
    """venue_id → canonical city id (alias City rows resolved)."""
    canon = dict(db.execute(text(
        "SELECT id, COALESCE(canonical_city_id, id) FROM cities")).fetchall())
    return {vid: canon.get(cid, cid) for vid, cid in db.execute(text(
        "SELECT id, city_id FROM venues")).fetchall()}


def _conflict(a: dict, b: dict, via: str) -> bool:
    ta, tb = _minutes(a["start_time"]), _minutes(b["start_time"])
    if a["scrape_source"] == b["scrape_source"] and (
            a["start_time"] != b["start_time"] or a["venue_id"] != b["venue_id"]):
        return False                       # one source: two times / places = two shows
    if ta is not None and tb is not None and abs(ta - tb) > WINDOW_MIN:
        return False
    if via == "artist" and not (a["venue_id"] == b["venue_id"] or a["start_time"] == b["start_time"]):
        return False                       # same artist elsewhere needs same venue / time
    return True


# ── B at ingest ─────────────────────────────────────────────────────────
def find_time_conflict(db: Session, *, name: str | None, artist: str | None,
                       start_date: date | None, start_time: str | None,
                       venue_id: int | None, city_id: int | None,
                       scrape_source: str | None, cache: dict | None = None) -> Event | None:
    """An existing event the incoming row is the same show as (rule B).
    ``cache`` (per ingest batch) holds each (date, city)'s candidates."""
    if start_date is None or city_id is None or not (name or artist):
        return None
    ck = (start_date, city_id)
    if cache is not None and ck in cache:
        rows = cache[ck]
    else:
        rows = _day_rows(db, start_date, city_id)
        if cache is not None:
            cache[ck] = rows
    return _pick_conflict(db, rows, name=name, artist=artist, start_time=start_time,
                          venue_id=venue_id, scrape_source=scrape_source)


def _day_rows(db: Session, start_date: date, city_id: int) -> list:
    return db.execute(text("""
        SELECT e.id, e.name, e.artist_name, e.start_time, e.scrape_source, e.venue_id
        FROM events e JOIN venues v ON v.id = e.venue_id JOIN cities c ON c.id = v.city_id
        WHERE e.start_date = :d AND e.sport IS NULL
          AND COALESCE(c.canonical_city_id, c.id) = :city
    """), {"d": start_date, "city": city_id}).fetchall()


def _pick_conflict(db: Session, rows: list, *, name, artist, start_time, venue_id,
                   scrape_source) -> Event | None:
    inc = {"start_time": start_time, "scrape_source": scrape_source, "venue_id": venue_id}
    nk, ak = normalize_title(name), _artist_key(artist)
    best = None
    for rid, rname, rartist, rtime, rsrc, rvenue in rows:
        cand = {"start_time": rtime, "scrape_source": rsrc, "venue_id": rvenue}
        via = ("name" if nk and normalize_title(rname) == nk else
               "artist" if ak and _artist_key(rartist) == ak else None)
        if via and _conflict(inc, cand, via):
            if best is None or (_minutes(rtime) or 9999) < (_minutes(best[1]) or 9999):
                best = (rid, rtime)
    return db.get(Event, best[0]) if best else None


def absorb_incoming(existing: Event, raw) -> bool:
    """Fold an incoming RawEvent into the existing row it conflicts with.
    Returns True when the row changed."""
    changed = False
    ti, te = _minutes(raw.start_time), _minutes(existing.start_time)
    if ti is not None and (te is None or ti < te):
        existing.start_time = raw.start_time
        if getattr(raw, "end_time", None):
            existing.end_time = raw.end_time
        changed = True
    for col, raw_col in (("artist_name", "artist_name"), ("artist_youtube_channel", "artist_youtube_channel"),
                         ("purchase_link", "purchase_link"), ("price", "price"),
                         ("image_url", "image_url"), ("description", "description")):
        v = getattr(raw, raw_col, None)
        if v not in (None, "") and getattr(existing, col) in (None, ""):
            setattr(existing, col, v)
            changed = True
    return changed


# ── A: propagation ──────────────────────────────────────────────────────
def propagate_same_title(db: Session, *, apply: bool, since: date | None = None,
                         country: str | None = None) -> list[dict]:
    since = since or date.today()
    city_of = _city_keys(db)
    params = {"since": since}
    where = "e.start_date >= :since AND e.sport IS NULL AND e.venue_id IS NOT NULL"
    if country:
        where += " AND e.venue_id IN (SELECT v.id FROM venues v JOIN cities c ON c.id = v.city_id WHERE c.country = :country)"
        params["country"] = country
    rows = db.execute(text(f"""
        SELECT e.id, e.name, e.artist_name, e.artist_youtube_channel, e.venue_id,
               (SELECT GROUP_CONCAT(event_type_id) FROM event_event_types WHERE event_id = e.id)
        FROM events e WHERE {where}"""), params).fetchall()
    groups: dict[tuple, list] = defaultdict(list)
    for r in rows:
        groups[(city_of.get(r[4]), normalize_title(r[1]))].append(r)
    plan = []
    for (city, key), grp in groups.items():
        if not key or len(grp) < 2:
            continue
        artists = Counter((r[2] or "").strip() for r in grp if (r[2] or "").strip())
        by_key = {_artist_key(a) for a in artists}
        types = Counter(r[5] for r in grp if r[5])
        yt = Counter(r[3] for r in grp if r[3])
        artist = artists.most_common(1)[0][0] if len(by_key) == 1 else None
        type_set = types.most_common(1)[0][0] if types else None
        for r in grp:
            ch = {}
            if artist and not (r[2] or "").strip():
                ch["artist_name"] = artist
                if not r[3] and yt:
                    ch["artist_youtube_channel"] = yt.most_common(1)[0][0]
            if type_set and not r[5]:
                ch["event_type_ids"] = [int(x) for x in type_set.split(",")]
            if ch:
                plan.append({"event_id": r[0], "name": r[1], **ch})
    if apply:
        for p in plan:
            sets = {k: v for k, v in p.items() if k in ("artist_name", "artist_youtube_channel")}
            if sets:
                db.execute(text("UPDATE events SET " + ", ".join(f"{k} = :{k}" for k in sets)
                                + " WHERE id = :id"), {**sets, "id": p["event_id"]})
            for tid in p.get("event_type_ids", []):
                db.execute(text("INSERT OR IGNORE INTO event_event_types (event_id, event_type_id) "
                                "VALUES (:e, :t)"), {"e": p["event_id"], "t": tid})
        db.commit()
    return plan


# ── B: sweep ────────────────────────────────────────────────────────────
def resolve_time_conflicts(db: Session, *, apply: bool, since: date | None = None,
                           country: str | None = None) -> list[dict]:
    since = since or date.today()
    city_of = _city_keys(db)
    venue_events = dict(db.execute(text(
        "SELECT venue_id, COUNT(*) FROM events WHERE venue_id IS NOT NULL GROUP BY venue_id")).fetchall())
    params = {"since": since}
    where = "e.start_date >= :since AND e.sport IS NULL AND e.venue_id IS NOT NULL"
    if country:
        where += " AND e.venue_id IN (SELECT v.id FROM venues v JOIN cities c ON c.id = v.city_id WHERE c.country = :country)"
        params["country"] = country
    rows = [dict(r._mapping) for r in db.execute(text(f"""
        SELECT e.id, e.name, e.artist_name, e.start_date, e.start_time, e.scrape_source,
               e.venue_id, e.purchase_link
        FROM events e WHERE {where}"""), params)]
    by_day: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        by_day[(r["start_date"], city_of.get(r["venue_id"]))].append(r)

    plan = []
    for (_, _city), day in by_day.items():
        if len(day) < 2:
            continue
        parent = {r["id"]: r["id"] for r in day}

        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        why: dict[int, str] = {}
        for via, keyf in (("name", lambda r: normalize_title(r["name"])),
                          ("artist", lambda r: _artist_key(r["artist_name"]))):
            buckets = defaultdict(list)
            for r in day:
                k = keyf(r)
                if k:
                    buckets[k].append(r)
            for grp in buckets.values():
                for i, a in enumerate(grp):
                    for b in grp[i + 1:]:
                        if _conflict(a, b, via) and find(a["id"]) != find(b["id"]):
                            parent[find(a["id"])] = find(b["id"])
                            why.setdefault(a["id"], via); why.setdefault(b["id"], via)
        clusters = defaultdict(list)
        for r in day:
            clusters[find(r["id"])].append(r)
        for members in clusters.values():
            if len(members) < 2:
                continue
            members.sort(key=lambda r: (_minutes(r["start_time"]) if _minutes(r["start_time"]) is not None else 9999,
                                        0 if r["purchase_link"] else 1, 0 if r["artist_name"] else 1, r["id"]))
            keep, drop = members[0], members[1:]
            venue = max({m["venue_id"] for m in members}, key=lambda v: venue_events.get(v, 0))
            plan.append({"keep_id": keep["id"], "name": keep["name"], "date": str(keep["start_date"]),
                         "keep_time": keep["start_time"], "keep_source": keep["scrape_source"],
                         "venue_id": venue, "matched_on": why.get(keep["id"], "name"),
                         "drop": [{"id": d["id"], "time": d["start_time"], "source": d["scrape_source"],
                                   "artist": d["artist_name"]} for d in drop]})
    if apply:
        for p in plan:
            keep = db.get(Event, p["keep_id"])
            dups = [db.get(Event, d["id"]) for d in p["drop"]]
            dups = [d for d in dups if d is not None]
            if keep is None or not dups:
                continue
            for col in FOLD_COLS:
                if getattr(keep, col) in (None, ""):
                    for d in dups:
                        if getattr(d, col) not in (None, ""):
                            setattr(keep, col, getattr(d, col)); break
            have = {t.id for t in keep.event_types}
            for d in dups:
                for t in d.event_types:
                    if t.id not in have:
                        keep.event_types.append(t); have.add(t.id)
            if p["venue_id"] and keep.venue_id != p["venue_id"]:
                keep.venue_id = p["venue_id"]
            for d in dups:
                db.execute(text("DELETE FROM event_themes WHERE event_id = :e"), {"e": d.id})
                db.delete(d)
            db.commit()
    return plan
