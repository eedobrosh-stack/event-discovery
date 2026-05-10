"""Brave-search fallback for artist YouTube channel URLs.

Companion to app.scheduler.jobs.enrich_youtube_job (which uses the
YouTube Data API). When the YouTube API misses an artist (no exact
channel match, throttled, or quota'd), this script tries a generic
Brave web search constrained to youtube.com and accepts the first
result that:

  • points at a real channel URL (youtube.com/@HANDLE,
    /channel/UC..., /c/CUSTOM, or /user/NAME — NOT a video or
    playlist URL); AND
  • whose handle or page title contains the artist's name (as a
    normalized substring) — defends against the search engine
    ranking a wrong artist with a similar name above the right
    one.

On a hit, writes ``Event.artist_youtube_channel`` for every event
with that artist (case-insensitive). On a miss, leaves the column
alone — the YouTube Data API cron's empty-string sentinel is
preserved, and the YouTube API job can keep retrying without our
overwrite.

Caching pattern mirrors scripts/improve_genre_via_brave.py — JSONL
append cache at scripts/_brave_youtube_cache.jsonl, keyed by lower-
cased artist name. Re-runs after partial caps don't re-spend Brave
quota on artists already attempted.

Cost: ~$0.005 per Brave query (Search plan), one query per artist
regardless of how many events that artist has. With ~10-15k
unmatched artists in the catalog and --limit 200/night, full
coverage takes 50-75 nights at ~$1/night. Cache amortises everything
beyond that.

Usage:
    PYTHONPATH=. python3 scripts/enrich_youtube_via_brave.py
    PYTHONPATH=. python3 scripts/enrich_youtube_via_brave.py --apply
    PYTHONPATH=. python3 scripts/enrich_youtube_via_brave.py --apply --limit 50
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(str(ROOT / ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("enrich_youtube_via_brave")

from sqlalchemy import func as _func, or_  # noqa: E402

from app.database import SessionLocal  # noqa: E402
from app.models import Event  # noqa: E402
from app.extractors.discovery_search import brave_search  # noqa: E402

# ── Cache ──────────────────────────────────────────────────────────────
CACHE_PATH = ROOT / "scripts" / "_brave_youtube_cache.jsonl"

# 1.1s pacing matches the genre-classifier script — Brave's free tier
# is 1qps; paid plans accept higher but staying conservative keeps the
# cost predictable and avoids any hidden per-second cap.
BRAVE_QPS_DELAY = 1.1


def load_cache() -> dict[str, dict]:
    """Read JSONL cache → {artist_lower → {results, channel_url}}."""
    cache: dict[str, dict] = {}
    if not CACHE_PATH.exists():
        return cache
    with CACHE_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            artist = rec.get("artist")
            if artist:
                cache[artist] = {
                    "results": rec.get("results") or [],
                    "channel_url": rec.get("channel_url"),
                }
    return cache


def append_cache(artist: str, results: list[dict], channel_url: str | None) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    rec = {
        "artist": artist,
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "channel_url": channel_url,
        "results": results,
    }
    with CACHE_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())


# ── Channel-URL validation ────────────────────────────────────────────
# Channel URL shapes YouTube exposes today (modern + legacy):
#   /@HANDLE          modern handle URL (post-2022)
#   /channel/UCxxxxx  permanent channel ID
#   /c/CUSTOM         legacy custom URL
#   /user/USERNAME    very-legacy user URL
#
# Reject everything else — /watch (video), /playlist (playlist),
# /results (search), /feed (subscriptions), /about, /shorts, etc.
_CHANNEL_PATTERNS = [
    re.compile(r"^https?://(?:www\.|m\.)?youtube\.com/@([\w\-.]+)/?(?:\?.*)?$", re.I),
    re.compile(r"^https?://(?:www\.|m\.)?youtube\.com/channel/(UC[\w\-]+)/?(?:\?.*)?$", re.I),
    re.compile(r"^https?://(?:www\.|m\.)?youtube\.com/c/([\w\-.]+)/?(?:\?.*)?$", re.I),
    re.compile(r"^https?://(?:www\.|m\.)?youtube\.com/user/([\w\-.]+)/?(?:\?.*)?$", re.I),
]


def _channel_handle(url: str) -> str | None:
    """Returns the handle/id captured group when ``url`` is a YouTube
    channel URL, else None. Strips leading/trailing whitespace."""
    if not url:
        return None
    s = url.strip()
    for pat in _CHANNEL_PATTERNS:
        m = pat.match(s)
        if m:
            return m.group(1)
    return None


def _normalize(s: str) -> str:
    """Lowercase, strip non-word chars (incl. accents/punct/whitespace).
    Used to compare artist names against channel handles / titles
    without false-rejecting on punctuation differences ("Ne-Yo" vs
    "neyo")."""
    return re.sub(r"\W+", "", (s or "").lower(), flags=re.UNICODE)


def _artist_match(artist: str, handle: str | None, title: str | None) -> bool:
    """Does the channel's handle OR the result title contain the
    artist's normalized name? Substring match — accepts "TaylorSwift"
    handle for artist "Taylor Swift", or title "Taylor Swift -
    Official YouTube Channel"."""
    a_norm = _normalize(artist)
    if len(a_norm) < 3:
        # Single-char / two-char artist names are too risky to match
        # via substring (would accept any channel containing those
        # letters). Skip rather than write a wrong URL.
        return False
    if handle and a_norm in _normalize(handle):
        return True
    if title and a_norm in _normalize(title):
        return True
    return False


# ── Brave search + channel pick ───────────────────────────────────────
def find_channel_for_artist(artist: str) -> tuple[str | None, list[dict]]:
    """Brave-search for the artist's YouTube channel. Returns
    (channel_url_or_None, results_for_audit). Empty results list means
    Brave failed (rate limit, transient error)."""
    query = f'"{artist}" site:youtube.com'
    hits = brave_search(query, n=10)
    results = [
        {"url": h.url, "title": h.title, "snippet": h.snippet}
        for h in hits
    ]
    for h in results:
        url = h.get("url") or ""
        handle = _channel_handle(url)
        if not handle:
            continue
        if _artist_match(artist, handle, h.get("title")):
            return url, results
    return None, results


# ── Targeting ─────────────────────────────────────────────────────────
def _select_artists_pending(db, limit: int) -> list[str]:
    """Distinct artist_names with at least one event lacking a YouTube
    channel — same pool the YouTube API job uses, ordered by event
    count desc. Sport rows excluded (artist_name there is a team)."""
    rows = (
        db.query(Event.artist_name, _func.count(Event.id).label("n"))
        .filter(
            Event.artist_name.isnot(None),
            Event.artist_name != "",
            Event.sport.is_(None),
            or_(
                Event.artist_youtube_channel.is_(None),
                Event.artist_youtube_channel == "",
            ),
        )
        .group_by(Event.artist_name)
        .order_by(_func.count(Event.id).desc())
        .limit(limit)
        .all()
    )
    return [r[0] for r in rows]


# ── Main ──────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--apply", action="store_true",
                        help="Write changes. Without this, runs as a dry-run.")
    parser.add_argument("--limit", type=int, default=200,
                        help="Cap on artists processed per run (default 200). "
                             "Each artist is one Brave query.")
    args = parser.parse_args()

    mode = "APPLY" if args.apply else "DRY-RUN"
    log.info(f"mode={mode} limit={args.limit}")

    db = SessionLocal()
    cache = load_cache()
    log.info(f"cache: {len(cache)} previously-attempted artists on disk")

    stats = {
        "targeted":          0,
        "cache_hits":        0,
        "brave_calls":       0,
        "brave_empty":       0,
        "channel_found":     0,
        "no_match":          0,
        "wrote_events":      0,
    }
    samples = []

    try:
        artists = _select_artists_pending(db, args.limit)
        log.info(f"artists pending YouTube channel: {len(artists):,}")

        for i, name in enumerate(artists, start=1):
            stats["targeted"] += 1
            key = (name or "").lower().strip()
            if not key:
                continue

            cached = cache.get(key)
            if cached is not None:
                # Cache hit — use whatever channel_url was determined
                # last time. None means "we tried and Brave didn't
                # surface a match" — don't re-Brave on re-run within
                # the same backlog window.
                stats["cache_hits"] += 1
                channel_url = cached.get("channel_url")
            else:
                channel_url, results = find_channel_for_artist(name)
                stats["brave_calls"] += 1
                if not results:
                    stats["brave_empty"] += 1
                append_cache(key, results, channel_url)
                cache[key] = {"results": results, "channel_url": channel_url}
                time.sleep(BRAVE_QPS_DELAY)

            if not channel_url:
                stats["no_match"] += 1
                continue

            stats["channel_found"] += 1
            samples.append({
                "artist": name,
                "channel_url": channel_url,
            })

            if args.apply:
                # Case-insensitive write to ALL events for this artist.
                # Preserves any non-empty value already on rows (don't
                # clobber a manually-set channel) — only fill empty/null.
                affected = (
                    db.query(Event)
                    .filter(
                        _func.lower(Event.artist_name) == key,
                        or_(
                            Event.artist_youtube_channel.is_(None),
                            Event.artist_youtube_channel == "",
                        ),
                    )
                    .update(
                        {"artist_youtube_channel": channel_url},
                        synchronize_session=False,
                    )
                )
                db.commit()
                stats["wrote_events"] += affected or 0

            if i % 25 == 0:
                log.info(
                    f"  progress {i}/{len(artists)}  "
                    f"channel_found={stats['channel_found']}  "
                    f"no_match={stats['no_match']}  "
                    f"brave_calls={stats['brave_calls']}  "
                    f"cache_hits={stats['cache_hits']}"
                )

        log.info(
            "done. "
            f"targeted={stats['targeted']}  "
            f"channel_found={stats['channel_found']}  "
            f"no_match={stats['no_match']}  "
            f"brave_calls={stats['brave_calls']}  "
            f"brave_empty={stats['brave_empty']}  "
            f"cache_hits={stats['cache_hits']}  "
            f"wrote_events={stats['wrote_events']}"
        )

        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        suffix = "apply" if args.apply else "dryrun"
        audit_path = ROOT / "data" / f"enrich_youtube_via_brave_{ts}_{suffix}.json"
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        audit_path.write_text(json.dumps({
            "stats": stats,
            "samples": samples[:50],
            "samples_total": len(samples),
        }, ensure_ascii=False, indent=2))
        log.info(f"audit written: {audit_path}")
        if not args.apply:
            log.info("DRY-RUN — re-run with --apply to write.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
