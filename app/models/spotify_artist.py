"""Every charting artist we've surfaced via the daily scanner.

Populated by `spotify_scan_job` from Last.fm's chart endpoints
(`chart.gettopartists` for the global ranking + `geo.gettopartists` for
the rotating country axis). Originally designed to pull from Spotify's
own editorial surfaces (featured-playlists, new-releases, browse), but
Spotify deprecated all of those for Client Credentials apps in late
2024 — the first prod run scanned 10 markets and returned 0 artists
because every endpoint we hit responded 403. The Last.fm pivot keeps
the same "anybody who's anybody" semantic (chart presence = real
listener count) without needing OAuth or quota-extension applications.

`SpotifyArtist.id` is the row's external identifier — preferentially a
MusicBrainz ID (Last.fm carries those when known), with a SHA1(lower
name) fallback for the long tail without an MBID. The table name and
column name still say "spotify" because the original design landed on
disk and the rename isn't worth a migration.

Lifecycle (`match_status`):

    pending          first seen this run; name-matching against Performer
                     hasn't run yet (transient — shouldn't outlive one job).
    matched          name found in Performer.normalized_name on first scan.
                     No Brave work needed.
    pending_brave    new to us; queued for `spotify_brave_query_job`.
    brave_done       Brave has run (either query variant or the winner) and
                     this row's contribution to the funnel is final.

The Brave A/B + funnel counters live here so the stats endpoint can group
by match_status without joining other tables.
"""
from sqlalchemy import (
    Column, Integer, String, DateTime, ForeignKey, Text,
)
from sqlalchemy.sql import func

from app.database import Base


SPOTIFY_ARTIST_STATUSES = frozenset({"pending", "matched", "pending_brave", "brave_done"})


class SpotifyArtist(Base):
    __tablename__ = "spotify_artists"

    # Spotify's artist ID is a base-62 22-char string; we use it as the PK
    # so the daily scan is a single ON CONFLICT upsert against the unique
    # natural key Spotify already gives us.
    id = Column(String(40), primary_key=True)
    name = Column(String(500), nullable=False)
    external_url = Column(String(500), nullable=True)

    # First/last time we observed this artist on any Spotify surface. Lets
    # the stats card show daily-new vs cumulative.
    first_seen_at = Column(DateTime, server_default=func.now())
    last_seen_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    # Match against Performer.normalized_name on first encounter. Two-step
    # workflow drives this: scan_job classifies as matched/pending_brave;
    # brave_query_job flips pending_brave → brave_done after running.
    # Indexes for match_status and last_seen_at are created in
    # _run_migrations (IF NOT EXISTS) — keeping them out of __table_args__
    # avoids the duplicate-name collision that broke the first deploy
    # (Column(..., index=True) auto-generates the same name as an
    # explicit Index(...) declaration would, so create_all emitted two
    # CREATE INDEX statements with the same name).
    match_status = Column(String(20), nullable=False, default="pending")
    matched_performer_id = Column(Integer, ForeignKey("performers.id"), nullable=True)

    # Comma-list of Spotify market codes ("US,GB,IL,…") this artist has
    # been seen on. Cheap geo-coverage signal for analytics; not
    # normalised to its own table since the cardinality is small (75
    # max) and we never query by market.
    markets_surfaced_in = Column(Text, nullable=True)

    # ── Brave A/B + funnel counters ─────────────────────────────────────
    # brave_attempt_count: total Brave calls fired for this artist (both
    # variants combined while the A/B is running; just the winner after).
    brave_attempt_count = Column(Integer, default=0)
    # new_websites_found: count of LLMSource rows registered as a direct
    # result of this artist's Brave query. Aggregated from
    # spotify_brave_attempt rows on write to avoid a hot join on read.
    new_websites_found = Column(Integer, default=0)
    # new_artists_via_websites: filled by an offline rollup that walks
    # Event.llm_source_id → LLMSource.spotify_artist_id → distinct new
    # Performer names. Defaults 0 so the stats card has a value before
    # the rollup runs.
    new_artists_via_websites = Column(Integer, default=0)

    notes = Column(Text, nullable=True)
