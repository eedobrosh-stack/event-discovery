import asyncio
import logging
import time
from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.database import Base, engine
from app.api import auth, cities, event_types, events, export, admin, venues, stats, suggestions, artists
from app.api import platform_venues as platform_venues_api
from app.api import metro_areas
from app.api import version as version_api
from app.api.cities import warm_cities_cache
from app.api.metro_areas import warm_metro_cache
from app.scheduler.jobs import collect_all_events, cleanup_past_events, collect_venue_websites, run_dedup, collect_platform_venues, enrich_youtube_job, enrich_performers_job, enrich_venue_urls_job, discover_venues_job, collect_bandsintown_job, collect_techconf_job, collect_mevalim_job, llm_extract_recurring_job, llm_discover_sources_job, seed_brave_from_zero_results_job, classify_new_artists_job, recompute_popularity_job, enrich_youtube_via_brave_job, categorize_new_events_job, spotify_scan_job, spotify_brave_query_job

scheduler = AsyncIOScheduler()


def _seed_platform_venues():
    """One-time migration: move hardcoded VenuePilot venues into the platform_venues table."""
    from app.database import SessionLocal
    from app.models.platform_venue import PlatformVenue
    from app.models import City
    from app.services.collectors.scrapers.venuepilot import VENUEPILOT_VENUES

    db = SessionLocal()
    try:
        for cfg in VENUEPILOT_VENUES:
            existing = db.query(PlatformVenue).filter(
                PlatformVenue.platform == "venuepilot",
                PlatformVenue.platform_id == str(cfg["account_id"]),
            ).first()
            if existing:
                continue  # already seeded
            # Resolve city: prefer the first city in run_for_cities that has a DB record
            city = None
            for city_name in cfg.get("run_for_cities", [cfg.get("city", "")]):
                city = db.query(City).filter(City.name.ilike(city_name)).first()
                if city:
                    break
            db.add(PlatformVenue(
                name=cfg["name"],
                city_id=city.id if city else None,
                platform="venuepilot",
                platform_id=str(cfg["account_id"]),
                website_url=cfg.get("website_url"),
                address=cfg.get("address"),
                active=True,
            ))
        db.commit()
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"_seed_platform_venues failed: {e}")
    finally:
        db.close()


def _seed_priority_cities():
    """Ensure every PRIORITY_CITIES entry exists in the cities table."""
    import logging
    from app.database import SessionLocal
    from app.models import City
    from app.scheduler.jobs import PRIORITY_CITIES

    # (name, country, state, timezone, lat, lon)
    CITY_META: dict[tuple[str, str], dict] = {
        # ── United States ────────────────────────────────────────────────
        ("New York",       "United States"): dict(state="NY", timezone="America/New_York",   latitude=40.7128,  longitude=-74.0060),
        ("Los Angeles",    "United States"): dict(state="CA", timezone="America/Los_Angeles", latitude=34.0522,  longitude=-118.2437),
        ("Chicago",        "United States"): dict(state="IL", timezone="America/Chicago",     latitude=41.8781,  longitude=-87.6298),
        ("San Francisco",  "United States"): dict(state="CA", timezone="America/Los_Angeles", latitude=37.7749,  longitude=-122.4194),
        ("Berkeley",       "United States"): dict(state="CA", timezone="America/Los_Angeles", latitude=37.8715,  longitude=-122.2730),
        # ── United Kingdom ───────────────────────────────────────────────
        ("London",         "United Kingdom"): dict(timezone="Europe/London",    latitude=51.5074,  longitude=-0.1278),
        ("Manchester",     "United Kingdom"): dict(timezone="Europe/London",    latitude=53.4808,  longitude=-2.2426),
        ("Edinburgh",      "United Kingdom"): dict(timezone="Europe/London",    latitude=55.9533,  longitude=-3.1883),
        # ── Germany ──────────────────────────────────────────────────────
        ("Berlin",         "Germany"):        dict(timezone="Europe/Berlin",    latitude=52.5200,  longitude=13.4050),
        ("Munich",         "Germany"):        dict(timezone="Europe/Berlin",    latitude=48.1351,  longitude=11.5820),
        # ── France ───────────────────────────────────────────────────────
        ("Paris",          "France"):         dict(timezone="Europe/Paris",     latitude=48.8566,  longitude=2.3522),
        # ── Italy ────────────────────────────────────────────────────────
        ("Rome",           "Italy"):          dict(timezone="Europe/Rome",      latitude=41.9028,  longitude=12.4964),
        ("Milan",          "Italy"):          dict(timezone="Europe/Rome",      latitude=45.4642,  longitude=9.1900),
        # ── Spain ────────────────────────────────────────────────────────
        ("Madrid",         "Spain"):          dict(timezone="Europe/Madrid",    latitude=40.4168,  longitude=-3.7038),
        ("Barcelona",      "Spain"):          dict(timezone="Europe/Madrid",    latitude=41.3851,  longitude=2.1734),
        # ── Netherlands ──────────────────────────────────────────────────
        ("Amsterdam",      "Netherlands"):    dict(timezone="Europe/Amsterdam", latitude=52.3676,  longitude=4.9041),
        # ── Portugal ─────────────────────────────────────────────────────
        ("Lisbon",         "Portugal"):       dict(timezone="Europe/Lisbon",    latitude=38.7169,  longitude=-9.1399),
        # ── Belgium ──────────────────────────────────────────────────────
        ("Brussels",       "Belgium"):        dict(timezone="Europe/Brussels",  latitude=50.8503,  longitude=4.3517),
        # ── Turkey ───────────────────────────────────────────────────────
        ("Istanbul",       "Turkey"):         dict(timezone="Europe/Istanbul",  latitude=41.0082,  longitude=28.9784),
        # ── Greece ───────────────────────────────────────────────────────
        ("Athens",         "Greece"):         dict(timezone="Europe/Athens",    latitude=37.9838,  longitude=23.7275),
        # ── Brazil ───────────────────────────────────────────────────────
        ("São Paulo",      "Brazil"):         dict(timezone="America/Sao_Paulo",    latitude=-23.5505, longitude=-46.6333),
        ("Rio de Janeiro", "Brazil"):         dict(timezone="America/Sao_Paulo",    latitude=-22.9068, longitude=-43.1729),
        # ── Argentina ────────────────────────────────────────────────────
        ("Buenos Aires",   "Argentina"):      dict(timezone="America/Argentina/Buenos_Aires", latitude=-34.6037, longitude=-58.3816),
        # ── Mexico ───────────────────────────────────────────────────────
        ("Mexico City",    "Mexico"):         dict(timezone="America/Mexico_City",  latitude=19.4326,  longitude=-99.1332),
        # ── Canada ───────────────────────────────────────────────────────
        ("Toronto",        "Canada"):         dict(timezone="America/Toronto",      latitude=43.6532,  longitude=-79.3832),
        ("Vancouver",      "Canada"):         dict(timezone="America/Vancouver",    latitude=49.2827,  longitude=-123.1207),
        # ── Australia ────────────────────────────────────────────────────
        ("Sydney",         "Australia"):      dict(timezone="Australia/Sydney",     latitude=-33.8688, longitude=151.2093),
        ("Melbourne",      "Australia"):      dict(timezone="Australia/Melbourne",  latitude=-37.8136, longitude=144.9631),
        ("Brisbane",       "Australia"):      dict(timezone="Australia/Brisbane",   latitude=-27.4698, longitude=153.0251),
        # ── Israel ───────────────────────────────────────────────────────
        ("Tel Aviv",       "Israel"):         dict(timezone="Asia/Jerusalem",       latitude=32.0853,  longitude=34.7818),
    }

    _log = logging.getLogger(__name__)
    db = SessionLocal()
    added = 0
    try:
        for name, country in PRIORITY_CITIES:
            exists = db.query(City).filter_by(name=name, country=country).first()
            if exists:
                continue
            meta = CITY_META.get((name, country), {})
            db.add(City(
                name=name,
                country=country,
                state=meta.get("state"),
                timezone=meta.get("timezone"),
                latitude=meta.get("latitude"),
                longitude=meta.get("longitude"),
            ))
            added += 1
        db.commit()
        if added:
            _log.info(f"_seed_priority_cities: added {added} new city records")
    except Exception as e:
        _log.warning(f"_seed_priority_cities failed: {e}")
        db.rollback()
    finally:
        db.close()


def _seed_event_types():
    """Insert any event types from seed data that are not yet in the DB."""
    from app.database import SessionLocal
    from app.models import EventType
    from app.seed.event_types import EVENT_TYPES

    db = SessionLocal()
    try:
        for et in EVENT_TYPES:
            exists = db.query(EventType).filter_by(name=et["name"]).first()
            if not exists:
                db.add(EventType(
                    name=et["name"],
                    category=et["category"],
                    keywords=et.get("keywords", ""),
                ))
        db.commit()
    except Exception as e:
        logging.getLogger(__name__).warning(f"_seed_event_types failed: {e}")
    finally:
        db.close()


def _seed_artist_classifications():
    """One-time seed: load Gemini's per-artist genre classifications from the
    bundled gzip-JSON file into `genre_taxonomy` and `artist_genre`.

    The bundle (`app/seed/artist_classifications.json.gz`) is generated by
    scripts/dump_classifications_seed.py from the local DB and committed to
    git. On every deploy this function runs idempotently:
      - taxonomy rows keyed by `sub_genre` (PK) — insert if absent
      - artist rows keyed by `normalized_name` (unique) — insert if absent

    No updates here. If a classification needs to change in prod, regenerate
    the bundle from the (corrected) local DB and ship a new deploy. We don't
    want startup to silently overwrite production hand-edits.

    Bails fast on the common case: if the artist row count already matches
    the bundle, skip the existence-check loop entirely.
    """
    import gzip
    import json
    import os
    from app.database import SessionLocal
    from app.models.genre import GenreTaxonomy, ArtistGenre

    _log = logging.getLogger(__name__)
    bundle_path = os.path.join(
        os.path.dirname(__file__), "seed", "artist_classifications.json.gz"
    )
    if not os.path.isfile(bundle_path):
        _log.warning(f"_seed_artist_classifications: bundle not found at {bundle_path}; skipping")
        return

    try:
        with gzip.open(bundle_path, "rb") as f:
            payload = json.loads(f.read().decode("utf-8"))
    except Exception as e:
        _log.warning(f"_seed_artist_classifications: failed to read bundle: {e}")
        return

    taxonomy = payload.get("taxonomy", [])
    artists = payload.get("artists", [])
    bundle_version = payload.get("version", "?")

    db = SessionLocal()
    try:
        # ── taxonomy ────────────────────────────────────────────────────
        existing_subgenres = {
            row[0] for row in db.query(GenreTaxonomy.sub_genre).all()
        }
        new_taxonomy = [
            GenreTaxonomy(sub_genre=t["sub_genre"], parent_genre=t["parent_genre"])
            for t in taxonomy
            if t["sub_genre"] not in existing_subgenres
        ]
        if new_taxonomy:
            db.bulk_save_objects(new_taxonomy)
            db.commit()
            _log.info(f"_seed_artist_classifications: +{len(new_taxonomy)} taxonomy rows")

        # ── artists ─────────────────────────────────────────────────────
        # Two operations on each boot:
        #   1. INSERT bundle rows we've never seen.
        #   2. UPGRADE existing rows whose primary_genre is NULL or
        #      'UNKNOWN' when the bundle has a real classification —
        #      lets a later iteration of improve_genre_coverage.py /
        #      Brave-augmented re-classify rescue rows that prod
        #      previously had no signal for.
        # We never DOWNGRADE: a known classification on disk always
        # wins over an UNKNOWN bundle entry, so a partial-bundle
        # accidentally redacting good data is impossible.
        existing_rows = {
            row[0]: row[1]
            for row in db.query(ArtistGenre.normalized_name, ArtistGenre.primary_genre).all()
        }

        new_artists: list[ArtistGenre] = []
        upgrade_mappings: list[dict] = []
        UPGRADABLE = {None, "UNKNOWN"}

        for a in artists:
            norm = a["normalized_name"]
            bundle_primary = a.get("primary_genre")
            existing_primary = existing_rows.get(norm, "__missing__")

            if existing_primary == "__missing__":
                # New row — straight insert.
                new_artists.append(ArtistGenre(
                    artist_name=a["artist_name"],
                    normalized_name=norm,
                    primary_genre=bundle_primary,
                    secondary_1=a.get("secondary_1"),
                    secondary_2=a.get("secondary_2"),
                    confidence=a.get("confidence"),
                    # Preserve bundle source if present (post-2026-05-09);
                    # default 'gemini' for the legacy bundle that didn't
                    # round-trip the field.
                    source=a.get("source") or "gemini",
                ))
                continue

            # Existing row — only upgrade NULL/UNKNOWN to a real label.
            if existing_primary in UPGRADABLE and bundle_primary not in UPGRADABLE:
                upgrade_mappings.append({
                    # ArtistGenre doesn't have a stable PK other than id,
                    # so we update by the unique normalized_name. SQLA's
                    # bulk_update_mappings needs the PK; emit one UPDATE
                    # statement per row via a where=normalized_name path.
                    "normalized_name": norm,
                    "primary_genre": bundle_primary,
                    "secondary_1": a.get("secondary_1"),
                    "secondary_2": a.get("secondary_2"),
                    "confidence": a.get("confidence"),
                    "source": a.get("source") or "gemini",
                })

        if new_artists:
            # bulk_save_objects skips per-row identity-map cost; ~10× faster
            # than individual db.add() loops at this scale.
            db.bulk_save_objects(new_artists)
            db.commit()
            _log.info(
                f"_seed_artist_classifications: +{len(new_artists):,} artist rows "
                f"from bundle v{bundle_version}"
            )

        if upgrade_mappings:
            # bulk_update_mappings requires the table's primary key column;
            # ArtistGenre's PK is `id`, so use a per-batch UPDATE keyed on
            # normalized_name. Done in a single pass via Core for speed at
            # 15K rows (well under 1s in practice).
            from sqlalchemy import update
            stmt = (
                update(ArtistGenre)
                .where(ArtistGenre.normalized_name == None)  # noqa: E711 — placeholder, replaced below
            )
            # SQLA's `update().values()` doesn't support per-row data in
            # one call; iterate but keep it short. The loop is bounded by
            # how many UNKNOWNs we have (low triple digits typically).
            for m in upgrade_mappings:
                db.execute(
                    update(ArtistGenre)
                    .where(ArtistGenre.normalized_name == m["normalized_name"])
                    .values(
                        primary_genre=m["primary_genre"],
                        secondary_1=m["secondary_1"],
                        secondary_2=m["secondary_2"],
                        confidence=m["confidence"],
                        source=m["source"],
                    )
                )
            db.commit()
            _log.info(
                f"_seed_artist_classifications: upgraded {len(upgrade_mappings):,} "
                f"NULL/UNKNOWN rows from bundle v{bundle_version}"
            )

        if not new_artists and not upgrade_mappings:
            _log.info(
                f"_seed_artist_classifications: bundle v{bundle_version} "
                f"applied — no inserts, no upgrades"
            )
    except Exception as e:
        _log.warning(f"_seed_artist_classifications failed: {e}")
        db.rollback()
    finally:
        db.close()


def _recover_stale_scan_logs():
    """
    Mark any scan_log rows with status='running' older than 2h as 'stale'.

    Background scraping jobs (enrich_youtube, bandsintown, etc.) write a
    ScanLog row with status='running' before they start and only flip it to
    'success'/'failed' in a finally block. When Render OOM-kills or redeploys
    the worker mid-job, that finally never runs and the row is stranded.

    Over days this leaves a growing graveyard of ghost rows in the admin
    dashboard (≈8 ghost rows/day observed on prod for enrich_youtube alone,
    and similar for bandsintown/collect_events). A single UPDATE on startup
    cleans them up before serving.

    2h threshold is well past the longest-running job (enrich_youtube ≈ 30min
    for 300 artists) so nothing in-flight when a new process boots could
    possibly still be legitimately 'running'.
    """
    from sqlalchemy import text
    try:
        with engine.connect() as conn:
            result = conn.execute(text(
                "UPDATE scan_logs "
                "SET status='stale', "
                "    finished_at=CURRENT_TIMESTAMP, "
                "    notes=COALESCE(notes, '') || ' [orphaned by worker restart]' "
                "WHERE status='running' "
                "  AND started_at < datetime('now', '-2 hours')"
            ))
            conn.commit()
            if result.rowcount:
                logging.getLogger(__name__).info(
                    f"_recover_stale_scan_logs: marked {result.rowcount} "
                    f"orphaned running rows as stale"
                )
    except Exception as e:
        logging.getLogger(__name__).warning(f"_recover_stale_scan_logs failed: {e}")


def _run_migrations():
    """Apply incremental schema changes that create_all() won't handle."""
    from sqlalchemy import text, inspect
    insp = inspect(engine)

    existing_venue_cols = [c["name"] for c in insp.get_columns("venues")]
    if "default_event_type_id" not in existing_venue_cols:
        with engine.connect() as conn:
            conn.execute(text("ALTER TABLE venues ADD COLUMN default_event_type_id INTEGER"))
            conn.commit()

    existing_performer_cols = [c["name"] for c in insp.get_columns("performers")]
    spotify_performer_cols = {
        "spotify_id":  "TEXT",
        "spotify_url": "TEXT",
        "image_url":   "TEXT",
        "popularity":  "INTEGER",
        # Internally-derived 0-100 popularity score. Replaces
        # `popularity` (Spotify-deprecated late 2024) as the live
        # signal. Populated by scripts/recompute_popularity.py.
        "derived_popularity": "INTEGER",
    }
    with engine.connect() as conn:
        for col, coltype in spotify_performer_cols.items():
            if col not in existing_performer_cols:
                conn.execute(text(f"ALTER TABLE performers ADD COLUMN {col} {coltype}"))
        conn.commit()

    existing_event_cols = [c["name"] for c in insp.get_columns("events")]
    artist_spotify_cols = {
        "artist_popularity": "INTEGER",
        "artist_spotify_url": "TEXT",
    }
    with engine.connect() as conn:
        for col, coltype in artist_spotify_cols.items():
            if col not in existing_event_cols:
                conn.execute(text(f"ALTER TABLE events ADD COLUMN {col} {coltype}"))
        conn.commit()

    sports_cols = {
        "sport":       "TEXT",
        "home_team":   "TEXT",
        "away_team":   "TEXT",
        "tv_channels": "TEXT",   # JSON stored as TEXT in SQLite
        # Tournament label ("FIFA World Cup", "NBA", "Wimbledon"…). Backs
        # the top-priority Tournament autocomplete chip — see the model
        # docstring and scripts/backfill_event_tournament.py for the
        # source of truth on which values get populated.
        "tournament":  "TEXT",
        # llm_source_id: FK into llm_sources for events extracted by
        # Cadence A. NULL for Route 2 collectors. Closes the funnel
        # spotify_artist → llm_source → event → performer so the
        # stats card can attribute new performers back to the Brave
        # query that discovered the source they live on.
        "llm_source_id": "INTEGER",
    }
    with engine.connect() as conn:
        for col, coltype in sports_cols.items():
            if col not in existing_event_cols:
                conn.execute(text(f"ALTER TABLE events ADD COLUMN {col} {coltype}"))
        conn.commit()
    # Single-column index on llm_source_id powers the funnel-attribution
    # query in the Spotify stats endpoint (events joined back to source).
    with engine.connect() as conn:
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS ix_events_llm_source ON events(llm_source_id)"
        ))
        conn.commit()
    # Index on tournament — small enum-like cardinality (one entry per
    # named competition), supports the cheap WHERE tournament = X path
    # from /api/events when the Tournament chip is selected.
    with engine.connect() as conn:
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS ix_events_tournament ON events(tournament)"
        ))
        conn.commit()

    # brave_query_coverage.priority — added 2026-05-16. Existing rows
    # default to 0 (long-tail); Cadence B recomputes priorities for
    # all rows on its next fire so Wave 1 / Wave 2 entries pick up
    # the right tier automatically. Create the composite index even
    # if create_all already made the table — IF NOT EXISTS handles
    # the duplicate case.
    if "brave_query_coverage" in insp.get_table_names():
        bqc_cols = [c["name"] for c in insp.get_columns("brave_query_coverage")]
        if "priority" not in bqc_cols:
            with engine.connect() as conn:
                conn.execute(text(
                    "ALTER TABLE brave_query_coverage "
                    "ADD COLUMN priority INTEGER NOT NULL DEFAULT 0"
                ))
                conn.commit()
        with engine.connect() as conn:
            conn.execute(text(
                "CREATE INDEX IF NOT EXISTS "
                "ix_brave_query_coverage_priority_fired "
                "ON brave_query_coverage(priority, fired_at)"
            ))
            conn.commit()

    # zero_result_searches.tournaments — logged when a search with a
    # Tournament chip returned 0 even after lookahead. Useful for
    # spotting "users want a tournament we don't carry yet" demand.
    if "zero_result_searches" in insp.get_table_names():
        zrs_cols = [c["name"] for c in insp.get_columns("zero_result_searches")]
        if "tournaments" not in zrs_cols:
            with engine.connect() as conn:
                conn.execute(text(
                    "ALTER TABLE zero_result_searches ADD COLUMN tournaments TEXT"
                ))
                conn.commit()
        # seeded_brave_at: when this dead-end query was fed into Brave
        # discovery. NULL means "still pending"; set by the
        # seed_brave_from_zero_results job so we don't re-fire the
        # same query every cycle.
        if "seeded_brave_at" not in zrs_cols:
            with engine.connect() as conn:
                conn.execute(text(
                    "ALTER TABLE zero_result_searches ADD COLUMN seeded_brave_at DATETIME"
                ))
                conn.commit()

    # artist_genre.classification_attempts — added so the auto-classification
    # cron can park artists that have failed to classify N times. Default 0
    # so existing rows are eligible for the next retry pass.
    # artist_genre.brave_total_results — added so the popularity recompute
    # job has access to "Brave web-footprint" as one input signal. NULL
    # for any row that pre-dates the column or hasn't been Brave-searched
    # since.
    if "artist_genre" in insp.get_table_names():
        existing_artist_genre_cols = [c["name"] for c in insp.get_columns("artist_genre")]
        if "classification_attempts" not in existing_artist_genre_cols:
            with engine.connect() as conn:
                conn.execute(text(
                    "ALTER TABLE artist_genre ADD COLUMN "
                    "classification_attempts INTEGER NOT NULL DEFAULT 0"
                ))
                conn.commit()
        if "brave_total_results" not in existing_artist_genre_cols:
            with engine.connect() as conn:
                conn.execute(text(
                    "ALTER TABLE artist_genre ADD COLUMN "
                    "brave_total_results INTEGER"
                ))
                conn.commit()

    # llm_sources: incremental columns added after the table's first ship.
    # The table itself is created by Base.metadata.create_all on first deploy
    # of d2d9383; new columns added later need ALTER TABLE on existing rows.
    if "llm_sources" in insp.get_table_names():
        existing_llm_cols = [c["name"] for c in insp.get_columns("llm_sources")]
        llm_incremental_cols: dict[str, str] = {
            # consecutive_success_runs: auto-promotion gate (cad99b7).
            "consecutive_success_runs": "INTEGER DEFAULT 0",
            # Drift detection (Half 1 task 2/4): rolling event-count window
            # + computed drift score per run + flag for the audit dashboard.
            "recent_event_counts":      "TEXT",   # JSON list under the hood
            "drift_score":              "REAL",
            "drift_flag":               "INTEGER DEFAULT 0",  # SQLite has no Boolean
            # Per-source URL templates (Half 1 task 4/4 — pagination Move 2).
            "url_template":              "TEXT",
            "url_template_range_months": "INTEGER",
            "url_template_values":       "TEXT",   # JSON list under the hood
            # Provenance: which discovery channel registered the row.
            # Backfilled to 'cadence_b' for all pre-existing rows since
            # that was the only writer in the project until the Spotify
            # funnel landed.
            "discovered_via":            "TEXT",
            "spotify_artist_id":         "TEXT",
        }
        with engine.connect() as conn:
            for col, coltype in llm_incremental_cols.items():
                if col not in existing_llm_cols:
                    conn.execute(text(
                        f"ALTER TABLE llm_sources ADD COLUMN {col} {coltype}"
                    ))
            conn.commit()

        # One-time backfill of discovered_via='cadence_b' for any row that
        # pre-dates the column. The only historical writers were
        # discover_via_search (Cadence B) and the manual seeder; the
        # seeder population is small enough that mis-classifying them as
        # 'cadence_b' is acceptable noise — operators can re-tag those
        # by hand if they care.
        if "discovered_via" not in existing_llm_cols:
            with engine.connect() as conn:
                conn.execute(text(
                    "UPDATE llm_sources SET discovered_via='cadence_b' "
                    "WHERE discovered_via IS NULL"
                ))
                conn.commit()

        # Composite-free indexes on the two provenance columns so the
        # stats endpoint can group/filter by discovered_via without a
        # full scan once spotify_artist_query rows start landing.
        with engine.connect() as conn:
            conn.execute(text(
                "CREATE INDEX IF NOT EXISTS "
                "ix_llm_sources_discovered_via ON llm_sources(discovered_via)"
            ))
            conn.execute(text(
                "CREATE INDEX IF NOT EXISTS "
                "ix_llm_sources_spotify_artist_id ON llm_sources(spotify_artist_id)"
            ))
            conn.commit()

    # spotify_artists indexes — kept out of the model's __table_args__
    # because Column(..., index=True) auto-generates a same-name index
    # and the two collide in create_all. IF NOT EXISTS here covers both
    # the fresh-DB case (table created by create_all without indexes)
    # AND the partial-state recovery case (first deploy failed mid-way
    # through index creation, leaving the table with one index and not
    # the other).
    if "spotify_artists" in insp.get_table_names():
        with engine.connect() as conn:
            conn.execute(text(
                "CREATE INDEX IF NOT EXISTS "
                "ix_spotify_artists_match_status ON spotify_artists(match_status)"
            ))
            conn.execute(text(
                "CREATE INDEX IF NOT EXISTS "
                "ix_spotify_artists_last_seen ON spotify_artists(last_seen_at)"
            ))
            conn.commit()

        # One-time cleanup: block any LLMSource on a domain we already
        # cover with a hand-coded collector. Prevents the recurring job
        # from spending Gemini tokens to re-extract events the existing
        # collectors already write. New rows on reserved domains are
        # filtered at discovery time (_is_reserved_discovery_url); this
        # block is for rows that pre-date the filter.
        try:
            from app.scheduler.jobs import _is_reserved_discovery_url
            from app.models import LLMSource
            from app.database import SessionLocal as _Sess
            _db = _Sess()
            try:
                blocked_now = 0
                for src in (
                    _db.query(LLMSource)
                    .filter(LLMSource.state.in_(["trial", "recurring"]))
                    .all()
                ):
                    if _is_reserved_discovery_url(src.url):
                        src.state = "blocked"
                        stamp = (
                            f"[auto-blocked at startup] domain reserved "
                            f"by an existing hand-coded collector"
                        )
                        src.notes = (src.notes + "\n" + stamp) if src.notes else stamp
                        blocked_now += 1
                if blocked_now:
                    _db.commit()
                    import logging as _lg
                    _lg.getLogger(__name__).info(
                        f"_run_migrations: auto-blocked {blocked_now} LLMSource "
                        f"row(s) on reserved domains"
                    )
            finally:
                _db.close()
        except Exception:
            # Cleanup is best-effort; never let it break boot.
            pass

    # job_state: persistent key/value store for scheduler state (e.g. the
    # rotating city-batch cursor) so it survives Render restarts / OOM kills.
    if "job_state" not in insp.get_table_names():
        with engine.connect() as conn:
            conn.execute(text(
                "CREATE TABLE job_state ("
                "  key VARCHAR(64) PRIMARY KEY,"
                "  value VARCHAR(255) NOT NULL,"
                "  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
                ")"
            ))
            conn.commit()

    # Search-path indexes for /api/suggestions and /api/events text matching.
    # Background: PRs #32-34 introduced 8 separate ILIKE-based queries per
    # /api/suggestions call (categories, types, sport-teams, artists, venues,
    # event-names, plus league-prefix detection) plus DISTINCT + ORDER BY
    # CASE relevance ranking on top. With 139K events and zero indexes on
    # the matched columns, autocomplete latency climbed from ~150ms to
    # 2-17 seconds (observed in 2026-04-28 13:15-13:17 prod logs) — slow
    # enough to bloat memory until the single uvicorn worker silently hung,
    # twice in one day.
    #
    # SQLAlchemy translates `column.ilike(...)` on SQLite to
    # `lower(column) LIKE lower(pattern)`, so expression indexes on
    # `LOWER(column)` are usable for the prefix-anchored half of every
    # `name_match_ilike` OR (`'term%'` branch). The `'% term%'` half still
    # full-scans, but SQLite's OR-optimization uses the index for the first
    # branch and unions, cutting the dominant short-prefix query from full
    # scan to index-seek. For the autocomplete-typing case (the main pain
    # point — every keystroke fires a request) that's the difference between
    # ~50ms and ~3000ms per call.
    #
    # IF NOT EXISTS makes this idempotent — first boot builds them (a few
    # seconds at the current size); subsequent boots no-op. ANALYZE refreshes
    # the optimizer's statistics so the planner actually picks the new
    # indexes; without it the first few hours after deploy keep using
    # full-scan plans from cached stats.
    search_indexes = [
        "CREATE INDEX IF NOT EXISTS ix_events_artist_name_lower    ON events(LOWER(artist_name))",
        "CREATE INDEX IF NOT EXISTS ix_events_name_lower           ON events(LOWER(name))",
        "CREATE INDEX IF NOT EXISTS ix_events_home_team_lower      ON events(LOWER(home_team))",
        "CREATE INDEX IF NOT EXISTS ix_events_away_team_lower      ON events(LOWER(away_team))",
        "CREATE INDEX IF NOT EXISTS ix_venues_name_lower           ON venues(LOWER(name))",
        "CREATE INDEX IF NOT EXISTS ix_venues_physical_city_lower  ON venues(LOWER(physical_city))",
    ]
    with engine.connect() as conn:
        for stmt in search_indexes:
            conn.execute(text(stmt))
        conn.execute(text("ANALYZE"))
        conn.commit()


def _fix_sports_categories():
    """
    One-time repair: events collected before the sports-categorization fix
    have sport=NULL, wrong artist_name (home team), and/or Music/Concert
    event types. Detect them by the "<League> - " name prefix and repair in
    place. Runs on every startup (idempotent) so newly-scraped or re-ingested
    events also get repaired if anything slipped through the registry.
    """
    import logging
    _log = logging.getLogger(__name__)
    from app.database import SessionLocal
    from app.models import EventType
    from app.models.event import Event

    # label → (sport value, preferred event-type name from seed)
    # Names MUST match app/seed/event_types.py — if a specific type doesn't
    # exist, we fall back to the generic "Sports Event".
    LEAGUE_MAP = {
        "NBA":                    ("Basketball",          "Basketball Game"),
        "WNBA":                   ("Basketball",          "Basketball Game"),
        "NHL":                    ("Ice Hockey",          "Hockey Game"),
        "NFL":                    ("American Football",   "American Football Game"),
        "MLS":                    ("Soccer",              "Soccer Match"),
        "MLB":                    ("Baseball",            "Baseball Game"),
        "AFL":                    ("Australian Football", "Sports Event"),
        "NRL":                    ("Rugby League",        "Sports Event"),
        "NBL":                    ("Basketball",          "Basketball Game"),
        "CFL":                    ("Canadian Football",   "American Football Game"),
        "EuroLeague":             ("Basketball",          "Basketball Game"),
        "EuroCup":                ("Basketball",          "Basketball Game"),
        "Premier League":         ("Soccer",              "Soccer Match"),
        "Bundesliga":             ("Soccer",              "Soccer Match"),
        "La Liga":                ("Soccer",              "Soccer Match"),
        "Serie A":                ("Soccer",              "Soccer Match"),
        "Ligue 1":                ("Soccer",              "Soccer Match"),
        "Eredivisie":             ("Soccer",              "Soccer Match"),
        "UEFA Champions League":  ("Soccer",              "Soccer Match"),
        "UEFA Europa League":     ("Soccer",              "Soccer Match"),
        "Formula 1":              ("Motorsport",          "Sports Event"),
    }

    db = SessionLocal()
    try:
        # Pre-resolve all event types once (IDs of Music/Comedy to remove,
        # and the Sports target for each league).
        music_et_ids = {
            row[0] for row in db.query(EventType.id)
            .filter(EventType.category.in_(["Music", "Comedy"]))
            .all()
        }
        sports_generic = db.query(EventType).filter_by(
            name="Sports Event", category="Sports"
        ).first()

        # Cache: event-type name → EventType instance
        et_cache: dict[str, EventType] = {}

        def _resolve_et(name: str):
            if name in et_cache:
                return et_cache[name]
            et = db.query(EventType).filter_by(name=name, category="Sports").first()
            # Fall back to the generic "Sports Event" if specific type missing
            if et is None:
                et = sports_generic
            et_cache[name] = et
            return et

        fixed = 0
        for label, (sport_val, et_name) in LEAGUE_MAP.items():
            prefix = f"{label} - %"
            # Match by name prefix regardless of current sport value — some
            # events were partially fixed (sport set) but still carry Music
            # event types from the original scrape.
            events = (
                db.query(Event)
                .filter(Event.name.ilike(prefix))
                .all()
            )
            if not events:
                continue

            sports_et = _resolve_et(et_name)

            for ev in events:
                dirty = False
                if ev.sport != sport_val:
                    ev.sport = sport_val
                    dirty = True
                if ev.artist_name:
                    # artist_name was set to home team in pre-fix events
                    ev.artist_name = None
                    dirty = True
                # Strip any Music/Comedy types; add the correct Sports one.
                current_ids = {et.id for et in (ev.event_types or [])}
                if current_ids & music_et_ids:
                    ev.event_types = [
                        et for et in ev.event_types if et.id not in music_et_ids
                    ]
                    dirty = True
                if sports_et and sports_et not in (ev.event_types or []):
                    ev.event_types.append(sports_et)
                    dirty = True
                # Backfill YouTube highlights search URL when missing
                if (
                    not ev.artist_youtube_channel
                    and ev.home_team
                    and ev.away_team
                ):
                    from urllib.parse import quote_plus
                    q = quote_plus(
                        f"{ev.home_team} vs {ev.away_team} highlights"
                    )
                    ev.artist_youtube_channel = (
                        f"https://www.youtube.com/results?search_query={q}"
                    )
                    dirty = True
                if dirty:
                    fixed += 1

        if fixed:
            db.commit()
            _log.info(f"_fix_sports_categories: repaired {fixed} events")
        else:
            _log.info("_fix_sports_categories: nothing to repair")
    except Exception as e:
        _log.warning(f"_fix_sports_categories failed: {e}")
        db.rollback()
    finally:
        db.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    import asyncio
    import logging
    _log = logging.getLogger(__name__)

    # Create tables on startup (fast, must complete before serving)
    Base.metadata.create_all(bind=engine)
    _run_migrations()
    _recover_stale_scan_logs()

    # Move cache warming to background so Render's health check passes quickly
    async def _deferred_startup():
        await asyncio.sleep(2)  # let uvicorn bind the port first
        try:
            await asyncio.get_event_loop().run_in_executor(None, warm_cities_cache)
            _log.info("Cities cache warmed")
            await asyncio.get_event_loop().run_in_executor(None, warm_metro_cache)
            _log.info("Metro areas cache warmed")
        except Exception as e:
            _log.warning(f"Cache warm failed: {e}")

        # Warm the suggestions index too — first user keystroke on a
        # fresh boot otherwise pays the ~2s build cost. Imports lazily so
        # the suggestions module isn't loaded at app-import time.
        try:
            from app.api._suggestions_index import warm_index
            from app.database import SessionLocal
            def _warm():
                db = SessionLocal()
                try:
                    warm_index(db)
                finally:
                    db.close()
            await asyncio.get_event_loop().run_in_executor(None, _warm)
            _log.info("Suggestions index warmed")
        except Exception as e:
            _log.warning(f"Suggestions index warm failed: {e}")
    asyncio.create_task(_deferred_startup())

    # Schedule jobs
    # Jobs are staggered (start_date offset) so they don't all fire simultaneously
    # and compete for memory on the same instance.
    from datetime import datetime as _dt, timedelta as _td
    _t = _dt.utcnow()

    # Jobs are staggered so they don't compete for memory on startup.
    # Light daily jobs (techconf t+3, mevalim t+6) run first so a redeploy
    # within 15 min doesn't keep pushing them out. Heavy scraping starts at
    # t+15 min — well after Render's health checks pass.
    scheduler.add_job(
        collect_all_events,
        IntervalTrigger(hours=settings.SCRAPE_INTERVAL_HOURS, start_date=_t + _td(minutes=15)),
        id="collect_events",
        replace_existing=True,
    )
    scheduler.add_job(
        cleanup_past_events,
        IntervalTrigger(hours=24, start_date=_t + _td(minutes=35)),
        id="cleanup_past",
        replace_existing=True,
    )
    scheduler.add_job(
        collect_venue_websites,
        IntervalTrigger(hours=24, start_date=_t + _td(minutes=50)),
        id="collect_venue_websites",
        replace_existing=True,
    )
    scheduler.add_job(
        run_dedup,
        CronTrigger(day_of_week="sun", hour=5, minute=0),  # weekly Sunday 5am UTC
        id="dedup_events",
        replace_existing=True,
    )
    scheduler.add_job(
        collect_platform_venues,
        IntervalTrigger(hours=24, start_date=_t + _td(minutes=65)),
        id="collect_platform_venues",
        replace_existing=True,
    )
    # --- Async enrichment + Spotify funnel ----------------------------------
    # Staggered so only one is alive at a time. Each holds an
    # httpx.AsyncClient open for its whole run; concurrent fires were
    # the primary driver of the 2GB OOM restarts observed on
    # 2026-04-21 / 2026-04-22.
    #
    # Timeline after boot (minutes):
    #   +25  bandsintown            (~25 min, ends +50)
    #   +90  enrich_performers      (~10 min, ends +100)
    #   +120 enrich_youtube         (~5 min,  4h re-fire cycle)
    #   +300 spotify_scan           (~15 min, daily — walks Spotify
    #                                editorial surfaces for 10
    #                                markets/day on rotation)
    #   +330 spotify_brave_query    (~10 min, 2h re-fire cycle —
    #                                A/B brave query for each
    #                                pending_brave SpotifyArtist)
    #
    # The old enrich_spotify_job (Performer-side lookup) was removed on
    # 2026-05-25 — Spotify's API gutting in late 2024 had been making
    # it write popularity=0 / genres=[] for every row.
    scheduler.add_job(
        enrich_performers_job,
        IntervalTrigger(hours=24, start_date=_t + _td(minutes=90)),
        id="enrich_performers",
        replace_existing=True,
    )
    scheduler.add_job(
        enrich_youtube_job,
        IntervalTrigger(hours=4, start_date=_t + _td(minutes=120)),
        id="enrich_youtube",
        replace_existing=True,
    )
    scheduler.add_job(
        enrich_venue_urls_job,
        IntervalTrigger(hours=24, start_date=_t + _td(minutes=155)),
        id="enrich_venue_urls",
        replace_existing=True,
    )
    scheduler.add_job(
        discover_venues_job,
        IntervalTrigger(hours=48, start_date=_t + _td(minutes=185)),
        id="discover_venues",
        replace_existing=True,
    )
    # spotify_scan — daily walk through Spotify's editorial surfaces
    # (Top 50 / Viral 50 / Featured Playlists / New Releases / Browse
    # Categories) to harvest "artists who matter". Rotates through 10
    # markets/day so the full 75-market sweep happens every 7-8 days,
    # well below the rolling rate-limit that hit the old enrich job.
    scheduler.add_job(
        spotify_scan_job,
        IntervalTrigger(hours=24, start_date=_t + _td(minutes=300)),
        id="spotify_scan",
        replace_existing=True,
    )
    # spotify_brave_query — for every SpotifyArtist with
    # match_status='pending_brave', fire one (or, during the A/B phase,
    # both) Brave queries to find event-listing pages we don't already
    # have. Registered LLMSources carry discovered_via='spotify_artist_query'
    # so Cadence A picks them up on the next tick and the funnel closes
    # naturally.
    scheduler.add_job(
        spotify_brave_query_job,
        IntervalTrigger(hours=2, start_date=_t + _td(minutes=330)),
        id="spotify_brave_query",
        replace_existing=True,
    )
    # llm_discover_sources — Cadence B. Daily Brave-Search-driven per-city
    # discovery scan, capped at 10 cities per fire. With ~30 priority
    # cities and LRU ordering, each city gets re-scanned every ~3 days.
    # Cost: 4 queries × 10 cities × 30 days ≈ 1,200 Brave calls/month,
    # comfortably inside the free $5 monthly credit (~1,000 queries).
    # The reserved-domain filter (176c619) plus the in-prompt exclusion
    # list keep the LLM classifier from wasting candidate slots on
    # aggregators we already cover via hand-coded collectors.
    #
    # CronTrigger at 02:30 UTC daily — restart-immune, same pattern as
    # Cadence A's CronTrigger (see comment block at the
    # llm_extract_recurring add_job below). Sits between Cadence A's
    # 00:15 and 03:15 ticks: the 00:15 A fire finishes well before
    # 02:30, B runs for ~22 min (per-city discovery + 500-query
    # vertical-geo phase), and A's 03:15 picks up cleanly after B
    # releases the _heavy_job_lock.
    #
    # Why this fix matters: the previous IntervalTrigger(days=1,
    # start_date=_t + 210min) reset on every Render redeploy, so
    # multiple pushes in a day meant Cadence B never got its 24h
    # window. The vertical-geo matrix sat at 0/410K despite shipping
    # because no Cadence B fire actually happened post-deploy.
    # 2026-05-16 morning audit caught it.
    scheduler.add_job(
        llm_discover_sources_job,
        CronTrigger(hour=2, minute=30),
        id="llm_discover_sources",
        replace_existing=True,
        misfire_grace_time=600,
        coalesce=True,
    )
    # seed_brave_from_zero_results — feeds user dead-end searches into
    # Brave discovery. Daily at 03:00 UTC (30 min after Cadence B's
    # 02:30, comfortably finishes before Cadence A's 03:15 tick).
    # CronTrigger here too so deploys don't reset its schedule.
    # Cost envelope unchanged: ~75 Brave calls/day for new dead-end
    # queries; clean-up handled by Cadence A's promote=1 / block=3 cycle.
    scheduler.add_job(
        seed_brave_from_zero_results_job,
        CronTrigger(hour=3, minute=0),
        id="seed_brave_from_zero_results",
        replace_existing=True,
        misfire_grace_time=600,
        coalesce=True,
    )
    # llm_extract_recurring — Cadence A of Route 1. Re-scans every active
    # LLMSource (state in trial/recurring) on a 3-hour cycle, capped at
    # 150 sources per fire. Math (Path A from the 2026-05-14 scaling
    # conversation): per-source ~23s average → ~57 min per fire of 150
    # sources, well under the next 3h interval; daily throughput ceiling
    # 8 fires × 150 = 1,200 sources/day, plenty for the current ~230
    # active sources to re-scan multiple times. The min_hours_since_last
    # gate (6h, set in jobs.py) is what actually limits re-scans of the
    # same source — the scheduler can fire every 3h but a source only
    # comes due for re-scan every 6h.
    #
    # CronTrigger (not IntervalTrigger) — fires at fixed UTC clock times
    # 00:15, 03:15, 06:15, …, 21:15. This is RESTART-IMMUNE: a Render
    # redeploy can't shove the next fire forward by +240min the way the
    # old boot-relative IntervalTrigger did. Before this change, a day
    # with 3 deploys lost ~12h of Cadence A time per redeploy
    # (the 2026-05-15 fire-skipping investigation). 8 fires/day every
    # day, deploys notwithstanding.
    #
    # misfire_grace_time=600 — if a fire-time falls within 10 min of
    # when the scheduler can actually dispatch (e.g. the box came back
    # up just after :15), APScheduler still runs it. Beyond 10 min, the
    # fire is dropped — coalesce=True means the next due fire absorbs
    # any further missed ones rather than firing back-to-back.
    #
    # Minute :15 sidesteps the on-the-hour competition with other
    # heavy jobs that drift boot-relative.
    scheduler.add_job(
        llm_extract_recurring_job,
        CronTrigger(hour="*/3", minute=15),
        id="llm_extract_recurring",
        replace_existing=True,
        misfire_grace_time=600,
        coalesce=True,
    )
    # classify_new_artists — Daily auto-classification for artists newly
    # introduced by Cadence A (the LLM extractor) and for the rolling
    # UNKNOWN-with-retries-left pool. +30 min after Cadence A so today's
    # extracts are queryable when this runs. Caps spend at 800 artists/
    # night; UNKNOWN-with-attempts >= retry_budget rows get parked and
    # excluded from the pool, so cost converges on net-new arrivals.
    scheduler.add_job(
        classify_new_artists_job,
        IntervalTrigger(hours=24, start_date=_t + _td(minutes=270)),
        id="classify_new_artists",
        replace_existing=True,
    )
    # enrich_youtube_via_brave — Brave-search fallback for artists the
    # YouTube Data API job didn't find a channel for. Daily, slot at
    # boot+330 min — late enough that the YT API job (every 4h) has
    # had several cycles to claim what it can. Hit rate ~40% on the
    # long-tail pool, ~$2.50/night at limit=500. Cache makes per-
    # artist cost one-shot.
    scheduler.add_job(
        enrich_youtube_via_brave_job,
        IntervalTrigger(hours=24, start_date=_t + _td(minutes=330)),
        id="enrich_youtube_via_brave",
        replace_existing=True,
    )
    # recompute_popularity — Weekly recompute of Performer.derived_popularity
    # AND the per-parent-genre percentile thresholds the API uses for UI
    # stars. Weekly cadence: scores change slowly, percentile boundaries
    # drift even slower, daily would be wasted work.
    #
    # CronTrigger (Monday 04:00 UTC) — restart-immune, same fix as Cadence B
    # (48e1631). The previous IntervalTrigger(weeks=1, start_date=_t+300min)
    # reset its 7-day window on every Render redeploy; with deploys landing
    # every day or two the job was effectively starved — last real fire was
    # 2026-05-14, missing its 2026-05-21 slot entirely (caught in the
    # 2026-05-23 audit). 04:00 UTC sits after Cadence B (02:30) and
    # seed-brave (03:00); Monday avoids the Sunday-05:00 dedup job.
    scheduler.add_job(
        recompute_popularity_job,
        CronTrigger(day_of_week="mon", hour=4, minute=0),
        id="recompute_popularity",
        replace_existing=True,
    )
    scheduler.add_job(
        collect_bandsintown_job,
        IntervalTrigger(hours=8, start_date=_t + _td(minutes=25)),
        id="collect_bandsintown",
        replace_existing=True,
    )
    # techconf + mevalim run BEFORE the heavy scraping window opens at t+15.
    # Render auto-redeploys reset _t on every push, so a job scheduled at
    # t+30 keeps getting bumped if pushes land within 30 min of each other.
    # Worse: bandsintown (t+25, ~25 min runtime) blocks the asyncio loop, so
    # APScheduler's 1s misfire_grace_time silently drops any +30…+50 trigger
    # that fires while bandsintown is still running. Result: techconf hadn't
    # successfully run since 2026-04-20 even though PR #8 landed on Apr 21.
    # Moving these to t+3 / t+6 lets them complete before bandsintown starts
    # and well before any reasonable redeploy cadence.
    scheduler.add_job(
        collect_techconf_job,
        IntervalTrigger(hours=24, start_date=_t + _td(minutes=3)),
        id="collect_techconf",
        replace_existing=True,
    )
    # Mevalim (IL event aggregator) — full crawl takes ~2 min via 4 concurrent
    # workers; honours the _heavy_job_lock internally.
    scheduler.add_job(
        collect_mevalim_job,
        IntervalTrigger(hours=24, start_date=_t + _td(minutes=6)),
        id="collect_mevalim",
        replace_existing=True,
    )
    # categorize_new_events — non-destructive, fills missing event_types
    # for rows ingested in the last 48h (LLM extractor output, manual
    # one-offs, some hand-coded collectors). Hourly cadence with a 48h
    # window so a missed tick on Render restart still gets caught next
    # run. Holds no heavy lock — read-heavy + tiny write batch.
    scheduler.add_job(
        categorize_new_events_job,
        IntervalTrigger(hours=1, start_date=_t + _td(minutes=18)),
        id="categorize_new_events",
        replace_existing=True,
    )
    scheduler.start()

    # Seed data in background (non-blocking)
    async def _deferred_seed():
        await asyncio.sleep(5)
        try:
            await asyncio.get_event_loop().run_in_executor(None, _seed_priority_cities)
            await asyncio.get_event_loop().run_in_executor(None, _seed_platform_venues)
            await asyncio.get_event_loop().run_in_executor(None, _seed_event_types)
            await asyncio.get_event_loop().run_in_executor(None, _seed_artist_classifications)
            _log.info("Seeding complete")
        except Exception as e:
            _log.warning(f"Seeding failed: {e}")
    asyncio.create_task(_deferred_seed())

    async def _deferred_sports_fix():
        # Must run *after* _seed_event_types (which starts at t+5s) finishes
        # so "Sports Event" and "Basketball Game" etc. are available to pick.
        await asyncio.sleep(20)
        try:
            await asyncio.get_event_loop().run_in_executor(None, _fix_sports_categories)
            _log.info("Sports category repair complete")
        except Exception as e:
            _log.warning(f"Sports category repair failed: {e}")
    asyncio.create_task(_deferred_sports_fix())

    # One-shot EuroLeague seed on every startup.
    # Rationale: EuroLeague has a tiny upcoming-game window (playoffs, a few
    # dozen games) in ~10 specific host cities. The main scheduler batches
    # 8 cities per 6h run, so EuroLeague-relevant cities like Istanbul,
    # Athens, Tel Aviv can take up to 24h to cycle in. Running *only* the
    # EuroLeague collector here is lightweight (one API call per city,
    # small result set) and makes the search usable immediately after deploy.
    async def _deferred_euroleague_seed():
        await asyncio.sleep(35)  # after seeding + sports fix
        try:
            from app.database import SessionLocal
            from app.models import City
            from app.services.collectors.scrapers.sports.euroleague import EuroLeagueCollector
            from app.scheduler.jobs import registry as main_registry
            EUROLEAGUE_HOSTS = [
                "Madrid", "Barcelona", "Athens", "Istanbul", "Tel Aviv",
                "Paris", "Munich", "Milan", "Belgrade", "Vilnius",
                "Kaunas", "Bologna", "Monaco", "Valencia",
            ]

            # Everything below runs in a worker thread so the sync DB work
            # in _save_events doesn't block uvicorn's event loop.  Inside
            # the thread, we spin up a dedicated event loop for the async
            # EuroLeague collector HTTP calls.
            def _run_blocking():
                import asyncio as _aio
                collector = EuroLeagueCollector()
                with SessionLocal() as id_db:
                    city_ids = [
                        row[0]
                        for row in id_db.query(City.id)
                        .filter(City.name.in_(EUROLEAGUE_HOSTS))
                        .all()
                    ]
                total_saved = 0
                loop = _aio.new_event_loop()
                try:
                    for city_id in city_ids:
                        with SessionLocal() as db:
                            city = db.query(City).get(city_id)
                            if not city:
                                continue
                            try:
                                raw = loop.run_until_complete(
                                    collector.collect(city.name, city.country)
                                )
                                if raw:
                                    saved = main_registry._save_events(raw, city, db)
                                    total_saved += saved
                                    _log.info(
                                        f"EuroLeague seed {city.name}: "
                                        f"fetched={len(raw)} saved={saved}"
                                    )
                            except Exception as e:
                                _log.warning(
                                    f"EuroLeague seed {city.name} failed: {e}"
                                )
                finally:
                    loop.close()
                _log.info(f"EuroLeague seed complete: saved {total_saved} events")

            await asyncio.get_event_loop().run_in_executor(None, _run_blocking)
        except Exception as e:
            _log.warning(f"EuroLeague startup seed failed: {e}")
    asyncio.create_task(_deferred_euroleague_seed())

    yield

    scheduler.shutdown()


app = FastAPI(title="Supercaly", lifespan=lifespan)


# Request-level timeout middleware.
#
# Why: we run a single uvicorn worker (WEB_CONCURRENCY=1) and most DB queries
# are sync — a slow request (e.g. unindexed full-table-scan on a 139K-row
# events table) blocks the asyncio loop and starves the health check, which
# can ultimately get the worker SIGKILL'd by Render. Capping every request
# at 15 s means a single bad query can degrade itself into a 503 instead
# of taking the whole worker down.
#
# Health checks (/ping) are exempt — they must always respond instantly.
_REQUEST_TIMEOUT_SECONDS = 15
_SLOW_REQUEST_LOG_THRESHOLD = 5  # log any request slower than this
_log_timeout = logging.getLogger("supercaly.timeout")


@app.middleware("http")
async def request_timeout_middleware(request: Request, call_next):
    # Skip the health check path so Render's probe never gets a 503.
    if request.url.path == "/ping":
        return await call_next(request)

    started = time.monotonic()
    try:
        response = await asyncio.wait_for(
            call_next(request), timeout=_REQUEST_TIMEOUT_SECONDS
        )
    except asyncio.TimeoutError:
        elapsed = time.monotonic() - started
        _log_timeout.warning(
            "request_timeout path=%s method=%s elapsed=%.2fs query=%s",
            request.url.path,
            request.method,
            elapsed,
            str(request.url.query)[:200],
        )
        return JSONResponse(
            status_code=503,
            content={"detail": "Request timed out. Please try again."},
        )

    elapsed = time.monotonic() - started
    if elapsed >= _SLOW_REQUEST_LOG_THRESHOLD:
        _log_timeout.warning(
            "slow_request path=%s method=%s elapsed=%.2fs status=%d query=%s",
            request.url.path,
            request.method,
            elapsed,
            response.status_code,
            str(request.url.query)[:200],
        )
    return response


# Health-check endpoint — must respond instantly, no DB / blocking work
# Configure this path in Render → Settings → Health Check Path: /ping
@app.get("/ping", include_in_schema=False)
def ping():
    return {"status": "ok"}


# API routers
app.include_router(auth.router)
app.include_router(cities.router)
app.include_router(event_types.router)
app.include_router(events.router)
app.include_router(export.router)
app.include_router(admin.router)
app.include_router(venues.router)
app.include_router(stats.router)
app.include_router(suggestions.router)
app.include_router(artists.router)
app.include_router(platform_venues_api.router)
app.include_router(metro_areas.router)
app.include_router(version_api.router)

# Explicit route for admin page (StaticFiles html=True doesn't reliably resolve /admin → admin.html)
@app.get("/admin")
def admin_page():
    return FileResponse("frontend/admin.html")


# Route 1 audit dashboard — read-only view of LLMSource registry. Operations
# (promote / block / re-extract) live in scripts/llm_run_source.py.
@app.get("/admin/llm-sources")
def admin_llm_sources_page():
    return FileResponse("frontend/llm-sources.html")

# Serve frontend
app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")
