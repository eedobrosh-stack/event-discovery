"""Registry of LLM-extractable event sources for Route 1.

A row per URL the LLM extractor scans. State machine:

    trial       freshly added; recent runs reviewed by a human OR auto-watched
                until quality signals justify promotion.

    recurring   passes muster; included in scheduled runs (Step 5).

    graduated   we wrote a custom collector for this source; the LLM
                extractor no longer needs to run against it.

    blocked     known-bad — hallucinations, dup of bigger source, fetch
                consistently fails. Skipped by the scheduler.

The CLI in scripts/llm_run_source.py creates / updates these rows on
each manual run. The scheduler integration (Step 5) reads
``state IN ('trial', 'recurring')`` to decide what to scan.
"""
from sqlalchemy import (
    Column, Integer, String, DateTime, Boolean, Float, Text, Index, JSON
)
from sqlalchemy.sql import func

from app.database import Base


# Allowed values for LLMSource.state. Kept here as a frozenset (not a SQL
# enum) so we can extend it without an alembic migration; validated at the
# Python layer.
LLM_SOURCE_STATES = frozenset({"trial", "recurring", "graduated", "blocked"})


class LLMSource(Base):
    __tablename__ = "llm_sources"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # ── Identity ─────────────────────────────────────────────────────────
    # The URL the extractor scans. Unique so re-runs upsert.
    url = Column(String(1000), nullable=False, unique=True)

    # Geographic context. May be null for nationwide / multi-city sources
    # (e.g. tickchak runs once and emits events for all of Israel — those
    # would have city_name=NULL, country='Israel').
    city_name = Column(String(200), nullable=True)
    country = Column(String(100), nullable=True)

    # ── Lifecycle ────────────────────────────────────────────────────────
    state = Column(String(20), nullable=False, default="trial", index=True)

    # ── Cadence + activity ───────────────────────────────────────────────
    last_run_at = Column(DateTime, nullable=True)
    runs_total = Column(Integer, default=0)

    # ── Most-recent-run signals (overwritten each run) ───────────────────
    last_event_count = Column(Integer, nullable=True)
    last_method = Column(String(20), nullable=True)   # html | url_context | error
    last_error = Column(String(500), nullable=True)

    # ── Cumulative metrics ───────────────────────────────────────────────
    events_seen_total = Column(Integer, default=0)    # ever-extracted (pre-dedup)
    events_saved_total = Column(Integer, default=0)   # made it to DB (post-dedup)

    # ── Quality + scheduling signals ─────────────────────────────────────
    # Rolling fraction of events that overlap with other collectors. Set
    # by an offline cron once we have the LLMSource model populated. High
    # dup_rate → candidate for demotion (we already have the inventory).
    dup_rate = Column(Float, nullable=True)
    # Demotion candidate when this passes a threshold (3+).
    consecutive_empty_runs = Column(Integer, default=0)
    # Promotion candidate. When state='trial' and this passes a threshold
    # (3+), the recurring scheduler flips the source to state='recurring'
    # so it stays on the regular cadence without manual review. Reset
    # symmetrically with consecutive_empty_runs.
    consecutive_success_runs = Column(Integer, default=0)

    # ── Pagination evidence (Move 1 of pagination plan) ──────────────────
    has_pagination = Column(Boolean, default=False)
    pagination_signal = Column(String(50), nullable=True)
    next_page_url = Column(String(1000), nullable=True)

    # ── Drift detection (Half 1 task 2/4) ────────────────────────────────
    # Sliding window of the last N event counts (one per run). Capped at
    # 10; oldest dropped when full. Stored as a JSON list under the hood
    # (TEXT in SQLite). Drives drift_score below.
    recent_event_counts = Column(JSON, nullable=True)
    # Computed each run: (prior_avg - last) / max(prior_avg, 1).
    # Range: ≤0 means events held steady or grew. >0 means events shrank;
    # 1.0 = total collapse. The flag is True only when drift_score
    # crosses the threshold AND we have enough history to be confident.
    drift_score = Column(Float, nullable=True)
    drift_flag = Column(Boolean, default=False)

    # ── Operational ──────────────────────────────────────────────────────
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        # state has index=True on the column above; only need the composite here
        Index("ix_llm_sources_country_city", "country", "city_name"),
    )
