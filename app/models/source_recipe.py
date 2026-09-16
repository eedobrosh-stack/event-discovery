"""Route 3 — recipe-driven (Gemini-free) long-tail extraction.

One row per DOMAIN. The `recipe` JSON column holds the full document
(see docs/recipe_extraction_design.md for the schema). Recipes are
authored interactively on Eedo's Mac (Claude Code session), kept in git
under recipes/<domain>.json as the source of truth, and pushed into this
table with scripts/recipe_run.py --upsert. The nightly
`recipe_extract_job` reads only from this table.

Health columns mirror LLMSource's drift machinery so the same mental
model applies: a recipe that stops yielding gets `drift_flag=True` and
lands in the next session's repair queue.
"""
from __future__ import annotations

from sqlalchemy import (
    Column, Integer, String, DateTime, Boolean, Text, JSON, Index,
)
from sqlalchemy.sql import func

from app.database import Base


# Allowed values for SourceRecipe.last_status. Python-level frozenset
# (not a SQL enum) so it can grow without a migration.
RECIPE_STATUSES = frozenset({"never_run", "ok", "empty", "error", "drift"})


class SourceRecipe(Base):
    __tablename__ = "source_recipes"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # ── Identity ─────────────────────────────────────────────────────────
    # Bare registered domain, lowercase, no "www." — e.g. "icm.org.il".
    domain = Column(String(255), nullable=False, unique=True)
    # Becomes RawEvent.source on every event this recipe emits, so the
    # existing (source, source_id) dedupe and /api/stats/source-matrix
    # work unchanged. Unique per recipe.
    source_name = Column(String(60), nullable=False, unique=True)

    enabled = Column(Boolean, nullable=False, default=True, index=True)

    # ── The recipe document ──────────────────────────────────────────────
    recipe = Column(JSON, nullable=False)
    recipe_version = Column(Integer, nullable=False, default=1)

    # Denormalised from recipe for the City fallback (same rule as
    # Cadence A: city_name → any City in country → skip persist).
    country = Column(String(100), nullable=True)
    city_name = Column(String(200), nullable=True)

    # ── Scheduling ───────────────────────────────────────────────────────
    # Higher runs first. Seeded from Cadence A yield for known domains.
    priority = Column(Integer, nullable=False, default=0)
    cadence_hours = Column(Integer, nullable=False, default=24)
    last_run_at = Column(DateTime, nullable=True)
    next_run_at = Column(DateTime, nullable=True, index=True)

    # ── Most-recent-run signals ──────────────────────────────────────────
    last_status = Column(String(20), nullable=False, default="never_run")
    last_error = Column(Text, nullable=True)
    last_fetched = Column(Integer, default=0)   # events parsed (pre-dedupe)
    last_saved = Column(Integer, default=0)     # events persisted (post-dedupe)
    last_requests = Column(Integer, default=0)  # HTTP requests spent
    last_duration_s = Column(Integer, default=0)

    # ── Cumulative ───────────────────────────────────────────────────────
    runs_total = Column(Integer, default=0)
    fetched_total = Column(Integer, default=0)
    saved_total = Column(Integer, default=0)

    # ── Drift / repair queue ─────────────────────────────────────────────
    recent_fetched_counts = Column(JSON, nullable=True)   # last 10 runs
    consecutive_zero_fetch = Column(Integer, default=0)
    consecutive_errors = Column(Integer, default=0)
    drift_flag = Column(Boolean, default=False, index=True)

    # ── Provenance ───────────────────────────────────────────────────────
    written_by = Column(String(40), nullable=True)   # claude-session | manual
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        Index("ix_source_recipes_enabled_next", "enabled", "next_run_at"),
    )

    def __repr__(self) -> str:  # pragma: no cover
        return (f"<SourceRecipe {self.domain} v{self.recipe_version} "
                f"{'on' if self.enabled else 'off'} {self.last_status}>")
