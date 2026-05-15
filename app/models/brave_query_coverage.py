"""Tracks which (vertical, geo) pairs Cadence-B Brave discovery has fired.

Lets the vertical-geo phase of llm_discover_sources_job rotate through
the ~3,700-pair matrix deterministically — pick the oldest-fired-first
(or never-fired-first) batch each cycle, fire them, stamp the row.
After a full pass the matrix re-circulates with the oldest entries due
again, so coverage refreshes monthly at the configured pacing.

Per row:
  kind        — "category_city" / "conference_city" / "conference_country"
  vertical    — the topical term (category name or conference vertical)
  geo_type    — "city" | "country"
  geo_name    — resolved place name
  fired_at    — NULL means "never fired yet"; otherwise last fire time
  hits        — Brave results returned on last fire
  new_sources — LLMSource rows created on last fire (post-dedupe + filter)

The unique constraint on (kind, vertical, geo_name) keeps the table
size bounded at the matrix cardinality — it doesn't grow with fires;
each fire just updates the existing row's fired_at + counts.
"""
from sqlalchemy import Column, Integer, String, DateTime, Index, UniqueConstraint
from sqlalchemy.sql import func

from app.database import Base


class BraveQueryCoverage(Base):
    __tablename__ = "brave_query_coverage"

    id = Column(Integer, primary_key=True, autoincrement=True)
    kind = Column(String(40), nullable=False)
    vertical = Column(String(200), nullable=False)
    geo_type = Column(String(20), nullable=False)
    geo_name = Column(String(200), nullable=False)
    fired_at = Column(DateTime, nullable=True)
    hits = Column(Integer, nullable=False, default=0)
    new_sources = Column(Integer, nullable=False, default=0)
    # 2 = Wave 1 (top-100 cities × all cats/verts),
    # 1 = Wave 2 (OECD × top-10 cats / top-6 verts),
    # 0 = long-tail rotation.
    # Picker orders by priority DESC, then fired_at NULLS FIRST.
    priority = Column(Integer, nullable=False, default=0)

    __table_args__ = (
        Index("ix_brave_query_coverage_fired_at", "fired_at"),
        # Composite index for the picker's hot path:
        # `ORDER BY priority DESC, fired_at NULLS FIRST` over the
        # whole table. Without this the picker does a full scan
        # every fire.
        Index("ix_brave_query_coverage_priority_fired",
              "priority", "fired_at"),
        UniqueConstraint("kind", "vertical", "geo_name",
                         name="uq_brave_query_coverage_combo"),
    )
