"""Route 3 prober bookkeeping: one row per domain we have tried to crack
automatically (see app/services/recipes/probe.py). Exists so the hourly
job never probes the same domain twice, and so the outcome distribution
(how many domains have JSON-LD / an ICS feed / a WP Events Calendar API
/ nothing free) is a query, not a log scrape.
"""
from __future__ import annotations

from sqlalchemy import Column, Integer, String, DateTime, Text, Index
from sqlalchemy.sql import func

from app.database import Base

PROBE_OUTCOMES = frozenset({"recipe", "none", "error", "no_country", "reserved"})


class SourceProbe(Base):
    __tablename__ = "source_probes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    domain = Column(String(255), nullable=False, unique=True)
    last_probed_at = Column(DateTime, nullable=True, index=True)
    attempts = Column(Integer, default=0)
    # recipe | none | error | no_country | reserved
    outcome = Column(String(20), nullable=False, default="none", index=True)
    # which detector produced the recipe: jsonld | ics | tribe_rest | (null)
    detector = Column(String(30), nullable=True)
    events_found = Column(Integer, default=0)
    pages_checked = Column(Integer, default=0)
    requests = Column(Integer, default=0)
    # short human-readable trail: statuses, feed URL found, error text
    evidence = Column(Text, nullable=True)
    # yield of this domain under Cadence A — drives probe order
    prior_yield = Column(Integer, default=0)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (Index("ix_source_probes_outcome_probed", "outcome", "last_probed_at"),)
