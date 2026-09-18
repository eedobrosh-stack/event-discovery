"""Cut-in-line requests for the Route 3 parse queue (superca.ly/queue.html).

One row per domain a human pushed to the front of the line. The prober
(app/services/recipes/probe.py::select_candidates) probes pinned domains
before anything else, in `rank` order, even if they were probed before
('none' outcome) or are not in the LLMSource pool at all (homepage is
used as the entry URL). An optional `country` lets the human supply what
the prober could not infer (the 'no_country' outcome). If the domain
already has a SourceRecipe, pinning bumps its priority and makes it due
for the next 3-hourly sweep instead.
"""
from __future__ import annotations

from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy.sql import func

from app.database import Base

PIN_STATUSES = frozenset({"queued", "probed", "recipe", "done"})


class QueuePin(Base):
    __tablename__ = "queue_pins"

    id = Column(Integer, primary_key=True, autoincrement=True)
    domain = Column(String(255), nullable=False, unique=True)
    rank = Column(Integer, nullable=False, default=0, index=True)   # lower = sooner
    country = Column(String(100), nullable=True)                   # human-supplied
    note = Column(Text, nullable=True)
    # queued → probed (outcome in `outcome`) | recipe (recipe existed / was
    # created) | done (recipe sweep ran it after the priority bump)
    status = Column(String(20), nullable=False, default="queued", index=True)
    outcome = Column(String(40), nullable=True)
    resolved_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
