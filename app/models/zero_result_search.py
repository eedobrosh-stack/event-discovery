"""Audit trail of searches that returned zero events.

Logged when the user runs a search that produces no events anywhere in the
foreseeable future (default-window AND lookahead-extended). Useful for
spotting demand the catalog can't serve — e.g. "12 people searched for
'Coldplay' last week and got nothing" hints at a catalog gap.

We don't dedupe at write-time. Repeated identical searches are useful
signal (popularity), and a periodic cleanup job keeps the table bounded.
"""
from sqlalchemy import Column, Integer, String, Date, DateTime, Index
from sqlalchemy.sql import func

from app.database import Base


class ZeroResultSearch(Base):
    __tablename__ = "zero_result_searches"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, server_default=func.now(), nullable=False)

    # Chip values, comma-joined per kind so the row reads at a glance.
    # Stored separately rather than as a single JSON blob so SQL queries
    # like "top 20 missing genres last 30 days" stay one-liners.
    genres = Column(String(500), nullable=True)        # "Rock,Electronic"
    artists = Column(String(500), nullable=True)       # "Coldplay"   (artist_exact)
    type_search = Column(String(500), nullable=True)   # "Rock Concert,Jazz"
    free_search = Column(String(500), nullable=True)   # the dedicated #search box

    # Geographic + temporal context the user was looking at when the
    # search came up empty. city_ids is a string (matches the URL form,
    # may include comma-separated values for metro areas).
    city_ids = Column(String(200), nullable=True)
    country = Column(String(100), nullable=True)
    start_date = Column(Date, nullable=True)
    end_date = Column(Date, nullable=True)

    # Truncated to 500 chars to bound a single bad client. Optional.
    user_agent = Column(String(500), nullable=True)

    __table_args__ = (
        Index("ix_zero_result_searches_timestamp", "timestamp"),
    )
