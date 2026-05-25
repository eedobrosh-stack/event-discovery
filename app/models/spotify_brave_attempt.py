"""One row per Brave query fired for a Spotify-sourced artist.

Drives the A/B test between the two seed query variants:
    "{artist} shows"
    "{artist} upcoming performances"

While the global trial count for either variant is below 100, every artist
gets BOTH queries. Once both variants have ≥100 attempts each, the
`spotify_brave_query_job` picks the winner — the variant with the higher
mean `new_llm_sources_registered` — and runs only that one going forward.

`new_llm_sources_registered` is the A/B metric (not raw `brave_results_count`)
because the goal is finding event-listing pages we don't already have, not
maximising Brave hits.
"""
from sqlalchemy import (
    Column, Integer, String, DateTime, ForeignKey, Index, Text,
)
from sqlalchemy.sql import func

from app.database import Base


SPOTIFY_BRAVE_VARIANTS = frozenset({"shows", "upcoming_performances"})


class SpotifyBraveAttempt(Base):
    __tablename__ = "spotify_brave_attempts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    spotify_artist_id = Column(
        String(40), ForeignKey("spotify_artists.id"), nullable=False, index=True
    )
    # "shows" | "upcoming_performances" — kept as a short enum string so
    # GROUP BY query_variant is a single index hit.
    query_variant = Column(String(40), nullable=False)
    attempted_at = Column(DateTime, server_default=func.now())

    # Raw Brave results returned. Useful for sanity-checking the A/B
    # against the "Brave volume" alternative metric we didn't pick.
    brave_results_count = Column(Integer, default=0)
    # The actual A/B metric: how many of those results passed the
    # event-listing classifier AND weren't already in llm_sources.
    new_llm_sources_registered = Column(Integer, default=0)

    notes = Column(Text, nullable=True)

    __table_args__ = (
        Index("ix_spotify_brave_attempts_variant", "query_variant"),
    )
