from sqlalchemy import Column, String, DateTime
from sqlalchemy.sql import func

from app.database import Base


class JobState(Base):
    """
    Key/value store for scheduler state that must survive process restarts.

    Module-level globals in scheduler.jobs (e.g. the rotating city batch
    index) were lost whenever Render restarted the app on OOM, so the
    rotation never advanced past the first batch. Persisting the cursor
    here guarantees we keep rotating through PRIORITY_CITIES even across
    restarts.
    """
    __tablename__ = "job_state"

    key = Column(String(64), primary_key=True)
    value = Column(String(255), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
