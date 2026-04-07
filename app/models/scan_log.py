from sqlalchemy import Column, Integer, String, Text, DateTime
from sqlalchemy.sql import func

from app.database import Base


class ScanLog(Base):
    """Records each scheduler job run for the admin dashboard."""
    __tablename__ = "scan_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_name = Column(String(100), nullable=False)   # collect_events, venue_websites, dedup, cleanup
    detail = Column(String(255), nullable=True)       # e.g. city name or source name
    started_at = Column(DateTime, server_default=func.now())
    finished_at = Column(DateTime, nullable=True)
    events_found = Column(Integer, default=0)
    events_saved = Column(Integer, default=0)
    status = Column(String(20), default="running")    # running | success | failed
    notes = Column(Text, nullable=True)
