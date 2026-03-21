from sqlalchemy import Column, Integer, String, Text, Table, ForeignKey
from sqlalchemy.orm import relationship

from app.database import Base

event_event_types = Table(
    "event_event_types",
    Base.metadata,
    Column("event_id", Integer, ForeignKey("events.id", ondelete="CASCADE"), primary_key=True),
    Column("event_type_id", Integer, ForeignKey("event_types.id", ondelete="CASCADE"), primary_key=True),
)


class EventType(Base):
    __tablename__ = "event_types"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False, unique=True)
    category = Column(String(100), nullable=True)
    description = Column(Text, nullable=True)
    keywords = Column(Text, nullable=True)

    events = relationship(
        "Event", secondary=event_event_types, back_populates="event_types"
    )
