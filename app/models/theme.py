"""Event themes — the per-event topic taxonomy.

Distinct from `genre_taxonomy`, which is artist-bound (an artist has
a primary_genre + secondaries, and events inherit genre through their
artist_name link to artist_genre). Themes are bound directly to events
via the event_themes association table — they exist for non-music
content where there's no headliner "artist" to derive a genre from.

Original use case (2026-05-30): conferences. The conference category
had ~5 event_types ('Tech Conference', 'AI Tech Conferences',
'Startup Showcases', 'Cybersecurity Conferences', 'Consumer
Electronics Shows') that baked the topic into the type. The cleaner
shape is type='Conference' + theme='AI' / 'Cybersecurity' / etc., so
users can filter by Conference alone (all conferences) AND by AI
alone (just AI-themed events) cleanly.

The pattern generalises: workshops, festivals, political marches,
career fairs — anything without an artist surface — can use themes
to get the same theme/topic filter UX music gets through genres.

Schema mirrors genre_taxonomy: PK is the theme NAME (string), with
a nullable parent_theme for future hierarchy (e.g. Tech ⊃ AI). V1
ships flat — no parent values set — but the column is there so we
don't migrate later when we want to add Tech / Business / Creative
roll-up categories.
"""
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Index
from sqlalchemy.sql import func

from app.database import Base


class Theme(Base):
    __tablename__ = "themes"

    name = Column(String(100), primary_key=True)
    # Future hierarchy: themes can roll up under a broader parent
    # (e.g. AI / Cybersecurity / DevOps roll up under Tech). V1 leaves
    # this NULL for every theme — flat structure is sufficient for
    # the initial conference-only theme set.
    parent_theme = Column(String(100), nullable=True)
    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("ix_themes_parent", "parent_theme"),
    )


class EventTheme(Base):
    """Many-to-many between events and themes.

    Composite PK (event_id, theme_name) gives natural dedup — an event
    can have multiple themes (e.g. an "AI in FinTech" conference would
    carry both AI and FinTech themes), but the same theme can't be
    written twice to the same event.
    """
    __tablename__ = "event_themes"

    event_id = Column(Integer, ForeignKey("events.id"), primary_key=True)
    theme_name = Column(String(100), ForeignKey("themes.name"), primary_key=True)

    __table_args__ = (
        Index("ix_event_themes_theme", "theme_name"),
    )


# Initial theme set seeded at startup (see _seed_themes in app/main.py).
# Broadened on 2026-05-31 from the original 10 (all tech/business
# conference topics) to ~30, after a "psychology conference" search
# surfaced 0 themed events and the user noted the seed set was
# tech-tilted. New additions cover non-tech conference verticals:
# Psychology / Education / Medicine / Sustainability / Climate / etc.
INITIAL_THEMES = (
    # Original tech-business themes
    "AI",
    "Cybersecurity",
    "Startup",
    "Crypto",
    "Consumer Electronics",
    "DevOps",
    "FinTech",
    "Healthcare",
    "Marketing",
    "Career",
    # 2026-05-31 additions — broader topical themes
    "Psychology",
    "Education",
    "Mental Health",
    "Medicine",
    "Pharmaceutical",
    "Sustainability",
    "Climate",
    "Energy",
    "Real Estate",
    "Legal",
    "Compliance",
    "Manufacturing",
    "Retail",
    "Media",
    "Hospitality",
    "Logistics",
    "Government",
    "Architecture",
    "Agriculture",
    "Insurance",
)
