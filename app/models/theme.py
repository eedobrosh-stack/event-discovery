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


# Search-time aliases — alternative terms a user might type that
# should surface as their OWN first-class chip in the autocomplete.
# NOT stored in the themes table: the canonical name is still the
# only thing event_themes rows ever carry. The events API resolves
# an alias → canonical theme(s) at query time (see
# _THEME_ALIAS_LOOKUP in app/api/events.py), so a URL like
# ?themes=Tech expands server-side to filter by the 6 tech-business
# themes.
#
# Each unique alias produces ONE chip whose display label is the
# alias itself and whose filter is the multi-theme bundle of every
# canonical it appears under in this dict. So typing "Tech" yields
# the single chip "Tech" (NOT 6 separate canonical chips), and
# clicking that chip returns events from all 6 tech-business
# themes. Same for biotech (Healthcare + Pharmaceutical),
# adtech (Marketing), VC (Startup), wellness (Mental Health), etc.
#
# Cross-theme aliases (intentional dual/triple maps — typing the term
# returns multiple chips):
#   Tech / Technology  → AI, Cybersecurity, Crypto, Consumer Electronics,
#                        DevOps, FinTech            (the 6 tech-business)
#   biotech            → Healthcare, Pharmaceutical
#   regtech            → FinTech, Compliance
#   automation         → DevOps, Manufacturing
#   robotics           → AI, Manufacturing
#   metaverse          → Crypto, Consumer Electronics
#   gaming             → Consumer Electronics, Media
#   advertising week   → Marketing, Media           (mirrors the
#                                                    classifier-side
#                                                    dual-tag)
THEME_ALIASES: dict[str, tuple[str, ...]] = {
    "AI": (
        "artificial intelligence", "machine learning", "ML",
        "deep learning", "GenAI", "generative AI", "LLM", "LLMs",
        "large language model", "neural networks", "data science",
        "MLOps", "AI/ML", "robotics",
        "Tech", "Technology",
    ),
    "Cybersecurity": (
        "cyber security", "cyber-security", "infosec", "info-sec",
        "information security", "IT security", "ethical hacking",
        "pentest", "penetration testing", "zero trust",
        "threat intel", "threat intelligence", "SOC", "security",
        "Tech", "Technology",
    ),
    "Startup": (
        "startups", "founders", "VC", "venture capital", "accelerator",
        "demo day", "pitch night", "Y Combinator", "YC",
        "entrepreneurship", "entrepreneur", "scaleup", "scale-up",
    ),
    "Crypto": (
        "cryptocurrency", "blockchain", "web3", "web 3.0", "web 3",
        "bitcoin", "BTC", "ethereum", "ETH", "NFT", "NFTs",
        "DeFi", "decentralized finance", "DAO", "token", "metaverse",
        "Tech", "Technology",
    ),
    "Consumer Electronics": (
        "CE", "electronics", "gadgets", "IoT", "internet of things",
        "CES", "smart home", "wearables", "hardware",
        "AR/VR", "AR", "VR", "augmented reality", "virtual reality",
        "metaverse", "gaming",
        "Tech", "Technology",
    ),
    "DevOps": (
        "dev ops", "dev-ops", "SRE", "site reliability",
        "platform engineering", "Kubernetes", "K8s", "KubeCon",
        "cloud native", "cloud", "CI/CD", "infrastructure",
        "observability", "automation",
        "Tech", "Technology",
    ),
    "FinTech": (
        "fin-tech", "fin tech", "financial technology", "payments",
        "banking", "neobank", "open banking", "embedded finance",
        "regtech", "Money 20/20",
        "Tech", "Technology",
    ),
    "Healthcare": (
        "healthtech", "health-tech", "health tech", "medtech", "med-tech",
        "biotech", "biotechnology", "life sciences", "digital health",
        "telemedicine", "telehealth",
    ),
    "Marketing": (
        "marketing tech", "martech", "mar-tech",
        "adtech", "ad-tech", "advertising", "advertising tech",
        "SEO", "SEM", "growth marketing", "growth hacking",
        "content marketing", "performance marketing", "CMO", "brand",
        "advertising week", "influencer",
    ),
    "Career": (
        "careers", "jobs", "job fair", "recruiting", "recruitment",
        "hiring", "talent acquisition", "professional development",
        "networking event",
    ),
    "Psychology": (
        "psych", "psychological", "psychologist", "cognitive science",
        "behavioral science", "neuropsychology", "clinical psychology",
    ),
    "Education": (
        "edtech", "ed-tech", "ed tech", "teaching", "schools",
        "K-12", "k12", "higher ed", "higher education", "university",
        "academic", "pedagogy", "learning", "e-learning",
    ),
    "Mental Health": (
        "mental wellness", "wellness", "wellbeing", "well-being",
        "psychiatry", "psychiatric", "psychotherapy", "therapy",
        "counseling", "mindfulness", "behavioral health",
    ),
    "Medicine": (
        "medical", "clinical", "cardiology", "oncology", "neurology",
        "surgery", "pediatrics", "radiology", "doctors", "physicians",
        "hospital",
    ),
    "Pharmaceutical": (
        "pharma", "pharmaceuticals", "drug development",
        "clinical trials", "big pharma", "drug discovery",
        "biopharma", "biotech",
    ),
    "Sustainability": (
        "ESG", "environmental social governance", "circular economy",
        "green", "eco", "sustainable business",
    ),
    "Climate": (
        "climate change", "climate tech", "climatetech",
        "cleantech", "clean-tech", "clean tech",
        "decarbonization", "decarbonisation",
        "net zero", "net-zero", "COP", "climate action",
    ),
    "Energy": (
        "renewable energy", "renewables", "solar", "wind", "hydrogen",
        "oil and gas", "oil & gas", "utilities", "power", "electricity",
        "nuclear",
    ),
    "Real Estate": (
        "proptech", "prop-tech", "prop tech", "property", "real-estate",
        "commercial real estate", "CRE", "property tech", "housing",
    ),
    "Legal": (
        "law", "legaltech", "legal-tech", "legal tech",
        "lawyers", "attorneys", "litigation", "law firm",
        "judicial", "in-house counsel",
    ),
    "Compliance": (
        "regulatory", "regulation", "AML", "anti-money laundering",
        "GDPR", "audit", "risk management", "KYC", "governance",
        "regtech", "data privacy",
    ),
    "Manufacturing": (
        "industry 4.0", "industry 4", "industrial",
        "smart manufacturing", "factory", "factory automation",
        "automation", "robotics",
    ),
    "Retail": (
        "ecommerce", "e-commerce", "e commerce",
        "retail tech", "retailtech", "retail-tech",
        "shopping", "omnichannel", "NRF", "Shoptalk",
        "retail innovation",
    ),
    "Media": (
        "broadcasting", "journalism", "news", "publishing",
        "creator economy", "streaming", "podcast", "podcasting",
        "advertising week", "gaming",
    ),
    "Hospitality": (
        "hotel", "hotels", "travel", "tourism", "hotelier",
        "hospitality industry", "hotel investment", "travel tech",
    ),
    "Logistics": (
        "supply chain", "freight", "shipping", "warehouse",
        "transportation", "last mile", "last-mile",
        "fulfillment", "fleet",
    ),
    "Government": (
        "govtech", "gov-tech", "gov tech", "public sector",
        "civic tech", "civictech", "policy",
        "government tech", "public administration",
    ),
    "Architecture": (
        "architects", "design", "urban planning", "urban design",
        "AIA", "built environment", "urbanism",
    ),
    "Agriculture": (
        "agtech", "ag-tech", "ag tech", "farming", "farmers",
        "food systems", "agribusiness", "agro", "agrotech", "agritech",
    ),
    "Insurance": (
        "insurtech", "insur-tech", "insur tech", "underwriting",
        "insurance tech", "reinsurance", "claims", "risk transfer",
    ),
}
