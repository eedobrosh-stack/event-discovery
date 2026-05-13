"""Third-party services and their monthly fees.

Static catalog rendered by the Admin → Services & Costs tab. Edit
this file when a plan changes; the admin page reads it via
``GET /api/admin/services``.

Conventions:

* ``monthly_fee_usd`` is the steady-state monthly cost in USD. Use
  ``0`` for free tiers, ``None`` (and a note) for usage-based or
  paid-but-unverified entries.
* ``tier`` is a short string for the plan family ("Free",
  "Pay-as-you-go", "Starter $49", etc.). Surfaced as a column on the
  admin page so a glance tells which entries are paid.
* ``purpose`` is a one-liner — what we use the service for, not what
  the service does in general.
* ``notes`` is optional — rate limits, link to dashboard, anything
  that helps future-you keep the number honest.

User-supplied monthly fees marked TBD below are placeholders — they
need to be filled in from the actual billing dashboard for each
provider. Costs that are usage-driven (Gemini per-token, ScrapingBee
per-credit) should reflect the *typical* current month rather than
the plan ceiling, so the dashboard total stays informative.
"""
from __future__ import annotations

from typing import TypedDict, Optional


class _Service(TypedDict):
    name: str
    category: str
    purpose: str
    tier: str
    monthly_fee_usd: Optional[float]
    notes: Optional[str]


SERVICES: list[_Service] = [
    # ── Hosting ─────────────────────────────────────────────────
    {
        "name": "Render",
        "category": "Hosting",
        "purpose": "App + SQLite persistent disk (~580 GB mount, ~107 GB free).",
        "tier": "Paid plan",
        "monthly_fee_usd": None,
        "notes": "TBD — fill in from Render dashboard. Hosts the FastAPI app and the events.db SQLite file.",
    },

    # ── LLM ────────────────────────────────────────────────────
    {
        "name": "Google Gemini",
        "category": "LLM",
        "purpose": "Cadence-A event extraction, Cadence-B source classification, genre classifier.",
        "tier": "Pay-as-you-go",
        "monthly_fee_usd": None,
        "notes": "TBD — variable per token. Gemini 1.5 Flash ~$0.075 / 1M input · $0.30 / 1M output. Check Google AI Studio billing.",
    },

    # ── Web fetch infrastructure ───────────────────────────────
    {
        "name": "Brave Search API",
        "category": "Discovery",
        "purpose": "Cadence-B source discovery (themed per-city queries) + Lever-C artist re-classify.",
        "tier": "Paid",
        "monthly_fee_usd": None,
        "notes": "TBD — Brave plans start at $3/mo for 20k queries; usage scales with city count.",
    },
    {
        "name": "ScrapingBee",
        "category": "Web fetch",
        "purpose": "Anti-bot HTML fallback when curl_cffi gets blocked (Cloudflare etc.).",
        "tier": "Starter $49 or higher",
        "monthly_fee_usd": None,
        "notes": "TBD — credits-based; consumed when curl_cffi hits a CF wall.",
    },

    # ── Free-tier enrichment APIs ──────────────────────────────
    {
        "name": "Spotify Web API",
        "category": "Enrichment",
        "purpose": "Performer image, popularity proxy, Spotify profile URL.",
        "tier": "Free (Client Credentials)",
        "monthly_fee_usd": 0.0,
        "notes": "Public popularity field was deprecated late 2024; only image + URL still useful.",
    },
    {
        "name": "YouTube Data API",
        "category": "Enrichment",
        "purpose": "Performer YouTube channel lookup for the YouTube column.",
        "tier": "Free (10k units/day)",
        "monthly_fee_usd": 0.0,
        "notes": "Per-day quota; not a billed service today.",
    },
    {
        "name": "TheSportsDB",
        "category": "Enrichment",
        "purpose": "Per-fixture TV broadcaster data for Tournament rows.",
        "tier": "Free (public dev key)",
        "monthly_fee_usd": 0.0,
        "notes": "Sparse pre-tournament; LLM-sourced fallback in app/services/tournaments.py fills the gaps.",
    },

    # ── Event ingestion APIs (Route 2 collectors) ─────────────
    {
        "name": "Ticketmaster Discovery API",
        "category": "Event source",
        "purpose": "Concerts + sports fixtures ingestion (52k events on prod — second-largest source).",
        "tier": "Free (rate-limited)",
        "monthly_fee_usd": 0.0,
        "notes": "Default 5 reqs/sec; no paid tier in use.",
    },
    {
        "name": "Bandsintown",
        "category": "Event source",
        "purpose": "Concert ingestion (65k events on prod — largest single source).",
        "tier": "Free",
        "monthly_fee_usd": 0.0,
        "notes": "No paid tier in use.",
    },
    {
        "name": "Eventbrite",
        "category": "Event source",
        "purpose": "Event listings — API deprecated for our use case; collector now web-scrapes.",
        "tier": "Free (scraped)",
        "monthly_fee_usd": 0.0,
        "notes": "EVENTBRITE_TOKEN kept in env for legacy code paths but the active collector is the scraper.",
    },
    {
        "name": "SeatGeek",
        "category": "Event source",
        "purpose": "Event listings.",
        "tier": "Free (B2B partner key)",
        "monthly_fee_usd": 0.0,
        "notes": None,
    },
    {
        "name": "PredictHQ",
        "category": "Event source",
        "purpose": "(Configured in .env but no active collector — verify usage before renewing.)",
        "tier": "Paid (custom)",
        "monthly_fee_usd": None,
        "notes": "TBD — confirm whether PredictHQ is still in use. If not, retire the token.",
    },
]


def total_known_monthly_fee_usd() -> float:
    """Sum of monthly fees where the value is known (None entries skipped)."""
    return sum(s["monthly_fee_usd"] or 0.0 for s in SERVICES if s["monthly_fee_usd"] is not None)


def unknown_fee_count() -> int:
    """How many entries still have a TBD fee — drives the 'X services
    need verification' badge on the admin tab."""
    return sum(1 for s in SERVICES if s["monthly_fee_usd"] is None)
