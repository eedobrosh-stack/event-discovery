from sqlalchemy import Column, Integer, String, Boolean, DateTime, Index
from sqlalchemy.sql import func

from app.database import Base


class FetchAttempt(Base):
    """Persisted record of every failed HTTP fetch in the extractor stack.

    Why this exists
    ---------------
    Before this table, fetch failures lived only in Render's stderr logs.
    Logs rotate, can't be SELECT'd, and don't tell us which domains are
    Cloudflare-walled vs. just transient flaky. That made the
    "unknown unknowns" — sources we've never tried, or sources that
    silently 403 every night — invisible.

    With this table:
      * `is_cf_challenge=True` rows = "needs paid scraper" queue
      * Aggregate `domain` + COUNT to find the worst offenders
      * `attempted_at` lets us spot regressions when a previously-working
        domain starts failing
      * Cross-reference with `LLMSource.url` to see which Route-1 sources
        are hopeless from our IP

    Write contract
    --------------
    Written by `_fetch_html` in app/extractors/llm_extractor.py, and only
    on failure (non-2xx, exception, or empty body). Best-effort — a write
    failure here never breaks the fetcher. Hand-coded collectors can
    opt into the same logging by importing `record_fetch_failure`.

    Retention
    ---------
    Keep ~30 days. A cleanup tick (TBD) can trim — for now the row
    volume is bounded by fetch volume (~1-2k attempts/day) so it's
    fine to let it grow until we wire retention.
    """
    __tablename__ = "fetch_attempt"

    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(String(1000), nullable=False)
    domain = Column(String(200), nullable=False, index=True)

    # http_status: NULL when the failure was at connection level (DNS,
    # TLS handshake, timeout) — no HTTP response was received. error_class
    # tells you which case it was.
    http_status = Column(Integer, nullable=True)
    error_class = Column(String(100), nullable=True)
    error_message = Column(String(500), nullable=True)

    # is_cf_challenge: response body matched the Cloudflare managed-
    # challenge signature ("Just a moment", "challenge-platform", or
    # "cf-mitigated" header). Used as the gate for paid-scraper fallback.
    is_cf_challenge = Column(Boolean, default=False, index=True)

    # fetcher: which path produced this attempt — 'curl_cffi', 'urllib',
    # 'scrapingbee'. Lets us tell apart "curl_cffi failed but urllib
    # worked" from "both paths failed."
    fetcher = Column(String(50), nullable=True)

    attempted_at = Column(DateTime, server_default=func.now(), index=True)


# Composite index for the most common query: "all CF-walled domains in
# the last N days, deduped." Without this, the dashboard query has to
# scan-and-sort.
Index(
    "ix_fetch_attempt_cf_recent",
    FetchAttempt.is_cf_challenge,
    FetchAttempt.attempted_at.desc(),
    FetchAttempt.domain,
)
