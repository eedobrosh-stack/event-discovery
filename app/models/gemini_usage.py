"""Per-day, per-model Gemini token usage — the foundation for spend
monitoring against the monthly cap (500 NIS as of 2026-06).

There is no billing API wired into this app; the circuit breaker only learns
the cap is hit *reactively* (Google returns a 429 "monthly spending cap").
This table gives us *foresight*: every Gemini response carries
``usage_metadata`` with exact token counts, so we record them here and a
daily report (scripts/gemini_spend_report.py) projects month-end NIS against
the cap.

We store raw TOKENS (exact), not dollars — pricing is applied at report time
so a rate change never requires a data migration. One row per (day, model);
``record_gemini_usage`` upserts-increments it. The recorder is fully
exception-safe: monitoring must never break an extraction.
"""
from datetime import datetime
import logging

from sqlalchemy import Column, Integer, String, UniqueConstraint, Index

from app.database import Base, SessionLocal

logger = logging.getLogger(__name__)


class GeminiUsage(Base):
    __tablename__ = "gemini_usage"

    id = Column(Integer, primary_key=True, autoincrement=True)
    day = Column(String(10), nullable=False)        # 'YYYY-MM-DD' (UTC)
    model = Column(String(60), nullable=False)      # e.g. gemini-2.5-flash
    calls = Column(Integer, default=0, nullable=False)
    prompt_tokens = Column(Integer, default=0, nullable=False)   # input
    output_tokens = Column(Integer, default=0, nullable=False)   # candidates + thoughts

    __table_args__ = (
        UniqueConstraint("day", "model", name="uq_gemini_usage_day_model"),
        Index("ix_gemini_usage_day", "day"),
    )


def record_gemini_usage(resp, model: str) -> None:
    """Accumulate one Gemini response's token usage into the daily row.

    Safe to call from any Gemini call site right after generate_content
    returns. Swallows all errors — a monitoring write must never propagate
    into the extraction/classification path. Heavy Gemini jobs are serialized
    by _heavy_job_lock so the get-or-create increment doesn't race in
    practice; if it ever does, the lost row is one call's worth of tokens.
    """
    try:
        um = getattr(resp, "usage_metadata", None)
        if um is None:
            return
        pt = int(getattr(um, "prompt_token_count", 0) or 0)
        ct = int(getattr(um, "candidates_token_count", 0) or 0)
        # 2.5 models bill "thinking" tokens as output; include when present.
        th = int(getattr(um, "thoughts_token_count", 0) or 0)
        out = ct + th
        day = datetime.utcnow().strftime("%Y-%m-%d")

        db = SessionLocal()
        try:
            row = (
                db.query(GeminiUsage)
                .filter(GeminiUsage.day == day, GeminiUsage.model == model)
                .first()
            )
            if row is None:
                row = GeminiUsage(
                    day=day, model=model, calls=0, prompt_tokens=0, output_tokens=0
                )
                db.add(row)
            row.calls += 1
            row.prompt_tokens += pt
            row.output_tokens += out
            db.commit()
        finally:
            db.close()
    except Exception:
        logger.debug("record_gemini_usage failed (non-fatal)", exc_info=True)
