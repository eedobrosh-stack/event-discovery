"""Process-wide circuit breaker for the Gemini billing-cap exception.

When the Gemini project hits its monthly spending cap, every call
returns the same definitive error — not a transient rate-limit:

    ClientError: 429 RESOURCE_EXHAUSTED.
    {'error': {'code': 429,
      'message': 'Your project has exceeded its monthly spending cap.
                  Please go to AI Studio at https://ai.studio/spend ...'}}

The existing retry loops in llm_extractor / discovery_search / etc.
correctly identify "RESOURCE_EXHAUSTED" as transient and back off 10/20/40s
between attempts. That's right for momentary rate limits — wrong for a
hard cap, which won't recover until the month rolls over (or the cap
is raised manually). Pre-circuit, the conference-classifier drain on
2026-05-31 spent the entire batch retrying-and-failing against the
exhausted cap, wasting wall-clock and adding to the user-visible error
flood in the logs.

This module gives callers a tiny shared state:

  is_open()           returns True when the cap has been observed.
                       Callers should check at the start of each
                       per-item iteration and BAIL the run if True.

  should_trip(msg)    True when the error message contains the
                       specific cap signature ("monthly spending
                       cap" / "spend cap"). False on transient 429s
                       and 503s so generic rate-limit retries still
                       get their chance.

  maybe_trip(msg)     If the error matches the cap signature, set
                       the breaker to open + log loudly. No-op on
                       transient errors.

State is process-local. Render's daily restarts reset it naturally;
manual reset is rarely needed but available via `reset()` for tests.

Not a substitute for raising the cap — it just stops the bleeding
once it's been hit.
"""
from __future__ import annotations

import logging
import threading
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


# How long the breaker stays tripped before probing again. The Gemini
# billing cap is monthly, so 6h is a conservative re-probe interval
# that covers the typical end-of-month → start-of-month rollover
# without sitting stale forever waiting for a manual restart. If the
# cap is still exhausted at the probe, the next failed call re-trips
# the breaker with a fresh timestamp; if the cap was raised or rolled
# over, the call succeeds and the breaker stays closed.
_AUTO_RESET_HOURS = 6

# Module-level state — protected by a lock for write paths so concurrent
# Gemini callers don't race on the trip flag. Reads are unlocked
# (boolean read is atomic; worst case we make one extra doomed call
# before noticing the breaker opened).
_lock = threading.Lock()
_is_open: bool = False
_tripped_at: Optional[datetime] = None
_trip_reason: str = ""


class GeminiCircuitOpen(Exception):
    """Raised by callers when they want to abort because the breaker is open.

    Lets background jobs catch this specifically and exit with a clean
    "skipped — Gemini cap" log instead of treating it as an
    extraction/classification error.
    """
    def __init__(self, reason: str = ""):
        self.reason = reason or _trip_reason
        super().__init__(f"Gemini circuit breaker is open: {self.reason}")


def is_open() -> bool:
    """True when the breaker has tripped. Callers should bail in that case.

    Self-healing: if the trip is older than _AUTO_RESET_HOURS, this call
    transitions the breaker back to closed (under lock) and returns
    False so the caller can probe. The next failed call with the
    spend-cap signature will re-trip the breaker with a fresh
    timestamp; a successful call leaves the breaker closed.
    """
    global _is_open, _tripped_at, _trip_reason
    if not _is_open:
        return False
    # Outside the lock first (fast path — boolean compare).
    if _tripped_at is None:
        return True
    elapsed = (datetime.utcnow() - _tripped_at).total_seconds()
    if elapsed <= _AUTO_RESET_HOURS * 3600:
        return True
    # Eligible for auto-reset — re-verify inside the lock to handle the
    # race where multiple threads cross the threshold simultaneously.
    with _lock:
        if not _is_open or _tripped_at is None:
            return False
        elapsed2 = (datetime.utcnow() - _tripped_at).total_seconds()
        if elapsed2 <= _AUTO_RESET_HOURS * 3600:
            return True
        logger.info(
            "Gemini circuit breaker AUTO-RESET after %.1f hours — probing "
            "fresh. If the cap is still exhausted, the next failed call "
            "will re-trip the breaker.",
            elapsed2 / 3600,
        )
        _is_open = False
        _tripped_at = None
        _trip_reason = ""
        return False


def tripped_at() -> Optional[datetime]:
    """When the breaker tripped (UTC), or None if it hasn't."""
    return _tripped_at


def trip_reason() -> str:
    """The error string that tripped the breaker, or empty string if not tripped."""
    return _trip_reason


# Error-text fragments that indicate a HARD billing cap rather than
# transient rate-limiting. Conservative — only match strings that
# Google's API uses for the spend-cap case. Generic "RESOURCE_EXHAUSTED"
# / "429" / "quota" stay treated as transient by existing retry logic.
_CAP_SIGNATURES = (
    "monthly spending cap",
    "exceeded its monthly spend",
    "monthly spend cap",
    "spend cap",
    "billing-cap",
    "billing cap",
)


def should_trip(error_text: str) -> bool:
    """True when the error message contains the hard-cap signature."""
    if not error_text:
        return False
    lowered = error_text.lower()
    return any(sig in lowered for sig in _CAP_SIGNATURES)


def maybe_trip(error_text: str) -> bool:
    """Trip the breaker if `error_text` matches the cap signature.

    Returns True when the breaker transitioned from closed → open on
    this call (so the caller can log the transition), False otherwise.
    """
    global _is_open, _tripped_at, _trip_reason
    if not should_trip(error_text):
        return False
    with _lock:
        if _is_open:
            return False
        _is_open = True
        _tripped_at = datetime.utcnow()
        _trip_reason = (error_text or "")[:200]
        logger.error(
            "Gemini circuit breaker TRIPPED — monthly spending cap exhausted. "
            "All subsequent Gemini calls in this process will be skipped until "
            "restart. Raise the cap at https://ai.studio/spend, then redeploy "
            "or restart the service. Reason: %s",
            _trip_reason,
        )
        return True


def raise_if_open() -> None:
    """Convenience wrapper for callers that prefer raising over branching.

    Use at the start of a Gemini call site:
        gemini_circuit_breaker.raise_if_open()
        resp = client.models.generate_content(...)
    """
    if _is_open:
        raise GeminiCircuitOpen()


def reset() -> None:
    """Manually reset the breaker. Primarily for tests; Render's daily
    restarts reset the process state naturally so production rarely
    needs this."""
    global _is_open, _tripped_at, _trip_reason
    with _lock:
        _is_open = False
        _tripped_at = None
        _trip_reason = ""
