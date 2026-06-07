#!/usr/bin/env python3
"""Gemini spend monitor — projects month-end NIS against the cap.

Reads the gemini_usage table (populated by record_gemini_usage at each
Gemini call site), applies per-model pricing, and projects whether this
month's spend will breach the cap. Pricing is applied HERE (not at record
time) so rate changes never need a data migration.

Usage (on the Render box):
    cd /opt/render/project/src && PYTHONPATH=. python3 scripts/gemini_spend_report.py

Note: the table only has data from when instrumentation was deployed
(2026-06-07), so "instrumented MTD" excludes earlier days in the month.
The authoritative full-month figure still lives in AI Studio; this report
gives the forward-looking daily burn + projection.
"""
import sys
from datetime import datetime, date
from collections import defaultdict

from app.database import SessionLocal
from app.models import GeminiUsage

# ── Config — VERIFY against current Google pricing + FX ──────────────────
# USD per 1M tokens. Defaults are gemini-2.5 tier list prices (early 2026);
# update if your plan/rates differ. Unknown models fall back to FLASH.
PRICING_USD_PER_1M = {
    "gemini-2.5-flash":      {"in": 0.30, "out": 2.50},
    "gemini-2.5-flash-lite": {"in": 0.10, "out": 0.40},
    "gemini-2.0-flash":      {"in": 0.10, "out": 0.40},
}
_FALLBACK = {"in": 0.30, "out": 2.50}
USD_TO_NIS = 3.70            # verify current FX
MONTHLY_CAP_NIS = 500.0


def _price(model: str):
    for key, p in PRICING_USD_PER_1M.items():
        if model.startswith(key):
            return p
    return _FALLBACK


def main():
    db = SessionLocal()
    today = date.today()
    month_prefix = today.strftime("%Y-%m")

    rows = (
        db.query(GeminiUsage)
        .filter(GeminiUsage.day.like(f"{month_prefix}-%"))
        .all()
    )
    if not rows:
        print(f"No gemini_usage rows for {month_prefix} yet "
              f"(instrumentation may have just deployed). Nothing to project.")
        return

    by_model = defaultdict(lambda: {"calls": 0, "in": 0, "out": 0, "nis": 0.0})
    days_seen = set()
    total_nis = 0.0
    for r in rows:
        days_seen.add(r.day)
        p = _price(r.model)
        nis = (r.prompt_tokens / 1e6 * p["in"] + r.output_tokens / 1e6 * p["out"]) * USD_TO_NIS
        m = by_model[r.model]
        m["calls"] += r.calls
        m["in"] += r.prompt_tokens
        m["out"] += r.output_tokens
        m["nis"] += nis
        total_nis += nis

    n_days = len(days_seen)
    daily_avg = total_nis / max(n_days, 1)
    days_in_month = 30  # close enough for a projection
    projected = daily_avg * days_in_month
    pct = projected / MONTHLY_CAP_NIS * 100

    print(f"=== Gemini spend — {month_prefix} (instrumented) ===")
    print(f"{'model':<26}{'calls':>8}{'in_tok':>12}{'out_tok':>12}{'NIS':>10}")
    for model, m in sorted(by_model.items(), key=lambda kv: -kv[1]["nis"]):
        print(f"{model:<26}{m['calls']:>8}{m['in']:>12,}{m['out']:>12,}{m['nis']:>10.2f}")
    print("-" * 68)
    print(f"instrumented MTD: {total_nis:.2f} NIS over {n_days} day(s) "
          f"→ {daily_avg:.2f} NIS/day")
    print(f"projected month  : {projected:.0f} NIS  ({pct:.0f}% of {MONTHLY_CAP_NIS:.0f} cap)")
    status = ("🔴 OVER cap — reduce extraction" if projected > MONTHLY_CAP_NIS
              else "🟡 within ~10% of cap — watch" if projected > 0.9 * MONTHLY_CAP_NIS
              else "🟢 under cap")
    print(f"status           : {status}")
    print("\n(Note: excludes pre-instrumentation days this month; cross-check "
          "AI Studio for the authoritative full-month figure.)")


if __name__ == "__main__":
    main()
