"""Enrich Event.price by fetching the event's purchase_link page and
extracting the price via JSON-LD (primary) or Gemini (fallback).

Symptom (observed 2026-05-10): the catalog has very thin price coverage.
Top-20 most-popular performers had prices_n=0 across 1k+ events apiece.

Pipeline (v2, accuracy-first):
  1. Pick targets — upcoming events with purchase_link set, no price.
     Order by start_date ASC (sooner-first).
  2. Skip URLs on known-403 hosts (Ticketmaster, RA, AXS, etc.) before
     spending a fetch — see BLOCKED_HOST_SUBSTRINGS.
  3. Fetch the page, run iter_events() to extract any JSON-LD events.
  4. Match the right JSON-LD event — same start_date AND artist/name
     overlap. If exactly one match has a usable price → write.
  5. Fallback (Gemini): when JSON-LD didn't yield a usable price AND
     the page HTML actually loaded, ask Gemini to extract the ticket
     price for the target event. Strict acceptance gate — only
     confidence="high" responses survive, plus sanity bounds and
     currency-format validation. ``--no-gemini`` opts out.
  6. Per first-run measurement: JSON-LD alone hit ~12% of reachable
     pages (3/25 in a 100-target sample). Gemini fallback closes the
     bigger gap — most pages display the price visibly without
     publishing schema.org markup.

Cap per run via ``--limit``. Default ``--limit 500`` keeps the wall
time and bandwidth bounded for nightly cron use.

Usage:
    PYTHONPATH=. python3 scripts/enrich_event_prices.py
    PYTHONPATH=. python3 scripts/enrich_event_prices.py --apply
    PYTHONPATH=. python3 scripts/enrich_event_prices.py --apply --limit 200
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(str(ROOT / ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("enrich_event_prices")

from sqlalchemy import or_  # noqa: E402

from app.database import SessionLocal  # noqa: E402
from app.models import Event  # noqa: E402
from app.extractors.llm_extractor import _fetch_html  # noqa: E402
from app.services.collectors._jsonld import iter_events  # noqa: E402
from scripts.improve_genre_coverage import _gemini_call  # noqa: E402

# Sanity bounds. A "price" outside this range almost certainly came from
# parsing a wrong number on the page (a phone number, a year, a runtime).
MIN_PRICE = 1.0
MAX_PRICE = 5000.0

# Polite pacing between HTTP fetches — we're walking event pages on
# venue/ticket sites that didn't ask to be re-fetched.
FETCH_DELAY_SEC = 0.6

# Domains whose anti-bot stack consistently blocks both curl_cffi and
# urllib (observed 2026-05-10: 100% 403 rate). Short-circuit before
# even attempting the fetch — saves wall time + bandwidth and stops
# us from sending repeated declined requests. Substring match against
# the URL host (lowercase, leading "www." stripped).
BLOCKED_HOST_SUBSTRINGS = (
    "ticketmaster.",   # .com, .de, .com.au, .ca, .co.uk, etc.
    "ra.co",            # Resident Advisor
    "axs.com",          # AXS
    "ticketweb.com",
    "stubhub.com",
    "vividseats.com",
    "seetickets.",
    "songkick.com",     # tracker, not a price source
)


def _host_is_blocked(url: str) -> bool:
    from urllib.parse import urlparse
    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        return False
    if host.startswith("www."):
        host = host[4:]
    return any(sub in host for sub in BLOCKED_HOST_SUBSTRINGS)


def _norm_text(s: str | None) -> str:
    return (s or "").strip().lower()


def _jsonld_start_date(ev: dict) -> str:
    """ISO YYYY-MM-DD date from a JSON-LD Event's startDate, or empty
    string if absent / unparseable. Just the prefix — we compare on
    date, not datetime."""
    return (ev.get("startDate") or "")[:10]


def _jsonld_artist_name(ev: dict) -> str:
    """Best-effort artist name from a JSON-LD Event's `performer`
    field. Performer can be a dict, a list of dicts, or a string."""
    p = ev.get("performer")
    if isinstance(p, list):
        p = p[0] if p else None
    if isinstance(p, dict):
        return (p.get("name") or "").strip()
    if isinstance(p, str):
        return p.strip()
    return ""


def _jsonld_offer_price(ev: dict) -> tuple[float | None, str | None]:
    """Extract (price, currency) from a JSON-LD Event's offers field.
    Mirrors the parsing in app.services.collectors._jsonld so behaviour
    here is consistent with the in-line collectors. lowPrice is
    preferred over price (price-range case)."""
    offers = ev.get("offers") or {}
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    if not isinstance(offers, dict):
        return None, None
    raw = offers.get("lowPrice") if offers.get("lowPrice") is not None else offers.get("price")
    if raw is None:
        return None, None
    try:
        price = float(str(raw).replace(",", "").replace("$", "").strip())
    except (TypeError, ValueError):
        return None, None
    currency = offers.get("priceCurrency")
    if isinstance(currency, str):
        currency = currency.strip().upper() or None
    else:
        currency = None
    return price, currency


def _matches_target(jsonld_ev: dict, target_event: Event) -> bool:
    """True when a JSON-LD event from the fetched page corresponds to
    the target Event we're trying to enrich.

    Match rules (must satisfy ALL):
      • same start_date (ISO prefix) — strict, no fuzzy date matching.
      • name OR artist overlap: at least one of the target's name /
        artist_name (lowercased) is a substring of the JSON-LD event's
        name OR performer, or vice-versa. Substring rather than
        equality because event-detail pages often append "Live in
        <City>" or " - <YYYY>" and we shouldn't reject those.
    """
    target_iso = target_event.start_date.isoformat() if target_event.start_date else ""
    if not target_iso or _jsonld_start_date(jsonld_ev) != target_iso:
        return False
    target_strs = [s for s in [
        _norm_text(target_event.name),
        _norm_text(target_event.artist_name),
    ] if s]
    cand_strs = [s for s in [
        _norm_text(jsonld_ev.get("name")),
        _norm_text(_jsonld_artist_name(jsonld_ev)),
    ] if s]
    if not target_strs or not cand_strs:
        return False
    for t in target_strs:
        for c in cand_strs:
            if t in c or c in t:
                return True
    return False


def _is_valid_price(price: float | None) -> bool:
    if price is None:
        return False
    try:
        p = float(price)
    except (TypeError, ValueError):
        return False
    return MIN_PRICE <= p <= MAX_PRICE


# ── Gemini fallback ────────────────────────────────────────────────────
# When JSON-LD doesn't yield a usable price, ask Gemini to extract from
# the page HTML. The bar is tight: confidence="high" required, a
# specific JSON shape, and the result is sanity-checked just like the
# JSON-LD path before we write.

# Cap on HTML chars sent to Gemini. Most event pages fit in ~30k after
# stripping noise. Going larger costs tokens with no recall gain — the
# price is almost always near the top / in obvious "Tickets" sections.
_LLM_HTML_CAP = 30_000

# Common ISO-4217-shaped strings we accept. Anything not matching
# /^[A-Z]{3}$/ is rejected as a "Gemini hallucinated currency" guard.
_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")

_PRICE_PROMPT = """\
You are extracting the ticket price for ONE specific event from a web page.

Target event:
  Name:   {name}
  Date:   {date}
  Artist: {artist}

Below is the page's HTML (with <script>/<style>/<svg> stripped, possibly
truncated). The page may list multiple events, multiple ticket tiers,
or include surrounding "related events" / merchandise — focus only on
the target.

Return STRICTLY this JSON shape — no prose, no markdown fences:

{{
  "price":       <number or null>,
  "currency":    <"USD" | "EUR" | "GBP" | "ILS" | "CAD" | "AUD" | "JPY" | "INR" | "BRL" | "MXN" | "ARS" | "ZAR" | "CHF" | other ISO-4217 3-letter code | null>,
  "confidence":  <"high" | "medium" | "low">,
  "is_free":     <true|false>,
  "is_sold_out": <true|false>,
  "is_range":    <true|false>
}}

Rules:
  R1. "high" confidence ONLY when the price is clearly stated FOR THIS
      EXACT event (date + artist/name match) on the page.
  R2. If the page shows tiered pricing ("Standard 30, VIP 80"), return
      the LOWEST tier as `price` and set is_range=true.
  R3. If sold out, set is_sold_out=true, price=null, confidence=high.
  R4. If free admission, set is_free=true, price=0, confidence=high.
  R5. If you can't find a clear price for the target event (page lists
      a different event, or doesn't show pricing), return price=null
      with confidence=low. Don't guess.

Page HTML:
{html}
"""


def _strip_html_for_llm(html: str) -> str:
    """Trim noise tags and truncate so Gemini sees relevant content
    only. Cheap regex strip — no BeautifulSoup dependency required."""
    s = html
    s = re.sub(r"<script[^>]*>.*?</script>", " ", s, flags=re.S | re.I)
    s = re.sub(r"<style[^>]*>.*?</style>",   " ", s, flags=re.S | re.I)
    s = re.sub(r"<svg[^>]*>.*?</svg>",       " ", s, flags=re.S | re.I)
    s = re.sub(r"<noscript[^>]*>.*?</noscript>", " ", s, flags=re.S | re.I)
    s = re.sub(r"<!--.*?-->", " ", s, flags=re.S)
    # Collapse runs of whitespace to keep token count low.
    s = re.sub(r"\s+", " ", s).strip()
    return s[:_LLM_HTML_CAP]


def _extract_price_via_gemini(html: str, target: Event) -> dict | None:
    """Returns the parsed Gemini JSON dict on success, None on
    transient failure / parse error / API not configured. The
    DECISION whether to accept the result lives at the call site —
    this function just runs the extraction."""
    prompt = _PRICE_PROMPT.format(
        name=(target.name or "(unknown)").strip()[:200],
        date=target.start_date.isoformat() if target.start_date else "(unknown)",
        artist=(target.artist_name or "(unknown)").strip()[:200],
        html=_strip_html_for_llm(html),
    )
    return _gemini_call(prompt)


def _accept_gemini_price(data: dict) -> tuple[float | None, str | None, str]:
    """Validate Gemini's extraction against the accuracy gate.
    Returns (price, currency, reason).
      price+currency set: accept
      price=None: reject — `reason` says why
    """
    if not isinstance(data, dict):
        return None, None, "not_dict"
    confidence = (data.get("confidence") or "").lower()
    if confidence != "high":
        return None, None, f"confidence={confidence!r}"
    if data.get("is_sold_out"):
        return None, None, "sold_out"
    if data.get("is_free"):
        # We deliberately don't write price=0 in v1 — keeps "missing"
        # vs "free" semantically distinct. Skip.
        return None, None, "free"
    raw_price = data.get("price")
    if raw_price is None:
        return None, None, "price_null"
    try:
        price = float(raw_price)
    except (TypeError, ValueError):
        return None, None, "price_not_numeric"
    if not _is_valid_price(price):
        return None, None, f"price_out_of_bounds({price})"
    currency = (data.get("currency") or "").strip().upper()
    if not _CURRENCY_RE.match(currency):
        return None, None, f"bad_currency({currency!r})"
    return price, currency, "ok"


def _fetch_targets(db, limit: int):
    """Upcoming events with purchase_link set + price still NULL."""
    today = date.today()
    return (
        db.query(Event)
        .filter(
            Event.start_date >= today,
            Event.purchase_link.isnot(None),
            Event.purchase_link != "",
            or_(Event.price.is_(None), Event.price == 0),
        )
        .order_by(Event.start_date.asc(), Event.id.asc())
        .limit(limit)
        .all()
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--apply", action="store_true",
                        help="Write changes. Without this, runs as a dry-run.")
    parser.add_argument("--limit", type=int, default=500,
                        help="Cap on events processed (default 500). Each "
                             "event is one HTTP fetch — keep this bounded "
                             "for wall-time / politeness.")
    parser.add_argument("--no-gemini", action="store_true",
                        help="Disable the Gemini fallback. Only the "
                             "JSON-LD primary path runs. Useful for cost-"
                             "free re-runs after a fix.")
    args = parser.parse_args()

    mode = "APPLY" if args.apply else "DRY-RUN"
    gemini_on = not args.no_gemini
    log.info(f"mode={mode} limit={args.limit} gemini={'on' if gemini_on else 'off'}")

    db = SessionLocal()
    stats = {
        "targeted":              0,
        "skipped_blocked_host":  0,
        "fetch_errors":          0,
        "no_jsonld_events":      0,
        "no_match":              0,
        "ambiguous_match":       0,
        "match_no_price":        0,
        "match_invalid_price":   0,
        "wrote_price":           0,
        # Gemini fallback path
        "gemini_attempts":       0,
        "gemini_no_response":    0,
        "gemini_rejected":       0,
        "gemini_wrote_price":    0,
    }
    # Track Gemini rejection reasons to spot prompt drift.
    gemini_rejection_reasons: dict[str, int] = {}
    samples = []

    try:
        targets = _fetch_targets(db, args.limit)
        log.info(f"target events (upcoming, purchase_link set, price null): "
                 f"{len(targets):,}")

        for i, ev in enumerate(targets, start=1):
            stats["targeted"] += 1

            # Pre-fetch domain check — short-circuit known-403 hosts
            # so we don't waste a 1-2s fetch budget per event for no
            # gain. See BLOCKED_HOST_SUBSTRINGS for the list.
            if _host_is_blocked(ev.purchase_link):
                stats["skipped_blocked_host"] += 1
                continue

            html = _fetch_html(ev.purchase_link)
            if not html:
                stats["fetch_errors"] += 1
                continue

            # JSON-LD primary path. Outcomes:
            #   - JSON-LD price found, valid → final_price/currency set, source="jsonld"
            #   - JSON-LD path failed at any step → fall through to Gemini fallback
            final_price: float | None = None
            final_currency: str | None = None
            source: str | None = None

            jsonld_events = list(iter_events(html, future_only=False))
            if not jsonld_events:
                stats["no_jsonld_events"] += 1
            else:
                matches = [j for j in jsonld_events if _matches_target(j, ev)]
                if len(matches) == 0:
                    stats["no_match"] += 1
                elif len(matches) > 1:
                    stats["ambiguous_match"] += 1
                else:
                    p, c = _jsonld_offer_price(matches[0])
                    if p is None:
                        stats["match_no_price"] += 1
                    elif not _is_valid_price(p):
                        stats["match_invalid_price"] += 1
                        log.debug(f"rejected jsonld price={p!r} for ev_id={ev.id} {ev.name!r}")
                    else:
                        final_price = float(p)
                        final_currency = c or ev.price_currency or "USD"
                        source = "jsonld"

            # Gemini fallback when JSON-LD didn't yield a price. Skip
            # the fallback when THIS event tripped the ambiguous_match
            # bucket — multiple JSON-LD events on the page matched
            # the target, and asking Gemini to disambiguate from
            # prose alone is an accuracy risk we don't need to take.
            if final_price is None and gemini_on:
                ambiguous_for_this_event = (
                    bool(jsonld_events)
                    and len([j for j in jsonld_events if _matches_target(j, ev)]) > 1
                )
                if not ambiguous_for_this_event:
                    stats["gemini_attempts"] += 1
                    data = _extract_price_via_gemini(html, ev)
                    if data is None:
                        stats["gemini_no_response"] += 1
                    else:
                        gp, gc, reason = _accept_gemini_price(data)
                        if gp is None:
                            stats["gemini_rejected"] += 1
                            gemini_rejection_reasons[reason] = (
                                gemini_rejection_reasons.get(reason, 0) + 1
                            )
                        else:
                            final_price = gp
                            final_currency = gc or ev.price_currency or "USD"
                            source = "gemini"
                            stats["gemini_wrote_price"] += 1

            if final_price is None:
                # Either JSON-LD failed AND Gemini was off / declined,
                # or the event went through ambiguous_match. Move on.
                if i % 25 == 0:
                    log.info(
                        f"  progress {i}/{len(targets)}  "
                        f"wrote={stats['wrote_price']}  "
                        f"gemini_wrote={stats['gemini_wrote_price']}  "
                        f"blocked={stats['skipped_blocked_host']}  "
                        f"fetch_err={stats['fetch_errors']}"
                    )
                time.sleep(FETCH_DELAY_SEC)
                continue

            stats["wrote_price"] += 1
            samples.append({
                "event_id":      ev.id,
                "name":          ev.name,
                "artist_name":   ev.artist_name,
                "start_date":    str(ev.start_date),
                "old_price":     ev.price,
                "new_price":     final_price,
                "currency":      final_currency,
                "source":        source,
                "purchase_link": ev.purchase_link,
            })

            if args.apply:
                ev.price = final_price
                ev.price_currency = final_currency
                # Commit per-event so a fetch failure halfway through
                # doesn't roll back hours of progress.
                db.commit()

            if i % 25 == 0:
                log.info(
                    f"  progress {i}/{len(targets)}  "
                    f"wrote={stats['wrote_price']}  "
                    f"(jsonld={stats['wrote_price']-stats['gemini_wrote_price']} "
                    f"gemini={stats['gemini_wrote_price']})  "
                    f"blocked={stats['skipped_blocked_host']}  "
                    f"fetch_err={stats['fetch_errors']}"
                )

            time.sleep(FETCH_DELAY_SEC)

        jsonld_writes = stats['wrote_price'] - stats['gemini_wrote_price']
        log.info(
            "done. "
            f"targeted={stats['targeted']}  "
            f"wrote_price={stats['wrote_price']} (jsonld={jsonld_writes} gemini={stats['gemini_wrote_price']})  "
            f"skipped_blocked_host={stats['skipped_blocked_host']}  "
            f"fetch_errors={stats['fetch_errors']}  "
            f"no_jsonld={stats['no_jsonld_events']}  "
            f"no_match={stats['no_match']}  "
            f"ambiguous={stats['ambiguous_match']}  "
            f"match_no_price={stats['match_no_price']}  "
            f"match_invalid_price={stats['match_invalid_price']}  "
            f"gemini_attempts={stats['gemini_attempts']} "
            f"no_response={stats['gemini_no_response']} "
            f"rejected={stats['gemini_rejected']}"
        )
        if gemini_rejection_reasons:
            log.info(f"gemini rejection reasons: {gemini_rejection_reasons}")

        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        suffix = "apply" if args.apply else "dryrun"
        audit_path = ROOT / "data" / f"enrich_event_prices_{ts}_{suffix}.json"
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        audit_path.write_text(json.dumps({
            "stats": stats,
            "gemini_rejection_reasons": gemini_rejection_reasons,
            "samples": samples[:50],   # cap audit size
            "samples_total": len(samples),
        }, ensure_ascii=False, indent=2))
        log.info(f"audit written: {audit_path}")
        if not args.apply:
            log.info("DRY-RUN — re-run with --apply to write.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
