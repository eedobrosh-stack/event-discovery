"""dict (from parse.py) → RawEvent.

Rules (from docs/recipe_extraction_design.md, "Field normalisation"):
  • dates: ISO / epoch / strptime(date_format) / dateparser(language, tz).
    Unparseable → event dropped, reason counted. Never guessed.
  • start_datetime pseudo-field splits into start_date + start_time.
  • source_id required; derived from sha1(name|date|venue) when missing.
  • relative URLs already absolutised by parse when the spec says so;
    here we only sanity-check scheme.
  • defaults from recipe.defaults fill missing fields.
  • JSON-LD rows reuse the existing _jsonld.jsonld_to_raw_event.
"""
from __future__ import annotations

import hashlib
import logging
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Optional

from app.services.collectors.base import RawEvent

logger = logging.getLogger(__name__)

_TIME_RE = re.compile(r"\b([01]?\d|2[0-3])[:.]([0-5]\d)\b")
_EPOCH_RE = re.compile(r"^\d{9,13}$")
_PRICE_RE = re.compile(r"(\d+(?:[.,]\d{1,2})?)")


@dataclass
class NormalizeResult:
    events: list = field(default_factory=list)
    dropped: Counter = field(default_factory=Counter)
    samples: dict = field(default_factory=dict)   # reason → first offending row


def _parse_dt(value: Any, *, fmt: Optional[str], language: Optional[str],
              tz: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        v = float(value)
        if v > 1e12:
            v /= 1000.0
        try:
            return datetime.fromtimestamp(v, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    s = str(value).strip()
    if not s:
        return None
    if _EPOCH_RE.match(s):
        return _parse_dt(int(s), fmt=None, language=None, tz=tz)
    if fmt:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    # ISO 8601 (most APIs, JSON-LD, <time datetime>)
    try:
        from dateutil import parser as du
        return du.isoparse(s)
    except (ValueError, OverflowError):
        pass
    # free text, possibly non-English
    try:
        import dateparser
        settings = {"PREFER_DATES_FROM": "future",
                    "RETURN_AS_TIMEZONE_AWARE": False}
        if tz:
            settings["TIMEZONE"] = tz
        langs = [language] if language else None
        return dateparser.parse(s, languages=langs, settings=settings)
    except Exception:
        return None


def _split_time(s: Any) -> Optional[str]:
    if s is None:
        return None
    m = _TIME_RE.search(str(s))
    return f"{int(m.group(1)):02d}:{m.group(2)}" if m else None


def _to_float(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        return float(v)
    m = _PRICE_RE.search(str(v).replace("₪", "").replace(",", "."))
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def _to_list(v: Any) -> list[str]:
    if v is None:
        return []
    if isinstance(v, str):
        return [x.strip() for x in re.split(r"[,/|]", v) if x.strip()]
    if isinstance(v, list):
        return [str(x).strip() for x in v if x not in (None, "")]
    return [str(v)]


def _str(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, list):
        v = " ".join(str(x) for x in v if x is not None)
    s = str(v).strip()
    return s or None


def normalize(rows: list[dict], recipe: dict) -> NormalizeResult:
    res = NormalizeResult()
    parse_cfg = recipe.get("parse") or {}
    fmt = parse_cfg.get("date_format")
    lang = recipe.get("language")
    tz = recipe.get("timezone")
    source = recipe["source_name"]
    defaults = dict(recipe.get("defaults") or {})
    today = date.today()

    for row in rows:
        page_url = row.get("_page_url") or ""

        # JSON-LD rows: delegate to the proven converter.
        if "_jsonld" in row:
            from app.services.collectors._jsonld import jsonld_to_raw_event
            try:
                ev = jsonld_to_raw_event(row["_jsonld"], source_name=source,
                                         source_url=page_url)
            except Exception as e:  # defensive: one bad block ≠ dead page
                res.dropped["jsonld_error"] += 1
                res.samples.setdefault("jsonld_error", str(e)[:200])
                continue
            if ev is None:
                res.dropped["jsonld_rejected"] += 1
                continue
            for k, v in defaults.items():
                if getattr(ev, k, None) in (None, "", [], "USD") and v is not None:
                    setattr(ev, k, v)
            res.events.append(ev)
            continue

        merged = dict(defaults)
        merged.update({k: v for k, v in row.items() if not k.startswith("_")})

        name = _str(merged.get("name"))
        if not name:
            res.dropped["no_name"] += 1
            res.samples.setdefault("no_name", row)
            continue

        # ── dates ────────────────────────────────────────────────────
        start_dt = None
        start_time = None
        if merged.get("start_datetime") is not None:
            start_dt = _parse_dt(merged["start_datetime"], fmt=fmt, language=lang, tz=tz)
            if start_dt and (start_dt.hour or start_dt.minute):
                start_time = start_dt.strftime("%H:%M")
        if start_dt is None and merged.get("start_date") is not None:
            start_dt = _parse_dt(merged["start_date"], fmt=fmt, language=lang, tz=tz)
            if start_dt and (start_dt.hour or start_dt.minute) and not merged.get("start_time"):
                start_time = start_dt.strftime("%H:%M")
        ongoing = False
        if start_dt is None and parse_cfg.get("ongoing_if_end_only"):
            # exhibitions / runs that only publish an end date ("To
            # 13/11/2026"): treat as ongoing from today, if still open
            end_raw0 = merged.get("end_datetime", merged.get("end_date"))
            end_dt0 = _parse_dt(end_raw0, fmt=fmt, language=lang, tz=tz) if end_raw0 is not None else None
            if end_dt0 is not None and end_dt0.date() >= today:
                start_dt = datetime.combine(today, datetime.min.time())
                ongoing = True
                res.dropped["ongoing_from_end_date"] += 1   # informational counter
        if start_dt is None:
            res.dropped["no_date"] += 1
            res.samples.setdefault("no_date", row)
            continue
        start_date = start_dt.date()
        if ongoing:
            start_time = None
        if start_date < today:
            res.dropped["past"] += 1
            continue
        if merged.get("start_time") and not ongoing:
            start_time = _split_time(merged["start_time"]) or start_time

        end_date = None
        end_time = None
        end_raw = merged.get("end_datetime", merged.get("end_date"))
        if end_raw is not None:
            end_dt = _parse_dt(end_raw, fmt=fmt, language=lang, tz=tz)
            if end_dt:
                end_date = end_dt.date()
                if end_dt.hour or end_dt.minute:
                    end_time = end_dt.strftime("%H:%M")
        if merged.get("end_time"):
            end_time = _split_time(merged["end_time"]) or end_time

        # ── ids / urls ───────────────────────────────────────────────
        venue_name = _str(merged.get("venue_name"))
        source_id = _str(merged.get("source_id"))
        if not source_id:
            key = f"{name}|{start_date.isoformat()}|{venue_name or ''}"
            source_id = hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]
        purchase_link = _str(merged.get("purchase_link"))
        if purchase_link and not purchase_link.startswith(("http://", "https://")):
            purchase_link = None
        image_url = _str(merged.get("image_url"))
        if image_url and not image_url.startswith(("http://", "https://")):
            image_url = None

        try:
            ev = RawEvent(
                name=name,
                start_date=start_date,
                start_time=start_time,
                end_date=end_date,
                end_time=end_time,
                artist_name=_str(merged.get("artist_name")),
                description=_str(merged.get("description")),
                price=_to_float(merged.get("price")),
                price_currency=_str(merged.get("price_currency")) or "USD",
                purchase_link=purchase_link or (page_url or None),
                image_url=image_url,
                is_online=bool(merged.get("is_online", False)),
                venue_name=venue_name,
                venue_address=_str(merged.get("venue_address")),
                venue_city=_str(merged.get("venue_city")),
                venue_country=_str(merged.get("venue_country")) or recipe.get("country"),
                venue_lat=_to_float(merged.get("venue_lat")),
                venue_lon=_to_float(merged.get("venue_lon")),
                venue_website_url=_str(merged.get("venue_website_url")),
                source=source,
                source_id=source_id,
                raw_categories=_to_list(merged.get("raw_categories")),
                home_team=_str(merged.get("home_team")),
                away_team=_str(merged.get("away_team")),
                sport=_str(merged.get("sport")),
                tournament=_str(merged.get("tournament")),
                venue_timezone=tz,
            )
        except Exception as e:
            res.dropped["build_error"] += 1
            res.samples.setdefault("build_error", f"{e}: {row}"[:300])
            continue
        res.events.append(ev)

    return res
