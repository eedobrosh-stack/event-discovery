"""Response → list[dict] for each parse kind, plus pagination helpers.

Output dicts use RawEvent field names (see schema.RAW_EVENT_FIELDS) with
*unnormalised* values — strings straight from the page/API. normalize.py
turns them into RawEvents. Keeping parse and normalise separate means
`--dry-run` can show exactly what the selectors matched before any date
parsing muddies the picture.
"""
from __future__ import annotations

import json
import re
from typing import Any, Iterator, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup


def soup(html: str) -> BeautifulSoup:
    """lxml is fast but chokes on some real-world markup (e.g. a bare
    ':' attribute name — seen on visitstockholm.com). Fall back to the
    stdlib parser rather than failing the whole page."""
    try:
        return BeautifulSoup(html, "lxml")
    except Exception:
        return BeautifulSoup(html, "html.parser")


# ── generic helpers ──────────────────────────────────────────────────────
_PATH_TOKEN = re.compile(r"([^.\[\]]+)|\[(\*|-?\d+)\]")


def get_path(obj: Any, path: str) -> Any:
    """Tiny JSONPath: 'data.events', 'venue.location.lat',
    'performers[0].name', 'tags[*].name'. '' or None → obj itself.
    Missing → None. '[*]' fans out into a list."""
    if path in (None, ""):
        return obj
    cur: Any = obj
    for m in _PATH_TOKEN.finditer(path):
        key, idx = m.group(1), m.group(2)
        if cur is None:
            return None
        if key is not None:
            if isinstance(cur, list):
                cur = [c.get(key) if isinstance(c, dict) else None for c in cur]
            elif isinstance(cur, dict):
                cur = cur.get(key)
            else:
                return None
        else:
            if not isinstance(cur, list):
                return None
            if idx == "*":
                # flatten one level so the next key applies element-wise
                cur = [x for x in cur]
            else:
                i = int(idx)
                cur = cur[i] if -len(cur) <= i < len(cur) else None
    return cur


def _apply_regex(val: Any, pattern: Optional[str]) -> Any:
    if val is None or not pattern:
        return val
    m = re.search(pattern, str(val))
    if not m:
        return None
    return m.group(1) if m.groups() else m.group(0)


def _strip_html(s: Any) -> Any:
    if isinstance(s, str) and "<" in s:
        return soup(s).get_text(" ", strip=True)
    return s


# ── api ──────────────────────────────────────────────────────────────────
def parse_api(body: Any, cfg: dict, page_url: str) -> list[dict]:
    """body is the decoded JSON (dict or list). cfg = recipe.parse."""
    items = get_path(body, cfg.get("items", ""))
    # A few WordPress load-more endpoints return JSON metadata plus an
    # HTML fragment (usually under ``htmldata``).  Let an API recipe hand
    # that fragment to the normal HTML parser instead of requiring a
    # bespoke collector for an otherwise deterministic response.
    if isinstance(items, str) and cfg.get("html_item"):
        return parse_html(items, {
            "item": cfg["html_item"],
            "fields": cfg.get("fields") or {},
        }, page_url)
    if isinstance(items, dict):
        # some APIs return {"123": {...}, "124": {...}}
        items = list(items.values())
    if not isinstance(items, list):
        return []
    out: list[dict] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        row: dict = {}
        for field, spec in (cfg.get("fields") or {}).items():
            if isinstance(spec, str):
                val = get_path(it, spec)
            else:
                if "const" in spec:
                    val = spec["const"]
                else:
                    val = get_path(it, spec.get("path") or spec.get("sel") or "")
                    val = _apply_regex(val, spec.get("regex"))
                    if spec.get("strip_html"):
                        val = _strip_html(val)
                    if isinstance(val, list) and spec.get("join"):
                        val = spec["join"].join(str(v) for v in val if v is not None)
                    if val in (None, "", []) and "default" in spec:
                        val = spec["default"]
                    if spec.get("absolute") and isinstance(val, str):
                        val = urljoin(page_url, val)
                    # "template": "https://www.kupat.co.il/{}" — build a URL
                    # from a bare slug (urljoin against the API path would
                    # land under /api/). Skipped when the value is empty.
                    if spec.get("template") and val not in (None, "", []):
                        val = spec["template"].format(val)
            if val is not None:
                row[field] = val
        row["_page_url"] = page_url
        out.append(row)
    return out


# ── html ─────────────────────────────────────────────────────────────────
def _select_value(node, spec, page_url: str):
    """Resolve one field spec against a BeautifulSoup node."""
    if isinstance(spec, str):
        el = node.select_one(spec)
        return el.get_text(" ", strip=True) if el else None
    if "const" in spec:
        return spec["const"]
    sel = spec.get("sel")
    if spec.get("all"):
        els = node.select(sel) if sel else [node]
        vals = []
        for el in els:
            v = el.get(spec["attr"]) if spec.get("attr") else el.get_text(" ", strip=True)
            if isinstance(v, list):
                v = " ".join(v)
            v = _apply_regex(v, spec.get("regex"))
            if v not in (None, ""):
                vals.append(v)
        if spec.get("join"):
            return spec["join"].join(vals) if vals else spec.get("default")
        return vals or spec.get("default")
    if sel:
        idx = spec.get("index", 0)
        els = node.select(sel)
        el = els[idx] if -len(els) <= idx < len(els) else None
    else:
        el = node
    if el is None:
        return spec.get("default")
    if spec.get("attr"):
        v = el.get(spec["attr"])
        if isinstance(v, list):
            v = " ".join(v)
    else:
        v = el.get_text(" ", strip=True)
    v = _apply_regex(v, spec.get("regex"))
    if v in (None, "") and "default" in spec:
        v = spec["default"]
    if spec.get("absolute") and isinstance(v, str):
        v = urljoin(page_url, v)
    return v


def parse_html(html: str, cfg: dict, page_url: str) -> list[dict]:
    doc = soup(html)
    item_sel = cfg.get("item")
    nodes = doc.select(item_sel) if item_sel else [doc]
    # page_fields: evaluated once against the whole document and merged
    # into every item row (item-level values win). This is how a show
    # page's title / image / blurb reach each of its performance rows.
    page_vals: dict = {}
    for field, spec in (cfg.get("page_fields") or {}).items():
        val = _select_value(doc, spec, page_url)
        if val is not None:
            page_vals[field] = val
    out: list[dict] = []
    for node in nodes:
        row: dict = dict(page_vals)
        for field, spec in (cfg.get("fields") or {}).items():
            val = _select_value(node, spec, page_url)
            if val is not None:
                row[field] = val
        row["_page_url"] = page_url
        out.append(row)
    return out


def extract_links(html: str, follow: dict, page_url: str, domain: str) -> list[str]:
    """entry.follow: collect the detail-page URLs a listing points at."""
    doc = soup(html)
    attr = follow.get("attr", "href")
    pat = follow.get("regex")
    same = follow.get("same_domain", True)
    out: list[str] = []
    for el in doc.select(follow["selector"]):
        v = el.get(attr)
        if isinstance(v, list):
            v = " ".join(v)
        if not v or str(v).startswith(("#", "javascript")):
            continue
        if pat:
            m = re.search(pat, str(v))
            if not m:
                continue
            v = m.group(1) if m.groups() else m.group(0)
        u = urljoin(page_url, str(v))
        if same:
            from app.services.recipes.schema import registered_domain
            if registered_domain(u) != domain:
                continue
        if u not in out:
            out.append(u)
    return out


# ── jsonld ───────────────────────────────────────────────────────────────
def parse_jsonld(html: str, page_url: str) -> list[dict]:
    """Return schema.org Event dicts (raw JSON-LD). Normalisation reuses
    the existing `_jsonld.jsonld_to_raw_event` converter, so this only
    collects them. Tagged with '_jsonld': True."""
    from app.services.collectors._jsonld import iter_events
    out = []
    for ev in iter_events(html, future_only=True):
        out.append({"_jsonld": ev, "_page_url": page_url})
    return out


# ── ics ──────────────────────────────────────────────────────────────────
def parse_ics(text: str, page_url: str) -> list[dict]:
    from icalendar import Calendar
    cal = Calendar.from_ical(text)
    out: list[dict] = []
    for comp in cal.walk("VEVENT"):
        def g(k):
            v = comp.get(k)
            return str(v) if v is not None else None
        dtstart = comp.get("dtstart")
        dtend = comp.get("dtend")
        row = {
            "source_id": g("uid"),
            "name": g("summary"),
            "start_datetime": dtstart.dt.isoformat() if dtstart is not None else None,
            "end_datetime": dtend.dt.isoformat() if dtend is not None else None,
            "venue_name": g("location"),
            "description": g("description"),
            "purchase_link": g("url"),
            "_page_url": page_url,
        }
        out.append({k: v for k, v in row.items() if v is not None})
    return out


# ── pagination ───────────────────────────────────────────────────────────
def next_link(html: str, selector: str, page_url: str) -> Optional[str]:
    doc = soup(html)
    el = doc.select_one(selector)
    if el is None:
        return None
    href = el.get("href") if el.name == "a" else (el.get("href") or el.get("data-href"))
    if not href:
        a = el.find("a", href=True)
        href = a["href"] if a else None
    if not href or href.startswith("#") or href.lower().startswith("javascript"):
        return None
    return urljoin(page_url, href)


def set_query_param(url: str, param: str, value) -> str:
    from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
    parts = urlsplit(url)
    q = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k != param]
    q.append((param, str(value)))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(q), parts.fragment))


def decode_json(text: str) -> Any:
    """Tolerant JSON decode: handles JSONP-ish wrappers and BOMs."""
    s = text.lstrip("﻿ \n\r\t")
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        m = re.match(r"^[\w$.]+\((.*)\);?\s*$", s, re.S)
        if m:
            return json.loads(m.group(1))
        raise
