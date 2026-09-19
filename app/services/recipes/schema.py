"""Recipe document validation.

Deliberately hand-rolled (no pydantic/jsonschema dependency) — the schema
is small and the error messages need to be human-friendly because the
person reading them is fixing a JSON file in an editor.
"""
from __future__ import annotations

import re
from urllib.parse import urlsplit

RECIPE_VERSION = 1

PARSE_KINDS = ("jsonld", "api", "html", "ics")
PAGINATE_MODES = ("none", "next_link", "page_param", "cursor", "next_url")
FETCH_METHODS = ("GET", "POST")
BODY_FORMATS = ("json", "form")
# Recipes a Render-side fetch cannot run (Cloudflare geo/IP walls on
# Israeli sites, 2026-09-20 katedra.co.il): "relay": "mac" hands the
# fetch+parse to the Mac-side relay (~/.claude/scripts/supercaly-relay),
# which POSTs the normalised events to /api/admin/recipes/{domain}/relay.
# The scheduler skips relay recipes; persistence and health stay identical.
RELAYS = ("mac",)
RENDER_MODES = ("none", "browser")

# RawEvent fields a recipe may populate. Anything else is rejected so a
# typo ("venue" instead of "venue_name") surfaces at authoring time.
RAW_EVENT_FIELDS = frozenset({
    "source_id", "name", "start_date", "start_time", "end_date", "end_time",
    "artist_name", "description", "price", "price_currency", "purchase_link",
    "image_url", "is_online", "venue_name", "venue_address", "venue_city",
    "venue_country", "venue_lat", "venue_lon", "venue_website_url",
    "raw_categories", "home_team", "away_team", "sport", "tournament",
    # pseudo-field: a full datetime that the normaliser splits into
    # start_date + start_time (most APIs give one ISO string)
    "start_datetime", "end_datetime",
})

_SOURCE_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_\-]{1,58}$")


def registered_domain(url_or_host: str) -> str:
    """'https://www.ICM.org.il/x' → 'icm.org.il'. '' on failure."""
    s = (url_or_host or "").strip()
    host = urlsplit(s).hostname if "://" in s else s.split("/")[0]
    host = (host or "").lower()
    return host[4:] if host.startswith("www.") else host


def _field_spec_errors(name: str, spec, where: str) -> list[str]:
    errs: list[str] = []
    if name not in RAW_EVENT_FIELDS:
        errs.append(f"{where}: unknown RawEvent field '{name}'")
    if isinstance(spec, str):
        return errs
    if not isinstance(spec, dict):
        errs.append(f"{where}.{name}: must be a string or an object")
        return errs
    allowed = {"sel", "attr", "regex", "absolute", "all", "join", "default",
               "index", "path", "format", "strip_html", "const"}
    for k in spec:
        if k not in allowed:
            errs.append(f"{where}.{name}: unknown key '{k}'")
    if "regex" in spec:
        try:
            re.compile(spec["regex"])
        except re.error as e:
            errs.append(f"{where}.{name}.regex: {e}")
    return errs


def validate_recipe(doc: dict) -> list[str]:
    """Return a list of human-readable problems. Empty list == valid."""
    errs: list[str] = []
    if not isinstance(doc, dict):
        return ["recipe must be a JSON object"]

    if doc.get("recipe_version") != RECIPE_VERSION:
        errs.append(f"recipe_version must be {RECIPE_VERSION}")

    dom = doc.get("domain")
    if not dom or registered_domain(dom) != dom:
        errs.append("domain: required, lowercase, no scheme, no 'www.' "
                    f"(got {dom!r})")

    sn = doc.get("source_name")
    if not sn or not _SOURCE_NAME_RE.match(sn):
        errs.append("source_name: required, [a-z0-9_-], 2–59 chars")

    if not doc.get("country"):
        errs.append("country: required (City fallback needs it)")

    # entry
    entry = doc.get("entry")
    if not isinstance(entry, dict):
        errs.append("entry: required object")
        entry = {}
    urls = entry.get("urls") or []
    if not isinstance(urls, list) or not all(isinstance(u, str) for u in urls):
        errs.append("entry.urls: must be a list of strings")
        urls = []
    if not urls and not entry.get("template"):
        errs.append("entry: needs at least one of urls[] or template")
    for u in urls:
        if registered_domain(u) != dom and not entry.get("allow_foreign_domains"):
            errs.append(f"entry.urls: {u} is not on domain {dom} "
                        "(set entry.allow_foreign_domains=true if intended)")
    tpl = entry.get("template")
    if tpl:
        has_month = "{year}" in tpl and "{month" in tpl
        has_value = "{value}" in tpl
        if has_month and not entry.get("range_months"):
            errs.append("entry.template uses {year}/{month} → set range_months")
        if has_value and not entry.get("values"):
            errs.append("entry.template uses {value} → set values[]")
        if not has_month and not has_value:
            errs.append("entry.template must contain {year}/{month:02d} or {value}")
    pag = entry.get("paginate") or {"mode": "none"}
    if not isinstance(pag, dict):
        errs.append("entry.paginate: must be an object")
    else:
        mode = pag.get("mode", "none")
        if mode not in PAGINATE_MODES:
            errs.append(f"entry.paginate.mode: one of {PAGINATE_MODES}")
        if mode == "next_link" and not pag.get("selector"):
            errs.append("entry.paginate.selector required for next_link")
        if mode == "page_param" and not pag.get("param"):
            errs.append("entry.paginate.param required for page_param")
        if mode == "cursor" and not (pag.get("cursor_path") and pag.get("param")):
            errs.append("entry.paginate: cursor mode needs cursor_path + param")
        if mode == "next_url" and not pag.get("path"):
            errs.append("entry.paginate: next_url mode needs 'path' (JSON path to the next page URL)")
        mp = pag.get("max_pages", 20)
        if not isinstance(mp, int) or mp < 1 or mp > 200:
            errs.append("entry.paginate.max_pages: int 1..200")

    follow = entry.get("follow")
    if follow is not None:
        if not isinstance(follow, dict) or not follow.get("selector"):
            errs.append("entry.follow: object with a CSS 'selector' for the links to follow")
        else:
            ml = follow.get("max_links", 100)
            if not isinstance(ml, int) or ml < 1 or ml > 300:
                errs.append("entry.follow.max_links: int 1..300")
            if "regex" in follow:
                try:
                    re.compile(follow["regex"])
                except re.error as e:
                    errs.append(f"entry.follow.regex: {e}")

    # fetch
    fetch = doc.get("fetch") or {}
    if not isinstance(fetch, dict):
        errs.append("fetch: must be an object")
        fetch = {}
    if fetch.get("method", "GET") not in FETCH_METHODS:
        errs.append(f"fetch.method: one of {FETCH_METHODS}")
    if fetch.get("body_format", "json") not in BODY_FORMATS:
        errs.append(f"fetch.body_format: one of {BODY_FORMATS}")
    if fetch.get("render", "none") not in RENDER_MODES:
        errs.append(f"fetch.render: one of {RENDER_MODES}")
    if fetch.get("render") == "browser":
        errs.append("fetch.render=browser is not runnable server-side in v1 "
                    "(keep the recipe disabled)")
    d = fetch.get("delay_seconds", 1.0)
    if not isinstance(d, (int, float)) or d < 0.5 or d > 30:
        errs.append("fetch.delay_seconds: 0.5..30")

    # parse
    parse = doc.get("parse")
    if not isinstance(parse, dict) or parse.get("kind") not in PARSE_KINDS:
        errs.append(f"parse.kind: required, one of {PARSE_KINDS}")
        parse = {}
    kind = parse.get("kind")
    if kind == "api":
        if "items" not in parse:
            errs.append("parse.items: required for api ('' means the body is the array)")
        fields = parse.get("fields") or {}
        if not isinstance(fields, dict) or not fields:
            errs.append("parse.fields: required non-empty object for api")
        else:
            for n, spec in fields.items():
                errs += _field_spec_errors(n, spec, "parse.fields")
            for req in ("name",):
                if req not in fields:
                    errs.append(f"parse.fields.{req}: required")
            if not ({"start_date", "start_datetime"} & set(fields)):
                errs.append("parse.fields: need start_date or start_datetime")
    elif kind == "html":
        if not parse.get("item"):
            errs.append("parse.item: CSS selector for one event container required")
        fields = parse.get("fields") or {}
        if not isinstance(fields, dict) or not fields:
            errs.append("parse.fields: required non-empty object for html")
        else:
            for n, spec in fields.items():
                errs += _field_spec_errors(n, spec, "parse.fields")
            provided = set(fields) | set((parse.get("page_fields") or {}) if isinstance(parse.get("page_fields"), dict) else {})
            if "name" not in provided:
                errs.append("parse.fields.name: required (or in parse.page_fields)")
            if not ({"start_date", "start_datetime"} & provided):
                errs.append("parse.fields: need start_date or start_datetime")
        if parse.get("date_format") is not None and not isinstance(parse["date_format"], str):
            errs.append("parse.date_format: strptime string")
    if kind == "html" and parse.get("page_fields") is not None:
        pf = parse["page_fields"]
        if not isinstance(pf, dict):
            errs.append("parse.page_fields: object of field specs evaluated once per page")
        else:
            for n, spec in pf.items():
                errs += _field_spec_errors(n, spec, "parse.page_fields")
    if kind in ("html", "api") and "ongoing_if_end_only" in parse and not isinstance(parse["ongoing_if_end_only"], bool):
        errs.append("parse.ongoing_if_end_only: true|false")

    # detail
    detail = doc.get("detail")
    if detail is not None:
        if not isinstance(detail, dict):
            errs.append("detail: must be an object")
        else:
            if detail.get("url_field", "purchase_link") not in RAW_EVENT_FIELDS:
                errs.append("detail.url_field: must be a RawEvent field")
            dp = detail.get("parse") or {}
            if dp.get("kind") not in ("html", "api", "jsonld"):
                errs.append("detail.parse.kind: html | api | jsonld")
            for n, spec in (dp.get("fields") or {}).items():
                errs += _field_spec_errors(n, spec, "detail.parse.fields")
            mpr = detail.get("max_per_run", 60)
            if not isinstance(mpr, int) or mpr < 0 or mpr > 500:
                errs.append("detail.max_per_run: int 0..500")

    ca = doc.get("city_aliases")
    if ca is not None and (not isinstance(ca, dict) or not all(isinstance(v, str) for v in ca.values())):
        errs.append("city_aliases: object of {raw city text: canonical City.name}")

    if "relay" in doc and doc["relay"] not in RELAYS:
        errs.append(f"relay: one of {RELAYS} (fetch+parse run off-box, events posted back)")

    # optional git-controlled scheduling knobs
    if "priority" in doc and not isinstance(doc["priority"], int):
        errs.append("priority: int")
    if "cadence_hours" in doc and (not isinstance(doc["cadence_hours"], int) or doc["cadence_hours"] < 1):
        errs.append("cadence_hours: int >= 1")
    if "enabled" in doc and not isinstance(doc["enabled"], bool):
        errs.append("enabled: true|false")

    # defaults
    dflt = doc.get("defaults") or {}
    if not isinstance(dflt, dict):
        errs.append("defaults: must be an object")
    else:
        for n in dflt:
            if n not in RAW_EVENT_FIELDS:
                errs.append(f"defaults: unknown RawEvent field '{n}'")

    return errs
