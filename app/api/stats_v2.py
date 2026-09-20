"""Stats v2 (roadmap #5, 2026-09-21) — one JSON for frontend/stats_v2.html.

Everything the operator asked for, in one round-trip, optionally scoped to a
country (?country=Israel, the default view on the page):
  totals        upcoming events / venues / artists
  flow          events added in the last 24h / 7d vs events that aged out
  by_country    upcoming events, venues, single-country artists per country
  artists       single-country vs international artists (+ top international)
  taxonomy      upcoming events by category, event type, genre, theme
  enrichment    % upcoming artists with a genre / with a YouTube link
  aggregation   last-24h success by channel: API collectors (attempts, success
                %, new, already-had), recipes (attempted / failed / ok / new),
                other jobs
  failures      failing domains: error class → concrete remedy
Raw SQL via text(): the events table is ~1.3M rows and the page must load
in a few seconds on Render's 2 GB box.
"""
from __future__ import annotations

import ast
import re
from collections import Counter, defaultdict
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database import get_db

router = APIRouter(prefix="/api/stats", tags=["stats"])

API_SOURCES = {"ticketmaster", "eventbrite", "bandsintown", "seatgeek", "predicthq", "meetup", "luma", "allevents",
               "resident_advisor", "xceed", "skiddle", "mlb_statsapi", "songkick", "dice", "tmisrael"}


def _error_class(err: Optional[str]) -> str:
    e = (err or "").lower()
    if not e:
        return "unknown"
    if "403" in e or "429" in e or "captcha" in e or "cloudflare" in e or "just a moment" in e:
        return "bot_wall"
    if "timeout" in e or "timed out" in e:
        return "timeout"
    if "no city resolvable" in e:
        return "geo_unresolved"
    if "robots" in e:
        return "robots_disallow"
    if "invalid recipe" in e:
        return "invalid_recipe"
    if "404" in e or "410" in e:
        return "gone"
    if "http 5" in e:
        return "server_error"
    if "no pages fetched" in e or "fetch" in e or "connect" in e:
        return "fetch_error"
    if "parse" in e or "json" in e:
        return "parse_error"
    return "other"


REMEDIES = {
    "bot_wall": "403/429 wall. If the Mac reaches it (QA geo_walls check) → \"relay\": \"mac\"; else fetch.impersonate=true or a rendered-DOM recipe; drop pure resellers.",
    "timeout": "Raise fetch.timeout, cut entry URLs, or switch to the site's JSON API (api kind).",
    "geo_unresolved": "Country/city alias — add it to canon_country() or set a city_name that exists in the City table.",
    "robots_disallow": "Respect it: disable the recipe; look for an alternate host.",
    "invalid_recipe": "Schema drift — recipe_run.py --dry-run and fix the JSON.",
    "gone": "Listing URL moved — re-discover the events URL and update entry.urls.",
    "server_error": "Transient; retry next sweep, disable after 3 in a row.",
    "fetch_error": "DNS / TLS / connection — verify the domain still resolves.",
    "parse_error": "Markup changed — re-inspect with recipe_run.py --dry-run and update selectors.",
    "other": "Needs a look at the trail (recipe_run.py --dry-run <domain>).",
    "unknown": "No error text recorded — re-run once.",
}


def _scope(country: Optional[str]) -> tuple[str, dict]:
    """JOIN + WHERE fragment restricting events to a country via venue→city."""
    if country:
        return ("join venues v on v.id = e.venue_id join cities c on c.id = v.city_id and c.country = :country",
                {"country": country})
    return ("left join venues v on v.id = e.venue_id left join cities c on c.id = v.city_id", {})


def _rows(db: Session, sql: str, **p):
    return db.execute(text(sql), p).all()


@router.get("/v2")
def stats_v2(country: Optional[str] = Query(None, description="scope to one country, e.g. Israel"),
             db: Session = Depends(get_db)) -> dict:
    join, p = _scope(country)
    UP = "e.start_date >= date('now')"
    ART = "e.artist_name is not null and trim(e.artist_name) <> ''"

    # ── totals + flow ──
    t = _rows(db, f"""select count(*), count(distinct e.venue_id),
                        count(distinct case when {ART} then lower(trim(e.artist_name)) end),
                        sum(e.created_at >= datetime('now','-1 day')), sum(e.created_at >= datetime('now','-7 day'))
                      from events e {join} where {UP}""", **p)[0]
    aged = _rows(db, f"""select sum(e.start_date >= date('now','-1 day')), sum(e.start_date >= date('now','-7 day'))
                        from events e {join} where e.start_date < date('now')""", **p)[0]
    totals = {"upcoming": t[0] or 0, "venues_with_upcoming": t[1] or 0, "artists_upcoming": t[2] or 0}
    flow = {"added_24h": t[3] or 0, "added_7d": t[4] or 0, "aged_out_24h": aged[0] or 0, "aged_out_7d": aged[1] or 0}
    flow["net_24h"] = flow["added_24h"] - flow["aged_out_24h"]
    flow["net_7d"] = flow["added_7d"] - flow["aged_out_7d"]

    # ── by country (always global so the page can switch scope) ──
    bc = _rows(db, f"""select c.country, count(*) ev, count(distinct v.id) ven
                      from events e join venues v on v.id = e.venue_id join cities c on c.id = v.city_id
                      where {UP} group by c.country order by ev desc""")
    # artists: countries per artist (global)
    ac = _rows(db, f"""select lower(trim(e.artist_name)) a, min(e.artist_name) name, count(distinct c.country) nc,
                             min(c.country) country, count(*) ev
                      from events e join venues v on v.id = e.venue_id join cities c on c.id = v.city_id
                      where {UP} and {ART} group by 1""")
    single_by_country = Counter(r.country for r in ac if r.nc == 1)
    international = [r for r in ac if r.nc > 1]
    by_country = [{"country": r.country, "upcoming": r.ev, "venues": r.ven,
                   "artists_single_country": single_by_country.get(r.country, 0)} for r in bc]
    artists = {"single_country": sum(1 for r in ac if r.nc == 1), "international": len(international),
               "top_international": [{"artist": r.name, "countries": r.nc, "upcoming": r.ev}
                                     for r in sorted(international, key=lambda r: (-r.nc, -r.ev))[:15]]}
    if country:
        # scoped view: artists seen in this country, split single vs international
        in_c = _rows(db, f"""select distinct lower(trim(e.artist_name)) from events e {join} where {UP} and {ART}""", **p)
        keys = {r[0] for r in in_c}
        artists["in_scope_single"] = sum(1 for r in ac if r.a in keys and r.nc == 1)
        artists["in_scope_international"] = sum(1 for r in ac if r.a in keys and r.nc > 1)

    # ── taxonomy ──
    cats = _rows(db, f"""select et.category, et.name, count(*) from events e {join}
                        join event_event_types eet on eet.event_id = e.id join event_types et on et.id = eet.event_type_id
                        where {UP} group by 1, 2 order by 3 desc""", **p)
    by_cat = Counter()
    for cat, _n, n in cats:
        by_cat[cat or "?"] += n
    genres = _rows(db, f"""select ag.primary_genre, count(*) from events e {join}
                          join artist_genre ag on ag.normalized_name = lower(trim(e.artist_name))
                          where {UP} and {ART} and ag.primary_genre is not null group by 1 order by 2 desc limit 25""", **p)
    themes = _rows(db, f"""select th.theme_name, count(*) from events e {join} join event_themes th on th.event_id = e.id
                          where {UP} group by 1 order by 2 desc limit 25""", **p) if _has(db, "event_themes", "theme_name") else []
    untyped = _rows(db, f"""select count(*) from events e {join} where {UP}
                           and not exists (select 1 from event_event_types x where x.event_id = e.id)""", **p)[0][0]
    taxonomy = {"categories": [{"name": k, "upcoming": v} for k, v in by_cat.most_common()],
                "event_types": [{"category": c, "name": n, "upcoming": k} for c, n, k in cats[:30]],
                "genres": [{"genre": g, "upcoming": n} for g, n in genres],
                "themes": [{"theme": g, "upcoming": n} for g, n in themes],
                "untyped_events": untyped or 0}

    # ── enrichment ──
    en = _rows(db, f"""with a as (select lower(trim(e.artist_name)) k,
                                  max(case when e.artist_youtube_channel is not null and e.artist_youtube_channel<>'' then 1 else 0 end) yt
                                  from events e {join} where {UP} and {ART} group by 1)
                      select count(*), sum(yt),
                             sum(exists(select 1 from artist_genre g where g.normalized_name = a.k and g.primary_genre is not null)),
                             sum(exists(select 1 from performers pf where pf.normalized_name = a.k and pf.genres is not null and pf.genres<>'' and pf.genres<>'[]'))
                      from a""", **p)[0]
    n_art = en[0] or 0
    with_genre = _rows(db, f"""with a as (select distinct lower(trim(e.artist_name)) k from events e {join} where {UP} and {ART})
                              select count(*) from a where exists(select 1 from artist_genre g where g.normalized_name=a.k and g.primary_genre is not null)
                                 or exists(select 1 from performers pf where pf.normalized_name=a.k and pf.genres is not null and pf.genres<>'' and pf.genres<>'[]')""", **p)[0][0]
    enrichment = {"artists_upcoming": n_art, "with_genre": with_genre or 0, "genre_via_classifier": en[2] or 0,
                  "genre_via_performer_tags": en[3] or 0, "with_youtube": en[1] or 0,
                  "pct_genre": round(100.0 * (with_genre or 0) / n_art, 1) if n_art else 0.0,
                  "pct_youtube": round(100.0 * (en[1] or 0) / n_art, 1) if n_art else 0.0}

    # ── aggregation, last 24h (global: jobs are not per country) ──
    logs = _rows(db, """select job_name, detail, status, coalesce(events_found,0), coalesce(events_saved,0), notes, started_at, finished_at
                        from scan_logs where started_at >= datetime('now','-1 day')""")
    api = defaultdict(lambda: {"attempts": 0, "success": 0, "fetched": 0, "saved": 0})
    recipes = {"attempted": 0, "failed": 0, "ok": 0, "empty": 0, "fetched": 0, "saved": 0, "failed_domains": []}
    other = defaultdict(lambda: {"runs": 0, "success": 0, "failed": 0, "fetched": 0, "saved": 0})
    for job, detail, status, found, saved, notes, st, fin in logs:
        if job == "collect_events":
            per = {}
            try:
                per = ast.literal_eval(notes) if notes and notes.strip().startswith("{") else {}
            except Exception:
                per = {}
            for src, d in (per.items() if isinstance(per, dict) else []):
                if not isinstance(d, dict):
                    continue
                a = api[src]
                a["attempts"] += 1
                a["fetched"] += int(d.get("fetched") or 0)
                a["saved"] += int(d.get("saved") or 0)
                if status == "success" and not d.get("error"):
                    a["success"] += 1
        elif job == "recipe_extract" and detail:
            recipes["attempted"] += 1
            recipes["fetched"] += found
            recipes["saved"] += saved
            if status == "failed" or (notes and "fatal" in (notes or "").lower()):
                recipes["failed"] += 1
                recipes["failed_domains"].append(detail)
            elif found == 0:
                recipes["empty"] += 1
            else:
                recipes["ok"] += 1
        elif job in ("recipe_extract",):
            continue                       # the parent sweep row
        else:
            o = other[job]
            o["runs"] += 1
            o["fetched"] += found
            o["saved"] += saved
            if status == "success":
                o["success"] += 1
            elif status == "failed":
                o["failed"] += 1
    new_by_source = dict(_rows(db, """select scrape_source, count(*) from events where created_at >= datetime('now','-1 day')
                                      group by 1 order by 2 desc"""))
    api_rows = []
    for src, a in api.items():
        api_rows.append({"source": src, "attempts": a["attempts"],
                         "success_pct": round(100.0 * a["success"] / a["attempts"], 1) if a["attempts"] else None,
                         "fetched": a["fetched"], "new_events": a["saved"], "already_had": max(a["fetched"] - a["saved"], 0),
                         "new_rows_24h": new_by_source.get(src, 0)})
    api_rows.sort(key=lambda r: -r["new_events"])
    api_tot = {"attempts": sum(r["attempts"] for r in api_rows), "fetched": sum(r["fetched"] for r in api_rows),
               "new_events": sum(r["new_events"] for r in api_rows), "already_had": sum(r["already_had"] for r in api_rows)}
    api_tot["success_pct"] = round(100.0 * sum(a["success"] for a in api.values()) / api_tot["attempts"], 1) if api_tot["attempts"] else None
    recipes["already_had"] = max(recipes["fetched"] - recipes["saved"], 0)
    recipes["failed_domains"] = recipes["failed_domains"][:40]
    recipe_health = dict(_rows(db, "select last_status, count(*) from source_recipes where enabled = 1 group by 1"))
    aggregation = {"window": "last 24h", "api": {"total": api_tot, "by_source": api_rows}, "recipes": {**recipes, "health_now": recipe_health},
                   "other_jobs": [{"job": j, **o} for j, o in sorted(other.items(), key=lambda kv: -kv[1]["saved"])],
                   "new_rows_by_source": [{"source": s, "new_rows": n} for s, n in list(new_by_source.items())[:25]]}

    # ── failures ──
    fr = _rows(db, """select domain, last_status, last_error, consecutive_errors, saved_total, last_run_at, json_extract(recipe,'$.relay')
                      from source_recipes where enabled = 1 and last_status in ('error','drift') order by saved_total desc""")
    by_class = defaultdict(list)
    for dom, st, err, ce, tot, lr, relay in fr:
        k = _error_class(err)
        by_class[k].append({"domain": dom, "status": st, "consecutive_errors": ce, "saved_total": tot, "last_run_at": lr,
                            "relay": relay, "error": (err or "")[:200]})
    failed_jobs = [{"job": j, "detail": d, "notes": (n or "")[:200], "at": st}
                   for j, d, s, _f, _s, n, st, _fin in logs if s == "failed"][:40]
    failures = {"failing_recipes": len(fr),
                "by_class": [{"class": k, "count": len(v), "remedy": REMEDIES.get(k, ""), "domains": v[:25]}
                             for k, v in sorted(by_class.items(), key=lambda kv: -len(kv[1]))],
                "failed_jobs_24h": failed_jobs}

    return {"as_of": datetime.utcnow().isoformat() + "Z", "country": country, "totals": totals, "flow": flow,
            "by_country": by_country[:60], "artists": artists, "taxonomy": taxonomy, "enrichment": enrichment,
            "aggregation": aggregation, "failures": failures}


def _has(db: Session, table: str, col: str) -> bool:
    try:
        cols = [r[1] for r in db.execute(text(f"pragma table_info({table})")).all()]
        return col in cols
    except Exception:
        return False
