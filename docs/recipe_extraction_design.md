# Route 3 — Recipe-driven extraction (Gemini-free long tail)

**Status:** Design for review. No code yet.
**Date:** 2026-09-16
**Supersedes as the main long-tail path:** Route 1 Cadence A (Gemini extraction).
Cadence A stays in the codebase as an opt-in fallback; Cadence B (Brave
discovery) is paused.

---

## Why

Route 1 proved the long tail exists: 29,832 active index pages across ~5k
domains, and the top 20 domains alone yielded ~850 events/week when Gemini
ran them. It also proved the cost model does not work: every nightly run
re-reads every page's HTML through Gemini (13.6k `html` + 3.6k
`url_context` sources), and the 500 NIS/month cap starves the queue.

The structure of a listing page changes maybe once a year. Paying an LLM to
rediscover it every night is the waste. Route 3 moves that understanding to
a **recipe**: a small JSON document, written once per domain by Claude in
an interactive session on Eedo's Mac (Claude Code subscription, zero
marginal cost), and executed nightly on Render by a deterministic runner
built on `httpx` + `BeautifulSoup` + `icalendar` (all already in
`requirements.txt`).

Split of labour:

| Where | When | Who | Does |
|---|---|---|---|
| Mac, Chrome MCP | daytime session | Claude + Eedo | inspect a site, find the cleanest data path, write + test a recipe, push it to prod |
| Render, APScheduler | nightly | runner | execute every enabled recipe, save events, record health, flag breakage |
| Mac, next session | daytime | Claude | repair recipes the nightly run flagged |

---

## Recipe schema

One recipe per **domain** (not per URL — feverup.com is 67 LLMSource rows
but one recipe). Stored as a row in a new table `source_recipes` with the
recipe body in a JSON column. Top level:

```jsonc
{
  "recipe_version": 1,
  "domain": "icm.org.il",
  "source_name": "icm",               // RawEvent.source; unique per recipe
  "country": "Israel",                // for City fallback, same rule as Cadence A
  "city_name": "Jerusalem",           // optional; per-event venue_city wins
  "timezone": "Asia/Jerusalem",
  "language": "he",                   // hint for date parsing
  "entry": { ... },                   // how to enumerate listing URLs (§ entry)
  "fetch": { ... },                   // how to GET them (§ fetch)
  "parse": { ... },                   // how to turn a response into events (§ parse)
  "detail": { ... },                  // optional second hop per event (§ detail)
  "defaults": { "price_currency": "ILS", "raw_categories": ["Jazz"] },
  "notes": "Server-rendered. Date in Hebrew, dd.mm.yy inside <time>."
}
```

### `entry` — which URLs to fetch

Reuses the semantics already in `resolve_template_urls` (months mode /
values mode) plus a plain list and pagination:

```jsonc
"entry": {
  "urls": ["https://icm.org.il/jazz-concerts", "https://icm.org.il/classical"],
  "template": "https://site.com/events?month={year}-{month:02d}",   // optional
  "range_months": 6,                                                  // with template
  "values": ["tel-aviv", "haifa"],                                    // or {value}
  "paginate": {
    "mode": "next_link" | "page_param" | "cursor" | "none",
    "selector": "a.next",              // next_link: CSS to the next anchor
    "param": "page", "start": 1,       // page_param: ?page=N until empty
    "cursor_path": "meta.next_cursor", // cursor (API): JSONPath-ish into body
    "max_pages": 20
  }
}
```

### `fetch` — how to GET

```jsonc
"fetch": {
  "method": "GET",                   // or POST for API recipes
  "headers": {"Accept": "application/json", "Accept-Language": "he"},
  "body": {"city": "{value}"},       // POST payload; templated like URLs
  "delay_seconds": 1.5,              // politeness between requests
  "timeout": 20,
  "render": "none"                   // "none" only in v1; see § Not covered
}
```

### `parse` — response → events

Four kinds. Exactly one per recipe.

**`jsonld`** — the page carries schema.org/Event. Zero config; reuses
`_jsonld.iter_events` / `jsonld_to_raw_event`. This is what already works
for 2,723 sources; the recipe just pins it and adds pagination/templating.

```jsonc
"parse": { "kind": "jsonld" }
```

**`api`** — the page is rendered from a JSON endpoint (Smarticket, Fever,
Tickchak, most SPAs). Most stable kind. `items` locates the array;
`fields` maps RawEvent fields to dotted paths in each item.

```jsonc
"parse": {
  "kind": "api",
  "items": "data.events",
  "fields": {
    "source_id":     "id",
    "name":          "title",
    "start_date":    "startsAt",              // ISO → date + time split by runner
    "artist_name":   "performers[0].name",
    "venue_name":    "venue.name",
    "venue_city":    "venue.city",
    "venue_address": "venue.address",
    "venue_lat":     "venue.location.lat",
    "venue_lon":     "venue.location.lng",
    "price":         "tickets.minPrice",
    "purchase_link": "url",
    "image_url":     "image.url",
    "description":   "summary",
    "raw_categories": "tags[*].name"
  }
}
```

**`html`** — classic server-rendered listing. CSS selectors relative to
an item container. Each field is either a selector string (text of first
match) or an object for attributes / regex / joins.

```jsonc
"parse": {
  "kind": "html",
  "item": "div.event-card",
  "fields": {
    "source_id":     {"sel": "a.title", "attr": "href", "regex": "/event/(\\d+)"},
    "name":          "a.title",
    "start_date":    {"sel": "time", "attr": "datetime"},          // or text + format
    "start_time":    {"sel": ".hour"},
    "artist_name":   ".performer",
    "venue_name":    ".venue",
    "purchase_link": {"sel": "a.buy", "attr": "href", "absolute": true},
    "image_url":     {"sel": "img", "attr": "src", "absolute": true},
    "price":         {"sel": ".price", "regex": "(\\d+)"}
  },
  "date_format": "%d.%m.%Y",        // strptime; omit to use dateparser (§ dates)
  "skip_if_missing": ["name", "start_date"]
}
```

**`ics`** — the site exposes an iCalendar feed. `entry.urls` are the feed
URLs; `icalendar` does the rest.

```jsonc
"parse": { "kind": "ics" }
```

### `detail` — optional second hop

For listings that show only title + date and hide the venue/price on the
event page. Bounded per run so a recipe cannot fan out into thousands of
requests.

```jsonc
"detail": {
  "url_field": "purchase_link",       // which parsed field holds the URL
  "parse": { "kind": "html", "fields": { "venue_name": ".venue", "price": {...} } },
  "max_per_run": 60,                  // only new source_ids get a detail fetch
  "only_new": true
}
```

### Field normalisation done by the runner (not the recipe)

- **Dates:** `start_date` may arrive as ISO 8601, epoch, `strptime` via
  `date_format`, or free text. Free text goes through `dateparser` with
  `languages=[recipe.language]` and the recipe timezone. `dateparser` is a
  new dependency (pure Python, supports Hebrew). Events with an unparseable
  date are dropped and counted (`dropped_no_date`), never guessed.
- **Times:** split from a datetime when present; else `start_time` field.
- **Relative URLs** resolved against the fetched page when `absolute: true`.
- **`source_id`:** required. If the recipe cannot provide one, the runner
  derives `sha1(name|start_date|venue_name)[:16]`. The existing
  `_save_events` dedupe on `(source, source_id)` then works unchanged.
- **Horizon:** the existing `MAX_FUTURE_DAYS` and past-date rejection in
  `_save_events` apply; the runner does not re-implement them.

---

## Storage

New table `source_recipes` (alembic migration, SQLite-compatible):

| Column | Type | Notes |
|---|---|---|
| id | int PK | |
| domain | str unique | `icm.org.il` |
| source_name | str unique | becomes `RawEvent.source` |
| enabled | bool | runner skips disabled |
| recipe | JSON | the document above |
| recipe_version | int | bumped on every edit |
| country / city_name | str | denormalised from recipe for the City fallback |
| priority | int | run order; higher first (seed from Cadence A yield) |
| cadence_hours | int | default 24; 168 for slow venue pages |
| last_run_at, next_run_at | datetime | |
| last_status | str | ok / empty / error / drift |
| last_error | text | truncated traceback or HTTP status |
| last_fetched, last_saved | int | |
| runs_total, saved_total | int | |
| recent_saved_counts | JSON | last 10, same drift logic as LLMSource |
| drift_flag | bool | true → surfaced for repair |
| written_by | str | `claude-session` / `manual` |
| created_at, updated_at | datetime | |

Relationship to `LLMSource`: none enforced. Existing LLMSource rows for a
recipe's domain get `state='graduated'` when the recipe is enabled (the
state already exists for exactly this meaning: "we wrote a custom
collector, the LLM extractor no longer needs to run"). That is also what
stops Cadence A from paying Gemini for a domain a recipe covers.

---

## Runner — `recipe_extract_job`

`app/services/recipes/runner.py` + one new job in `app/scheduler/jobs.py`,
registered in `main.py` next to the other add_job calls.

```
nightly 02:00 UTC (after Route 2 collect_all_events, before cleanup)
async with _heavy_job_lock:
  ScanLog(job_name="recipe_extract", status="running")
  for recipe in enabled, next_run_at <= now, ORDER BY priority DESC:
      ScanLog(job_name="recipe_extract", detail=recipe.domain)
      urls = expand(recipe.entry)                     # reuse resolve_template_urls
      raw_events = []
      for url in urls (with pagination, delay_seconds, max_pages):
          body = fetch(recipe.fetch, url)              # httpx, retries 2, UA rotation
          raw_events += parse(recipe.parse, body, url)
      raw_events = detail_hop(recipe.detail, raw_events)   # bounded
      raw_events = normalise(raw_events, recipe)           # dates, ids, defaults
      city = resolve_city(recipe)                          # same rule as Cadence A
      saved = registry._save_events(raw_events, city, db)  # existing ingest path
      update health columns; drift check on recent_saved_counts
      db.expire_all(); gc.collect()                        # 2 GB Render box
  ScanLog → success (or failed with the first exception)
```

Design rules:

- **Reuse the ingest path.** `_save_events` already handles entity
  decoding, `@`-venue splitting, dedupe, venue resolution, horizon
  bounds, and event_type categorisation downstream. The runner produces
  `RawEvent`s and nothing else.
- **Per-recipe isolation.** One recipe's exception is recorded on its row
  and the loop continues. A wedged fetch is bounded by `timeout` and a
  per-recipe wall clock (default 10 min) using the same `asyncio.wait_for`
  pattern that bounds the LLM extractor today.
- **Politeness.** `delay_seconds` per request, one domain at a time, a
  fixed descriptive User-Agent (`Supercaly/1.0 (+https://superca.ly)`),
  `robots.txt` checked once per domain per run and honoured.
- **Budget.** Hard cap of 400 HTTP requests per recipe per run and 20k per
  job; both logged. No paid fetcher (ScrapingBee) is called by the runner.
- **Memory.** Flush to DB per recipe, not per job; recipes never hold more
  than one listing page's items in memory at once beyond the aggregated
  list, which is capped at 5,000 events per recipe per run.

### Health and drift

Same idea as `_update_drift_state` on LLMSource, reused not copied:

| Signal | Row state | Nightly action |
|---|---|---|
| saved > 0 | `ok` | schedule `next_run_at = now + cadence_hours` |
| fetched > 0, saved = 0 for 3 runs | `empty` | probably all dupes; keep running, lower priority |
| fetched = 0 for 2 runs | `drift` | `drift_flag=true`, `cadence_hours` doubled |
| HTTP 4xx/5xx or parse exception | `error` | `drift_flag=true` after 2 consecutive |

`drift_flag=true` rows are the **repair queue** for the next Mac session.

### Observability

- `/api/stats/recipes` — one row per recipe: status, last_saved, saved_total,
  drift_flag, last_error. Sorted drift-first.
- The morning digest (`supercaly-morning-digest` skill) gains a "Recipes"
  block: saved last night by recipe, and the repair queue.
- Every event saved carries `source = recipe.source_name`, so the existing
  `/api/stats/source-matrix` shows recipe yield with no changes.

---

## Authoring loop (Mac side)

A new personal skill, `/supercaly-recipe`, drives this. Per domain:

1. **Pick the target.** From the repair queue first, then the proven-yield
   list (`/api/stats/graduation` promotion candidates, in yield order),
   then the hand-picked Israeli seeds.
2. **Inspect.** Open the listing in Chrome (MCP). Check, in this order,
   because each is more stable than the next: ICS/RSS link → JSON-LD in
   source → XHR/fetch JSON call in the Network panel → server-rendered
   HTML selectors.
3. **Draft the recipe** as JSON in `~/supercaly/recipes/<domain>.json`.
4. **Dry-run locally:** `PYTHONPATH=. python3 scripts/recipe_run.py
   recipes/icm.org.il.json --dry-run` prints parsed events (count, first
   10, date-parse failures, missing-field counts) without touching any DB.
5. **Publish:** commit + push to `main`. Startup (`_deferred_seed`)
   runs `sync_recipes_from_dir` — upserts the `source_recipes` row,
   bumps `recipe_version` on change, resets `next_run_at`, marks the
   domain's LLMSource rows `graduated` — and a one-shot
   `recipe_extract` fires ~25 min after boot for due recipes.
   (`scripts/recipe_run.py --upsert` over SSH remains as a manual path;
   SSH from the Claude session is blocked by policy, which is why deploy
   became the publish step.)
6. **Verify next morning** in the digest.

Recipes live in git (`recipes/*.json`) as the source of truth and in the
DB as the runtime copy. `recipe_run.py --sync-all` pushes the folder.

Google/Chrome discovery of *new* index sites is **not part of this
phase**. It comes back only when the recipe queue runs dry, and then as
a slow, human-paced Chrome MCP loop feeding `entry.urls` for new recipes.

---

## Scaling beyond hand-written recipes (added 2026-09-17)

The LLMSource pool is ~5k domains with a power-law yield; hand-writing
recipes for all of them is neither possible nor worth it. Three gears:

1. **Auto-enroll JSON-LD domains** (shipped): `auto_enroll.py` turns every
   domain whose Cadence A pages extracted via JSON-LD into a generic
   `jsonld` recipe with the domain's pages as `entry.urls` (≤40, by
   yield). Runs at startup and weekly. Git recipes take precedence.
2. **Platform templates + detector** (next): one recipe template per
   platform (WordPress The Events Calendar REST, Elementor grids,
   Squarespace/Wix event blocks, Drupal, Tickchak/Smarticket white-labels)
   and a nightly marker detector that attaches the template to matching
   queued domains. No LLM.
3. **Autonomous drafting** (later): a nightly headless `claude -p` job on
   the Mac drafts recipes for the highest-yield remaining domains,
   dry-runs them, validates against the Gemini-era `events_saved_total`
   for the same pages, and queues passing drafts for a morning approval.

## Not covered in v1 (and what to do instead)

| Gap | v1 answer |
|---|---|
| JS-rendered page with no discoverable JSON API | Mark recipe `render: "browser"`, disabled. Revisit with a headless fetch **on the Mac** during the daytime session (Chrome MCP can save rendered HTML), not on the 2 GB Render box. |
| Sites needing login / anti-bot | Skip. Not worth the fragility. |
| Multi-city aggregators (feverup: 67 city paths) | `entry.values` list + `{value}` template; `venue_city` per event drives City assignment as today. |
| Hebrew / non-Gregorian dates | `dateparser` with `languages=["he"]`; where a site uses a fixed format, prefer `date_format` (strptime) for determinism. |
| Recipe correctness testing | `--dry-run` output is the test. Add `recipes/tests/<domain>.expected.json` (count ≥ N, required fields present) only for the top-yield domains. |

---

## Delivery plan

| Step | Scope | Effort |
|---|---|---|
| 1 | Migration + `SourceRecipe` model + `/api/stats/recipes` | half day |
| 2 | Runner: fetch / parse (jsonld, api, html, ics) / normalise / save; `scripts/recipe_run.py` with `--dry-run` and `--upsert` | 1–2 days |
| 3 | Job wiring + drift + ScanLog + digest block | half day |
| 4 | First 5 recipes: feverup, visitberlin, rausgegangen, plus icm.org.il and one Smarticket venue | one session each |
| 5 | `/supercaly-recipe` skill (authoring checklist, dry-run, push) | half day |
| 6 | Graduate covered LLMSource domains; confirm Cadence A/B stay disabled; watch Gemini spend go to ~0 | ongoing |

Success criterion for the phase: recipe-sourced events exceed Ticketmaster's
daily inflow (currently ~200/day) within three weeks of step 4, at zero
Gemini and zero Brave spend.
