# Supercaly — context for Claude sessions

The Super Calendar at **superca.ly**. Event-discovery webapp aggregating
concerts / theatre / sports / comedy / festivals across ~30 priority cities.
Hosted on Render. This file is auto-read by Claude Code at session start;
also works as Project knowledge if pasted into a claude.ai Project.

---

## Stack at a glance

- **Backend**: FastAPI + SQLAlchemy. Single uvicorn process on Render.
- **DB**: SQLite both locally (`./data/events.db`) and on Render (the
  Render free tier ships with a persistent disk, so we run SQLite there
  too rather than maintaining a separate Postgres). Schemas in
  `app/models/`; `database.py` exposes `SessionLocal`. Practical
  consequence: any cross-DB SQL (LEAST/GREATEST, FILTER aggregates,
  array ops, etc.) does NOT work on prod — stick to SQLite-compatible
  syntax even when scripts feel like they're running against Postgres.
- **Frontend**: vanilla HTML + JS (no framework, no bundler). `index.html`
  is the homepage, `results.html` the search-results page, `stats.html`
  the admin coverage dashboard, `admin.html` and `llm-sources.html` for
  ops views. `app.js` (results-page logic) and `home.js` (homepage logic)
  carry per-page JS — small overlap, intentionally not extracted.
- **Scheduling**: APScheduler in-process. All jobs defined in
  `app/scheduler/jobs.py`; wired up in `app/main.py` startup.
- **External APIs**: **Gemini** (LLM extraction + classifier), **Brave Search**
  (Cadence-B discovery + Lever-C re-classify), **Bandsintown**, **Ticketmaster**,
  **Spotify** (artist enrichment), **Eventbrite** (web-scraped, the API is
  deprecated). Keys live in `.env`. See `app/config.py`.
- **Deploy**: push to `main` → Render auto-builds and redeploys.
  Health check on `/` so port-bind happens fast; cache warming runs
  in a deferred startup task (cities, metros, suggestions index).

## Two-route event collection

**Route 2 — Hand-coded collectors** (the bulk of events). Each source has
a `BaseCollector` subclass in `app/services/collectors/scrapers/`. They're
registered in `app/scheduler/jobs.py` (top of file, ~line 60) and invoked
per priority city by `collect_all_events`. Sources include Ticketmaster,
Bandsintown, Eventbrite, ResidentAdvisor, Dice, Songkick, Skiddle, Xceed,
Meetup, Lu.ma, plus dedicated venue scrapers (Barby, Cameri, Hatarbut,
Leaan, Smarticket, Tickchak, Mevalim, NYC venues, IsraelSites, etc.).
Sport collectors: ESPN, MLB StatsAPI, OpenF1, EuroLeague, ChooseChicago.

**Route 1 — LLM-driven long-tail** (for sources too small or hetero-
geneous to hand-code). Two cadences:
- **Cadence B (Discovery)** — `llm_discover_sources_job` in `jobs.py`.
  Uses **Brave Search** (`app/extractors/discovery_search.py`) to fire
  themed per-city queries, then a Gemini classifier to filter results
  for genuine event-listing pages. Found pages get registered as
  `LLMSource` rows in state=`trial`. Daily, 10 cities/run with LRU
  rotation through `PRIORITY_CITIES` (~30 cities → ~3-day full cycle).
- **Cadence A (Extraction)** — `llm_extract_recurring_job`. Walks each
  active LLMSource (state in {trial, recurring}), fetches HTML, runs
  the LLM extractor (`app/extractors/llm_extractor.py`) which tries
  JSON-LD first then falls through to Gemini-generated structured
  extraction. Auto-promotes trial → recurring after 3 consecutive
  successful runs; auto-blocks after 3 consecutive empty runs (drift
  detection in `_update_drift_state`).
- Schedule: B fires +210 min from boot, A fires +240 min — B first
  so the same-night pool gets extracted (`a942a16`).

### Discovery method selection
Env var `DISCOVERY_METHOD` ∈ {`search`, `gemini`}. Auto-detects `search`
when `BRAVE_API_KEY` is set. The Gemini-grounded path
(`discover_via_gemini` in `discovery.py`) is kept as a fallback and
generates URLs from scratch — higher hallucination rate, used only if
Brave is misconfigured. **Brave was the Q2 pivot from Google CSE** —
Google deprecated whole-web Programmable Search Engines for new
accounts (`db65cfd`).

## Genre / artist classification (the data-quality layer)

Two-level taxonomy in `genre_taxonomy` (13 parent genres, 92 sub-genres).
Per-artist classification in `artist_genre` (primary + 2 secondaries +
confidence). Coverage was lifted from 74.7% → 87.4% via three levers:
- **A** — `scripts/improve_genre_coverage.py` Phase A: bridge
  `Performer.genres` (Spotify/MusicBrainz tags) into our taxonomy.
- **B** — same script, Phase B: pure Gemini classifier on remaining
  unmatched artists.
- **C** — `scripts/improve_genre_via_brave.py`: Brave-augmented retry
  for artists Gemini doesn't recognise from name alone. Searches the
  artist, feeds the top 3 result snippets to Gemini as classification
  context. Now accepts `--country=Israel` to target geo-buckets
  (`0e9a89d`).

Seed bundle at `app/seed/artist_classifications.json.gz` — dumped by
`scripts/dump_classifications_seed.py`, loaded by
`_seed_artist_classifications()` in `main.py` on every startup.
Loader supports both inserts AND upgrades-of-UNKNOWN (`fa375a3`).

## Themes (per-event topic classification)

Parallel to the artist-bound genre system above, but bound directly
to events — for non-music content (conferences, eventually workshops,
festivals) that has no headliner artist to derive a topic from.

- **Schema** (`app/models/theme.py`): `themes` (PK = theme name) +
  `event_themes` association (event_id, theme_name).
- **30 themes seeded**: AI / Cybersecurity / Startup / Crypto /
  Consumer Electronics / DevOps / FinTech / Healthcare / Marketing /
  Career / Psychology / Education / Mental Health / Medicine /
  Pharmaceutical / Sustainability / Climate / Energy / Real Estate /
  Legal / Compliance / Manufacturing / Retail / Media / Hospitality /
  Logistics / Government / Architecture / Agriculture / Insurance.
- **Conference event_type**: collapsed 2026-05-30 from 5 redundant
  subtypes (Tech Conference / AI Tech Conferences / Startup Showcases
  / Cybersecurity Conferences / Consumer Electronics Shows) into a
  single `Conference` type. Topic specialization moved entirely to
  themes. `artist_name` is auto-nulled when assigning Conference type
  (speakers ≠ artists).
- **Two-tier classifier**:
  - **Keyword path** in `scripts/categorize_events.theme_match()` —
    runs hourly inside `categorize_new_events_job`. Conservative
    keyword sets per theme.
  - **LLM path** in `app/scheduler/jobs.llm_classify_conferences_job`
    — Gemini Flash-Lite, hourly. Picks Conference events with no
    themes, classifies via LLM. Two outcomes per event:
      `is_conference=true`  → assigns themes from the catalog.
      `is_conference=false` → strips Conference event_type (catches
                              false-positives the broad
                              "conference"/"summit" keywords swept
                              in — volleyball meetups, speed dating,
                              etc.).
- **API**: `/api/events?themes=AI,Cybersecurity` filters via the
  event_themes association. `/api/suggestions?q=AI` returns
  `kind=theme` autocomplete chips.

## Autocomplete architecture

- **Server**: `app/api/suggestions.py` + `_suggestions_index.py`. The
  index is a pre-built in-memory structure (artists, sport teams,
  event types, categories, parent + sub genres, venues, event names).
  Built once at startup, refreshed every 30 min. Match scan typically
  0.5–3 ms (`3ec2004`); prefix bucketing on the artists/event_names
  lists narrows worst-case scans (`7ee82f5`).
- **Per-query response cache**: 5-min TTL, in-memory.
- **Frontend cache**: per-tab Map keyed by query string, 5-min TTL.
  Plus *forward-typing chaining*: deriving "jazzy" results from cached
  "jazz" results without a fetch. Guard at length 4 because the
  matching rule changes (whole-word vs word-start) — see `e74b377`.
- **Match semantics**: `name_match_ilike` in `_search_filters.py`.
  Multi-word queries AND each token's word-start match (so "Tel A"
  matches "Hapoel IBI Tel Aviv"). Single-token queries use word-start
  if ≥4 chars, whole-word if shorter.
- **Priority** (post-`8f00c67`): Sub-genre / Genre / Artist+Team /
  Format (EventType.name) / Category (EventType.category) / Venue /
  Event-name. Sub-genre chips filter to their parent (Flavor 1 — clicking
  "Opera" returns all Classical events). Sport teams tied with
  artists at slot 3.
- **Cache busters**: `index.html` uses `home.js?v=N`, `results.html`
  uses `app.js?v=M`. Bump on every JS change or browsers serve stale
  code (this has bitten us — see `3a43ef0`).

## Location hierarchy

Autocomplete cascade (top to bottom): **Direct city hit → Metro →
Country → US State → Other cities**. State layer added in `b4e4805`
— `app/api/_us_states.py` maps 2-letter codes to canonical full
names. City labels for US become `"{name}, {state} State"`; if the
city's name collides with a state name (NYC, Mississippi City, etc.),
" City" gets appended for disambiguation. Direct city-name hits
promote above their containing metro (`4ffa15b` — typing "tel av"
shows Tel Aviv before Gush Dan).

## Important conventions

- **All commits** carry detailed multi-line messages explaining WHY
  not just WHAT. Match this style.
- **Cache busters on JS files**: bump `?v=N` in the script tags
  whenever you change `home.js` or `app.js`.
- **Don't break Cadence schedules**: `a942a16` set Cadence B (+210)
  before Cadence A (+240) so the same-night pool gets extracted.
- **`_heavy_job_lock`** serialises long-running jobs (collect_events,
  enrich_youtube, llm_extract, etc.) so we don't OOM Render's worker.
- **Gemini spend-cap circuit breaker**
  (`app/services/gemini_circuit_breaker.py`). All LLM callers check
  `is_open()` before calling and trip the breaker when the
  "monthly spending cap" error fires. Auto-resets after 6h so the
  month-rollover unblocks itself. When LLM jobs mysteriously skip
  with empty results, first probe the breaker via SSH — don't assume
  the job is broken.
- **Render-only ops** for prod-touching scripts. Run them over SSH
  to the Render box for `dedupe_us_cities.py`,
  `backfill_mevalim_artist_name.py`, `improve_genre_via_brave.py`,
  `compute_artist_related.py`. **Claude runs these** — the user
  does not want to run scripts manually. Always dry-run first; if
  numbers look sane (match expected magnitudes, sample looks
  right), proceed straight to `--apply`. If anything looks off,
  surface the dry-run summary and ask before applying.
- **Dry-run first** is the convention for any data-mutating script.
- **PYTHONPATH=. + dotenv** for local script invocation:
  `PYTHONPATH=. python3 scripts/foo.py`. Most scripts call
  `load_dotenv("/Users/eedo.b/supercaly/.env")` at the top.
- **`Event.name` vs `Event.artist_name`**: some sources (mevalim,
  techconf) historically left `artist_name` empty and put the
  performer in `name`. The mevalim collector now mirrors them
  (`d66b55b`); the AC index has a safety net for both columns; the
  `artist_exact` events filter matches both columns when artist_name
  is empty (`e926a6e`).
- **Sport events** named "League - Home vs Away" — the sport-league
  early-exit in `suggestions.py` strips out the rest of the suggestion
  pipeline when the query matches a league prefix.

## Key file map

| Path | What |
|---|---|
| `app/main.py` | FastAPI app, startup hooks, all scheduler `add_job` calls (~line 700-870) |
| `app/scheduler/jobs.py` | Every background job. 2K+ lines, search by `async def collect_*` etc. |
| `app/services/collectors/scrapers/*.py` | One file per source. `BaseCollector.collect()` is the contract. |
| `app/extractors/discovery_search.py` | Brave-driven Cadence B discovery |
| `app/extractors/discovery.py` | Legacy Gemini-grounded Cadence B fallback |
| `app/extractors/llm_extractor.py` | Cadence A — JSON-LD-first then LLM extraction |
| `app/api/_search_filters.py` | `name_match_ilike` + `resolve_genre_artist_names` + format-fallback |
| `app/api/_suggestions_index.py` | In-memory AC candidate index + matchers |
| `app/api/suggestions.py` | `/api/suggestions` endpoint |
| `app/api/events.py` | `/api/events` (the search results) |
| `app/api/cities.py` | `/api/cities`, `/api/cities/countries`, `/api/cities/states` |
| `app/api/stats.py` | `/api/stats/*` — coverage dashboards |
| `app/api/_us_states.py` | Code → name + name-overlap detection for city disambig |
| `app/seed/artist_classifications.json.gz` | The genre taxonomy + artist classifications, loaded at boot |
| `frontend/app.js` | Results-page JS — autocomplete, filter chips, search call |
| `frontend/home.js` | Homepage JS — same patterns, slimmer |
| `frontend/index.html` `frontend/results.html` `frontend/stats.html` | Pages |
| `scripts/improve_genre_coverage.py` | Levers A+B (Performer.genres bridge + Gemini batch) |
| `scripts/improve_genre_via_brave.py` | Lever C (Brave-augmented retry) — supports `--country` |
| `scripts/dump_classifications_seed.py` | Dump local artist_genre + taxonomy → seed bundle |
| `scripts/dedupe_us_cities.py` | Idempotent city-row deduper |
| `scripts/dedupe_venues.py` | Venue-row deduper — ≥2-of-5 signals (geo / events / phone / url / address), name-based sub-venue (hall) veto, `--merge-pair` override for known cross-language clusters |
| `scripts/dedupe_events.py` | Cross-source event-row deduper — buckets on (start_date, venue_id, primary identifier), unions event_types onto canonical, ORM-driven so m2m cascades |
| `scripts/backfill_mevalim_artist_name.py` | One-off SQL: name → artist_name for mevalim rows |
| `scripts/seed_llm_sources.py` | Manual seed of LLMSource trial pool |

## Active issues / open queue

(See user's TodoWrite list for live state — these are the long-tail
items that have been queued for a while.)

- Onboard `travelportland.com` via Move 2 (URL-template iteration)
- Re-test `dopdx.com` after Gemini API recovers
- Add a raw-response logger to investigate Gemini malformed-JSON
  failures
- Pagination Move 3 (generic `rel=next` walker)
- Refactor `enrich_youtube` to commit + free memory per batch
- GA4: register custom dimensions
- Add tennis to ESPN league config
- "Athletes-as-Artists" — surface athletes via the artist surface
- Improve `event_type` classifier quality (long-tail mis-tagging)
- Investigate transient duplicate row (`Showing 2 of 1`) if it recurs
- Mis-categorisation of events (general)
- Restore `osm enrich` WIP from stash@{0}
- User has an idea for handling sub-genre noise in the artist-
  classification population — deferred until they re-share

## Working with the user

- Prefers concise, honest assessments. If a path is a dead end,
  surface that early and propose the pivot rather than grinding.
- Approves designs ("ok, go ahead") before code changes for non-
  trivial work. For pure bugs / one-line fixes, ship and explain.
- Does NOT want to run scripts manually. Claude runs prod-touching
  scripts over SSH to the Render box (`srv-d7838vchg0os73ak8osg@ssh.oregon.render.com`,
  cd `/opt/render/project/src` then `PYTHONPATH=. python3 scripts/…`).
  Always dry-run first; auto-proceed to `--apply` if the dry-run
  output matches expectations, otherwise surface the summary and ask.
- Cost-conscious but not penny-pinching ("I'm willing to pay" came
  up around Brave Search API). Always quote $ estimates for paid
  API runs before committing.
- Uses screenshots heavily — the chat history has dozens of cropped
  terminal/browser screenshots. Treat them as definitive evidence.
- Commits go directly to `main`; no branching. Push triggers
  Render auto-deploy. The user generally watches the redeploy and
  tests on prod immediately.
