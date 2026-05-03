# Route 1 — LLM as long-tail scout

**Status:** Design draft. No production wiring yet.
**Date:** 2026-05-01
**Author:** working notes — sleep on this before plumbing.

---

## Why we're doing this

The big event APIs (Ticketmaster, Eventbrite, SeatGeek, Bandsintown, PredictHQ)
cover the high-volume English-market inventory well. For everything else —
local-language venues, hyper-local lifestyle sites, niche promoter pages,
city-specific aggregators that no global feed indexes — coverage is poor.

**The long tail here is long.** A Tel Aviv user wants to see what's on at
Cameri tonight. A São Paulo user wants Sympla and city-magazine listings. The
collectors we have today won't find these because they require either
schema.org/JSON-LD adoption (rare in non-English markets) or a hand-written
scraper per source (doesn't scale).

Route 1 fills that gap by making Gemini do two jobs that don't scale with
human effort:

1. **Discover** sources for a city (`find_city_guides_ai.py`).
2. **Extract** structured events from any HTML, including SPAs
   (`extract_events_via_ai.py` + `url_context` tool).

Spikes already proved both work — including SPA support via `url_context`
without Playwright.

---

## Where this fits in the existing architecture

```
                       ┌──────────────────────────────┐
                       │  Existing collectors         │
                       │  (TM, Eventbrite, JSON-LD…)  │
                       └──────────────┬───────────────┘
                                      │
                    ┌────────────────────────────────┐
                    │  Normalized event ingestion    │
                    │  (app.models.event.Event)      │
                    └────────────────┬───────────────┘
                                      │
                       ┌──────────────────────────────┐
                       │   Route 1: LLM extractor     │   ← NEW
                       │   • Discovery (per city)     │
                       │   • Extraction (per source)  │
                       │   • Source graduation        │
                       └──────────────────────────────┘
```

The LLM extractor produces rows in the same `Event` shape. From the rest of
the app's perspective it is just one more collector — no schema migrations
required at v1.

---

## The two pipelines

### A. Discovery — `find_city_guides_ai.py` (already spiked)

**Input:** city name.
**Output:** ranked list of candidate URLs that are likely to publish
upcoming-event listings.

```
city ──► Gemini (google_search grounding) ──► JSON candidate list
                                              │
                                              ▼
                            _probe() each (reuse find_city_guides)
                                              │
                                              ▼
                              winners + JSON-LD count + samples
```

Cost: ~$0.005/city. Run weekly or on city onboarding.
Open issue: Gemini occasionally hallucinates URLs (e.g. fabricated
`live.today/הופעות-היום…`). Mitigation: probe stage already verifies the URL
loads and contains plausible event content; failed probes are dropped before
graduation.

### B. Extraction — `extract_events_via_ai.py` + `url_context`

**Input:** an events listing URL.
**Output:** array of normalized event dicts (Event schema).

Two execution modes depending on whether the page is server-rendered:

```
                  ┌─ static HTML available?
                  │      │
       ┌──────────┘      ├── yes ──► HTTP fetch + BS4 clean ──┐
       │                 │                                      │
       │                 └── no  ──► Gemini url_context tool ──┤
       │                            (server-side JS render)   │
       │                                                       ▼
       │                                          Gemini structured output
       │                                          (response_schema = Event)
       │                                                       │
       └───────────────────────────────────────────────────────┴─► events[]
```

The decision rule is mechanical: try cleaned HTML first, and if `clean_html`
output is below a threshold (e.g. <2KB after stripping chrome) the page is a
SPA and we re-call with `url_context` instead. The Cameri spike showed
url_context returning real Hebrew event titles directly from a JS-rendered
page — no Playwright dependency.

Cost: ~$0.05/page (a typical listing page is ~50K tokens cleaned input + a
small structured output). At 200 sources × 4 runs/month ≈ $40/mo.

---

## Source graduation lifecycle

A new source isn't trusted on day one and isn't paid-for forever.

```
   discovered          on trial               recurring             graduated
   ─────────────►      ────────────►          ─────────────►        ────────────►
   Gemini found it     LLM extracts,          consistently good     human writes
   and probe passed    human reviews          → keep on schedule    custom scraper
                       N runs                 with LLM extractor    or schema.org
                                                                    integration
                                              OR
                                              ─────────────────►    blocklisted
                                              consistently bad      (hallucination,
                                                                    duplicate of
                                                                    bigger source,
                                                                    etc.)
```

Stored as `LLMSource` rows:

| field            | purpose                                                |
| ---------------- | ------------------------------------------------------ |
| `url`            | listing page URL                                       |
| `city`           | which city it serves                                   |
| `state`          | `trial` / `recurring` / `graduated` / `blocked`        |
| `last_run_at`    | scheduling                                             |
| `events_seen`    | rolling counter — empty results n times → demote      |
| `dup_rate`       | fraction of events also found by other collectors     |
| `notes`          | freeform — why it was promoted/demoted                |

Recurring sources run on a schedule (daily/weekly per source-yield curve).
Trial sources run once or twice and get human eyeballs before promotion.

---

## Dedup with the existing collectors

The risk: an LLM-discovered source republishes Ticketmaster/Eventbrite
content. We don't want duplicates and we don't want to pay LLM extraction
costs for things we already have.

**Stable source_id (canonical hash):**

```python
source_id = sha256(f"{scrape_source}|{page_url}|{name_normalized}|{start_date}")
```

Used as a unique key on ingest. Same event seen twice from the same source
overwrites cleanly.

**Cross-collector dedup:** done after ingestion in the normalization layer
that already exists for the other collectors — match on
`(name_fuzzy, start_date, venue_name_fuzzy)` and merge. If a Ticketmaster
event and an LLM-extracted event point to the same show, the canonical
record keeps both `purchase_link`s and the higher-quality `image_url`.

`dup_rate` per LLMSource is computed from these merges — that's the signal
that a source is mostly-redundant and should be demoted.

---

## Quality gates

LLMs hallucinate. Three layers protect ingestion:

1. **Schema validation at extraction time.** `response_schema` enforces
   structure. We already use this in `classify_via_api.py` — rejection rate
   collapsed from 1–4% (chat UI) to ~0.01%.
2. **Plausibility check on each event.** Reject if `start_date` is in the
   past, more than 2 years out, or missing a venue when the source is a
   single-venue site. Reject if `purchase_link` host doesn't match the
   source domain or a known ticketing host.
3. **Sampled human review.** Trial sources get 100% review; recurring
   sources get random-sample audit (5%) — flagged to a queue. Repeat
   offenders → blocklist.

---

## Cost model (target steady state)

| activity                    | per-unit cost | volume/mo  | $/mo  |
| --------------------------- | ------------- | ---------- | ----- |
| Discovery (per city)        | $0.005        | 50 cities  | $0.25 |
| Extraction (HTTP path)      | $0.04         | 600 runs   | $24   |
| Extraction (url_context)    | $0.06         | 200 runs   | $12   |
| Audit sampling              | $0.05         | 100 events | $5    |
| **Total**                   |               |            | **~$40** |

Well under the ~$80/mo budget the user authorized. Headroom for growth and
for a periodic re-discovery sweep when a city's source list goes stale.

---

## Build order

Smallest steps that each ship value:

1. **Refactor `extract_events_via_ai.py` → `app/extractors/llm_extractor.py`.**
   Make it importable, add the `url_context` fallback path, return Event
   dicts instead of printing.
2. **Add `LLMSource` model + migration.** Just the registry — no scheduler
   yet. Backfill from current spike runs.
3. **CLI: `scripts/llm_run_source.py URL`.** One-shot extraction →
   normalization → ingestion. Manual driver for the trial state. Lets us
   onboard sources by hand and watch them.
4. **Wire into the existing scheduler** as a new collector. Reads
   `LLMSource WHERE state IN ('recurring','trial')`, runs each at its
   cadence, writes to the same `Event` table.
5. **Discovery as a periodic job.** `find_city_guides_ai.py` per city,
   monthly. New URLs land in `LLMSource(state='trial')`.
6. **Audit dashboard.** Tiny FastAPI page listing trial sources + sampled
   events for thumbs-up/down. Promotes/demotes via DB writes.

Steps 1–3 are the minimum viable path: we can extract from the long tail by
hand for any city, with no scheduler risk. Steps 4–6 turn it into a system.

---

## Risks — accuracy and scale

Honest read before committing. Sorted by how much each one should worry us.

### Accuracy risks

**1. Hallucination of events that don't exist.** *(High severity, hard to detect)*
When fetch quality degrades silently (page didn't render, anti-bot 401,
url_context blocked) the model improvises rather than reporting nothing.
Habima blocking url_context is the canary. Today's plausibility checks catch
gross errors but not "this concert is real but on the wrong Tuesday."
**Mitigation:** structural cross-check — if an event has no string trace in
the raw fetched HTML / url_context output, drop it. Don't rely on prompt
guards.

**2. Date errors.** *(Medium-high, very common)*
Hebrew sites mix DD/MM/YYYY, יום ה' 18:30, "next Saturday." Multi-day
festivals collapse to one day. Recurring shows ("every Thursday in May")
become a single date. Model picks the *first* date when seeing a range.
**Mitigation:** schema is too forgiving today. Make date confidence
mandatory; add a `recurrence` field rather than emitting one row per show;
require both `start_date` and `end_date` together for ranges.

**3. Translation/transliteration drift breaks dedup.** *(High, sneaky)*
Cameri's "השותפה" might come back as "Ha'shutafa" / "The Partner" / Hebrew
across runs. Three names, three rows, dedup never matches because string
distance between Hebrew and Latin is meaningless.
**Mitigation:** keep original-language name canonical — don't translate at
extraction time. Dedup on `(date, venue_id, image-fingerprint)`, not name.

**4. Past-event leakage.** *(Medium)*
Listings include "last season" or "previous shows" sections. Cleaning can't
tell which `<div>` is which; prompt rule "skip past events" is partial.
**Mitigation:** hard post-extraction filter — drop everything before
`today - 1d`.

**5. Price/currency errors.** *(Low-medium)*
"From ₪150" vs "₪150–280" vs "Free with reservation". Schema expects one
number; model picks inconsistently.
**Mitigation:** accept as approximate; surface a range when both bounds
visible. UX problem more than data integrity.

**6. Purchase-link hallucination.** *(Medium, user-visible)*
Model fabricates ticket URLs when it can't see one. A user clicking "Buy
tickets" landing on a 404 is the worst possible trust failure.
**Mitigation:** validate every `purchase_link` exists as a literal substring
in the cleaned HTML before ingestion. If not present verbatim, null it out.

**7. Schema violations even with `response_schema`.** *(Rare, ~0.01%)*
Measured in the classifier work. Steady trickle at scale.
**Mitigation:** already handled — defensive parsing + reject. Non-issue.

### Scale risks

**1. Pagination is unsolved.** *(High, structural)*
City aggregators show ~20 events × ~30 pages. Today's extractor sees one
page. We get 1/30th the inventory and don't know what we're missing. This is
the biggest scale gap — without it, "long-tail coverage" is a marketing
claim, because we capture only the front page of each long-tail source.
**Mitigation:** real solution required — page-walking in the extractor
(multiplies cost ~30×) and/or filter-by-month URL discovery during the
discovery pass. Probably both, depending on source.

**2. 200KB truncation cuts off the tail silently.** *(Medium)*
Pages packing a month of listings into one DOM blow past the cap.
Truncation is silent — late-month events vanish with no signal.
**Mitigation:** split-and-resume — if `len(cleaned) > max_chars`, run two
extractions on first/second halves. Detect at extraction time.

**3. SPA support has selection bias.** *(Medium, hard to measure)*
url_context worked on Cameri, blocked on Habima. We don't know in advance
which sites work. The whole pitch is "long tail" — and the long tail is
exactly where weird auth/anti-bot setups live.
**Mitigation:** track url_context success rate per source; quietly fall
back to a tiny Playwright fleet for sources where it's worth the effort.
Accept that some inventory is unreachable.

**4. Discovery throws too many candidates over time.** *(Medium)*
50 cities × 20 candidates × monthly = 1,000 new candidates/month. Most
won't graduate. `LLMSource` grows unboundedly with stale `trial` rows that
nobody reviews. Trial review is a human bottleneck.
**Mitigation:** auto-demote — trial sources that produce <3 valid events
across 2 runs go to `blocked` without human review.

**5. dup_rate is wrong at small N.** *(Medium, structural)*
A new source's first 20 events look unique. Months later, Ticketmaster
expands to that city and 80% becomes redundant — and we don't notice
because dup_rate is computed cumulative, not rolling.
**Mitigation:** rolling 60-day dup_rate window. Re-graduate sources when
their value changes.

**6. Cost is per-page, not per-event.** *(Low-medium)*
A page with 5 events costs the same as a page with 50. Low-yield sources
silently expensive per-event. The $40/mo target assumes ~50 events/page
average — if real-world is 10, the bill quadruples.
**Mitigation:** track yield per source; demote low-yield sources even if
their events are fine.

**7. Per-minute rate limits on Tier 1.** *(Low)*
Daily caps removed by billing upgrade; RPM still applies. Bursts of 200
sources will throttle.
**Mitigation:** token-bucket scheduler. Trivial when we get there.

### The two that are existential

If only two get fixed before scaling:

- **Pagination.** Without it the strategy fails its own pitch.
- **Hallucination cross-check.** Without a structural "LLM cited text not
  in source" guard, accuracy degrades silently with scale, and we learn
  about it from user complaints.

Both solvable. Neither solved in the current spike. **Both must land
between build steps 1 and 2** before going further.

---

## Open questions to revisit before plumbing

- **Re-discovery cadence.** Cities aren't static — new venues open, sites
  redesign. Monthly feels right; needs a check on whether `LLMSource` row
  count grows unboundedly or stabilizes.
- **Blocklist persistence.** Should blocked URLs survive a re-discovery
  sweep (yes — and we should suppress them from candidate output).
- **Incremental extraction.** Today the extractor sees the whole page and
  returns everything. Worth caching `(source_id → last_seen)` so we only
  ingest new events on subsequent runs?
- **Per-source prompt tuning.** Sites with quirky date formats might need a
  one-line prompt addendum stored on `LLMSource.notes`. Worth it once we
  have data on which sources fail extraction.
- **Quota / parallelism.** Tier 1 billing removes daily caps but rate
  limits still apply. Need a token-bucket if we run >50 sources in a burst.
- **What about Habima-style 401s?** Some sites block `url_context`. For
  those we either accept lost coverage or fall back to Playwright as a
  later optimization.

---

## What this is NOT

- Not a replacement for the schema.org collectors. Schema.org sources are
  free, fast, and accurate — keep them.
- Not a default extractor for every event. The LLM is the long-tail scout;
  the cheap path always runs first.
- Not a one-shot scrape. The whole point is the recurring extractor +
  graduation flow that turns a model call into stable inventory over time.
