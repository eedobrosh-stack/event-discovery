# recipes/ — Route 3 source recipes

One JSON file per domain. Git is the source of truth; the `source_recipes`
table on Render is the runtime copy. Full schema and rationale:
`docs/recipe_extraction_design.md`. Engine: `app/services/recipes/`.

## Authoring checklist (one domain, ~20–40 min)

1. **Pick** from the repair queue (`/api/stats/recipes`, `drift_flag=true`)
   → then proven-yield domains (`/api/stats/graduation` promotion list)
   → then hand-picked seeds.
2. **Inspect the listing page** in Chrome. Check in this order, each is
   more stable than the next:
   - `<link type="text/calendar">` / `.ics` link → `parse.kind = "ics"`
   - `<script type="application/ld+json">` with `@type: Event` → `"jsonld"`
   - Network panel: XHR/fetch returning JSON with the events → `"api"`
     (WordPress: try `/wp-json/wp/v2/<type>`; check whether the date field
     is actually exposed — ACF often isn't)
   - otherwise CSS selectors on the server-rendered HTML → `"html"`.
     Confirm the events are in `curl` output, not injected by JS.
3. **Draft** `recipes/<domain>.json`. Required: `domain` (no www),
   `source_name`, `country`, `entry.urls`, `parse`. Put a `date_format`
   (strptime) whenever the site uses one fixed format; free-text dates go
   through dateparser with `language`.
4. **Dry-run** (no DB, capped at 25 requests by default):
   ```
   PYTHONPATH=. python3 scripts/recipe_run.py recipes/<domain>.json --dry-run --show 10
   ```
   Read: rows parsed vs events built, the `dropped` reasons, the fill
   rate line, the date range. `no_date` > 0 means the selector or format
   is off. `past` is normal on pages that list history.
5. **Push** to prod (Claude runs this over SSH to the Render box):
   ```
   cd /opt/render/project/src && PYTHONPATH=. python3 scripts/recipe_run.py recipes/<domain>.json --upsert
   PYTHONPATH=. python3 scripts/recipe_run.py --execute-dry <domain>   # optional sanity run on prod
   PYTHONPATH=. python3 scripts/recipe_run.py --execute <domain>       # persist now instead of waiting for 01:00 UTC
   ```
   `--upsert` bumps `recipe_version` when the document changed, resets
   `next_run_at` to now, clears the drift flag, and marks the domain's
   LLMSource rows `graduated`.
6. **Verify** next morning in the digest's Recipes block or `--list`.

## Field spec cheat-sheet (`parse.fields`)

| Spec | Meaning |
|---|---|
| `".title"` | text of first match |
| `{"sel": "a", "attr": "href", "absolute": true}` | attribute, resolved against the page URL |
| `{"sel": ".date", "regex": "(\\d{2}\\.\\d{2}\\.\\d{4})"}` | first capture group |
| `{"sel": ".tag", "all": true}` | list of all matches (for `raw_categories`) |
| `{"sel": ".tag", "all": true, "join": ", "}` | joined string |
| `{"attr": "data-id"}` | attribute of the item container itself |
| `{"const": "Barby"}` | constant |
| api kind: `"venue.location.lat"`, `"performers[0].name"`, `"tags[*].name"` | dotted path into each item |

Pseudo-fields: `start_datetime` / `end_datetime` (one ISO/epoch value the
runner splits into date + time). `source_id` is strongly recommended
(slug or numeric id from the URL); without it the runner hashes
name+date+venue.

## Rules the runner enforces regardless of recipe

- robots.txt honoured; `delay_seconds` ≥ 0.5 between requests
- ≤ 400 requests per recipe per run, ≤ 5,000 rows per run
- events with no parseable date are dropped, never guessed
- past-dated events dropped; far-future bound applied by `_save_events`
- one recipe's failure never stops the others
