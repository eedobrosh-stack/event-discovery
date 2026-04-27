#!/usr/bin/env python3
"""
Coverage smoke check for event collectors.

Runs per-source probes from `scripts/collector_coverage.json` against the
LIVE upstream sites (no DB needed) and reports whether each anchor name /
city still surfaces. Catches the class of silent regression that hid
"טונה" for months: when an aggregator restructures its taxonomy or a
venue rewrites its URL pattern, this script fails loudly within one run
instead of waiting for an analyst to type a known name and get nothing.

Usage:
    python scripts/check_collector_coverage.py            # all sources
    python scripts/check_collector_coverage.py mevalim    # one source

Exit code is 0 on full pass, 1 on any failure — wire this into a cron or
manual pre-deploy check whenever you want a heartbeat on each scraper.

Adding a probe:
    Edit `scripts/collector_coverage.json`. Two probe types are supported:
      url_contains_name → fetch a specific page, parse, look for a name.
      city_min_events   → run the collector for a city, assert >= N events.
    Use url_contains_name for narrow regressions (a single act disappeared)
    and city_min_events for broad coverage health (the whole city listing
    page now returns 0 events because the parser broke).

This is a smoke check, NOT a unit test — it makes real HTTPS requests to
upstream sites and intentionally avoids mocking. Run it sparingly and
politely.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path

import httpx

# Make `app.*` imports work whether the script is run from repo root or scripts/
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from app.services.collectors.scrapers import (  # noqa: E402
    concreteplayground as cp_mod,
    mevalim as mv_mod,
    xceed as xc_mod,
)

CONFIG_PATH = _REPO_ROOT / "scripts" / "collector_coverage.json"

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger("coverage")


# ---------------------------------------------------------------------------
# Probe runners — one per (source, probe_type). Each returns a Result.
# ---------------------------------------------------------------------------

@dataclass
class Result:
    source: str
    label: str
    ok: bool
    detail: str


async def _probe_mevalim_url(probe: dict) -> Result:
    url = probe["url"]
    expected = probe["expect_name_contains"]
    label = f"mevalim url_contains_name '{expected}'"
    async with httpx.AsyncClient(timeout=15, headers=mv_mod.HEADERS) as client:
        sem = asyncio.Semaphore(1)
        events = await mv_mod._fetch_page_events(client, sem, url)
    if not events:
        return Result("mevalim", label, False,
                      f"page returned 0 events ({url})")
    matches = [e for e in events if expected in (e.name or "")]
    if not matches:
        sample = ", ".join(repr((e.name or "")[:30]) for e in events[:3])
        return Result("mevalim", label, False,
                      f"no event name contains {expected!r}; got [{sample}]")
    return Result("mevalim", label, True,
                  f"{len(matches)} matching event(s) on {len(events)}-event page")


async def _probe_concreteplayground_city(probe: dict) -> Result:
    city = probe["city"]
    minimum = probe["min_events"]
    label = f"concreteplayground city_min_events {city} >= {minimum}"
    coll = cp_mod.ConcretePlaygroundCollector()
    events = await coll.collect(city)
    if len(events) < minimum:
        return Result("concreteplayground", label, False,
                      f"{city}: got {len(events)} events, need {minimum}")
    return Result("concreteplayground", label, True,
                  f"{city}: {len(events)} events")


async def _probe_xceed_city(probe: dict) -> Result:
    """Xceed exposes parse_listing(html, slug); we replicate the collector's
    one-shot fetch instead of calling the registry, to keep this check
    free of downstream side-effects (DB writes, classifier registration,
    etc.) — the goal is to validate parsing, not full ingestion."""
    city = probe["city"]
    minimum = probe["min_events"]
    label = f"xceed city_min_events {city} >= {minimum}"
    slug = xc_mod.CITY_SLUGS.get(city)
    if not slug:
        return Result("xceed", label, False,
                      f"{city!r} not in xceed CITY_SLUGS — add it or correct probe")
    url = f"{xc_mod.BASE_URL}/{slug}"
    headers = getattr(xc_mod, "_HEADERS", None) or getattr(xc_mod, "HEADERS", {})
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        try:
            resp = await client.get(url, headers=headers)
        except Exception as e:
            return Result("xceed", label, False, f"{city}: fetch error {e}")
    if resp.status_code != 200:
        return Result("xceed", label, False,
                      f"{city}: HTTP {resp.status_code} for {url}")
    events = xc_mod.parse_listing(resp.text, slug)
    if len(events) < minimum:
        return Result("xceed", label, False,
                      f"{city}: got {len(events)} events, need {minimum}")
    return Result("xceed", label, True, f"{city}: {len(events)} events")


# Dispatch: (source, probe_type) → coroutine factory
DISPATCH = {
    ("mevalim", "url_contains_name"):           _probe_mevalim_url,
    ("concreteplayground", "city_min_events"):  _probe_concreteplayground_city,
    ("xceed", "city_min_events"):               _probe_xceed_city,
}


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

async def run(only_sources: list[str] | None) -> int:
    config = json.loads(CONFIG_PATH.read_text())
    config = {k: v for k, v in config.items() if not k.startswith("_")}

    if only_sources:
        unknown = [s for s in only_sources if s not in config]
        if unknown:
            print(f"unknown source(s): {unknown}; available: {list(config)}",
                  file=sys.stderr)
            return 2
        config = {k: v for k, v in config.items() if k in only_sources}

    tasks = []
    for source, probes in config.items():
        for probe in probes:
            ptype = probe.get("type")
            handler = DISPATCH.get((source, ptype))
            if not handler:
                tasks.append(_unsupported(source, probe))
                continue
            tasks.append(handler(probe))

    results: list[Result] = await asyncio.gather(*tasks)

    print()
    by_source: dict[str, list[Result]] = {}
    for r in results:
        by_source.setdefault(r.source, []).append(r)
    for source in sorted(by_source):
        print(f"=== {source} ===")
        for r in by_source[source]:
            mark = "PASS" if r.ok else "FAIL"
            print(f"  [{mark}] {r.label}")
            print(f"         {r.detail}")
        print()

    failed = [r for r in results if not r.ok]
    print(f"{len(results) - len(failed)}/{len(results)} probes passed")
    return 0 if not failed else 1


async def _unsupported(source: str, probe: dict) -> Result:
    return Result(source, f"{source} unsupported_probe",
                  False, f"no handler for type={probe.get('type')!r}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("sources", nargs="*",
                    help="optionally restrict to one or more source names")
    args = ap.parse_args()
    return asyncio.run(run(args.sources or None))


if __name__ == "__main__":
    sys.exit(main())
