"""Polite, budgeted HTTP for the recipe runner.

One `Fetcher` per recipe run. It owns:
  • the request budget (hard cap per recipe per run)
  • the inter-request delay (recipe.fetch.delay_seconds)
  • robots.txt (fetched once per host, honoured; failures → allow)
  • retries (2, on 429/5xx/timeouts, with backoff)
  • optional curl_cffi Chrome impersonation for sites that 403 plain
    clients (recipe.fetch.impersonate=true) — same tool the scraper
    fleet already uses; never the paid ScrapingBee path.

Everything is synchronous; the job runs it via asyncio.to_thread so the
scheduler loop stays responsive.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Optional
from urllib import robotparser
from urllib.parse import urlsplit

import httpx

logger = logging.getLogger(__name__)

USER_AGENT = "Supercaly/1.0 (+https://superca.ly; events calendar bot)"
BROWSER_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")

DEFAULT_MAX_REQUESTS = 400
RETRY_STATUSES = {429, 500, 502, 503, 504}


class BudgetExhausted(RuntimeError):
    pass


class RobotsDisallowed(RuntimeError):
    pass


@dataclass
class Response:
    url: str
    status: int
    text: str
    headers: dict

    def json(self):
        return json.loads(self.text)


class Fetcher:
    def __init__(self, fetch_cfg: Optional[dict] = None, *,
                 max_requests: int = DEFAULT_MAX_REQUESTS,
                 respect_robots: bool = True):
        cfg = fetch_cfg or {}
        self.method = (cfg.get("method") or "GET").upper()
        self.headers = {"User-Agent": USER_AGENT,
                        "Accept-Language": "en,he;q=0.8"}
        self.headers.update(cfg.get("headers") or {})
        self.body_template = cfg.get("body")
        self.delay = float(cfg.get("delay_seconds", 1.0))
        self.timeout = float(cfg.get("timeout", 20))
        self.impersonate = bool(cfg.get("impersonate", False))
        # fall back to impersonation on a 403/429 (default on; recipes
        # can pin "auto_impersonate": false)
        self.auto_impersonate = bool(cfg.get("auto_impersonate", True))
        self.switched_to_impersonation = False
        self.max_requests = int(cfg.get("max_requests", max_requests))
        self.respect_robots = respect_robots
        self.requests_made = 0
        self._last_at = 0.0
        self._robots: dict[str, Optional[robotparser.RobotFileParser]] = {}
        self._client = httpx.Client(
            timeout=self.timeout, follow_redirects=True, headers=self.headers,
        )

    # ── lifecycle ────────────────────────────────────────────────────────
    def close(self):
        try:
            self._client.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()

    # ── robots ───────────────────────────────────────────────────────────
    def _allowed(self, url: str) -> bool:
        if not self.respect_robots:
            return True
        parts = urlsplit(url)
        base = f"{parts.scheme}://{parts.netloc}"
        if base not in self._robots:
            rp = robotparser.RobotFileParser()
            try:
                r = self._client.get(base + "/robots.txt", timeout=8)
                if r.status_code == 200 and r.text:
                    rp.parse(r.text.splitlines())
                    self._robots[base] = rp
                else:
                    self._robots[base] = None
            except Exception:
                self._robots[base] = None
        rp = self._robots[base]
        if rp is None:
            return True
        try:
            return rp.can_fetch(USER_AGENT, url) or rp.can_fetch("*", url)
        except Exception:
            return True

    # ── core ─────────────────────────────────────────────────────────────
    def _pace(self):
        wait = self.delay - (time.monotonic() - self._last_at)
        if wait > 0:
            time.sleep(wait)
        self._last_at = time.monotonic()

    def _render_body(self, values: Optional[dict]):
        if self.body_template is None:
            return None
        if not values:
            return self.body_template
        s = json.dumps(self.body_template)
        for k, v in values.items():
            s = s.replace("{%s}" % k, str(v))
        return json.loads(s)

    def get(self, url: str, *, values: Optional[dict] = None) -> Response:
        """GET (or POST when the recipe says so). Raises BudgetExhausted /
        RobotsDisallowed / httpx.HTTPError after retries."""
        if self.requests_made >= self.max_requests:
            raise BudgetExhausted(f"{self.max_requests} requests")
        if not self._allowed(url):
            raise RobotsDisallowed(url)

        last_exc: Optional[Exception] = None
        for attempt in range(3):
            self._pace()
            self.requests_made += 1
            try:
                if self.impersonate:
                    resp = self._get_impersonated(url, values)
                else:
                    body = self._render_body(values)
                    if self.method == "POST":
                        r = self._client.post(url, json=body)
                    else:
                        r = self._client.get(url)
                    resp = Response(str(r.url), r.status_code, r.text, dict(r.headers))
                    # Bot wall on the plain client (jambase, concertfix, …
                    # 30 of the first 114 auto-enrolled domains). Retry
                    # once with Chrome TLS impersonation and, if that
                    # works, keep it for the rest of this run. Costs one
                    # extra request only on 403/429-walled sites.
                    if resp.status in (403, 429) and not self.impersonate and self.auto_impersonate:
                        try:
                            self.requests_made += 1
                            alt = self._get_impersonated(url, values)
                            if alt.status < 400:
                                self.impersonate = True
                                self.switched_to_impersonation = True
                                return alt
                        except Exception:
                            pass
                if resp.status in RETRY_STATUSES and attempt < 2:
                    time.sleep(2.0 * (attempt + 1))
                    continue
                return resp
            except (httpx.TimeoutException, httpx.TransportError) as e:
                last_exc = e
                time.sleep(2.0 * (attempt + 1))
        raise last_exc or RuntimeError("fetch failed")

    def _get_impersonated(self, url: str, values: Optional[dict]) -> Response:
        from curl_cffi import requests as cffi  # lazy; optional at dev time
        kw = dict(impersonate="chrome120", timeout=self.timeout,
                  headers={k: v for k, v in self.headers.items()
                           if k.lower() != "user-agent"})
        if self.method == "POST":
            r = cffi.post(url, json=self._render_body(values), **kw)
        else:
            r = cffi.get(url, **kw)
        return Response(str(r.url), r.status_code, r.text, dict(r.headers))
