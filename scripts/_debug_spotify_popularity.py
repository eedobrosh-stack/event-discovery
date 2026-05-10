"""Diagnostic — fetch a single artist via the Spotify Search API and
print the raw response shape, so we can see whether `popularity` is
present (and what it actually is) on a known-popular artist.

This is a temporary one-off. Delete after we've identified the bug.

Usage:
    PYTHONPATH=. python3 scripts/_debug_spotify_popularity.py "Jim Gaffigan"
    PYTHONPATH=. python3 scripts/_debug_spotify_popularity.py "Drake"
"""
from __future__ import annotations
import asyncio
import os
import sys
from pathlib import Path

import httpx
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(str(ROOT / ".env"))

from app.services.spotify_lookup import _get_token  # noqa: E402
from app.config import settings  # noqa: E402


async def main():
    name = sys.argv[1] if len(sys.argv) > 1 else "Jim Gaffigan"
    cid = settings.SPOTIFY_CLIENT_ID
    csec = settings.SPOTIFY_CLIENT_SECRET
    if not cid or not csec:
        print("NO Spotify creds resolved from settings — check app/config.py")
        return
    async with httpx.AsyncClient(timeout=10) as c:
        token = await _get_token(c, cid, csec)
        r = await c.get(
            "https://api.spotify.com/v1/search",
            params={"q": name, "type": "artist", "limit": 3},
            headers={"Authorization": f"Bearer {token}"},
        )
        print(f"=== /v1/search?q={name!r} status={r.status_code} ===")
        items = r.json().get("artists", {}).get("items") or []
        for i, a in enumerate(items):
            keys = sorted(a.keys())
            print(f"  [{i}] name={a['name']!r}  id={a['id']}  "
                  f"popularity={a.get('popularity')}  "
                  f"followers={(a.get('followers') or {}).get('total')}  "
                  f"genres={a.get('genres')}  "
                  f"keys={keys}")
        if items:
            artist_id = items[0]["id"]
            r2 = await c.get(
                f"https://api.spotify.com/v1/artists/{artist_id}",
                headers={"Authorization": f"Bearer {token}"},
            )
            print(f"\n=== /v1/artists/{artist_id} status={r2.status_code} ===")
            full = r2.json()
            print(f"  name={full.get('name')!r}  popularity={full.get('popularity')}  "
                  f"followers={(full.get('followers') or {}).get('total')}  "
                  f"genres={full.get('genres')}")


if __name__ == "__main__":
    asyncio.run(main())
