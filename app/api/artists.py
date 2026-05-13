"""Artist-level metadata endpoints.

Currently exposes the pre-computed peer list from ``artist_related``
(populated by ``scripts/compute_artist_related.py`` — see that script's
docstring for the scoring + tie-break algorithm).

Kept as its own file rather than tucked into ``suggestions.py`` so the
"related artists" feature can be enabled/disabled or rolled back
independently of the autocomplete layer.
"""
from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import ArtistRelated

router = APIRouter(prefix="/api/artists", tags=["artists"])

# Cap so callers can't ask for arbitrarily large peer sets — the table
# only stores top-20 per anchor anyway, so 50 is comfortable headroom
# in case we ever bump TOP_N in compute_artist_related.py.
MAX_LIMIT = 50


@router.get("/related")
def list_related(
    name: str = Query(..., description="Artist name (case-insensitive — matched against anchor_normalized_name)."),
    limit: int = Query(20, ge=1, le=MAX_LIMIT, description="Max peers to return."),
    db: Session = Depends(get_db),
) -> dict:
    """Return up to ``limit`` peer artists for ``name``.

    Single indexed read on ``artist_related`` keyed by
    ``anchor_normalized_name``. Returns an empty ``peers`` list (200, not
    404) when the anchor isn't classified or hasn't been scored yet —
    the frontend treats the link as a no-op in that case.
    """
    normalized = (name or "").strip().lower()
    if not normalized:
        return {"anchor": name, "count": 0, "peers": []}

    rows: List[ArtistRelated] = (
        db.query(ArtistRelated)
        .filter(ArtistRelated.anchor_normalized_name == normalized)
        .order_by(ArtistRelated.rank.asc())
        .limit(limit)
        .all()
    )

    peers = [
        {
            "rank": r.rank,
            "artist_name": r.peer_artist_name,
            "normalized_name": r.peer_normalized_name,
            "score": r.score,
            "popularity": r.peer_popularity,
        }
        for r in rows
    ]

    return {
        "anchor": name,
        "count": len(peers),
        "peers": peers,
    }
