"""Export artist_genre + genre_taxonomy from the local DB to a seed bundle.

Run this whenever the local classifications change so prod's startup seed
(in app/main.py) picks up the new data on next deploy.

Output:
    app/seed/artist_classifications.json.gz

Format:
    {
      "version": "<ISO timestamp of dump>",
      "taxonomy": [{"sub_genre": "...", "parent_genre": "..."}, ...],
      "artists":  [{"artist_name": "...", "normalized_name": "...",
                    "primary_genre": "...", "secondary_1": "..."|null,
                    "secondary_2": "..."|null, "confidence": "high|medium|low"}, ...]
    }

Why gzip + JSON: the raw 15K-row dump is ~2 MB. Gzipped it's ~350 KB —
small enough to ship in git, simple enough that any consumer (the startup
seed, smoke tests, anyone curious) can read it without a DB.

Idempotent: rerunning produces a new bundle but the prod seed dedupes
by primary key (sub_genre / normalized_name) so reseeding never duplicates.
"""
from __future__ import annotations

import gzip
import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.database import SessionLocal  # noqa: E402
from app.models.genre import ArtistGenre, GenreTaxonomy  # noqa: E402

OUT_PATH = ROOT / "app" / "seed" / "artist_classifications.json.gz"


def main() -> int:
    db = SessionLocal()
    try:
        taxonomy = [
            {"sub_genre": r.sub_genre, "parent_genre": r.parent_genre}
            for r in db.query(GenreTaxonomy).order_by(GenreTaxonomy.parent_genre,
                                                     GenreTaxonomy.sub_genre).all()
        ]
        artists = [
            {
                "artist_name": r.artist_name,
                "normalized_name": r.normalized_name,
                "primary_genre": r.primary_genre,
                "secondary_1": r.secondary_1,
                "secondary_2": r.secondary_2,
                "confidence": r.confidence,
            }
            for r in db.query(ArtistGenre).order_by(ArtistGenre.normalized_name).all()
        ]
    finally:
        db.close()

    payload = {
        "version": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "taxonomy": taxonomy,
        "artists": artists,
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    raw_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    with gzip.open(OUT_PATH, "wb", compresslevel=9) as f:
        f.write(raw_json)

    size_kb = OUT_PATH.stat().st_size / 1024
    print(f"✓ wrote {OUT_PATH.relative_to(ROOT)}")
    print(f"  taxonomy rows: {len(taxonomy):,}")
    print(f"  artist rows:   {len(artists):,}")
    print(f"  raw JSON:      {len(raw_json)/1024:,.1f} KB")
    print(f"  gzipped:       {size_kb:,.1f} KB")
    print(f"  version:       {payload['version']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
