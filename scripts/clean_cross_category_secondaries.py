"""Null out music secondaries on Comedy / Spoken Word primaries.

The brave-bridge and gemini classifiers sometimes assigned music
sub-genre tags as `secondary_1`/`secondary_2` to artists whose
primary lives in Comedy or Spoken Word (a stand-up comedian tagged
with "Hard Rock"; a poet tagged with "Punk Rock"). The filter-side
guard in `_search_filters.resolve_genre_artist_names` (f94c201)
prevents the user-visible leak, but the data is still wrong and
displays a music genre on the event row's Genre column.

This script targets the unambiguous cases only:
  primary_genre parent ∈ {Comedy, Spoken Word}
    AND any secondary_genre parent ∈ MUSIC_PARENTS
  → null out only the music secondaries; keep Comedy/Spoken Word
    secondaries intact

Theatre / Family / Dance primaries are NOT touched here — those
have legitimate music-secondary overlaps (jukebox musicals, ballets
performed to orchestral scores, etc.). Cleanup of those needs a
case-by-case review and isn't worth the recall loss.

Idempotent. Re-running on already-clean data is a no-op.

Run:
    PYTHONPATH=. python3 scripts/clean_cross_category_secondaries.py
    PYTHONPATH=. python3 scripts/clean_cross_category_secondaries.py --apply
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database import SessionLocal
from app.models.genre import ArtistGenre, GenreTaxonomy

# Parents we treat as "Music" for the cross-category check. Mirrors
# the constant used by app/api/_search_filters.py's filter-side guard
# (f94c201) so both layers agree on the boundary.
MUSIC_PARENTS = {"Rock", "Pop", "Jazz", "Hip-Hop", "Latin", "Country",
                 "Classical", "Electronic", "World"}

# Primaries whose secondaries get the strict no-music rule. Theatre /
# Family / Dance are left alone — those acts genuinely cross over.
STRICT_PRIMARY_PARENTS = {"Comedy", "Spoken Word"}


def _sub_to_parent(db) -> dict[str, str]:
    return {row.sub_genre: row.parent_genre for row in db.query(GenreTaxonomy).all()}


def run(*, apply: bool = False) -> dict:
    db = SessionLocal()
    sub_to_parent = _sub_to_parent(db)
    stats = {"rows_inspected": 0, "rows_changed": 0,
             "sec1_cleared": 0, "sec2_cleared": 0}
    samples: list[tuple[str, str, str | None, str | None, str | None, str | None]] = []
    try:
        # Limit the candidate set to rows where primary is in our strict set
        # (cheaper than scanning the whole ArtistGenre table).
        strict_subgenres = [
            row.sub_genre for row in db.query(GenreTaxonomy)
            .filter(GenreTaxonomy.parent_genre.in_(STRICT_PRIMARY_PARENTS))
            .all()
        ]
        candidates = (
            db.query(ArtistGenre)
            .filter(ArtistGenre.primary_genre.in_(strict_subgenres))
            .all()
        )
        for r in candidates:
            stats["rows_inspected"] += 1
            s1p = sub_to_parent.get(r.secondary_1)
            s2p = sub_to_parent.get(r.secondary_2)
            changed = False
            old_s1, old_s2 = r.secondary_1, r.secondary_2
            if s1p in MUSIC_PARENTS:
                r.secondary_1 = None
                stats["sec1_cleared"] += 1
                changed = True
            if s2p in MUSIC_PARENTS:
                r.secondary_2 = None
                stats["sec2_cleared"] += 1
                changed = True
            if changed:
                stats["rows_changed"] += 1
                if len(samples) < 15:
                    samples.append((r.normalized_name, r.primary_genre,
                                    old_s1, old_s2,
                                    r.secondary_1, r.secondary_2))
        if apply:
            db.commit()
        return {"stats": stats, "samples": samples}
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--apply", action="store_true",
                        help="Write changes. Without this, runs as a dry-run.")
    args = parser.parse_args()
    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"mode={mode}")
    out = run(apply=args.apply)
    s = out["stats"]
    print(f"\nresults:")
    print(f"  rows inspected (Comedy/Spoken Word primary)... {s['rows_inspected']:>5}")
    print(f"  rows changed................................. {s['rows_changed']:>5}")
    print(f"  secondary_1 cleared.......................... {s['sec1_cleared']:>5}")
    print(f"  secondary_2 cleared.......................... {s['sec2_cleared']:>5}")
    if out["samples"]:
        print(f"\nsamples (up to 15):")
        for name, primary, os1, os2, ns1, ns2 in out["samples"]:
            print(f"  {name!r:<45} primary={primary!r}")
            print(f"    sec1: {os1!r:<25} -> {ns1!r}")
            print(f"    sec2: {os2!r:<25} -> {ns2!r}")
    if not args.apply and s["rows_changed"]:
        print(f"\nRe-run with --apply to commit.")


if __name__ == "__main__":
    main()
