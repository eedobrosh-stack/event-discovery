"""Compute the ``artist_related`` peer table from ``artist_genre`` + ``performers``.

For each classified anchor artist, picks the top-20 most-similar peer
artists by genre overlap, breaking ties with internally-derived popularity
so that famous peers surface ahead of obscure ones with identical genre
bags.

Scoring (per anchor A vs candidate B):

  - +2 if ``B.primary_genre == A.primary_genre``
  - +1 per genre in ``A.bag ∩ B.bag`` not already counted above (where
    each artist's "bag" is the set of non-null values across
    ``primary_genre``, ``secondary_1``, ``secondary_2``)
  - +1 extra if ``B.primary_genre`` is in ``A.bag`` but
    ``B.primary_genre != A.primary_genre`` — rewards primary-slot match
    from either side, so ``primary=Soft Rock, secondary=Pop Rock`` peers
    of Sting (``primary=Pop Rock, secondary=Soft Rock``) score the same
    as ``primary=Pop Rock, secondary=Soft Rock`` peers.
  - −0.5 per genre in ``B.bag`` not in ``A.bag`` — the "tightness"
    penalty. Without it, an artist tagged ``{Pop Rock, Soft Rock, Hard
    Rock}`` would tie with one tagged exactly ``{Pop Rock, Soft Rock}``
    despite being noisier.

Filters (the candidate pool):

  - ``source == "gemini"`` AND ``confidence == "high"`` AND
    ``primary_genre != "UNKNOWN"`` — keep only confident classifications.
  - Drop tribute acts (``"Tribute Show"`` in any slot) and obvious
    non-artist placeholder names (lowercase-leading, or containing
    "tribute" / "the music of"). Same noise filter used in the dry-run
    that produced ``data/artist_related_top20.csv``.

Tie-break (when raw scores match — common for sparse-bag anchors like
Sting whose ``{Pop Rock, Soft Rock}`` produces ~23 candidates tied at
the top score):

  1. ``Performer.derived_popularity DESC`` (NULLS LAST). Picked over
     raw upcoming-event count because it's already a multi-signal score
     (upcoming events + past events + distinct cities + ticket price +
     Brave footprint) computed weekly by ``recompute_popularity.py``.
  2. Anchor's row id ASC, as a final deterministic fallback.

Output:

  - **Dry-run (default)** — prints summary stats + writes a CSV preview
    to ``data/artist_related_<ts>.csv`` (anchor → top-20 peers, with
    rank/score/popularity). No DB writes. Same shape as the original
    dry-run we eyeballed at ``~/Downloads/artist_related_top20.csv``.
  - **--apply** — per-anchor ``DELETE FROM artist_related WHERE
    anchor_normalized_name = ?``, then bulk insert top-20 rows.
    Idempotent (each anchor's slate is rebuilt from scratch every run).
    SQLite-compatible; no Postgres-only syntax.

The whole compute fits comfortably in memory — ~10k anchors × ~750 avg
candidates per anchor, scoring is pure Python after a single load —
so we don't bother with chunked SQL. Total runtime ~30s on the local
SQLite, ~1m on Render.

Usage:
    PYTHONPATH=. python3 scripts/compute_artist_related.py
    PYTHONPATH=. python3 scripts/compute_artist_related.py --apply
    PYTHONPATH=. python3 scripts/compute_artist_related.py --anchor Sting   # debug single artist
"""
from __future__ import annotations

import argparse
import csv
import logging
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(str(ROOT / ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("compute_artist_related")

from sqlalchemy import text  # noqa: E402

from app.database import SessionLocal  # noqa: E402
from app.models import ArtistGenre, ArtistRelated, Performer  # noqa: E402

# Scoring constants — see module docstring for derivation.
PRIMARY_MATCH_PTS = 2
SECONDARY_MATCH_PTS = 1
CROSS_PRIMARY_BONUS = 1     # B.primary ∈ A.bag but B.primary ≠ A.primary
TIGHTNESS_PENALTY = 0.5     # per extra genre in B.bag not in A.bag

TOP_N = 20


def is_noise(a: ArtistGenre) -> bool:
    """Drop rows that are tribute acts or look like non-artist placeholders."""
    if "Tribute Show" in (a.primary_genre, a.secondary_1, a.secondary_2):
        return True
    name = a.artist_name or ""
    if not name or name[0].islower():
        return True
    low = name.lower()
    return "tribute" in low or "the music of" in low


def bag(a: ArtistGenre) -> set[str]:
    return {g for g in (a.primary_genre, a.secondary_1, a.secondary_2) if g}


def score_pair(anchor: ArtistGenre, anchor_bag: set[str], cand: ArtistGenre) -> float:
    cand_bag = bag(cand)
    s = 0.0
    if cand.primary_genre == anchor.primary_genre:
        s += PRIMARY_MATCH_PTS
        remaining = anchor_bag - {anchor.primary_genre}
    else:
        remaining = anchor_bag
    s += SECONDARY_MATCH_PTS * len(remaining & cand_bag)
    if cand.primary_genre != anchor.primary_genre and cand.primary_genre in anchor_bag:
        s += CROSS_PRIMARY_BONUS
    s -= TIGHTNESS_PENALTY * len(cand_bag - anchor_bag)
    return s


def load_artists(db) -> list[ArtistGenre]:
    """Pool: gemini, high-confidence, recognized primary, not noise."""
    rows = (
        db.query(ArtistGenre)
        .filter(
            ArtistGenre.source == "gemini",
            ArtistGenre.confidence == "high",
            ArtistGenre.primary_genre != "UNKNOWN",
        )
        .all()
    )
    return [a for a in rows if not is_noise(a)]


def load_popularity(db) -> dict[str, int]:
    """Map normalized_name → derived_popularity for fast tie-break lookup."""
    rows = (
        db.query(Performer.normalized_name, Performer.derived_popularity)
        .filter(Performer.derived_popularity.isnot(None))
        .all()
    )
    return {n: p for n, p in rows}


def build_inverted_index(artists: list[ArtistGenre]) -> dict[str, list[ArtistGenre]]:
    """genre → list of artists carrying that genre anywhere in their bag.

    Lets us gather candidate sets per anchor by union over the anchor's
    genres, avoiding an O(N²) all-pairs scan.
    """
    idx: dict[str, list[ArtistGenre]] = defaultdict(list)
    for a in artists:
        for g in bag(a):
            idx[g].append(a)
    return idx


def compute_peers(
    anchor: ArtistGenre,
    inv_idx: dict[str, list[ArtistGenre]],
    pop: dict[str, int],
    top_n: int = TOP_N,
) -> list[tuple[float, int | None, ArtistGenre]]:
    """Return up to ``top_n`` peers as (score, popularity, candidate)."""
    anchor_bag = bag(anchor)
    cand_map: dict[int, ArtistGenre] = {}
    for g in anchor_bag:
        for c in inv_idx.get(g, []):
            if c.id == anchor.id:
                continue
            cand_map[c.id] = c

    scored: list[tuple[float, int | None, ArtistGenre]] = []
    for c in cand_map.values():
        s = score_pair(anchor, anchor_bag, c)
        if s <= 0:
            continue
        scored.append((s, pop.get(c.normalized_name), c))

    # Sort: score DESC, popularity DESC (None last), id ASC (deterministic).
    # In Python, None can't be compared with int — coerce None to -1 so it
    # ranks below every real popularity value.
    scored.sort(key=lambda t: (-t[0], -(t[1] if t[1] is not None else -1), t[2].id))
    return scored[:top_n]


def write_csv(rows: list[dict], out_path: Path) -> None:
    fields = [
        "anchor_artist", "anchor_primary", "anchor_sec1", "anchor_sec2",
        "rank", "peer_artist", "peer_primary", "peer_sec1", "peer_sec2",
        "score", "peer_popularity",
    ]
    with out_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--apply", action="store_true",
                   help="Write to artist_related table. Default: dry-run + CSV preview.")
    p.add_argument("--anchor", type=str, default=None,
                   help="Compute peers for a single anchor (by normalized_name or artist_name). Implies --no-csv unless --apply.")
    p.add_argument("--top-n", type=int, default=TOP_N,
                   help=f"Peers per anchor (default {TOP_N}).")
    p.add_argument("--csv-dir", type=str, default=str(ROOT / "data"),
                   help="Directory for the dry-run CSV preview.")
    args = p.parse_args()

    db = SessionLocal()
    try:
        log.info("Loading classified artist pool …")
        artists = load_artists(db)
        log.info("  pool size after noise filter: %d", len(artists))

        log.info("Loading Performer.derived_popularity for tie-break …")
        pop = load_popularity(db)
        log.info("  performers with popularity: %d", len(pop))

        log.info("Building inverted genre index …")
        inv_idx = build_inverted_index(artists)
        log.info("  distinct genres indexed: %d", len(inv_idx))

        # Anchor scoping for debug runs.
        if args.anchor:
            needle = args.anchor.strip().lower()
            anchors = [a for a in artists
                       if a.normalized_name == needle or (a.artist_name and a.artist_name.lower() == needle)]
            if not anchors:
                log.error("Anchor %r not found in pool.", args.anchor)
                return 1
            log.info("Scoped to single anchor: %s", anchors[0].artist_name)
        else:
            anchors = artists

        # Compute + collect.
        log.info("Scoring %d anchor(s) (top %d each) …", len(anchors), args.top_n)
        all_rows: list[dict] = []
        write_payload: list[dict] = []
        for anchor in anchors:
            peers = compute_peers(anchor, inv_idx, pop, top_n=args.top_n)
            for rank, (score, peer_pop, cand) in enumerate(peers, start=1):
                all_rows.append({
                    "anchor_artist": anchor.artist_name,
                    "anchor_primary": anchor.primary_genre,
                    "anchor_sec1": anchor.secondary_1 or "",
                    "anchor_sec2": anchor.secondary_2 or "",
                    "rank": rank,
                    "peer_artist": cand.artist_name,
                    "peer_primary": cand.primary_genre,
                    "peer_sec1": cand.secondary_1 or "",
                    "peer_sec2": cand.secondary_2 or "",
                    "score": f"{score:.1f}",
                    "peer_popularity": "" if peer_pop is None else peer_pop,
                })
                write_payload.append({
                    "anchor_normalized_name": anchor.normalized_name,
                    "peer_normalized_name": cand.normalized_name,
                    "peer_artist_name": cand.artist_name,
                    "rank": rank,
                    "score": score,
                    "peer_popularity": peer_pop,
                })

        log.info("Total peer rows: %d (avg %.1f per anchor)",
                 len(all_rows), len(all_rows) / max(len(anchors), 1))

        if args.apply:
            log.info("APPLYING — writing to artist_related …")
            anchor_keys = list({r["anchor_normalized_name"] for r in write_payload})
            # Delete-by-anchor in chunks (SQLite IN-clause has a default
            # 999-param limit; 500 is comfortably under).
            CHUNK = 500
            for i in range(0, len(anchor_keys), CHUNK):
                chunk = anchor_keys[i:i + CHUNK]
                db.execute(
                    text(
                        "DELETE FROM artist_related "
                        "WHERE anchor_normalized_name IN ("
                        + ",".join(f":k{j}" for j in range(len(chunk)))
                        + ")"
                    ),
                    {f"k{j}": k for j, k in enumerate(chunk)},
                )
            db.commit()
            log.info("  cleared %d anchor slates", len(anchor_keys))

            db.bulk_insert_mappings(ArtistRelated, write_payload)
            db.commit()
            log.info("  inserted %d peer rows", len(write_payload))
        else:
            ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            csv_dir = Path(args.csv_dir)
            csv_dir.mkdir(parents=True, exist_ok=True)
            out_path = csv_dir / f"artist_related_{ts}.csv"
            write_csv(all_rows, out_path)
            log.info("Dry-run CSV → %s", out_path)
            log.info("Re-run with --apply to write to artist_related.")

        return 0
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
