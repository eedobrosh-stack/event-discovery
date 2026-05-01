"""Clean the raw distinct-artist list and split it into Gemini-ready batches.

Input:  scripts/distinct_artists.txt  (one raw artist per line)
Output: scripts/artist_batches/batch_NN.txt  (500 cleaned artists per file)
        scripts/artist_batches/_dropped_sports.txt
        scripts/artist_batches/_changed.txt    (audit trail of cleanups)

Cleaning rules (kept conservative — over-cleaning risks dropping real bands):

  1. Strip surrounding ASCII double-quotes  ("שיחות"  → שיחות)
  2. Strip surrounding asterisks            (*Tia*   → Tia)
  3. Strip leading bracket-tag prefixes that scrapers prepend, e.g.
       "*SOLD OUT* Flea and the Honora" → "Flea and the Honora"
       "[CANCELLED] Sting"             → "Sting"
  4. Strip trailing tour suffixes:
       "(Touring)", "(Live)", "(Tour)", "(US Tour)", "(2026 Tour)"
  5. Collapse internal whitespace.
  6. Re-dedupe case-insensitively after cleaning.

Sports-club filter (only very obvious patterns — bands often have ambiguous
names so we err on the side of KEEPING):

  - "1. FC Magdeburg", "1. FSV Mainz", "2. Bundesliga ..." (German football
    leading-number pattern)
  - Lines starting with "FC ", "AFC ", "AC ", "VfL ", "VfB " followed by a
    capitalized place name (heuristic).

Anything filtered out is written to _dropped_sports.txt so we can spot-check
that we didn't accidentally remove legitimate bands like "AC/DC".
"""
import re
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC  = ROOT / "distinct_artists.txt"
OUT  = ROOT / "artist_batches"
OUT.mkdir(exist_ok=True)

BATCH_SIZE = 500

# ── Cleanup regexes ──────────────────────────────────────────────────────────
# Bracket-tag prefixes: "*SOLD OUT*", "[CANCELLED]", "**RESCHEDULED**", etc.
PREFIX_TAG = re.compile(r"^[\*\[]+\s*(SOLD\s*OUT|CANCELLED|RESCHEDULED|FREE|NEW|HOT|POSTPONED)\s*[\*\]]+\s+", re.IGNORECASE)
# Trailing tour/live suffixes in parens.
TRAILING_SUFFIX = re.compile(
    r"\s*\((?:Touring|Live|Tour|US\s+Tour|UK\s+Tour|World\s+Tour|\d{4}\s+Tour|2025\s+Tour|2026\s+Tour|Live\s+in\s+Concert)\)\s*$",
    re.IGNORECASE,
)

# ── Sports-club filters (keep tight) ─────────────────────────────────────────
SPORTS_PATTERNS = [
    re.compile(r"^\d+\.\s+(FC|FSV|FK|FK|SV)\s+"),   # "1. FC Magdeburg", "1. FSV Mainz"
    re.compile(r"^\d+\.\s+Bundesliga", re.IGNORECASE),
    re.compile(r"^(FC|AFC|FK|SK|VfL|VfB|TSV|MSV|FSV)\s+[A-ZÄÖÜ]"),  # "FC Bayern", "VfL Bochum"
    # Avoid catching "AC/DC", "AC Hotel" — the slash and lowercase break it.
    re.compile(r"^AC\s+(Milan|Sparta|Roma|Florentina|Fiorentina|Pisa|Monza)$", re.IGNORECASE),
]


def clean(name: str) -> str:
    s = name.strip()

    # Strip wrapping ASCII double-quotes (Hebrew-quoted names from scrapers)
    while len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        s = s[1:-1].strip()

    # Strip wrapping asterisks (*Tia* → Tia) — but not asterisks inside.
    while len(s) >= 2 and s[0] == "*" and s[-1] == "*":
        s = s[1:-1].strip()

    # Strip leading bracket-tag prefixes (*SOLD OUT*, [CANCELLED], …)
    while True:
        new = PREFIX_TAG.sub("", s)
        if new == s:
            break
        s = new

    # Strip trailing tour suffixes
    s = TRAILING_SUFFIX.sub("", s)

    # Collapse runs of whitespace
    s = re.sub(r"\s+", " ", s).strip()

    return s


def is_sports_club(name: str) -> bool:
    return any(p.search(name) for p in SPORTS_PATTERNS)


def main():
    raw = SRC.read_text(encoding="utf-8").splitlines()
    print(f"Input: {len(raw):,} raw artist lines")

    cleaned: dict[str, str] = {}     # lower → canonical
    dropped_sports: list[str] = []
    changed: list[tuple[str, str]] = []

    for original in raw:
        name = clean(original)
        if not name:
            continue
        if is_sports_club(name):
            dropped_sports.append(original)
            continue
        if name != original:
            changed.append((original, name))
        cleaned.setdefault(name.lower(), name)

    sorted_names = sorted(cleaned.values(), key=lambda s: s.lower())
    print(f"After cleaning + sports filter: {len(sorted_names):,}")
    print(f"Dropped as sports clubs: {len(dropped_sports):,}")
    print(f"Names changed by cleanup: {len(changed):,}")

    # Wipe previous batches so old runs don't linger
    for old in OUT.glob("batch_*.txt"):
        old.unlink()

    n_batches = (len(sorted_names) + BATCH_SIZE - 1) // BATCH_SIZE
    for i in range(n_batches):
        batch = sorted_names[i * BATCH_SIZE : (i + 1) * BATCH_SIZE]
        path = OUT / f"batch_{i+1:02d}.txt"
        path.write_text("\n".join(batch) + "\n", encoding="utf-8")

    (OUT / "_dropped_sports.txt").write_text("\n".join(dropped_sports) + "\n", encoding="utf-8")
    (OUT / "_changed.txt").write_text(
        "\n".join(f"{a}\t→\t{b}" for a, b in changed[:500]) + "\n",
        encoding="utf-8",
    )

    print(f"\nWrote {n_batches} batches → {OUT}/batch_NN.txt (≤{BATCH_SIZE} each)")
    print(f"Audit: _dropped_sports.txt, _changed.txt (first 500 cleanups)")


if __name__ == "__main__":
    main()
