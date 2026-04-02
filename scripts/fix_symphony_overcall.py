"""
Corrects events incorrectly tagged as "Symphony Orchestral Performances"
by scanning for keywords that indicate a different type.

Run after recategorize_from_performers.py and refine_concerts.py.

Usage:
    python3 scripts/fix_symphony_overcall.py
    python3 scripts/fix_symphony_overcall.py --dry-run
"""
import argparse
import os
import re
import sys
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database import SessionLocal
from app.models import Event, EventType

# Rules: if event text matches → reclassify away from Symphony
# Only applied to events currently tagged Symphony Orchestral Performances
# Ordered most-specific first
OVERRIDE_RULES = [
    # Phantom of the Opera and similar named musicals
    ("Art", "Play / Drama", [
        r"\bphantom of the opera\b", r"\bbroadway\b", r"\bmusical\b",
        r"\boff.?broadway\b", r"\btheatre\b", r"\btheater\b",
        r"\bwicked\b", r"\bhamilton\b", r"\blion king\b",
        r"\bshen yun\b", r"\bstomp\b",
    ]),
    # Funk / Jazz orchestras — have "orchestra" in name but not classical
    ("Music", "Jazz Concert", [
        r"\bjazz\b", r"\bfunk\b.*\borchest", r"\borchest.*\bfunk\b",
        r"\bbig\s*band\b", r"\bswing\b", r"\bblues\b.*\borchest",
        r"\bjazz.*\bquartet\b", r"\bjazz.*\bquintet\b", r"\bjazz.*\btrio\b",
        r"\btribute.*jazz\b", r"\bjazz.*tribute\b",
        r"\bgerry\s*mulligan\b", r"\bmingus\b", r"\bellington\b",
    ]),
    # Electronic / dance events misclassified
    ("Music", "Electronic / DJ Set", [
        r"\bballet\b.*electronic", r"\brave\b", r"\bedm\b",
        r"\bhouse\s*music\b", r"\btechno\b",
        r"\belectronic.*dance\b", r"\bdance.*party\b", r"\bindie\s*dance\b",
    ]),
    # Rock / indie bands misclassified (have band/orchestra in name)
    ("Music", "Rock Concert", [
        r"\brock\b", r"\bindier?\b", r"\bpunk\b", r"\bmetal\b",
        r"\balternative\b", r"\bemo\b", r"\bgrunge\b",
    ]),
    # Soul / R&B
    ("Music", "R&B / Soul Concert", [
        r"\bsoul\b", r"\br&b\b", r"\bfunk\b", r"\bmotown\b",
        r"\bteddy\s*pendergrass\b", r"\bgospel\b",
    ]),
    # Pop / indie pop
    ("Music", "Pop Concert", [
        r"\bpop\b", r"\bindipop\b", r"\bsynth.?pop\b",
    ]),
    # Country
    ("Music", "Country Concert", [
        r"\bcountry\b", r"\bbluegrass\b", r"\bamericana\b",
    ]),
    # Latin
    ("Music", "Latin Concert", [
        r"\blatin\b", r"\bsalsa\b", r"\btango\b", r"\bflamenco\b",
    ]),
    # Comedy
    ("Comedy", "Comedy Club Headliners", [
        r"\bcomedy\b", r"\bstand.?up\b", r"\bcomedi[an]\b", r"\bimprov\b",
    ]),
]

COMPILED_OVERRIDES = [
    (cat, typ, [re.compile(p, re.IGNORECASE) for p in pats])
    for cat, typ, pats in OVERRIDE_RULES
]

# Patterns that CONFIRM it really is a symphony/classical event
# If any of these match, leave it alone
CONFIRM_SYMPHONY = [re.compile(p, re.IGNORECASE) for p in [
    r"\bsymphon\b", r"\bphilharmon\b", r"\bchamber\s*orchestra\b",
    r"\bstring\s*(quartet|ensemble)\b", r"\bconcerto\b",
    r"\boverture\b", r"\bopus\b", r"\bbeethoven\b", r"\bmozart\b",
    r"\bbach\b", r"\bchopins?\b", r"\bschubert\b", r"\bbrahms\b",
    r"\bvivaldi\b", r"\bhandel\b", r"\btchaikovsky\b",
    r"\bby candlelight\b", r"\bclassical\s*music\b",
    r"\bneoclassical\b", r"\boratorio\b", r"\bcantata\b",
    r"\bchamber\s*music\b", r"\bchamber\s*society\b",
]]


def is_confirmed_symphony(text: str) -> bool:
    return any(p.search(text) for p in CONFIRM_SYMPHONY)


def classify_override(text: str) -> Optional[tuple]:
    for cat, typ, patterns in COMPILED_OVERRIDES:
        for pat in patterns:
            if pat.search(text):
                return cat, typ
    return None


def run(dry_run: bool = False):
    db = SessionLocal()
    try:
        et_by_name = {et.name: et for et in db.query(EventType).all()}
        symphony_et = et_by_name.get("Symphony Orchestral Performances")
        if not symphony_et:
            print("ERROR: Symphony Orchestral Performances type not found")
            return

        # Only events currently tagged Symphony
        symphony_events = [
            e for e in (
                db.query(Event)
                .join(Event.event_types)
                .filter(EventType.name == "Symphony Orchestral Performances")
                .all()
            )
            if len(e.event_types) == 1
            and e.event_types[0].name == "Symphony Orchestral Performances"
        ]

        print(f"Events tagged Symphony Orchestral: {len(symphony_events):,}")

        updated = 0
        kept = 0
        breakdown: dict[str, int] = {}

        for event in symphony_events:
            text = " ".join(filter(None, [
                event.name or "",
                event.artist_name or "",
                event.description or "",
                event.venue_name or "",
            ]))

            # If text confirms it's really classical, leave it
            if is_confirmed_symphony(text):
                kept += 1
                continue

            # Otherwise try to reclassify
            result = classify_override(text)
            if not result:
                kept += 1
                continue

            cat, type_name = result
            new_et = et_by_name.get(type_name)
            if not new_et:
                kept += 1
                continue

            if not dry_run:
                event.event_types = [new_et]
            updated += 1
            breakdown[type_name] = breakdown.get(type_name, 0) + 1

        if not dry_run:
            db.commit()

        print(f"Reclassified: {updated:,}  |  Kept as Symphony: {kept:,}")
        if breakdown:
            print("\nReclassified into:")
            for t, n in sorted(breakdown.items(), key=lambda x: -x[1]):
                print(f"  {t:<40} {n:>5,}")

        if dry_run:
            print("\n[DRY RUN — nothing committed]")

    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
