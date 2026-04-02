"""
Second-pass categorization: refines events still tagged as generic "Concert"
by scanning the event name and artist name for genre keywords.

Only touches events currently tagged as exactly "Concert".
Does NOT override performer-matched types.

Usage:
    python3 scripts/refine_concerts.py
    python3 scripts/refine_concerts.py --dry-run
"""
import argparse
import os
import re
import sys
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database import SessionLocal
from app.models import Event, EventType

# ── keyword → event_type_name ─────────────────────────────────────────────────
# Ordered from most-specific to least-specific.
# First match wins.
RULES = [
    # Classical / Orchestral
    ("Symphony Orchestral Performances", [
        r"\borchestra\b", r"\bsymphon", r"\bphilharmon", r"\bchamber music\b",
        r"\bquartet\b", r"\bquintet\b", r"\bopera\b", r"\bconcerto\b",
        r"\bchamber orch", r"\bstring ensemble\b", r"\bchoral\b",
        r"\bjuilliard\b", r"\bcarnegie\b.*classic", r"\bclassical\b",
        r"\bmaestro\b", r"\bsinfoni",
    ]),
    # Electronic / DJ
    ("Electronic / DJ Set", [
        r"\bedm\b", r"\belectronic\b", r"\bhouse music\b", r"\btechno\b",
        r"\bdrums?\s*&\s*bass\b", r"\bd&b\b", r"\bdubstep\b", r"\bsynth\b",
        r"\bdj\s+set\b", r"\brave\b", r"\bnight\s*club\b", r"\bdisco\b",
        r"\bdance\s*music\b", r"\belectro\b", r"\btrance\b", r"\bambient\b",
        r"\bsynthony\b", r"\b808\b", r"\bbass\s+music\b",
    ]),
    # Jazz
    ("Jazz Concert", [
        r"\bjazz\b", r"\bblues\b", r"\bswing\b", r"\bbebop\b", r"\bfusion\b",
        r"\bsoul\s*jazz\b", r"\bsmooth\s*jazz\b", r"\bbig\s*band\b",
        r"\btrio\b.*jazz", r"\bjazz\s*club\b",
    ]),
    # Hip-Hop / Rap
    ("Hip-Hop / Rap Concert", [
        r"\bhip.?hop\b", r"\brap\b", r"\btrapp?\b", r"\bgrime\b",
        r"\br&b\s*&\s*hip", r"\bfreestyle\b", r"\blyricist\b",
    ]),
    # Rock
    ("Rock Concert", [
        r"\brock\b", r"\bmetal\b", r"\bpunk\b", r"\bgrunge\b", r"\bindier?\b",
        r"\balternative\b", r"\bemo\b", r"\bhardcore\b", r"\bpost.?rock\b",
        r"\bpsychedelic\b", r"\bstoner\b", r"\bprog(ressive)?\b",
        r"\bfolkrock\b", r"\bblues.?rock\b",
    ]),
    # Pop
    ("Pop Concert", [
        r"\bpop\b", r"\bpop\s*music\b", r"\bindipop\b", r"\bdream\s*pop\b",
        r"\bsynth.?pop\b", r"\bpower\s*pop\b", r"\bk-pop\b", r"\bj-pop\b",
        r"\b90s\b", r"\b2000s\b", r"\b00s\b", r"\bnineties\b",
    ]),
    # R&B / Soul
    ("R&B / Soul Concert", [
        r"\br&b\b", r"\bsoul\b", r"\bfunk\b", r"\bmotown\b", r"\bgospel\b",
        r"\bnew\s*jack\b", r"\bneo.?soul\b",
    ]),
    # Country
    ("Country Concert", [
        r"\bcountry\b", r"\bbluegrass\b", r"\bamericana\b", r"\bfiddl",
        r"\bnashville\b", r"\bwestern\s*swing\b", r"\balt.?country\b",
    ]),
    # Latin
    ("Latin Concert", [
        r"\blatin\b", r"\bsalsa\b", r"\bregaeton\b", r"\bflamenco\b",
        r"\bbachata\b", r"\bcumbia\b", r"\btango\b", r"\bbolero\b",
        r"\bmerengue\b", r"\bbossa\s*nova\b",
    ]),
    # Reggae
    ("Reggae / Calypso Concert", [
        r"\breggae\b", r"\bdancehall\b", r"\bska\b", r"\bcalypso\b",
        r"\bafrobeat\b", r"\bgospel\s*reggae\b",
    ]),
    # Folk / Acoustic — no dedicated type, keep as Concert
    # (skipped — would need "Folk Concert" type added to DB)
    # Comedy (catch misclassified comedy events)
    ("Comedy Club Headliners", [
        r"\bstandup\b", r"\bstand.?up\s*com", r"\bcomedy\s*(show|night|tour)\b",
        r"\bcomedi[an]", r"\bimprov\b", r"\bsketch\s*com",
    ]),
    # Dance / Ballet
    ("Ballet Performance", [
        r"\bballet\b", r"\bdance\s*show\b", r"\bdance\s*perform",
        r"\bcontemporary\s*dance\b", r"\bchoreograph",
    ]),
    # Theatre / Broadway / Musicals
    ("Play / Drama", [
        r"\btheater\b", r"\btheatre\b", r"\bplay\b.*stage",
        r"\bbroadway\b", r"\boff.?broadway\b", r"\bmusical\b",
        r"\bdrama\b", r"\bmonologue\b",
        r"\bstranger things\b", r"\bhamilton\b", r"\bwicked\b",
        r"\blion king\b", r"\bphantom of the opera\b", r"\bshen yun\b",
        r"\bshadow.*first\b", r"\btwo strangers\b",
    ]),
    # Art / Museum / Exhibition
    ("Special Museum Exhibitions", [
        r"\bmuseum\b", r"\bexhibit(ion)?\b", r"\binstallation\b",
        r"\bmoca\b", r"\bmoma\b", r"\bsmithsonian\b", r"\bmetropolitan\b",
        r"\btour experience\b", r"\bbuilding tour\b",
        r"\btussauds\b", r"\bwax\s*museum\b",
        r"\bprehistoric\b", r"\bdinosaur\b", r"\bplanetarium\b",
        r"\billusion\s*museum\b", r"\btwist\s*museum\b",
        r"\bnatural\s*history\b", r"\bscience\s*museum\b",
        r"\bgift\s*card\s*redemption\b",
    ]),
    # Gallery / Banksy-style pop-up art
    ("Commercial Gallery Exhibitions", [
        r"\bgallery\b", r"\bartwork\b", r"\bart show\b",
        r"\bbanksy\b", r"\bpop.?up\s*art\b",
    ]),
    # Craft / Workshop
    ("Artisan Events", [
        r"\bcrafting\b", r"\bcraft\s*(class|workshop|fair|session)\b",
        r"\bworkshop\b", r"\bpottery\b", r"\bpainting\s*class\b",
        r"\bceramic\b", r"\bknitting\b", r"\bsewing\b",
    ]),
    # Kids / Family Entertainment — map to nearest existing type
    ("Interactive Art Installations", [
        r"\bbubble show\b", r"\bgazillion\b",
        r"\bkids\s*(show|concert|event)\b", r"\bfamily\s*(show|event|fun)\b",
        r"\bpuppet\b",
    ]),
    # Tourist attractions / landmark experiences
    ("Theme Park Nighttime Shows", [
        r"\blondon\s*eye\b", r"\bferris\s*wheel\b",
        r"\btower\s*of\s*london\b", r"\bbuckingham\s*palace\b",
        r"\bshard\b.*view", r"\bskygarden\b", r"\bobservation\s*deck\b",
        r"\battraction\b.*ticket", r"\bstandard\s*entr(y|ance)\b",
        r"\bdisney\s*on\s*ice\b", r"\bice\s*show\b",
    ]),
]

# Pre-compile all patterns
COMPILED_RULES = [
    (type_name, [re.compile(p, re.IGNORECASE) for p in patterns])
    for type_name, patterns in RULES
]


def classify(text: str) -> Optional[str]:
    """Return first matching event_type_name, or None."""
    for type_name, patterns in COMPILED_RULES:
        for pat in patterns:
            if pat.search(text):
                return type_name
    return None


def run(dry_run: bool = False):
    db = SessionLocal()
    try:
        et_by_name = {et.name: et for et in db.query(EventType).all()}
        concert_et = et_by_name.get("Concert")
        if not concert_et:
            print("ERROR: 'Concert' event type not found in DB")
            return

        # Only grab events currently tagged as generic "Concert"
        generic_concerts = [
            e for e in (
                db.query(Event)
                .join(Event.event_types)
                .filter(EventType.name == "Concert")
                .all()
            )
            if len(e.event_types) == 1 and e.event_types[0].name == "Concert"
        ]

        print(f"Generic 'Concert' events to refine: {len(generic_concerts):,}")

        updated = 0
        unchanged = 0
        breakdown: dict[str, int] = {}

        for event in generic_concerts:
            combined = " ".join(filter(None, [
                event.name or "",
                event.artist_name or "",
                event.description or "",
            ]))

            new_type_name = classify(combined)

            if not new_type_name:
                unchanged += 1
                continue

            new_et = et_by_name.get(new_type_name)
            if not new_et:
                unchanged += 1
                continue

            if not dry_run:
                event.event_types = [new_et]
            updated += 1
            breakdown[new_type_name] = breakdown.get(new_type_name, 0) + 1

        if not dry_run:
            db.commit()

        print(f"\nResults:")
        print(f"  Refined:   {updated:,}")
        print(f"  Unchanged: {unchanged:,}  (no keyword match)")
        if breakdown:
            print(f"\nBreakdown of refined types:")
            for t, n in sorted(breakdown.items(), key=lambda x: -x[1]):
                print(f"  {t:<40} {n:>6,}")

        if dry_run:
            print("\n[DRY RUN — nothing committed]")

    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview changes without committing")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
