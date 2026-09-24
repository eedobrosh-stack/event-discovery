"""Israeli artist_name hygiene: performer vs show / event title.

About half of the Israeli values collectors put in ``events.artist_name``
are show titles ("אור לגויים - תיאטרון בית ליסין", "אליסה בארץ הפלאות –
חנוכה 2026"). Rule (Eedo, 2026-09-25): more than two words, a year, title
punctuation or an event word = an event name, not an artist. A performer
the title clearly names ("דרור קרן במופע סטנדאפ", "סינדרלה - בכיכובה של
רינת גבאי") is kept as the artist; otherwise the field is cleared.
Manual review marks and merged spellings live in artist_overrides.json.

``clean_israeli_artist`` is applied at ingest for Israeli cities
(CollectorRegistry._save_events) and to existing rows by
scripts/clean_israel_artist_names.py.
"""
from __future__ import annotations

import re

# Words that make a string a show / event title, not a performer.
EVENT_WORDS = {
    # Hebrew
    "תיאטרון", "תאטרון", "הצגה", "הצגת", "מופע", "מופעי", "המופע", "חנוכה", "פסח", "סוכות", "פורים", "קיץ", "חורף",
    "סיור", "סיורי", "הרצאה", "הרצאות", "ערב", "מסיבה", "מסיבת", "פסטיבל", "פסטיבלי", "סדנה", "סדנת", "קונצרט", "קונצרטים",
    "בכיכובה", "בכיכובו", "בכיכובם", "מחזמר", "הופעה", "הופעת", "טקס", "כנס", "יריד", "תערוכה", "תערוכת", "סטנדאפ",
    "לילדים", "משפחתי", "משפחתית", "חגיגה", "חגיגת", "מחווה", "ספיישל", "אורח", "אורחת", "אורחים", "השקה", "השקת",
    "סדרת", "סדרה", "מנוי", "הקרנה", "הקרנת", "סרט", "טורניר", "משחק", "מרוץ", "הפנינג", "שבוע", "יום", "לילה", "בוקר",
    "אירוע", "קבלת", "פתיחת", "נעילת", "עונת", "חוויה", "חוויית", "ביום", "בערב", "למבוגרים", "להורים", "פעילויות", "פעילות", "צלילי", "מסיבות", "דיג׳ייז", "דיג'ייז", "דיג’ייז",
    # English
    "theatre", "theater", "show", "tour", "festival", "party", "night", "live", "tribute", "concert", "concerts",
    "presents", "featuring", "feat", "ft", "experience", "edition", "season", "workshop", "lecture", "screening",
    "musical", "opening", "closing", "celebration", "special", "vs", "versus", "meets", "plays", "sings", "evening",
    "candlelight", "gala", "premiere", "launch", "series", "day", "weekend", "tickets", "event", "the musical",
}
SEPARATORS = re.compile(r"\s[-–—|:]\s|[|:–—]|\(|\)|\"|״|!|\?|/")
YEAR = re.compile(r"\b(19|20)\d{2}\b")


# Honorifics: a lecturer is a person ("פרופ' שלמה בידרמן" is 2 words).
HONORIFIC = re.compile(r"^(?:(?:הרב|הרבנית|ד\"?ר|ד״ר|דר'|פרופ'?|פרופסור|פרופ׳|מאסטרו|dr\.?|prof\.?|rabbi|maestro)\s+)+", re.IGNORECASE)
# "X במופע …", "X מארח …", "X - מופע חדש", "… - X": a performer inside a title.
LEAD_VERBS = re.compile(r"^(.+?)\s+(?:במופע|בהופעה|בהופעת|מארח|מארחת|מארחים|עושה|שר|שרה|שרים|מגיש|מגישה|מציג|מציגה|בערב|בסטנדאפ|live|in concert)\b", re.IGNORECASE)
SPLIT = re.compile(r"\s*[-–—|:]\s+|\s+[-–—|:]\s*")
GENERIC_PARTS = re.compile(r"(תיאטרון|תאטרון|הבימה|הקאמרי|ליסין|מופע|הצגה|להקת|מבית|אולם)", re.IGNORECASE)


PERF_WORDS = {"מופע", "מופעה", "במופע", "מגיע", "מגיעה", "מגיעים", "מחווה", "סטנדאפ", "סטנד", "הופעה", "בהופעה",
              "live", "tribute", "tour", "trio", "טריו", "קונצרט", "השקת", "אלבום", "הופעת", "באולם", "in", "concert", "show"}
PLAY_WORDS = re.compile(r"(קרקס|circus|^from\b|^the\b|תיאטרון|תאטרון|הצגה|הצגות|קומדיה|עיבוד|מחזה|מחזמר|בימתי|להקת המחול|הבימה|הקאמרי|ליסין|גשר|סיפור)")
STARRING = re.compile(r"בכיכוב(?:ה|ו|ם|ן)?\s+של\s+([^,|–—\-:!]+)")


def _clean(x: str) -> str:
    x = re.sub(r"&#0?39;|&quot;", "'", x)
    x = x.strip(" *'\"״!.-–—")
    return HONORIFIC.sub("", x).strip()


def performer_in_title(name: str, is_artist) -> str | None:
    """The performer a show title names, when it clearly names one."""
    n = re.sub(r"&#0?39;|&quot;", "'", name).strip()
    m = STARRING.search(n)
    if m:
        # "בכיכובה של רינת גבאי קיץ 2026": drop a trailing season / year
        who = re.sub(r"(\s+(?:קיץ|חורף|סתיו|אביב|חנוכה|פסח|סוכות|(?:19|20)\d{2}))+\s*$", "", m.group(1).strip())
        if is_artist(_clean(who)):
            return _clean(who)
    m = LEAD_VERBS.match(n)
    if m and is_artist(_clean(m.group(1))) and not PLAY_WORDS.search(m.group(1)):
        return _clean(m.group(1))
    parts = [x for x in SPLIT.split(n) if x.strip(" !*")]
    if len(parts) >= 2:
        first, rest = parts[0], " ".join(parts[1:])
        rest_toks = {t.lower().strip("!.,") for t in words(rest)}
        lecture = bool(HONORIFIC.match(first.strip(" *")))
        if (lecture or rest_toks & PERF_WORDS) and not PLAY_WORDS.search(rest) and not PLAY_WORDS.search(first.lower()):
            cand = _clean(first)
            if cand and is_artist(cand):
                return cand
    return None


def words(name: str) -> list[str]:
    return [w for w in re.split(r"[\s,]+", name.strip()) if w]


def classify(name: str, *, known_performer: bool = False) -> tuple[str, str]:
    """('artist' | 'event', reason)."""
    n = HONORIFIC.sub("", name.strip())
    toks = [t.lower().strip(".,'’׳\"") for t in words(n)]
    toks_he = {re.sub(r"^[ובלהמש](?=[֐-׿]{3,})", "", t) for t in toks}  # drop one-letter Hebrew prefixes
    kw = (set(toks) | toks_he) & EVENT_WORDS
    if YEAR.search(n):
        return "event", "contains a year"
    if kw:
        return "event", f"event word: {sorted(kw)[0]}"
    if SEPARATORS.search(n):
        return ("artist", "separator, but known performer") if known_performer and len(toks) <= 4 else ("event", "title punctuation")
    if len(toks) > 2:
        if known_performer and len(toks) <= 4:
            return "artist", "3–4 words, known performer (MusicBrainz/Spotify)"
        return "event", "more than 2 words"
    return "artist", "1–2 words"


# ── overrides + the one entry point ───────────────────────────────────
import html as _html
import json as _json
from pathlib import Path as _Path

_OV = _json.loads((_Path(__file__).with_name("artist_overrides.json")).read_text())
NOT_ARTISTS = {_html.unescape(x).strip().lower() for x in _OV["not_artists"]}
DUPLICATES = {_html.unescape(k).strip().lower(): _html.unescape(v).strip() for k, v in _OV["duplicates"].items()}
SHOW_SUBGENRES = {"Musical Theatre", "Cirque", "Educational"}
KEEP_DESPITE_GENRE = {"voca people", "ווקה פיפל"}


def clean_israeli_artist(value: str | None, *, known_performer=lambda n: False,
                         sub_genre=lambda n: None) -> str | None:
    """The artist_name an Israeli event should carry: the performer, or
    None when the value is a show / event title naming no one.

    ``known_performer(name)`` → bool (MusicBrainz / Spotify match) and
    ``sub_genre(name)`` → artist_genre.primary_genre are injected so this
    module stays DB-free and testable.
    """
    if not value or not value.strip():
        return value
    v = _html.unescape(value).strip()
    key = v.lower()
    if key in DUPLICATES:
        return DUPLICATES[key]
    if key in NOT_ARTISTS:
        return None

    def is_artist(x: str) -> bool:
        k = x.strip().lower()
        if k in NOT_ARTISTS:
            return False
        if k in DUPLICATES:
            return True
        kind, _ = classify(x, known_performer=known_performer(k))
        return kind == "artist" and not (sub_genre(k) in SHOW_SUBGENRES and k not in KEEP_DESPITE_GENRE)

    if is_artist(v):
        return v
    p = performer_in_title(v, is_artist)
    if p:
        return DUPLICATES.get(p.lower(), p)
    return None
