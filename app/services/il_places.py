"""Israeli place names → one canonical English city.

Used by ``scripts/dedupe_venues.py --site-pass`` and by ``/api/cities``
(Hebrew spellings as city aliases, so typing "תל אביב" finds Tel Aviv).
The dedupe pass uses it for two things:

* ``canon_place(physical_city, city_name)`` puts a venue in its real
  city. ``venues.physical_city`` holds every spelling collectors emit
  ("Tel Aviv-Yafo", "תל אביב-יפו", "נמל יפו", "Kefar Sava"), and the
  venue's ``city_id`` is often a default-city fallback (Tel Aviv holds
  ~270 venues that are physically elsewhere), so neither column alone
  says where a venue is.
* ``name_places(name)`` reads the city a venue *name* names ("היכל
  התרבות אופקים" → Ofakim) — two rows on one site whose names name
  different cities are two branches, never one venue.

Keys are matched after whitespace folding; regional councils, kibbutzim
and moshavim map to their own English name. Values that start with
"Outside Israel" / "Online" mark rows that should not be in Israel at
all.
"""
from __future__ import annotations

import re

PLACES: dict[str, str] = {
    'Nes Ziona': 'Ness Ziona',
    'Nes Tziona': 'Ness Ziona',
    "Modi'in": 'Modiin',
    'Modi’in': 'Modiin',
    'Kiryat Motzkin': 'Kiryat Motzkin',
    'Petah Tiqva': 'Petah Tikva',
    "Be'er Sheva": 'Beersheba',
    'Beer Sheva': 'Beersheba',
    'Hertsliya': 'Herzliya',
    'Herzeliya': 'Herzliya',
    'Тель-Авив': 'Tel Aviv',
    'Тель-Авив-Яффо': 'Tel Aviv',
    "Modi'in-Maccabim-Re'ut": 'Modiin',
    'Modiin-Maccabim-Reut': 'Modiin',
    "Binyamina-Giv'at Ada": 'Binyamina',
    'Pardes Hanna-Karkur': 'Pardes Hanna',
    'Glil Yam': 'Glil Yam',
    'Tel-Aviv': 'Tel Aviv',
    'Jaffa': 'Tel Aviv',
    'Yafo': 'Tel Aviv',
    'Rosh Haayin': 'Rosh HaAyin',
    'Ramat-Gan': 'Ramat Gan',
    'Petach Tikva': 'Petah Tikva',
    'Kfar-Saba': 'Kfar Saba',
    'Raanana': 'Raanana',
    "Ra'anana": 'Raanana',
    'Netania': 'Netanya',
    'Haifa, Israel': 'Haifa',
    'Ashqelon': 'Ashkelon',
    'Boston': 'Outside Israel (US)',
    'Caesarea': 'Caesarea',
    'Dallas': 'Outside Israel (US)',
    "Giv'atayim": 'Givatayim',
    'Houston': 'Outside Israel (US)',
    'H̱olon': 'Holon',
    'Kefar Sava': 'Kfar Saba',
    'Kfar Shmaryahu': 'Kfar Shmaryahu',
    'Petaẖ Tiqwa': 'Petah Tikva',
    'Ramat Gan': 'Ramat Gan',
    'Rishon LeẔiyyon': 'Rishon LeZion',
    'Sweimeh': 'Outside Israel (Jordan)',
    'Tel Aviv-Jaffa': 'Tel Aviv',
    'Tel Aviv-Yafo': 'Tel Aviv',
    'Tel Aviv-yafo': 'Tel Aviv',
    'Yehud-Monosson': 'Yehud',
    'אבו גוש': 'Abu Ghosh',
    'אבן יהודה': 'Even Yehuda',
    'אום אל פחם': 'Umm al-Fahm',
    'אונליין (ONLINE)': 'Online',
    'אופקים': 'Ofakim',
    'אור יהודה': 'Or Yehuda',
    'אור עקיבא': 'Or Akiva',
    'אורנית': 'Oranit',
    'איירפורט סיטי': 'Airport City',
    'אילת': 'Eilat',
    'אלון שבות': 'Alon Shvut',
    'אמאוס ניקופוליס': 'Latrun',
    'אפרת': 'Efrat',
    'אריאל': 'Ariel',
    'אשדוד': 'Ashdod',
    'אשקלון': 'Ashkelon',
    'באר טוביה': "Be'er Tuvia",
    'באר יעקב': "Be'er Ya'akov",
    'באר שבע': 'Beersheba',
    'בארותיים': "Be'erotayim",
    'בית גבריאל': 'Beit Gabriel',
    'בית שאן': "Beit She'an",
    'בית שמש': 'Beit Shemesh',
    'בני ברק': 'Bené Beraq',
    'בנימינה גבעת עדה': 'Binyamina',
    'בנימינה-גבעת עדה': 'Binyamina',
    'בת ים': 'Bat Yam',
    'גבעת אבני': 'Givat Avni',
    'גבעת ברנר': 'Givat Brenner',
    'גבעת זאב': "Givat Ze'ev",
    'גבעת חביבה': 'Givat Haviva',
    'גבעת שמואל': 'Givat Shmuel',
    'גבעתיים': 'Givatayim',
    'גדרה': 'Gedera',
    'גן שמואל': 'Gan Shmuel',
    'גני תקווה': 'Gani Tikva',
    'געש': "Ga'ash",
    'דיזנגוף סנטר': 'Tel Aviv',
    'דימונה': 'Dimona',
    'הוד השרון': 'Hod HaSharon',
    'הרצליה': 'Herzliya',
    'זיכרון יעקב': 'Zichron Yaakov',
    'זכרון יעקב': 'Zichron Yaakov',
    'חבל מודיעין': 'Hevel Modiin',
    'חדרה': 'Hadera',
    'חולון': 'Holon',
    'חולתה': 'Hulata',
    'חוף הכרמל': 'Hof HaCarmel',
    'חיפה': 'Haifa',
    'חרות': 'Herut',
    'חריש': 'Harish',
    'טבריה': 'Tiberias',
    'טירת כרמל': 'Tirat Carmel',
    'יבנה': 'Yavne',
    'יהוד': 'Yehud',
    'יהוד-מונוסון': 'Yehud',
    'יובלים': 'Yuvalim',
    'יוון': 'Outside Israel (Greece)',
    'יוקנעם': 'Yokneam',
    'יחיעם': 'Yehiam',
    'ים המלח': 'Dead Sea',
    'יפו': 'Tel Aviv',
    'יקיר': 'Yakir',
    'יקנעם': 'Yokneam',
    'ירוחם': 'Yeruham',
    'ירושלים': 'Jerusalem',
    'ישובי כיכר סדום': 'Kikar Sdom',
    'ישראל': 'Israel - Other',
    'כברי': 'Kabri',
    'כוכב יאיר': 'Kochav Yair',
    'כוכב יאיר צור יגאל': 'Kochav Yair',
    'כורזים': 'Korazim',
    'כינרת': 'Kinneret',
    'כישור': 'Kishor',
    'כנרת': 'Kinneret',
    'כפר דניאל': 'Kfar Daniel',
    'כפר האורנים': 'Kfar HaOranim',
    'כפר ורדים': 'Kfar Vradim',
    "כפר חסידים ב'": 'Kfar Hasidim',
    'כפר יהושע': 'Kfar Yehoshua',
    'כפר יונה': 'Kfar Yona',
    'כפר מסריק': 'Kfar Masaryk',
    'כפר סבא': 'Kfar Saba',
    'כפר עציון': 'Kfar Etzion',
    'כרם מהר"ל': 'Kerem Maharal',
    'כרמיאל': 'Karmiel',
    'להבים': 'Lehavim',
    'לוד': 'Lod',
    'לטרון': 'Latrun',
    'מבוא חורון': 'Mevo Horon',
    'מבשרת ציון': 'Mevaseret Zion',
    'מגדל': 'Migdal',
    'מגדל העמק': 'Migdal HaEmek',
    'מדרשת רופין': 'Midreshet Ruppin',
    'מודיעין': 'Modiin',
    'מודיעין-מכבים-רעות': 'Modiin',
    'מועצה אזורית אשכול': 'Eshkol',
    'מועצה אזורית ברנר': 'Brenner',
    'מועצה אזורית גדרות': 'Gderot',
    'מועצה אזורית גולן': 'Golan',
    'מועצה אזורית הגליל העליון': 'Upper Galilee',
    'מועצה אזורית זבולון': 'Zevulun',
    'מועצה אזורית חוף השרון': 'Hof HaSharon',
    'מועצה אזורית עמק המעיינות': 'Emek HaMaayanot',
    'מועצה האזורית חוף הכרמל': 'Hof HaCarmel',
    'מועצה האיזורית דרום השרון': 'Drom HaSharon',
    'מועצה מקומית אורנית': 'Oranit',
    'מועצה מקומית פרדסיה': 'Pardesiya',
    'מושב באר טוביה': "Be'er Tuvia",
    'מושב בית אלעזרי': 'Beit Elazari',
    'מושב מנוף': 'Manof',
    'מושב ריחן': 'Rehan',
    'מזכרת בתיה': 'Mazkeret Batya',
    'מטה אשר': 'Mateh Asher',
    'מטה יהודה': 'Mateh Yehuda',
    'מנוף': 'Manof',
    "מנזר בית ג'מל": 'Beit Jimal',
    'מנשה': 'Menashe',
    'מעלה אדומים': 'Maale Adumim',
    'מעלות': 'Maalot-Tarshiha',
    'מעלות תרשיחא': 'Maalot-Tarshiha',
    'מרחבים': 'Merhavim',
    'נהלל': 'Nahalal',
    'נהריה': 'Nahariya',
    'נווה ירק': 'Neve Yarak',
    'נחל ציפורי': 'Nahal Tzippori',
    'נמל אילת': 'Eilat',
    'נמל יפו': 'Tel Aviv',
    'נמל קיסריה': 'Caesarea',
    'נס ציונה': 'Ness Ziona',
    'נצרת': 'Nazareth',
    'נשר': 'Nesher',
    'נתיבות': 'Netivot',
    'נתניה': 'Netanya',
    'סביון': 'Savyon',
    'ספיר': 'Sapir',
    'עומר': 'Omer',
    'עין דור': 'Ein Dor',
    'עין הוד': 'Ein Hod',
    'עין השופט': 'Ein HaShofet',
    'עין שמר': 'Ein Shemer',
    'עכו': 'Acre',
    'עמק יזרעאל': 'Jezreel Valley',
    'עפולה': 'Afula',
    'ערד': 'Arad',
    'ערוגות': 'Arugot',
    'עשרת': 'Aseret',
    'פסוטה': 'Fassuta',
    'פרדס חנה - כרכור': 'Pardes Hanna',
    'פרדסיה': 'Pardesiya',
    'פתח תקוה': 'Petah Tikva',
    'פתח תקווה': 'Petah Tikva',
    'צפת': 'Safed',
    'צרעה': 'Tzora',
    'קדימה': 'Kadima-Tzoran',
    'קדימה צורן': 'Kadima-Tzoran',
    'קדרון': 'Kidron',
    'קיבוץ אל-רום': 'El Rom',
    'קיבוץ גבעת ברנר': 'Givat Brenner',
    'קיבוץ הגושרים': 'HaGoshrim',
    'קיבוץ הזורע': 'Hazorea',
    'קיבוץ חולדה': 'Hulda',
    'קיבוץ חולתה': 'Hulata',
    'קיבוץ חמדיה': 'Hamadia',
    'קיבוץ חצרים': 'Hatzerim',
    'קיבוץ יגור': 'Yagur',
    'קיבוץ יפעת': 'Yifat',
    'קיבוץ נען': "Na'an",
    'קיבוץ עין חרוד': 'Ein Harod',
    'קידה': 'Kida',
    'קידר': 'Kedar',
    'קיסריה': 'Caesarea',
    'קריית אונו': 'Kiryat Ono',
    'קריית ביאליק': 'Kiryat Bialik',
    'קריית גת': 'Kiryat Gat',
    'קריית חיים': 'Kiryat Haim',
    'קריית טבעון': 'Kiryat Tivon',
    'קריית ים': 'Kiryat Yam',
    'קריית מוצקין': 'Kiryat Motzkin',
    'קריית מלאכי': 'Kiryat Malakhi',
    'קריית שמונה': 'Kiryat Shmona',
    'קרית חיים': 'Kiryat Haim',
    'קרית מוצקין': 'Kiryat Motzkin',
    'קרני שומרון': 'Karnei Shomron',
    'ראש העין': 'Rosh HaAyin',
    'ראש פינה': 'Rosh Pina',
    'ראשון לציון': 'Rishon LeZion',
    'ראשל"צ': 'Rishon LeZion',
    'רגבה': 'Regba',
    'רחובות': 'Rehovot',
    'רמלה': 'Ramla',
    'רמת גן': 'Ramat Gan',
    'רמת דלתון': 'Ramat Dalton',
    'רמת השרון': 'Ramat HaSharon',
    'רמת ישי': 'Ramat Yishai',
    'רעננה': 'Raanana',
    'שבי ציון': 'Shavei Zion',
    'שדה אילן': 'Sde Ilan',
    'שדות נגב': 'Sdot Negev',
    'שדרות': 'Sderot',
    'שוהם': 'Shoham',
    'שיטים': 'Shittim',
    'שילה': 'Shiloh',
    'שלומי': 'Shlomi',
    'שער העמקים': "Sha'ar HaAmakim",
    'ת"א': 'Tel Aviv',
    'תל אביב': 'Tel Aviv',
    'תל אביב יפו': 'Tel Aviv',
    'תל אביב-יפו': 'Tel Aviv',
    'תל מונד': 'Tel Mond',
    'תל-אביב': 'Tel Aviv',
}

# Place names that are also everyday words ("מגדל" tower, "חרות"
# freedom, "ישראל" in "מוזיאון ארץ ישראל") — fine as a physical_city,
# never read out of a venue name.
NAME_SKIP = {"ישראל", "מגדל", "חרות", "ספיר", "מנוף", "יקיר", "געש", "קידה",
             "Online", "Caesarea", "Kfar Shmaryahu", "Ramat Gan", "Rehovot",
             "Jerusalem", "Kefar Sava", "Giv'atayim", "Yehud-Monosson"}

_WS = re.compile(r"\s+")
_BY_LEN = sorted((k for k in PLACES if k not in NAME_SKIP and re.search("[\u0590-\u05ff]", k)),
                 key=len, reverse=True)


def _fold(s: str | None) -> str:
    s = (s or "").replace("&quot;", '"').replace("&#039;", "'")
    s = re.sub(r"\s*-\s*", "-", s)          # "תל אביב -יפו" → "תל אביב-יפו"
    return _WS.sub(" ", s).strip()


_FOLDED: dict[str, str] = {}
for _k, _v in PLACES.items():
    _FOLDED.setdefault(_fold(_k), _v)
    _FOLDED.setdefault(_fold(_v), _v)


def canon_place(physical_city: str | None, city_name: str | None = None) -> str:
    """The venue's real city: ``physical_city`` canonicalised, else the
    attached City row's name, else "Israel - Other"."""
    p = _fold(physical_city)
    p = re.sub(r",\s*(Israel|ישראל)$", "", p, flags=re.IGNORECASE).strip() or p
    if p.lower() in ("israel", "ישראל"):
        p = ""                                 # the country says nothing
    if p:
        return _FOLDED.get(p, _WS.sub(" ", physical_city).strip())
    c = _fold(city_name)
    if c:
        return _FOLDED.get(c, _WS.sub(" ", city_name).strip())
    return "Israel - Other"


def name_places(name: str | None) -> set[str]:
    """Canonical cities named inside a venue name (longest match first, so
    "קריית ביאליק" wins over a bare token). Only whole-word matches."""
    s = " " + re.sub(r"[^\w\s\"'-]+", " ", _fold(name)) + " "
    found: set[str] = set()
    for key in _BY_LEN:
        pat = " " + key + " "
        if pat in s:
            found.add(PLACES[key])
            s = s.replace(pat, " | ")
    return found


def aliases_for(city_name: str) -> list[str]:
    """Every non-English spelling that canonicalises to ``city_name``
    (Hebrew names, "Tel Aviv-Yafo"-style variants). Used for the location
    autocomplete."""
    return _ALIASES.get(city_name, [])


_ALIASES: dict[str, list[str]] = {}
for _k, _v in PLACES.items():
    if _k != _v and not _v.startswith(("Outside Israel", "Online", "Israel - Other")):
        _ALIASES.setdefault(_v, []).append(_k)
