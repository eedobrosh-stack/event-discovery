"""Israeli artist_name hygiene: show titles are not artists."""
from app.services.artist_names import clean_israeli_artist as clean


def test_performers_stay():
    assert clean("גיל שוחט") == "גיל שוחט"
    assert clean("טיפקס") == "טיפקס"
    assert clean("פרופ' עוזי רבי") == "פרופ' עוזי רבי"


def test_titles_cleared_or_reduced_to_performer():
    assert clean("אור לגויים - תיאטרון בית ליסין") is None
    assert clean("אליסה בארץ הפלאות – חנוכה 2026") is None
    assert clean("דרור קרן במופע סטנדאפ חדש!") == "דרור קרן"
    assert clean("סינדרלה - בכיכובה של רינת גבאי קיץ 2026") == "רינת גבאי"
    assert clean("פרופ&#039; עילם גרוס - אלוהים של המדען") == "עילם גרוס"


def test_review_overrides():
    assert clean("חנאל ומכבית") is None            # marked not an artist
    assert clean("דיויד ברוזה") == "דויד ברוזה"      # duplicate spelling
    assert clean("סינגולדה וחברים") == "סינגולדה"


def test_show_genre():
    assert clean("קברט", sub_genre=lambda n: "Musical Theatre") is None
    assert clean("נמרוד הראל", sub_genre=lambda n: "Magic Show") == "נמרוד הראל"
