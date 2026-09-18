"""Canonical country names for Route 3.

City.country holds English short names ("Germany", "United States").
Cadence A wrote whatever the LLM emitted into LLMSource.country
("Deutschland", "United States of America", even "102#Italy"), and those
strings flowed into auto-enrolled recipes, where `_resolve_city` then
failed with "no City resolvable" — 8 recipes were stuck on that on
2026-09-18. Every country string that reaches a City lookup goes through
canon_country() first.
"""
from __future__ import annotations

import re
from typing import Optional

_ALIASES = {
    # German / local names
    "deutschland": "Germany", "germany": "Germany", "de": "Germany",
    "österreich": "Austria", "oesterreich": "Austria", "austria": "Austria", "at": "Austria",
    "schweiz": "Switzerland", "suisse": "Switzerland", "svizzera": "Switzerland", "switzerland": "Switzerland", "ch": "Switzerland",
    "españa": "Spain", "espana": "Spain", "spain": "Spain", "es": "Spain",
    "italia": "Italy", "italy": "Italy", "it": "Italy",
    "nederland": "Netherlands", "the netherlands": "Netherlands", "holland": "Netherlands", "netherlands": "Netherlands", "nl": "Netherlands",
    "belgië": "Belgium", "belgique": "Belgium", "belgium": "Belgium", "be": "Belgium",
    "portugal": "Portugal", "pt": "Portugal",
    "brasil": "Brazil", "brazil": "Brazil", "br": "Brazil",
    "méxico": "Mexico", "mexico": "Mexico", "mx": "Mexico",
    "polska": "Poland", "poland": "Poland", "pl": "Poland",
    "česko": "Czech Republic", "czechia": "Czech Republic", "czech republic": "Czech Republic", "cz": "Czech Republic",
    "sverige": "Sweden", "sweden": "Sweden", "se": "Sweden",
    "norge": "Norway", "norway": "Norway", "no": "Norway",
    "danmark": "Denmark", "denmark": "Denmark", "dk": "Denmark",
    "suomi": "Finland", "finland": "Finland", "fi": "Finland",
    "ελλάδα": "Greece", "greece": "Greece", "gr": "Greece",
    "türkiye": "Turkey", "turkiye": "Turkey", "turkey": "Turkey", "tr": "Turkey",
    "magyarország": "Hungary", "hungary": "Hungary", "hu": "Hungary",
    "éire": "Ireland", "ireland": "Ireland", "republic of ireland": "Ireland", "ie": "Ireland",
    "france": "France", "fr": "France",
    # English variants
    "united states": "United States", "united states of america": "United States", "usa": "United States",
    "u.s.a.": "United States", "u.s.": "United States", "us": "United States", "america": "United States",
    "united kingdom": "United Kingdom", "uk": "United Kingdom", "u.k.": "United Kingdom", "great britain": "United Kingdom",
    "england": "United Kingdom", "scotland": "United Kingdom", "wales": "United Kingdom", "northern ireland": "United Kingdom", "gb": "United Kingdom",
    "israel": "Israel", "ישראל": "Israel", "il": "Israel",
    "canada": "Canada", "ca": "Canada",
    "australia": "Australia", "au": "Australia",
    "new zealand": "New Zealand", "aotearoa": "New Zealand", "nz": "New Zealand",
    "south africa": "South Africa", "za": "South Africa",
    "united arab emirates": "United Arab Emirates", "uae": "United Arab Emirates", "ae": "United Arab Emirates",
    "japan": "Japan", "jp": "Japan", "south korea": "South Korea", "korea": "South Korea", "kr": "South Korea",
    "india": "India", "in": "India", "singapore": "Singapore", "sg": "Singapore", "thailand": "Thailand", "th": "Thailand",
    "argentina": "Argentina", "ar": "Argentina", "chile": "Chile", "cl": "Chile", "cyprus": "Cyprus", "cy": "Cyprus",
}
_JUNK_PREFIX = re.compile(r"^\s*\d+\s*#\s*")   # "102#Italy" (Cadence A artefact)


def canon_country(value: Optional[str]) -> Optional[str]:
    """'Deutschland' → 'Germany', 'United States of America' → 'United
    States', '102#Italy' → 'Italy'. Unknown strings pass through trimmed
    (a City.country we don't list is still a valid lookup key)."""
    if not value:
        return None
    s = _JUNK_PREFIX.sub("", str(value)).strip().strip(".,;")
    if not s:
        return None
    return _ALIASES.get(s.lower(), s)
