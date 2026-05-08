"""US state code → display-name mapping + helpers.

Cities in our DB store state as a 2-letter code (CA, NY, IL, …) for
US rows. Display surfaces want the full name (California, New York,
Illinois) so users can search for what they actually call the place.

Two known data inconsistencies as of 2026-05-08: a small number of US
city rows have ``state`` set to the full name (``"Ohio"``,
``"West Virginia"``) instead of the canonical code. ``normalize()``
accepts either input shape so the display layer handles both
transparently — no migration required.

Public surface:
    normalize(value)          → canonical full state name
    is_state_name(name)       → True if the value (case-sensitive) is
                                a US state's display name. Used to
                                detect when a city's name overlaps a
                                state name and append a "City" suffix
                                for disambiguation (e.g. "New York" the
                                city in "New York" the state → "New
                                York City").
"""
from __future__ import annotations


# Canonical mapping: 2-letter code → full display name.
# Order is alphabetical-by-name for readability; iteration order is not
# semantically meaningful elsewhere.
US_STATE_NAMES: dict[str, str] = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "DC": "District of Columbia",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
}

# Set of canonical full names — used to detect city-name / state-name
# collisions. Frozen so callers can't mutate.
US_STATE_NAME_SET: frozenset[str] = frozenset(US_STATE_NAMES.values())

# Reverse lookup for full-name → code; tolerates the data inconsistency
# where some rows are already stored as full names.
_FULL_NAME_TO_CODE: dict[str, str] = {v: k for k, v in US_STATE_NAMES.items()}


def normalize(value: str | None) -> str | None:
    """Return canonical full state name for ``value``.

    Accepts either a 2-letter code (``"CA"``) or a full name
    (``"California"``). Returns the input unchanged if unrecognised
    (so non-US states / typos pass through harmlessly).
    """
    if not value:
        return value
    v = value.strip()
    if not v:
        return v
    # 2-letter code path (case-insensitive — code stored uppercase but
    # users could type lowercase elsewhere down the line).
    upper = v.upper()
    if upper in US_STATE_NAMES:
        return US_STATE_NAMES[upper]
    # Full-name path — already canonical, return as-is.
    if v in _FULL_NAME_TO_CODE:
        return v
    return v


def is_state_name(name: str | None) -> bool:
    """True if ``name`` exactly matches a US state's canonical full name.

    Used by the city-label layer to decide whether to append " City"
    for disambiguation: a city literally named "New York" shows as
    "New York City"; a city named "Indianapolis" doesn't.
    """
    if not name:
        return False
    return name.strip() in US_STATE_NAME_SET


__all__ = [
    "US_STATE_NAMES",
    "US_STATE_NAME_SET",
    "normalize",
    "is_state_name",
]
