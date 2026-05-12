"""Country → (continent, sub_continent) mapping for the location
autocomplete cascade.

Layered on top of the existing Direct city → Metro → Country → US State
→ Other cities hierarchy. Continent / sub-continent chips select via
the same multi-city-id plumbing the states layer uses (b4e4805).

Naming intent
=============
Sub-continent names follow common geographic English ("Northern Europe",
"East Asia", "Northern America" — distinct from the parent "North
America" which includes Mexico, Central America, and the Caribbean).
The autocomplete matcher is word-start aware so users typing "south
europe" or "east asia" hit the right entry without exact-match
gymnastics.

Coverage
========
Every country that has events in our DB at the time of writing is
mapped. Country-name variants (Türkiye/Turkey, Czechia/Czech Republic,
日本/Japan) are listed as separate keys so the aggregation catches
all storage forms. Add new countries here as they appear.
"""

# Maps canonical country name (matching City.country in DB) →
# (continent, sub_continent). Keep parent-continent strings consistent
# so the aggregation rolls up cleanly. Sub-continents are nullable
# when the continent has no useful sub-division (Oceania, Antarctica).
COUNTRY_TO_CONTINENT: dict[str, tuple[str, str | None]] = {
    # ── Europe — Northern ─────────────────────────────────────────
    "United Kingdom":      ("Europe", "Northern Europe"),
    "Ireland":             ("Europe", "Northern Europe"),
    "Iceland":             ("Europe", "Northern Europe"),
    "Denmark":             ("Europe", "Northern Europe"),
    "Sweden":              ("Europe", "Northern Europe"),
    "Norway":              ("Europe", "Northern Europe"),
    "Finland":             ("Europe", "Northern Europe"),
    "Estonia":             ("Europe", "Northern Europe"),
    "Latvia":              ("Europe", "Northern Europe"),
    "Lithuania":           ("Europe", "Northern Europe"),
    # ── Europe — Western ──────────────────────────────────────────
    "France":              ("Europe", "Western Europe"),
    "Germany":             ("Europe", "Western Europe"),
    "Belgium":             ("Europe", "Western Europe"),
    "Netherlands":         ("Europe", "Western Europe"),
    "Luxembourg":          ("Europe", "Western Europe"),
    "Switzerland":         ("Europe", "Western Europe"),
    "Austria":             ("Europe", "Western Europe"),
    "Liechtenstein":       ("Europe", "Western Europe"),
    "Monaco":              ("Europe", "Western Europe"),
    # ── Europe — Southern ─────────────────────────────────────────
    "Spain":               ("Europe", "Southern Europe"),
    "Portugal":            ("Europe", "Southern Europe"),
    "Italy":               ("Europe", "Southern Europe"),
    "Greece":              ("Europe", "Southern Europe"),
    "Malta":               ("Europe", "Southern Europe"),
    "Cyprus":              ("Europe", "Southern Europe"),
    "San Marino":          ("Europe", "Southern Europe"),
    "Vatican City":        ("Europe", "Southern Europe"),
    "Andorra":             ("Europe", "Southern Europe"),
    # ── Europe — Eastern ──────────────────────────────────────────
    "Poland":              ("Europe", "Eastern Europe"),
    "Czechia":             ("Europe", "Eastern Europe"),
    "Czech Republic":      ("Europe", "Eastern Europe"),
    "Slovakia":            ("Europe", "Eastern Europe"),
    "Hungary":             ("Europe", "Eastern Europe"),
    "Romania":             ("Europe", "Eastern Europe"),
    "Bulgaria":            ("Europe", "Eastern Europe"),
    "Slovenia":            ("Europe", "Eastern Europe"),
    "Croatia":             ("Europe", "Eastern Europe"),
    "Serbia":              ("Europe", "Eastern Europe"),
    "Bosnia and Herzegovina": ("Europe", "Eastern Europe"),
    "Montenegro":          ("Europe", "Eastern Europe"),
    "Albania":             ("Europe", "Eastern Europe"),
    "North Macedonia":     ("Europe", "Eastern Europe"),
    "Moldova":             ("Europe", "Eastern Europe"),
    "Ukraine":             ("Europe", "Eastern Europe"),
    "Belarus":             ("Europe", "Eastern Europe"),
    "Russia":              ("Europe", "Eastern Europe"),
    "Russian Federation":  ("Europe", "Eastern Europe"),
    "Kosovo":              ("Europe", "Eastern Europe"),

    # ── Asia — East ───────────────────────────────────────────────
    "China":               ("Asia", "East Asia"),
    "Japan":               ("Asia", "East Asia"),
    "日本":                 ("Asia", "East Asia"),
    "South Korea":         ("Asia", "East Asia"),
    "North Korea":         ("Asia", "East Asia"),
    "Korea, Republic of":  ("Asia", "East Asia"),
    "Republic of Korea":   ("Asia", "East Asia"),
    "Mongolia":            ("Asia", "East Asia"),
    "Taiwan":              ("Asia", "East Asia"),
    "Hong Kong":           ("Asia", "East Asia"),
    "Macau":               ("Asia", "East Asia"),
    # ── Asia — Southeast ──────────────────────────────────────────
    "Vietnam":             ("Asia", "Southeast Asia"),
    "Thailand":            ("Asia", "Southeast Asia"),
    "Indonesia":           ("Asia", "Southeast Asia"),
    "Philippines":         ("Asia", "Southeast Asia"),
    "Malaysia":            ("Asia", "Southeast Asia"),
    "Singapore":           ("Asia", "Southeast Asia"),
    "Cambodia":            ("Asia", "Southeast Asia"),
    "Laos":                ("Asia", "Southeast Asia"),
    "Myanmar":             ("Asia", "Southeast Asia"),
    "Brunei":              ("Asia", "Southeast Asia"),
    "Timor-Leste":         ("Asia", "Southeast Asia"),
    # ── Asia — South ──────────────────────────────────────────────
    "India":               ("Asia", "South Asia"),
    "Pakistan":            ("Asia", "South Asia"),
    "Bangladesh":          ("Asia", "South Asia"),
    "Sri Lanka":           ("Asia", "South Asia"),
    "Nepal":               ("Asia", "South Asia"),
    "Bhutan":              ("Asia", "South Asia"),
    "Maldives":            ("Asia", "South Asia"),
    "Afghanistan":         ("Asia", "South Asia"),
    # ── Asia — Central ────────────────────────────────────────────
    "Kazakhstan":          ("Asia", "Central Asia"),
    "Uzbekistan":          ("Asia", "Central Asia"),
    "Kyrgyzstan":          ("Asia", "Central Asia"),
    "Tajikistan":          ("Asia", "Central Asia"),
    "Turkmenistan":        ("Asia", "Central Asia"),
    # ── Asia — Western (Middle East) ──────────────────────────────
    "Turkey":              ("Asia", "Western Asia"),
    "Türkiye":             ("Asia", "Western Asia"),
    "Israel":              ("Asia", "Western Asia"),
    "United Arab Emirates": ("Asia", "Western Asia"),
    "Saudi Arabia":        ("Asia", "Western Asia"),
    "Qatar":               ("Asia", "Western Asia"),
    "Bahrain":             ("Asia", "Western Asia"),
    "Kuwait":              ("Asia", "Western Asia"),
    "Oman":                ("Asia", "Western Asia"),
    "Yemen":               ("Asia", "Western Asia"),
    "Iran":                ("Asia", "Western Asia"),
    "Iraq":                ("Asia", "Western Asia"),
    "Syria":               ("Asia", "Western Asia"),
    "Lebanon":             ("Asia", "Western Asia"),
    "Jordan":              ("Asia", "Western Asia"),
    "Palestine":           ("Asia", "Western Asia"),
    "Armenia":             ("Asia", "Western Asia"),
    "Azerbaijan":          ("Asia", "Western Asia"),
    "Georgia":             ("Asia", "Western Asia"),

    # ── Africa — Northern ─────────────────────────────────────────
    "Egypt":               ("Africa", "Northern Africa"),
    "Morocco":             ("Africa", "Northern Africa"),
    "Tunisia":             ("Africa", "Northern Africa"),
    "Algeria":             ("Africa", "Northern Africa"),
    "Libya":               ("Africa", "Northern Africa"),
    "Sudan":               ("Africa", "Northern Africa"),
    # ── Africa — Sub-Saharan (rolled up; most of our coverage is
    #     light here, splitting further isn't useful yet) ─────────
    "South Africa":        ("Africa", "Southern Africa"),
    "Namibia":             ("Africa", "Southern Africa"),
    "Botswana":            ("Africa", "Southern Africa"),
    "Zimbabwe":            ("Africa", "Southern Africa"),
    "Lesotho":             ("Africa", "Southern Africa"),
    "Eswatini":            ("Africa", "Southern Africa"),
    "Nigeria":             ("Africa", "Western Africa"),
    "Ghana":               ("Africa", "Western Africa"),
    "Senegal":             ("Africa", "Western Africa"),
    "Côte d'Ivoire":       ("Africa", "Western Africa"),
    "Cote d'Ivoire":       ("Africa", "Western Africa"),
    "Mali":                ("Africa", "Western Africa"),
    "Burkina Faso":        ("Africa", "Western Africa"),
    "Cameroon":            ("Africa", "Western Africa"),
    "Kenya":               ("Africa", "Eastern Africa"),
    "Ethiopia":            ("Africa", "Eastern Africa"),
    "Tanzania":            ("Africa", "Eastern Africa"),
    "Uganda":              ("Africa", "Eastern Africa"),
    "Rwanda":              ("Africa", "Eastern Africa"),
    "Madagascar":          ("Africa", "Eastern Africa"),

    # ── North America — Northern America ──────────────────────────
    "United States":             ("North America", "Northern America"),
    "United States of America":  ("North America", "Northern America"),
    "Canada":                    ("North America", "Northern America"),
    "Bermuda":                   ("North America", "Northern America"),
    # ── North America — Central America ───────────────────────────
    "Mexico":              ("North America", "Central America"),
    "Guatemala":           ("North America", "Central America"),
    "Honduras":            ("North America", "Central America"),
    "El Salvador":         ("North America", "Central America"),
    "Nicaragua":           ("North America", "Central America"),
    "Costa Rica":          ("North America", "Central America"),
    "Panama":              ("North America", "Central America"),
    "Belize":              ("North America", "Central America"),
    # ── North America — Caribbean ─────────────────────────────────
    "Cuba":                ("North America", "Caribbean"),
    "Dominican Republic":  ("North America", "Caribbean"),
    "Haiti":               ("North America", "Caribbean"),
    "Jamaica":             ("North America", "Caribbean"),
    "Puerto Rico":         ("North America", "Caribbean"),
    "Trinidad and Tobago": ("North America", "Caribbean"),
    "Barbados":            ("North America", "Caribbean"),
    "Bahamas":             ("North America", "Caribbean"),

    # ── South America ─────────────────────────────────────────────
    "Brazil":              ("South America", "South America"),
    "Argentina":           ("South America", "South America"),
    "Chile":               ("South America", "South America"),
    "Peru":                ("South America", "South America"),
    "Colombia":            ("South America", "South America"),
    "Ecuador":             ("South America", "South America"),
    "Venezuela":           ("South America", "South America"),
    "Bolivia":             ("South America", "South America"),
    "Paraguay":            ("South America", "South America"),
    "Uruguay":             ("South America", "South America"),
    "Guyana":              ("South America", "South America"),
    "Suriname":            ("South America", "South America"),
    "French Guiana":       ("South America", "South America"),

    # ── Oceania ───────────────────────────────────────────────────
    "Australia":           ("Oceania", "Australia and New Zealand"),
    "New Zealand":         ("Oceania", "Australia and New Zealand"),
    "Fiji":                ("Oceania", "Melanesia"),
    "Papua New Guinea":    ("Oceania", "Melanesia"),
    "Solomon Islands":     ("Oceania", "Melanesia"),
    "Vanuatu":             ("Oceania", "Melanesia"),
    "Samoa":               ("Oceania", "Polynesia"),
    "Tonga":               ("Oceania", "Polynesia"),
}


def continent_of(country: str) -> str | None:
    rec = COUNTRY_TO_CONTINENT.get(country)
    return rec[0] if rec else None


def sub_continent_of(country: str) -> str | None:
    rec = COUNTRY_TO_CONTINENT.get(country)
    return rec[1] if rec else None
