"""
Metropolitan / county area definitions and API endpoint.

Each metro entry maps a human-friendly area name to the list of city names
that belong to it. The endpoint resolves those names against the DB so only
cities that actually have events are returned.

GET /api/metro-areas  →  list of MetroAreaOut
"""
from __future__ import annotations

import time
from typing import List, Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database import get_db, SessionLocal

router = APIRouter(prefix="/api/metro-areas", tags=["metro-areas"])

# ── Static metro-area definitions ──────────────────────────────────────────────
# City names must match the `name` column in the cities table (case-insensitive).
# Only cities that exist AND have events will be included in the response.

METRO_AREAS: list[dict] = [

    # ── United States ──────────────────────────────────────────────────────────
    {
        "id": "bay-area",
        "name": "Bay Area",
        "country": "United States",
        "cities": [
            "San Francisco", "Oakland", "San Jose", "Berkeley",
            "Fremont", "Hayward", "Sunnyvale", "Santa Clara", "Concord",
            "Vallejo", "Richmond", "Antioch", "San Mateo", "Daly City",
            "San Leandro", "South San Francisco", "Alameda", "Walnut Creek",
            "Livermore", "Napa", "Santa Rosa", "Petaluma", "Novato",
            "San Rafael", "Mill Valley", "Sausalito", "Palo Alto",
            "Mountain View", "Menlo Park", "Redwood City", "San Bruno",
            "Burlingame", "Foster City", "Millbrae",
        ],
    },
    {
        "id": "greater-nyc",
        "name": "Greater New York",
        "country": "United States",
        "cities": [
            "New York", "Brooklyn", "Queens", "Bronx", "Staten Island",
            "Newark", "Jersey City", "Hoboken", "Yonkers", "New Rochelle",
            "White Plains", "Stamford", "Bridgeport", "Long Island City",
            "Astoria", "Flushing", "Jamaica", "Harlem", "Hempstead",
            "Paterson", "Elizabeth", "Trenton",
        ],
    },
    {
        "id": "greater-la",
        "name": "Greater Los Angeles",
        "country": "United States",
        "cities": [
            "Los Angeles", "Long Beach", "Anaheim", "Santa Ana", "Riverside",
            "Irvine", "Glendale", "Burbank", "Pasadena", "Santa Monica",
            "Culver City", "Beverly Hills", "Hollywood", "Venice",
            "Inglewood", "Compton", "Torrance", "El Monte", "Pomona",
            "Thousand Oaks", "Ontario", "Rancho Cucamonga", "Garden Grove",
            "Fullerton", "Orange", "Corona", "Moreno Valley", "Fontana",
        ],
    },
    {
        "id": "greater-chicago",
        "name": "Greater Chicago",
        "country": "United States",
        "cities": [
            "Chicago", "Aurora", "Joliet", "Naperville", "Elgin",
            "Waukegan", "Cicero", "Evanston", "Schaumburg", "Bolingbrook",
            "Arlington Heights", "Peoria", "Rockford", "Gary",
        ],
    },
    {
        "id": "greater-miami",
        "name": "Greater Miami",
        "country": "United States",
        "cities": [
            "Miami", "Miami Beach", "Hialeah", "Fort Lauderdale",
            "Boca Raton", "West Palm Beach", "Coral Gables", "Aventura",
            "Hollywood", "Pompano Beach", "Deerfield Beach", "Delray Beach",
            "Hallandale Beach", "North Miami", "Coral Springs", "Dania Beach",
        ],
    },
    {
        "id": "greater-boston",
        "name": "Greater Boston",
        "country": "United States",
        "cities": [
            "Boston", "Cambridge", "Somerville", "Quincy", "Newton",
            "Brookline", "Waltham", "Medford", "Malden", "Lowell",
            "Lynn", "Worcester", "Providence", "Springfield", "Hartford",
        ],
    },
    {
        "id": "greater-seattle",
        "name": "Greater Seattle",
        "country": "United States",
        "cities": [
            "Seattle", "Bellevue", "Tacoma", "Redmond", "Kirkland",
            "Renton", "Everett", "Sammamish", "Shoreline", "Kent",
        ],
    },
    {
        "id": "greater-dc",
        "name": "Washington DC Metro",
        "country": "United States",
        "cities": [
            "Washington", "Arlington", "Alexandria", "Bethesda",
            "Silver Spring", "Rockville", "Fairfax", "Reston",
            "McLean", "Gaithersburg", "Frederick", "Baltimore",
            "Annapolis", "College Park", "Germantown",
        ],
    },
    {
        "id": "dallas-fort-worth",
        "name": "Dallas–Fort Worth",
        "country": "United States",
        "cities": [
            "Dallas", "Fort Worth", "Arlington", "Plano", "Garland",
            "Irving", "Frisco", "McKinney", "Grand Prairie", "Denton",
            "Mesquite", "Carrollton", "Richardson", "Lewisville",
        ],
    },
    {
        "id": "greater-houston",
        "name": "Greater Houston",
        "country": "United States",
        "cities": [
            "Houston", "The Woodlands", "Sugar Land", "Pasadena",
            "Pearland", "League City", "Baytown", "Conroe", "Galveston",
        ],
    },
    {
        "id": "greater-phoenix",
        "name": "Greater Phoenix",
        "country": "United States",
        "cities": [
            "Phoenix", "Mesa", "Chandler", "Gilbert", "Tempe",
            "Peoria", "Scottsdale", "Glendale", "Surprise", "Tucson",
        ],
    },
    {
        "id": "greater-atlanta",
        "name": "Greater Atlanta",
        "country": "United States",
        "cities": [
            "Atlanta", "Sandy Springs", "Marietta", "Roswell", "Johns Creek",
            "Alpharetta", "Smyrna", "Decatur", "Kennesaw", "Savannah",
        ],
    },
    {
        "id": "greater-denver",
        "name": "Greater Denver",
        "country": "United States",
        "cities": [
            "Denver", "Aurora", "Lakewood", "Thornton", "Arvada",
            "Westminster", "Boulder", "Fort Collins", "Pueblo", "Colorado Springs",
        ],
    },
    {
        "id": "greater-minneapolis",
        "name": "Greater Minneapolis",
        "country": "United States",
        "cities": [
            "Minneapolis", "Saint Paul", "Bloomington", "Plymouth",
            "Brooklyn Park", "Duluth", "Rochester",
        ],
    },
    {
        "id": "greater-portland",
        "name": "Greater Portland",
        "country": "United States",
        "cities": [
            "Portland", "Gresham", "Beaverton", "Hillsboro",
            "Lake Oswego", "Salem", "Eugene",
        ],
    },
    {
        "id": "greater-san-diego",
        "name": "Greater San Diego",
        "country": "United States",
        "cities": [
            "San Diego", "Chula Vista", "El Cajon", "Oceanside",
            "Escondido", "Carlsbad", "San Marcos", "La Jolla",
        ],
    },
    {
        "id": "greater-las-vegas",
        "name": "Greater Las Vegas",
        "country": "United States",
        "cities": [
            "Las Vegas", "Henderson", "North Las Vegas", "Paradise",
            "Summerlin", "Boulder City", "Reno",
        ],
    },
    {
        "id": "greater-nashville",
        "name": "Greater Nashville",
        "country": "United States",
        "cities": [
            "Nashville", "Murfreesboro", "Franklin", "Brentwood",
            "Hendersonville", "Clarksville", "Knoxville", "Memphis",
        ],
    },
    {
        "id": "greater-charlotte",
        "name": "Greater Charlotte",
        "country": "United States",
        "cities": [
            "Charlotte", "Concord", "Gastonia", "Rock Hill",
            "Greensboro", "Durham", "Raleigh", "Cary", "Chapel Hill",
        ],
    },
    {
        "id": "greater-tampa",
        "name": "Greater Tampa Bay",
        "country": "United States",
        "cities": [
            "Tampa", "St. Petersburg", "Clearwater", "Brandon",
            "Sarasota", "Bradenton", "Orlando", "Kissimmee", "Gainesville",
        ],
    },
    {
        "id": "greater-austin",
        "name": "Greater Austin",
        "country": "United States",
        "cities": [
            "Austin", "Round Rock", "Cedar Park", "Georgetown",
            "San Marcos", "Pflugerville", "Kyle", "San Antonio",
        ],
    },
    {
        "id": "greater-pittsburgh",
        "name": "Greater Pittsburgh",
        "country": "United States",
        "cities": [
            "Pittsburgh", "Bethel Park", "Monroeville", "McKeesport",
            "Allentown", "Philadelphia", "Reading",
        ],
    },
    {
        "id": "greater-new-orleans",
        "name": "Greater New Orleans",
        "country": "United States",
        "cities": [
            "New Orleans", "Metairie", "Baton Rouge", "Shreveport",
            "Lafayette", "Jackson",
        ],
    },

    # ── Canada ─────────────────────────────────────────────────────────────────
    {
        "id": "greater-toronto",
        "name": "Greater Toronto",
        "country": "Canada",
        "cities": [
            "Toronto", "Mississauga", "Brampton", "Markham", "Vaughan",
            "Richmond Hill", "Oakville", "Burlington", "Ajax", "Pickering",
            "Whitby", "Oshawa", "Hamilton",
        ],
    },
    {
        "id": "greater-vancouver",
        "name": "Greater Vancouver",
        "country": "Canada",
        "cities": [
            "Vancouver", "Surrey", "Burnaby", "Richmond", "Abbotsford",
            "Kelowna", "Victoria", "New Westminster", "Langley", "Coquitlam",
            "Delta", "North Vancouver",
        ],
    },
    {
        "id": "greater-montreal",
        "name": "Greater Montréal",
        "country": "Canada",
        "cities": [
            "Montreal", "Laval", "Longueuil", "Terrebonne", "Brossard",
            "Quebec City", "Gatineau", "Ottawa",
        ],
    },
    {
        "id": "greater-calgary",
        "name": "Greater Calgary",
        "country": "Canada",
        "cities": [
            "Calgary", "Edmonton", "Red Deer", "Lethbridge",
            "Medicine Hat", "Airdrie", "St. Albert",
        ],
    },

    # ── Mexico ─────────────────────────────────────────────────────────────────
    {
        "id": "greater-mexico-city",
        "name": "Greater Mexico City",
        "country": "Mexico",
        "cities": [
            "Mexico City", "Ecatepec", "Nezahualcóyotl", "Naucalpan",
            "Chimalhuacán", "Tlalnepantla", "Puebla", "Querétaro",
            "Toluca", "Cuernavaca",
        ],
    },
    {
        "id": "greater-guadalajara",
        "name": "Greater Guadalajara",
        "country": "Mexico",
        "cities": [
            "Guadalajara", "Zapopan", "Tlaquepaque", "Tonalá",
            "Tlajomulco de Zúñiga",
        ],
    },
    {
        "id": "greater-monterrey",
        "name": "Greater Monterrey",
        "country": "Mexico",
        "cities": [
            "Monterrey", "San Nicolás de los Garza", "Guadalupe",
            "Apodaca", "General Escobedo", "Santa Catarina", "Saltillo",
        ],
    },

    # ── United Kingdom ─────────────────────────────────────────────────────────
    {
        "id": "greater-london",
        "name": "Greater London",
        "country": "United Kingdom",
        "cities": [
            "London", "Westminster", "Camden", "Islington", "Hackney",
            "Southwark", "Lambeth", "Greenwich", "Lewisham", "Bromley",
            "Croydon", "Kingston upon Thames", "Richmond upon Thames",
            "Wandsworth", "Hammersmith", "Kensington", "Chelsea",
            "Tower Hamlets", "Newham", "Barking", "Waltham Forest",
            "Haringey", "Enfield", "Barnet",
        ],
    },
    {
        "id": "greater-manchester",
        "name": "Greater Manchester",
        "country": "United Kingdom",
        "cities": [
            "Manchester", "Salford", "Stockport", "Oldham", "Rochdale",
            "Bolton", "Wigan", "Bury", "Liverpool", "Leeds", "Sheffield",
            "Bradford", "Coventry", "Leicester", "Nottingham", "Birmingham",
            "Bristol", "Hull",
        ],
    },
    {
        "id": "greater-glasgow",
        "name": "Greater Glasgow & Scotland",
        "country": "United Kingdom",
        "cities": [
            "Glasgow", "Edinburgh", "Dundee", "Aberdeen",
            "Paisley", "Motherwell", "Livingston", "Hamilton",
        ],
    },

    # ── France ─────────────────────────────────────────────────────────────────
    {
        "id": "greater-paris",
        "name": "Greater Paris",
        "country": "France",
        "cities": [
            "Paris", "Boulogne-Billancourt", "Saint-Denis", "Argenteuil",
            "Montreuil", "Nanterre", "Créteil", "Versailles",
            "Colombes", "Asnières-sur-Seine", "Courbevoie",
        ],
    },
    {
        "id": "greater-lyon",
        "name": "Greater Lyon",
        "country": "France",
        "cities": [
            "Lyon", "Villeurbanne", "Saint-Étienne", "Grenoble",
            "Clermont-Ferrand", "Bourg-en-Bresse",
        ],
    },
    {
        "id": "greater-marseille",
        "name": "Greater Marseille",
        "country": "France",
        "cities": [
            "Marseille", "Aix-en-Provence", "Nice", "Toulon",
            "Montpellier", "Nîmes", "Avignon",
        ],
    },

    # ── Germany ────────────────────────────────────────────────────────────────
    {
        "id": "greater-berlin",
        "name": "Greater Berlin",
        "country": "Germany",
        "cities": [
            "Berlin", "Potsdam", "Brandenburg an der Havel",
            "Frankfurt (Oder)", "Cottbus",
        ],
    },
    {
        "id": "greater-munich",
        "name": "Greater Munich",
        "country": "Germany",
        "cities": [
            "Munich", "Augsburg", "Regensburg", "Ingolstadt",
            "Nuremberg", "Salzburg",
        ],
    },
    {
        "id": "rhine-main",
        "name": "Rhine-Main (Frankfurt)",
        "country": "Germany",
        "cities": [
            "Frankfurt", "Wiesbaden", "Darmstadt", "Offenbach",
            "Mainz", "Hanau", "Mannheim", "Heidelberg", "Karlsruhe",
        ],
    },
    {
        "id": "greater-hamburg",
        "name": "Greater Hamburg",
        "country": "Germany",
        "cities": [
            "Hamburg", "Bremen", "Lübeck", "Kiel", "Flensburg",
            "Rostock", "Hannover", "Brunswick",
        ],
    },
    {
        "id": "greater-cologne",
        "name": "Greater Cologne–Ruhr",
        "country": "Germany",
        "cities": [
            "Cologne", "Düsseldorf", "Dortmund", "Essen", "Duisburg",
            "Bochum", "Wuppertal", "Bonn", "Mönchengladbach", "Gelsenkirchen",
            "Aachen", "Oberhausen", "Krefeld", "Bielefeld", "Münster",
        ],
    },

    # ── Spain ──────────────────────────────────────────────────────────────────
    {
        "id": "greater-madrid",
        "name": "Greater Madrid",
        "country": "Spain",
        "cities": [
            "Madrid", "Getafe", "Alcalá de Henares", "Leganés",
            "Fuenlabrada", "Móstoles", "Alcorcón", "Torrejón de Ardoz",
            "Toledo", "Guadalajara",
        ],
    },
    {
        "id": "greater-barcelona",
        "name": "Greater Barcelona",
        "country": "Spain",
        "cities": [
            "Barcelona", "Hospitalet de Llobregat", "Badalona", "Terrassa",
            "Sabadell", "Mataró", "Santa Coloma de Gramenet",
            "Girona", "Lleida", "Tarragona",
        ],
    },

    # ── Italy ──────────────────────────────────────────────────────────────────
    {
        "id": "greater-milan",
        "name": "Greater Milan",
        "country": "Italy",
        "cities": [
            "Milan", "Bergamo", "Brescia", "Monza", "Como",
            "Varese", "Novara", "Pavia", "Piacenza", "Verona", "Turin",
        ],
    },
    {
        "id": "greater-rome",
        "name": "Greater Rome",
        "country": "Italy",
        "cities": [
            "Rome", "Latina", "Frosinone", "Viterbo", "Naples",
            "Bologna", "Florence",
        ],
    },

    # ── Netherlands ────────────────────────────────────────────────────────────
    {
        "id": "randstad",
        "name": "Randstad (Netherlands)",
        "country": "Netherlands",
        "cities": [
            "Amsterdam", "Rotterdam", "The Hague", "Utrecht",
            "Eindhoven", "Tilburg", "Groningen", "Almere",
            "Haarlem", "Amstelveen", "Breda", "Nijmegen",
            "Zaandam", "Hoofddorp", "Delft", "Leiden",
        ],
    },

    # ── Belgium ────────────────────────────────────────────────────────────────
    {
        "id": "greater-brussels",
        "name": "Greater Brussels",
        "country": "Belgium",
        "cities": [
            "Brussels", "Antwerp", "Ghent", "Bruges", "Liège",
            "Namur", "Leuven", "Charleroi", "Mons",
        ],
    },

    # ── Switzerland & Austria ──────────────────────────────────────────────────
    {
        "id": "greater-zurich",
        "name": "Greater Zurich",
        "country": "Switzerland",
        "cities": [
            "Zurich", "Bern", "Basel", "Geneva", "Lausanne",
            "Winterthur", "Lucerne", "St. Gallen",
        ],
    },
    {
        "id": "greater-vienna",
        "name": "Greater Vienna",
        "country": "Austria",
        "cities": [
            "Vienna", "Graz", "Linz", "Salzburg", "Innsbruck",
            "Klagenfurt", "Villach", "Wels",
        ],
    },

    # ── Nordics ────────────────────────────────────────────────────────────────
    {
        "id": "greater-stockholm",
        "name": "Greater Stockholm",
        "country": "Sweden",
        "cities": [
            "Stockholm", "Gothenburg", "Malmö", "Uppsala",
            "Västerås", "Örebro", "Linköping", "Helsingborg",
        ],
    },
    {
        "id": "greater-oslo",
        "name": "Greater Oslo",
        "country": "Norway",
        "cities": [
            "Oslo", "Bergen", "Trondheim", "Stavanger",
            "Kristiansand", "Fredrikstad", "Tromsø",
        ],
    },
    {
        "id": "greater-copenhagen",
        "name": "Greater Copenhagen",
        "country": "Denmark",
        "cities": [
            "Copenhagen", "Aarhus", "Odense", "Aalborg",
            "Malmö", "Helsingør", "Roskilde",
        ],
    },
    {
        "id": "greater-helsinki",
        "name": "Greater Helsinki",
        "country": "Finland",
        "cities": [
            "Helsinki", "Espoo", "Tampere", "Vantaa",
            "Oulu", "Turku", "Jyväskylä", "Lahti",
        ],
    },

    # ── Eastern Europe ─────────────────────────────────────────────────────────
    {
        "id": "greater-warsaw",
        "name": "Greater Warsaw",
        "country": "Poland",
        "cities": [
            "Warsaw", "Łódź", "Kraków", "Wrocław", "Poznań",
            "Gdańsk", "Lublin", "Katowice", "Bydgoszcz", "Szczecin",
        ],
    },
    {
        "id": "greater-prague",
        "name": "Greater Prague",
        "country": "Czech Republic",
        "cities": [
            "Prague", "Brno", "Ostrava", "Plzeň", "Liberec",
            "Olomouc", "Pardubice", "Hradec Králové",
        ],
    },
    {
        "id": "greater-budapest",
        "name": "Greater Budapest",
        "country": "Hungary",
        "cities": [
            "Budapest", "Pécs", "Miskolc", "Debrecen", "Szeged",
            "Győr", "Nyíregyháza", "Kecskemét",
        ],
    },
    {
        "id": "greater-bucharest",
        "name": "Greater Bucharest",
        "country": "Romania",
        "cities": [
            "Bucharest", "Cluj-Napoca", "Timișoara", "Iași",
            "Constanța", "Craiova", "Brașov", "Galați",
        ],
    },
    {
        "id": "greater-athens",
        "name": "Greater Athens",
        "country": "Greece",
        "cities": [
            "Athens", "Thessaloniki", "Patras", "Heraklion",
            "Larissa", "Piraeus", "Kallithea",
        ],
    },
    {
        "id": "greater-lisbon",
        "name": "Greater Lisbon",
        "country": "Portugal",
        "cities": [
            "Lisbon", "Porto", "Amadora", "Setúbal", "Braga",
            "Funchal", "Coimbra", "Almada",
        ],
    },
    {
        "id": "greater-dublin",
        "name": "Greater Dublin & Ireland",
        "country": "Ireland",
        "cities": [
            "Dublin", "Cork", "Limerick", "Galway", "Waterford",
            "Belfast", "Derry", "Drogheda",
        ],
    },
    {
        "id": "greater-kyiv",
        "name": "Greater Kyiv",
        "country": "Ukraine",
        "cities": [
            "Kyiv", "Kharkiv", "Odessa", "Dnipro", "Lviv",
            "Zaporizhzhia", "Vinnytsia", "Mykolaiv",
        ],
    },
    {
        "id": "greater-moscow",
        "name": "Greater Moscow",
        "country": "Russia",
        "cities": [
            "Moscow", "Saint Petersburg", "Novosibirsk", "Yekaterinburg",
            "Kazan", "Nizhny Novgorod", "Chelyabinsk", "Samara",
            "Ufa", "Rostov-on-Don", "Krasnoyarsk", "Volgograd",
        ],
    },

    # ── Middle East ────────────────────────────────────────────────────────────
    {
        "id": "gush-dan",
        "name": "Gush Dan (Tel Aviv Metro)",
        "country": "Israel",
        "cities": [
            "Tel Aviv", "Ramat Gan", "Givatayim", "Petah Tikva", "Bnei Brak",
            "Bat Yam", "Holon", "Rishon LeZion", "Or Yehuda", "Kiryat Ono",
            "Herzliya", "Netanya", "Rehovot", "Kfar Saba", "Ra'anana",
        ],
    },
    {
        "id": "greater-haifa",
        "name": "Haifa Metro",
        "country": "Israel",
        "cities": [
            "Haifa", "Acre", "Nahariya", "Krayot", "Tirat Carmel",
        ],
    },
    {
        "id": "greater-jerusalem",
        "name": "Jerusalem Metro",
        "country": "Israel",
        "cities": [
            "Jerusalem", "Bethlehem", "Ramallah", "Beit Shemesh",
            "Maale Adumim",
        ],
    },
    {
        "id": "greater-dubai",
        "name": "Dubai Metro",
        "country": "United Arab Emirates",
        "cities": [
            "Dubai", "Abu Dhabi", "Sharjah", "Ajman",
            "Fujairah", "Ras al-Khaimah", "Al Ain",
        ],
    },
    {
        "id": "greater-riyadh",
        "name": "Greater Riyadh",
        "country": "Saudi Arabia",
        "cities": [
            "Riyadh", "Jeddah", "Mecca", "Medina", "Dammam",
            "Khobar", "Dhahran",
        ],
    },
    {
        "id": "greater-doha",
        "name": "Greater Doha",
        "country": "Qatar",
        "cities": [
            "Doha", "Al Wakrah", "Al Khor", "Lusail",
        ],
    },
    {
        "id": "greater-cairo",
        "name": "Greater Cairo",
        "country": "Egypt",
        "cities": [
            "Cairo", "Giza", "Alexandria", "Shubra el-Kheima",
            "Suez", "Luxor", "Aswan",
        ],
    },
    {
        "id": "greater-amman",
        "name": "Greater Amman",
        "country": "Jordan",
        "cities": [
            "Amman", "Zarqa", "Irbid", "Aqaba",
        ],
    },
    {
        "id": "greater-beirut",
        "name": "Greater Beirut",
        "country": "Lebanon",
        "cities": [
            "Beirut", "Tripoli", "Sidon", "Tyre", "Jounieh",
        ],
    },
    {
        "id": "greater-istanbul",
        "name": "Greater Istanbul",
        "country": "Turkey",
        "cities": [
            "Istanbul", "Ankara", "Izmir", "Bursa", "Antalya",
            "Adana", "Gaziantep", "Konya",
        ],
    },
    {
        "id": "greater-tehran",
        "name": "Greater Tehran",
        "country": "Iran",
        "cities": [
            "Tehran", "Mashhad", "Isfahan", "Karaj", "Tabriz",
            "Shiraz", "Ahvaz", "Qom",
        ],
    },

    # ── East Asia ──────────────────────────────────────────────────────────────
    {
        "id": "greater-tokyo",
        "name": "Greater Tokyo",
        "country": "Japan",
        "cities": [
            "Tokyo", "Yokohama", "Kawasaki", "Saitama", "Chiba",
            "Sagamihara", "Funabashi", "Hachioji", "Machida",
        ],
    },
    {
        "id": "greater-osaka",
        "name": "Osaka–Kobe–Kyoto",
        "country": "Japan",
        "cities": [
            "Osaka", "Kobe", "Kyoto", "Nara", "Himeji",
            "Wakayama", "Otsu", "Sakai",
        ],
    },
    {
        "id": "greater-nagoya",
        "name": "Greater Nagoya",
        "country": "Japan",
        "cities": [
            "Nagoya", "Hamamatsu", "Shizuoka", "Toyota",
            "Gifu", "Okazaki",
        ],
    },
    {
        "id": "greater-fukuoka",
        "name": "Greater Fukuoka",
        "country": "Japan",
        "cities": [
            "Fukuoka", "Kitakyushu", "Sapporo", "Sendai",
            "Hiroshima", "Kumamoto", "Kagoshima",
        ],
    },
    {
        "id": "greater-seoul",
        "name": "Greater Seoul",
        "country": "South Korea",
        "cities": [
            "Seoul", "Incheon", "Suwon", "Goyang", "Yongin",
            "Seongnam", "Bucheon", "Ansan", "Anyang", "Hwaseong",
            "Busan", "Daegu", "Daejeon", "Gwangju", "Ulsan",
        ],
    },
    {
        "id": "greater-shanghai",
        "name": "Greater Shanghai",
        "country": "China",
        "cities": [
            "Shanghai", "Suzhou", "Nanjing", "Hangzhou", "Ningbo",
            "Wuxi", "Changzhou", "Nantong", "Hefei",
        ],
    },
    {
        "id": "greater-beijing",
        "name": "Greater Beijing",
        "country": "China",
        "cities": [
            "Beijing", "Tianjin", "Baoding", "Shijiazhuang",
            "Tangshan", "Langfang",
        ],
    },
    {
        "id": "pearl-river-delta",
        "name": "Pearl River Delta",
        "country": "China",
        "cities": [
            "Guangzhou", "Shenzhen", "Dongguan", "Foshan",
            "Zhuhai", "Zhongshan", "Huizhou", "Zhaoqing",
        ],
    },
    {
        "id": "greater-hong-kong",
        "name": "Hong Kong & Macau",
        "country": "Hong Kong",
        "cities": [
            "Hong Kong", "Macau", "Kowloon",
        ],
    },
    {
        "id": "greater-chengdu",
        "name": "Greater Chengdu",
        "country": "China",
        "cities": [
            "Chengdu", "Chongqing", "Kunming", "Guiyang",
            "Nanchang", "Changsha", "Wuhan", "Xi'an",
        ],
    },
    {
        "id": "greater-taipei",
        "name": "Greater Taipei",
        "country": "Taiwan",
        "cities": [
            "Taipei", "New Taipei", "Taoyuan", "Taichung",
            "Tainan", "Kaohsiung", "Hsinchu",
        ],
    },

    # ── South Asia ─────────────────────────────────────────────────────────────
    {
        "id": "greater-delhi",
        "name": "Delhi NCR",
        "country": "India",
        "cities": [
            "Delhi", "Noida", "Gurgaon", "Faridabad", "Ghaziabad",
            "Meerut", "Agra", "Jaipur", "Chandigarh",
        ],
    },
    {
        "id": "greater-mumbai",
        "name": "Mumbai Metropolitan Region",
        "country": "India",
        "cities": [
            "Mumbai", "Pune", "Nashik", "Navi Mumbai", "Thane",
            "Kalyan", "Aurangabad", "Surat",
        ],
    },
    {
        "id": "greater-bangalore",
        "name": "Greater Bangalore",
        "country": "India",
        "cities": [
            "Bangalore", "Mysore", "Mangalore", "Hubli",
            "Davangere", "Belagavi",
        ],
    },
    {
        "id": "greater-chennai",
        "name": "Greater Chennai",
        "country": "India",
        "cities": [
            "Chennai", "Coimbatore", "Madurai", "Tiruchirappalli",
            "Salem", "Tirunelveli", "Vellore",
        ],
    },
    {
        "id": "greater-hyderabad",
        "name": "Greater Hyderabad",
        "country": "India",
        "cities": [
            "Hyderabad", "Secunderabad", "Warangal",
            "Vijayawada", "Visakhapatnam",
        ],
    },
    {
        "id": "greater-kolkata",
        "name": "Greater Kolkata",
        "country": "India",
        "cities": [
            "Kolkata", "Asansol", "Durgapur", "Kharagpur",
            "Siliguri", "Bhubaneswar",
        ],
    },
    {
        "id": "greater-dhaka",
        "name": "Greater Dhaka",
        "country": "Bangladesh",
        "cities": [
            "Dhaka", "Chittagong", "Sylhet", "Rajshahi",
            "Khulna", "Comilla",
        ],
    },
    {
        "id": "greater-karachi",
        "name": "Greater Karachi",
        "country": "Pakistan",
        "cities": [
            "Karachi", "Lahore", "Faisalabad", "Rawalpindi",
            "Islamabad", "Multan", "Gujranwala", "Peshawar",
        ],
    },
    {
        "id": "greater-colombo",
        "name": "Greater Colombo",
        "country": "Sri Lanka",
        "cities": [
            "Colombo", "Dehiwala-Mount Lavinia", "Moratuwa",
            "Sri Jayawardenepura Kotte",
        ],
    },
    {
        "id": "greater-kathmandu",
        "name": "Kathmandu Valley",
        "country": "Nepal",
        "cities": [
            "Kathmandu", "Patan", "Bhaktapur", "Pokhara",
        ],
    },

    # ── Southeast Asia ─────────────────────────────────────────────────────────
    {
        "id": "greater-singapore",
        "name": "Singapore & Johor",
        "country": "Singapore",
        "cities": [
            "Singapore", "Johor Bahru", "Batam",
        ],
    },
    {
        "id": "greater-jakarta",
        "name": "Greater Jakarta (Jabodetabek)",
        "country": "Indonesia",
        "cities": [
            "Jakarta", "Bekasi", "Depok", "Tangerang", "Bogor",
            "Bandung", "Surabaya", "Medan", "Semarang", "Makassar",
        ],
    },
    {
        "id": "greater-manila",
        "name": "Metro Manila",
        "country": "Philippines",
        "cities": [
            "Manila", "Quezon City", "Caloocan", "Las Piñas",
            "Makati", "Pasig", "Taguig", "Cebu City", "Davao",
        ],
    },
    {
        "id": "greater-bangkok",
        "name": "Greater Bangkok",
        "country": "Thailand",
        "cities": [
            "Bangkok", "Nonthaburi", "Pak Kret", "Samut Prakan",
            "Chiang Mai", "Pattaya", "Phuket",
        ],
    },
    {
        "id": "greater-kuala-lumpur",
        "name": "Greater Kuala Lumpur",
        "country": "Malaysia",
        "cities": [
            "Kuala Lumpur", "Petaling Jaya", "Shah Alam", "Klang",
            "Subang Jaya", "George Town", "Ipoh", "Johor Bahru",
        ],
    },
    {
        "id": "greater-ho-chi-minh",
        "name": "Ho Chi Minh City Metro",
        "country": "Vietnam",
        "cities": [
            "Ho Chi Minh City", "Hanoi", "Da Nang", "Cần Thơ",
            "Biên Hòa", "Vũng Tàu",
        ],
    },
    {
        "id": "greater-yangon",
        "name": "Greater Yangon",
        "country": "Myanmar",
        "cities": [
            "Yangon", "Mandalay", "Naypyidaw",
        ],
    },

    # ── Australia & New Zealand ─────────────────────────────────────────────────
    {
        "id": "greater-sydney",
        "name": "Greater Sydney",
        "country": "Australia",
        "cities": [
            "Sydney", "Parramatta", "Liverpool", "Penrith",
            "Wollongong", "Newcastle", "Manly", "Bondi",
        ],
    },
    {
        "id": "greater-melbourne",
        "name": "Greater Melbourne",
        "country": "Australia",
        "cities": [
            "Melbourne", "Geelong", "Ballarat", "Bendigo",
            "Shepparton", "St Kilda", "Fitzroy",
        ],
    },
    {
        "id": "greater-brisbane",
        "name": "Greater Brisbane",
        "country": "Australia",
        "cities": [
            "Brisbane", "Gold Coast", "Sunshine Coast", "Townsville",
            "Cairns", "Toowoomba", "Ipswich",
        ],
    },
    {
        "id": "greater-perth",
        "name": "Greater Perth",
        "country": "Australia",
        "cities": [
            "Perth", "Mandurah", "Bunbury", "Geraldton",
            "Albany", "Fremantle",
        ],
    },
    {
        "id": "greater-auckland",
        "name": "Greater Auckland & New Zealand",
        "country": "New Zealand",
        "cities": [
            "Auckland", "Wellington", "Christchurch", "Hamilton",
            "Tauranga", "Napier", "Dunedin", "Palmerston North",
        ],
    },

    # ── Latin America ──────────────────────────────────────────────────────────
    {
        "id": "greater-sao-paulo",
        "name": "Greater São Paulo",
        "country": "Brazil",
        "cities": [
            "São Paulo", "Santos", "Guarulhos", "Campinas", "Osasco",
            "Santo André", "São Bernardo do Campo", "Sorocaba", "Ribeirão Preto",
        ],
    },
    {
        "id": "greater-rio",
        "name": "Greater Rio de Janeiro",
        "country": "Brazil",
        "cities": [
            "Rio de Janeiro", "Niterói", "Nova Iguaçu", "Duque de Caxias",
            "Belford Roxo", "São Gonçalo", "Petrópolis",
        ],
    },
    {
        "id": "greater-buenos-aires",
        "name": "Greater Buenos Aires",
        "country": "Argentina",
        "cities": [
            "Buenos Aires", "Córdoba", "Rosario", "La Plata", "Mar del Plata",
            "Mendoza", "San Miguel de Tucumán", "Santa Fe",
        ],
    },
    {
        "id": "greater-bogota",
        "name": "Greater Bogotá",
        "country": "Colombia",
        "cities": [
            "Bogotá", "Medellín", "Cali", "Barranquilla", "Cartagena",
            "Cúcuta", "Bucaramanga", "Pereira", "Santa Marta",
        ],
    },
    {
        "id": "greater-lima",
        "name": "Greater Lima",
        "country": "Peru",
        "cities": [
            "Lima", "Callao", "Arequipa", "Trujillo", "Chiclayo", "Piura",
        ],
    },
    {
        "id": "greater-santiago",
        "name": "Greater Santiago",
        "country": "Chile",
        "cities": [
            "Santiago", "Valparaíso", "Viña del Mar", "Concepción",
            "La Serena", "Antofagasta", "Temuco",
        ],
    },
    {
        "id": "greater-caracas",
        "name": "Greater Caracas",
        "country": "Venezuela",
        "cities": [
            "Caracas", "Maracaibo", "Valencia", "Barquisimeto",
            "Maracay", "Barcelona",
        ],
    },
    {
        "id": "greater-quito",
        "name": "Greater Quito",
        "country": "Ecuador",
        "cities": [
            "Quito", "Guayaquil", "Cuenca", "Manta",
        ],
    },
    {
        "id": "greater-lima-peru",
        "name": "Andean Cities",
        "country": "Peru",
        "cities": [
            "Lima", "La Paz", "Santa Cruz de la Sierra", "Cochabamba",
            "Asunción", "Montevideo",
        ],
    },

    # ── Africa ─────────────────────────────────────────────────────────────────
    {
        "id": "greater-lagos",
        "name": "Lagos Metro",
        "country": "Nigeria",
        "cities": [
            "Lagos", "Ibadan", "Abuja", "Port Harcourt", "Kano",
            "Kaduna", "Benin City", "Enugu", "Onitsha",
        ],
    },
    {
        "id": "greater-nairobi",
        "name": "Greater Nairobi",
        "country": "Kenya",
        "cities": [
            "Nairobi", "Mombasa", "Kisumu", "Nakuru", "Eldoret",
            "Kampala", "Dar es Salaam", "Addis Ababa",
        ],
    },
    {
        "id": "greater-johannesburg",
        "name": "Johannesburg–Pretoria (Gauteng)",
        "country": "South Africa",
        "cities": [
            "Johannesburg", "Pretoria", "Cape Town", "Durban",
            "Port Elizabeth", "Bloemfontein", "East London",
            "Soweto", "Sandton",
        ],
    },
    {
        "id": "greater-casablanca",
        "name": "Greater Casablanca",
        "country": "Morocco",
        "cities": [
            "Casablanca", "Rabat", "Fez", "Marrakesh", "Tangier",
            "Agadir", "Oujda", "Kenitra", "Tetouan",
        ],
    },
    {
        "id": "greater-accra",
        "name": "Greater Accra",
        "country": "Ghana",
        "cities": [
            "Accra", "Kumasi", "Cape Coast", "Tamale", "Tema",
        ],
    },
    {
        "id": "greater-dakar",
        "name": "Greater Dakar",
        "country": "Senegal",
        "cities": [
            "Dakar", "Abidjan", "Conakry", "Freetown", "Monrovia",
            "Bamako", "Ouagadougou", "Lomé", "Cotonou",
        ],
    },
    {
        "id": "greater-addis",
        "name": "Greater Addis Ababa",
        "country": "Ethiopia",
        "cities": [
            "Addis Ababa", "Dire Dawa", "Hargeisa", "Asmara",
            "Djibouti", "Mogadishu",
        ],
    },
    {
        "id": "greater-algiers",
        "name": "Greater Algiers",
        "country": "Algeria",
        "cities": [
            "Algiers", "Oran", "Constantine", "Annaba", "Blida",
            "Tunis", "Sfax", "Tripoli",
        ],
    },

]

# ── Schema ─────────────────────────────────────────────────────────────────────

class MetroAreaOut(BaseModel):
    id: str
    name: str
    country: str
    city_ids: str          # comma-separated city IDs that exist in the DB
    city_names: List[str]  # matched city names (for display)
    city_count: int


# ── Cache ─────────────────────────────────────────────────────────────────────

_cache: List[MetroAreaOut] = []
_cache_ts: float = 0.0
_TTL = 3600  # 1 hour — changes only when new cities are scraped


def _build_metro_list(db: Session) -> List[MetroAreaOut]:
    result = []
    for metro in METRO_AREAS:
        # Fetch all cities that match any name in this metro's city list
        # Use case-insensitive matching
        placeholders = ", ".join([f":n{i}" for i in range(len(metro["cities"]))])
        params = {f"n{i}": name for i, name in enumerate(metro["cities"])}

        rows = db.execute(text(f"""
            SELECT MIN(c.id) as city_id, c.name
            FROM cities c
            WHERE LOWER(c.name) IN ({placeholders})
              AND c.id IN (
                  SELECT DISTINCT v.city_id
                  FROM venues v
                  WHERE v.city_id IS NOT NULL
                    AND v.id IN (
                        SELECT DISTINCT e.venue_id
                        FROM events e
                        WHERE e.venue_id IS NOT NULL
                    )
              )
            GROUP BY LOWER(c.name)
            ORDER BY c.name
        """), {k: v.lower() for k, v in params.items()}).fetchall()

        if not rows:
            continue  # skip metros with no matching cities in DB

        city_ids = ",".join(str(r[0]) for r in rows)
        city_names = [r[1] for r in rows]

        result.append(MetroAreaOut(
            id=metro["id"],
            name=metro["name"],
            country=metro["country"],
            city_ids=city_ids,
            city_names=city_names,
            city_count=len(rows),
        ))

    return result


def warm_metro_cache():
    """Call once at startup alongside warm_cities_cache."""
    global _cache, _cache_ts
    db = SessionLocal()
    try:
        _cache = _build_metro_list(db)
        _cache_ts = time.time()
    finally:
        db.close()


@router.get("", response_model=List[MetroAreaOut])
def list_metro_areas(db: Session = Depends(get_db)):
    global _cache, _cache_ts
    if _cache and (time.time() - _cache_ts) < _TTL:
        return _cache
    _cache = _build_metro_list(db)
    _cache_ts = time.time()
    return _cache
