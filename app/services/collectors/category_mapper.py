from __future__ import annotations
"""Maps external API category taxonomies to our 24 internal categories."""

TICKETMASTER_SEGMENT_MAP = {
    "KZFzniwnSyZfZ7v7nJ": "Music",
    "KZFzniwnSyZfZ7v7na": "Art",
    "KZFzniwnSyZfZ7v7nE": "Sports",   # Ticketmaster "Sports" segment
    "KZFzniwnSyZfZ7v7n1": "Comedy",
    "KZFzniwnSyZfZ7v7nI": "Film",
}

TICKETMASTER_GENRE_MAP = {
    "Classical": "Music",
    "Opera": "Music",
    "Jazz": "Music",
    "Rock": "Music",
    "Pop": "Music",
    "R&B": "Music",
    "Hip-Hop/Rap": "Music",
    "Country": "Music",
    "Electronic": "Music",
    "Comedy": "Comedy",
    "Dance": "Dance",
    "Theatre": "Art",
    "Musical": "Art",
    "Film": "Film",
    "Family": "Festival",
    "Circus & Specialty Acts": "Festival",
}

EVENTBRITE_MAP = {
    "Music": "Music",
    "Business & Professional": "Career",
    "Food & Drink": "Food & Drink",
    "Community & Culture": "Festival",
    "Performing & Visual Arts": "Art",
    "Film, Media & Entertainment": "Film",
    "Sports & Fitness": "Sports",
    "Health & Wellness": "Fitness",
    "Science & Technology": "Technology",
    "Travel & Outdoor": "Outdoor",
    "Charity & Causes": "Charity",
    "Religion & Spirituality": "Religious",
    "Family & Education": "Academic",
    "Seasonal & Holiday": "Festival",
    "Government & Politics": "Political",
    "Fashion & Beauty": "Art",
    "Home & Lifestyle": "Home & Garden",
    "Auto, Boat & Air": "Automotive",
    "Hobbies & Special Interest": "Workshop",
    "Other": None,
    "School Activities": "Academic",
}

SEATGEEK_MAP = {
    "concert": "Music",
    "sports": "Sports",
    "theater": "Art",
    "comedy": "Comedy",
    "dance_performance_tour": "Dance",
    "family": "Festival",
    "film": "Film",
    "literary": "Literature",
    "food_and_drink": "Food & Drink",
}

PREDICTHQ_MAP = {
    "concerts": "Music",
    "conferences": "Technology",
    "expos": "Technology",
    "festivals": "Festival",
    "performing-arts": "Art",
    "community": "Charity",
    "sports": "Sports",
    "academic": "Academic",
    "politics": "Political",
    "religion": "Religious",
    "outdoor": "Outdoor",
}

LUMA_MAP = {
    "Technology":   "Technology",
    "Networking":   "Career",
    "Education":    "Workshop",
    "Music":        "Music",
    "Art":          "Art",
    "Food":         "Food & Drink",
    "Sports":       "Sports",
    "Fitness":      "Fitness",
    "Gaming":       "Gaming",
    "Charity":      "Charity",
    "Politics":     "Political",
    "Religion":     "Religious",
    "Automotive":   "Automotive",
    "Craft":        "Craft",
    "Outdoor":      "Outdoor",
    "Pet":          "Pet",
}


def map_category(source: str, raw_category: str) -> str | None:
    """Map an external category to one of our 24 categories."""
    maps = {
        "ticketmaster_segment": TICKETMASTER_SEGMENT_MAP,
        "ticketmaster_genre": TICKETMASTER_GENRE_MAP,
        "eventbrite": EVENTBRITE_MAP,
        "seatgeek": SEATGEEK_MAP,
        "predicthq": PREDICTHQ_MAP,
        "luma": LUMA_MAP,
    }
    source_map = maps.get(source, {})
    return source_map.get(raw_category)
