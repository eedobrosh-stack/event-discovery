from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str = "sqlite:///./data/events.db"

    # API Keys
    EVENTBRITE_TOKEN: str = ""
    TICKETMASTER_KEY: str = ""
    SEATGEEK_CLIENT_ID: str = ""
    SEATGEEK_SECRET: str = ""
    PREDICTHQ_TOKEN: str = ""
    YOUTUBE_API_KEY: str = ""
    BANDSINTOWN_APP_ID: str = ""
    SERPER_API_KEY: str = ""       # google.serper.dev — paid fallback for venue URL search
    CRICAPI_KEY: str = ""          # cricapi.com — free 100 req/day; sign up at https://cricapi.com
    SPOTIFY_CLIENT_ID: str = ""    # developer.spotify.com — Client Credentials flow, no user login
    SPOTIFY_CLIENT_SECRET: str = ""

    # Google OAuth
    GOOGLE_CLIENT_ID: str = ""
    GOOGLE_CLIENT_SECRET: str = ""
    GOOGLE_REDIRECT_URI: str = "http://localhost:8000/api/auth/google/callback"

    # Scheduling
    SCRAPE_INTERVAL_HOURS: int = 12
    CLEANUP_DAYS_AGO: int = 7
    RATE_LIMIT_PER_SECOND: int = 2

    # extra="ignore" — keeps unrelated keys in .env (e.g. GEMINI_API_KEY used by
    # offline scripts) from breaking app startup.
    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}


settings = Settings()
