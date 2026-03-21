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

    # Google OAuth
    GOOGLE_CLIENT_ID: str = ""
    GOOGLE_CLIENT_SECRET: str = ""
    GOOGLE_REDIRECT_URI: str = "http://localhost:8000/api/auth/google/callback"

    # Scheduling
    SCRAPE_INTERVAL_HOURS: int = 6
    CLEANUP_DAYS_AGO: int = 7
    RATE_LIMIT_PER_SECOND: int = 2

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
