"""Entry point for the Supercaly backend server."""
import sys
import os

BASE = os.path.dirname(os.path.abspath(__file__))
if BASE not in sys.path:
    sys.path.insert(0, BASE)
os.chdir(BASE)

# Ensure the data directory exists (important for fresh deployments)
db_url = os.environ.get("DATABASE_URL", "sqlite:///./data/events.db")
if db_url.startswith("sqlite:///"):
    db_path = db_url.replace("sqlite:///", "")
    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

import uvicorn
port = int(os.environ.get("PORT", 8000))
uvicorn.run("app.main:app", host="0.0.0.0", port=port, reload=False, app_dir=BASE)
