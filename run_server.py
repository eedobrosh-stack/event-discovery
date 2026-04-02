import sys
import os

DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(DIR)
if DIR not in sys.path:
    sys.path.insert(0, DIR)

# Ensure PYTHONPATH is set so uvicorn's reload subprocess can find 'app'
existing = os.environ.get("PYTHONPATH", "")
os.environ["PYTHONPATH"] = DIR + (":" + existing if existing else "")

import uvicorn
uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=False, app_dir=DIR)
