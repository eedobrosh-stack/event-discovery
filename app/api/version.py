"""Version endpoint — exposes the deployed git commit so the frontend can
show a build badge.

On Render, the SHA comes from the auto-injected `RENDER_GIT_COMMIT` env var.
Locally (or anywhere RENDER_* isn't set), we fall back to `git rev-parse HEAD`
so the dev server still has a meaningful badge.
"""
from __future__ import annotations

import os
import subprocess
from functools import lru_cache
from pathlib import Path

from fastapi import APIRouter

router = APIRouter(prefix="/api/version", tags=["version"])

_REPO_URL = "https://github.com/eedobrosh-stack/event-discovery"


@lru_cache(maxsize=1)
def _local_sha() -> str | None:
    """Run `git rev-parse HEAD` once at startup. Cached for the life of the
    process; we never need to re-read it (the SHA can't change without a
    redeploy, which restarts the process)."""
    try:
        repo_root = Path(__file__).resolve().parent.parent.parent
        out = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=str(repo_root),
            stderr=subprocess.DEVNULL,
            timeout=2,
        )
        return out.decode().strip() or None
    except Exception:
        return None


@router.get("")
def get_version() -> dict:
    sha = os.getenv("RENDER_GIT_COMMIT") or _local_sha()
    branch = os.getenv("RENDER_GIT_BRANCH") or "local"
    return {
        "sha":    sha,
        "short":  (sha or "dev")[:7],
        "branch": branch,
        "url":    f"{_REPO_URL}/commit/{sha}" if sha else None,
    }
