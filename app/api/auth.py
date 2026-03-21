"""Google OAuth2 flow for Google Sheets export."""

from __future__ import annotations

import os
import secrets
import traceback
from typing import Optional

# Allow HTTP redirect URIs for localhost development
os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

from fastapi import APIRouter, Request, Response
from fastapi.responses import RedirectResponse
from google_auth_oauthlib.flow import Flow

from app.config import settings

router = APIRouter(prefix="/api/auth", tags=["auth"])

# In-memory credential store keyed by session token.
_credentials_store: dict = {}

# Store the actual Flow objects so PKCE code_verifier is preserved
_pending_flows: dict = {}  # state -> (session_token, flow)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

SESSION_COOKIE = "sheets_session"


def _make_flow() -> Flow:
    client_config = {
        "web": {
            "client_id": settings.GOOGLE_CLIENT_ID,
            "client_secret": settings.GOOGLE_CLIENT_SECRET,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": [settings.GOOGLE_REDIRECT_URI],
        }
    }
    flow = Flow.from_client_config(client_config, scopes=SCOPES)
    flow.redirect_uri = settings.GOOGLE_REDIRECT_URI
    return flow


def get_credentials(session_token):
    """Return stored google.oauth2.credentials.Credentials or None."""
    if not session_token:
        return None
    creds = _credentials_store.get(session_token)
    if creds and creds.expired and creds.refresh_token:
        from google.auth.transport.requests import Request as GRequest
        creds.refresh(GRequest())
        _credentials_store[session_token] = creds
    return creds


@router.get("/google")
def google_auth_start(request: Request):
    """Redirect user to Google consent screen."""
    if not settings.GOOGLE_CLIENT_ID or not settings.GOOGLE_CLIENT_SECRET:
        return {"error": "Google OAuth credentials not configured in .env"}

    session_token = request.cookies.get(SESSION_COOKIE) or secrets.token_urlsafe(32)

    flow = _make_flow()
    auth_url, state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
    )

    # Store the flow object itself so code_verifier is preserved for the callback
    _pending_flows[state] = (session_token, flow)

    response = RedirectResponse(url=auth_url)
    response.set_cookie(SESSION_COOKIE, session_token, httponly=True, samesite="lax", path="/")
    return response


@router.get("/google/callback")
def google_auth_callback(request: Request, state: str, code: str):
    """Handle the OAuth callback from Google."""
    try:
        pending = _pending_flows.pop(state, None)
        if not pending:
            return RedirectResponse(url="/?auth_error=invalid_state")

        session_token, flow = pending

        # Use the SAME flow object that generated the auth URL (has the code_verifier)
        flow.fetch_token(code=code)

        _credentials_store[session_token] = flow.credentials

        response = RedirectResponse(url="/?sheets_auth=success")
        response.set_cookie(SESSION_COOKIE, session_token, httponly=True, samesite="lax", path="/")
        return response
    except Exception as e:
        traceback.print_exc()
        error_msg = str(e).replace(" ", "+")
        return RedirectResponse(url=f"/?auth_error={error_msg}")
