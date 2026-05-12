"""slack_oauth.py (v2.67.126)
================================

User-OAuth flow for the Viktor bridge.

The Slack-side bot we already deploy uses a single workspace-wide
BOT TOKEN. That bot token can post messages but always appears as
the bot — which Viktor (and any other Slack AI app) filters out as
abuse-prevention. To make Viktor respond, our messages must look
like they came from a real human.

This module manages USER-OAUTH tokens, per-staff-member. Each
team member authorises the dashboard once; we store their
user-scoped access token (encrypted at rest); the dashboard's AI
Assistant then uses that token to post to Slack AS that user
when forwarding marketing questions to Viktor.

Public API
----------
- `build_authorize_url(state)`  — kick off the OAuth dance
- `exchange_code_for_token(code)` — callback handler swaps code → token
- `encrypt_token(plaintext)` / `decrypt_token(ciphertext)`
- `get_user_token(user_id)`     — returns the decrypted user-scoped
                                  access token, or None if not connected
- `post_as_user(user_id, channel_id, text)` — posts to Slack on
                                  the user's behalf, returns the
                                  message's (channel, ts, thread_ts)

Required env vars
-----------------
- SLACK_OAUTH_CLIENT_ID
- SLACK_OAUTH_CLIENT_SECRET
- SLACK_OAUTH_REDIRECT_URI       (must match the redirect set in
                                  the Slack app's OAuth & Permissions
                                  page; e.g. https://<dashboard>/
                                  ?slack_oauth=callback)
- SLACK_USER_TOKEN_ENCRYPTION_KEY (Fernet key — generate via
                                   `cryptography.fernet.Fernet.generate_key()`
                                   and store as a base64 string)

Slack App configuration (one-time, manual)
------------------------------------------
In the Slack app's settings (api.slack.com/apps/<your-app-id>):
1. OAuth & Permissions → User Token Scopes → add `chat:write`
2. Redirect URLs → add the SLACK_OAUTH_REDIRECT_URI above
3. Save changes; re-install the app to your workspace so the new
   scopes are recognised
"""

from __future__ import annotations

import logging
import os
import time
import urllib.parse
from typing import Optional, Tuple

import requests

try:
    from cryptography.fernet import Fernet, InvalidToken
except ImportError:  # noqa: BLE001
    # Cryptography is in requirements.txt but be defensive — if
    # the package is missing in this env, the import error gives
    # a clear message rather than a runtime AttributeError.
    Fernet = None  # type: ignore
    InvalidToken = Exception  # type: ignore

import db

log = logging.getLogger("slack_oauth")

SLACK_AUTH_URL = "https://slack.com/oauth/v2/authorize"
SLACK_TOKEN_URL = "https://slack.com/api/oauth.v2.access"
SLACK_POST_URL = "https://slack.com/api/chat.postMessage"

# User-scoped scope needed to post AS the user.
USER_SCOPES = ["chat:write"]


# ---------------------------------------------------------------------------
# Encryption helpers
# ---------------------------------------------------------------------------
def _get_fernet() -> "Fernet":
    """Build a Fernet instance from the env-var key. Raises a
    clear error if misconfigured — easier to debug than a silent
    encryption failure later."""
    if Fernet is None:
        raise RuntimeError(
            "cryptography package not installed; add `cryptography` "
            "to requirements.txt")
    key = os.environ.get(
        "SLACK_USER_TOKEN_ENCRYPTION_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "SLACK_USER_TOKEN_ENCRYPTION_KEY env var not set. "
            "Generate with: python -c \"from cryptography.fernet "
            "import Fernet; print(Fernet.generate_key().decode())\""
        )
    try:
        return Fernet(key.encode())
    except Exception as exc:
        raise RuntimeError(
            f"Invalid SLACK_USER_TOKEN_ENCRYPTION_KEY: {exc}. "
            f"Must be a 32-byte url-safe-base64 string (Fernet "
            f"format).") from exc


def encrypt_token(plaintext: str) -> str:
    """Encrypt a plaintext access token. Returned string is
    base64; safe to store in SQLite TEXT column."""
    if not plaintext:
        return ""
    f = _get_fernet()
    return f.encrypt(plaintext.encode()).decode()


def decrypt_token(ciphertext: str) -> Optional[str]:
    """Decrypt a stored access token. Returns None if the
    ciphertext is empty or fails to decrypt (rotated key, etc.)."""
    if not ciphertext:
        return None
    f = _get_fernet()
    try:
        return f.decrypt(ciphertext.encode()).decode()
    except (InvalidToken, Exception) as exc:
        log.warning(
            "Failed to decrypt Slack user token (probably key "
            "rotated): %s", exc)
        return None


# ---------------------------------------------------------------------------
# OAuth flow
# ---------------------------------------------------------------------------
def _oauth_config() -> dict:
    """Read OAuth env vars + raise if any required ones are
    missing. Centralised so each caller doesn't reimplement the
    check."""
    cfg = {
        "client_id": os.environ.get("SLACK_OAUTH_CLIENT_ID", "").strip(),
        "client_secret": os.environ.get(
            "SLACK_OAUTH_CLIENT_SECRET", "").strip(),
        "redirect_uri": os.environ.get(
            "SLACK_OAUTH_REDIRECT_URI", "").strip(),
    }
    missing = [k for k, v in cfg.items() if not v]
    if missing:
        raise RuntimeError(
            f"Slack OAuth missing env vars: {missing}. See "
            f"slack_oauth.py docstring for setup steps.")
    return cfg


def build_authorize_url(state: str) -> str:
    """Build the Slack OAuth authorization URL. Caller passes a
    `state` string (random, opaque) which Slack returns on the
    callback for CSRF protection."""
    cfg = _oauth_config()
    params = {
        # user_scope (singular) — Slack v2 OAuth differentiates
        # bot scopes from user scopes; we only need user scopes
        # here (chat:write for posting as the user).
        "user_scope": ",".join(USER_SCOPES),
        "client_id": cfg["client_id"],
        "redirect_uri": cfg["redirect_uri"],
        "state": state,
    }
    return f"{SLACK_AUTH_URL}?{urllib.parse.urlencode(params)}"


def exchange_code_for_token(code: str) -> dict:
    """Exchange an OAuth callback code for an access token.
    Returns the full Slack response dict on success (with
    `authed_user.access_token` etc.) or {} on failure."""
    cfg = _oauth_config()
    try:
        r = requests.post(
            SLACK_TOKEN_URL,
            data={
                "client_id": cfg["client_id"],
                "client_secret": cfg["client_secret"],
                "code": code,
                "redirect_uri": cfg["redirect_uri"],
            },
            timeout=15,
        )
    except requests.RequestException as exc:
        log.error("Slack oauth.v2.access network error: %s", exc)
        return {}
    if r.status_code != 200:
        log.error("Slack oauth.v2.access HTTP %d: %s",
                    r.status_code, r.text[:300])
        return {}
    body = r.json()
    if not body.get("ok"):
        log.error("Slack oauth.v2.access returned ok=false: %s",
                    body)
        return {}
    return body


def store_user_token_from_oauth(user_id: int,
                                       oauth_response: dict) -> None:
    """Extract the user-scoped token from Slack's OAuth response
    and persist it (encrypted). Slack v2 OAuth puts the user
    token at response.authed_user.access_token (NOT the top-level
    access_token, which is the bot token)."""
    au = (oauth_response.get("authed_user") or {})
    user_access_token = au.get("access_token") or ""
    if not user_access_token:
        raise RuntimeError(
            "OAuth response did not include authed_user."
            "access_token. Did the user grant the chat:write "
            "USER-token scope (not just bot scope)?")
    slack_user_id = au.get("id") or ""
    slack_team_id = (oauth_response.get("team") or {}).get("id") or ""
    scopes = au.get("scope") or ""
    encrypted = encrypt_token(user_access_token)
    db.upsert_slack_user_token(
        user_id=user_id,
        slack_user_id=slack_user_id,
        slack_team_id=slack_team_id,
        access_token_enc=encrypted,
        scopes=scopes,
    )
    log.info("Stored Slack user token for user_id=%s (slack_uid=%s)",
              user_id, slack_user_id)


def get_user_token(user_id: int) -> Optional[str]:
    """Return the decrypted user-scoped Slack access token for
    this user, or None if not connected. Bumps last_used_at."""
    row = db.get_slack_user_token_row(user_id)
    if not row:
        return None
    token = decrypt_token(row.get("access_token_enc") or "")
    if token:
        db.touch_slack_user_token(user_id)
    return token


def get_user_slack_id(user_id: int) -> Optional[str]:
    """Return the Slack U-id for a user, or None if not connected.
    Useful for the dashboard UI ('Connected as <@U...>')."""
    row = db.get_slack_user_token_row(user_id)
    if not row:
        return None
    return row.get("slack_user_id") or None


# ---------------------------------------------------------------------------
# Posting as the user
# ---------------------------------------------------------------------------
def post_as_user(user_id: int, channel_id: str,
                    text: str) -> Tuple[Optional[str],
                                              Optional[str]]:
    """Post a message to Slack on behalf of `user_id`. Uses their
    stored user-scoped token, so the message appears in Slack as
    posted by them — not by our bot.

    Returns (posted_ts, thread_ts). posted_ts is the ts of the
    message we just posted; thread_ts is the thread anchor (Slack
    sets this to the parent thread's ts if posting in a thread,
    or to posted_ts for top-level messages).

    Returns (None, None) on failure."""
    token = get_user_token(user_id)
    if not token:
        log.warning(
            "post_as_user: no Slack token stored for user_id=%s",
            user_id)
        return None, None
    try:
        r = requests.post(
            SLACK_POST_URL,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json; charset=utf-8",
            },
            json={
                "channel": channel_id,
                "text": text,
                "unfurl_links": False,
                "unfurl_media": False,
            },
            timeout=15,
        )
    except requests.RequestException as exc:
        log.error("post_as_user network error: %s", exc)
        return None, None
    if r.status_code != 200:
        log.error("post_as_user HTTP %d: %s",
                    r.status_code, r.text[:300])
        return None, None
    body = r.json()
    if not body.get("ok"):
        log.error("post_as_user returned ok=false: %s", body)
        # Common errors:
        #   not_in_channel — user isn't a member of that channel
        #   token_revoked  — user revoked the OAuth grant
        #   invalid_auth   — token wrong / encryption key rotated
        return None, None
    posted_ts = body.get("ts")
    # For top-level posts, Slack uses the same value for thread_ts.
    # If we ever post into an existing thread, thread_ts will be
    # the parent's ts.
    msg = body.get("message") or {}
    thread_ts = msg.get("thread_ts") or posted_ts
    return posted_ts, thread_ts


def is_user_connected(user_id: int) -> bool:
    """Cheap presence check for UI rendering — no decryption,
    just confirms a row exists."""
    return db.get_slack_user_token_row(user_id) is not None


def disconnect_user(user_id: int) -> None:
    """User clicked 'Disconnect' in the dashboard."""
    db.delete_slack_user_token(user_id)
    log.info("Disconnected Slack user token for user_id=%s", user_id)
