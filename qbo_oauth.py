"""qbo_oauth.py (v2.67.211)
================================

OAuth2 flow for the QuickBooks Online (QBO) integration that powers
the Cashflow Management page.

Unlike the Slack bridge (one token per staff member), QBO is a
single company-wide connection: Wired4Signs has ONE QuickBooks
books file, so we store exactly one row in `qbo_connection`.

QBO token model — the important bit
-----------------------------------
- access_token   — short-lived, expires after ~1 hour.
- refresh_token  — long-lived (~100 days) BUT it ROTATES: every
                   time you refresh, Intuit hands back a NEW
                   refresh token and invalidates the old one.
                   So we MUST persist both tokens on every
                   refresh, or the connection silently dies.

Public API
----------
- `build_authorize_url(state)`        — kick off the OAuth dance
- `exchange_code_for_token(code, realm_id)` — callback → tokens
- `get_valid_access_token()`          — decrypted access token,
                                        auto-refreshing if stale
- `is_connected()`                    — cheap presence check
- `connection_info()`                 — dict for UI rendering
- `disconnect()`                      — drop stored tokens
- `encrypt_token` / `decrypt_token`

Required env vars
-----------------
- QBO_CLIENT_ID
- QBO_CLIENT_SECRET
- QBO_REDIRECT_URI        (must match the redirect registered in
                           the Intuit app; e.g.
                           https://wired4signs-app.onrender.com/
                           ?qbo_oauth=callback)
- QBO_ENVIRONMENT         ('sandbox' or 'production'; default
                           'sandbox')
- QBO_TOKEN_ENCRYPTION_KEY (Fernet key. If unset we fall back to
                           SLACK_USER_TOKEN_ENCRYPTION_KEY so a
                           single key can cover both integrations.)

Intuit app configuration (one-time, manual)
--------------------------------------------
In the Intuit developer portal (developer.intuit.com → your app):
1. Keys & OAuth → add the QBO_REDIRECT_URI above to Redirect URIs.
2. Note the Client ID / Client Secret for the matching environment
   (Development keys → sandbox; Production keys → production).
3. Scope used: com.intuit.quickbooks.accounting (accounting data).
"""

from __future__ import annotations

import datetime as _dt
import logging
import os
import time as _time
import urllib.parse
from typing import Optional

import requests

try:
    from cryptography.fernet import Fernet, InvalidToken
except ImportError:  # noqa: BLE001
    Fernet = None  # type: ignore
    InvalidToken = Exception  # type: ignore

import db

log = logging.getLogger("qbo_oauth")

# Intuit OAuth2 endpoints.
#
# v2.67.215 — rather than hardcode these, we read them at runtime
# from Intuit's OpenID Connect *discovery document* (the
# .well-known config), which is Intuit's recommended practice:
# if Intuit ever moves an endpoint, the discovery doc updates and
# we follow automatically. The constants below are kept ONLY as a
# fallback for when the discovery fetch fails (offline, transient
# network error) — they are the currently-published values.
_DISCOVERY_URL = {
    "production":
        "https://developer.api.intuit.com/.well-known/"
        "openid_configuration",
    "sandbox":
        "https://developer.api.intuit.com/.well-known/"
        "openid_sandbox_configuration",
}
_FALLBACK_AUTH_URL = "https://appcenter.intuit.com/connect/oauth2"
_FALLBACK_TOKEN_URL = ("https://oauth.platform.intuit.com/oauth2/"
                       "v1/tokens/bearer")
_FALLBACK_REVOKE_URL = ("https://developer.api.intuit.com/v2/"
                        "oauth2/tokens/revoke")

# Module-level cache of the discovered endpoints, keyed by
# environment. Populated lazily on first use; the discovery doc
# is effectively static so a process-lifetime cache is fine.
_discovery_cache: dict = {}

# Accounting scope — read/write access to the books data the
# Cashflow page needs (invoices, bills, P&L, cash-flow report).
QBO_SCOPES = ["com.intuit.quickbooks.accounting"]

# Refresh the access token this many seconds BEFORE its stated
# expiry, so an in-flight API call never races the boundary.
_REFRESH_SKEW_SECONDS = 120


# ---------------------------------------------------------------------------
# Encryption helpers
# ---------------------------------------------------------------------------
def _get_fernet() -> "Fernet":
    """Build a Fernet instance. Prefers QBO_TOKEN_ENCRYPTION_KEY;
    falls back to SLACK_USER_TOKEN_ENCRYPTION_KEY so one key can
    cover both integrations if the operator prefers."""
    if Fernet is None:
        raise RuntimeError(
            "cryptography package not installed; add `cryptography` "
            "to requirements.txt")
    key = (os.environ.get("QBO_TOKEN_ENCRYPTION_KEY", "").strip()
           or os.environ.get(
               "SLACK_USER_TOKEN_ENCRYPTION_KEY", "").strip())
    if not key:
        raise RuntimeError(
            "QBO_TOKEN_ENCRYPTION_KEY env var not set (and no "
            "SLACK_USER_TOKEN_ENCRYPTION_KEY to fall back on). "
            "Generate with: python -c \"from cryptography.fernet "
            "import Fernet; print(Fernet.generate_key().decode())\""
        )
    try:
        return Fernet(key.encode())
    except Exception as exc:
        raise RuntimeError(
            f"Invalid QBO token encryption key: {exc}. Must be a "
            f"32-byte url-safe-base64 string (Fernet format).") from exc


def encrypt_token(plaintext: str) -> str:
    """Encrypt a plaintext token. Returned string is base64; safe
    to store in a TEXT column."""
    if not plaintext:
        return ""
    return _get_fernet().encrypt(plaintext.encode()).decode()


def decrypt_token(ciphertext: str) -> Optional[str]:
    """Decrypt a stored token. Returns None on empty input or a
    decrypt failure (e.g. the encryption key was rotated)."""
    if not ciphertext:
        return None
    try:
        return _get_fernet().decrypt(ciphertext.encode()).decode()
    except (InvalidToken, Exception) as exc:  # noqa: BLE001
        log.warning(
            "Failed to decrypt QBO token (probably key rotated): %s",
            exc)
        return None


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
def environment() -> str:
    """'sandbox' or 'production'. Default sandbox — safe while the
    integration is still being built out."""
    env = os.environ.get("QBO_ENVIRONMENT", "").strip().lower()
    return "production" if env == "production" else "sandbox"


def _oauth_config() -> dict:
    """Read OAuth env vars + raise if any required ones are
    missing."""
    cfg = {
        "client_id": os.environ.get("QBO_CLIENT_ID", "").strip(),
        "client_secret": os.environ.get(
            "QBO_CLIENT_SECRET", "").strip(),
        "redirect_uri": os.environ.get(
            "QBO_REDIRECT_URI", "").strip(),
    }
    missing = [k for k, v in cfg.items() if not v]
    if missing:
        raise RuntimeError(
            f"QBO OAuth missing env vars: {missing}. See "
            f"qbo_oauth.py docstring for setup steps.")
    return cfg


def is_configured() -> bool:
    """True if the OAuth env vars are present — lets the UI show a
    helpful 'not configured' state instead of crashing."""
    try:
        _oauth_config()
        return True
    except RuntimeError:
        return False


# ---------------------------------------------------------------------------
# Endpoint discovery (OpenID Connect .well-known)
# ---------------------------------------------------------------------------
def _discover_endpoints() -> dict:
    """Fetch + cache Intuit's OAuth2 endpoints from its OpenID
    Connect discovery document (the .well-known config). This is
    Intuit's recommended practice — if Intuit moves an endpoint,
    the discovery doc updates and we follow automatically.

    Returns a dict with keys 'authorize', 'token', 'revoke'.
    Falls back to the published constants if the fetch fails, so
    the OAuth flow keeps working through a transient network
    error. Cached for the process lifetime (the doc is static)."""
    env = environment()
    cached = _discovery_cache.get(env)
    if cached:
        return cached
    endpoints = {
        "authorize": _FALLBACK_AUTH_URL,
        "token": _FALLBACK_TOKEN_URL,
        "revoke": _FALLBACK_REVOKE_URL,
    }
    url = _DISCOVERY_URL.get(env, _DISCOVERY_URL["sandbox"])
    try:
        r = requests.get(
            url, headers={"Accept": "application/json"}, timeout=15)
        if r.status_code == 200:
            doc = r.json()
            endpoints = {
                "authorize": (doc.get("authorization_endpoint")
                               or _FALLBACK_AUTH_URL),
                "token": (doc.get("token_endpoint")
                           or _FALLBACK_TOKEN_URL),
                "revoke": (doc.get("revocation_endpoint")
                            or _FALLBACK_REVOKE_URL),
            }
            log.info("QBO discovery doc loaded (%s): %s",
                     env, endpoints)
        else:
            log.warning(
                "QBO discovery doc HTTP %d — using fallback "
                "endpoints.", r.status_code)
    except (requests.RequestException, ValueError) as exc:
        log.warning(
            "QBO discovery doc fetch failed (%s) — using fallback "
            "endpoints.", exc)
    _discovery_cache[env] = endpoints
    return endpoints


# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------
def _utcnow() -> _dt.datetime:
    return _dt.datetime.now(_dt.timezone.utc)


def _iso(dt: _dt.datetime) -> str:
    """Serialise to a naive UTC ISO string — matches what the rest
    of the DB stores (datetime('now') yields naive UTC)."""
    return dt.astimezone(_dt.timezone.utc).replace(
        tzinfo=None).isoformat(sep=" ", timespec="seconds")


def _parse_dt(value) -> Optional[_dt.datetime]:
    """Parse a stored expiry value into an aware UTC datetime.
    psycopg returns datetime objects; SQLite returns strings —
    handle both."""
    if value is None or value == "":
        return None
    if isinstance(value, _dt.datetime):
        dt = value
    else:
        try:
            dt = _dt.datetime.fromisoformat(str(value).strip())
        except ValueError:
            try:
                dt = _dt.datetime.fromisoformat(
                    str(value).strip()[:19])
            except ValueError:
                return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_dt.timezone.utc)
    return dt


# ---------------------------------------------------------------------------
# OAuth flow
# ---------------------------------------------------------------------------
def build_authorize_url(state: str) -> str:
    """Build the Intuit OAuth2 authorization URL. `state` is an
    opaque random string echoed back on the callback for CSRF
    protection."""
    cfg = _oauth_config()
    params = {
        "client_id": cfg["client_id"],
        "redirect_uri": cfg["redirect_uri"],
        "response_type": "code",
        "scope": " ".join(QBO_SCOPES),
        "state": state,
    }
    authorize_url = _discover_endpoints()["authorize"]
    return f"{authorize_url}?{urllib.parse.urlencode(params)}"


def _token_request(payload: dict) -> dict:
    """POST to Intuit's token endpoint with HTTP Basic auth
    (client_id:client_secret). Used for both the authorization-code
    exchange and refreshes. Returns the parsed JSON body, or raises
    RuntimeError with a readable message."""
    cfg = _oauth_config()
    url = _discover_endpoints()["token"]
    # v2.67.216 — retry ONCE on a transient failure (network
    # error, HTTP 429, or 5xx). Deterministic 4xx failures
    # (invalid_grant, invalid_client, etc.) are NOT retried —
    # retrying them is pointless and can trip rate limits.
    last_error = ""
    for attempt in range(2):
        try:
            r = requests.post(
                url,
                data=payload,
                auth=(cfg["client_id"], cfg["client_secret"]),
                headers={"Accept": "application/json"},
                timeout=20,
            )
        except requests.RequestException as exc:
            last_error = f"network error: {exc}"
            if attempt == 0:
                _time.sleep(1.5)
                continue
            raise RuntimeError(
                f"QBO token endpoint {last_error}") from exc
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError as exc:
                raise RuntimeError(
                    f"QBO token endpoint returned non-JSON: "
                    f"{r.text[:300]}") from exc
        # Non-200 — retry only transient server-side statuses.
        if (r.status_code in (429, 500, 502, 503, 504)
                and attempt == 0):
            last_error = f"HTTP {r.status_code}"
            _time.sleep(1.5)
            continue
        # v2.67.217 — include Intuit's transaction id so support
        # can trace a failed token call.
        _tid = (r.headers.get("intuit_tid")
                or r.headers.get("Intuit-Tid") or "")
        log.error("QBO token endpoint HTTP %d (intuit_tid=%s): %s",
                  r.status_code, _tid, r.text[:300])
        raise RuntimeError(
            f"QBO token endpoint HTTP {r.status_code} "
            f"(intuit_tid={_tid}): {r.text[:300]}")
    raise RuntimeError(
        f"QBO token endpoint failed after retry: {last_error}")


def _persist_tokens(realm_id: str, body: dict,
                     connected_by: Optional[str]) -> None:
    """Encrypt + store the access/refresh tokens from a token
    response. Shared by the code exchange and refresh paths since
    QBO rotates the refresh token on every call."""
    access_token = body.get("access_token") or ""
    refresh_token = body.get("refresh_token") or ""
    if not access_token or not refresh_token:
        raise RuntimeError(
            f"QBO token response missing tokens: keys="
            f"{sorted(body.keys())}")
    now = _utcnow()
    # expires_in is the access-token lifetime (~3600s);
    # x_refresh_token_expires_in is the refresh-token lifetime
    # (~8726400s ≈ 101 days).
    access_ttl = int(body.get("expires_in") or 3600)
    refresh_ttl = int(
        body.get("x_refresh_token_expires_in") or 8726400)
    db.save_qbo_connection(
        realm_id=realm_id,
        access_token_enc=encrypt_token(access_token),
        refresh_token_enc=encrypt_token(refresh_token),
        access_expires_at=_iso(
            now + _dt.timedelta(seconds=access_ttl)),
        refresh_expires_at=_iso(
            now + _dt.timedelta(seconds=refresh_ttl)),
        environment=environment(),
        connected_by=connected_by,
    )


def exchange_code_for_token(code: str, realm_id: str,
                            connected_by: Optional[str] = None
                            ) -> str:
    """Exchange an OAuth callback `code` for tokens and persist
    them. `realm_id` is the QBO company id Intuit appends to the
    callback URL as `?realmId=...`. Returns the realm_id on
    success; raises RuntimeError on failure."""
    if not code:
        raise RuntimeError("QBO callback had no authorization code.")
    if not realm_id:
        raise RuntimeError(
            "QBO callback had no realmId — cannot identify the "
            "company file.")
    cfg = _oauth_config()
    body = _token_request({
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": cfg["redirect_uri"],
    })
    _persist_tokens(realm_id, body, connected_by)
    log.info("QBO connected: realm_id=%s environment=%s by=%s",
             realm_id, environment(), connected_by)
    return realm_id


def _refresh_access_token(row: dict) -> Optional[str]:
    """Use the stored refresh token to obtain a fresh access
    token. Persists BOTH rotated tokens. Returns the new decrypted
    access token, or None on failure."""
    refresh_token = decrypt_token(row.get("refresh_token_enc") or "")
    if not refresh_token:
        log.error(
            "QBO refresh failed: stored refresh token could not be "
            "decrypted (encryption key rotated?).")
        return None
    try:
        body = _token_request({
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        })
    except RuntimeError as exc:
        log.error("QBO refresh failed: %s", exc)
        return None
    try:
        _persist_tokens(
            row.get("realm_id") or "",
            body,
            row.get("connected_by"),
        )
    except RuntimeError as exc:
        log.error("QBO refresh: could not persist new tokens: %s",
                  exc)
        return None
    log.info("QBO access token refreshed for realm_id=%s",
             row.get("realm_id"))
    return body.get("access_token") or None


def get_valid_access_token() -> Optional[str]:
    """Return a usable (decrypted, non-expired) QBO access token,
    transparently refreshing it if it is stale or about to expire.
    Returns None if QBO is not connected or the refresh fails."""
    row = db.get_qbo_connection()
    if not row:
        return None
    expires_at = _parse_dt(row.get("access_expires_at"))
    needs_refresh = (
        expires_at is None
        or _utcnow() >= (
            expires_at
            - _dt.timedelta(seconds=_REFRESH_SKEW_SECONDS)))
    if not needs_refresh:
        token = decrypt_token(row.get("access_token_enc") or "")
        if token:
            return token
        # Fall through to refresh if the stored token won't decrypt.
    return _refresh_access_token(row)


# ---------------------------------------------------------------------------
# Connection state / UI helpers
# ---------------------------------------------------------------------------
def is_connected() -> bool:
    """True if a QBO connection row exists. Does NOT validate the
    token — use get_valid_access_token() for that."""
    return db.get_qbo_connection() is not None


def connection_info() -> Optional[dict]:
    """Return a UI-friendly summary of the current connection, or
    None if not connected. Never exposes token material."""
    row = db.get_qbo_connection()
    if not row:
        return None
    refresh_expires = _parse_dt(row.get("refresh_expires_at"))
    refresh_days_left = None
    if refresh_expires is not None:
        refresh_days_left = max(
            0, (refresh_expires - _utcnow()).days)
    return {
        "realm_id": row.get("realm_id"),
        "environment": row.get("environment") or environment(),
        "connected_by": row.get("connected_by"),
        "connected_at": row.get("connected_at"),
        "updated_at": row.get("updated_at"),
        "access_expires_at": row.get("access_expires_at"),
        "refresh_expires_at": row.get("refresh_expires_at"),
        "refresh_days_left": refresh_days_left,
    }


def disconnect() -> None:
    """Disconnect QBO. Attempts a best-effort token revoke on the
    Intuit side, then drops the stored row regardless."""
    row = db.get_qbo_connection()
    if row:
        refresh_token = decrypt_token(
            row.get("refresh_token_enc") or "")
        if refresh_token and is_configured():
            cfg = _oauth_config()
            try:
                requests.post(
                    _discover_endpoints()["revoke"],
                    json={"token": refresh_token},
                    auth=(cfg["client_id"], cfg["client_secret"]),
                    headers={"Accept": "application/json"},
                    timeout=15,
                )
            except requests.RequestException as exc:
                log.warning(
                    "QBO token revoke call failed (clearing "
                    "locally anyway): %s", exc)
    db.clear_qbo_connection()
    log.info("QBO disconnected; stored tokens cleared.")
