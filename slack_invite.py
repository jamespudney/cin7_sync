"""slack_invite.py (v2.67.191)
==================================

Send a Slack DM invite to a newly-created dashboard user. Looks
up the user in Slack by email, opens a DM channel, posts a
welcome message with the dashboard URL and their display name
(the name they type at the gate to sign in).

Why this exists:
  • Per James — admins create users on the User Permissions
    page; the user needs to know (a) the URL and (b) what name
    to type. Manual handoff is friction; Slack DM is the team's
    native channel.
  • We deliberately DON'T include the shared APP_PASSWORD in
    the DM. Putting that into chat history is a mild leak risk
    and any admin can pass it 1:1.

Required Slack scopes on the bot token:
  - users:read       (so users.lookupByEmail works)
  - users:read.email (same)
  - chat:write       (already present — we post messages all
                       day)
  - im:write         (so we can open a DM channel; many bot
                       installs have this implicitly via the
                       channel-write scope)

Public API:
  send_invite(email, display_name, role) -> (ok: bool,
                                              detail: str)
"""

from __future__ import annotations

import logging
import os
from typing import Tuple

import requests

log = logging.getLogger("slack_invite")


_SLACK_API_BASE = "https://slack.com/api"


def _dashboard_url() -> str:
    """Where the user should go to sign in. Configurable so
    we can switch domains without code changes."""
    return (
        os.environ.get("APP_DASHBOARD_URL", "").strip()
        or "https://wired4signs-app.onrender.com"
    )


def _bot_token() -> str:
    return os.environ.get("SLACK_BOT_TOKEN", "").strip()


def _api(path: str, payload: dict) -> dict:
    """POST to a Slack API endpoint with the bot token. Returns
    the parsed JSON; never raises."""
    token = _bot_token()
    if not token:
        return {"ok": False,
                  "error": "SLACK_BOT_TOKEN not set"}
    try:
        r = requests.post(
            f"{_SLACK_API_BASE}/{path}",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json;charset=utf-8",
            },
            json=payload, timeout=15)
        return r.json()
    except Exception as exc:
        return {"ok": False, "error": f"http_error: {exc}"}


def _lookup_user_by_email(email: str) -> Tuple[str, str]:
    """Return (slack_user_id, detail). user_id is empty string
    on miss; detail describes the failure reason."""
    if not email or "@" not in email:
        return "", "email is empty or malformed"
    r = requests.get(
        f"{_SLACK_API_BASE}/users.lookupByEmail",
        headers={"Authorization": f"Bearer {_bot_token()}"},
        params={"email": email}, timeout=15)
    try:
        data = r.json()
    except Exception as exc:
        return "", f"non-json response: {exc}"
    if not data.get("ok"):
        # Common errors: users_not_found (typo / not in
        # workspace), missing_scope (token lacks
        # users:read.email).
        return "", f"slack_error: {data.get('error')}"
    user = data.get("user") or {}
    return (str(user.get("id") or ""), "ok")


def _open_dm(slack_user_id: str) -> Tuple[str, str]:
    """Open a DM channel with a Slack user. Returns
    (channel_id, detail)."""
    data = _api("conversations.open",
                  {"users": slack_user_id})
    if not data.get("ok"):
        return "", f"slack_error: {data.get('error')}"
    return (str((data.get("channel") or {}).get("id") or ""),
              "ok")


def _post_message(channel_id: str, text: str) -> Tuple[bool, str]:
    """Post a message to a Slack channel. Returns (ok, detail)."""
    data = _api("chat.postMessage", {
        "channel": channel_id,
        "text": text,
        "unfurl_links": False,
        "unfurl_media": False,
    })
    if not data.get("ok"):
        return False, f"slack_error: {data.get('error')}"
    return True, str(data.get("ts") or "")


def _compose_invite(display_name: str, role: str) -> str:
    """Build the welcome DM body. Keep it short and concrete —
    URL + the name to type. No password (admin shares 1:1)."""
    url = _dashboard_url()
    role_blurb = {
        "admin": (
            "You've been added as an *admin*, so you'll see "
            "every section + the User Permissions page where "
            "you can manage other team members."),
        "buyer": (
            "Your role is *buyer* — you'll see the ordering "
            "workbench, slow movers, supplier pricing, and "
            "the AI Assistant."),
        "sales": (
            "Your role is *sales* — you'll see the AI "
            "Assistant for stock and customer questions, "
            "plus product detail pages."),
        "viewer": (
            "Your role is *viewer* — read-only access to the "
            "main dashboards."),
    }.get((role or "sales").lower(),
            f"Your role is *{role}*.")

    return (
        f":wave: *Welcome to the Wired4Signs ops dashboard, "
        f"{display_name}!*\n\n"
        f"You've been invited by an admin. "
        f"{role_blurb}\n\n"
        f":link: *Sign in here:* {url}\n"
        f":bust_in_silhouette: *Type this name at the gate:* "
        f"`{display_name}`  (case doesn't matter — the system "
        f"will match your existing profile)\n\n"
        f"_The shared app password will be sent to you "
        f"separately. Ask the admin if you don't have it._\n\n"
        f":bulb: *What you can do here:*\n"
        f"• Run live stock queries (\"is SO-XXXXX available?\")\n"
        f"• Browse slow movers + clearance value tied up\n"
        f"• Check incoming POs and ETAs\n"
        f"• Ask the AI Assistant anything about CIN7 / Shopify "
        f"data\n\n"
        f"Questions? DM the admin who set this up, or post in "
        f"#bot-question.")


def send_invite(email: str,
                  display_name: str,
                  role: str = "sales"
                  ) -> Tuple[bool, str]:
    """Send a welcome DM to a Slack user identified by email.
    Returns (ok, detail). On success, detail is the message ts.
    On failure, detail is a short human-readable reason the
    admin UI can surface."""
    if not _bot_token():
        return False, "SLACK_BOT_TOKEN not configured"
    if not email or "@" not in email:
        return False, "email is required and must be valid"

    user_id, reason = _lookup_user_by_email(email)
    if not user_id:
        return False, f"Slack user not found: {reason}"

    dm_id, reason = _open_dm(user_id)
    if not dm_id:
        return False, f"Couldn't open DM: {reason}"

    text = _compose_invite(display_name, role)
    ok, detail = _post_message(dm_id, text)
    if not ok:
        return False, f"DM post failed: {detail}"

    log.info(
        "Sent Slack invite to %s (%s) — role=%s, ts=%s",
        display_name, email, role, detail)
    return True, detail
