"""Slack integration — message ingest + responder (v2.67.57).

Two responsibilities:

1. **Ingest** (this module) — poll Slack `conversations.history` for
   each configured channel every ~60s, write new messages into the
   local `slack_messages` table. Same pattern as cin7_sync /
   shipstation_sync — incremental cursor-based pulls so we don't
   re-fetch the whole channel history every time.

2. **Listen + respond** (slack_listener.py) — separate module that
   classifies un-processed messages, composes responses via the AI
   tool chain, posts threaded replies, mirrors to the audit channel.

Why the split: ingest needs to be fast and reliable so the AI's
read-only `get_slack_messages` tool always has fresh data, even if
the responder is broken / disabled / paused. Conversely, a
responder bug shouldn't stop messages from being captured.

CLI entrypoints:
    python slack_sync.py poll                 # one-shot pull
    python slack_sync.py loop --interval 60   # loop forever (used
                                              # by slack_loop.sh on
                                              # Render)
    python slack_sync.py backfill --hours 24  # initial pull on first
                                              # deploy

Env vars:
    SLACK_BOT_TOKEN     xoxb-... bot token (required)
    SLACK_AI_CHANNELS   comma-separated channel IDs the bot should
                        watch (e.g. C0123,C0456). Channels not in
                        this list are NEVER touched even if the bot
                        is invited to them.
    SLACK_AUDIT_CHANNEL channel ID where bot posts audit digests.
                        Optional; if empty, audit posts are skipped
                        (still written to slack_bot_responses table
                        for retrospective query).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
import db  # noqa: E402

LOG_FORMAT = "%(asctime)s  %(levelname)-8s %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
log = logging.getLogger("slack_sync")

SLACK_API = "https://slack.com/api"
DEFAULT_TIMEOUT = 30.0
MAX_RETRIES = 5


# ---------------------------------------------------------------------------
# Slack HTTP helpers
# ---------------------------------------------------------------------------


def _build_session(token: str) -> requests.Session:
    """Create a requests.Session with bot-token auth headers."""
    if not token:
        raise RuntimeError(
            "Missing SLACK_BOT_TOKEN env var. Get a Bot User OAuth "
            "token from api.slack.com/apps → your app → OAuth & "
            "Permissions, then set it on Render env vars.")
    s = requests.Session()
    s.headers.update({
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "User-Agent": "cin7_sync-slack/2.67.57",
    })
    return s


def _slack_get(session: requests.Session,
                method: str,
                params: Dict[str, Any]) -> Dict[str, Any]:
    """Call a Slack Web API method with retries on 429 / network errors.
    Slack returns 200 with `ok=false` on logical errors — we surface
    those as exceptions too so they can't be silently ignored."""
    url = f"{SLACK_API}/{method}"
    attempt = 0
    while True:
        attempt += 1
        try:
            r = session.get(url, params=params, timeout=DEFAULT_TIMEOUT)
        except requests.RequestException as exc:
            if attempt >= MAX_RETRIES:
                raise
            wait = 2 ** attempt
            log.warning("Slack %s network err %s — retrying in %ds",
                          method, exc, wait)
            time.sleep(wait)
            continue
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "5"))
            log.warning("Slack %s 429 — sleeping %ss", method, wait)
            time.sleep(wait)
            continue
        if not r.ok:
            raise RuntimeError(
                f"Slack {method} HTTP {r.status_code}: {r.text[:300]}")
        body = r.json() or {}
        if not body.get("ok"):
            err = body.get("error", "unknown")
            # `ratelimited` shouldn't happen here (429 catches it) but
            # belt-and-braces. `not_in_channel` means the bot wasn't
            # invited — surface clearly so the admin knows to add it.
            if err == "ratelimited":
                wait = int(body.get("retry_after", 5))
                time.sleep(wait)
                continue
            raise RuntimeError(
                f"Slack {method} error '{err}'. "
                f"Full response: {json.dumps(body)[:300]}")
        return body


def _slack_post(session: requests.Session,
                 method: str,
                 payload: Dict[str, Any]) -> Dict[str, Any]:
    """POST equivalent for chat.postMessage / reactions.add. Same
    retry shape as _slack_get."""
    url = f"{SLACK_API}/{method}"
    attempt = 0
    while True:
        attempt += 1
        try:
            r = session.post(url, json=payload, timeout=DEFAULT_TIMEOUT,
                             headers={"Content-Type":
                                       "application/json; charset=utf-8"})
        except requests.RequestException as exc:
            if attempt >= MAX_RETRIES:
                raise
            wait = 2 ** attempt
            log.warning("Slack %s network err %s — retrying in %ds",
                          method, exc, wait)
            time.sleep(wait)
            continue
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "5"))
            time.sleep(wait)
            continue
        if not r.ok:
            raise RuntimeError(
                f"Slack {method} HTTP {r.status_code}: {r.text[:300]}")
        body = r.json() or {}
        if not body.get("ok"):
            raise RuntimeError(
                f"Slack {method} error: {json.dumps(body)[:300]}")
        return body


# ---------------------------------------------------------------------------
# User / channel name resolution
# ---------------------------------------------------------------------------


_USER_CACHE: Dict[str, str] = {}
_CHANNEL_NAME_CACHE: Dict[str, str] = {}


def _resolve_user(session: requests.Session, user_id: str) -> str:
    """Resolve a Slack user_id to a display name. Cached per process —
    user names change rarely. Falls back to the raw ID on lookup
    failure (e.g. deactivated user)."""
    if not user_id:
        return ""
    if user_id in _USER_CACHE:
        return _USER_CACHE[user_id]
    try:
        body = _slack_get(session, "users.info", {"user": user_id})
        user = body.get("user") or {}
        name = (user.get("real_name") or user.get("name")
                or user.get("profile", {}).get("display_name") or user_id)
        _USER_CACHE[user_id] = name
        return name
    except Exception:
        _USER_CACHE[user_id] = user_id
        return user_id


def _resolve_channel(session: requests.Session,
                       channel_id: str) -> str:
    """Resolve channel_id to its #channel-name. Cached per process."""
    if not channel_id:
        return ""
    if channel_id in _CHANNEL_NAME_CACHE:
        return _CHANNEL_NAME_CACHE[channel_id]
    try:
        body = _slack_get(session, "conversations.info",
                            {"channel": channel_id})
        ch = body.get("channel") or {}
        name = ch.get("name") or channel_id
        _CHANNEL_NAME_CACHE[channel_id] = name
        return name
    except Exception:
        _CHANNEL_NAME_CACHE[channel_id] = channel_id
        return channel_id


# ---------------------------------------------------------------------------
# Bot self-id (so we never reply to our own posts)
# ---------------------------------------------------------------------------


_BOT_SELF_ID: Optional[str] = None


def get_bot_self_id(session: requests.Session) -> str:
    """Return our own bot user_id. Cached per process. Critical for
    the 'never reply to self' loop guard in the listener."""
    global _BOT_SELF_ID
    if _BOT_SELF_ID is not None:
        return _BOT_SELF_ID
    body = _slack_get(session, "auth.test", {})
    _BOT_SELF_ID = body.get("user_id") or ""
    log.info("Slack bot self user_id: %s (team: %s)",
              _BOT_SELF_ID, body.get("team"))
    return _BOT_SELF_ID


# ---------------------------------------------------------------------------
# Channel list
# ---------------------------------------------------------------------------


def _configured_channels() -> List[str]:
    raw = os.environ.get("SLACK_AI_CHANNELS", "").strip()
    if not raw:
        return []
    return [c.strip() for c in raw.split(",") if c.strip()]


# ---------------------------------------------------------------------------
# Ingest a single channel
# ---------------------------------------------------------------------------


def _ingest_channel(session: requests.Session,
                      channel_id: str,
                      bot_self_id: str,
                      lookback_hours: Optional[int] = None) -> int:
    """Pull new messages for one channel. Returns count of new
    messages stored. Uses the cursor in slack_channel_cursors so
    each call is incremental."""
    # Get the cursor for this channel.
    with db.connect() as c:
        row = c.execute(
            "SELECT last_ts FROM slack_channel_cursors "
            "WHERE channel_id = ?", (channel_id,)).fetchone()
        last_ts = (row["last_ts"] if row else None)

    # If we've never pulled this channel and lookback_hours is set
    # (backfill mode), seed the cursor with that age.
    oldest = None
    if last_ts:
        oldest = last_ts
    elif lookback_hours:
        oldest = str(int(time.time()) - lookback_hours * 3600) + ".000000"
    # else: leave None → Slack returns from beginning of channel
    # history (we cap at 200 messages for safety).

    cursor = None
    new_count = 0
    highest_ts = last_ts or "0"
    pages = 0
    while True:
        params = {
            "channel": channel_id,
            "limit": 200,
            "inclusive": "false",
        }
        if oldest:
            params["oldest"] = oldest
        if cursor:
            params["cursor"] = cursor
        body = _slack_get(session, "conversations.history", params)
        messages = body.get("messages") or []
        pages += 1

        for m in messages:
            if not isinstance(m, dict):
                continue
            ts = m.get("ts")
            if not ts:
                continue
            if ts > highest_ts:
                highest_ts = ts
            user_id = m.get("user") or m.get("bot_id") or ""
            is_bot = 1 if (m.get("bot_id") or m.get("subtype")
                            == "bot_message") else 0
            is_our_bot = 1 if user_id == bot_self_id else 0
            user_name = ""
            if user_id and not is_bot:
                user_name = _resolve_user(session, user_id)
            elif is_bot:
                # Bot messages: use the bot_profile name if available.
                bp = m.get("bot_profile") or {}
                user_name = bp.get("name") or "(bot)"

            text = m.get("text", "")
            thread_ts = m.get("thread_ts") or None
            try:
                with db.connect() as c:
                    c.execute(
                        "INSERT OR IGNORE INTO slack_messages "
                        "(channel_id, ts, user_id, user_name, text, "
                        " thread_ts, is_bot, is_our_bot, raw_event) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (channel_id, ts, user_id, user_name, text,
                         thread_ts, is_bot, is_our_bot,
                         json.dumps(m)[:8000]))
                    if c.total_changes > 0:
                        new_count += 1
            except Exception as exc:  # noqa: BLE001
                log.warning("Failed to store slack message %s/%s: %s",
                              channel_id, ts, exc)

        cursor = (body.get("response_metadata") or {}).get("next_cursor")
        if not cursor:
            break
        if pages >= 50:
            log.warning("  stopped paginating channel %s after 50 "
                          "pages — adjust lookback or run more often",
                          channel_id)
            break

    # Update cursor.
    channel_name = _resolve_channel(session, channel_id)
    with db.connect() as c:
        c.execute(
            "INSERT OR REPLACE INTO slack_channel_cursors "
            "(channel_id, channel_name, last_ts, last_pulled_at) "
            "VALUES (?, ?, ?, datetime('now'))",
            (channel_id, channel_name, highest_ts))
    return new_count


# ---------------------------------------------------------------------------
# Top-level pull
# ---------------------------------------------------------------------------


def poll_once(lookback_hours: Optional[int] = None) -> Dict[str, int]:
    """Pull new messages from every configured channel once. Returns
    {channel_id: new_count}. Safe to call from a loop or a one-off.

    `lookback_hours` only applies on FIRST pull of a channel that has
    no cursor yet. Subsequent pulls are always incremental from the
    last_ts cursor."""
    token = os.environ.get("SLACK_BOT_TOKEN", "").strip()
    channels = _configured_channels()
    if not token:
        log.warning("SLACK_BOT_TOKEN not set; skipping Slack ingest.")
        return {}
    if not channels:
        log.warning("SLACK_AI_CHANNELS not set; skipping Slack ingest.")
        return {}
    session = _build_session(token)
    bot_self = get_bot_self_id(session)
    out = {}
    for ch in channels:
        try:
            n = _ingest_channel(session, ch, bot_self,
                                  lookback_hours=lookback_hours)
            ch_name = _resolve_channel(session, ch)
            log.info("  #%s (%s): %d new messages",
                       ch_name, ch, n)
            out[ch] = n
        except Exception as exc:  # noqa: BLE001
            log.error("Channel %s ingest failed: %s", ch, exc)
            out[ch] = -1
    return out


def loop_forever(interval_seconds: int = 60) -> None:
    """Poll loop used by the Render background worker. Catches
    everything so a transient API hiccup doesn't kill the loop —
    next iteration retries."""
    log.info("Starting Slack ingest loop (interval=%ds)", interval_seconds)
    while True:
        try:
            poll_once()
        except Exception as exc:  # noqa: BLE001
            log.error("Poll iteration failed: %s", exc)
        time.sleep(max(10, interval_seconds))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: Optional[List[str]] = None) -> int:
    load_dotenv(SCRIPT_DIR / ".env")
    p = argparse.ArgumentParser(description="Slack message ingest.")
    sub = p.add_subparsers(dest="cmd", required=True)
    pp = sub.add_parser("poll", help="One-shot pull.")
    bp = sub.add_parser("backfill",
                          help="Initial pull (lookback hours).")
    bp.add_argument("--hours", type=int, default=24)
    lp = sub.add_parser("loop", help="Loop forever (background worker).")
    lp.add_argument("--interval", type=int, default=60)
    args = p.parse_args(argv)

    if args.cmd == "poll":
        poll_once()
    elif args.cmd == "backfill":
        poll_once(lookback_hours=args.hours)
    elif args.cmd == "loop":
        loop_forever(args.interval)
    return 0


if __name__ == "__main__":
    sys.exit(main())
