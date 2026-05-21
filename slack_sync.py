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
_BOT_SELF_BOT_ID: Optional[str] = None     # v2.67.120


def get_bot_self_id(session: requests.Session) -> str:
    """Return our own bot user_id (U-prefix). Cached per process.
    Critical for the 'never reply to self' loop guard.

    v2.67.120 — also caches the bot_id (B-prefix). Slack's
    conversations.history returns the bot's own posts with
    `bot_id` set but `user` often missing, so matching only on
    user_id produced is_our_bot=0 on every bot post (and the
    listener's `_is_thread_we_posted_in` check then never found
    its own threads → bot ignored follow-up questions). We now
    capture both ids at startup and match either."""
    global _BOT_SELF_ID, _BOT_SELF_BOT_ID
    if _BOT_SELF_ID is not None:
        return _BOT_SELF_ID
    body = _slack_get(session, "auth.test", {})
    _BOT_SELF_ID = body.get("user_id") or ""
    _BOT_SELF_BOT_ID = body.get("bot_id") or ""
    log.info("Slack bot self user_id: %s bot_id: %s (team: %s)",
              _BOT_SELF_ID, _BOT_SELF_BOT_ID, body.get("team"))
    return _BOT_SELF_ID


def get_bot_self_bot_id() -> str:
    """v2.67.120 — separately exposed bot_id (B-prefix) for callers
    that need to match bot-message events. Populated as a side
    effect of get_bot_self_id; returns '' if that hasn't run yet."""
    return _BOT_SELF_BOT_ID or ""


# ---------------------------------------------------------------------------
# Channel list
# ---------------------------------------------------------------------------


def _configured_channels() -> List[str]:
    """Combined channel set the bot polls. Includes:
      - SLACK_AI_CHANNELS         classify + respond
      - SLACK_INGEST_ONLY_CHANNELS  read-only (bot stays silent;
                                     v2.67.98)
    Both lists are polled into slack_messages so the AI can
    reference the content via get_slack_messages tool. Whether the
    bot responds is decided by slack_listener.py."""
    out: List[str] = []
    for var in ("SLACK_AI_CHANNELS", "SLACK_INGEST_ONLY_CHANNELS"):
        raw = os.environ.get(var, "").strip()
        if not raw:
            continue
        for c in raw.split(","):
            c = c.strip()
            if c and c not in out:
                out.append(c)
    # v2.67.259 — auto-include the dedicated single-purpose
    # channels. Each has its own env var AND the bot processes
    # INCOMING messages there (UPS emails, stock issues, PO
    # commentary, shipping reviews). Previously the operator had
    # to ALSO remember to add each one to SLACK_AI_CHANNELS — a
    # silent gap that left #dropship-tracking completely
    # un-polled, so UPS emails never reached the handler.
    for var in ("SLACK_DROPSHIP_TRACKING_CHANNEL_ID",
                 "SLACK_STOCK_ISSUES_CHANNEL_ID",
                 "SLACK_PURCHASE_BACKORDER_CHANNEL_ID",
                 "SLACK_PO_COMMENTARY_SOURCE_CHANNEL_ID",
                 "SLACK_SHIPPING_ISSUES_CHANNEL_ID"):
        c = os.environ.get(var, "").strip()
        if c and c not in out:
            out.append(c)
    return out


def _ingest_only_channels() -> set:
    """Channels we POLL but never RESPOND to. Tagged on each
    message at classification time."""
    raw = os.environ.get(
        "SLACK_INGEST_ONLY_CHANNELS", "").strip()
    if not raw:
        return set()
    return {c.strip() for c in raw.split(",") if c.strip()}


# ---------------------------------------------------------------------------
# v2.67.66 — Feedback ingest helpers
# ---------------------------------------------------------------------------
# When ingesting messages, we ALSO capture two flavours of feedback
# the team gives on bot replies:
#   1. Emoji reactions ON OUR BOT'S POSTS — 👍 / 👎 / 🛑 / ✅ / etc.
#   2. Thread replies in threads where our bot has posted (humans
#      replying to the bot, or the bot's audit-channel mirror).
# Both feed slack_audit_feedback. The daily summarizer
# (bot_self_improvement.py) digests this into 'lessons learned'
# that gets prepended to the system prompt.

# Polarity classification — common Slack emoji shortcodes.
_POSITIVE_EMOJI = {
    "+1", "thumbsup", "white_check_mark", "heavy_check_mark",
    "ok", "ok_hand", "100", "tada", "raised_hands", "muscle",
    "clap", "fire", "star", "sparkles", "heart", "green_heart",
    "blue_heart", "purple_heart", "yellow_heart", "pray",
    "bow", "thank_you", "trophy", "medal", "gold_medal",
    "rocket", "saluting_face",
}
_NEGATIVE_EMOJI = {
    "-1", "thumbsdown", "x", "no_entry", "no_entry_sign",
    "stop_sign", "warning", "rage", "angry", "facepalm",
    "person_facepalming", "man_facepalming", "woman_facepalming",
    "exclamation", "heavy_exclamation_mark", "skull",
    "skull_and_crossbones", "poop", "negative_squared_cross_mark",
}


def _emoji_polarity(name: str) -> int:
    """Return 1/-1/0 polarity for a reaction emoji shortcode."""
    n = (name or "").strip().lower()
    if n in _POSITIVE_EMOJI:
        return 1
    if n in _NEGATIVE_EMOJI:
        return -1
    return 0


def _lookup_response_id_by_ts(channel_id: str,
                                 message_ts: str) -> Optional[int]:
    """Find the slack_bot_responses row whose response_ts matches
    this message ts. Used to map an incoming reaction/thread-reply
    back to the bot reply being commented on."""
    if not message_ts:
        return None
    try:
        with db.connect() as c:
            row = c.execute(
                "SELECT id FROM slack_bot_responses "
                "WHERE in_channel = ? AND response_ts = ?",
                (channel_id, message_ts)).fetchone()
        return int(row["id"]) if row else None
    except Exception:
        return None


def _lookup_response_id_by_thread(channel_id: str,
                                     thread_ts: str
                                     ) -> Optional[int]:
    """Find the slack_bot_responses row whose in_thread_ts matches
    (i.e. our bot replied in this thread). Used when a human posts
    in the same thread as the bot — that reply is feedback on our
    bot's contribution to the thread."""
    if not thread_ts:
        return None
    try:
        with db.connect() as c:
            row = c.execute(
                "SELECT id FROM slack_bot_responses "
                "WHERE in_channel = ? AND in_thread_ts = ? "
                "ORDER BY id DESC LIMIT 1",
                (channel_id, thread_ts)).fetchone()
        return int(row["id"]) if row else None
    except Exception:
        return None


def _record_feedback(response_id: int,
                      feedback_type: str,
                      user_id: str,
                      user_name: str,
                      content: str,
                      is_positive: int,
                      feedback_ts: str = "") -> bool:
    """Insert a feedback row, idempotent via the UNIQUE index on
    (response_id, feedback_type, user_id, content). Returns True
    if a new row was inserted."""
    if not response_id or not content:
        return False
    try:
        with db.connect() as c:
            c.execute(
                "INSERT OR IGNORE INTO slack_audit_feedback "
                "(response_id, feedback_type, user_id, user_name, "
                " content, is_positive, feedback_ts) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (response_id, feedback_type, user_id, user_name,
                 content[:2000], is_positive, feedback_ts))
            return c.total_changes > 0
    except Exception as exc:
        log.warning("Failed to record feedback: %s", exc)
        return False


def _capture_feedback_from_message(session: requests.Session,
                                       channel_id: str,
                                       m: Dict[str, Any],
                                       bot_self_id: str
                                       ) -> int:
    """Inspect one message for feedback signals and persist any
    found. Returns count of new feedback rows inserted.

    Two signal types extracted:
      1. The message is a bot reply with `reactions` array →
         capture each reaction as feedback against that response.
      2. The message is a HUMAN reply in a thread where our bot
         replied → capture the message text as thread-reply
         feedback against the bot's response.
    """
    if not isinstance(m, dict):
        return 0
    inserted = 0
    msg_ts = m.get("ts")
    # v2.67.120 — match on either user_id or bot_id (see ingest)
    raw_user = m.get("user") or ""
    raw_bot = m.get("bot_id") or ""
    user_id = raw_user or raw_bot or ""
    bot_self_bot_id = get_bot_self_bot_id()
    is_our_bot = (
        (raw_user and raw_user == bot_self_id)
        or (raw_bot and bot_self_bot_id
              and raw_bot == bot_self_bot_id)
    )

    # Case 1: reactions on our bot's posts.
    if is_our_bot and msg_ts:
        response_id = _lookup_response_id_by_ts(channel_id, msg_ts)
        if response_id:
            for r in (m.get("reactions") or []):
                if not isinstance(r, dict):
                    continue
                emoji = r.get("name") or ""
                polarity = _emoji_polarity(emoji)
                # Each user who reacted is a separate feedback event.
                for u in (r.get("users") or []):
                    u_name = _resolve_user(session, u)
                    if _record_feedback(
                            response_id, "reaction", u, u_name,
                            emoji, polarity, msg_ts):
                        inserted += 1

    # Case 2: human reply in a thread where bot also replied.
    thread_ts = m.get("thread_ts")
    if (thread_ts and not is_our_bot
            and not (m.get("bot_id") or m.get("subtype")
                      == "bot_message")):
        response_id = _lookup_response_id_by_thread(
            channel_id, thread_ts)
        if response_id:
            text = (m.get("text") or "").strip()
            if text and len(text) > 2:
                u_name = _resolve_user(session, user_id)
                if _record_feedback(
                        response_id, "thread_reply", user_id,
                        u_name, text, 0, msg_ts):
                    inserted += 1

    return inserted


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
    # v2.67.60 — when there's NO cursor AND NO explicit lookback,
    # default to a 1-hour window. Prevents the first-run pull from
    # grabbing the ENTIRE channel history (the v2.67.59 boot pulled
    # 8,000+ historical messages from 7 channels — overwhelmed the
    # listener queue and risked spamming responses to old threads).
    oldest = None
    if last_ts:
        oldest = last_ts
    elif lookback_hours:
        oldest = str(int(time.time()) - lookback_hours * 3600) + ".000000"
    else:
        # No cursor + no explicit lookback → default to 1 hour.
        oldest = str(int(time.time()) - 3600) + ".000000"

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
            # v2.67.120 — match on EITHER user_id (U-prefix) or
            # bot_id (B-prefix). Slack returns bot-authored
            # messages with `user` often missing and only `bot_id`
            # set, so matching only against bot_self_id (the U-id
            # from auth.test) classified every bot post as
            # is_our_bot=0 — which then broke the listener's
            # thread-recognition logic for follow-up questions.
            raw_user = m.get("user") or ""
            raw_bot = m.get("bot_id") or ""
            user_id = raw_user or raw_bot or ""
            is_bot = 1 if (raw_bot or m.get("subtype")
                            == "bot_message") else 0
            bot_self_bot_id = get_bot_self_bot_id()
            is_our_bot = 1 if (
                (raw_user and raw_user == bot_self_id)
                or (raw_bot and bot_self_bot_id
                      and raw_bot == bot_self_bot_id)
            ) else 0
            user_name = ""
            if user_id and not is_bot:
                user_name = _resolve_user(session, user_id)
            elif is_bot:
                # Bot messages: use the bot_profile name if available.
                bp = m.get("bot_profile") or {}
                user_name = bp.get("name") or "(bot)"

            text = m.get("text", "")
            thread_ts = m.get("thread_ts") or None
            # v2.67.158 — raw_event cap raised 8000 → 200000.
            # The 8000-char limit truncated email-file messages
            # mid-JSON (UPS shipment emails are 70KB+ with the
            # HTML preview), breaking downstream handlers that
            # need to json.loads(raw_event). 200KB is generous
            # enough for typical email forwards while still
            # bounding any single message's DB footprint.
            try:
                raw_str = json.dumps(m)
                if len(raw_str) > 200000:
                    # Strip the heaviest HTML field (preview) if
                    # present; keep plain_text which is what the
                    # downstream parsers actually use.
                    try:
                        m_lite = dict(m)
                        for fobj in (m_lite.get("files") or []):
                            if isinstance(fobj, dict):
                                fobj.pop("preview", None)
                        raw_str = json.dumps(m_lite)
                    except Exception:
                        pass
                    if len(raw_str) > 200000:
                        raw_str = raw_str[:200000]
                with db.connect() as c:
                    c.execute(
                        "INSERT OR IGNORE INTO slack_messages "
                        "(channel_id, ts, user_id, user_name, text, "
                        " thread_ts, is_bot, is_our_bot, raw_event) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (channel_id, ts, user_id, user_name, text,
                         thread_ts, is_bot, is_our_bot, raw_str))
                    if c.total_changes > 0:
                        new_count += 1
            except Exception as exc:  # noqa: BLE001
                log.warning("Failed to store slack message %s/%s: %s",
                              channel_id, ts, exc)

            # v2.67.66 — also capture feedback signals from this
            # message (reactions on bot posts + thread replies in
            # bot threads). Idempotent via unique index, so
            # safe to call on every poll.
            try:
                _capture_feedback_from_message(
                    session, channel_id, m, bot_self_id)
            except Exception as exc:  # noqa: BLE001
                log.warning("Feedback capture failed for "
                              "%s/%s: %s", channel_id, ts, exc)

        cursor = (body.get("response_metadata") or {}).get("next_cursor")
        if not cursor:
            break
        if pages >= 50:
            log.warning("  stopped paginating channel %s after 50 "
                          "pages — adjust lookback or run more often",
                          channel_id)
            break

    # Update cursor. v2.67.166 — ON CONFLICT DO UPDATE works on
    # both SQLite (>=3.24) and Postgres; the previous INSERT OR
    # REPLACE was SQLite-only.
    channel_name = _resolve_channel(session, channel_id)
    with db.connect() as c:
        c.execute(
            "INSERT INTO slack_channel_cursors "
            "(channel_id, channel_name, last_ts, last_pulled_at) "
            "VALUES (?, ?, ?, datetime('now')) "
            "ON CONFLICT(channel_id) DO UPDATE SET "
            "  channel_name=excluded.channel_name, "
            "  last_ts=excluded.last_ts, "
            "  last_pulled_at=excluded.last_pulled_at",
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
