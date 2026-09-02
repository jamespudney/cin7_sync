"""fablab_slack.py

Shared Slack posting helper for all 865FabLab notifications. James set
up a dedicated channel (2026-09-01) with 865FabLab staff, buyers, and
stock controllers all as members. Phase 1 was one-way notifications
only; Phase 2 (2026-09-02, fablab_stock_alert.py check-replies) lets a
buyer's reply in a stock-alert thread trigger a real CIN7 Draft PO --
`post()`'s `thread_ts` param is what makes that reply land in the same
thread the alert was posted in, rather than as a new top-level message.

Same underlying mechanism as every other worker notification
(slack_sync._build_session / _slack_post) -- just centralized here so
the call sites (autotag summary, stock-drop alert + its approval
replies, PO push, materials-consumed) don't each reimplement it.

CLI: none -- this is a library, imported by fablab_corner_autotag.py,
fablab_stock_alert.py, and app_pages/fablab_work_orders.py.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

log = logging.getLogger("fablab_slack")

FABLAB_CHANNEL_ID = "C0BU77GC3SS"


def post(text: str, channel_id: str = FABLAB_CHANNEL_ID,
          thread_ts: Optional[str] = None
          ) -> tuple[Optional[str], Optional[str]]:
    """Post a message to the 865FabLab Slack channel, or a threaded
    reply within it if thread_ts is given. Returns (posted_ts, error)
    -- never raises, since a Slack outage shouldn't break the CIN7
    action that triggered the notification."""
    try:
        import slack_sync
    except ImportError as exc:
        err = f"slack_sync import failed: {exc}"
        log.error(err)
        return None, err
    token = os.environ.get("SLACK_BOT_TOKEN", "").strip()
    if not token:
        return None, "SLACK_BOT_TOKEN not set"
    try:
        session = slack_sync._build_session(token)
        body_payload = {
            "channel": channel_id,
            "text": text,
            "unfurl_links": False,
            "unfurl_media": False,
        }
        if thread_ts:
            body_payload["thread_ts"] = thread_ts
        body = slack_sync._slack_post(
            session, "chat.postMessage", body_payload)
        if not body.get("ok"):
            err = f"slack returned ok=false: {body}"
            log.error(err)
            return None, err
        return body.get("ts"), None
    except Exception as exc:  # noqa: BLE001
        err = f"post error: {exc}"
        log.error(err)
        return None, err
