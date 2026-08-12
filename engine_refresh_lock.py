"""engine_refresh_lock.py
==========================
Single, shared atomic lock guarding warm_engine.py launches.

Why this exists
----------------
warm_engine.py is a subprocess that duplicates the dashboard's full
in-memory dataset while it runs. THREE independent places can launch
it: app.py's Streamlit-triggered background refresh, sync_loop.sh
(after the nightly daily_sync.sh), and nearsync.sh (after every
~15-minute nearsync). An earlier fix (PR #47, "claude/fix-engine-
refresh-race-condition") made ONE of those call sites atomic, inside
app.py, via `os.open(O_CREAT | O_EXCL)`. Render's "wired4signs-app
exceeded its memory limit" crashes continued after that fix because
the other two call sites were never touched: nearsync.sh had NO lock
check at all, and sync_loop.sh reimplemented its own bash lock with a
`[ -f lock ] ... > lock` check-then-write -- the exact TOCTOU gap the
Python fix eliminated, reintroduced in shell against the same file.

This module is the ONE place the acquire/release/liveness logic
lives now. app.py imports it directly (in-process); sync_loop.sh and
nearsync.sh invoke it as a CLI (`python engine_refresh_lock.py
acquire ...`) so all three call sites run identical code instead of
three independent (and inconsistent) reimplementations.

Second gap closed here: the previous stale-lock reclaim only checked
the lock file's AGE, never whether the process that created it is
still alive. A legitimately slow warm_engine.py run -- more likely
now that sale_lines can be a much wider window than when
max_age_minutes=45 was chosen -- could exceed that age while still
genuinely running, and a second caller would "reclaim" the lock and
start a duplicate on top of it, doubling peak memory. Storing the
holder's PID in the lock payload and checking `os.kill(pid, 0)`
before reclaiming closes that gap: a lock is only treated as stale
(safe to reclaim) when it is BOTH old AND its holder process is gone.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from data_paths import OUTPUT_DIR

LOCK_PATH = OUTPUT_DIR / "engine_refresh.lock"
DEFAULT_MAX_AGE_MINUTES = 45


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Process exists but is owned by another user -- treat as alive
        # rather than risk reclaiming a lock out from under a live run.
        return True
    except OSError:
        return False
    return True


def _read_lock_payload() -> Optional[dict]:
    try:
        return json.loads(LOCK_PATH.read_text(encoding="utf-8") or "{}")
    except (OSError, ValueError):
        return None


def is_running(max_age_minutes: int = DEFAULT_MAX_AGE_MINUTES) -> bool:
    """True if a live engine refresh already holds the lock.

    A lock younger than max_age_minutes is always treated as running
    (fast path, no need to inspect its payload). An older lock is only
    treated as stale (returns False, i.e. safe to reclaim) if its
    recorded holder PID is no longer alive; missing/corrupt PID info
    falls back to age-only, matching the pre-liveness-check behaviour."""
    try:
        if not LOCK_PATH.exists():
            return False
        age_min = (
            datetime.now().timestamp() - LOCK_PATH.stat().st_mtime
        ) / 60.0
    except OSError:
        return False
    if age_min <= max_age_minutes:
        return True
    payload = _read_lock_payload()
    pid = None
    if payload:
        try:
            pid = int(payload.get("pid") or 0)
        except (TypeError, ValueError):
            pid = None
    if pid and _pid_alive(pid):
        return True
    return False


def acquire(payload: dict,
            max_age_minutes: int = DEFAULT_MAX_AGE_MINUTES) -> bool:
    """Atomically acquire the lock. Returns True if THIS call acquired
    it (safe to launch warm_engine.py), False if another
    process/thread/session already holds a live one.

    `os.open(..., O_CREAT | O_EXCL)` makes "does the lock exist" and
    "create it" a single atomic syscall -- only one caller can ever
    win when several race the exact same instant. If a lock is
    already present but is_running() says it's genuinely stale
    (old AND holder process gone), it's removed and creation is
    retried once; a second caller racing that exact retry just loses
    the O_EXCL a second time and correctly backs off."""
    payload = dict(payload)
    payload.setdefault("pid", os.getpid())
    payload_bytes = json.dumps(payload, default=str).encode("utf-8")

    def _try_create() -> bool:
        try:
            fd = os.open(str(LOCK_PATH),
                         os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            return False
        try:
            os.write(fd, payload_bytes)
        finally:
            os.close(fd)
        return True

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if _try_create():
        return True
    if not is_running(max_age_minutes):
        try:
            LOCK_PATH.unlink()
        except OSError:
            pass
        return _try_create()
    return False


def release() -> None:
    try:
        LOCK_PATH.unlink(missing_ok=True)
    except OSError:
        pass


def _cli(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Shared engine-refresh lock CLI for shell callers.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    acquire_p = sub.add_parser(
        "acquire", help="Try to acquire the lock; exit 0 if acquired, "
                        "1 if another refresh already holds it.")
    acquire_p.add_argument("--reason", default="")
    acquire_p.add_argument("--max-age-minutes", type=int,
                            default=DEFAULT_MAX_AGE_MINUTES)

    sub.add_parser("release", help="Release the lock unconditionally.")

    running_p = sub.add_parser(
        "is-running", help="Exit 0 if a live refresh holds the lock, "
                           "1 otherwise. Does not acquire.")
    running_p.add_argument("--max-age-minutes", type=int,
                           default=DEFAULT_MAX_AGE_MINUTES)

    args = parser.parse_args(argv)

    if args.cmd == "acquire":
        payload = {
            "started_at": datetime.utcnow().isoformat() + "Z",
            "reason": args.reason,
        }
        ok = acquire(payload, max_age_minutes=args.max_age_minutes)
        print("ACQUIRED" if ok else "BUSY")
        return 0 if ok else 1
    if args.cmd == "release":
        release()
        print("RELEASED")
        return 0
    if args.cmd == "is-running":
        running = is_running(max_age_minutes=args.max_age_minutes)
        print("RUNNING" if running else "IDLE")
        return 0 if running else 1
    return 2


if __name__ == "__main__":
    sys.exit(_cli())
