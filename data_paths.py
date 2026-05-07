"""
data_paths.py
=============
Single source of truth for where the app keeps its persistent data.

Why this exists
---------------
Locally the app runs from C:\\Tools\\cin7_sync and writes everything
relative to that folder. On Render (or any Linux host), the code lives
at /app/ but data needs to live on a mounted persistent disk so it
survives deploys. We route everything through a DATA_DIR env var so
there's exactly one knob to turn.

Resolution order (first match wins)
-----------------------------------
1. $DATA_DIR env var (explicit override)
2. /data if it exists and is writable (v2.67.62 — Render workers
   with a persistent disk attached. This was previously requiring
   manual DATA_DIR env-var on every new worker; the new worker the
   user set up wrote 350MB of data to ephemeral container storage
   for hours before this was caught.)
3. The folder containing this file (project root — local dev)

Both options resolve to an absolute path. OUTPUT_DIR and DB_PATH are
derived from DATA_DIR — never construct your own. Importing this
module also creates OUTPUT_DIR if it doesn't exist (zero-config first
boot on a fresh disk).

Usage
-----
    from data_paths import DATA_DIR, OUTPUT_DIR, DB_PATH

    # CSVs
    OUTPUT_DIR / f"products_{stamp}.csv"

    # SQLite — but db.py uses DB_PATH directly, you don't usually
    # have to think about it
    sqlite3.connect(str(DB_PATH))
"""

from __future__ import annotations

import os
from pathlib import Path


def _resolve_data_dir() -> Path:
    """Resolve DATA_DIR with safe fallback chain.

    Order:
      1. $DATA_DIR env var (explicit override)
      2. /data if it exists AND is writable (Render workers with
         persistent disk attached)
      3. The folder containing this file (project root — local dev)

    v2.67.62: option 2 added after a worker silently wrote 350MB to
    ephemeral container storage for hours because DATA_DIR wasn't set.
    Now any Render service with /data mounted automatically uses it
    without per-service env-var configuration.
    """
    env = os.environ.get("DATA_DIR", "").strip()
    if env:
        p = Path(env).resolve()
    elif Path("/data").exists() and os.access("/data", os.W_OK):
        p = Path("/data")
    else:
        # Local dev or no persistent disk: directory containing
        # this file. Works for any CLI script AND Streamlit.
        p = Path(__file__).resolve().parent
    p.mkdir(parents=True, exist_ok=True)
    return p


DATA_DIR: Path = _resolve_data_dir()
OUTPUT_DIR: Path = DATA_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# SQLite file. db.py imports this directly so its DB_PATH stays in
# lockstep with everything else.
DB_PATH: Path = DATA_DIR / "team_actions.db"
