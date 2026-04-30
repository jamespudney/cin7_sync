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
1. $DATA_DIR (set on Render to e.g. /data)
2. The folder containing this file (i.e. the project root)

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
    """Resolve DATA_DIR with safe fallback to the project folder."""
    env = os.environ.get("DATA_DIR", "").strip()
    if env:
        p = Path(env).resolve()
    else:
        # Default: directory containing this file. Works for any CLI
        # script invoked from anywhere AND for Streamlit.
        p = Path(__file__).resolve().parent
    p.mkdir(parents=True, exist_ok=True)
    return p


DATA_DIR: Path = _resolve_data_dir()
OUTPUT_DIR: Path = DATA_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# SQLite file. db.py imports this directly so its DB_PATH stays in
# lockstep with everything else.
DB_PATH: Path = DATA_DIR / "team_actions.db"
