"""api_query.py — Read-only SQL query API for Claude/diagnostic use.

Runs as a background thread alongside Streamlit. Exposes a single
POST endpoint:

    POST /api/query
    Headers: X-API-Key: <QUERY_API_KEY>
    Body:    {"sql": "SELECT ...", "params": [...], "limit": 500}
    Returns: {"columns": [...], "rows": [...], "count": N}

Security:
- READ-ONLY: only SELECT statements are accepted.
- Authenticated via X-API-Key header (set QUERY_API_KEY env var).
- Row limit capped at 2000 regardless of request.
- Runs on port 8502 (internal to Render — not exposed publicly
  unless you add a route; access via wired4signs-app.onrender.com
  is proxied through the Streamlit server... see start.sh note).

To expose via the main domain, nginx/caddy would be needed.
Instead we bind on 0.0.0.0:8502 and rely on Render's port routing:
add ?port=8502 to the service or use a second Render web service.
Simplest: run on 8502 and expose via a /api/* route in start.sh
using a socat/nginx shim, or just open 8502 directly on Render
by setting it as an additional port (not supported on all plans).

Easiest working approach: bind on the SAME port as Streamlit is not
running on (8502), and access via the Render service shell or by
having start.sh expose it. For now: run alongside on 8502, add
a separate Render web service pointing to this file for direct access.

v2.67.375
"""

from __future__ import annotations

import os
import re
import threading
from typing import Any

import psycopg2
import psycopg2.extras
from http.server import BaseHTTPRequestHandler, HTTPServer
import json

_DB_URL = os.environ.get("DATABASE_URL", "")
_API_KEY = os.environ.get("QUERY_API_KEY", "")
_MAX_ROWS = 2000
_PORT = int(os.environ.get("PORT", os.environ.get("QUERY_API_PORT", "8502")))

# Only SELECT statements allowed — strip comments then check first keyword
_SELECT_RE = re.compile(
    r"^\s*(?:--[^\n]*)?\s*SELECT\b", re.IGNORECASE | re.DOTALL)
_DANGEROUS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|GRANT|REVOKE"
    r"|EXECUTE|CALL|COPY|pg_read_file|pg_ls_dir|lo_import|lo_export)\b",
    re.IGNORECASE)


def _is_safe_sql(sql: str) -> tuple[bool, str]:
    """Return (ok, reason). Only pure SELECT statements pass."""
    stripped = re.sub(r"--[^\n]*", "", sql)
    stripped = re.sub(r"/\*.*?\*/", "", stripped, flags=re.DOTALL).strip()
    if not _SELECT_RE.match(stripped):
        return False, "Only SELECT statements are permitted"
    if _DANGEROUS.search(stripped):
        return False, "Statement contains a disallowed keyword"
    return True, ""


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):  # silence default access log
        pass

    def _respond(self, code: int, body: Any):
        data = json.dumps(body, default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        if self.path != "/api/query":
            self._respond(404, {"error": "Not found"})
            return

        # Auth
        key = self.headers.get("X-API-Key", "")
        if not _API_KEY or key != _API_KEY:
            self._respond(401, {"error": "Unauthorized"})
            return

        # Parse body
        length = int(self.headers.get("Content-Length", 0))
        try:
            payload = json.loads(self.rfile.read(length))
        except Exception:
            self._respond(400, {"error": "Invalid JSON"})
            return

        sql = str(payload.get("sql", "")).strip()
        params = payload.get("params", [])
        limit = min(int(payload.get("limit", 500)), _MAX_ROWS)

        if not sql:
            self._respond(400, {"error": "sql is required"})
            return

        ok, reason = _is_safe_sql(sql)
        if not ok:
            self._respond(400, {"error": reason})
            return

        # Add LIMIT if not present
        if not re.search(r"\bLIMIT\b", sql, re.IGNORECASE):
            sql = f"{sql} LIMIT {limit}"

        # Execute
        if not _DB_URL:
            self._respond(500, {"error": "DATABASE_URL not configured"})
            return
        try:
            conn = psycopg2.connect(_DB_URL, connect_timeout=10)
            conn.set_session(readonly=True, autocommit=True)
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(sql, params or None)
            rows = [dict(r) for r in cur.fetchall()]
            columns = [d.name for d in cur.description] if cur.description else []
            cur.close()
            conn.close()
            self._respond(200, {
                "columns": columns,
                "rows": rows,
                "count": len(rows),
            })
        except Exception as exc:
            self._respond(500, {"error": str(exc)})

    def do_GET(self):
        if self.path == "/api/health":
            self._respond(200, {"status": "ok", "service": "cin7-query-api"})
        else:
            self._respond(404, {"error": "Not found"})


def start_query_api(daemon: bool = True) -> threading.Thread:
    """Start the query API server in a background thread.
    Call from start.sh or app.py boot. Returns the thread."""
    if not _API_KEY:
        print("[query-api] QUERY_API_KEY not set — API disabled")
        return threading.Thread(target=lambda: None, daemon=True)

    server = HTTPServer(("0.0.0.0", _PORT), _Handler)

    def _run():
        print(f"[query-api] Listening on port {_PORT}")
        server.serve_forever()

    t = threading.Thread(target=_run, daemon=daemon, name="query-api")
    t.start()
    return t


if __name__ == "__main__":
    # Run standalone: python3 api_query.py
    start_query_api(daemon=False).join()
