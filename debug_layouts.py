"""Quick diagnostic: dump every saved ui_prefs row so we can see which
user/view keys have layouts saved. Run once, paste the output back.

    .venv\\Scripts\\python debug_layouts.py
"""
import db

with db.connect() as c:
    rows = c.execute(
        "SELECT user, view, "
        "       length(columns_csv) AS cols_len, "
        "       length(widths_csv)  AS w_len, "
        "       substr(columns_csv, 1, 80) AS cols_preview, "
        "       widths_csv, "
        "       updated_at "
        "FROM ui_prefs "
        "ORDER BY updated_at DESC"
    ).fetchall()

if not rows:
    print("(no saved layouts in ui_prefs)")
else:
    for r in rows:
        print("------------------------------------------------------------")
        print(f"user:        '{r['user']}'")
        print(f"view:        '{r['view']}'")
        print(f"columns_csv: {r['cols_len']} chars -> {r['cols_preview']}...")
        print(f"widths_csv:  {r['w_len'] or 0} chars -> {r['widths_csv']}")
        print(f"updated_at:  {r['updated_at']}")
    print("------------------------------------------------------------")
    print(f"Total: {len(rows)} row(s)")
