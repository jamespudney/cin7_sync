"""Update Neonica air lead time from 28 days to 21 days
(2-week manufacture + 1-week air freight)."""
import sqlite3
from datetime import datetime

db = sqlite3.connect("team_actions.db")

cur = db.execute(
    "SELECT lead_time_air_days, lead_time_sea_days, note "
    "FROM supplier_config "
    "WHERE supplier_name = 'Neonica Polska Sp. z o.o.'"
).fetchone()

if cur is None:
    print("Neonica config not found — name might differ slightly.")
    print("Listing what we have:")
    for r in db.execute("SELECT supplier_name FROM supplier_config"):
        print(f"  {r[0]}")
    db.close()
    exit(1)

print(f"Current: air {cur[0]}d, sea {cur[1]}d, note='{cur[2]}'")

new_air = 21
new_note = f"Updated {datetime.now():%Y-%m-%d}: 2wk manufacture + 1wk air freight"

db.execute(
    "UPDATE supplier_config "
    "SET lead_time_air_days = ?, set_by = ?, set_at = datetime('now'), "
    "    note = ? "
    "WHERE supplier_name = 'Neonica Polska Sp. z o.o.'",
    (new_air, "james", new_note),
)
db.commit()

# Verify
cur = db.execute(
    "SELECT lead_time_air_days, note "
    "FROM supplier_config "
    "WHERE supplier_name = 'Neonica Polska Sp. z o.o.'"
).fetchone()
print(f"Updated: air {cur[0]}d, note='{cur[1]}'")

db.close()
