"""Audit current supplier configs to find missing air-freight settings."""
import sqlite3

db = sqlite3.connect("team_actions.db")
db.row_factory = sqlite3.Row

total = db.execute(
    "SELECT COUNT(*) AS n FROM supplier_config").fetchone()[0]
print(f"Total supplier configs in DB: {total}")
print()


def safe_int(v):
    try:
        return int(v) if v not in (None, "") else None
    except (ValueError, TypeError):
        return None


def safe_real(v):
    try:
        return float(v) if v not in (None, "") else None
    except (ValueError, TypeError):
        return None


with_sea = 0
with_air = 0
air_eligible = 0
no_lt_at_all = 0

print(
    f"{'Supplier':<40} {'Sea LT':>7} {'Air LT':>7} "
    f"{'Air?':>5} {'MaxLen':>7} {'MOQ':>5} {'MOV':>10} {'Pref':>6}")
print("-" * 100)

rows = db.execute(
    "SELECT * FROM supplier_config ORDER BY supplier_name").fetchall()
for r in rows:
    name = (r["supplier_name"] or "")[:39]
    sea = safe_int(r["lead_time_sea_days"])
    air = safe_int(r["lead_time_air_days"])
    air_ok = bool(r["air_eligible_default"])
    maxlen = safe_int(r["air_max_length_mm"])
    moq = safe_real(r["moq_units"])
    mov = safe_real(r["mov_amount"])
    pref = r["preferred_freight"] or ""

    if sea:
        with_sea += 1
    if air:
        with_air += 1
    if air_ok:
        air_eligible += 1
    if not sea and not air:
        no_lt_at_all += 1

    print(
        f"{name:<40} "
        f"{str(sea) if sea else '-':>7} "
        f"{str(air) if air else '-':>7} "
        f"{'yes' if air_ok else 'no':>5} "
        f"{str(maxlen) if maxlen else '-':>7} "
        f"{str(int(moq)) if moq else '-':>5} "
        f"{str(int(mov)) if mov else '-':>10} "
        f"{pref:>6}")

print()
print(f"=== Summary ===")
print(f"  Total configs: {total}")
print(f"  With sea lead time configured: {with_sea}")
print(f"  With air lead time configured: {with_air}")
print(f"  Marked air-eligible by default: {air_eligible}")
print(f"  With NO lead time at all: {no_lt_at_all}")
print()
if with_air < total / 2:
    print("  Most suppliers don't have air lead time set.")
    print("  Engine falls back to 35-day sea default for them.")
    print("  Setting lead_time_air_days + air_eligible_default=True")
    print("  for LED-strip suppliers will reduce target stock.")

db.close()
