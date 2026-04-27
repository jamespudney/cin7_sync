"""Spot-check BOM Quantity ratios to verify they match clean unit
conversions (1ft = 0.003048 of a 100m roll, 1ft = 0.061 of a 5m roll)."""
import pandas as pd
import glob

bom_files = sorted(glob.glob("output/boms_*.csv"))
df = pd.read_csv(bom_files[-1])
print(f"BOM file: {bom_files[-1]}\n")

samples = [
    "LED-WLNW-40K-IP20-0305",
    "LED-TSB2835-300-24-6000-0305",
    "LEDIRIS2200-180-0305",
]

EXPECTED_FOOT_PER_100M = 0.3048 / 100.0   # 0.003048
EXPECTED_FOOT_PER_5M   = 0.3048 / 5.0     # 0.06096

for s in samples:
    rows = df[df["AssemblySKU"] == s]
    print(f"=== {s} ===")
    if rows.empty:
        print("  (not in BOM)")
        print()
        continue
    for _, r in rows.iterrows():
        ratio = float(r["Quantity"])
        comp = r["ComponentSKU"]
        if ratio <= 0:
            print(f"  -> {comp}, ratio = {ratio} (invalid)")
            continue
        implied_ft = 1.0 / ratio
        implied_m = implied_ft * 0.3048

        print(f"  -> {comp}")
        print(f"     ratio: {ratio}")
        print(f"     implies: 1 master roll = {implied_ft:.1f} ft "
              f"= {implied_m:.2f} m")

        if abs(implied_m - 100) < 1:
            waste_pct = (ratio / EXPECTED_FOOT_PER_100M - 1) * 100
            tag = "[CLEAN]" if abs(waste_pct) < 1 else f"[+{waste_pct:.1f}% buffer]"
            print(f"     {tag} matches 100m roll")
        elif abs(implied_m - 5) < 0.5:
            waste_pct = (ratio / EXPECTED_FOOT_PER_5M - 1) * 100
            tag = "[CLEAN]" if abs(waste_pct) < 1 else f"[+{waste_pct:.1f}% buffer]"
            print(f"     {tag} matches 5m roll")
        else:
            print(f"     [???] does not match a known roll size")
    print()

# Also do an aggregate scan: how many BOMs have ratios suggesting clean
# 100m or 5m roll math, vs unusual ratios?
print("=== Family-wide scan: ratios pointing to LED-* roll masters ===")
roll_targeting = df[
    df["ComponentSKU"].astype(str).str.contains(
        r"-100M$|-5m$|-5M$", regex=True, na=False)
].copy()
roll_targeting["ratio"] = pd.to_numeric(
    roll_targeting["Quantity"], errors="coerce")
print(f"Total BOMs whose component is a *-100M or *-5m roll: "
      f"{len(roll_targeting)}")

for label, expected, low, high in [
    ("100m roll (clean = 0.003048)", EXPECTED_FOOT_PER_100M, 0.0028, 0.0035),
    ("5m roll (clean = 0.061)", EXPECTED_FOOT_PER_5M, 0.055, 0.065),
]:
    in_range = roll_targeting[
        (roll_targeting["ratio"] >= low)
        & (roll_targeting["ratio"] <= high)]
    above = roll_targeting[roll_targeting["ratio"] > high]
    below = roll_targeting[
        (roll_targeting["ratio"] < low)
        & (roll_targeting["ratio"] > 0)]
    print(f"  {label}:")
    print(f"    in expected range [{low}-{high}]: {len(in_range)}")
    print(f"    above range (excessive waste assumption): {len(above)}")
    print(f"    below range (might be wrong unit): {len(below)}")
