"""Investigate LED-KP24-6000K-IP20-100M specifically — what does the
engine see, and is dormancy firing under the new v2.4 rules?"""
import pandas as pd
import glob

target = "LED-KP24-6000K-IP20-100M"
today = pd.Timestamp.now().normalize()
cutoff_365 = today - pd.Timedelta(days=365)
cutoff_90 = today - pd.Timedelta(days=90)
cutoff_45 = today - pd.Timedelta(days=45)

# Load data
sl_files = sorted(glob.glob("output/sale_lines_*.csv"))
sl = pd.concat([pd.read_csv(f, low_memory=False) for f in sl_files],
                ignore_index=True)
if "LineID" in sl.columns:
    sl = sl.drop_duplicates(subset=["LineID"], keep="last")
sl["InvoiceDate"] = pd.to_datetime(sl["InvoiceDate"], errors="coerce")
sl["Quantity"] = pd.to_numeric(sl["Quantity"], errors="coerce").fillna(0)

stock = pd.read_csv(
    sorted(glob.glob("output/stock_on_hand_*.csv"))[-1], low_memory=False)
boms = pd.read_csv(sorted(glob.glob("output/boms_*.csv"))[-1])
products = pd.read_csv(
    sorted(glob.glob("output/products_*.csv"))[-1], low_memory=False)

# Direct sales of master
target_sl = sl[sl["SKU"].astype(str) == target]
direct_12mo = float(
    target_sl[target_sl["InvoiceDate"] >= cutoff_365]["Quantity"].sum())
direct_90d = float(
    target_sl[target_sl["InvoiceDate"] >= cutoff_90]["Quantity"].sum())
direct_45d = float(
    target_sl[target_sl["InvoiceDate"] >= cutoff_45]["Quantity"].sum())

# Children rolling up via BOM
children_bom = boms[boms["ComponentSKU"] == target]
rollup_12mo = 0.0
rollup_90d = 0.0
print(f"=== {target} ===\n")
print(f"Direct master sales:")
print(f"  12mo: {direct_12mo}")
print(f"  90d:  {direct_90d}")
print(f"  45d:  {direct_45d}")

print(f"\nChildren whose BOM points to this master ({len(children_bom)}):")
for _, b in children_bom.iterrows():
    c_sku = b["AssemblySKU"]
    ratio = float(b["Quantity"])
    c_sl = sl[sl["SKU"].astype(str) == str(c_sku)]
    c_12 = float(c_sl[c_sl["InvoiceDate"] >= cutoff_365]["Quantity"].sum())
    c_90 = float(c_sl[c_sl["InvoiceDate"] >= cutoff_90]["Quantity"].sum())
    c_45 = float(c_sl[c_sl["InvoiceDate"] >= cutoff_45]["Quantity"].sum())
    contrib_12 = c_12 * ratio
    contrib_90 = c_90 * ratio
    rollup_12mo += contrib_12
    rollup_90d += contrib_90
    print(f"  {c_sku}: 12mo={c_12}, 90d={c_90}, 45d={c_45}, ratio={ratio}")
    print(f"     -> contrib 12mo: {contrib_12:.3f}, 90d: {contrib_90:.3f}")

# Effective demand
eff_12mo = direct_12mo + rollup_12mo
eff_90d = direct_90d + rollup_90d
rate_12mo = eff_12mo / 365.0
rate_90d = eff_90d / 90.0

print(f"\nEffective demand (master rolls):")
print(f"  Direct: 12mo={direct_12mo}, 90d={direct_90d}")
print(f"  Rollup: 12mo={rollup_12mo:.3f}, 90d={rollup_90d:.3f}")
print(f"  Total:  12mo={eff_12mo:.3f}, 90d={eff_90d:.3f}")
print(f"  Daily rate (12mo basis): {rate_12mo:.4f} master rolls/day")
print(f"  Daily rate (90d basis):  {rate_90d:.4f}")

# Dormancy check (v2.4 logic)
print(f"\nDormancy verdict (v2.4 rules):")
if eff_12mo <= 0:
    verdict = "Not dormant (no historical demand at all)"
elif eff_90d <= 0:
    verdict = "DORMANT (Tier 1: zero 90d activity, has 12mo history)"
elif rate_12mo < 0.01:
    verdict = "DORMANT (Tier 1b: 12mo rate < 0.01/day = under 4 units/year)"
elif rate_12mo < 0.05:
    verdict = "Not dormant (Tier 2 skip — historical too tiny)"
elif rate_90d < 0.20 * rate_12mo:
    verdict = "DORMANT (Tier 2: 90d rate < 20% of 12mo rate)"
else:
    verdict = "Not dormant (active)"
print(f"  {verdict}")

# Stock
target_stock_rows = stock[stock["SKU"] == target]
if not target_stock_rows.empty:
    onhand = float(pd.to_numeric(target_stock_rows["OnHand"],
                                  errors="coerce").sum() or 0)
    onorder = float(pd.to_numeric(target_stock_rows.get("OnOrder", 0),
                                   errors="coerce").sum() or 0)
    print(f"\nStock: OnHand={onhand}, OnOrder={onorder}")
else:
    onhand = onorder = 0
    print(f"\nStock: not found in stock_on_hand")

# Predicted Suggest under v2.4
print(f"\n=== Expected Suggest under v2.4 ===")
if "DORMANT" in verdict:
    print("  Suggest: 0  (dormancy override, avg_daily=0)")
else:
    avg_daily_used = rate_12mo  # active SKU uses 12mo rate
    target_qty = avg_daily_used * (28 + 28*0.20 + 30)  # Neonica 21d air + safety + review
    shortfall = max(0, target_qty - onhand - onorder)
    print(f"  avg_daily: {avg_daily_used:.4f}")
    print(f"  target_stock (Neonica 28d air): {target_qty:.3f}")
    print(f"  shortfall: max(0, {target_qty:.3f} - {onhand}-{onorder}) = {shortfall:.3f}")
    metres = shortfall * 100  # 100m roll
    if metres < 5:
        print(f"  Snap to 10m floor: 0 (metres {metres:.2f} < 5)")
    else:
        rounded = round(metres / 10) * 10
        if rounded < 10:
            rounded = 10
        print(f"  Snap to 10m: {metres:.2f}m -> {rounded}m = {rounded/100:.2f} rolls")
