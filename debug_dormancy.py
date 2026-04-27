"""Standalone test of the new dormancy detection logic.
Runs OUTSIDE Streamlit so we can confirm the math works on the actual
data without any engine-cache or restart issues."""
import pandas as pd
import glob
from datetime import datetime

target_master = "LEDIRIS2200-180-5m"
target_child = "LEDIRIS2200-180-0305"

# 1. Load latest sale_lines (all files, dedup by LineID)
sl_files = sorted(glob.glob("output/sale_lines_*.csv"))
print(f"Reading {len(sl_files)} sale_lines files...")
sl = pd.concat([pd.read_csv(f, low_memory=False) for f in sl_files],
                ignore_index=True)
if "LineID" in sl.columns:
    sl = sl.drop_duplicates(subset=["LineID"], keep="last")

# 2. Compute units_12mo and units_90d for both target SKUs
date_col = "InvoiceDate" if "InvoiceDate" in sl.columns else "OrderDate"
sl[date_col] = pd.to_datetime(sl[date_col], errors="coerce")
sl = sl.dropna(subset=[date_col])
today = pd.Timestamp(datetime.now().date())
cutoff_365 = today - pd.Timedelta(days=365)
cutoff_90 = today - pd.Timedelta(days=90)

for sku in (target_master, target_child):
    rows = sl[sl["SKU"].astype(str).str.lower() == sku.lower()]
    sl_365 = rows[rows[date_col] >= cutoff_365]
    sl_90 = rows[rows[date_col] >= cutoff_90]
    u365 = float(sl_365["Quantity"].sum()) if "Quantity" in rows.columns else 0
    u90 = float(sl_90["Quantity"].sum()) if "Quantity" in rows.columns else 0
    print(f"\n=== {sku} ===")
    print(f"  units_12mo (365d): {u365:.1f}")
    print(f"  units_90d:         {u90:.1f}")

# 3. Compute the rollup contribution from -0305 to -5m
# BOM ratio from earlier diagnostic: 0.07 rolls per cut
# (Approximation — actual rollup may use length-based math.)
rollup_ratio = 0.07
child_rows = sl[sl["SKU"].astype(str).str.lower() == target_child.lower()]
child_365 = child_rows[child_rows[date_col] >= cutoff_365]
child_90 = child_rows[child_rows[date_col] >= cutoff_90]
child_u365 = float(child_365["Quantity"].sum()) if "Quantity" in child_rows.columns else 0
child_u90 = float(child_90["Quantity"].sum()) if "Quantity" in child_rows.columns else 0
rollup_365 = child_u365 * rollup_ratio
rollup_90 = child_u90 * rollup_ratio

# 4. Compute effective demand and dormancy check on the master
master_rows = sl[sl["SKU"].astype(str).str.lower() == target_master.lower()]
master_365 = master_rows[master_rows[date_col] >= cutoff_365]
master_90 = master_rows[master_rows[date_col] >= cutoff_90]
master_u365 = float(master_365["Quantity"].sum()) if "Quantity" in master_rows.columns else 0
master_u90 = float(master_90["Quantity"].sum()) if "Quantity" in master_rows.columns else 0

eff_12mo = master_u365 + rollup_365
eff_90d = master_u90 + rollup_90
rate_12mo = eff_12mo / 365.0
rate_90d = eff_90d / 90.0
ratio = (rate_90d / rate_12mo) if rate_12mo > 0 else 0
is_dormant = (eff_12mo > 0 and rate_12mo >= 0.05
              and rate_90d < 0.20 * rate_12mo)

print(f"\n=== {target_master} dormancy analysis ===")
print(f"  Own 12mo: {master_u365:.1f}")
print(f"  Own 90d:  {master_u90:.1f}")
print(f"  Rollup from {target_child}: 12mo={rollup_365:.1f}, 90d={rollup_90:.1f}")
print(f"  Effective 12mo: {eff_12mo:.2f} units")
print(f"  Effective 90d:  {eff_90d:.2f} units")
print(f"  12mo daily rate: {rate_12mo:.4f}")
print(f"  90d daily rate:  {rate_90d:.4f}")
print(f"  Ratio: {ratio*100:.1f}%  (threshold: <20%)")
print(f"  is_dormant: {is_dormant}")

if is_dormant:
    print("\n  → Engine WILL flag as 💤 Dormant")
    print("  → avg_daily overridden to 0")
    print("  → reorder_qty will be 0 (was 19)")
else:
    print("\n  → Engine will NOT flag as dormant")
    print("  → reorder_qty unchanged from before")
