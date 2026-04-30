"""Investigate LED-WLNW-40K-IP20-100M to confirm:
- Whether the engine flags it as a bulk-master (length >= 50m)
- What its rollup demand actually is (BOM + strip)
- Whether the engine would produce a FRACTIONAL or integer reorder qty
- What value to expect (e.g., 0.40 of a roll vs forced 1 roll)"""
import pandas as pd
import glob
import re

target = "LED-WLNW-40K-IP20-100M"

# ---- Load data ----
products_files = sorted(glob.glob("output/products_*.csv"))
products = pd.read_csv(products_files[-1], low_memory=False) if products_files else pd.DataFrame()
stock_files = sorted(glob.glob("output/stock_on_hand_*.csv"))
stock = pd.read_csv(stock_files[-1], low_memory=False) if stock_files else pd.DataFrame()
bom_files = sorted(glob.glob("output/boms_*.csv"))
boms = pd.read_csv(bom_files[-1]) if bom_files else pd.DataFrame()
sl_files = sorted(glob.glob("output/sale_lines_*.csv"))
sl = pd.concat([pd.read_csv(f, low_memory=False) for f in sl_files], ignore_index=True)
if "LineID" in sl.columns:
    sl = sl.drop_duplicates(subset=["LineID"], keep="last")
sl["InvoiceDate"] = pd.to_datetime(sl["InvoiceDate"], errors="coerce")
sl["Quantity"] = pd.to_numeric(sl["Quantity"], errors="coerce").fillna(0)

today = pd.Timestamp.now().normalize()
cutoff_365 = today - pd.Timedelta(days=365)
cutoff_90 = today - pd.Timedelta(days=90)

print(f"=== {target} ===\n")

# ---- 1. Is this SKU in products? Check length parse ----
match = products[products["SKU"] == target]
if match.empty:
    print(f"NOT in products. Cannot proceed.")
    exit(0)
prow = match.iloc[0]
print(f"Name: {prow.get('Name', '')[:80]}")
print(f"Supplier: {prow.get('Supplier', '—')}")
print(f"Avg cost: {prow.get('AverageCost', 0)}")

# Parse length from SKU suffix (mimic engine's _parse_length / strip parser)
# The "100M" suffix should parse to 100 metres.
def parse_length_m(sku):
    m = re.search(r"-(\d+)([Mm]?)$", sku)
    if not m:
        return None
    n = int(m.group(1))
    unit = m.group(2).lower()
    if unit == "m":
        return float(n)
    return None  # would need fallback; this is rough

length_m = parse_length_m(target)
print(f"\nParsed length: {length_m}m")
if length_m and length_m >= 50:
    print("  ✓ FRACTIONAL ELIGIBLE (length >= 50m → is_bulk_master = True)")
else:
    print("  ✗ NOT fractional — length too small or unparseable")

# ---- 2. OnHand ----
stock_for = stock[stock["SKU"] == target] if not stock.empty else pd.DataFrame()
if not stock_for.empty:
    onhand = float(pd.to_numeric(stock_for["OnHand"], errors="coerce").sum() or 0)
    onorder = float(pd.to_numeric(stock_for.get("OnOrder", 0), errors="coerce").sum() or 0)
    available = float(pd.to_numeric(stock_for.get("Available", 0), errors="coerce").sum() or 0)
else:
    onhand = onorder = available = 0
print(f"\nStock: OnHand={onhand}, OnOrder={onorder}, Available={available}")

# ---- 3. Direct sales of master ----
sl_target = sl[sl["SKU"] == target]
target_12mo = float(sl_target[sl_target["InvoiceDate"] >= cutoff_365]["Quantity"].sum())
target_90d  = float(sl_target[sl_target["InvoiceDate"] >= cutoff_90]["Quantity"].sum())
print(f"\nDirect sales of {target}:")
print(f"  Last 12mo: {target_12mo}")
print(f"  Last 90d:  {target_90d}")

# ---- 4. BOM-based rollup ----
print(f"\nBOM-based rollup (children whose BOM points to {target}):")
bom_children = boms[boms["ComponentSKU"] == target] if not boms.empty else pd.DataFrame()
total_bom_12 = 0.0
total_bom_90 = 0.0
if bom_children.empty:
    print("  (no BOM children — per-foot variant might not be in BOM)")
else:
    for _, b in bom_children.iterrows():
        c_sku = b["AssemblySKU"]
        ratio = float(b["Quantity"])
        c_sl = sl[sl["SKU"] == c_sku]
        c_12 = float(c_sl[c_sl["InvoiceDate"] >= cutoff_365]["Quantity"].sum())
        c_90 = float(c_sl[c_sl["InvoiceDate"] >= cutoff_90]["Quantity"].sum())
        contrib_12 = c_12 * ratio
        contrib_90 = c_90 * ratio
        total_bom_12 += contrib_12
        total_bom_90 += contrib_90
        print(f"  {c_sku}: 12mo={c_12}, 90d={c_90}, ratio={ratio}, "
              f"contrib 12mo={contrib_12:.3f}, 90d={contrib_90:.3f}")

# ---- 5. Strip-rollup contribution (length-based) ----
# Find siblings sharing the LED-WLNW-40K-IP20 family prefix
print(f"\nFamily siblings (prefix LED-WLNW-40K-IP20-*):")
family_prefix = "LED-WLNW-40K-IP20-"
fam_skus = products[products["SKU"].str.startswith(family_prefix)]["SKU"].tolist()
total_strip_12 = 0.0
total_strip_90 = 0.0
for s in fam_skus:
    if s == target:
        continue
    s_sl = sl[sl["SKU"] == s]
    s_12 = float(s_sl[s_sl["InvoiceDate"] >= cutoff_365]["Quantity"].sum())
    s_90 = float(s_sl[s_sl["InvoiceDate"] >= cutoff_90]["Quantity"].sum())
    sibling_len = parse_length_m(s)
    # Try to detect length from suffix even if not "Xm" pattern
    if sibling_len is None:
        m = re.search(r"-(\d+)$", s)
        if m:
            n = int(m.group(1))
            # 0305 = 0.305m (1 foot); other small numbers might be metres
            if n >= 1000:
                sibling_len = n / 10000.0  # like 0305 → 0.305m
            elif 100 <= n < 1000:
                sibling_len = n / 1000.0   # like 305 → 0.305m
            else:
                sibling_len = float(n)     # like 5 → 5m, 100 → 100m
    print(f"  {s}: 12mo={s_12}, 90d={s_90}, length={sibling_len}m")
    if sibling_len and 0 < sibling_len < length_m:
        contrib_12 = s_12 * sibling_len / length_m
        contrib_90 = s_90 * sibling_len / length_m
        total_strip_12 += contrib_12
        total_strip_90 += contrib_90
        print(f"    → contrib (length math): 12mo={contrib_12:.3f}, 90d={contrib_90:.3f}")

# ---- 6. Effective demand and reorder math ----
total_demand_12 = target_12mo + total_bom_12 + total_strip_12
total_demand_90 = target_90d + total_bom_90 + total_strip_90
avg_daily_12 = total_demand_12 / 365.0
avg_daily_90 = total_demand_90 / 90.0

print(f"\n=== Demand summary ===")
print(f"  Direct master:  12mo={target_12mo}, 90d={target_90d}")
print(f"  BOM rollup:     12mo={total_bom_12:.3f}, 90d={total_bom_90:.3f}")
print(f"  Strip rollup:   12mo={total_strip_12:.3f}, 90d={total_strip_90:.3f}")
print(f"  TOTAL:          12mo={total_demand_12:.3f}, 90d={total_demand_90:.3f}")
print(f"  Daily rate (12mo basis): {avg_daily_12:.4f} master rolls/day")
print(f"  Daily rate (90d basis):  {avg_daily_90:.4f}")

# Check if dormant
if total_demand_12 > 0 and avg_daily_90 < 0.20 * avg_daily_12 and avg_daily_12 >= 0.05:
    print("  ⚠ Would be flagged 💤 DORMANT — reorder = 0")
    avg_daily_used = avg_daily_90
    is_dormant = True
else:
    avg_daily_used = avg_daily_12
    is_dormant = False

# ---- 7. Suggested reorder calc ----
LEAD = 35  # default sea LT
SAFETY = 0.20
REVIEW = 30  # B class default
target_stock = avg_daily_used * (LEAD + LEAD * SAFETY + REVIEW)
effective_pos = available + onorder
shortfall = max(0, target_stock - effective_pos)

print(f"\n=== Reorder calculation (rough) ===")
print(f"  Target stock: {avg_daily_used:.4f} × ({LEAD} + {LEAD*SAFETY:.0f} + {REVIEW}) "
      f"= {target_stock:.4f}")
print(f"  Effective position: {available} + {onorder} = {effective_pos}")
print(f"  Shortfall: max(0, {target_stock:.4f} - {effective_pos}) = {shortfall:.4f}")

if length_m and length_m >= 50:
    print(f"\n  → FRACTIONAL: engine would suggest {shortfall:.2f} master rolls")
    print(f"    (NOT rounded up; supplier accepts decimal qtys)")
else:
    print(f"\n  → INTEGER: engine would suggest {int(round(shortfall))} master rolls")

print(f"\nIf you're seeing '1' in the app, possible reasons:")
print(f"  - Decimal {shortfall:.2f} is being shown as '1.00' (still fractional, just")
print(f"    happens to round to 1 in display) → look closely at decimals in Suggest col")
print(f"  - is_bulk_master not being set → supplier might have allow_fractional_qty=False,")
print(f"    or length wasn't parsed from SKU correctly")
print(f"  - MOQ is forcing it to 1 (only happens if NOT use_fractional)")
