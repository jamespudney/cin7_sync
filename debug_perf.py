"""Profile where engine spends its time, run outside Streamlit."""
import time
import pandas as pd
import glob

t0 = time.time()
products = pd.read_csv(
    sorted(glob.glob("output/products_*.csv"))[-1], low_memory=False)
print(f"Load products ({len(products):,} rows): {time.time()-t0:.2f}s")

t0 = time.time()
sl_files = sorted(glob.glob("output/sale_lines_*.csv"))
sl = pd.concat([pd.read_csv(f, low_memory=False) for f in sl_files],
                ignore_index=True)
if "LineID" in sl.columns:
    sl = sl.drop_duplicates(subset=["LineID"], keep="last")
print(f"Load + dedupe sale_lines ({len(sl_files)} files, "
      f"{len(sl):,} rows): {time.time()-t0:.2f}s")

t0 = time.time()
stock = pd.read_csv(
    sorted(glob.glob("output/stock_on_hand_*.csv"))[-1], low_memory=False)
print(f"Load stock ({len(stock):,} rows): {time.time()-t0:.2f}s")

t0 = time.time()
boms = pd.read_csv(sorted(glob.glob("output/boms_*.csv"))[-1])
print(f"Load boms ({len(boms):,} rows): {time.time()-t0:.2f}s")

print(f"\nTotal data files on disk:")
print(f"  products: {len(glob.glob('output/products_*.csv'))}")
print(f"  sale_lines: {len(glob.glob('output/sale_lines_*.csv'))}")
print(f"  stock: {len(glob.glob('output/stock_on_hand_*.csv'))}")
print(f"  boms: {len(glob.glob('output/boms_*.csv'))}")
