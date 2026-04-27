import pandas as pd
import glob

f = sorted(glob.glob("output/boms_*.csv"))[-1]
df = pd.read_csv(f)

print(f"File: {f}")
print(f"Total BOM rows: {len(df):,}")
print(f"Unique assemblies: {df['AssemblySKU'].nunique():,}")

missing = df[df["ComponentSKU"].isna()]
print(f"Rows with missing ComponentSKU: {len(missing)}")
print(f"Unique assemblies missing components: {missing['AssemblySKU'].nunique()}")
print()

if len(missing) > 0:
    print("=== Assemblies with missing components ===")
    for s in missing["AssemblySKU"].unique():
        print(f"  {s}")
else:
    print("No missing components — all clean!")
