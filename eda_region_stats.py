import polars as pl

# 1. Read the TSV file
df = pl.read_csv("p2000_2026-01-03-10:40.tsv", separator='\t', quote_char=None)

# 2. Advanced Filtering (Clean Data)
df_clean = (
    df
    .with_columns(pl.col("Timestamp").str.to_datetime("%d-%m-%Y %H:%M:%S"))
    .filter(
        (pl.col("Message") != "TESTOPROEP MOB") &
        (~pl.col("Message").str.contains(r"(?i)\btest\b"))
    )
)

# 3. Aggregate per Region and Service
# First, get counts per Region + Service
stats = (
    df_clean
    .group_by(["Region", "Service"])
    .len()  # Count rows in each group
)

# 4. Pivot to create columns for each service
# We want: Region | Ambulance | Brandweer | Politie
pivot_df = (
    stats
    .pivot(
        values="len",
        index="Region",
        on="Service",
        aggregate_function="sum"
    )
    .fill_null(0) # Replace NaNs with 0
)

# 5. Clean up columns and Calculate Total
# Ensure expected columns exist (in case one service had 0 calls total)
expected_cols = ["Ambulance", "Brandweer", "Politie"]
existing_cols = pivot_df.columns

for col in expected_cols:
    if col not in existing_cols:
        pivot_df = pivot_df.with_columns(pl.lit(0).alias(col))

# Rename to English as requested and calculate Total
final_table = (
    pivot_df
    .rename({
        "Brandweer": "Firefighters",
        "Politie": "Police"
    })
    .with_columns(
        (pl.col("Ambulance") + pl.col("Firefighters") + pl.col("Police")).alias("Total")
    )
    .sort("Total", descending=True)
)

# 6. Display
print(f"Stats based on {df_clean.height} cleaned records.\n")
print(final_table)
