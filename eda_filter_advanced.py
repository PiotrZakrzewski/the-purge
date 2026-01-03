import polars as pl

# 1. Read the TSV file
df = pl.read_csv("p2000_2026-01-03-10:40.tsv", separator='\t', quote_char=None)

# 2. Advanced Filtering
# - Parse dates
# - Filter out exact "TESTOPROEP MOB"
# - Filter out messages containing the whole word "test" (case-insensitive)
df_clean = (
    df
    .with_columns(pl.col("Timestamp").str.to_datetime("%d-%m-%Y %H:%M:%S"))
    .filter(
        (pl.col("Message") != "TESTOPROEP MOB") &
        (~pl.col("Message").str.contains(r"(?i)\btest\b"))
    )
)

# 3. Stats
print(f"Original Rows: {df.height}")
print(f"Cleaned Rows:  {df_clean.height}")
print(f"Rows Removed:  {df.height - df_clean.height}")
print("-" * 40)

# 4. Check Top 20 Remaining Messages
print("\nTop 20 Remaining Messages:")
print(
    df_clean["Message"]
    .value_counts()
    .sort("count", descending=True)
    .head(20)
)
