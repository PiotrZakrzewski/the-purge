import polars as pl

# 1. Read the TSV file
df = pl.read_csv("p2000_2026-01-03-10:40.tsv", separator='\t', quote_char=None)

# 2. Pre-process: Parse dates and filter out test calls
# We filter out "TESTOPROEP MOB" from the Message column
df_filtered = (
    df
    .with_columns(pl.col("Timestamp").str.to_datetime("%d-%m-%Y %H:%M:%S"))
    .filter(pl.col("Message") != "TESTOPROEP MOB")
)

# 3. Basic Stats for Filtered Data
print(f"Original Rows: {df.height}")
print(f"Filtered Rows: {df_filtered.height}")
print(f"Rows Removed:  {df.height - df_filtered.height}")
print("-" * 40)

# 4. Datetime Range of Filtered Data
min_ts = df_filtered["Timestamp"].min()
max_ts = df_filtered["Timestamp"].max()
duration = max_ts - min_ts

print(f"Filtered Start Time: {min_ts}")
print(f"Filtered End Time:   {max_ts}")
print(f"Filtered Duration:   {duration}")
print("-" * 40)

# 5. Top 20 Unique Values for Text Columns (Updated)
string_cols = [name for name, dtype in df_filtered.schema.items() if dtype == pl.String]

for col_name in string_cols:
    print(f"\nTop 20 frequencies for column: '{col_name}' (Filtered)")
    
    top_20 = (
        df_filtered[col_name]
        .value_counts()
        .sort("count", descending=True)
        .head(20)
    )
    
    print(top_20)
