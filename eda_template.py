import polars as pl

# 1. Read the TSV file
# Note: quote_char=None is often safer for raw scraping data if quotes aren't strictly used for escaping
df = pl.read_csv("p2000_2026-01-03-10:40.tsv", separator='\t', quote_char=None)

# 2. Convert 'Timestamp' to actual datetime objects
# Format is DD-MM-YYYY HH:MM:SS
df = df.with_columns(
    pl.col("Timestamp").str.to_datetime("%d-%m-%Y %H:%M:%S")
)

# 3. Basic Stats
print(f"Total Rows: {df.height}")
print(f"Total Columns: {df.width}")
print("-" * 40)

# 4. Datetime Range
min_ts = df["Timestamp"].min()
max_ts = df["Timestamp"].max()
duration = max_ts - min_ts

print(f"Start Time: {min_ts}")
print(f"End Time:   {max_ts}")
print(f"Duration:   {duration}")
print("-" * 40)

# 5. Top 20 Unique Values for Text Columns
# We filter for String type columns specifically
string_cols = [name for name, dtype in df.schema.items() if dtype == pl.String]

for col_name in string_cols:
    print(f"\nTop 20 frequencies for column: '{col_name}'")
    
    # Calculate value counts, sort descending, and take top 20
    top_20 = (
        df[col_name]
        .value_counts()
        .sort("count", descending=True)
        .head(20)
    )
    
    print(top_20)
