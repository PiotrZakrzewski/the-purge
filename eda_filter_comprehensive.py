# Comprehensive filter for "test" (case-insensitive)
df_clean = (
    df
    .with_columns(pl.col("Timestamp").str.to_datetime("%d-%m-%Y %H:%M:%S"))
    .filter(
        ~pl.col("Message").str.contains("(?i)test")
    )
)

# Basic Stats for Cleaned Data
print(f"Original Rows: {df.height}")
print(f"Cleaned Rows:  {df_clean.height}")
print(f"Rows Removed:  {df.height - df_clean.height}")
print("-" * 40)

# Check Top 20 for Message to see what remains
top_20_messages = (
    df_clean["Message"]
    .value_counts()
    .sort("count", descending=True)
    .head(20)
)
print("\nTop 20 Remaining Messages:")
print(top_20_messages)
