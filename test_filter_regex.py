import polars as pl

def run_test():
    data = {
        "id": [1, 2, 3, 4, 5, 6],
        "Message": [
            "This is a valid message",
            "TESTOPROEP MOB",       # Ignores (Compound word)
            "This is a Test: run",  # Matches (Test followed by non-word char :)
            "test bla bla",         # Matches (Whole word test)
            "Don't touch my testicles", # Ignores (test is part of word)
            "A contest is fun"      # Ignores (test is part of word)
        ]
    }
    
    df = pl.DataFrame(data)
    
    # Regex: Case-insensitive, whole word 'test'
    regex = r"(?i)\btest\b"
    
    df_filtered = df.filter(
        ~pl.col("Message").str.contains(regex)
    )
    
    print("\nRegex used:", regex)
    print("Filtered Data:")
    print(df_filtered)
    
    remaining_ids = df_filtered["id"].to_list()
    # We expect 'TESTOPROEP' (2) to remain because it's not the word 'test'.
    # We expect 'testicles' (5) to remain.
    # We expect 3 and 4 to be removed.
    expected_ids = [1, 2, 5, 6]
    
    if remaining_ids == expected_ids:
        print("\n✅ TEST PASSED: Regex matches 'Test:' and 'test bla' but ignores 'testicles'.")
    else:
        print(f"\n❌ TEST FAILED: Expected ids {expected_ids}, got {remaining_ids}")
        exit(1)

if __name__ == "__main__":
    run_test()