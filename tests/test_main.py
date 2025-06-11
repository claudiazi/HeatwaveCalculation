import asyncio
import os
import sys

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from download_data_from_api import main

async def test_main():
    print("Testing main function...")

    # Call the main function
    dataframes = await main()

    # Print information about the returned DataFrames
    print(f"\nNumber of DataFrames: {len(dataframes)}")

    for filename, df in dataframes.items():
        print(f"\nDataFrame for {filename}:")
        print(f"Shape: {df.shape}")
        print(f"Columns: {df.columns.tolist()}")
        print("\nFirst 5 rows:")
        print(df.head())
        print("\nBasic statistics:")
        print(df.describe())

if __name__ == "__main__":
    asyncio.run(test_main())
