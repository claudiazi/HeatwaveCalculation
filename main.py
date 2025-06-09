"""
Heatwave Calculation Application

This application calculates heatwaves for The Netherlands based on KNMI's definition:
- A period of at least 5 consecutive days with max temperature ≥ 25°C
- Within those 5+ days, at least 3 days with max temperature ≥ 30°C

Author: AI Assistant
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import os
import sys
import urllib.request
import tarfile
import tempfile
import glob
from datetime import datetime

def download_and_extract_data(url, extract_to="."):
    """
    Download and extract the data from the given URL.

    Args:
        url (str): URL to download the data from
        extract_to (str): Directory to extract the data to

    Returns:
        str: Path to the extracted data directory
    """
    print(f"Downloading data from {url}...")

    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        urllib.request.urlretrieve(url, temp_file.name)

        print(f"Extracting data to {extract_to}...")
        with tarfile.open(temp_file.name) as tar:
            tar.extractall(path=extract_to)

    os.unlink(temp_file.name)
    return extract_to

def load_data(spark, data_dir):
    """
    Load the KNMI weather data from CSV files.

    Args:
        spark (SparkSession): The Spark session
        data_dir (str): Directory containing the data files

    Returns:
        DataFrame: A DataFrame containing the weather data
    """
    # Define the schema for the data
    schema = StructType([
        StructField("DTG", StringType(), True),
        StructField("LOCATION", StringType(), True),
        StructField("NAME", StringType(), True),
        StructField("LATITUDE", DoubleType(), True),
        StructField("LONGITUDE", DoubleType(), True),
        StructField("ALTITUDE", DoubleType(), True),
        StructField("U_BOOL_10", StringType(), True),
        StructField("T_DRYB_10", DoubleType(), True),
        StructField("TN_10CM_PAST_6H_10", DoubleType(), True),
        StructField("T_DEWP_10", DoubleType(), True),
        StructField("T_DEWP_SEA_10", DoubleType(), True),
        StructField("T_DRYB_SEA_10", DoubleType(), True),
        StructField("TN_DRYB_10", DoubleType(), True),
        StructField("T_WETB_10", DoubleType(), True),
        StructField("TX_DRYB_10", DoubleType(), True),
        StructField("U_10", DoubleType(), True),
        StructField("U_SEA_10", DoubleType(), True)
    ])

    # Find all CSV files in the data directory
    csv_files = glob.glob(os.path.join(data_dir, "**/*.csv"), recursive=True)

    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {data_dir}")

    print(f"Found {len(csv_files)} CSV files")

    # Load all CSV files into a single DataFrame
    df = spark.read.csv(csv_files, header=True, schema=schema, sep=",")

    # Convert date string to date type
    df = df.withColumn("DATE", F.to_date(F.substring("DTG", 1, 8), "yyyyMMdd"))

    return df

def filter_de_bilt_data(df, include_min_temp=False):
    """
    Filter the data for the De Bilt weather station.

    Args:
        df (DataFrame): The input DataFrame
        include_min_temp (bool): Whether to include the minimum temperature column (for coldwave calculation)

    Returns:
        DataFrame: A DataFrame containing only data for De Bilt
    """
    # Filter for De Bilt weather station (LOCATION = 260_T_a)
    de_bilt_df = df.filter(df.LOCATION == "260_T_a")

    # Select the columns we need
    if include_min_temp:
        de_bilt_df = de_bilt_df.select("DATE", "TX_DRYB_10", "TN_DRYB_10")
        # Filter out rows with null values in either column
        de_bilt_df = de_bilt_df.filter(
            de_bilt_df.TX_DRYB_10.isNotNull() & 
            de_bilt_df.TN_DRYB_10.isNotNull()
        )
    else:
        de_bilt_df = de_bilt_df.select("DATE", "TX_DRYB_10")
        # Filter out rows with null values
        de_bilt_df = de_bilt_df.filter(de_bilt_df.TX_DRYB_10.isNotNull())

    # Filter for data from 2003 onwards
    de_bilt_df = de_bilt_df.filter(F.year(de_bilt_df.DATE) >= 2003)

    return de_bilt_df

def calculate_heatwaves(df):
    """
    Calculate heatwaves based on KNMI's definition.

    A heatwave is defined as:
    - A period of at least 5 consecutive days with max temperature ≥ 25°C
    - Within those 5+ days, at least 3 days with max temperature ≥ 30°C

    Args:
        df (DataFrame): The input DataFrame with DATE and TX_DRYB_10 columns

    Returns:
        DataFrame: A DataFrame containing the heatwave periods
    """
    # Create a column indicating if the day is hot (≥ 25°C)
    df = df.withColumn("is_hot", F.when(df.TX_DRYB_10 >= 25.0, 1).otherwise(0))

    # Create a column indicating if the day is tropical (≥ 30°C)
    df = df.withColumn("is_tropical", F.when(df.TX_DRYB_10 >= 30.0, 1).otherwise(0))

    # Create a window specification for consecutive days analysis
    window_spec = Window.orderBy("DATE")

    # Identify groups of consecutive hot days
    df = df.withColumn("hot_day_change", 
                      F.when(F.lag("is_hot", 1).over(window_spec) != df.is_hot, 1)
                       .otherwise(0))

    # Create a group ID for each sequence of consecutive hot days
    df = df.withColumn("hot_group_id", F.sum("hot_day_change").over(window_spec))

    # Filter for hot days only (is_hot = 1)
    hot_days_df = df.filter(df.is_hot == 1)

    # Group by hot_group_id to analyze each potential heatwave
    grouped_df = hot_days_df.groupBy("hot_group_id").agg(
        F.min("DATE").alias("start_date"),
        F.max("DATE").alias("end_date"),
        F.count("DATE").alias("duration"),
        F.sum("is_tropical").alias("tropical_days"),
        F.max("TX_DRYB_10").alias("max_temperature")
    )

    # Filter for periods that meet the heatwave criteria
    heatwaves_df = grouped_df.filter(
        (grouped_df.duration >= 5) & (grouped_df.tropical_days >= 3)
    )

    # Format the dates for output
    heatwaves_df = heatwaves_df.withColumn(
        "start_date_formatted", 
        F.date_format(heatwaves_df.start_date, "dd MMM yyyy")
    )
    heatwaves_df = heatwaves_df.withColumn(
        "end_date_formatted", 
        F.date_format(heatwaves_df.end_date, "dd MMM yyyy")
    )

    # Select and order the columns for output
    result_df = heatwaves_df.select(
        "start_date_formatted",
        "end_date_formatted",
        "duration",
        "tropical_days",
        "max_temperature"
    ).orderBy("start_date")

    return result_df

def calculate_coldwaves(df):
    """
    Calculate coldwaves based on the definition:
    - A period of at least 5 consecutive days with max temperature < 0°C
    - Within those 5+ days, at least 3 days with min temperature < -10°C

    Args:
        df (DataFrame): The input DataFrame with DATE, TX_DRYB_10 (max temp), and TN_DRYB_10 (min temp) columns

    Returns:
        DataFrame: A DataFrame containing the coldwave periods
    """
    # Create a column indicating if the day is cold (max temp < 0°C)
    df = df.withColumn("is_cold", F.when(df.TX_DRYB_10 < 0.0, 1).otherwise(0))

    # Create a column indicating if the day has high frost (min temp < -10°C)
    df = df.withColumn("is_high_frost", F.when(df.TN_DRYB_10 < -10.0, 1).otherwise(0))

    # Create a window specification for consecutive days analysis
    window_spec = Window.orderBy("DATE")

    # Identify groups of consecutive cold days
    df = df.withColumn("cold_day_change", 
                      F.when(F.lag("is_cold", 1).over(window_spec) != df.is_cold, 1)
                       .otherwise(0))

    # Create a group ID for each sequence of consecutive cold days
    df = df.withColumn("cold_group_id", F.sum("cold_day_change").over(window_spec))

    # Filter for cold days only (is_cold = 1)
    cold_days_df = df.filter(df.is_cold == 1)

    # Group by cold_group_id to analyze each potential coldwave
    grouped_df = cold_days_df.groupBy("cold_group_id").agg(
        F.min("DATE").alias("start_date"),
        F.max("DATE").alias("end_date"),
        F.count("DATE").alias("duration"),
        F.sum("is_high_frost").alias("high_frost_days"),
        F.min("TN_DRYB_10").alias("min_temperature")
    )

    # Filter for periods that meet the coldwave criteria
    coldwaves_df = grouped_df.filter(
        (grouped_df.duration >= 5) & (grouped_df.high_frost_days >= 3)
    )

    # Format the dates for output
    coldwaves_df = coldwaves_df.withColumn(
        "start_date_formatted", 
        F.date_format(coldwaves_df.start_date, "dd MMM yyyy")
    )
    coldwaves_df = coldwaves_df.withColumn(
        "end_date_formatted", 
        F.date_format(coldwaves_df.end_date, "dd MMM yyyy")
    )

    # Select and order the columns for output
    result_df = coldwaves_df.select(
        "start_date_formatted",
        "end_date_formatted",
        "duration",
        "high_frost_days",
        "min_temperature"
    ).orderBy("start_date")

    return result_df

def main():
    """
    Main function to run the heatwave and coldwave calculation application.
    """
    try:
        # Parse command line arguments
        import argparse
        parser = argparse.ArgumentParser(description='Calculate heatwaves and coldwaves for The Netherlands.')
        parser.add_argument('--mode', choices=['heatwaves', 'coldwaves', 'both'], default='both',
                            help='Calculation mode: heatwaves, coldwaves, or both (default: both)')
        args = parser.parse_args()

        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("Weather Extremes Calculation") \
            .getOrCreate()

        # Set log level to reduce verbosity
        spark.sparkContext.setLogLevel("WARN")

        print(f"Starting Weather Extremes Calculation Application (Mode: {args.mode})")

        # Download and extract data if it doesn't exist
        data_dir = "data"
        if not os.path.exists(data_dir):
            url = "https://gddassesmentdata.blob.core.windows.net/knmi-data/data.tgz?sp=r&st=2024-01-03T14:42:11Z&se=2025-01-03T22:42:11Z&spr=https&sv=2022-11-02&sr=c&sig=jcOeksvhjJGDTCM%2B2CzrjR3efJI7jq5a3SnT8aiQBc8%3D"
            download_and_extract_data(url, data_dir)

        # Load the data
        print("Loading data...")
        df = load_data(spark, data_dir)
        print(f"Loaded {df.count()} records")

        # Calculate heatwaves if requested
        if args.mode in ['heatwaves', 'both']:
            # Filter for De Bilt data (only max temperature needed for heatwaves)
            print("Filtering for De Bilt weather station (for heatwaves)...")
            de_bilt_df_heat = filter_de_bilt_data(df, include_min_temp=False)
            print(f"Found {de_bilt_df_heat.count()} records for De Bilt from 2003 onwards")

            # Calculate heatwaves
            print("Calculating heatwaves...")
            heatwaves_df = calculate_heatwaves(de_bilt_df_heat)

            # Display the results
            print("\nHeatwaves in The Netherlands (De Bilt) from 2003 onwards:")
            print("=" * 80)
            print(f"{'From date':<12} | {'To date (inc.)':<12} | {'Duration (days)':<15} | {'Tropical days':<15} | {'Max temp (°C)':<12}")
            print("-" * 80)

            # Collect the results to the driver
            heatwaves = heatwaves_df.collect()

            if not heatwaves:
                print("No heatwaves found.")
            else:
                for row in heatwaves:
                    print(f"{row['start_date_formatted']:<12} | {row['end_date_formatted']:<12} | {row['duration']:<15} | {row['tropical_days']:<15} | {row['max_temperature']:<12.1f}")

            # Save the results to a CSV file
            output_file = "heatwaves.csv"
            print(f"\nSaving heatwave results to {output_file}...")

            # Convert to Pandas DataFrame for easier saving
            pandas_df = heatwaves_df.toPandas()

            # Rename columns to match the required output format
            pandas_df.columns = ["From date", "To date (inc.)", "Duration (in days)", "Number of tropical days", "Max temperature"]

            # Save to CSV
            pandas_df.to_csv(output_file, index=False)
            print(f"Heatwave results saved to {output_file}")

        # Calculate coldwaves if requested
        if args.mode in ['coldwaves', 'both']:
            # Filter for De Bilt data (including min temperature for coldwaves)
            print("\nFiltering for De Bilt weather station (for coldwaves)...")
            de_bilt_df_cold = filter_de_bilt_data(df, include_min_temp=True)
            print(f"Found {de_bilt_df_cold.count()} records for De Bilt from 2003 onwards")

            # Calculate coldwaves
            print("Calculating coldwaves...")
            coldwaves_df = calculate_coldwaves(de_bilt_df_cold)

            # Display the results
            print("\nColdwaves in The Netherlands (De Bilt) from 2003 onwards:")
            print("=" * 80)
            print(f"{'From date':<12} | {'To date (inc.)':<12} | {'Duration (days)':<15} | {'High frost days':<15} | {'Min temp (°C)':<12}")
            print("-" * 80)

            # Collect the results to the driver
            coldwaves = coldwaves_df.collect()

            if not coldwaves:
                print("No coldwaves found.")
            else:
                for row in coldwaves:
                    print(f"{row['start_date_formatted']:<12} | {row['end_date_formatted']:<12} | {row['duration']:<15} | {row['high_frost_days']:<15} | {row['min_temperature']:<12.1f}")

            # Save the results to a CSV file
            output_file = "coldwaves.csv"
            print(f"\nSaving coldwave results to {output_file}...")

            # Convert to Pandas DataFrame for easier saving
            pandas_df = coldwaves_df.toPandas()

            # Rename columns to match the required output format
            pandas_df.columns = ["From date", "To date (inc.)", "Duration (in days)", "Number of high frost days", "Min temperature"]

            # Save to CSV
            pandas_df.to_csv(output_file, index=False)
            print(f"Coldwave results saved to {output_file}")

        print("\nWeather extremes calculation completed successfully")

    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)
    finally:
        # Stop Spark session
        if 'spark' in locals():
            spark.stop()
            print("Spark session stopped")

if __name__ == "__main__":
    main()
