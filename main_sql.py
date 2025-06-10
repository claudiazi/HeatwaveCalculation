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

def read_nc_file(file_path):
    """
    Read a NetCDF (.nc) file and return its contents as a pandas DataFrame.

    Args:
        file_path (str): Path to the NetCDF file

    Returns:
        pd.DataFrame: DataFrame containing the data from the NetCDF file
    """
    try:
        import xarray as xr
        import pandas as pd

        # Check if file exists
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            return pd.DataFrame()

        # Check if file is a NetCDF file
        if not str(file_path).lower().endswith('.nc'):
            print(f"File is not a NetCDF file: {file_path}")
            return pd.DataFrame()

        # Open the NetCDF file using xarray
        print(f"Reading NetCDF file: {file_path}")
        ds = xr.open_dataset(file_path)

        # Display metadata
        print(f"NetCDF file metadata: {ds}")

        # Convert to pandas DataFrame
        df = ds.to_dataframe().reset_index()

        # Close the dataset
        ds.close()

        print(f"Successfully read NetCDF file: {file_path}")
        return df

    except Exception as e:
        print(f"Error reading NetCDF file {file_path}: {str(e)}")
        return pd.DataFrame()

def load_data(spark, data_dir):
    """
    Load the KNMI weather data from CSV files or NetCDF files.

    Args:
        spark (SparkSession): The Spark session
        data_dir (str): Directory containing the data files

    Returns:
        DataFrame: A DataFrame containing the weather data
    """
    # Check for NetCDF files in airflow/data directory
    nc_files = glob.glob(os.path.join("airflow", "data", "**/*.nc"), recursive=True)

    if nc_files:
        print(f"Found {len(nc_files)} NetCDF files in airflow/data directory")

        # Read all NetCDF files and convert to pandas DataFrames
        all_dfs = []
        for nc_file in nc_files:
            print(f"Processing NetCDF file: {nc_file}")
            df = read_nc_file(nc_file)
            if not df.empty:
                all_dfs.append(df)
                print(f"Successfully read DataFrame from {nc_file} with shape {df.shape}")
            else:
                print(f"Failed to read DataFrame from {nc_file}")

        if not all_dfs:
            print("Error: no file existed")
            raise FileNotFoundError("No valid data found in NetCDF files")

        # Combine all DataFrames
        import pandas as pd
        combined_df = pd.concat(all_dfs, ignore_index=True)

        # Convert pandas DataFrame to Spark DataFrame
        spark_df = spark.createDataFrame(combined_df)

        # Add DATE column if it doesn't exist but DTG exists
        if "DATE" not in spark_df.columns and "DTG" in spark_df.columns:
            spark_df = spark_df.withColumn("DATE", F.to_date(F.substring("DTG", 1, 8), "yyyyMMdd"))
            print("Added DATE column based on DTG")

        # Ensure the DataFrame has the required columns
        required_columns = ["DATE", "TX_DRYB_10", "TN_DRYB_10", "LOCATION"]
        missing_columns = [col for col in required_columns if col not in spark_df.columns]

        if missing_columns:
            print("Error: no file existed")
            raise FileNotFoundError(f"NetCDF data is missing required columns: {missing_columns}")

        return spark_df
    else:
        print("Error: no file existed")
        raise FileNotFoundError("No NetCDF files found in airflow/data directory")

def load_csv_data(spark, data_dir):
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
    Filter the data for the De Bilt weather station using PySpark SQL.

    Args:
        df (DataFrame): The input DataFrame
        include_min_temp (bool): Whether to include the minimum temperature column (for coldwave calculation)

    Returns:
        DataFrame: A DataFrame containing only data for De Bilt
    """
    # Register the DataFrame as a temporary SQL table
    df.createOrReplaceTempView("all_weather_data")

    # Use SQL to filter the data
    spark = df.sparkSession

    if include_min_temp:
        # For coldwave calculation, include minimum temperature
        de_bilt_df = spark.sql("""
            SELECT DATE, TX_DRYB_10, TN_DRYB_10
            FROM all_weather_data
            WHERE LOCATION = '260_T_a'
              AND TX_DRYB_10 IS NOT NULL
              AND TN_DRYB_10 IS NOT NULL
              AND YEAR(DATE) >= 2003
        """)
    else:
        # For heatwave calculation, only include maximum temperature
        de_bilt_df = spark.sql("""
            SELECT DATE, TX_DRYB_10
            FROM all_weather_data
            WHERE LOCATION = '260_T_a'
              AND TX_DRYB_10 IS NOT NULL
              AND YEAR(DATE) >= 2003
        """)

    return de_bilt_df

def calculate_heatwaves(df):
    """
    Calculate heatwaves based on KNMI's definition using PySpark SQL.

    A heatwave is defined as:
    - A period of at least 5 consecutive days with max temperature ≥ 25°C
    - Within those 5+ days, at least 3 days with max temperature ≥ 30°C

    Args:
        df (DataFrame): The input DataFrame with DATE and TX_DRYB_10 columns

    Returns:
        DataFrame: A DataFrame containing the heatwave periods
    """
    # Register the DataFrame as a temporary SQL table
    df.createOrReplaceTempView("weather_data")

    # Use SQL to create a view with hot and tropical day indicators
    spark = df.sparkSession
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW weather_with_indicators AS
        SELECT 
            *,
            CASE WHEN TX_DRYB_10 >= 25.0 THEN 1 ELSE 0 END AS is_hot,
            CASE WHEN TX_DRYB_10 >= 30.0 THEN 1 ELSE 0 END AS is_tropical
        FROM weather_data
    """)

    # Use SQL to identify groups of consecutive hot days
    # First, create a view with the lag function to detect changes
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW weather_with_changes AS
        SELECT 
            *,
            CASE 
                WHEN LAG(is_hot, 1) OVER (PARTITION BY YEAR(DATE) ORDER BY DATE) != is_hot THEN 1 
                ELSE 0 
            END AS hot_day_change
        FROM weather_with_indicators
    """)

    # Create a view with group IDs for consecutive hot days
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW weather_with_groups AS
        SELECT 
            *,
            SUM(hot_day_change) OVER (PARTITION BY YEAR(DATE) ORDER BY DATE) AS hot_group_id
        FROM weather_with_changes
    """)

    # Filter for hot days only and group by hot_group_id
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW hot_days AS
        SELECT * FROM weather_with_groups
        WHERE is_hot = 1
    """)

    # Group by hot_group_id to analyze each potential heatwave
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW grouped_hot_days AS
        SELECT 
            hot_group_id,
            MIN(DATE) AS start_date,
            MAX(DATE) AS end_date,
            COUNT(DATE) AS duration,
            SUM(is_tropical) AS tropical_days,
            MAX(TX_DRYB_10) AS max_temperature
        FROM hot_days
        GROUP BY hot_group_id
    """)

    # Filter for periods that meet the heatwave criteria
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW heatwaves AS
        SELECT 
            hot_group_id,
            start_date,
            end_date,
            duration,
            tropical_days,
            max_temperature,
            date_format(start_date, 'dd MMM yyyy') AS start_date_formatted,
            date_format(end_date, 'dd MMM yyyy') AS end_date_formatted
        FROM grouped_hot_days
        WHERE duration >= 5 AND tropical_days >= 3
    """)

    # Select and order the columns for output
    result_df = spark.sql("""
        SELECT 
            start_date_formatted,
            end_date_formatted,
            duration,
            tropical_days,
            max_temperature
        FROM heatwaves
        ORDER BY start_date
    """)

    return result_df

def calculate_coldwaves(df):
    """
    Calculate coldwaves based on the definition using PySpark SQL:
    - A period of at least 5 consecutive days with max temperature < 0°C
    - Within those 5+ days, at least 3 days with min temperature < -10°C

    Args:
        df (DataFrame): The input DataFrame with DATE, TX_DRYB_10 (max temp), and TN_DRYB_10 (min temp) columns

    Returns:
        DataFrame: A DataFrame containing the coldwave periods
    """
    # Register the DataFrame as a temporary SQL table
    df.createOrReplaceTempView("cold_weather_data")

    # Use SQL to create a view with cold and high frost day indicators
    spark = df.sparkSession
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW cold_weather_with_indicators AS
        SELECT 
            *,
            CASE WHEN TX_DRYB_10 < 0.0 THEN 1 ELSE 0 END AS is_cold,
            CASE WHEN TN_DRYB_10 < -10.0 THEN 1 ELSE 0 END AS is_high_frost
        FROM cold_weather_data
    """)

    # Use SQL to identify groups of consecutive cold days
    # First, create a view with the lag function to detect changes
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW cold_weather_with_changes AS
        SELECT 
            *,
            CASE 
                WHEN LAG(is_cold, 1) OVER (PARTITION BY YEAR(DATE) ORDER BY DATE) != is_cold THEN 1 
                ELSE 0 
            END AS cold_day_change
        FROM cold_weather_with_indicators
    """)

    # Create a view with group IDs for consecutive cold days
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW cold_weather_with_groups AS
        SELECT 
            *,
            SUM(cold_day_change) OVER (PARTITION BY YEAR(DATE) ORDER BY DATE) AS cold_group_id
        FROM cold_weather_with_changes
    """)

    # Filter for cold days only and group by cold_group_id
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW cold_days AS
        SELECT * FROM cold_weather_with_groups
        WHERE is_cold = 1
    """)

    # Group by cold_group_id to analyze each potential coldwave
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW grouped_cold_days AS
        SELECT 
            cold_group_id,
            MIN(DATE) AS start_date,
            MAX(DATE) AS end_date,
            COUNT(DATE) AS duration,
            SUM(is_high_frost) AS high_frost_days,
            MIN(TN_DRYB_10) AS min_temperature
        FROM cold_days
        GROUP BY cold_group_id
    """)

    # Filter for periods that meet the coldwave criteria
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW coldwaves AS
        SELECT 
            cold_group_id,
            start_date,
            end_date,
            duration,
            high_frost_days,
            min_temperature,
            date_format(start_date, 'dd MMM yyyy') AS start_date_formatted,
            date_format(end_date, 'dd MMM yyyy') AS end_date_formatted
        FROM grouped_cold_days
        WHERE duration >= 5 AND high_frost_days >= 3
    """)

    # Select and order the columns for output
    result_df = spark.sql("""
        SELECT 
            start_date_formatted,
            end_date_formatted,
            duration,
            high_frost_days,
            min_temperature
        FROM coldwaves
        ORDER BY start_date
    """)

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

        # Download and extract data if it doesn't exist or is empty
        data_dir = "airflow/data"

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
