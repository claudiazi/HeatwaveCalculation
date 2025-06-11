"""
Heatwave Calculation Application

This application calculates heatwaves for The Netherlands based on KNMI's definition:
- A period of at least 5 consecutive days with max temperature ≥ 25°C
- Within those 5+ days, at least 3 days with max temperature ≥ 30°C

"""

import glob
import os
import sys
import tarfile
import tempfile
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window


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


def extract_local_data(file_path, extract_to="."):
    """
    Extract data from a local file.

    Args:
        file_path (str): Path to the local file
        extract_to (str): Directory to extract the data to

    Returns:
        str: Path to the extracted data directory
    """
    print(f"Extracting local file {file_path} to {extract_to}...")

    with tarfile.open(file_path) as tar:
        tar.extractall(path=extract_to)

    return extract_to


def load_data(spark, data_dir):
    """
    Load the KNMI weather data from data.tgz.
    First checks if extracted_data path is not empty and uses existing files if found.
    If empty, checks if data.tgz exists in the root path and reads directly from it if found.
    If not found, tries to download it from the URL.
    No NetCDF file checking is performed.

    Args:
        spark (SparkSession): The Spark session
        data_dir (str): Directory containing the data files

    Returns:
        DataFrame: A DataFrame containing the weather data
    """
    # First check if the extracted_data path already has files
    all_files = glob.glob(os.path.join(data_dir, "**/*"), recursive=True)
    if all_files:
        print(f"Found {len(all_files)} existing files in {data_dir}. Using these files...")
        return load_text_data(spark, data_dir)

    data_tgz_extracted = False

    # If no existing files, check if data.tgz exists in the root path
    if os.path.exists("data.tgz"):
        print("Found data.tgz file in root path. Reading directly from it...")
        try:
            # Use extract_local_data for local files
            extract_local_data("data.tgz", data_dir)
            data_tgz_extracted = True
        except Exception as e:
            print(f"Failed to extract data.tgz from root path: {str(e)}")
    else:
        print("No data.tgz file found in root path. Trying to download...")

        # URL for data.tgz (using the same URL as in the Airflow DAG)
        data_tgz_url = "https://gddassesmentdata.blob.core.windows.net/knmi-data/data.tgz?sp=r&st=2024-01-03T14:42:11Z&se=2025-01-03T22:42:11Z&spr=https&sv=2022-11-02&sr=c&sig=jcOeksvhjJGDTCM%2B2CzrjR3efJI7jq5a3SnT8aiQBc8%3D"

        try:
            # Download and extract data.tgz to the data directory
            download_and_extract_data(data_tgz_url, data_dir)
            data_tgz_extracted = True
        except Exception as e:
            print(f"Failed to download data.tgz from URL: {str(e)}")

    # If data.tgz was successfully extracted, we can proceed
    if data_tgz_extracted:
        print("Successfully extracted data.tgz")

        # Check if any files were extracted
        all_files = glob.glob(os.path.join(data_dir, "**/*.*"), recursive=True)
        if all_files:
            print(f"Found {len(all_files)} files in the extracted data")
            # Load the actual data using load_text_data
            return load_text_data(spark, data_dir)
        else:
            print("Warning: No files found in the extracted data")
            raise FileNotFoundError("Failed to load data: no files found in extracted data")
    else:
        # If we couldn't extract data.tgz, raise an error
        print("Error: Failed to extract data.tgz")
        raise FileNotFoundError("Failed to load data: could not find or extract data.tgz")

def load_text_data(spark, data_dir):
    """
    Load KNMI weather data from space-delimited text files using header positions.
    Skips files without proper headers and logs warnings.

    Args:
        spark (SparkSession): The Spark session
        data_dir (str): Directory containing the data files

    Returns:
        DataFrame: Combined DataFrame containing weather data with DATE column
    """
    # Find all files in the data directory (including files without extensions)
    all_files = glob.glob(os.path.join(data_dir, "**/*"), recursive=True)

    if not all_files:
        raise FileNotFoundError(f"No files found in {data_dir}")

    print(f"Found {len(all_files)} files to process")

    # Field names in expected order
    field_names = ["DTG", "LOCATION", "NAME", "LATITUDE", "LONGITUDE", "ALTITUDE", 
                  "U_BOOL_10", "T_DRYB_10", "TN_10CM_PAST_6H_10", "T_DEWP_10",
                  "T_DEWP_SEA_10", "T_DRYB_SEA_10", "TN_DRYB_10", "T_WETB_10", 
                  "TX_DRYB_10", "U_10", "U_SEA_10"]

    valid_dataframes = []

    # Process each file individually to check for headers
    for file_path in all_files:
        try:
            df = spark.read.option("multiline", "true").option("wholetext", "true").text(file_path)
            
            # Extract header line
            header_df = df.select(F.split(F.col("value"), "\n").alias("lines")) \
                .select(F.explode(F.col("lines")).alias("line")) \
                .filter(F.col("line").startswith("#")) \
                .filter(F.col("line").contains("DTG")) \
                .limit(1)
            
            header_line = header_df.collect()[0]["line"] if header_df.count() > 0 else None
            
            if not header_line:
                print(f"Warning: Skipping file {file_path} - no valid header found")
                continue

            # Parse field positions from header
            # DTG starts at position 0 (same as #), other fields start where they appear
            field_positions = []
            for i, field_name in enumerate(field_names):
                if field_name == "DTG":
                    # DTG starts at the same position as the # character
                    field_positions.append(0)
                else:
                    pos = header_line.find(field_name)
                    field_positions.append(pos if pos >= 0 else len(header_line))
            field_positions.append(len(header_line))

            # Extract data lines
            lines_df = df.select(F.split(F.col("value"), "\n").alias("lines")) \
                .select(F.explode(F.col("lines")).alias("line")) \
                .filter(~F.col("line").startswith("#")) \
                .filter(F.length(F.trim(F.col("line"))) > 0)

            # Parse using fixed-width positions
            parsed_df = lines_df.select(
                *[F.trim(F.substring(F.col("line"), pos + 1, 
                                    field_positions[i+1] - pos)
                        ).alias(field_names[i]) 
                  for i, pos in enumerate(field_positions[:-1])]
            )

            # Convert to proper types and handle nulls
            structured_df = parsed_df.select(
                F.when(F.length(F.trim(F.col("DTG"))) == 0, F.lit(None)).otherwise(F.col("DTG")).alias("DTG"),
                F.when(F.length(F.trim(F.col("LOCATION"))) == 0, F.lit(None)).otherwise(F.col("LOCATION")).alias("LOCATION"),
                F.when(F.length(F.trim(F.col("NAME"))) == 0, F.lit(None)).otherwise(F.col("NAME")).alias("NAME"),
                F.when(F.length(F.trim(F.col("LATITUDE"))) == 0, F.lit(None)).otherwise(F.col("LATITUDE")).cast(DoubleType()).alias("LATITUDE"),
                F.when(F.length(F.trim(F.col("LONGITUDE"))) == 0, F.lit(None)).otherwise(F.col("LONGITUDE")).cast(DoubleType()).alias("LONGITUDE"),
                F.when(F.length(F.trim(F.col("ALTITUDE"))) == 0, F.lit(None)).otherwise(F.col("ALTITUDE")).cast(DoubleType()).alias("ALTITUDE"),
                F.when(F.length(F.trim(F.col("U_BOOL_10"))) == 0, F.lit(None)).otherwise(F.col("U_BOOL_10")).alias("U_BOOL_10"),
                F.when(F.length(F.trim(F.col("T_DRYB_10"))) == 0, F.lit(None)).otherwise(F.col("T_DRYB_10")).cast(DoubleType()).alias("T_DRYB_10"),
                F.when(F.length(F.trim(F.col("TN_10CM_PAST_6H_10"))) == 0, F.lit(None)).otherwise(F.col("TN_10CM_PAST_6H_10")).cast(DoubleType()).alias("TN_10CM_PAST_6H_10"),
                F.when(F.length(F.trim(F.col("T_DEWP_10"))) == 0, F.lit(None)).otherwise(F.col("T_DEWP_10")).cast(DoubleType()).alias("T_DEWP_10"),
                F.when(F.length(F.trim(F.col("T_DEWP_SEA_10"))) == 0, F.lit(None)).otherwise(F.col("T_DEWP_SEA_10")).cast(DoubleType()).alias("T_DEWP_SEA_10"),
                F.when(F.length(F.trim(F.col("T_DRYB_SEA_10"))) == 0, F.lit(None)).otherwise(F.col("T_DRYB_SEA_10")).cast(DoubleType()).alias("T_DRYB_SEA_10"),
                F.when(F.length(F.trim(F.col("TN_DRYB_10"))) == 0, F.lit(None)).otherwise(F.col("TN_DRYB_10")).cast(DoubleType()).alias("TN_DRYB_10"),
                F.when(F.length(F.trim(F.col("T_WETB_10"))) == 0, F.lit(None)).otherwise(F.col("T_WETB_10")).cast(DoubleType()).alias("T_WETB_10"),
                F.when(F.length(F.trim(F.col("TX_DRYB_10"))) == 0, F.lit(None)).otherwise(F.col("TX_DRYB_10")).cast(DoubleType()).alias("TX_DRYB_10"),
                F.when(F.length(F.trim(F.col("U_10"))) == 0, F.lit(None)).otherwise(F.col("U_10")).cast(DoubleType()).alias("U_10"),
                F.when(F.length(F.trim(F.col("U_SEA_10"))) == 0, F.lit(None)).otherwise(F.col("U_SEA_10")).cast(DoubleType()).alias("U_SEA_10")
            ).filter(F.col("DTG").isNotNull() & F.col("LOCATION").isNotNull())

            # Add DATE column - DTG contains timestamps like '2019-11-01 00:10:00'
            df_with_date = structured_df.withColumn("DATE", F.to_date(F.col("DTG"), "yyyy-MM-dd HH:mm:ss"))
            valid_dataframes.append(df_with_date)

        except Exception as e:
            print(f"Warning: Error processing file {file_path}: {str(e)}")
            continue

    if not valid_dataframes:
        raise RuntimeError("No valid files with headers found")

    # Union all valid dataframes
    result_df = valid_dataframes[0]
    for df in valid_dataframes[1:]:
        result_df = result_df.union(df)

    return result_df

def filter_de_bilt_data(df, include_min_temp=False):
    """
    Filter the data for the De Bilt weather station and aggregate by date.
    Since DTG is timestamp, we aggregate to get daily max and min temperatures.

    Args:
        df (DataFrame): The input DataFrame
        include_min_temp (bool): Whether to include the minimum temperature column (for coldwave calculation)

    Returns:
        DataFrame: A DataFrame containing daily aggregated data for De Bilt
    """
    # Filter for De Bilt weather station (LOCATION = 260_T_a)
    de_bilt_df = df.filter(df.LOCATION == "260_T_a")

    # Filter out rows with null temperature values first
    if include_min_temp:
        de_bilt_df = de_bilt_df.filter(
            de_bilt_df.TX_DRYB_10.isNotNull() & 
            de_bilt_df.TN_DRYB_10.isNotNull()
        )
    else:
        de_bilt_df = de_bilt_df.filter(de_bilt_df.TX_DRYB_10.isNotNull())

    # Filter for data from 2003 onwards
    de_bilt_df = de_bilt_df.filter(F.year(de_bilt_df.DATE) >= 2003)

    # Aggregate by date to get daily max and min temperatures
    if include_min_temp:
        # For coldwaves: get both daily max and min temperatures
        daily_df = de_bilt_df.groupBy("DATE").agg(
            F.max("TX_DRYB_10").alias("TX_DRYB_10"),  # Daily maximum temperature
            F.min("TN_DRYB_10").alias("TN_DRYB_10")   # Daily minimum temperature
        )
    else:
        # For heatwaves: only need daily max temperature
        daily_df = de_bilt_df.groupBy("DATE").agg(
            F.max("TX_DRYB_10").alias("TX_DRYB_10")   # Daily maximum temperature
        )

    return daily_df

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
    # First, ensure data is sorted by date
    df = df.orderBy("DATE")
    
    # Create a column indicating if the day is hot (≥ 25°C)
    df = df.withColumn("is_hot", F.when(df.TX_DRYB_10 >= 25.0, 1).otherwise(0))

    # Create a column indicating if the day is tropical (≥ 30°C)
    df = df.withColumn("is_tropical", F.when(df.TX_DRYB_10 >= 30.0, 1).otherwise(0))

    # Create a window specification for consecutive days analysis (no partitioning by year)
    window_spec = Window.orderBy("DATE")

    # Identify groups of consecutive hot days using a different approach
    # Create a group identifier that changes when is_hot changes
    df = df.withColumn("prev_is_hot", F.lag("is_hot", 1).over(window_spec))
    df = df.withColumn("group_change", 
                      F.when((F.col("prev_is_hot") != F.col("is_hot")) | 
                            F.col("prev_is_hot").isNull(), 1).otherwise(0))
    
    # Create cumulative sum to get unique group IDs
    df = df.withColumn("group_id", F.sum("group_change").over(window_spec))
    
    # Create a unique hot group ID by combining group_id with is_hot
    df = df.withColumn("hot_group_id", 
                      F.when(F.col("is_hot") == 1, 
                            F.concat(F.col("group_id"), F.lit("_hot"))).otherwise(None))

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
    # First, ensure data is sorted by date
    df = df.orderBy("DATE")
    
    # Create a column indicating if the day is cold (max temp < 0°C)
    df = df.withColumn("is_cold", F.when(df.TX_DRYB_10 < 0.0, 1).otherwise(0))

    # Create a column indicating if the day has high frost (min temp < -10°C)
    df = df.withColumn("is_high_frost", F.when(df.TN_DRYB_10 < -10.0, 1).otherwise(0))

    # Create a window specification for consecutive days analysis (no partitioning by year)
    window_spec = Window.orderBy("DATE")

    # Identify groups of consecutive cold days using the same approach as heatwaves
    df = df.withColumn("prev_is_cold", F.lag("is_cold", 1).over(window_spec))
    df = df.withColumn("group_change", 
                      F.when((F.col("prev_is_cold") != F.col("is_cold")) | 
                            F.col("prev_is_cold").isNull(), 1).otherwise(0))
    
    # Create cumulative sum to get unique group IDs
    df = df.withColumn("group_id", F.sum("group_change").over(window_spec))
    
    # Create a unique cold group ID by combining group_id with is_cold
    df = df.withColumn("cold_group_id", 
                      F.when(F.col("is_cold") == 1, 
                            F.concat(F.col("group_id"), F.lit("_cold"))).otherwise(None))

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

        # Download and extract data if it doesn't exist or is empty
        data_dir = "airflow/extracted_data"

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
