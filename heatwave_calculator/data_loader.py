"""
Data Loading Module for KNMI Weather Data

Handles downloading, extracting, and loading weather data from various sources.
"""

import glob
import logging
import os
import tarfile
import tempfile
import urllib.request
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

logger = logging.getLogger(__name__)


class DataExtractor:
    """Handles data extraction from various sources."""

    @staticmethod
    def download_and_extract(url: str, extract_to: str = ".") -> str:
        """Download and extract data from URL.

        Args:
            url: URL to download the data from
            extract_to: Directory to extract the data to

        Returns:
            Path to the extracted data directory

        Raises:
            Exception: If download or extraction fails
        """
        logger.info(f"Downloading data from {url}")

        temp_file_path = None
        try:
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_file_path = temp_file.name
                urllib.request.urlretrieve(url, temp_file_path)

            logger.info(f"Extracting data to {extract_to}")
            with tarfile.open(temp_file_path) as tar:
                tar.extractall(path=extract_to)

        except Exception as e:
            logger.error(f"Failed to download/extract data: {e}")
            raise
        finally:
            if temp_file_path and os.path.exists(temp_file_path):
                os.unlink(temp_file_path)

        return extract_to

    @staticmethod
    def extract_local_file(file_path: str, extract_to: str = ".") -> str:
        """Extract data from a local file.

        Args:
            file_path: Path to the local file
            extract_to: Directory to extract the data to

        Returns:
            Path to the extracted data directory

        Raises:
            FileNotFoundError: If file doesn't exist
            Exception: If extraction fails
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        logger.info(f"Extracting local file {file_path} to {extract_to}")

        try:
            with tarfile.open(file_path) as tar:
                tar.extractall(path=extract_to)
        except Exception as e:
            logger.error(f"Failed to extract {file_path}: {e}")
            raise

        return extract_to


class WeatherDataLoader:
    """Loads and processes KNMI weather data."""

    # Field names in expected order
    FIELD_NAMES = [
        "DTG", "LOCATION", "NAME", "LATITUDE", "LONGITUDE", "ALTITUDE",
        "U_BOOL_10", "T_DRYB_10", "TN_10CM_PAST_6H_10", "T_DEWP_10",
        "T_DEWP_SEA_10", "T_DRYB_SEA_10", "TN_DRYB_10", "T_WETB_10",
        "TX_DRYB_10", "U_10", "U_SEA_10"
    ]

    # Data source configuration
    DATA_TGZ_URL = "https://gddassesmentdata.blob.core.windows.net/knmi-data/data.tgz?sp=r&st=2024-01-03T14:42:11Z&se=2025-01-03T22:42:11Z&spr=https&sv=2022-11-02&sr=c&sig=jcOeksvhjJGDTCM%2B2CzrjR3efJI7jq5a3SnT8aiQBc8%3D"

    def __init__(self, spark: SparkSession):
        """Initialize the data loader.

        Args:
            spark: The Spark session to use for data processing
        """
        self.spark = spark
        self.extractor = DataExtractor()

    def load_data(self, data_dir: str) -> DataFrame:
        """Load KNMI weather data from various sources.

        Args:
            data_dir: Directory containing or to contain the data files

        Returns:
            DataFrame containing the weather data

        Raises:
            FileNotFoundError: If no data source is available
        """
        # Check if extracted_data path already has files
        all_files = glob.glob(os.path.join(data_dir, "**/*"), recursive=True)
        if all_files:
            logger.info(f"Found {len(all_files)} existing files in {data_dir}")
            return self._load_text_data(data_dir)

        # Try to extract from local data.tgz
        if os.path.exists("data.tgz"):
            logger.info("Found data.tgz file in root path")
            try:
                self.extractor.extract_local_file("data.tgz", data_dir)
                return self._load_text_data(data_dir)
            except Exception as e:
                logger.warning(f"Failed to extract data.tgz from root path: {e}")

        # Download and extract data
        logger.info("Downloading data from remote source")
        try:
            self.extractor.download_and_extract(self.DATA_TGZ_URL, data_dir)
            return self._load_text_data(data_dir)
        except Exception as e:
            logger.error(f"Failed to download data: {e}")
            raise FileNotFoundError("Failed to load data: no data source available")

    def _load_text_data(self, data_dir: str) -> DataFrame:
        """Load KNMI weather data from space-delimited text files.

        Args:
            data_dir: Directory containing the data files

        Returns:
            Combined DataFrame containing weather data with DATE column

        Raises:
            FileNotFoundError: If no files found in directory
            RuntimeError: If no valid files with headers found
        """
        all_files = glob.glob(os.path.join(data_dir, "**/*"), recursive=True)
        data_files = [f for f in all_files if os.path.isfile(f)]

        if not data_files:
            raise FileNotFoundError(f"No files found in {data_dir}")

        logger.info(f"Processing {len(data_files)} files")

        valid_dataframes = []

        for file_path in data_files:
            try:
                df = self._process_file(file_path)
                if df is not None:
                    valid_dataframes.append(df)
            except Exception as e:
                logger.warning(f"Error processing file {file_path}: {e}")
                continue

        if not valid_dataframes:
            raise RuntimeError("No valid files with headers found")

        # Union all valid dataframes
        result_df = valid_dataframes[0]
        for df in valid_dataframes[1:]:
            result_df = result_df.union(df)

        return result_df

    def _process_file(self, file_path: str) -> Optional[DataFrame]:
        """Process a single weather data file.

        Args:
            file_path: Path to the file to process

        Returns:
            DataFrame with processed data or None if file is invalid
        """
        df = self.spark.read.option("multiline", "true").option("wholetext", "true").text(file_path)

        # Extract header line
        header_df = df.select(F.split(F.col("value"), "\n").alias("lines")) \
            .select(F.explode(F.col("lines")).alias("line")) \
            .filter(F.col("line").startswith("#")) \
            .filter(F.col("line").contains("DTG")) \
            .limit(1)

        if header_df.count() == 0:
            logger.warning(f"Skipping file {file_path} - no valid header found")
            return None

        header_line = header_df.collect()[0]["line"]

        # Parse field positions from header
        field_positions = self._parse_header_positions(header_line)

        # Extract and parse data lines
        lines_df = df.select(F.split(F.col("value"), "\n").alias("lines")) \
            .select(F.explode(F.col("lines")).alias("line")) \
            .filter(~F.col("line").startswith("#")) \
            .filter(F.length(F.trim(F.col("line"))) > 0)

        # Parse using fixed-width positions
        parsed_df = self._parse_data_lines(lines_df, field_positions)

        # Convert to proper types and add DATE column
        structured_df = self._structure_data(parsed_df)

        return structured_df

    def _parse_header_positions(self, header_line: str) -> List[int]:
        """Parse field positions from header line.

        Args:
            header_line: The header line containing field names

        Returns:
            List of field positions
        """
        field_positions = []
        for i, field_name in enumerate(self.FIELD_NAMES):
            if field_name == "DTG":
                field_positions.append(0)
            else:
                pos = header_line.find(field_name)
                field_positions.append(pos if pos >= 0 else len(header_line))
        field_positions.append(len(header_line))
        return field_positions

    def _parse_data_lines(self, lines_df: DataFrame, field_positions: List[int]) -> DataFrame:
        """Parse data lines using fixed-width positions.

        Args:
            lines_df: DataFrame containing data lines
            field_positions: List of field positions

        Returns:
            DataFrame with parsed fields
        """
        return lines_df.select(
            *[F.trim(F.substring(F.col("line"), pos + 1,
                                 field_positions[i + 1] - pos)
                     ).alias(self.FIELD_NAMES[i])
              for i, pos in enumerate(field_positions[:-1])]
        )

    def _structure_data(self, parsed_df: DataFrame) -> DataFrame:
        """Convert parsed data to proper types and add derived columns.

        Args:
            parsed_df: DataFrame with parsed string fields

        Returns:
            DataFrame with proper types and DATE column
        """
        # Convert to proper types and handle nulls
        structured_df = parsed_df.select(
            F.when(F.length(F.trim(F.col("DTG"))) == 0, F.lit(None)).otherwise(F.col("DTG")).alias("DTG"),
            F.when(F.length(F.trim(F.col("LOCATION"))) == 0, F.lit(None)).otherwise(F.col("LOCATION")).alias(
                "LOCATION"),
            F.when(F.length(F.trim(F.col("NAME"))) == 0, F.lit(None)).otherwise(F.col("NAME")).alias("NAME"),
            F.when(F.length(F.trim(F.col("LATITUDE"))) == 0, F.lit(None)).otherwise(F.col("LATITUDE")).cast(
                DoubleType()).alias("LATITUDE"),
            F.when(F.length(F.trim(F.col("LONGITUDE"))) == 0, F.lit(None)).otherwise(F.col("LONGITUDE")).cast(
                DoubleType()).alias("LONGITUDE"),
            F.when(F.length(F.trim(F.col("ALTITUDE"))) == 0, F.lit(None)).otherwise(F.col("ALTITUDE")).cast(
                DoubleType()).alias("ALTITUDE"),
            F.when(F.length(F.trim(F.col("U_BOOL_10"))) == 0, F.lit(None)).otherwise(F.col("U_BOOL_10")).alias(
                "U_BOOL_10"),
            F.when(F.length(F.trim(F.col("T_DRYB_10"))) == 0, F.lit(None)).otherwise(F.col("T_DRYB_10")).cast(
                DoubleType()).alias("T_DRYB_10"),
            F.when(F.length(F.trim(F.col("TN_10CM_PAST_6H_10"))) == 0, F.lit(None)).otherwise(
                F.col("TN_10CM_PAST_6H_10")).cast(DoubleType()).alias("TN_10CM_PAST_6H_10"),
            F.when(F.length(F.trim(F.col("T_DEWP_10"))) == 0, F.lit(None)).otherwise(F.col("T_DEWP_10")).cast(
                DoubleType()).alias("T_DEWP_10"),
            F.when(F.length(F.trim(F.col("T_DEWP_SEA_10"))) == 0, F.lit(None)).otherwise(F.col("T_DEWP_SEA_10")).cast(
                DoubleType()).alias("T_DEWP_SEA_10"),
            F.when(F.length(F.trim(F.col("T_DRYB_SEA_10"))) == 0, F.lit(None)).otherwise(F.col("T_DRYB_SEA_10")).cast(
                DoubleType()).alias("T_DRYB_SEA_10"),
            F.when(F.length(F.trim(F.col("TN_DRYB_10"))) == 0, F.lit(None)).otherwise(F.col("TN_DRYB_10")).cast(
                DoubleType()).alias("TN_DRYB_10"),
            F.when(F.length(F.trim(F.col("T_WETB_10"))) == 0, F.lit(None)).otherwise(F.col("T_WETB_10")).cast(
                DoubleType()).alias("T_WETB_10"),
            F.when(F.length(F.trim(F.col("TX_DRYB_10"))) == 0, F.lit(None)).otherwise(F.col("TX_DRYB_10")).cast(
                DoubleType()).alias("TX_DRYB_10"),
            F.when(F.length(F.trim(F.col("U_10"))) == 0, F.lit(None)).otherwise(F.col("U_10")).cast(DoubleType()).alias(
                "U_10"),
            F.when(F.length(F.trim(F.col("U_SEA_10"))) == 0, F.lit(None)).otherwise(F.col("U_SEA_10")).cast(
                DoubleType()).alias("U_SEA_10")
        ).filter(F.col("DTG").isNotNull() & F.col("LOCATION").isNotNull())

        # Add DATE column - DTG contains timestamps like '2019-11-01 00:10:00'
        return structured_df.withColumn("DATE", F.to_date(F.col("DTG"), "yyyy-MM-dd HH:mm:ss"))


class DataProcessor:
    """Processes and filters weather data for analysis."""

    DE_BILT_STATION_CODE = "260_T_a"
    START_YEAR = 2003

    @staticmethod
    def filter_de_bilt_data(df: DataFrame, include_min_temp: bool = False) -> DataFrame:
        """Filter data for De Bilt weather station and aggregate by date.

        Args:
            df: The input DataFrame
            include_min_temp: Whether to include minimum temperature (for coldwave calculation)

        Returns:
            DataFrame containing daily aggregated data for De Bilt
        """
        # Filter for De Bilt weather station
        de_bilt_df = df.filter(df.LOCATION == DataProcessor.DE_BILT_STATION_CODE)

        # Filter out rows with null temperature values
        if include_min_temp:
            de_bilt_df = de_bilt_df.filter(
                de_bilt_df.TX_DRYB_10.isNotNull() &
                de_bilt_df.TN_DRYB_10.isNotNull()
            )
        else:
            de_bilt_df = de_bilt_df.filter(de_bilt_df.TX_DRYB_10.isNotNull())

        # Filter for data from specified start year onwards
        de_bilt_df = de_bilt_df.filter(F.year(de_bilt_df.DATE) >= DataProcessor.START_YEAR)

        # Aggregate by date to get daily max and min temperatures
        if include_min_temp:
            daily_df = de_bilt_df.groupBy("DATE").agg(
                F.max("TX_DRYB_10").alias("TX_DRYB_10"),  # Daily maximum temperature
                F.min("TN_DRYB_10").alias("TN_DRYB_10")  # Daily minimum temperature
            )
        else:
            daily_df = de_bilt_df.groupBy("DATE").agg(
                F.max("TX_DRYB_10").alias("TX_DRYB_10")  # Daily maximum temperature
            )

        return daily_df