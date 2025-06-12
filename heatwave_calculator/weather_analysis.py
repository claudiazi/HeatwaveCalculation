"""
Weather Analysis Module

Contains classes and functions for calculating heatwaves and coldwaves
based on KNMI definitions.
"""

import logging
from typing import NamedTuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


class WeatherCriteria(NamedTuple):
    """Criteria for weather extreme detection."""
    min_duration: int
    temp_threshold: float
    extreme_days_required: int
    extreme_temp_threshold: float


class WeatherAnalyzer:
    """Base class for weather extreme analysis."""

    def __init__(self, criteria: WeatherCriteria):
        """Initialize analyzer with criteria.

        Args:
            criteria: Weather criteria for detection
        """
        self.criteria = criteria

    def _identify_consecutive_periods(self, df: DataFrame, condition_col: str, extreme_col: str,
                                      period_prefix: str) -> DataFrame:
        """Identify consecutive periods meeting weather conditions.

        Args:
            df: Input DataFrame with weather data
            condition_col: Column name for basic condition (e.g., 'is_hot', 'is_cold')
            extreme_col: Column name for extreme condition (e.g., 'is_tropical', 'is_high_frost')
            period_prefix: Prefix for group ID column

        Returns:
            DataFrame with period analysis
        """
        # Filter for days meeting condition first (performance optimization)
        condition_df = df.filter(F.col(condition_col) == 1)

        # Ensure data is sorted by date and persist for performance
        condition_df = condition_df.orderBy("DATE")
        condition_df = condition_df.persist()

        # Use row number trick for grouping (performance optimization)
        window = Window.orderBy("DATE")
        condition_df = condition_df.withColumn("row_num", F.row_number().over(window))
        condition_df = condition_df.withColumn("date_diff", F.datediff("DATE", F.lit("1970-01-01")))

        # Grouping logic: consecutive if date_diff - row_num is constant
        condition_df = condition_df.withColumn("group_id", F.expr("date_diff - row_num"))

        # Group by group_id to analyze each potential period
        grouped_df = condition_df.groupBy("group_id").agg(
            F.min("DATE").alias("start_date"),
            F.max("DATE").alias("end_date"),
            F.count("DATE").alias("duration"),
            F.sum(extreme_col).alias("extreme_days"),
            F.max("TX_DRYB_10").alias("max_temperature") if "hot" in period_prefix
            else F.min("TN_DRYB_10").alias("min_temperature")
        )

        return grouped_df

    def _format_results(self, periods_df: DataFrame, temp_col: str) -> DataFrame:
        """Format analysis results for output.

        Args:
            periods_df: DataFrame with analyzed periods
            temp_col: Temperature column name for output

        Returns:
            Formatted DataFrame with date strings and proper ordering
        """
        # Format dates for output
        formatted_df = periods_df.withColumn(
            "start_date_formatted",
            F.date_format(periods_df.start_date, "dd MMM yyyy")
        ).withColumn(
            "end_date_formatted",
            F.date_format(periods_df.end_date, "dd MMM yyyy")
        )

        # Select and order columns for output
        result_cols = [
            "start_date_formatted",
            "end_date_formatted",
            "duration",
            "extreme_days",
            temp_col
        ]

        return formatted_df.select(*result_cols).orderBy("start_date")


class HeatwaveAnalyzer(WeatherAnalyzer):
    """Analyzes heatwaves based on KNMI definition."""

    # KNMI heatwave criteria
    HEATWAVE_CRITERIA = WeatherCriteria(
        min_duration=5,  # At least 5 consecutive days
        temp_threshold=25.0,  # Max temperature ≥ 25°C
        extreme_days_required=3,  # At least 3 days
        extreme_temp_threshold=30.0  # Max temperature ≥ 30°C
    )

    def __init__(self):
        """Initialize heatwave analyzer with KNMI criteria."""
        super().__init__(self.HEATWAVE_CRITERIA)
        logger.info("Initialized heatwave analyzer with KNMI criteria")

    def calculate_heatwaves(self, df: DataFrame) -> DataFrame:
        """Calculate heatwaves based on KNMI definition.

        A heatwave is defined as:
        - A period of at least 5 consecutive days with max temperature ≥ 25°C
        - Within those 5+ days, at least 3 days with max temperature ≥ 30°C

        Args:
            df: Input DataFrame with DATE and TX_DRYB_10 columns

        Returns:
            DataFrame containing heatwave periods
        """
        logger.info("Starting heatwave calculation")

        # Create condition columns
        df = df.withColumn("is_hot",
                           F.when(df.TX_DRYB_10 >= self.criteria.temp_threshold, 1).otherwise(0))
        df = df.withColumn("is_tropical",
                           F.when(df.TX_DRYB_10 >= self.criteria.extreme_temp_threshold, 1).otherwise(0))


        # Identify consecutive hot periods
        grouped_df = self._identify_consecutive_periods(df, "is_hot", "is_tropical", "hot")

        # Filter for periods meeting heatwave criteria
        heatwaves_df = grouped_df.filter(
            (grouped_df.duration >= self.criteria.min_duration) &
            (grouped_df.extreme_days >= self.criteria.extreme_days_required)
        )

        # Format results
        result_df = self._format_results(heatwaves_df, "max_temperature")

        # Rename extreme_days column for heatwaves
        result_df = result_df.withColumnRenamed("extreme_days", "tropical_days")

        logger.info(f"Found {result_df.count()} heatwave periods")
        return result_df


class ColdwaveAnalyzer(WeatherAnalyzer):
    """Analyzes coldwaves based on definition."""

    # Coldwave criteria
    COLDWAVE_CRITERIA = WeatherCriteria(
        min_duration=5,  # At least 5 consecutive days
        temp_threshold=0.0,  # Max temperature < 0°C
        extreme_days_required=3,  # At least 3 days
        extreme_temp_threshold=-10.0  # Min temperature < -10°C
    )

    def __init__(self):
        """Initialize coldwave analyzer with criteria."""
        super().__init__(self.COLDWAVE_CRITERIA)
        logger.info("Initialized coldwave analyzer with criteria")

    def calculate_coldwaves(self, df: DataFrame) -> DataFrame:
        """Calculate coldwaves based on definition.

        A coldwave is defined as:
        - A period of at least 5 consecutive days with max temperature < 0°C
        - Within those 5+ days, at least 3 days with min temperature < -10°C

        Args:
            df: Input DataFrame with DATE, TX_DRYB_10 (max temp), and TN_DRYB_10 (min temp) columns

        Returns:
            DataFrame containing coldwave periods
        """
        logger.info("Starting coldwave calculation")

        # Create condition columns
        df = df.withColumn("is_cold",
                           F.when(df.TX_DRYB_10 < self.criteria.temp_threshold, 1).otherwise(0))
        df = df.withColumn("is_high_frost",
                           F.when(df.TN_DRYB_10 < self.criteria.extreme_temp_threshold, 1).otherwise(0))

        # Identify consecutive cold periods
        grouped_df = self._identify_consecutive_periods(df, "is_cold", "is_high_frost", "cold")

        # Filter for periods meeting coldwave criteria
        coldwaves_df = grouped_df.filter(
            (grouped_df.duration >= self.criteria.min_duration) &
            (grouped_df.extreme_days >= self.criteria.extreme_days_required)
        )

        # Format results
        result_df = self._format_results(coldwaves_df, "min_temperature")

        # Rename extreme_days column for coldwaves
        result_df = result_df.withColumnRenamed("extreme_days", "high_frost_days")

        logger.info(f"Found {result_df.count()} coldwave periods")
        return result_df


class ResultsExporter:
    """Handles exporting analysis results to various formats."""

    @staticmethod
    def export_to_csv(df: DataFrame, filename: str, column_mapping: dict) -> None:
        """Export DataFrame to CSV with proper column names.

        Args:
            df: DataFrame to export
            filename: Output filename
            column_mapping: Mapping of current column names to output column names
        """
        logger.info(f"Exporting results to {filename}")

        # Convert to Pandas DataFrame for easier CSV export
        pandas_df = df.toPandas()

        # Rename columns according to mapping
        pandas_df.columns = [column_mapping.get(col, col) for col in pandas_df.columns]

        # Save to CSV
        pandas_df.to_csv(filename, index=False)
        logger.info(f"Results saved to {filename}")

    @staticmethod
    def display_results(df: DataFrame, title: str, headers: list) -> None:
        """Display results in a formatted table.

        Args:
            df: DataFrame to display
            title: Table title
            headers: List of column headers for display
        """
        print(f"\n{title}")
        print("=" * 80)

        # Create header row
        header_row = " | ".join([f"{header:<12}" if i < 2 else f"{header:<15}"
                                 for i, header in enumerate(headers)])
        print(header_row)
        print("-" * 80)

        # Display data rows
        results = df.collect()
        if not results:
            print("No results found.")
        else:
            for row in results:
                # Format row based on column types
                formatted_values = []
                for i, value in enumerate(row):
                    if i < 2:  # Date columns
                        formatted_values.append(f"{value:<12}")
                    elif isinstance(value, float):  # Temperature columns
                        formatted_values.append(f"{value:<12.1f}")
                    else:  # Integer columns
                        formatted_values.append(f"{value:<15}")

                print(" | ".join(formatted_values))
