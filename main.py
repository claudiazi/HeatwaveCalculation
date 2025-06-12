"""
Weather Extremes Calculation Application

This application calculates heatwaves and coldwaves for The Netherlands based on KNMI definitions:
- Heatwave: ≥5 consecutive days with max temp ≥25°C, including ≥3 days ≥30°C
- Coldwave: ≥5 consecutive days with max temp <0°C, including ≥3 days with min temp <-10°C

Author: Weather Analysis Team
Version: 2.0
"""

import argparse
import logging
import os
import sys

from pyspark.sql import SparkSession

from heatwave_calculator.data_loader import WeatherDataLoader, DataProcessor
from heatwave_calculator.weather_analysis import HeatwaveAnalyzer, ColdwaveAnalyzer, ResultsExporter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class WeatherExtremeCalculator:
    """Main application class for weather extreme calculations."""

    def __init__(self):
        """Initialize the calculator with required components."""
        self.spark = None
        self.data_loader = None
        self.data_processor = DataProcessor()
        self.heatwave_analyzer = HeatwaveAnalyzer()
        self.coldwave_analyzer = ColdwaveAnalyzer()
        self.results_exporter = ResultsExporter()

        # Column mappings for CSV export
        self.heatwave_columns = {
            "start_date_formatted": "From date",
            "end_date_formatted": "To date (inc.)",
            "duration": "Duration (in days)",
            "tropical_days": "Number of tropical days",
            "max_temperature": "Max temperature"
        }

        self.coldwave_columns = {
            "start_date_formatted": "From date",
            "end_date_formatted": "To date (inc.)",
            "duration": "Duration (in days)",
            "high_frost_days": "Number of high frost days",
            "min_temperature": "Min temperature"
        }

    def initialize_spark(self) -> SparkSession:
        """Initialize and configure Spark session."""
        logger.info("Initializing Spark session")

        self.spark = SparkSession.builder \
            .appName("Weather Extremes Calculation") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.sql.adaptive.enabled", "false") \
            .master("local[*]") \
            .getOrCreate()

        # Set log level to reduce verbosity
        self.spark.sparkContext.setLogLevel("WARN")

        # Initialize data loader with Spark session
        self.data_loader = WeatherDataLoader(self.spark)

        return self.spark

    def process_heatwaves(self, df) -> None:
        """Process and export heatwave data."""
        logger.info("Processing heatwaves")

        # Filter for De Bilt data (only max temperature needed)
        de_bilt_df = self.data_processor.filter_de_bilt_data(df, include_min_temp=False)
        logger.info(f"Found {de_bilt_df.count()} De Bilt records from 2003 onwards")

        # Calculate heatwaves
        heatwaves_df = self.heatwave_analyzer.calculate_heatwaves(de_bilt_df)

        # Display results
        headers = ["From date", "To date (inc.)", "Duration (days)", "Tropical days", "Max temp (°C)"]
        self.results_exporter.display_results(
            heatwaves_df,
            "Heatwaves in The Netherlands (De Bilt) from 2003 onwards:",
            headers
        )

        # Export to CSV
        os.makedirs("results", exist_ok=True)
        self.results_exporter.export_to_csv(
            heatwaves_df,
            os.path.join("results", "heatwaves.csv"),
            self.heatwave_columns
        )

    def process_coldwaves(self, df) -> None:
        """Process and export coldwave data."""
        logger.info("Processing coldwaves")

        # Filter for De Bilt data (including min temperature)
        de_bilt_df = self.data_processor.filter_de_bilt_data(df, include_min_temp=True)
        logger.info(f"Found {de_bilt_df.count()} De Bilt records from 2003 onwards")

        # Calculate coldwaves
        coldwaves_df = self.coldwave_analyzer.calculate_coldwaves(de_bilt_df)

        # Display results
        headers = ["From date", "To date (inc.)", "Duration (days)", "High frost days", "Min temp (°C)"]
        self.results_exporter.display_results(
            coldwaves_df,
            "Coldwaves in The Netherlands (De Bilt) from 2003 onwards:",
            headers
        )

        # Export to CSV
        os.makedirs("results", exist_ok=True)
        self.results_exporter.export_to_csv(
            coldwaves_df,
            os.path.join("results", "coldwaves.csv"),
            self.coldwave_columns
        )

    def run(self, mode: str, data_dir: str = "extracted_data", check_data_quality: bool = False) -> None:
        """Run the weather extremes calculation.

        Args:
            mode: Calculation mode ('heatwaves', 'coldwaves', or 'both')
            data_dir: Directory containing the weather data
            check_data_quality: Whether to generate a data quality report (default: False)
        """
        try:
            logger.info(f"Starting Weather Extremes Calculation (Mode: {mode})")

            # Initialize Spark
            self.initialize_spark()

            # Load data
            logger.info("Loading weather data")
            df = self.data_loader.load_data(data_dir)
            logger.info(f"Loaded {df.count()} records")

            # Generate and print data quality report if requested
            if check_data_quality:
                logger.info("Generating data quality report")
                self.data_processor.generate_data_quality_report(df)

            # Process based on mode
            if mode in ['heatwaves', 'both']:
                self.process_heatwaves(df)

            if mode in ['coldwaves', 'both']:
                self.process_coldwaves(df)

            logger.info("Weather extremes calculation completed successfully")

        except Exception as e:
            logger.error(f"Error during calculation: {e}")
            raise
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("Spark session stopped")


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Calculate heatwaves and coldwaves for The Netherlands.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --mode heatwaves                     # Calculate only heatwaves
  %(prog)s --mode coldwaves                     # Calculate only coldwaves
  %(prog)s --mode both                          # Calculate both (default)
  %(prog)s --mode both --check-data-quality     # Calculate both and generate data quality report
        """
    )

    parser.add_argument(
        '--mode',
        choices=['heatwaves', 'coldwaves', 'both'],
        default='both',
        help='Calculation mode (default: both)'
    )

    parser.add_argument(
        '--data-dir',
        default='extracted_data',
        help='Directory containing weather data (default: extracted_data)'
    )

    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Set logging level (default: INFO)'
    )

    parser.add_argument(
        '--check-data-quality',
        action='store_true',
        default=False,
        help='Generate a data quality report (default: False)'
    )

    return parser.parse_args()


def main():
    """Main function to run the weather extremes calculation application."""
    try:
        # Parse arguments
        args = parse_arguments()

        # Set logging level
        logging.getLogger().setLevel(getattr(logging, args.log_level))

        # Initialize and run calculator
        calculator = WeatherExtremeCalculator()
        calculator.run(args.mode, args.data_dir, args.check_data_quality)

    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Application failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
