"""
Configuration module for Airflow DAG

Contains all configuration settings and constants for the KNMI weather data processing DAG.
"""

import os
from datetime import datetime, timedelta
from typing import Dict, Any


class AirflowConfig:
    """Configuration settings for the Airflow DAG."""
    
    # DAG configuration
    DAG_ID = 'knmi_weather_processing'
    DESCRIPTION = 'Process local KNMI weather data and calculate heatwaves and coldwaves'
    SCHEDULE_INTERVAL = timedelta(days=1)
    START_DATE = datetime(2024, 1, 1)
    CATCHUP = False
    
    # Default arguments for tasks
    DEFAULT_ARGS = {
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'start_date': START_DATE,
    }
    
    # Directory configuration
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    EXTRACTED_DATA_DIR = os.path.join(PROJECT_ROOT, 'extracted_data')
    
    # Data source URLs
    BLOB_DATA_URL = "https://gddassesmentdata.blob.core.windows.net/knmi-data/data.tgz?sp=r&st=2024-01-03T14:42:11Z&se=2025-01-03T22:42:11Z&spr=https&sv=2022-11-02&sr=c&sig=jcOeksvhjJGDTCM%2B2CzrjR3efJI7jq5a3SnT8aiQBc8%3D"
    
    # KNMI API configuration
    KNMI_API_CONFIG = {
        'api_key': "eyJvcmciOiI1ZTU1NGUxOTI3NGE5NjAwMDEyYTNlYjEiLCJpZCI6ImE1OGI5NGZmMDY5NDRhZDNhZjFkMDBmNDBmNTQyNjBkIiwiaCI6Im11cm11cjEyOCJ9",
        'dataset_name': "etmaalgegevensKNMIstations",
        'dataset_version': "1",
        'base_url': "https://api.dataplatform.knmi.nl/open-data/v1",
        'overwrite': False,
        'max_keys': 500,
        'size_for_threading': 10_000_000,  # 10 MB
        'max_threads': 10
    }
    
    # Task configuration
    BASH_COMMANDS = {
        'heatwaves': f'cd {PROJECT_ROOT} && python main.py --mode heatwaves',
        'coldwaves': f'cd {PROJECT_ROOT} && python main.py --mode coldwaves'
    }


class EnvironmentConfig:
    """Environment-specific configuration."""
    
    @staticmethod
    def setup_environment() -> Dict[str, str]:
        """Set up environment variables for tasks."""
        return {
            'no_proxy': '*'  # Workaround for macOS network proxy issues
        }
    
    @staticmethod
    def get_project_params() -> Dict[str, Any]:
        """Get parameters to pass to tasks."""
        return {
            'project_dir': AirflowConfig.PROJECT_ROOT
        }