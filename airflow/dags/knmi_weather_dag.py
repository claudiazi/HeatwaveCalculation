"""
Airflow DAG for KNMI Weather Data Processing (Refactored)

This DAG processes weather data from local extracted_data folder and KNMI API to calculate
heatwaves and coldwaves. Refactored for better modularity and maintainability.
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

# Add the project root and src to the path
# Handle both local and Docker container paths
if os.path.exists('/opt/airflow/project'):
    # Running in Airflow Docker container
    project_root = '/opt/airflow/project'
else:
    # Running locally
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

src_path = os.path.join(project_root, 'src')
sys.path.extend([project_root, src_path])

from heatwave_calculator.airflow_workflows.airflow_config import AirflowConfig, EnvironmentConfig
from heatwave_calculator.airflow_workflows.airflow_utils import (
    download_knmi_data,
    get_knmi_etmaal_data,
    check_data_availability
)

# Set up environment configuration
env_config = EnvironmentConfig.setup_environment()
project_params = EnvironmentConfig.get_project_params()

# Create DAG instance
dag = DAG(
    dag_id=AirflowConfig.DAG_ID,
    default_args=AirflowConfig.DEFAULT_ARGS,
    description=AirflowConfig.DESCRIPTION,
    schedule=AirflowConfig.SCHEDULE_INTERVAL,
    catchup=AirflowConfig.CATCHUP,
)

# Data availability check task
check_data_availability_task = PythonOperator(
    task_id='check_data_availability',
    python_callable=check_data_availability,
    env=env_config,
    dag=dag,
)

# Data download tasks (concurrent)
download_knmi_data_task = PythonOperator(
    task_id='download_knmi_data',
    python_callable=download_knmi_data,
    env=env_config,
    dag=dag,
)

download_knmi_etmaal_data_task = PythonOperator(
    task_id='download_knmi_etmaal_data',
    python_callable=get_knmi_etmaal_data,
    env=env_config,
    dag=dag,
)

# Weather analysis tasks (concurrent)
calculate_heatwaves_task = BashOperator(
    task_id='calculate_heatwaves',
    bash_command='cd /opt/airflow/project && python main.py --mode heatwaves',
    env=env_config,
    dag=dag,
)

calculate_coldwaves_task = BashOperator(
    task_id='calculate_coldwaves',
    bash_command='cd /opt/airflow/project && python main.py --mode coldwaves',
    env=env_config,
    dag=dag,
)

# Combined calculation task (alternative to separate tasks)
calculate_both_task = BashOperator(
    task_id='calculate_both',
    bash_command='cd /opt/airflow/project && python main.py --mode both',
    env=env_config,
    dag=dag,
)

# Task dependencies
# Primary workflow: Check data -> Download data (concurrent) -> Calculate (concurrent)
check_data_availability_task >> download_knmi_etmaal_data_task >> [
    calculate_heatwaves_task,
    calculate_coldwaves_task
]
