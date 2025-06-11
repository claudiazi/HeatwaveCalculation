"""
Airflow DAG for KNMI Weather Data Processing

This DAG processes weather data from local extracted_data folder to calculate heatwaves and coldwaves.
The API functions are kept for reference but tasks only use local data.
"""

import os
import sys
import tarfile
import tempfile
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import requests
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

from airflow import DAG

# Add the parent directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

# Create the DAG
dag = DAG(
    'knmi_weather_processing',
    default_args=default_args,
    description='Process local KNMI weather data and calculate heatwaves and coldwaves',
    schedule=timedelta(days=1),  # Run daily
    catchup=False,
)

# Define directories
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
extracted_data_dir = os.path.join(project_root, 'airflow', 'extracted_data')

def download_knmi_data(**kwargs):
    """
    Download the latest weather data from the KNMI API.

    This function uses the KNMI API to download the latest weather data.
    It saves the data to the data directory.
    """
    # Create the data directory if it doesn't exist
    os.makedirs(extracted_data_dir, exist_ok=True)

    # Set no_proxy environment variable to avoid segmentation fault on macOS
    # This is a workaround for a known issue with macOS and network proxy configuration
    os.environ['no_proxy'] = '*'

    # Get the current date
    current_date = datetime.now()

    # Define the KNMI API URL
    # Note: This is a placeholder URL. The actual KNMI API URL should be used.
    knmi_api_url = "https://gddassesmentdata.blob.core.windows.net/knmi-data/data.tgz?sp=r&st=2024-01-03T14:42:11Z&se=2025-01-03T22:42:11Z&spr=https&sv=2022-11-02&sr=c&sig=jcOeksvhjJGDTCM%2B2CzrjR3efJI7jq5a3SnT8aiQBc8%3D"

    # Download the data
    print(f"Downloading data from {knmi_api_url}...")

    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        urllib.request.urlretrieve(knmi_api_url, temp_file.name)

        print(f"Extracting data to {extracted_data_dir}...")
        with tarfile.open(temp_file.name) as tar:
            tar.extractall(path=extracted_data_dir)

    os.unlink(temp_file.name)

    # Return the path to the data directory
    return extracted_data_dir

def download_dataset_file(
    session: requests.Session,
    base_url: str,
    dataset_name: str,
    dataset_version: str,
    filename: str,
    directory: str,
    overwrite: bool,
) -> tuple[bool, str]:
    """
    Download a single file from the KNMI dataset.

    Args:
        session: The requests session with authentication
        base_url: The base URL for the API
        dataset_name: The name of the dataset
        dataset_version: The version of the dataset
        filename: The name of the file to download
        directory: The directory to save the file to
        overwrite: Whether to overwrite existing files

    Returns:
        A tuple of (success, filename)
    """
    # if a file from this dataset already exists, skip downloading it.
    file_path = Path(directory, filename).resolve()
    if not overwrite and file_path.exists():
        print(f"Dataset file '{filename}' was already downloaded.")
        return True, filename

    endpoint = f"{base_url}/datasets/{dataset_name}/versions/{dataset_version}/files/{filename}/url"
    get_file_response = session.get(endpoint)

    # retrieve download URL for dataset file
    if get_file_response.status_code != 200:
        print(f"Unable to get file: {filename}")
        print(get_file_response.content)
        return False, filename

    # use download URL to GET dataset file
    download_url = get_file_response.json().get("temporaryDownloadUrl")
    return download_file_from_temporary_download_url(download_url, directory, filename)


def download_file_from_temporary_download_url(download_url, directory, filename):
    """
    Download a file from a temporary download URL.

    Args:
        download_url: The URL to download the file from
        directory: The directory to save the file to
        filename: The name of the file to save

    Returns:
        A tuple of (success, filename)
    """
    try:
        with requests.get(download_url, stream=True) as r:
            r.raise_for_status()
            with open(f"{directory}/{filename}", "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
    except Exception as e:
        print(f"Unable to download file using download URL: {str(e)}")
        return False, filename

    print(f"Downloaded dataset file '{filename}'")

    # If the file is a tar file, extract it
    file_path = os.path.join(directory, filename)
    if filename.endswith('.tar') or filename.endswith('.tgz') or filename.endswith('.tar.gz'):
        print(f"Extracting {filename}...")
        try:
            with tarfile.open(file_path) as tar:
                tar.extractall(path=directory)
            print(f"Extracted {filename} to {directory}")
        except Exception as e:
            print(f"Error extracting {filename}: {str(e)}")
            return False, filename

    return True, filename


def list_dataset_files(
    session: requests.Session,
    base_url: str,
    dataset_name: str,
    dataset_version: str,
    params: dict[str, str],
) -> tuple[list[str], dict[str, Any]]:
    """
    List files in a dataset with pagination support.

    Args:
        session: The requests session with authentication
        base_url: The base URL for the API
        dataset_name: The name of the dataset
        dataset_version: The version of the dataset
        params: Query parameters for the API request

    Returns:
        A tuple of (filenames, response_json)
    """
    print(f"Retrieve dataset files with query params: {params}")

    list_files_endpoint = f"{base_url}/datasets/{dataset_name}/versions/{dataset_version}/files"
    list_files_response = session.get(list_files_endpoint, params=params)

    if list_files_response.status_code != 200:
        raise Exception("Unable to list dataset files")

    try:
        list_files_response_json = list_files_response.json()
        dataset_files = list_files_response_json.get("files")
        dataset_filenames = list(map(lambda x: x.get("filename"), dataset_files))
        return dataset_filenames, list_files_response_json
    except Exception as e:
        print(f"Error parsing API response: {str(e)}")
        raise Exception(e)


def get_max_worker_count(filesizes):
    """
    Determine the optimal number of worker threads based on file sizes.

    Args:
        filesizes: A list of file sizes

    Returns:
        The number of worker threads to use
    """
    size_for_threading = 10_000_000  # 10 MB
    average = sum(filesizes) / len(filesizes)
    # to prevent downloading multiple half files in case of a network failure with big files
    if average > size_for_threading:
        threads = 1
    else:
        threads = 10
    return threads


def get_knmi_etmaal_data(**kwargs):
    """
    Download data from the KNMI etmaalgegevensKNMIstations dataset.

    This function uses the KNMI Data Platform API to download data from the
    etmaalgegevensKNMIstations dataset. It saves the data to the data directory.

    API endpoint: https://api.dataplatform.knmi.nl/open-data/v1/datasets/etmaalgegevensKNMIstations/versions/1/files
    """
    # Create the data directory if it doesn't exist
    os.makedirs(extracted_data_dir, exist_ok=True)

    # Set no_proxy environment variable to avoid segmentation fault on macOS
    # This is a workaround for a known issue with macOS and network proxy configuration
    os.environ['no_proxy'] = '*'

    # API configuration
    api_key = "eyJvcmciOiI1ZTU1NGUxOTI3NGE5NjAwMDEyYTNlYjEiLCJpZCI6ImE1OGI5NGZmMDY5NDRhZDNhZjFkMDBmNDBmNTQyNjBkIiwiaCI6Im11cm11cjEyOCJ9"
    dataset_name = "etmaalgegevensKNMIstations"
    dataset_version = "1"
    base_url = "https://api.dataplatform.knmi.nl/open-data/v1"
    # When set to True, if a file with the same name exists the output is written over the file.
    # To prevent unnecessary bandwidth usage, leave it set to False.
    overwrite = False

    # Make sure to send the API key with every HTTP request
    session = requests.Session()
    session.headers.update({"Authorization": api_key})

    try:
        filenames = []
        max_keys = 500
        next_page_token = None
        file_sizes = []

        # Use the API to get a list of all dataset filenames with pagination
        while True:
            # Retrieve dataset files after given filename
            dataset_filenames, response_json = list_dataset_files(
                session,
                base_url,
                dataset_name,
                dataset_version,
                {"maxKeys": f"{max_keys}", "nextPageToken": next_page_token},
            )
            file_sizes.extend(file["size"] for file in response_json.get("files"))
            # Store filenames
            filenames += dataset_filenames

            # If the result is not truncated, we retrieved all filenames
            next_page_token = response_json.get("nextPageToken")
            if not next_page_token:
                print("Retrieved names of all dataset files")
                break

        print(f"Number of files to download: {len(filenames)}")

        # Determine optimal number of worker threads
        worker_count = get_max_worker_count(file_sizes)

        # Use ThreadPoolExecutor for concurrent downloads
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = []

            # Create tasks for downloading each file
            for dataset_filename in filenames:
                future = executor.submit(
                    download_dataset_file,
                    session,
                    base_url,
                    dataset_name,
                    dataset_version,
                    dataset_filename,
                    extracted_data_dir,
                    overwrite,
                )
                futures.append(future)

            # Wait for all tasks to complete and gather results
            results = [future.result() for future in futures]

        # Check for failed downloads
        failed_downloads = list(filter(lambda x: not x[0], results))
        if len(failed_downloads) > 0:
            print("Failed to download the following dataset files:")
            print(list(map(lambda x: x[1], failed_downloads)))

        print(f"Finished '{dataset_name}' dataset download")

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from KNMI API: {str(e)}")
        raise
    except Exception as e:
        print(f"Error processing KNMI data: {str(e)}")
        raise

    # Return the path to the data directory
    return extracted_data_dir

# ============================================================================
# TASKS - Only process local data from extracted_data folder
# ============================================================================

def check_data_availability(**kwargs):
    """
    Check if data is available in the extracted_data directory.
    """
    import glob
    
    # Check if extracted_data directory exists and has files
    if not os.path.exists(extracted_data_dir):
        print(f"Warning: {extracted_data_dir} does not exist")
        return False
    
    # Look for data files
    all_files = glob.glob(os.path.join(extracted_data_dir, "**/*"), recursive=True)
    data_files = [f for f in all_files if os.path.isfile(f)]
    
    print(f"Found {len(data_files)} files in {extracted_data_dir}")
    
    if not data_files:
        print("No data files found in extracted_data directory")
        return False
    
    # Print some file info for debugging
    for i, file_path in enumerate(data_files[:5]):  # Show first 5 files
        file_size = os.path.getsize(file_path)
        print(f"  {i+1}. {os.path.basename(file_path)} ({file_size} bytes)")
    
    if len(data_files) > 5:
        print(f"  ... and {len(data_files) - 5} more files")
    
    return True

# Task to check data availability
check_data_task = PythonOperator(
    task_id='check_data_availability',
    python_callable=check_data_availability,
    dag=dag,
)

# Task to run heatwave calculation using local data
calculate_heatwaves_task = BashOperator(
    task_id='calculate_heatwaves',
    bash_command='cd {{ params.project_dir }} && python main.py --mode heatwaves',
    params={'project_dir': project_root},
    dag=dag,
)

# Task to run coldwave calculation using local data
calculate_coldwaves_task = BashOperator(
    task_id='calculate_coldwaves', 
    bash_command='cd {{ params.project_dir }} && python main.py --mode coldwaves',
    params={'project_dir': project_root},
    dag=dag,
)

# ============================================================================
# TASK DEPENDENCIES
# ============================================================================

# Check data availability first, then run calculations
check_data_task >> [calculate_heatwaves_task, calculate_coldwaves_task]

