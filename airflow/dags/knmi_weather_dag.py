"""
Airflow DAG for KNMI Weather Data Processing

This DAG processes weather data from local extracted_data folder to calculate heatwaves and coldwaves.
The API functions are kept for reference but tasks only use local data.
"""

import glob
import os
import sys
import tarfile
import tempfile
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import requests
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

# ============================================================================
# CONFIGURATION
# ============================================================================

# Add the parent directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Define directories
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
extracted_data_dir = os.path.join(project_root, 'extracted_data')

# KNMI API configuration
KNMI_API_KEY = "eyJvcmciOiI1ZTU1NGUxOTI3NGE5NjAwMDEyYTNlYjEiLCJpZCI6ImE1OGI5NGZmMDY5NDRhZDNhZjFkMDBmNDBmNTQyNjBkIiwiaCI6Im11cm11cjEyOCJ9"
KNMI_DATASET_NAME = "etmaalgegevensKNMIstations"
KNMI_DATASET_VERSION = "1"
KNMI_BASE_URL = "https://api.dataplatform.knmi.nl/open-data/v1"
KNMI_OVERWRITE_FILES = False  # When True, overwrites existing files during download

# Fallback data URL (used in download_knmi_data function)
KNMI_FALLBACK_URL = "https://gddassesmentdata.blob.core.windows.net/knmi-data/data.tgz?se=2025-12-31&sp=r&spr=https&sv=2022-11-02&sr=c&sig=kEM%2BqoaJt1BGPRXvCk5d8aTOGs7vvGv%2BYc1acDEaZAU%3D"

# Threading configuration
SIZE_THRESHOLD_FOR_THREADING = 10_000_000  # 10 MB
MAX_WORKER_THREADS = 10
MAX_KEYS_PER_PAGE = 500

# ============================================================================
# DAG DEFINITION
# ============================================================================

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 6, 11),
}

# Create the DAG
dag = DAG(
    'knmi_weather_processing',
    default_args=default_args,
    description='Process local KNMI weather data and calculate heatwaves and coldwaves',
    schedule=None,  # Run only once
    catchup=False,
)

# ============================================================================
# API FUNCTIONS - For reference, not used in actual tasks
# ============================================================================

def download_knmi_data(**kwargs) -> str:
    """
    Download the latest weather data from the KNMI API using a fallback URL.

    This is a simplified function that downloads data from a static URL.
    It saves the data to the extracted_data directory.

    Args:
        **kwargs: Keyword arguments passed from Airflow

    Returns:
        str: Path to the extracted data directory
    """
    # Create the data directory if it doesn't exist
    os.makedirs(extracted_data_dir, exist_ok=True)

    # Set no_proxy environment variable to avoid segmentation fault on macOS
    os.environ['no_proxy'] = '*'

    print(f"Downloading data from {KNMI_FALLBACK_URL}...")

    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        urllib.request.urlretrieve(KNMI_FALLBACK_URL, temp_file.name)

        print(f"Extracting data to {extracted_data_dir}...")
        with tarfile.open(temp_file.name) as tar:
            tar.extractall(path=extracted_data_dir)

    os.unlink(temp_file.name)
    return extracted_data_dir


def download_dataset_file(
    session: requests.Session,
    base_url: str,
    dataset_name: str,
    dataset_version: str,
    filename: str,
    directory: str,
    overwrite: bool,
) -> Tuple[bool, str]:
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
        Tuple[bool, str]: A tuple of (success, filename)
    """
    # If a file from this dataset already exists, skip downloading it
    file_path = Path(directory, filename).resolve()
    if not overwrite and file_path.exists():
        print(f"Dataset file '{filename}' was already downloaded.")
        return True, filename

    # Get the temporary download URL for the file
    endpoint = f"{base_url}/datasets/{dataset_name}/versions/{dataset_version}/files/{filename}/url"
    get_file_response = session.get(endpoint)

    if get_file_response.status_code != 200:
        print(f"Unable to get file: {filename}")
        print(get_file_response.content)
        return False, filename

    # Use the temporary download URL to get the file
    download_url = get_file_response.json().get("temporaryDownloadUrl")
    return download_file_from_temporary_download_url(download_url, directory, filename)


def download_file_from_temporary_download_url(
    download_url: str, 
    directory: str, 
    filename: str
) -> Tuple[bool, str]:
    """
    Download a file from a temporary download URL.

    Args:
        download_url: The URL to download the file from
        directory: The directory to save the file to
        filename: The name of the file to save

    Returns:
        Tuple[bool, str]: A tuple of (success, filename)
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
    params: Dict[str, str],
) -> Tuple[List[str], Dict[str, Any]]:
    """
    List files in a dataset with pagination support.

    Args:
        session: The requests session with authentication
        base_url: The base URL for the API
        dataset_name: The name of the dataset
        dataset_version: The version of the dataset
        params: Query parameters for the API request

    Returns:
        Tuple[List[str], Dict[str, Any]]: A tuple of (filenames, response_json)
    """
    print(f"Retrieve dataset files with query params: {params}")

    list_files_endpoint = f"{base_url}/datasets/{dataset_name}/versions/{dataset_version}/files"
    list_files_response = session.get(list_files_endpoint, params=params)

    if list_files_response.status_code != 200:
        raise Exception("Unable to list dataset files")

    try:
        list_files_response_json = list_files_response.json()
        dataset_files = list_files_response_json.get("files")
        dataset_filenames = [file.get("filename") for file in dataset_files]
        return dataset_filenames, list_files_response_json
    except Exception as e:
        print(f"Error parsing API response: {str(e)}")
        raise


def get_max_worker_count(filesizes: List[int]) -> int:
    """
    Determine the optimal number of worker threads based on file sizes.

    Args:
        filesizes: A list of file sizes in bytes

    Returns:
        int: The number of worker threads to use
    """
    if not filesizes:
        return 1

    average = sum(filesizes) / len(filesizes)
    # To prevent downloading multiple half files in case of a network failure with big files
    if average > SIZE_THRESHOLD_FOR_THREADING:
        return 1
    else:
        return MAX_WORKER_THREADS


def get_knmi_etmaal_data(**kwargs) -> str:
    """
    Download data from the KNMI etmaalgegevensKNMIstations dataset.

    This function uses the KNMI Data Platform API to download data from the
    etmaalgegevensKNMIstations dataset. It saves the data to the extracted_data directory.

    Args:
        **kwargs: Keyword arguments passed from Airflow

    Returns:
        str: Path to the extracted data directory
    """
    # Create the data directory if it doesn't exist
    os.makedirs(extracted_data_dir, exist_ok=True)

    # Set no_proxy environment variable to avoid segmentation fault on macOS
    os.environ['no_proxy'] = '*'

    # Make sure to send the API key with every HTTP request
    session = requests.Session()
    session.headers.update({"Authorization": KNMI_API_KEY})

    try:
        filenames = []
        next_page_token = None
        file_sizes = []

        # Use the API to get a list of all dataset filenames with pagination
        while True:
            # Retrieve dataset files with pagination
            dataset_filenames, response_json = list_dataset_files(
                session,
                KNMI_BASE_URL,
                KNMI_DATASET_NAME,
                KNMI_DATASET_VERSION,
                {"maxKeys": f"{MAX_KEYS_PER_PAGE}", "nextPageToken": next_page_token},
            )
            file_sizes.extend(file["size"] for file in response_json.get("files"))
            filenames.extend(dataset_filenames)

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
                    KNMI_BASE_URL,
                    KNMI_DATASET_NAME,
                    KNMI_DATASET_VERSION,
                    dataset_filename,
                    extracted_data_dir,
                    KNMI_OVERWRITE_FILES,
                )
                futures.append(future)

            # Wait for all tasks to complete and gather results
            results = [future.result() for future in futures]

        # Check for failed downloads
        failed_downloads = [result for result in results if not result[0]]
        if failed_downloads:
            print("Failed to download the following dataset files:")
            print([result[1] for result in failed_downloads])

        print(f"Finished '{KNMI_DATASET_NAME}' dataset download")

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from KNMI API: {str(e)}")
        raise
    except Exception as e:
        print(f"Error processing KNMI data: {str(e)}")
        raise

    return extracted_data_dir

# ============================================================================
# TASK FUNCTIONS - Process local data from extracted_data folder
# ============================================================================

def check_data_availability(**kwargs) -> bool:
    """
    Check if data is available in the extracted_data directory.

    This function verifies that the extracted_data directory exists and contains files.
    It also prints information about the first few files for debugging purposes.

    Args:
        **kwargs: Keyword arguments passed from Airflow

    Returns:
        bool: True if data is available, False otherwise
    """
    # Check if extracted_data directory exists
    if not os.path.exists(extracted_data_dir):
        print(f"Warning: {extracted_data_dir} does not exist")
        return False

    # Look for data files recursively
    all_files = glob.glob(os.path.join(extracted_data_dir, "**/*"), recursive=True)
    data_files = [f for f in all_files if os.path.isfile(f)]

    print(f"Found {len(data_files)} files in {extracted_data_dir}")

    if not data_files:
        print("No data files found in extracted_data directory")
        return False

    # Print information about the first few files for debugging
    MAX_FILES_TO_DISPLAY = 5
    for i, file_path in enumerate(data_files[:MAX_FILES_TO_DISPLAY]):
        file_size = os.path.getsize(file_path)
        print(f"  {i+1}. {os.path.basename(file_path)} ({file_size} bytes)")

    if len(data_files) > MAX_FILES_TO_DISPLAY:
        print(f"  ... and {len(data_files) - MAX_FILES_TO_DISPLAY} more files")

    return True

# ============================================================================
# TASK DEFINITIONS
# ============================================================================

# Task to check data availability
check_data_task = PythonOperator(
    task_id='check_data_availability',
    python_callable=check_data_availability,
    dag=dag,
    doc_md="""
    ### Check Data Availability Task

    This task checks if the required weather data is available in the extracted_data directory.
    If no data is found, subsequent tasks will not run.
    """,
)

# Task to run heatwave calculation using local data
calculate_heatwaves_task = BashOperator(
    task_id='calculate_heatwaves',
    bash_command='cd {{ params.project_dir }} && python main.py --mode heatwaves',
    params={'project_dir': project_root},
    dag=dag,
    doc_md="""
    ### Calculate Heatwaves Task

    This task runs the heatwave calculation script using the local data.
    It executes the main.py script with the --mode heatwaves parameter.
    """,
)

# Task to run coldwave calculation using local data
calculate_coldwaves_task = BashOperator(
    task_id='calculate_coldwaves', 
    bash_command='cd {{ params.project_dir }} && python main.py --mode coldwaves',
    params={'project_dir': project_root},
    dag=dag,
    doc_md="""
    ### Calculate Coldwaves Task

    This task runs the coldwave calculation script using the local data.
    It executes the main.py script with the --mode coldwaves parameter.
    """,
)

# ============================================================================
# TASK DEPENDENCIES
# ============================================================================

# Check data availability first, then run calculations in parallel
check_data_task >> [calculate_heatwaves_task, calculate_coldwaves_task]
