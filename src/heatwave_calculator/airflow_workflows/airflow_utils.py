"""
Utility functions for Airflow DAG

Contains helper functions for data downloading, file processing, and KNMI API interactions.
"""

import glob
import logging
import os
import tarfile
import tempfile
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List, Tuple, Dict, Any

import requests

from airflow_config import AirflowConfig

logger = logging.getLogger(__name__)


class DataDownloader:
    """Handles data downloading from various sources."""
    
    @staticmethod
    def download_blob_data(extract_dir: str) -> str:
        """Download data from blob storage.
        
        Args:
            extract_dir: Directory to extract data to
            
        Returns:
            Path to extracted data directory
            
        Raises:
            Exception: If download or extraction fails
        """
        logger.info(f"Downloading data from blob storage")
        os.makedirs(extract_dir, exist_ok=True)
        
        # Set no_proxy to avoid segmentation fault on macOS
        os.environ['no_proxy'] = '*'
        
        temp_file_path = None
        try:
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_file_path = temp_file.name
                urllib.request.urlretrieve(AirflowConfig.BLOB_DATA_URL, temp_file_path)
            
            logger.info(f"Extracting data to {extract_dir}")
            with tarfile.open(temp_file_path) as tar:
                tar.extractall(path=extract_dir)
                
        except Exception as e:
            logger.error(f"Failed to download/extract blob data: {e}")
            raise
        finally:
            if temp_file_path and os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
        
        return extract_dir


class KNMIApiClient:
    """Client for interacting with KNMI Data Platform API."""
    
    def __init__(self):
        """Initialize the API client with configuration."""
        self.config = AirflowConfig.KNMI_API_CONFIG
        self.session = requests.Session()
        self.session.headers.update({"Authorization": self.config['api_key']})
        
        # Set no_proxy to avoid segmentation fault on macOS
        os.environ['no_proxy'] = '*'
    
    def list_dataset_files(self, params: Dict[str, str]) -> Tuple[List[str], Dict[str, Any]]:
        """List files in a dataset with pagination support.
        
        Args:
            params: Query parameters for the API request
            
        Returns:
            Tuple of (filenames, response_json)
            
        Raises:
            Exception: If API request fails
        """
        logger.info(f"Retrieving dataset files with params: {params}")
        
        endpoint = f"{self.config['base_url']}/datasets/{self.config['dataset_name']}/versions/{self.config['dataset_version']}/files"
        response = self.session.get(endpoint, params=params)
        
        if response.status_code != 200:
            raise Exception(f"Unable to list dataset files: {response.status_code}")
        
        try:
            response_json = response.json()
            dataset_files = response_json.get("files", [])
            filenames = [file_info.get("filename") for file_info in dataset_files]
            return filenames, response_json
        except Exception as e:
            logger.error(f"Error parsing API response: {e}")
            raise
    
    def download_dataset_file(self, filename: str, directory: str) -> Tuple[bool, str]:
        """Download a single file from the KNMI dataset.
        
        Args:
            filename: Name of the file to download
            directory: Directory to save the file to
            
        Returns:
            Tuple of (success, filename)
        """
        file_path = Path(directory, filename).resolve()
        
        # Skip if file already exists and overwrite is disabled
        if not self.config['overwrite'] and file_path.exists():
            logger.info(f"Dataset file '{filename}' already exists")
            return True, filename
        
        try:
            # Get download URL
            endpoint = f"{self.config['base_url']}/datasets/{self.config['dataset_name']}/versions/{self.config['dataset_version']}/files/{filename}/url"
            response = self.session.get(endpoint)
            
            if response.status_code != 200:
                logger.error(f"Unable to get download URL for {filename}")
                return False, filename
            
            download_url = response.json().get("temporaryDownloadUrl")
            return self._download_from_url(download_url, directory, filename)
            
        except Exception as e:
            logger.error(f"Error downloading {filename}: {e}")
            return False, filename
    
    def _download_from_url(self, download_url: str, directory: str, filename: str) -> Tuple[bool, str]:
        """Download file from temporary download URL.
        
        Args:
            download_url: URL to download from
            directory: Directory to save to
            filename: Name of file to save
            
        Returns:
            Tuple of (success, filename)
        """
        try:
            with requests.get(download_url, stream=True) as response:
                response.raise_for_status()
                file_path = os.path.join(directory, filename)
                
                with open(file_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
            
            logger.info(f"Downloaded dataset file '{filename}'")
            
            # Extract if it's a compressed file
            if filename.endswith(('.tar', '.tgz', '.tar.gz')):
                self._extract_file(file_path, directory, filename)
            
            return True, filename
            
        except Exception as e:
            logger.error(f"Unable to download file from URL: {e}")
            return False, filename
    
    def _extract_file(self, file_path: str, directory: str, filename: str) -> None:
        """Extract compressed file.
        
        Args:
            file_path: Path to file to extract
            directory: Directory to extract to
            filename: Name of file being extracted
        """
        logger.info(f"Extracting {filename}")
        try:
            with tarfile.open(file_path) as tar:
                tar.extractall(path=directory)
            logger.info(f"Extracted {filename} to {directory}")
        except Exception as e:
            logger.error(f"Error extracting {filename}: {e}")
            raise
    
    def download_all_files(self, extract_dir: str) -> str:
        """Download all files from the KNMI dataset.
        
        Args:
            extract_dir: Directory to extract files to
            
        Returns:
            Path to extracted data directory
            
        Raises:
            Exception: If download fails
        """
        logger.info("Starting KNMI etmaal data download")
        os.makedirs(extract_dir, exist_ok=True)
        
        try:
            # Get all filenames with pagination
            filenames, file_sizes = self._get_all_filenames()
            logger.info(f"Found {len(filenames)} files to download")
            
            # Determine optimal thread count
            worker_count = self._get_optimal_worker_count(file_sizes)
            
            # Download files concurrently
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                futures = [
                    executor.submit(self.download_dataset_file, filename, extract_dir)
                    for filename in filenames
                ]
                results = [future.result() for future in futures]
            
            # Check for failures
            failed_downloads = [result for result in results if not result[0]]
            if failed_downloads:
                failed_files = [result[1] for result in failed_downloads]
                logger.warning(f"Failed to download: {failed_files}")
            
            logger.info(f"Finished '{self.config['dataset_name']}' dataset download")
            return extract_dir
            
        except Exception as e:
            logger.error(f"Error downloading KNMI data: {e}")
            raise
    
    def _get_all_filenames(self) -> Tuple[List[str], List[int]]:
        """Get all filenames from dataset with pagination.
        
        Returns:
            Tuple of (filenames, file_sizes)
        """
        filenames = []
        file_sizes = []
        next_page_token = None
        
        while True:
            params = {
                "maxKeys": str(self.config['max_keys']),
                "nextPageToken": next_page_token
            }
            # Remove None values
            params = {k: v for k, v in params.items() if v is not None}
            
            dataset_filenames, response_json = self.list_dataset_files(params)
            
            # Extract file sizes
            files_info = response_json.get("files", [])
            file_sizes.extend([file_info.get("size", 0) for file_info in files_info])
            
            filenames.extend(dataset_filenames)
            
            next_page_token = response_json.get("nextPageToken")
            if not next_page_token:
                logger.info("Retrieved names of all dataset files")
                break
        
        return filenames, file_sizes
    
    def _get_optimal_worker_count(self, file_sizes: List[int]) -> int:
        """Determine optimal number of worker threads.
        
        Args:
            file_sizes: List of file sizes
            
        Returns:
            Number of worker threads to use
        """
        if not file_sizes:
            return 1
        
        average_size = sum(file_sizes) / len(file_sizes)
        
        # Use fewer threads for large files to prevent network issues
        if average_size > self.config['size_for_threading']:
            return 1
        else:
            return self.config['max_threads']


class DataChecker:
    """Utility functions for checking data availability."""
    
    @staticmethod
    def check_data_availability(data_dir: str) -> bool:
        """Check if data is available in the specified directory.
        
        Args:
            data_dir: Directory to check for data files
            
        Returns:
            True if data is available, False otherwise
        """
        logger.info(f"Checking data availability in {data_dir}")
        
        if not os.path.exists(data_dir):
            logger.warning(f"Directory {data_dir} does not exist")
            return False
        
        # Look for data files
        all_files = glob.glob(os.path.join(data_dir, "**/*"), recursive=True)
        data_files = [f for f in all_files if os.path.isfile(f)]
        
        logger.info(f"Found {len(data_files)} files in {data_dir}")
        
        if not data_files:
            logger.warning("No data files found")
            return False
        
        # Log some file info for debugging
        for i, file_path in enumerate(data_files[:5]):
            file_size = os.path.getsize(file_path)
            logger.info(f"  {i+1}. {os.path.basename(file_path)} ({file_size} bytes)")
        
        if len(data_files) > 5:
            logger.info(f"  ... and {len(data_files) - 5} more files")
        
        return True


# Airflow task functions
def download_knmi_data(**kwargs) -> str:
    """Airflow task to download KNMI data from blob storage."""
    downloader = DataDownloader()
    return downloader.download_blob_data(AirflowConfig.EXTRACTED_DATA_DIR)


def get_knmi_etmaal_data(**kwargs) -> str:
    """Airflow task to download KNMI etmaal data from API."""
    client = KNMIApiClient()
    return client.download_all_files(AirflowConfig.EXTRACTED_DATA_DIR)


def check_data_availability(**kwargs) -> bool:
    """Airflow task to check data availability."""
    checker = DataChecker()
    return checker.check_data_availability(AirflowConfig.EXTRACTED_DATA_DIR)