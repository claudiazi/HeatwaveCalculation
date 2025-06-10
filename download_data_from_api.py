import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, Union

import pandas as pd
import requests
import xarray as xr
from requests import Session

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))


def download_dataset_file(
    session: Session,
    base_url: str,
    dataset_name: str,
    dataset_version: str,
    filename: str,
    directory: str,
    overwrite: bool,
) -> tuple[bool, str]:
    # if a file from this dataset already exists, skip downloading it.
    file_path = Path(directory, filename).resolve()
    if not overwrite and file_path.exists():
        logger.info(f"Dataset file '{filename}' was already downloaded.")
        return True, filename

    endpoint = f"{base_url}/datasets/{dataset_name}/versions/{dataset_version}/files/{filename}/url"
    get_file_response = session.get(endpoint)

    # retrieve download URL for dataset file
    if get_file_response.status_code != 200:
        logger.warning(f"Unable to get file: {filename}")
        logger.warning(get_file_response.content)
        return False, filename

    # use download URL to GET dataset file. We don't need to set the 'Authorization' header,
    # The presigned download URL already has permissions to GET the file contents
    download_url = get_file_response.json().get("temporaryDownloadUrl")
    return download_file_from_temporary_download_url(download_url, directory, filename)


def download_file_from_temporary_download_url(download_url, directory, filename):
    try:
        with requests.get(download_url, stream=True) as r:
            r.raise_for_status()
            with open(f"{directory}/{filename}", "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
    except Exception:
        logger.exception("Unable to download file using download URL")
        return False, filename

    logger.info(f"Downloaded dataset file '{filename}'")
    return True, filename


def list_dataset_files(
    session: Session,
    base_url: str,
    dataset_name: str,
    dataset_version: str,
    params: dict[str, str],
) -> tuple[list[str], dict[str, Any]]:
    logger.info(f"Retrieve dataset files with query params: {params}")

    list_files_endpoint = f"{base_url}/datasets/{dataset_name}/versions/{dataset_version}/files"
    list_files_response = session.get(list_files_endpoint, params=params)

    if list_files_response.status_code != 200:
        raise Exception("Unable to list initial dataset files")

    try:
        list_files_response_json = list_files_response.json()
        dataset_files = list_files_response_json.get("files")
        dataset_filenames = list(map(lambda x: x.get("filename"), dataset_files))
        return dataset_filenames, list_files_response_json
    except Exception as e:
        logger.exception(e)
        raise Exception(e)


def get_max_worker_count(filesizes):
    size_for_threading = 10_000_000  # 10 MB
    average = sum(filesizes) / len(filesizes)
    # to prevent downloading multiple half files in case of a network failure with big files
    if average > size_for_threading:
        threads = 1
    else:
        threads = 10
    return threads


async def main():
    api_key = "eyJvcmciOiI1ZTU1NGUxOTI3NGE5NjAwMDEyYTNlYjEiLCJpZCI6ImE1OGI5NGZmMDY5NDRhZDNhZjFkMDBmNDBmNTQyNjBkIiwiaCI6Im11cm11cjEyOCJ9"
    dataset_name = "daily-in-situ-meteorological-observations"
    dataset_version = "1.0"
    base_url = "https://api.dataplatform.knmi.nl/open-data/v1"
    # When set to True, if a file with the same name exists the output is written over the file.
    # To prevent unnecessary bandwidth usage, leave it set to False.
    overwrite = False

    download_directory = "./data"

    # Make sure to send the API key with every HTTP request
    session = requests.Session()
    session.headers.update({"Authorization": api_key})

    # Verify that the download directory exists
    if not Path(download_directory).is_dir() or not Path(download_directory).exists():
        raise Exception(f"Invalid or non-existing directory: {download_directory}")

    filenames = []
    max_keys = 500
    next_page_token = None
    file_sizes = []
    # Use the API to get a list of all dataset filenames
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
            logger.info("Retrieved names of all dataset files")
            break

    logger.info(f"Number of files to download: {len(filenames)}")

    worker_count = get_max_worker_count(file_sizes)
    loop = asyncio.get_event_loop()

    # Allow up to 10 separate threads to download dataset files concurrently
    executor = ThreadPoolExecutor(max_workers=worker_count)
    futures = []

    # Create tasks that download the dataset files
    for dataset_filename in filenames:
        # Create future for dataset file
        future = loop.run_in_executor(
            executor,
            download_dataset_file,
            session,
            base_url,
            dataset_name,
            dataset_version,
            dataset_filename,
            download_directory,
            overwrite,
        )
        futures.append(future)

    # # Wait for all tasks to complete and gather the results
    future_results = await asyncio.gather(*futures)
    logger.info(f"Finished '{dataset_name}' dataset download")

    failed_downloads = list(filter(lambda x: not x[0], future_results))

    if len(failed_downloads) > 0:
        logger.warning("Failed to download the following dataset files:")
        logger.warning(list(map(lambda x: x[1], failed_downloads)))

    # Read all downloaded NetCDF files and get DataFrames from them
    logger.info("Reading downloaded NetCDF files...")
    nc_files = list(Path(download_directory).glob("**/*.nc"))
    dataframes = {}

    for nc_file in nc_files:
        logger.info(f"Processing NetCDF file: {nc_file}")
        df = read_nc_file(nc_file)
        if not df.empty:
            dataframes[nc_file.name] = df
            logger.info(f"Successfully read DataFrame from {nc_file.name} with shape {df.shape}")
        else:
            logger.warning(f"Failed to read DataFrame from {nc_file.name}")

    logger.info(f"Read {len(dataframes)} NetCDF files into DataFrames")
    return dataframes


def read_nc_file(file_path: Union[str, Path]) -> pd.DataFrame:
    """
    Read a NetCDF (.nc) file and return its contents as a pandas DataFrame.

    Args:
        file_path (Union[str, Path]): Path to the NetCDF file

    Returns:
        pd.DataFrame: DataFrame containing the data from the NetCDF file
    """
    try:
        # Convert to Path object if string
        if isinstance(file_path, str):
            file_path = Path(file_path)

        # Check if file exists
        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            return pd.DataFrame()

        # Check if file is a NetCDF file
        if not str(file_path).lower().endswith('.nc'):
            logger.error(f"File is not a NetCDF file: {file_path}")
            return pd.DataFrame()

        # Open the NetCDF file using xarray
        logger.info(f"Reading NetCDF file: {file_path}")
        ds = xr.open_dataset(file_path)

        # Display metadata
        logger.info(f"NetCDF file metadata: {ds}")

        # Convert to pandas DataFrame
        df = ds.to_dataframe().reset_index()

        # Close the dataset
        ds.close()

        logger.info(f"Successfully read NetCDF file: {file_path}")
        return df

    except Exception as e:
        logger.exception(f"Error reading NetCDF file {file_path}: {str(e)}")
        return pd.DataFrame()


if __name__ == "__main__":
    # import glob
    # nc_files = glob.glob(os.path.join("data", "**/*.nc"), recursive=True)
    #
    # df = read_nc_file(nc_files[0])
    df = asyncio.run(main())
    # print(1)
