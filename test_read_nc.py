import os
from download_data_from_api import read_nc_file

# Path to the NetCDF file
nc_file_path = os.path.join("data", "KIS___OPER_P___OBS_____L2.nc")

# Read the NetCDF file
df = read_nc_file(nc_file_path)

# Print some basic information about the DataFrame
print(f"NetCDF file: {nc_file_path}")
print(f"DataFrame shape: {df.shape}")
print(f"DataFrame columns: {df.columns.tolist()}")

# Print the first few rows of the DataFrame
print("\nFirst 5 rows of the DataFrame:")
print(df.head())

# Print some basic statistics
print("\nBasic statistics:")
print(df.describe())
