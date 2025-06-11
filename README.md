# Heatwave and Coldwave Calculation Application

This application calculates heatwaves and coldwaves for The Netherlands based on meteorological data from KNMI.

## Definitions

### Heatwave
A heatwave is defined by KNMI as:
- A period of at least 5 consecutive days with maximum temperature ≥ 25°C
- Within those 5+ days, at least 3 days with maximum temperature ≥ 30°C

### Coldwave
A coldwave is defined as:
- A period of at least 5 consecutive days with maximum temperature < 0°C (freezing)
- Within those 5+ days, at least 3 days with minimum temperature < -10°C (high frost)

## Requirements

- Python 3.11
- Apache Spark 3.0+
- pandas

## Installation

### Option 1: Using Docker Compose (Recommended)

1. Make sure you have Docker and Docker Compose installed on your system.
2. Run the entire system (calculation and API) with a single command:
   ```
   docker-compose up
   ```
   This will:
   - Build and run the calculator service to calculate heatwaves and coldwaves
   - Build and run the API service to provide access to the results
   - The API will be available at http://localhost:5000

### Option 2: Using Docker

1. Make sure you have Docker installed on your system.
2. Build the Docker image:
   ```
   docker build -t heatwave-calculation .
   ```
3. Run the application:
   ```
   docker run -it heatwave-calculation
   ```
4. To run the API, build and run the API Docker image:
   ```
   docker build -t heatwave-api -f Dockerfile.api .
   docker run -it -p 5000:5000 -v $(pwd):/app heatwave-api
   ```

### Option 3: Manual Installation

1. Install Apache Spark and Python.
2. Install the required Python packages using the requirements.txt file:
   ```
   pip install -r requirements.txt
   ```

3. Run the application:
   ```
   spark-submit main.py
   ```

## Usage

### Command Line Interface

The application supports different modes of operation:

- Calculate both heatwaves and coldwaves (default):
  ```
  spark-submit main.py --mode both
  ```

- Calculate only heatwaves:
  ```
  spark-submit main.py --mode heatwaves
  ```

- Calculate only coldwaves:
  ```
  spark-submit main.py --mode coldwaves
  ```

When using Docker, you can specify the mode as follows:
```
docker run -it heatwave-calculation --mode heatwaves
```

### REST API

The application also provides a REST API for accessing the heatwave and coldwave data. To start the API server:

```
python api.py
```

The API will be available at http://localhost:5000 and provides the following endpoints:

- `GET /api/heatwaves`: Get all heatwaves or filter by year
  - Query parameters:
    - `year`: Filter heatwaves by year (e.g., 2003)

- `GET /api/coldwaves`: Get all coldwaves or filter by year
  - Query parameters:
    - `year`: Filter coldwaves by year (e.g., 2010)

- `GET /api/summary`: Get a summary of heatwaves and coldwaves by year

- `GET /`: API documentation

Example usage:
```
# Get all heatwaves
curl http://localhost:5000/api/heatwaves

# Get heatwaves for 2003
curl http://localhost:5000/api/heatwaves?year=2003

# Get a summary of heatwaves and coldwaves by year
curl http://localhost:5000/api/summary
```

## Output

The application produces two CSV files in the `results` directory:
- `results/heatwaves.csv`: Contains information about heatwaves
- `results/coldwaves.csv`: Contains information about coldwaves

Each file includes the following columns:
- From date: Start date of the extreme weather period
- To date (inc.): End date of the extreme weather period
- Duration (in days): Number of days in the period
- Number of tropical days / high frost days: Number of days with temperature ≥ 30°C (for heatwaves) or < -10°C (for coldwaves)
- Max/Min temperature: Maximum temperature during the heatwave or minimum temperature during the coldwave

## Data Source

The application uses meteorological data from KNMI (Royal Netherlands Meteorological Institute). The data is automatically downloaded when the application is run for the first time.

## Implementation Details

The application is implemented using Apache Spark, a distributed computing framework that allows for efficient processing of large datasets. The implementation follows these steps:

1. Download and extract the KNMI data
2. Load the data into a Spark DataFrame
3. Filter the data for the De Bilt weather station
4. Calculate heatwaves and/or coldwaves based on the defined criteria
5. Display the results and save them to CSV files

The application is designed to be horizontally scalable and can be extended to process other types of meteorological data.

## Repository Structure

The repository is organized as follows:

```
.
├── airflow/                    # Airflow configuration and DAGs
│   ├── dags/                   # Airflow DAG definitions
│   │   └── knmi_weather_dag.py # DAG for KNMI weather data processing
│   ├── config/                 # Airflow configuration file
│   └── ...                    # Other Airflow-related files
├── extracted_data/            # Extracted and processed data
├── heatwave_calculator/       # Core application code
│   ├── data_loader.py         # Data loading functionality
│   └── weather_analysis.py    # Weather analysis and heatwave/coldwave calculation
├── results/                   # Output directory for calculation results
│   ├── heatwaves.csv          # Heatwave calculation results
│   └── coldwaves.csv          # Coldwave calculation results
├── tests/                     # To be added
├── api.py                     # REST API implementation
├── main.py                    # Main application entry point
├── Dockerfile                 # Docker configuration for the calculator
├── Dockerfile.api             # Docker configuration for the API
├── docker-compose.yml         # Docker Compose configuration
├── docker-compose-airflow.yml # Docker Compose configuration for Airflow
├── requirements.txt           # Python dependencies
├── setup_airflow.sh           # Script for setting up Airflow
└── start_airflow.sh           # Script for managing Airflow services
```

## Airflow Integration

The application includes an Apache Airflow DAG for automated data processing. This DAG:

1. Checks if the required weather data is available in the extracted_data directory
2. Runs the heatwave and coldwave calculations using local data
3. Is scheduled to run only once to process the weather data

Note: The DAG doesn't download the latest data from the KNMI API because we couldn't find the matching endpoint from the API. That's why we skipped the download latest data from API functionality and instead process local data from the extracted_data directory.

### Setting up Airflow

#### Option 1: Automated Setup Script (Recommended for local development)

We provide a convenient setup script that automates the entire Airflow setup process:

1. Make the script executable:
   ```
   chmod +x setup_airflow.sh
   ```

2. Run the setup script:
   ```
   ./setup_airflow.sh
   ```

3. Follow the instructions printed at the end of the script to start the Airflow webserver and scheduler.

   Alternatively, you can use the provided start_airflow.sh script to manage Airflow:
   ```
   # Make the script executable
   chmod +x start_airflow.sh

   # Start Airflow (webserver and scheduler)
   ./start_airflow.sh start

   # Check Airflow status
   ./start_airflow.sh status

   # Stop Airflow
   ./start_airflow.sh stop

   # Restart Airflow
   ./start_airflow.sh restart
   ```

#### Option 2: Using Docker Compose for Airflow

For a containerized Airflow setup:

1. Make sure you have Docker and Docker Compose installed on your system.
2. Run Airflow using Docker Compose:
   ```
   docker-compose -f docker-compose-airflow.yml up -d
   ```
3. Access the Airflow API at http://localhost:8080 and use the following credentials for authentication:
   - Username: admin
   - Password: admin

To stop the Airflow containers:
```
docker-compose -f docker-compose-airflow.yml down
```
However, it is not working as expected yet. The docker-compose-airflow.yml file is not fully functional and requires further development to ensure proper integration with the application.


### Using the Airflow DAG

The DAG is located in the `dags` directory and is named `knmi_weather_dag.py`. It includes the following tasks:

1. `check_data_availability`: Checks if the required weather data is available in the extracted_data directory
2. `calculate_heatwaves`: Runs the heatwave calculation using local data
3. `calculate_coldwaves`: Runs the coldwave calculation using local data

The workflow first checks data availability, and then runs both calculation tasks in parallel if data is available.

The DAG (named 'knmi_weather_processing') is configured to run only once, but you can also trigger it manually from the Airflow web UI.

To customize the DAG, you can edit the `dags/knmi_weather_dag.py` file. For example, you can change the schedule interval or add additional tasks. The file also contains API functions for reference, but these are not used in the actual tasks as the DAG processes local data from the extracted_data directory.
