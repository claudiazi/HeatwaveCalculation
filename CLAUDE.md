# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Problem Statement

# Assessment: Heatwave in The Netherlands
Note that this assessment is not in the public domain, sharing this assessment is disallowed.
For this assessment you will create an application to calculate heatwaves in the Netherlands for the past 20 years.

In real life, the presence of a heatwave is calculated by meteorological institutions and widely used by municipalities and other authorities to prevent major power outages, physical damages, wildfires, and other negative consequences for people.

## Assessment
The definition of a heatwave varies per country, but the general idea stays the same. A heatwave is a period of excessively hot weather. 

For this assessment we will use the [definition](https://en.wikipedia.org/wiki/Heat_wave) of a heatwave as provided by the [KNMI](https://en.wikipedia.org/wiki/Royal_Netherlands_Meteorological_Institute).

The KNMI defines a heat wave as a period of at least 5 consecutive days in which the maximum temperature in De Bilt reaches or surpasses 25°C.
Additionally, during this 5 day period, the maximum temperature in De Bilt should reach or surpass 30°C for at least 3 days.

### Required solution
#### 1. Implement an application to calculate heatwaves for The Netherlands starting from 2003 

The output should contain the following information (example):

| From date   | To date (inc.) | Duration (in days) | Number of tropical days | Max temperature |
| ------------| -------------  | ------------------ | ----------------------- | --------------- |
| 31 jul 2003 | 13 aug 2003    | 14                 | 7                       | 35.0            |

#### 2. Presentation 
Prepare a presentation for the Board of Directors and consists of business and IT managers. You can assume that the technical skills are very broadly diversified.
See Evaluation below for the expectations of the presentation.

### Bonus points
If you have some time to spare, or want to show off your skills you could extend the assignment with one of the following:

- Write your application in such way that it also calculates coldwaves. A coldwave is a period of excessively cold weather with a minimum of five consecutive days below freezing (max temperature below 0.0 °C) and at least three days with high frost (min temperature is lower than -10.0 °C).
- Make your application run in a Docker container(s).
- Implement a REST API to get the result data set.
- Create an Airflow DAG which downloads new data using the KNMI api, and run the application.

### Languages / frameworks / tools
A requirement is that the solution should be easily horizontally scalable (across more than one machine) and easy extendable for other possible processing of input data.dat

We don't impose a particular framework, but Spark is the preferred framework for this assessment. 
We do however limit you to use a mainstream programming language; eg Python, Java or Scala. 
Moreover, for the sake of keeping things interesting, we do *rule out* SQL based solutions, such as Hive and Spark SQL (eg the use of the `.sql(...)` method).

### Data 
Data is available in data.tgz file in the root of this repository.

| Field                | Description                                                         |
| ---------------------|---------------------------------------------------------------------|
| `DTG`                | date of measurement                                                 |
| `LOCATION`           | location of the meteorological station                              |
| `NAME`               | name of the meteorological station                                  |
| `LATITUDE`           | in degrees (WGS84)                                                  |
| `LONGITUDE`          | in degrees (WGS84)                                                  |
| `ALTITUDE`           | in 0.1 m relative to Mean Sea Level (MSL)                           |
| `U_BOOL_10`          | air humidity code boolean 10' unit                                  |
| `T_DRYB_10`          | air temperature 10' unit Celsius degrees                            |
| `TN_10CM_PAST_6H_10` | air temperature minimum 0.1m 10' unit Celsius degrees               |
| `T_DEWP_10`          | air temperature derived dewpoint - 10' unit Celsius degrees         |
| `T_DEWP_SEA_10`      | air temperature derived dewpoint- sea 10' unit Celsius degrees      |
| `T_DRYB_SEA_10`      | air temperature height oil platform 10 minutes unit Celsius degrees |
| `TN_DRYB_10`         | air temperature minimum 10' unit Celsius degrees                    |
| `T_WETB_10`          | air temperature derived wet bulb- 10' unit Celsius degrees          |
| `TX_DRYB_10`         | air temperature maximum 10' unit Celsius degrees                    |
| `U_10`               | relative air humidity 10' unit %                                    |
| `U_SEA_10`           | is relative sea air humidity 10' unit %                             |

The `De Bilt` weather station is indicated by the Location `260_T_a`.

__IMPORTANT:__ Some columns in data might be empty. Empty values are excluded from calculations. Be aware that format of data might change from year to year of meteorological observations. 

### Evaluation
The evaluation of your solution is based on a presentation given by you for members of our team which represents the different roles within the board of directors and the IT - specialists and on the solution itself. Please provide us with the working solution and the presentation beforehand (the latest the day before you presentation) by sending it to [laura.holschbach@xebia.com](mailto:laura.holschbach@xebia.com).

While the given problem is seemingly simple, don't treat it as just a programming exercise for the sake of validating that you know how to write code (in that case, we'd have asked you to write Quicksort or some other very well defined problem). Instead, treat it as if there will be a real user depending on this solution for detecting heatwaves. Obviously, you'd like to start with a simple solution that works, but we value evidence that you have thought about the problem and perhaps expose ideas for improvement upon this. We would like to see how you bridge the gap between business and IT.

The goal of this assignment and the presentation is to assess candidates' skills in the following areas:

- Computer science
- Software development
- Distributed systems
- Coding productivity
- Presentation skills
- Bridging the gap between business and IT


## Project Overview

HeatwaveCalculation is a weather data processing application that calculates heatwaves and coldwaves for The Netherlands using KNMI meteorological data. It identifies extreme weather periods based on official KNMI definitions using Apache Spark for data processing.

## Common Development Commands

### Running the Application
```bash
# Run heatwave/coldwave calculations
python main.py --mode both          # Calculate both heatwaves and coldwaves
python main.py --mode heatwaves     # Calculate heatwaves only
python main.py --mode coldwaves     # Calculate coldwaves only

# Start the REST API
python api.py                       # Runs on port 5000

# Alternative data download approach
python download_data_from_api.py    # Direct KNMI API download with NetCDF processing
```

### Docker Operations
```bash
# Main application stack
docker-compose up                   # Run calculator and API services
docker-compose up calculator        # Run calculations only
docker-compose up api              # Run API only

# Airflow workflow orchestration
docker-compose -f docker-compose-airflow.yml up -d    # Full Airflow stack
```

### Airflow Management
```bash
# Setup and start Airflow
./setup_airflow.sh                  # Initialize Airflow environment
./start_airflow.sh start           # Start Airflow services
./start_airflow.sh stop            # Stop Airflow services
./start_airflow.sh restart         # Restart Airflow services
./start_airflow.sh status          # Check service status
```

### Testing
```bash
# Run all tests
python -m pytest tests/

# Run specific test files
python -m pytest tests/test_main.py
python -m pytest tests/test_load_data.py
python -m pytest tests/test_read_nc.py
```

## Architecture

### Core Data Processing (main.py)
- **Technology**: Apache Spark (PySpark) for distributed processing
- **Data Sources**: Processes KNMI data from data.tgz archive or direct API downloads
- **Text Processing**: Handles space-delimited weather data files with robust parsing
- **Station Focus**: Primarily processes De Bilt weather station (260_T_a) data
- **Output**: Generates CSV files (heatwaves.csv, coldwaves.csv) with period details

### REST API (api.py)
- **Framework**: Flask-based REST API
- **Endpoints**: 
  - `/api/heatwaves` - Heatwave data with optional year filtering
  - `/api/coldwaves` - Coldwave data with optional year filtering
  - `/api/summary` - Yearly summaries with counts and temperature extremes
- **Auto-processing**: Triggers calculations if result files don't exist

### Workflow Orchestration (airflow/dags/knmi_weather_dag.py)
- **Scheduler**: Daily Airflow DAG for automated processing
- **Tasks**: Data download, extraction, heatwave/coldwave calculations
- **Data Sources**: Both blob storage (data.tgz) and KNMI API with concurrent downloads
- **Resilience**: Retry logic and error handling throughout pipeline

### Weather Criteria Definitions
- **Heatwaves**: ≥5 consecutive days with max temp ≥25°C, including ≥3 days ≥30°C
- **Coldwaves**: ≥5 consecutive days with max temp <0°C, including ≥3 days with min temp <-10°C

## Key Dependencies

- **pyspark**: Distributed data processing engine
- **flask**: REST API framework
- **apache-airflow**: Workflow orchestration
- **pandas**: Data manipulation
- **xarray/netCDF4**: Scientific data format handling
- **requests**: HTTP client for API interactions

## Data Flow

1. **Ingestion**: Downloads from KNMI API or extracts from data.tgz
2. **Processing**: Spark-based parsing of space-delimited text files
3. **Filtering**: Extracts De Bilt station data from 2003 onwards
4. **Analysis**: Window functions identify consecutive extreme weather periods
5. **Validation**: Applies KNMI criteria for heatwave/coldwave classification
6. **Output**: CSV files and JSON API responses

## File Processing Notes

- Weather data files are space-delimited text (not CSV despite file loading functions)
- Files may lack extensions but follow consistent formatting
- Station code 260 represents De Bilt weather station
- Date format: YYYYMMDD in DTG field, converted to DATE column
- Temperature fields: TX (max), TN (min) in 0.1°C units