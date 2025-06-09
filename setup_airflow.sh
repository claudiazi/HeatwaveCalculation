#!/bin/bash
# Script to set up a local Airflow environment for the Heatwave Calculation Application

set -e  # Exit immediately if a command exits with a non-zero status

# Define colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Create a virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo -e "${YELLOW}Creating virtual environment...${NC}"
    python3 -m venv venv
fi

# Activate the virtual environment
echo -e "${YELLOW}Activating virtual environment...${NC}"
source venv/bin/activate

# Install dependencies
echo -e "${YELLOW}Installing dependencies...${NC}"
pip install -r requirements.txt

# Set up Airflow home directory
export AIRFLOW_HOME=$(pwd)/airflow
mkdir -p $AIRFLOW_HOME
mkdir -p $AIRFLOW_HOME/dags

# Copy the DAG file to the Airflow dags directory
echo -e "${YELLOW}Copying DAG file to Airflow dags directory...${NC}"
cp dags/knmi_weather_dag.py $AIRFLOW_HOME/dags/

# Initialize the Airflow database
echo -e "${YELLOW}Initializing Airflow database...${NC}"
airflow db init

# Create an Airflow admin user if it doesn't exist
echo -e "${YELLOW}Creating Airflow admin user...${NC}"
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Print instructions for starting Airflow
echo -e "${GREEN}Airflow has been set up successfully!${NC}"
echo -e "${GREEN}To start the Airflow webserver, run:${NC}"
echo -e "source venv/bin/activate"
echo -e "export AIRFLOW_HOME=$(pwd)/airflow"
echo -e "airflow webserver --port 8080"
echo -e "${GREEN}In a separate terminal, start the Airflow scheduler:${NC}"
echo -e "source venv/bin/activate"
echo -e "export AIRFLOW_HOME=$(pwd)/airflow"
echo -e "airflow scheduler"
echo -e "${GREEN}Access the Airflow web UI at http://localhost:8080${NC}"
echo -e "${GREEN}Username: admin, Password: admin${NC}"