#!/bin/bash
# Script to start airflow api-server and scheduler

set -e  # Exit immediately if a command exits with a non-zero status

# Define colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if the virtual environment exists
if [ ! -d "venv" ]; then
    echo -e "${YELLOW}Virtual environment not found. Please run setup_airflow.sh first.${NC}"
    exit 1
fi

# Set up Airflow home directory
export AIRFLOW_HOME=$(pwd)/airflow

# Check if Airflow home directory exists
if [ ! -d "$AIRFLOW_HOME" ]; then
    echo -e "${YELLOW}Airflow home directory not found. Please run setup_airflow.sh first.${NC}"
    exit 1
fi

# Function to clear Python cache
clear_python_cache() {
    echo -e "${YELLOW}Clearing Python cache...${NC}"
    find airflow/dags -name "*.pyc" -delete
    find airflow/dags -name "__pycache__" -exec rm -rf {} +
    echo -e "${GREEN}Python cache cleared.${NC}"
}

# Function to update Airflow configuration
update_airflow_config() {
    echo -e "${YELLOW}Updating Airflow configuration...${NC}"
    # Set min_file_process_interval to 1 second to detect DAG changes more quickly
    sed -i.bak 's/^min_file_process_interval = .*/min_file_process_interval = 1/' airflow/config/airflow.cfg
    echo -e "${GREEN}Airflow configuration updated.${NC}"
}

# Function to reload DAGs
reload_dags() {
    echo -e "${YELLOW}Reloading DAGs...${NC}"
    source venv/bin/activate
    # Wait a moment for Airflow to start
    sleep 5
    # Trigger a DAG refresh
    airflow dags reserialize
    echo -e "${GREEN}DAGs reloaded.${NC}"
}

# Function to start the webserver
start_webserver() {
    echo -e "${YELLOW}Starting airflow api-server...${NC}"
    source venv/bin/activate
    nohup airflow api-server --port 8080 > airflow/webserver.log 2>&1 &
    echo $! > airflow/webserver.pid
    echo -e "${GREEN}airflow api-server started. PID: $(cat airflow/webserver.pid)${NC}"
}

# Function to start the scheduler
start_scheduler() {
    echo -e "${YELLOW}Starting Airflow scheduler...${NC}"
    source venv/bin/activate
    nohup airflow scheduler > airflow/scheduler.log 2>&1 &
    echo $! > airflow/scheduler.pid
    echo -e "${GREEN}Airflow scheduler started. PID: $(cat airflow/scheduler.pid)${NC}"
}

# Function to stop Airflow components
stop_airflow() {
    # First try to stop processes using PID files
    if [ -f "airflow/webserver.pid" ]; then
        echo -e "${YELLOW}Stopping airflow api-server...${NC}"
        PID=$(cat airflow/webserver.pid)
        kill $PID 2>/dev/null || true
        # Wait a moment for the process to terminate
        sleep 2
        # Check if process is still running and force kill if necessary
        if ps -p $PID > /dev/null 2>&1; then
            echo -e "${YELLOW}Process still running, force killing...${NC}"
            kill -9 $PID 2>/dev/null || true
        fi
        rm airflow/webserver.pid
    fi

    if [ -f "airflow/scheduler.pid" ]; then
        echo -e "${YELLOW}Stopping Airflow scheduler...${NC}"
        PID=$(cat airflow/scheduler.pid)
        kill $PID 2>/dev/null || true
        # Wait a moment for the process to terminate
        sleep 2
        # Check if process is still running and force kill if necessary
        if ps -p $PID > /dev/null 2>&1; then
            echo -e "${YELLOW}Process still running, force killing...${NC}"
            kill -9 $PID 2>/dev/null || true
        fi
        rm airflow/scheduler.pid
    fi

    # Find and kill any remaining airflow processes
    echo -e "${YELLOW}Checking for any remaining Airflow processes...${NC}"
    REMAINING_PIDS=$(ps aux | grep "airflow" | grep -v grep | grep -v "start_airflow.sh" | awk '{print $2}')

    if [ ! -z "$REMAINING_PIDS" ]; then
        echo -e "${YELLOW}Found additional Airflow processes. Terminating...${NC}"
        for PID in $REMAINING_PIDS; do
            echo -e "${YELLOW}Killing process $PID...${NC}"
            kill $PID 2>/dev/null || true
            sleep 1
            # Force kill if still running
            if ps -p $PID > /dev/null 2>&1; then
                echo -e "${YELLOW}Process still running, force killing...${NC}"
                kill -9 $PID 2>/dev/null || true
            fi
        done
    fi

    # Verify all processes are stopped
    REMAINING_PIDS=$(ps aux | grep "airflow" | grep -v grep | grep -v "start_airflow.sh" | awk '{print $2}')
    if [ -z "$REMAINING_PIDS" ]; then
        echo -e "${GREEN}All Airflow processes have been stopped.${NC}"
    else
        echo -e "${YELLOW}Warning: Some Airflow processes could not be terminated. Manual intervention may be required.${NC}"
        echo -e "${YELLOW}Remaining processes: $REMAINING_PIDS${NC}"
    fi
}

# Parse command line arguments
case "$1" in
    start)
        clear_python_cache
        update_airflow_config
        start_webserver
        start_scheduler
        reload_dags
        echo -e "${GREEN}Airflow is now running!${NC}"
        echo -e "${GREEN}Access the Airflow web UI at http://localhost:8080${NC}"
        echo -e "${GREEN}Username: admin, Password: admin${NC}"
        echo -e "${GREEN}To stop Airflow, run: ./start_airflow.sh stop${NC}"
        ;;
    stop)
        stop_airflow
        ;;
    restart)
        stop_airflow
        sleep 2
        clear_python_cache
        update_airflow_config
        start_webserver
        start_scheduler
        reload_dags
        echo -e "${GREEN}Airflow has been restarted!${NC}"
        echo -e "${GREEN}Access the Airflow web UI at http://localhost:8080${NC}"
        ;;
    status)
        if [ -f "airflow/webserver.pid" ] && [ -f "airflow/scheduler.pid" ]; then
            echo -e "${GREEN}Airflow is running.${NC}"
            echo -e "${GREEN}Webserver PID: $(cat airflow/webserver.pid)${NC}"
            echo -e "${GREEN}Scheduler PID: $(cat airflow/scheduler.pid)${NC}"
        else
            echo -e "${YELLOW}Airflow is not running.${NC}"
        fi
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status}"
        exit 1
        ;;
esac

exit 0
