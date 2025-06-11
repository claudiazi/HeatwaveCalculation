# Project Structure

This document describes the reorganized structure of the Heatwave Calculator project.

## Directory Layout

```
HeatwaveCalculation/
├── src/                                    # Source code package
│   └── heatwave_calculator/               # Main package
│       ├── __init__.py                   # Package initialization
│       ├── core/                         # Core weather analysis modules
│       │   ├── __init__.py
│       │   ├── main.py                   # Main application logic
│       │   ├── data_loader.py           # Data loading and processing
│       │   └── weather_analysis.py      # Weather analysis algorithms
│       ├── airflow_workflows/           # Airflow-related modules
│       │   ├── __init__.py
│       │   ├── airflow_config.py        # Airflow configuration
│       │   └── airflow_utils.py         # Airflow utility functions
│       └── api/                         # REST API modules
│           ├── __init__.py
│           └── api.py                   # Flask API implementation
├── airflow/                            # Airflow workspace
│   └── dags/
│       ├── knmi_weather_dag.py         # Current DAG (restructured)
│       └── knmi_weather_dag_restructured.py  # New structured DAG
├── tests/                              # Test modules
├── extracted_data/                     # Weather data storage
├── config.py                          # Project configuration
├── main_new.py                        # New main entry point
├── api_new.py                         # New API entry point
├── setup.py                           # Package setup
├── requirements.txt                    # Dependencies
├── docker-compose.yml                 # Docker configuration
├── Dockerfile                         # Main Docker image
├── Dockerfile.api                     # API Docker image
└── README.md                          # Project documentation
```

## Module Organization

### Core Package (`src/heatwave_calculator/core/`)
- **main.py**: Main application with `WeatherExtremeCalculator` class
- **data_loader.py**: Data extraction, loading, and processing classes
- **weather_analysis.py**: Heatwave and coldwave analysis algorithms

### Airflow Workflows (`src/heatwave_calculator/airflow_workflows/`)
- **airflow_config.py**: Centralized Airflow configuration
- **airflow_utils.py**: Utility functions for data download and processing

### API (`src/heatwave_calculator/api/`)
- **api.py**: Flask REST API for serving weather analysis results

## Entry Points

### Command Line Interface
```bash
# Using new structured entry point
python main_new.py --mode both

# Using package installation
pip install -e .
heatwave-calculator --mode heatwaves
```

### REST API
```bash
# Using new structured entry point
python api_new.py

# Direct module execution
python -m heatwave_calculator.api.api
```

### Airflow DAG
- Use `knmi_weather_dag_restructured.py` for the new structured approach
- Properly imports from the restructured package layout

## Benefits of New Structure

1. **Modularity**: Clear separation of concerns between core logic, workflows, and API
2. **Testability**: Easier to write unit tests for individual components
3. **Maintainability**: Changes are isolated to specific modules
4. **Reusability**: Core components can be imported and used in different contexts
5. **Scalability**: Easy to add new features or extend existing functionality
6. **Package Distribution**: Can be installed as a proper Python package

## Configuration Management

- **config.py**: Central configuration for the entire project
- **airflow_config.py**: Airflow-specific configuration
- Environment variables and runtime parameters are properly managed

## Migration Guide

### For Users
1. Use `main_new.py` instead of `main.py`
2. Use `api_new.py` instead of `api.py`
3. Install as package: `pip install -e .`

### For Developers
1. Import from new package structure: `from heatwave_calculator.core import ...`
2. Use centralized configuration from `config.py`
3. Follow the modular architecture for new features

### For Airflow
1. Use `knmi_weather_dag_restructured.py` for new deployments
2. Update import paths in existing DAGs
3. Ensure proper Python path configuration