"""
Heatwave Calculator Core Package

Contains the main weather analysis functionality.
"""

from .core.main import WeatherExtremeCalculator
from .core.data_loader import WeatherDataLoader, DataProcessor, DataExtractor
from .core.weather_analysis import HeatwaveAnalyzer, ColdwaveAnalyzer, ResultsExporter

__all__ = [
    'WeatherExtremeCalculator',
    'WeatherDataLoader', 
    'DataProcessor',
    'DataExtractor',
    'HeatwaveAnalyzer',
    'ColdwaveAnalyzer', 
    'ResultsExporter'
]