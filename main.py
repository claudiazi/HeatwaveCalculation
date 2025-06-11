#!/usr/bin/env python3
"""
Weather Extremes Calculation Application Entry Point

This is the main entry point for the heatwave/coldwave calculation application.
"""

import sys
import os

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from heatwave_calculator.core.main import main

if __name__ == "__main__":
    main()