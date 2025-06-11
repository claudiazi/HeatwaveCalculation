#!/usr/bin/env python3
"""
Weather API Entry Point

This is the main entry point for the REST API server.
"""

import sys
import os

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Import and run the API
if __name__ == "__main__":
    try:
        from heatwave_calculator.api.api import app
    except ImportError:
        # Fallback: try direct import if module structure fails
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src', 'heatwave_calculator', 'api'))
        from api import app
    
    app.run(host='0.0.0.0', port=5000, debug=True)