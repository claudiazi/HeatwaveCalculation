"""
REST API for Heatwave and Coldwave Calculation Application

This module provides a REST API to access the heatwave and coldwave calculation results.
"""

from flask import Flask, jsonify, request
import os
import pandas as pd
import subprocess
import json

app = Flask(__name__)

# Configuration
DATA_DIR = "data"
HEATWAVES_CSV = "heatwaves.csv"
COLDWAVES_CSV = "coldwaves.csv"

def ensure_data_exists():
    """
    Ensure that the data files exist by running the calculation if necessary.
    """
    if not os.path.exists(HEATWAVES_CSV) or not os.path.exists(COLDWAVES_CSV):
        print("Calculating heatwaves and coldwaves...")
        subprocess.run(["python", "main.py", "--mode", "both"], check=True)

@app.route('/api/heatwaves', methods=['GET'])
def get_heatwaves():
    """
    Get all heatwaves or filter by year.
    
    Query parameters:
    - year: Filter heatwaves by year
    """
    ensure_data_exists()
    
    # Load heatwaves data
    if os.path.exists(HEATWAVES_CSV):
        df = pd.read_csv(HEATWAVES_CSV)
        
        # Convert to datetime for filtering
        df['From date'] = pd.to_datetime(df['From date'], format='%d %b %Y')
        
        # Filter by year if specified
        year = request.args.get('year')
        if year:
            try:
                year = int(year)
                df = df[df['From date'].dt.year == year]
            except ValueError:
                return jsonify({"error": f"Invalid year: {year}"}), 400
        
        # Convert back to string format for JSON
        df['From date'] = df['From date'].dt.strftime('%d %b %Y')
        df['To date (inc.)'] = pd.to_datetime(df['To date (inc.)']).dt.strftime('%d %b %Y')
        
        # Convert to list of dictionaries for JSON response
        heatwaves = df.to_dict(orient='records')
        return jsonify(heatwaves)
    else:
        return jsonify({"error": "Heatwaves data not found"}), 404

@app.route('/api/coldwaves', methods=['GET'])
def get_coldwaves():
    """
    Get all coldwaves or filter by year.
    
    Query parameters:
    - year: Filter coldwaves by year
    """
    ensure_data_exists()
    
    # Load coldwaves data
    if os.path.exists(COLDWAVES_CSV):
        df = pd.read_csv(COLDWAVES_CSV)
        
        # Convert to datetime for filtering
        df['From date'] = pd.to_datetime(df['From date'], format='%d %b %Y')
        
        # Filter by year if specified
        year = request.args.get('year')
        if year:
            try:
                year = int(year)
                df = df[df['From date'].dt.year == year]
            except ValueError:
                return jsonify({"error": f"Invalid year: {year}"}), 400
        
        # Convert back to string format for JSON
        df['From date'] = df['From date'].dt.strftime('%d %b %Y')
        df['To date (inc.)'] = pd.to_datetime(df['To date (inc.)']).dt.strftime('%d %b %Y')
        
        # Convert to list of dictionaries for JSON response
        coldwaves = df.to_dict(orient='records')
        return jsonify(coldwaves)
    else:
        return jsonify({"error": "Coldwaves data not found"}), 404

@app.route('/api/summary', methods=['GET'])
def get_summary():
    """
    Get a summary of heatwaves and coldwaves by year.
    """
    ensure_data_exists()
    
    summary = {"heatwaves": {}, "coldwaves": {}}
    
    # Process heatwaves
    if os.path.exists(HEATWAVES_CSV):
        df = pd.read_csv(HEATWAVES_CSV)
        df['From date'] = pd.to_datetime(df['From date'], format='%d %b %Y')
        df['year'] = df['From date'].dt.year
        
        # Group by year and count
        yearly_counts = df.groupby('year').size().to_dict()
        yearly_max_temp = df.groupby('year')['Max temperature'].max().to_dict()
        
        for year in yearly_counts:
            summary["heatwaves"][str(year)] = {
                "count": yearly_counts[year],
                "max_temperature": yearly_max_temp[year]
            }
    
    # Process coldwaves
    if os.path.exists(COLDWAVES_CSV):
        df = pd.read_csv(COLDWAVES_CSV)
        df['From date'] = pd.to_datetime(df['From date'], format='%d %b %Y')
        df['year'] = df['From date'].dt.year
        
        # Group by year and count
        yearly_counts = df.groupby('year').size().to_dict()
        yearly_min_temp = df.groupby('year')['Min temperature'].min().to_dict()
        
        for year in yearly_counts:
            summary["coldwaves"][str(year)] = {
                "count": yearly_counts[year],
                "min_temperature": yearly_min_temp[year]
            }
    
    return jsonify(summary)

@app.route('/', methods=['GET'])
def index():
    """
    API documentation.
    """
    return jsonify({
        "name": "Heatwave and Coldwave Calculation API",
        "endpoints": [
            {
                "path": "/api/heatwaves",
                "method": "GET",
                "description": "Get all heatwaves or filter by year",
                "parameters": [
                    {
                        "name": "year",
                        "type": "query",
                        "description": "Filter heatwaves by year (e.g., 2003)"
                    }
                ]
            },
            {
                "path": "/api/coldwaves",
                "method": "GET",
                "description": "Get all coldwaves or filter by year",
                "parameters": [
                    {
                        "name": "year",
                        "type": "query",
                        "description": "Filter coldwaves by year (e.g., 2010)"
                    }
                ]
            },
            {
                "path": "/api/summary",
                "method": "GET",
                "description": "Get a summary of heatwaves and coldwaves by year"
            }
        ]
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)