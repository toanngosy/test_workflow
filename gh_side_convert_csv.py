#!/usr/bin/env python3
"""
GitHub Actions script to convert CSV reports to JSON format for GitHub Pages
"""

import csv
import json
import os
from pathlib import Path


def convert_csv_to_json(csv_file_path, json_file_path):
    """Convert CSV file to JSON format"""
    data = []
    
    if not os.path.exists(csv_file_path):
        print(f"Warning: CSV file {csv_file_path} not found")
        return []
    
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Clean empty strings and strip whitespace
                cleaned_row = {k: v.strip() if v.strip() else None for k, v in row.items()}
                data.append(cleaned_row)
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(json_file_path), exist_ok=True)
        
        # Write JSON file
        with open(json_file_path, 'w', encoding='utf-8') as jsonfile:
            json.dump(data, jsonfile, indent=2, default=str)
        
        print(f"Converted {len(data)} records from {csv_file_path} to {json_file_path}")
        return data
        
    except Exception as e:
        print(f"Error converting {csv_file_path}: {e}")
        return []


def main():
    """Main function to convert all CSV reports to JSON"""
    print("Starting CSV to JSON conversion...")
    
    # Ensure docs/data directory exists
    os.makedirs('docs/data', exist_ok=True)
    
    # Convert log.csv (main runs data)
    log_csv = 'report/server/rocky/log.csv'
    log_json = 'docs/data/runs.json'
    runs_data = convert_csv_to_json(log_csv, log_json)
    
    # Convert scenario.csv (scenario metadata)
    scenario_csv = 'report/scenario.csv'
    scenario_json = 'docs/data/scenarios.json'
    scenarios_data = convert_csv_to_json(scenario_csv, scenario_json)
    
    # Create summary stats
    summary = {
        'total_runs': len(runs_data),
        'total_scenarios': len(scenarios_data),
        'last_updated': None,
        'status_counts': {}
    }
    
    if runs_data:
        # Count statuses
        status_counts = {}
        latest_timestamp = None
        
        for run in runs_data:
            status = run.get('state', 'unknown')
            status_counts[status] = status_counts.get(status, 0) + 1
            
            # Find latest timestamp
            timestamp = run.get('last_updated_timestamp')
            if timestamp and (not latest_timestamp or timestamp > latest_timestamp):
                latest_timestamp = timestamp
        
        summary['status_counts'] = status_counts
        summary['last_updated'] = latest_timestamp
    
    # Write summary
    with open('docs/data/summary.json', 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, default=str)
    
    print(f"Summary: {summary}")
    print("CSV to JSON conversion completed!")


if __name__ == '__main__':
    main() 