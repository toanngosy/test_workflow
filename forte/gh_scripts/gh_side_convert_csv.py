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


def process_runs_data(all_servers_data):
    """Process runs data to group by run_uuid and get latest status"""
    runs_by_uuid = {}
    
    for run in all_servers_data:
        run_uuid = run.get('run_uuid')
        if not run_uuid:
            continue
            
        timestamp = run.get('last_updated_timestamp', '')
        
        # If this run_uuid doesn't exist or has older timestamp, update it
        if (run_uuid not in runs_by_uuid or 
            timestamp > runs_by_uuid[run_uuid].get('last_updated_timestamp', '')):
            
            # Determine status - if additional_info contains a path, it's succeed
            state = run.get('state', '0')
            additional_info = run.get('additional_info', '')
            
            if additional_info and ('/' in additional_info or '\\' in additional_info):
                status = 'succeed'
            else:
                status_map = {
                    '0': 'pending',
                    '1': 'running', 
                    '2': 'completed',
                    '3': 'failed'
                }
                status = status_map.get(state, 'unknown')
            
            # Generate output path if succeed
            output_path = ''
            if status == 'succeed':
                site_id = run.get('site_id', '')
                if site_id and run_uuid:
                    output_path = f"report/{site_id}/{run_uuid}"
            
            runs_by_uuid[run_uuid] = {
                'site_id': run.get('site_id', ''),
                'server': run.get('server', ''),
                'run_uuid': run_uuid,
                'last_updated_timestamp': timestamp,
                'status': status,
                'params': run.get('file_path', ''),
                'output': output_path,
                'actor': run.get('actor', ''),
                'process_id': run.get('process_id', ''),
                'state': state
            }
    
    return list(runs_by_uuid.values())


def main():
    """Main function to convert all CSV reports to JSON"""
    print("Starting CSV to JSON conversion...")
    
    # Ensure docs/data directory exists
    os.makedirs('docs/data', exist_ok=True)
    
    # Process all server log files
    all_runs_data = []
    server_dir = 'report/server'
    
    if os.path.exists(server_dir):
        for server_name in os.listdir(server_dir):
            server_path = os.path.join(server_dir, server_name)
            if os.path.isdir(server_path):
                log_csv = os.path.join(server_path, 'log.csv')
                if os.path.exists(log_csv):
                    print(f"Processing {server_name} server data...")
                    server_data = []
                    
                    try:
                        with open(log_csv, 'r', encoding='utf-8') as csvfile:
                            reader = csv.DictReader(csvfile)
                            for row in reader:
                                # Add server name to each row
                                cleaned_row = {k: v.strip() if v and v.strip() else None for k, v in row.items()}
                                cleaned_row['server'] = server_name
                                server_data.append(cleaned_row)
                                
                        all_runs_data.extend(server_data)
                        print(f"Loaded {len(server_data)} records from {server_name}")
                        
                    except Exception as e:
                        print(f"Error reading {log_csv}: {e}")
    
    # Process runs data to group by run_uuid
    processed_runs = process_runs_data(all_runs_data)
    
    # Write processed runs to JSON
    with open('docs/data/runs.json', 'w', encoding='utf-8') as f:
        json.dump(processed_runs, f, indent=2, default=str)
    
    print(f"Processed {len(processed_runs)} unique runs from {len(all_runs_data)} total records")
    
    # Convert scenario.csv (scenario metadata)
    scenario_csv = 'report/scenario.csv'
    scenario_json = 'docs/data/scenarios.json'
    scenarios_data = convert_csv_to_json(scenario_csv, scenario_json)
    
    # Create summary stats
    summary = {
        'total_runs': len(processed_runs),
        'total_scenarios': len(scenarios_data),
        'last_updated': None,
        'status_counts': {}
    }
    
    if processed_runs:
        # Count statuses
        status_counts = {}
        latest_timestamp = None
        
        for run in processed_runs:
            status = run.get('status', 'unknown')
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