#!/usr/bin/env python3
"""
Unified FIO Analysis Tool
========================

This script analyzes both IOPS and bandwidth (bw_mean) values from FIO JSON files and generates comprehensive reports and graphs.

Features:
- Analyzes FIO JSON files from any subdirectories (not just vm-*)
- Supports both IOPS and bandwidth analysis
- Generates CSV files with performance data
- Creates bar charts, line graphs, or both
- Supports operation summary graphs with all block sizes
- Configurable block size filtering
- Customizable output directory
- X-axis machine marking for better readability
- Supports mixed read/write operations (rw, randrw)

Usage:
    python3 analyze_bw_mean_with_graphs.py [options]

Options:
    --iops                 Analyze IOPS data (iops_mean)
    --bw                   Analyze bandwidth data (bw_mean)
    --input-dir DIR        Directory containing FIO JSON files (default: current directory)
    --output-dir DIR       Directory to save output files (default: current directory)
    --graph-type TYPE      Type of graphs: bar, line, or both (default: bar)
    --block-sizes SIZES    Comma-separated block sizes to analyze (e.g., "4k,8k,128k")
    --operation-summary    Generate operation summary graphs (all block sizes combined)
    --summary-only         Generate only summary graphs (skip per-Machine comparison graphs)
    --help                 Show this help message

Examples:
    python3 analyze_bw_mean_with_graphs.py --iops
    python3 analyze_bw_mean_with_graphs.py --bw
    python3 analyze_bw_mean_with_graphs.py --iops --input-dir /path/to/data --output-dir /path/to/results
    python3 analyze_bw_mean_with_graphs.py --bw --graph-type line --block-sizes 4k,8k,128k
    python3 analyze_bw_mean_with_graphs.py --iops --operation-summary --graph-type both
    python3 analyze_bw_mean_with_graphs.py --iops --summary-only

Supported Operations:
    - read: Sequential read operations
    - write: Sequential write operations  
    - randread: Random read operations
    - randwrite: Random write operations
    - rw: Sequential mixed reads and writes (readwrite)
    - randrw: Random mixed reads and writes
"""

import json
import os
import matplotlib.pyplot as plt
import pandas as pd
import glob
import re
import argparse
import sys
import csv
from collections import defaultdict
from pathlib import Path

def ensure_csv_directory(output_dir):
    """Ensure the csv_files subdirectory exists within the output directory."""
    csv_dir = os.path.join(output_dir, 'csv_files')
    os.makedirs(csv_dir, exist_ok=True)
    return csv_dir

# Global variable to store FIO configurations for subtitles
FIO_CONFIGS = {}

def get_block_size_display_name(block_size):
    """Convert block size to display name."""
    block_size_map = {
        '4k': '4K',
        '8k': '8K', 
        '16k': '16K',
        '32k': '32K',
        '64k': '64K',
        '128k': '128K',
        '256k': '256K',
        '512k': '512K',
        '1024k': '1024K',
        '4096k': '4096K'
    }
    return block_size_map.get(block_size.lower(), block_size.upper())

def get_x_axis_labels_and_positions(df_sorted):
    """
    Determine X-axis labels and positions based on number of Machines.
    Show every Machine if <= 20, every 30th if <= 500, every 50th if > 500.
    """
    num_vms = len(df_sorted)
    
    if num_vms <= 20:
        # Show every machine for small datasets
        x_positions = range(num_vms)
        x_labels = [f'Machine {i+1}' for i in x_positions]
        return x_positions, x_labels
    elif num_vms <= 500:
        # Show every 30th machine for medium datasets
        x_positions = range(0, num_vms, 30)
        x_labels = [f'Machine {i+1}' for i in x_positions]
        return x_positions, x_labels
    elif num_vms <= 1000:
        # Show every 50th machine for large datasets (> 500 machines)
        x_positions = range(0, num_vms, 50)
        x_labels = [f'Machine {i+1}' for i in x_positions]
        return x_positions, x_labels
    else:
        # Show every 100th machine for large datasets (> 500 machines)
        x_positions = range(0, num_vms, 100)
        x_labels = [f'Machine {i+1}' for i in x_positions]
        return x_positions, x_labels
    

def extract_fio_config_from_json(json_file_path):
    """
    Extract FIO configuration from JSON file for subtitle display.
    Returns a dictionary with configuration parameters.
    """
    config_data = {}
    try:
        with open(json_file_path, 'r') as f:
            data = json.load(f)
        
        # Extract configuration from first job
        if 'jobs' in data and len(data['jobs']) > 0:
            job_options = data['jobs'][0].get('job options', {})
            config_data = {
                'size': job_options.get('size', 'N/A'),
                'bs': job_options.get('bs', 'N/A'),
                'runtime': job_options.get('runtime', 'N/A'),
                'direct': job_options.get('direct', 'N/A'),
                'numjobs': job_options.get('numjobs', 'N/A'),
                'iodepth': job_options.get('iodepth', 'N/A'),
                'rate_iops': job_options.get('rate_iops', 'N/A')
            }
    except Exception as e:
        print(f"Error extracting FIO config from {json_file_path}: {e}")
    
    return config_data

def format_fio_subtitle(config_data, exclude_bs=False):
    """
    Format FIO configuration data into a subtitle string.
    
    Args:
        config_data: Dictionary containing FIO configuration
        exclude_bs: If True, exclude block size from subtitle (for comparison graphs)
    """
    subtitle_parts = []
    
    if config_data.get('size') != 'N/A':
        subtitle_parts.append(f"Size: {config_data['size']}")
    if config_data.get('bs') != 'N/A' and not exclude_bs:
        subtitle_parts.append(f"BS: {config_data['bs']}")
    if config_data.get('runtime') != 'N/A':
        subtitle_parts.append(f"Runtime: {config_data['runtime']}s")
    if config_data.get('direct') != 'N/A':
        subtitle_parts.append(f"Direct: {config_data['direct']}")
    if config_data.get('numjobs') != 'N/A':
        subtitle_parts.append(f"NumJobs: {config_data['numjobs']}")
    if config_data.get('iodepth') != 'N/A':
        subtitle_parts.append(f"IODepth: {config_data['iodepth']}")
    if config_data.get('rate_iops') != 'N/A':
        subtitle_parts.append(f"Rate IOPS: {config_data['rate_iops']}")
    
    return " | ".join(subtitle_parts)

def extract_block_size_from_filename(filename):
    """
    Extract block size from filename (e.g., 'fio-test-read-bs-4k.json' -> '4k')
    """
    match = re.search(r'bs-(\d+[kmg]?)', filename, re.IGNORECASE)
    if match:
        return match.group(1).lower()
    return 'unknown'

def extract_iops_from_json(json_file_path):
    """
    Extract IOPS values from a FIO JSON file.
    Returns a dictionary with operation types, block sizes, and their aggregated IOPS values.
    Also stores FIO configuration data for subtitles.
    """
    global FIO_CONFIGS
    iops_data = {}
    
    try:
        with open(json_file_path, 'r') as f:
            data = json.load(f)
        
        # Check if jobs exist
        if 'jobs' not in data:
            return iops_data
        
        # Extract operation type and block size from filename
        filename = os.path.basename(json_file_path)
        block_size = extract_block_size_from_filename(filename)
        
        # Skip files with unknown block sizes
        if block_size == 'unknown':
            print(f"Skipping {filename} - unknown block size")
            return iops_data
        
        # Determine operation type from filename
        if 'randrw' in filename or 'randrw' in filename.lower():
            operation = 'randrw'
        elif 'rw' in filename or 'readwrite' in filename:
            operation = 'rw'
        elif 'randread' in filename:
            operation = 'randread'
        elif 'randwrite' in filename:
            operation = 'randwrite'
        elif 'read' in filename:
            operation = 'read'
        elif 'write' in filename:
            operation = 'write'
        else:
            return iops_data
        
        # Extract FIO configuration for subtitles
        config_data = extract_fio_config_from_json(json_file_path)
        config_key = (operation, block_size)
        FIO_CONFIGS[config_key] = config_data
        
        # Collect all IOPS values and latency data for this operation across all jobs
        iops_values = []
        latency_values = []
        
        for job in data['jobs']:
            # For mixed operations (rw, randrw), we need to handle both read and write data
            if operation in ['rw', 'randrw']:
                # For mixed operations, sum both read and write IOPS
                total_iops = 0
                total_latency = 0
                latency_count = 0
                
                # Process read data
                if 'read' in job and 'iops_mean' in job['read']:
                    read_iops = job['read']['iops_mean']
                    if read_iops > 0:
                        total_iops += read_iops
                
                # Process write data  
                if 'write' in job and 'iops_mean' in job['write']:
                    write_iops = job['write']['iops_mean']
                    if write_iops > 0:
                        total_iops += write_iops
                
                # Extract latency data from both read and write
                if 'read' in job and 'lat_ns' in job['read'] and 'mean' in job['read']['lat_ns']:
                    latency_ns = job['read']['lat_ns']['mean']
                    latency_ms = latency_ns / 1000000
                    total_latency += latency_ms
                    latency_count += 1
                
                if 'write' in job and 'lat_ns' in job['write'] and 'mean' in job['write']['lat_ns']:
                    latency_ns = job['write']['lat_ns']['mean']
                    latency_ms = latency_ns / 1000000
                    total_latency += latency_ms
                    latency_count += 1
                
                # Store combined IOPS and average latency
                if total_iops > 0:
                    iops_values.append(total_iops)
                
                if latency_count > 0:
                    avg_latency = total_latency / latency_count
                    latency_values.append(avg_latency)
            else:
                # For single operations (read, write, randread, randwrite)
                data_key = 'read' if operation in ['read', 'randread'] else 'write'
                
                if data_key in job:
                    # Extract IOPS data (only non-zero values)
                    if 'iops_mean' in job[data_key]:
                        iops_value = job[data_key]['iops_mean']
                        if iops_value > 0:  # Only include non-zero IOPS
                            iops_values.append(iops_value)
                    
                    # Extract latency data independently (regardless of IOPS value)
                    if 'lat_ns' in job[data_key] and 'mean' in job[data_key]['lat_ns']:
                        latency_ns = job[data_key]['lat_ns']['mean']
                        latency_ms = latency_ns / 1000000  # Convert nanoseconds to milliseconds
                        latency_values.append(latency_ms)
        
        # Aggregate IOPS values (sum them since they represent total jobs per machine)
        if iops_values:
            # Sum all jobs to get total IOPS per machine
            total_iops = int(sum(iops_values))
            
            # Calculate average latency across all jobs
            avg_latency_ms = sum(latency_values) / len(latency_values) if latency_values else 0
            
            # Store both IOPS and latency data
            iops_data[(operation, block_size)] = {
                'total_iops': total_iops,
                'avg_latency_ms': avg_latency_ms
            }
    
    except (json.JSONDecodeError, KeyError, FileNotFoundError) as e:
        print(f"Error processing {json_file_path}: {e}")
    
    return iops_data

def extract_bw_mean_from_json(file_path):
    """Extract bw_mean values from a JSON file, filtering out zero values."""
    global FIO_CONFIGS
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        bw_values = []
        latency_values = []

        # Extract FIO parameters for subtitle
        fio_params = {}
        
        # Determine operation type from filename (same logic as IOPS extraction)
        filename = os.path.basename(file_path)
        if 'randrw' in filename or 'randrw' in filename.lower():
            operation = 'randrw'
        elif 'rw' in filename or 'readwrite' in filename:
            operation = 'rw'
        elif 'randread' in filename:
            operation = 'randread'
        elif 'randwrite' in filename:
            operation = 'randwrite'
        elif 'read' in filename:
            operation = 'read'
        elif 'write' in filename:
            operation = 'write'
        else:
            return bw_values, {}
        
        # Look for bw_mean in the jobs section (filtering out zero values)
        if 'jobs' in data:
            for job in data['jobs']:
                # For mixed operations (rw, randrw), we need to handle both read and write data
                if operation in ['rw', 'randrw']:
                    # For mixed operations, sum both read and write bandwidth
                    total_bw = 0
                    total_latency = 0
                    latency_count = 0
                    
                    # Process read data
                    if 'read' in job and 'bw_mean' in job['read']:
                        read_bw = job['read']['bw_mean']
                        if read_bw > 0:
                            total_bw += read_bw
                    
                    # Process write data
                    if 'write' in job and 'bw_mean' in job['write']:
                        write_bw = job['write']['bw_mean']
                        if write_bw > 0:
                            total_bw += write_bw
                    
                    # Extract latency data from both read and write
                    if 'read' in job and 'lat_ns' in job['read'] and 'mean' in job['read']['lat_ns']:
                        latency_ns = job['read']['lat_ns']['mean']
                        latency_ms = latency_ns / 1000000
                        total_latency += latency_ms
                        latency_count += 1
                    
                    if 'write' in job and 'lat_ns' in job['write'] and 'mean' in job['write']['lat_ns']:
                        latency_ns = job['write']['lat_ns']['mean']
                        latency_ms = latency_ns / 1000000
                        total_latency += latency_ms
                        latency_count += 1
                    
                    # Store combined bandwidth and average latency
                    if total_bw > 0:
                        bw_values.append({
                            'operation': operation,
                            'bw_mean': int(total_bw),
                            'job_name': job.get('jobname', 'unknown')
                        })
                    
                    if latency_count > 0:
                        avg_latency = total_latency / latency_count
                        latency_values.append(avg_latency)
                else:
                    # For single operations (read, write, randread, randwrite)
                    data_key = 'read' if operation in ['read', 'randread'] else 'write'
                    
                    if data_key in job and 'bw_mean' in job[data_key]:
                        bw_mean_val = job[data_key]['bw_mean']
                        # Skip zero values
                        if bw_mean_val > 0:
                            bw_values.append({
                                'operation': operation,
                                'bw_mean': int(bw_mean_val),
                                'job_name': job.get('jobname', 'unknown')
                            })
                            
                            # Extract latency data if available
                            if 'lat_ns' in job[data_key] and 'mean' in job[data_key]['lat_ns']:
                                latency_ns = job[data_key]['lat_ns']['mean']
                                latency_ms = latency_ns / 1000000  # Convert nanoseconds to milliseconds
                                latency_values.append(latency_ms)
        
        # Also search for bw_mean in the entire JSON structure
        json_str = json.dumps(data)
        bw_mean_matches = re.findall(r'"bw_mean"\s*:\s*([0-9.]+)', json_str)
        
        if bw_mean_matches and not bw_values:
            # If we found bw_mean values but couldn't parse them properly
            for i, bw_val in enumerate(bw_mean_matches):
                try:
                    bw_val_float = float(bw_val)
                    # Skip zero values
                    if bw_val_float > 0:
                        bw_values.append({
                            'operation': 'unknown',
                            'bw_mean': int(bw_val_float),
                            'job_name': f'job_{i}'
                        })
                except ValueError:
                    continue
        

        # Extract FIO configuration for subtitles
        config_data = extract_fio_config_from_json(file_path)
        
        # Calculate average latency if we have latency data
        avg_latency_ms = sum(latency_values) / len(latency_values) if latency_values else 0
        
        # Add latency data to each bandwidth entry
        for bw_entry in bw_values:
            bw_entry['avg_latency_ms'] = avg_latency_ms
        
        return bw_values, config_data
    
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return [], {}

def parse_filename_info(filename):
    """Parse operation and block size from filename."""
    # Pattern: fio-test-{operation}-bs-{blocksize}.json
    match = re.match(r'fio-test-(\w+)-bs-(\w+)\.json', filename)
    if match:
        operation = match.group(1).lower()  # Convert to lowercase for consistency
        block_size = match.group(2).lower()  # Convert to lowercase for consistency
        
        # Normalize operation names
        if operation in ['readwrite', 'rw']:
            operation = 'rw'
        elif operation in ['randrw', 'randreadwrite']:
            operation = 'randrw'
        elif operation == 'randread':
            operation = 'randread'
        elif operation == 'randwrite':
            operation = 'randwrite'
        elif operation == 'read':
            operation = 'read'
        elif operation == 'write':
            operation = 'write'
        
        return operation, block_size
    return None, None

def analyze_all_directories(input_dir='.'):
    """Analyze all directories for JSON files and extract bw_mean values."""
    
    # Find all directories containing FIO JSON files
    test_dirs = []
    for item in os.listdir(input_dir):
        item_path = os.path.join(input_dir, item)
        if os.path.isdir(item_path):
            # Check if this directory contains FIO JSON files
            json_files = glob.glob(os.path.join(item_path, "*.json"))
            if json_files:
                test_dirs.append(item_path)
    
    test_dirs.sort()  # Sort to ensure consistent order
    
    if not test_dirs:
        print("No directories containing FIO JSON files found!")
        return {}, {}
    
    # Results storage
    results = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    all_machines_results = defaultdict(lambda: defaultdict(list))
    
    print(f"Found {len(test_dirs)} directories with FIO JSON files:")
    for test_dir in test_dirs:
        dir_name = os.path.basename(test_dir)
        json_count = len(glob.glob(os.path.join(test_dir, "*.json")))
        print(f"  - {dir_name} ({json_count} JSON files)")
    
    for directory in test_dirs:
        dir_name = os.path.basename(directory)
        print(f"\nAnalyzing directory: {dir_name}")
        
        # Find all JSON files in this directory
        json_files = glob.glob(os.path.join(directory, "*.json"))
        
        for json_file in json_files:
            filename = os.path.basename(json_file)
            operation, block_size = parse_filename_info(filename)
            
            if operation and block_size:
                print(f"  Processing: {filename} (op: {operation}, bs: {block_size})")
                
                # Extract bw_mean values
                bw_values, config_data = extract_bw_mean_from_json(json_file)
                
                # Store FIO configuration for subtitles
                config_key = (operation, block_size)
                FIO_CONFIGS[config_key] = config_data
                
                for bw_data in bw_values:
                    # Store per machine
                    results[directory][operation][block_size].append({
                        'bw_mean': bw_data['bw_mean'],
                        'job_name': bw_data['job_name'],
                        'avg_latency_ms': bw_data.get('avg_latency_ms', 0)
                    })
                    
                    # Store for all machines aggregation
                    all_machines_results[operation][block_size].append({
                        'machine': directory,
                        'bw_mean': bw_data['bw_mean'],
                        'job_name': bw_data['job_name'],
                        'avg_latency_ms': bw_data.get('avg_latency_ms', 0)
                    })
            else:

                print(f"  Skipping: {filename} (could not parse operation/block_size)")
    
    return results, all_machines_results

def calculate_statistics(values):
    """Calculate statistics for a list of values (IOPS or bandwidth)."""
    if not values:
        return {}
    
    # Check if values are IOPS (integers) or bandwidth (dictionaries)
    if isinstance(values[0], (int, float)):
        # IOPS values are direct numbers
        bw_values = [v for v in values if isinstance(v, (int, float))]
    else:
        # Bandwidth values are dictionaries with 'bw_mean' key
        bw_values = [v['bw_mean'] for v in values if isinstance(v.get('bw_mean'), (int, float))]
    
    if not bw_values:
        return {}
    
    return {
        'count': len(bw_values),
        'min': min(bw_values),
        'max': max(bw_values),
        'mean': sum(bw_values) / len(bw_values),
        'values': bw_values
    }

def generate_report(results, all_machines_results):
    """Generate a comprehensive report."""
    
    print("\n" + "="*80)
    print("BANDWIDTH MEAN ANALYSIS REPORT")
    print("="*80)
    
    # Per machine results
    print("\nPER MACHINE RESULTS:")
    print("-" * 50)
    
    # Check if results is using tuple keys (IOPS) or nested dict structure (bandwidth)
    if results and isinstance(next(iter(results.keys())), tuple):
        # IOPS results structure: {(vm_name, operation, block_size): value}
        for (vm_name, operation, block_size), value in sorted(results.items()):
            print(f"\nMachine: {vm_name}")
            print("-" * 30)
            print(f"  Operation: {operation}")
            print(f"    Block Size: {block_size}")
            if isinstance(value, dict):
                # New structure with latency data
                print(f"      Total IOPS: {value.get('total_iops', 0):.0f}")
                print(f"      Avg Latency: {value.get('avg_latency_ms', 0):.2f} ms")
            else:
                # Old structure (backward compatibility)
                print(f"      Value: {value:.2f}")
    else:
        # Bandwidth results structure: {machine: {operation: {block_size: value}}}
        for machine in sorted(results.keys()):
            print(f"\nMachine: {machine}")
            print("-" * 30)
            
            for operation in sorted(results[machine].keys()):
                print(f"  Operation: {operation}")
                for block_size in sorted(results[machine][operation].keys()):
                    stats = calculate_statistics(results[machine][operation][block_size])
                    if stats:
                        print(f"    Block Size: {block_size}")
                        print(f"      Count: {stats['count']}")
                        print(f"      Min: {stats['min']:.2f}")
                        print(f"      Max: {stats['max']:.2f}")
                        print(f"      Mean: {stats['mean']:.2f}")
                        print(f"      Values: {[f'{v:.2f}' for v in stats['values']]}")
    
    # Aggregated results across all machines
    print("\n\nAGGREGATED RESULTS (ALL MACHINES):")
    print("-" * 50)
    
    for operation in sorted(all_machines_results.keys()):
        print(f"\nOperation: {operation}")
        for block_size in sorted(all_machines_results[operation].keys()):
            stats = calculate_statistics(all_machines_results[operation][block_size])
            if stats:
                print(f"  Block Size: {block_size}")
                print(f"    Total Count: {stats['count']}")
                print(f"    Overall Min: {stats['min']:.2f}")
                print(f"    Overall Max: {stats['max']:.2f}")
                print(f"    Overall Mean: {stats['mean']:.2f}")
                
                # Show per-machine breakdown
                machine_stats = defaultdict(list)
                for item in all_machines_results[operation][block_size]:
                    if isinstance(item, (int, float)):
                        # IOPS data - direct values
                        machine_stats['all_machines'].append(item)
                    else:
                        # Bandwidth data - dictionary with machine and bw_mean
                        machine_stats[item['machine']].append(item['bw_mean'])
                
                print(f"    Per Machine:")
                for machine in sorted(machine_stats.keys()):
                    machine_mean = sum(machine_stats[machine]) / len(machine_stats[machine])
                    print(f"      {machine}: {machine_mean:.2f} (n={len(machine_stats[machine])})")

def filter_results_by_block_sizes(results, all_machines_results, selected_block_sizes):
    """Filter results to only include selected block sizes."""
    # Check if results is using tuple keys (IOPS) or nested dict structure (bandwidth)
    if results and isinstance(next(iter(results.keys())), tuple):
        # IOPS results structure: {(vm_name, operation, block_size): value}
        filtered_results = {}
        for (vm_name, operation, block_size), value in results.items():
            if block_size in selected_block_sizes:
                filtered_results[(vm_name, operation, block_size)] = value
    else:
        # Bandwidth results structure: {machine: {operation: {block_size: value}}}
        filtered_results = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        for machine in results:
            for operation in results[machine]:
                for block_size in results[machine][operation]:
                    if block_size in selected_block_sizes:
                        filtered_results[machine][operation][block_size] = results[machine][operation][block_size]
    
    # Filter all-machines results (same structure for both)
    filtered_all_machines_results = defaultdict(lambda: defaultdict(list))
    for operation in all_machines_results:
        for block_size in all_machines_results[operation]:
            if block_size in selected_block_sizes:
                filtered_all_machines_results[operation][block_size] = all_machines_results[operation][block_size]
    
    return filtered_results, filtered_all_machines_results

def write_operation_summary_csv_files(all_machines_results, selected_block_sizes=None, output_dir='.'):
    """
    Write CSV files that combine all block sizes for each operation type.
    Creates files like: summary-write-all-blocks.csv, summary-read-all-blocks.csv, etc.
    """
    # Group results by operation only (combining all block sizes)
    operation_results = {}
    
    for operation, block_data in all_machines_results.items():
        if operation not in operation_results:
            operation_results[operation] = {}
        
        for block_size, items in block_data.items():
            # Check if items are IOPS (integers) or bandwidth (dictionaries)
            if items and isinstance(items[0], (int, float)):
                # IOPS data - items are direct values (old structure), need to create generic machine names
                for i, iops_value in enumerate(items):
                    vm_name = f"machine-{i+1}"  # Create generic machine names for IOPS data
                    if vm_name not in operation_results[operation]:
                        operation_results[operation][vm_name] = {}
                    operation_results[operation][vm_name][block_size] = iops_value
            elif items and isinstance(items[0], dict) and 'total_iops' in items[0]:
                # IOPS data - new structure with latency data
                for i, iops_data in enumerate(items):
                    # Use actual machine name if available, otherwise create generic machine name
                    vm_name = iops_data.get('machine', f"machine-{i+1}")
                    if vm_name not in operation_results[operation]:
                        operation_results[operation][vm_name] = {}
                    operation_results[operation][vm_name][block_size] = iops_data.get('total_iops', 0)
            else:
                # Bandwidth data - items are dictionaries with machine and bw_mean
                # Group by machine and sum all jobs per machine
                machine_groups = {}
                for item in items:
                    machine = item['machine']
                    if machine not in machine_groups:
                        machine_groups[machine] = []
                    machine_groups[machine].append(item['bw_mean'])
                
                # Sum all jobs per machine and store the total
                for vm_name, bw_values in machine_groups.items():
                    total_bw = sum(bw_values) if bw_values else 0
                    
                    if vm_name not in operation_results[operation]:
                        operation_results[operation][vm_name] = {}
                    operation_results[operation][vm_name][block_size] = total_bw
    
    # Write CSV files for each operation
    csv_files_created = []
    csv_dir = ensure_csv_directory(output_dir)
    
    for operation, vm_data in operation_results.items():
        filename = f"summary-{operation}-all-blocks.csv"
        filepath = os.path.join(csv_dir, filename)
        
        # Get all unique block sizes for this operation
        all_block_sizes = set()
        for vm_name, block_data in vm_data.items():
            all_block_sizes.update(block_data.keys())
        all_block_sizes = sorted(all_block_sizes)
        
        # Filter block sizes if selection is specified
        if selected_block_sizes:
            all_block_sizes = [bs for bs in all_block_sizes if bs in selected_block_sizes]
            if not all_block_sizes:
                print(f"No selected block sizes found for operation {operation}, skipping...")
                continue
        
        with open(filepath, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header: vm_name, block_size_1, block_size_2, ...
            header = ['vm_name'] + all_block_sizes
            writer.writerow(header)
            
            # Sort by vm_name for consistent output
            sorted_vms = sorted(vm_data.keys())
            
            # Write data
            for vm_name in sorted_vms:
                row = [vm_name]
                for block_size in all_block_sizes:
                    bw_value = vm_data[vm_name].get(block_size, 0)  # 0 if not found
                    row.append(bw_value)
                writer.writerow(row)
        
        display_names = [get_block_size_display_name(bs) for bs in all_block_sizes]
        print(f"Created {filepath} with {len(sorted_vms)} Machines and {len(all_block_sizes)} block sizes: {', '.join(display_names)}")
        csv_files_created.append(filepath)
    
    return csv_files_created

def save_results_to_files(results, all_machines_results, output_dir='.', selected_block_sizes=None):
    """Save results to CSV files for further analysis."""
    
    # Save per-machine results
    csv_dir = ensure_csv_directory(output_dir)
    for machine in results.keys():
        filename = f"{os.path.basename(machine)}_bw_mean_results.csv"
        filepath = os.path.join(csv_dir, filename)
        with open(filepath, 'w') as f:
            f.write("Operation,BlockSize,JobName,BwMean\n")
            for operation in results[machine].keys():
                for block_size in results[machine][operation].keys():
                    for item in results[machine][operation][block_size]:
                        f.write(f"{operation},{block_size},{item['job_name']},{item['bw_mean']}\n")
        print(f"Saved per-machine results to: {filepath}")
    
    # Save aggregated results per operation
    for operation in all_machines_results.keys():
        filename = f"{operation}_all_machines_bw_mean_results.csv"
        filepath = os.path.join(csv_dir, filename)
        with open(filepath, 'w') as f:
            f.write("BlockSize,Machine,JobName,BwMean\n")
            for block_size in all_machines_results[operation].keys():
                for item in all_machines_results[operation][block_size]:
                    f.write(f"{block_size},{item['machine']},{item['job_name']},{item['bw_mean']}\n")
        print(f"Saved {operation} aggregated results to: {filepath}")
    
    # Save results per block size and operation combination
    for operation in all_machines_results.keys():
        for block_size in all_machines_results[operation].keys():
            # Skip if block size filtering is enabled and this block size is not selected
            if selected_block_sizes and block_size not in selected_block_sizes:
                continue
                
            # Create filename with block size and operation
            # Replace 'k' with 'k' and format the filename
            if block_size.endswith('k'):
                size_part = block_size
            else:
                size_part = block_size
            
            filename = f"all_machines_block_size_{size_part}_operation_{operation}.csv"
            filepath = os.path.join(csv_dir, filename)
            with open(filepath, 'w') as f:
                f.write("Machine,JobName,BwMean\n")
                for item in all_machines_results[operation][block_size]:
                    f.write(f"{item['machine']},{item['job_name']},{item['bw_mean']}\n")
            print(f"Saved block size {block_size} operation {operation} results to: {filepath}")
    
    # Also save combined results for backward compatibility
    filename = "all_machines_bw_mean_results.csv"
    filepath = os.path.join(csv_dir, filename)
    with open(filepath, 'w') as f:
        f.write("Operation,BlockSize,Machine,JobName,BwMean\n")
        for operation in all_machines_results.keys():
            for block_size in all_machines_results[operation].keys():
                for item in all_machines_results[operation][block_size]:
                    f.write(f"{operation},{block_size},{item['machine']},{item['job_name']},{item['bw_mean']}\n")
    print(f"Saved combined aggregated results to: {filepath}")
    
    # Save job-summarized results (sum of all jobs per machine)
    save_job_summarized_results(results, all_machines_results, output_dir, selected_block_sizes)

def save_job_summarized_results(results, all_machines_results, output_dir='.', selected_block_sizes=None):
    """Save results with sum of all jobs per machine, operation, and block size."""
    
    # Save per-machine job-summarized results
    csv_dir = ensure_csv_directory(output_dir)
    for machine in results.keys():
        filename = f"{os.path.basename(machine)}_bw_mean_job_summary.csv"
        filepath = os.path.join(csv_dir, filename)
        with open(filepath, 'w') as f:
            f.write("Operation,BlockSize,TotalBwMean\n")
            for operation in results[machine].keys():
                for block_size in results[machine][operation].keys():
                    items = results[machine][operation][block_size]
                    total_bw = sum(item['bw_mean'] for item in items) if items else 0
                    
                    f.write(f"{operation},{block_size},{total_bw}\n")
        print(f"Saved job-summarized results to: {filepath}")
    
    # Save all machines job-summarized results
    filename = "all_machines_bw_mean_job_summary.csv"
    filepath = os.path.join(csv_dir, filename)
    with open(filepath, 'w') as f:
        f.write("Operation,BlockSize,Machine,TotalBwMean\n")
        for operation in all_machines_results.keys():
            for block_size in all_machines_results[operation].keys():
                # Group by machine
                machine_groups = {}
                for item in all_machines_results[operation][block_size]:
                    machine = item['machine']
                    if machine not in machine_groups:
                        machine_groups[machine] = []
                    machine_groups[machine].append(item)
                
                # Calculate totals for each machine
                for machine, items in machine_groups.items():
                    total_bw = sum(item['bw_mean'] for item in items) if items else 0
                    
                    f.write(f"{operation},{block_size},{machine},{total_bw}\n")
    
    print(f"Saved all machines job-summarized results to: {filepath}")
    
    # Save block size and operation specific job-summarized results
    for operation in all_machines_results.keys():
        for block_size in all_machines_results[operation].keys():
            # Skip if block size filtering is enabled and this block size is not selected
            if selected_block_sizes and block_size not in selected_block_sizes:
                continue
                
            filename = f"all_machines_block_size_{block_size}_operation_{operation}_job_summary.csv"
            filepath = os.path.join(csv_dir, filename)
            with open(filepath, 'w') as f:
                f.write("Machine,TotalBwMean\n")
                
                # Group by machine
                machine_groups = {}
                for item in all_machines_results[operation][block_size]:
                    machine = item['machine']
                    if machine not in machine_groups:
                        machine_groups[machine] = []
                    machine_groups[machine].append(item)
                
                # Calculate totals for each machine
                for machine, items in machine_groups.items():
                    total_bw = sum(item['bw_mean'] for item in items) if items else 0
                    
                    f.write(f"{machine},{total_bw}\n")
            
            print(f"Saved job-summarized results to: {filepath}")






def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description='Unified FIO Analysis Tool - IOPS and Bandwidth Analysis',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 analyze_bw_mean_with_graphs.py --iops                    # Analyze IOPS data
  python3 analyze_bw_mean_with_graphs.py --bw                      # Analyze bandwidth data
  python3 analyze_bw_mean_with_graphs.py --iops --input-dir /path/to/data  # Analyze IOPS from specific directory
  python3 analyze_bw_mean_with_graphs.py --bw --output-dir /path/to/results  # Save bandwidth results to specific directory
  python3 analyze_bw_mean_with_graphs.py --iops --graph-type line  # Generate IOPS line graphs
  python3 analyze_bw_mean_with_graphs.py --bw --graph-type both  # Generate both bar and line bandwidth graphs
  python3 analyze_bw_mean_with_graphs.py --iops --block-sizes 4k,8k,128k  # Analyze specific block sizes for IOPS
  python3 analyze_bw_mean_with_graphs.py --bw --operation-summary  # Generate bandwidth operation summary graphs
  python3 analyze_bw_mean_with_graphs.py --iops --input-dir /data --output-dir /results --graph-type line --block-sizes 4k,8k --operation-summary  # All IOPS options
        """
    )
    
    # Analysis type selection
    parser.add_argument('--iops',
                       action='store_true',
                       help='Analyze IOPS data (iops_mean)')
    
    parser.add_argument('--bw',
                       action='store_true',
                       help='Analyze bandwidth data (bw_mean)')
    
    parser.add_argument('--input-dir',
                       type=str,
                       default='.',
                       help='Directory containing FIO JSON files in subdirectories (any name). Default: current directory')
    
    parser.add_argument('--output-dir',
                       type=str,
                       default='.',
                       help='Directory to save output files (CSV and PNG). Default: current directory')
    
    parser.add_argument('--graph-type',
                       choices=['bar', 'line', 'both'],
                       default='bar',
                       help='Type of graphs to generate (default: bar)')
    
    parser.add_argument('--block-sizes',
                       type=str,
                       help='Comma-separated list of block sizes to analyze (e.g., "4k,8k,128k")')
    
    parser.add_argument('--operation-summary',
                       action='store_true',
                       help='Generate operation summary files and graphs (all block sizes combined)')
    
    parser.add_argument('--summary-only',
                       action='store_true',
                       help='Generate only summary graphs (skip per-Machine comparison graphs)')
    
    args = parser.parse_args()
    
    # Validate input directory
    input_dir = os.path.abspath(args.input_dir)
    if not os.path.exists(input_dir):
        print(f"Error: Input directory does not exist: {input_dir}")
        sys.exit(1)
    
    # Create output directory if it doesn't exist
    output_dir = os.path.abspath(args.output_dir)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")
    else:
        print(f"Using output directory: {output_dir}")
    
    # Validate that at least one analysis type is selected
    if not args.iops and not args.bw:
        print("Error: You must specify either --iops or --bw (or both)")
        parser.print_help()
        sys.exit(1)
    
    print("=" * 60)
    print("UNIFIED FIO ANALYSIS TOOL")
    print("=" * 60)
    print(f"Using input directory: {input_dir}")
    print(f"Using output directory: {output_dir}")
    
    # Parse selected block sizes if provided
    selected_block_sizes = None
    if args.block_sizes:
        selected_block_sizes = [bs.strip().lower() for bs in args.block_sizes.split(',')]
        display_selected = [get_block_size_display_name(bs) for bs in selected_block_sizes]
        print(f"Selected block sizes for analysis: {', '.join(display_selected)}")
    
    # Handle IOPS analysis
    if args.iops:
        print("\n" + "=" * 60)
        print("IOPS ANALYSIS")
        print("=" * 60)
        
        # Analyze all directories for IOPS
        results, all_machines_results = analyze_all_directories_iops(input_dir)
        
        if not results:
            print("No IOPS data found to analyze.")
        else:
            # Filter results to only include selected block sizes
            if selected_block_sizes:
                results, all_machines_results = filter_results_by_block_sizes(results, all_machines_results, selected_block_sizes)
                
                if not results:
                    print("No IOPS data found for the selected block sizes.")
                else:
                    # Generate report
                    generate_report(results, all_machines_results)
                    
                    # Save results to files
                    save_results_to_files_iops(results, all_machines_results, output_dir, selected_block_sizes)
                    
                    # Save job summarized results
                    save_job_summarized_results_iops(results, all_machines_results, output_dir, selected_block_sizes)
                    
                    # Create graphs from job summaries
                    if args.summary_only:
                        print(f"\nCreating {args.graph_type} summary graphs only (per-Machine graphs skipped)...")
                    else:
                        print(f"\nCreating {args.graph_type} graphs...")
                    create_graphs_from_job_summaries(output_dir, args.graph_type, args.summary_only, 'iops')
                    
                    # Create latency correlation graph
                    print(f"\nCreating latency correlation graph...")
                    create_latency_performance_correlation_graph(all_machines_results, output_dir, 'iops')
                    
                    # Save latency data to files
                    print(f"\nSaving latency data to files...")
                    save_latency_data_to_files(all_machines_results, output_dir, 'iops')
                    
                    # Save job summarized results
                    save_job_summarized_results_iops(results, all_machines_results, output_dir, selected_block_sizes)
                    
                    # Create graphs from job summaries
                    if args.summary_only:
                        print(f"\nCreating {args.graph_type} summary graphs only (per-Machine graphs skipped)...")
                    else:
                        print(f"\nCreating {args.graph_type} graphs...")
                    create_graphs_from_job_summaries(output_dir, args.graph_type, args.summary_only, 'iops')
            else:
                # Generate report
                generate_report(results, all_machines_results)
                
                # Save results to files
                save_results_to_files_iops(results, all_machines_results, output_dir, selected_block_sizes)
                
                # Create latency correlation graph
                print(f"\nCreating latency correlation graph...")
                create_latency_performance_correlation_graph(all_machines_results, output_dir, 'iops')
                
                # Save latency data to files
                print(f"\nSaving latency data to files...")
                save_latency_data_to_files(all_machines_results, output_dir, 'iops')
                
                # Save job summarized results
                save_job_summarized_results_iops(results, all_machines_results, output_dir, selected_block_sizes)
                
                # Create graphs from job summaries
                if args.summary_only:
                    print(f"\nCreating {args.graph_type} summary graphs only (per-Machine graphs skipped)...")
                else:
                    print(f"\nCreating {args.graph_type} graphs...")
                create_graphs_from_job_summaries(output_dir, args.graph_type, args.summary_only, 'iops')
    
    # Handle bandwidth analysis
    if args.bw:
        print("\n" + "=" * 60)
        print("BANDWIDTH ANALYSIS")
        print("=" * 60)
        
        # Analyze all directories for bandwidth
        results, all_machines_results = analyze_all_directories(input_dir)
        
        if not results:
            print("No bandwidth data found to analyze.")
        else:
            # Filter results to only include selected block sizes
            if selected_block_sizes:
                results, all_machines_results = filter_results_by_block_sizes(results, all_machines_results, selected_block_sizes)
                
                if not results:
                    print("No bandwidth data found for the selected block sizes.")
                else:
                    # Generate report
                    generate_report(results, all_machines_results)
                    
                    # Save results to files
                    save_results_to_files(results, all_machines_results, output_dir, selected_block_sizes)
                    
                    # Save job summarized results
                    save_job_summarized_results(results, all_machines_results, output_dir, selected_block_sizes)
                    
                    # Create graphs from job summaries
                    if args.summary_only:
                        print(f"\nCreating {args.graph_type} summary graphs only (per-Machine graphs skipped)...")
                    else:
                        print(f"\nCreating {args.graph_type} graphs...")
                    create_graphs_from_job_summaries(output_dir, args.graph_type, args.summary_only, 'bandwidth')
                    
                    # Create latency correlation graph
                    print(f"\nCreating latency correlation graph...")
                    create_latency_performance_correlation_graph(all_machines_results, output_dir, 'bandwidth')
                    
                    # Save latency data to files
                    print(f"\nSaving latency data to files...")
                    save_latency_data_to_files(all_machines_results, output_dir, 'bandwidth')
            else:
                # Generate report
                generate_report(results, all_machines_results)
                
                # Save results to files
                save_results_to_files(results, all_machines_results, output_dir, selected_block_sizes)
                
                # Create latency correlation graph
                print(f"\nCreating latency correlation graph...")
                create_latency_performance_correlation_graph(all_machines_results, output_dir, 'bandwidth')
                
                # Save latency data to files
                print(f"\nSaving latency data to files...")
                save_latency_data_to_files(all_machines_results, output_dir, 'bandwidth')
                
                # Save job summarized results
                save_job_summarized_results(results, all_machines_results, output_dir, selected_block_sizes)
                
                # Create graphs from job summaries
                if args.summary_only:
                    print(f"\nCreating {args.graph_type} summary graphs only (per-Machine graphs skipped)...")
                else:
                    print(f"\nCreating {args.graph_type} graphs...")
                create_graphs_from_job_summaries(output_dir, args.graph_type, args.summary_only, 'iops')
    
    # Generate operation summary files and graphs (if requested)
    if args.operation_summary:
        if args.iops:
            print(f"\nGenerating IOPS operation summary files...")
            print("-" * 50)
            # Get IOPS results for operation summary
            iops_results, iops_all_machines_results = analyze_all_directories_iops(input_dir)
            if selected_block_sizes:
                iops_results, iops_all_machines_results = filter_results_by_block_sizes(iops_results, iops_all_machines_results, selected_block_sizes)
            
            if iops_all_machines_results:
                operation_summary_files = write_operation_summary_csv_files(iops_all_machines_results, selected_block_sizes, output_dir)
                
                if operation_summary_files:
                    print(f"\nGenerating IOPS operation summary graphs...")
                    print("-" * 50)
                    summary_success_count = create_operation_summary_graphs(operation_summary_files, args.graph_type, output_dir, 'iops')
                    print(f"\nSuccessfully created {summary_success_count} IOPS operation summary graphs")
        
        if args.bw:
            print(f"\nGenerating bandwidth operation summary files...")
            print("-" * 50)
            # Get bandwidth results for operation summary
            bw_results, bw_all_machines_results = analyze_all_directories(input_dir)
            if selected_block_sizes:
                bw_results, bw_all_machines_results = filter_results_by_block_sizes(bw_results, bw_all_machines_results, selected_block_sizes)
            
            if bw_all_machines_results:
                operation_summary_files = write_operation_summary_csv_files(bw_all_machines_results, selected_block_sizes, output_dir)
                
                if operation_summary_files:
                    print(f"\nGenerating bandwidth operation summary graphs...")
                    print("-" * 50)
                    summary_success_count = create_operation_summary_graphs(operation_summary_files, args.graph_type, output_dir, 'bandwidth')
                    print(f"\nSuccessfully created {summary_success_count} bandwidth operation summary graphs")
        
        # List generated operation summary PNG files
        import glob
        summary_png_files = glob.glob(os.path.join(output_dir, "summary-*-all-blocks_comparison-*.png"))
        if summary_png_files:
            print(f"\nGenerated operation summary PNG files:")
            for png_file in sorted(summary_png_files):
                print(f"  - {os.path.basename(png_file)}")
    
    print("\nAnalysis complete!")

# IOPS Processing Functions
def process_vm_directory_iops(vm_dir):
    """
    Process all JSON files in a directory for IOPS data.
    Returns a dictionary with (operation, block_size) as key and iops as value.
    """
    results = {}
    vm_name = os.path.basename(vm_dir)
    
    # Find all JSON files in the directory
    json_files = glob.glob(os.path.join(vm_dir, "*.json"))
    
    for json_file in json_files:
        iops_data = extract_iops_from_json(json_file)
        
        for (operation, block_size), iops in iops_data.items():
            key = (vm_name, operation, block_size)
            results[key] = iops
    
    return results

def analyze_all_directories_iops(input_dir='.'):
    """Analyze all directories for JSON files and extract IOPS values."""
    
    # Find all directories containing FIO JSON files
    test_dirs = []
    for item in os.listdir(input_dir):
        item_path = os.path.join(input_dir, item)
        if os.path.isdir(item_path):
            # Check if this directory contains FIO JSON files
            json_files = glob.glob(os.path.join(item_path, "*.json"))
            if json_files:
                test_dirs.append(item_path)
    
    if not test_dirs:
        print(f"No directories with JSON files found in {input_dir}")
        return {}, {}
    
    print(f"Found {len(test_dirs)} directories with FIO JSON files:")
    for test_dir in test_dirs:
        print(f"  - {os.path.basename(test_dir)}")
    
    # Process each directory
    all_results = {}
    all_machines_results = defaultdict(lambda: defaultdict(list))
    
    for test_dir in test_dirs:
        print(f"\nAnalyzing directory: {os.path.basename(test_dir)}")
        vm_results = process_vm_directory_iops(test_dir)
        all_results.update(vm_results)
        
        # Group by operation and block size for aggregated results
        for (vm_name, operation, block_size), iops in vm_results.items():
            # Store with machine name for latency correlation graphs
            all_machines_results[operation][block_size].append({
                'machine': vm_name,
                'total_iops': iops['total_iops'],
                'avg_latency_ms': iops['avg_latency_ms']
            })
            print(f"  Processing: {vm_name} (op: {operation}, bs: {block_size})")
    
    return all_results, all_machines_results

def save_results_to_files_iops(results, all_machines_results, output_dir='.', selected_block_sizes=None):
    """Save IOPS results to CSV files."""
    
    csv_dir = ensure_csv_directory(output_dir)
    
    # Filter results by selected block sizes if specified
    if selected_block_sizes:
        filtered_results = {}
        for (vm_name, operation, block_size), iops in results.items():
            if block_size in selected_block_sizes:
                filtered_results[(vm_name, operation, block_size)] = iops
        results = filtered_results
        
        # Also filter all_machines_results
        filtered_all_machines = defaultdict(lambda: defaultdict(list))
        for operation, block_sizes in all_machines_results.items():
            for block_size, iops_list in block_sizes.items():
                if block_size in selected_block_sizes:
                    filtered_all_machines[operation][block_size] = iops_list
        all_machines_results = filtered_all_machines
    
    # Save per-machine results
    # Group results by Machine name
    vm_results = defaultdict(lambda: defaultdict(dict))
    for (vm_name, operation, block_size), iops in results.items():
        vm_results[vm_name][operation][block_size] = iops
    
    for vm_name in vm_results.keys():
        filename = f"{vm_name}_iops_results.csv"
        filepath = os.path.join(csv_dir, filename)
        
        with open(filepath, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Operation', 'BlockSize', 'IOPS'])
            
            for operation in sorted(vm_results[vm_name].keys()):
                for block_size in sorted(vm_results[vm_name][operation].keys()):
                    iops_data = vm_results[vm_name][operation][block_size]
                    if isinstance(iops_data, dict):
                        # New structure with latency data
                        writer.writerow([operation, block_size, iops_data.get('total_iops', 0)])
                    else:
                        # Old structure (backward compatibility)
                        writer.writerow([operation, block_size, iops_data])
        
        print(f"Saved per-machine results to: {filepath}")
    
    # Save aggregated results by operation
    for operation, block_sizes in all_machines_results.items():
        filename = f"{operation}_all_machines_iops_results.csv"
        filepath = os.path.join(csv_dir, filename)
        
        with open(filepath, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['BlockSize', 'TotalIOPS', 'MinIOPS', 'MaxIOPS', 'MeanIOPS'])
            
            for block_size, iops_list in block_sizes.items():
                # Extract IOPS values from the new data structure
                iops_values = []
                for iops_data in iops_list:
                    if isinstance(iops_data, dict):
                        # New structure with latency data
                        iops_values.append(iops_data.get('total_iops', 0))
                    else:
                        # Old structure (backward compatibility)
                        iops_values.append(iops_data)
                
                if iops_values:
                    total_iops = sum(iops_values)
                    min_iops = min(iops_values)
                    max_iops = max(iops_values)
                    mean_iops = sum(iops_values) / len(iops_values)
                    writer.writerow([block_size, total_iops, min_iops, max_iops, mean_iops])
        
        print(f"Saved {operation} aggregated results to: {filepath}")
    
    # Save combined aggregated results
    filename = "all_machines_iops_results.csv"
    filepath = os.path.join(csv_dir, filename)
    
    with open(filepath, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Operation', 'BlockSize', 'TotalIOPS', 'MinIOPS', 'MaxIOPS', 'MeanIOPS'])
        
        for operation, block_sizes in all_machines_results.items():
            for block_size, iops_list in block_sizes.items():
                # Extract IOPS values from the new data structure
                iops_values = []
                for iops_data in iops_list:
                    if isinstance(iops_data, dict):
                        # New structure with latency data
                        iops_values.append(iops_data.get('total_iops', 0))
                    else:
                        # Old structure (backward compatibility)
                        iops_values.append(iops_data)
                
                if iops_values:
                    total_iops = sum(iops_values)
                    min_iops = min(iops_values)
                    max_iops = max(iops_values)
                    mean_iops = sum(iops_values) / len(iops_values)
                    writer.writerow([operation, block_size, total_iops, min_iops, max_iops, mean_iops])
    
    print(f"Saved combined aggregated results to: {filepath}")

def save_job_summarized_results_iops(results, all_machines_results, output_dir='.', selected_block_sizes=None):
    """Save IOPS job summarized results to CSV files."""
    
    csv_dir = ensure_csv_directory(output_dir)
    
    # Filter results by selected block sizes if specified
    if selected_block_sizes:
        filtered_results = {}
        for (vm_name, operation, block_size), iops in results.items():
            if block_size in selected_block_sizes:
                filtered_results[(vm_name, operation, block_size)] = iops
        results = filtered_results
        
        # Also filter all_machines_results
        filtered_all_machines = defaultdict(lambda: defaultdict(list))
        for operation, block_sizes in all_machines_results.items():
            for block_size, iops_list in block_sizes.items():
                if block_size in selected_block_sizes:
                    filtered_all_machines[operation][block_size] = iops_list
        all_machines_results = filtered_all_machines
    
    # Save per-machine job summarized results
    # Group results by Machine name
    vm_results = defaultdict(lambda: defaultdict(dict))
    for (vm_name, operation, block_size), iops in results.items():
        vm_results[vm_name][operation][block_size] = iops
    
    for vm_name in vm_results.keys():
        filename = f"{vm_name}_iops_job_summary.csv"
        filepath = os.path.join(csv_dir, filename)
        
        with open(filepath, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Operation', 'BlockSize', 'TotalIOPS'])
            
            for operation in sorted(vm_results[vm_name].keys()):
                for block_size in sorted(vm_results[vm_name][operation].keys()):
                    iops_data = vm_results[vm_name][operation][block_size]
                    if isinstance(iops_data, dict):
                        # New structure with latency data
                        writer.writerow([operation, block_size, iops_data.get('total_iops', 0)])
                    else:
                        # Old structure (backward compatibility)
                        writer.writerow([operation, block_size, iops_data])
        
        print(f"Saved job-summarized results to: {filepath}")
    
    # Save all machines job summarized results
    filename = "all_machines_iops_job_summary.csv"
    filepath = os.path.join(csv_dir, filename)
    
    with open(filepath, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Machine', 'TotalIOPS'])
        
        for (vm_name, operation, block_size), iops in results.items():
            if isinstance(iops, dict):
                # New structure with latency data
                writer.writerow([vm_name, iops.get('total_iops', 0)])
            else:
                # Old structure (backward compatibility)
                writer.writerow([vm_name, iops])
    
    print(f"Saved all machines job-summarized results to: {filepath}")
    
    # Save block size + operation job summarized results
    for operation, block_sizes in all_machines_results.items():
        for block_size, iops_list in block_sizes.items():
            filename = f"all_machines_block_size_{block_size}_operation_{operation}_iops_job_summary.csv"
            filepath = os.path.join(csv_dir, filename)
            
            with open(filepath, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['Machine', 'TotalIOPS'])
                
                for i, iops in enumerate(iops_list):
                    if isinstance(iops, dict):
                        # New structure with latency data
                        machine_name = iops.get('machine', f"machine-{i+1}")
                        writer.writerow([machine_name, iops.get('total_iops', 0)])
                    else:
                        # Old structure (backward compatibility)
                        writer.writerow([f"machine-{i+1}", iops])
            
            print(f"Saved job-summarized results to: {filepath}")

def create_graphs_from_job_summaries(output_dir='.', graph_type='bar', summary_only=False, data_type='bandwidth'):
    """
    Create graphs from block size + operation job summary CSV files.
    
    Args:
        output_dir: Directory to save output files
        graph_type: Type of graphs ('bar', 'line', or 'both')
        summary_only: If True, skip per-Machine graphs and only create summary graphs
    """
    try:
        import matplotlib.pyplot as plt
        import pandas as pd
        
        # Set matplotlib to use a non-interactive backend
        plt.switch_backend('Agg')
        
        # Find all job summary CSV files in the csv_files subdirectory
        csv_dir = ensure_csv_directory(output_dir)
        if data_type == 'iops':
            job_summary_files = glob.glob(os.path.join(csv_dir, "all_machines_block_size_*_operation_*_iops_job_summary.csv"))
        else:
            job_summary_files = glob.glob(os.path.join(csv_dir, "all_machines_block_size_*_operation_*_job_summary.csv"))
        
        # Filter files based on data type by checking the column names
        filtered_files = []
        for file_path in job_summary_files:
            try:
                df = pd.read_csv(file_path)
                if data_type == 'iops' and 'TotalIOPS' in df.columns:
                    filtered_files.append(file_path)
                elif data_type == 'bandwidth' and 'TotalBwMean' in df.columns:
                    filtered_files.append(file_path)
                elif data_type == 'iops' and 'TotalBwMean' in df.columns:
                    # For IOPS graphs, we can use bandwidth files and convert them
                    # Create a temporary IOPS file
                    df_iops = df.copy()
                    df_iops['TotalIOPS'] = df_iops['TotalBwMean']  # Use bandwidth data as IOPS
                    df_iops = df_iops.drop('TotalBwMean', axis=1)
                    
                    # Save temporary IOPS file
                    temp_file = file_path.replace('_job_summary.csv', '_temp_iops_job_summary.csv')
                    df_iops.to_csv(temp_file, index=False)
                    filtered_files.append(temp_file)
            except Exception as e:
                print(f"Error reading {file_path}: {e}")
                continue
        
        job_summary_files = filtered_files
        
        if not job_summary_files:
            print("No block size + operation job summary files found to create graphs from.")
            return
        
        if summary_only:
            print(f"\nSkipping per-Machine graphs (summary-only mode enabled)")
            return
        
        print(f"\nCreating {graph_type} graphs from {len(job_summary_files)} block size + operation job summary files...")
        
        if graph_type == 'both':
            # Generate both bar and line graphs
            for graph_subtype in ['bar', 'line']:
                print(f"Creating {graph_subtype} graphs...")
                for csv_file in job_summary_files:
                    try:
                        create_single_graph(csv_file, graph_subtype, output_dir)
                    except Exception as e:
                        print(f"Error creating {graph_subtype} graph for {csv_file}: {e}")
                        continue
        else:
            # Generate single graph type
            for csv_file in job_summary_files:
                try:
                    create_single_graph(csv_file, graph_type, output_dir)
                except Exception as e:
                    print(f"Error creating {graph_type} graph for {csv_file}: {e}")
                    continue
        
        print(f"\nGraph creation complete! Created {graph_type} graphs.")
        
        # Clean up temporary files if any were created
        for csv_file in job_summary_files:
            if '_temp_iops_job_summary.csv' in csv_file:
                try:
                    os.remove(csv_file)
                    print(f"Cleaned up temporary file: {csv_file}")
                except Exception as e:
                    print(f"Error cleaning up temporary file {csv_file}: {e}")
        
    except ImportError as e:
        print(f"Error importing required libraries: {e}")
        print("Please install required dependencies: pip install matplotlib pandas")
    except Exception as e:
        print(f"Error in graph creation: {e}")


def extract_latency_data_for_graph(operation, block_size, output_dir, data_type='iops'):
    """
    Extract average latency data for a specific operation and block size.
    
    Args:
        operation: Operation name (e.g., 'read', 'write', 'randread', 'randwrite')
        block_size: Block size (e.g., '4k', '8k', '128k')
        output_dir: Output directory where latencydata is stored
        data_type: Data type ('iops' or 'bandwidth')
    
    Returns:
        Dictionary with machine names as keys and average latency as values
    """
    try:
        import os
        import glob
        
        # Construct the latency file path
        data_type_suffix = 'bw' if data_type == 'bandwidth' else 'iops'
        # Convert block size to lowercase for file naming consistency
        block_size_lower = block_size.lower()
        
        # Search for latency files in multiple possible locations
        possible_paths = [
            # Original path
            os.path.join(output_dir, 'latencydata', operation, f"{block_size_lower}_{data_type_suffix}.txt"),
            # Path in output-dir-results_lat
            os.path.join(output_dir, 'output-dir-results_lat', '*', 'latencydata', operation, f"{block_size_lower}_{data_type_suffix}.txt"),
            # Path in double_graphs_outdir
            os.path.join(output_dir, 'double_graphs_outdir', '*', 'latencydata', operation, f"{block_size_lower}_{data_type_suffix}.txt"),
            # Path in results_to_analyze
            os.path.join(output_dir, 'results_to_analyze', '*', 'latencydata', operation, f"{block_size_lower}_{data_type_suffix}.txt"),
            # Path in output-dir-results
            os.path.join(output_dir, 'output-dir-results', '*', 'latencydata', operation, f"{block_size_lower}_{data_type_suffix}.txt"),
        ]
        
        latency_file = None
        for path_pattern in possible_paths:
            if '*' in path_pattern:
                # Use glob to find matching files
                matching_files = glob.glob(path_pattern)
                if matching_files:
                    latency_file = matching_files[0]  # Use the first match
                    break
            else:
                # Direct path
                if os.path.exists(path_pattern):
                    latency_file = path_pattern
                    break
        
        if not latency_file or not os.path.exists(latency_file):
            print(f"Latency file not found for {operation} {block_size} {data_type_suffix}. Searched paths:")
            for path in possible_paths:
                print(f"  - {path}")
            return {}
        
        print(f"Found latency file: {latency_file}")
        latency_data = {}
        
        with open(latency_file, 'r') as f:
            lines = f.readlines()
            
        machine_name = None
        for line in lines:
            if line.startswith('Machine:'):
                # Extract machine name (remove 'Machine: ' prefix)
                machine_name = line.replace('Machine:', '').strip()
            elif line.startswith('Average Latency:') and machine_name:
                # Extract latency value (remove 'Average Latency: ' prefix and ' ms' suffix)
                latency_str = line.replace('Average Latency:', '').replace('ms', '').strip()
                try:
                    latency_value = float(latency_str)
                    latency_data[machine_name] = latency_value
                except ValueError:
                    continue
                machine_name = None  # Reset for next machine
        
        print(f"Extracted latency data for {len(latency_data)} machines")
        return latency_data
        
    except Exception as e:
        print(f"Error extracting latency data: {e}")
        return {}


def create_single_graph(csv_file, graph_type, output_dir):
    """Create a single graph from a CSV file with dual-axis for latency data."""
    try:
        # Read the CSV file
        df = pd.read_csv(csv_file)
        
        # Skip if file doesn't have the expected columns
        if 'Machine' not in df.columns:
            print(f"Skipping {csv_file}: Missing Machine column")
            return
        
        # Determine if this is IOPS or bandwidth data
        if 'TotalIOPS' in df.columns:
            data_column = 'TotalIOPS'
            data_type = 'IOPS'
            y_label = 'Total IOPS per Machine (sum of all jobs)'
            data_type_for_latency = 'iops'
        elif 'TotalBwMean' in df.columns:
            data_column = 'TotalBwMean'
            data_type = 'Bandwidth'
            y_label = 'Total bw_mean per Machine (sum of all jobs) [KB]'
            data_type_for_latency = 'iops'  # Use IOPS latency data since it's the same source
        else:
            print(f"Skipping {csv_file}: Missing TotalIOPS or TotalBwMean column")
            return
        
        # Extract block size and operation from filename for latency data
        operation = None
        block_size = None
        latency_data = {}
        
        if 'block_size_' in csv_file and '_operation_' in csv_file:
            filename = os.path.basename(csv_file)
            parts = filename.replace('_job_summary.csv', '').split('_')
            if len(parts) >= 7:
                block_size = parts[4]  # 4k
                operation = parts[6]   # read
                
                # Extract latency data for this operation and block size
                latency_data = extract_latency_data_for_graph(operation, block_size, output_dir, data_type_for_latency)
        
        # Create the plot with dual axes
        fig, ax1 = plt.subplots(figsize=(12, 6))
        
        # Create numeric x-axis positions for all data points
        machines = df['Machine'].tolist()
        total_data = df[data_column].tolist()
        all_positions = range(len(machines))
        
        # Get X-axis labels and positions based on number of Machines
        x_positions, x_labels = get_x_axis_labels_and_positions(df)
        
        # Plot primary data (IOPS or Bandwidth)
        if graph_type == 'bar':
            bars = ax1.bar(all_positions, total_data, color='skyblue', edgecolor='navy', alpha=0.7)
        else:  # line graph
            ax1.plot(all_positions, total_data, 
                    marker='o', linewidth=3, markersize=4, 
                    color='steelblue', markerfacecolor='lightblue', 
                    markeredgecolor='navy', markeredgewidth=2)

        # Set x-axis ticks based on visibility rules
        ax1.set_xticks(x_positions)
        ax1.set_xticklabels(x_labels, rotation=45, ha='right')
        
        # Calculate average (Total / number of machines)
        total_data_sum = sum(total_data)
        num_machines = len(machines)
        average_data = total_data_sum / num_machines if num_machines > 0 else 0
        
        # Add horizontal line for average
        if data_type == 'IOPS':
            ax1.axhline(y=average_data, color='red', linestyle='--', linewidth=2, alpha=0.8, 
                       label=f'Average: {average_data:.1f} IOPS')
        else:
            ax1.axhline(y=average_data, color='red', linestyle='--', linewidth=2, alpha=0.8, 
                       label=f'Average: {average_data:.1f} KB')
                
        # Customize the primary axis
        ax1.set_ylabel(y_label, fontsize=10, fontweight='bold', color='blue')
        ax1.set_xlabel('Machine Index', fontsize=10, fontweight='bold')
        ax1.tick_params(axis='y', labelcolor='blue')
        
        # Add latency data on secondary axis if available
        if latency_data and len(latency_data) > 0:
            # Create secondary y-axis for latency
            ax2 = ax1.twinx()
            
            # Match latency data to machines in the same order
            latency_values = []
            for machine in machines:
                # Try to find matching latency data (handle different machine name formats)
                found_latency = None
                # Extract machine name from full path (e.g., /path/to/machine-name -> machine-name)
                machine_basename = os.path.basename(machine)
                
                # Try exact match first (case-insensitive)
                for lat_machine, lat_value in latency_data.items():
                    if machine_basename.lower() == lat_machine.lower():
                        found_latency = lat_value
                        break
                
                # If no exact match, try partial matching (case-insensitive)
                if found_latency is None:
                    for lat_machine, lat_value in latency_data.items():
                        # Check if machine name contains latency machine name or vice versa
                        if (lat_machine.lower() in machine_basename.lower() or 
                            machine_basename.lower() in lat_machine.lower()):
                            found_latency = lat_value
                            break
                
                # If still no match, try path-based matching
                if found_latency is None:
                    for lat_machine, lat_value in latency_data.items():
                        # Check if machine path ends with latency machine name
                        if machine.rstrip('/').endswith('/' + lat_machine):
                            found_latency = lat_value
                            break
                
                if found_latency is not None:
                    latency_values.append(found_latency)
                else:
                    latency_values.append(0)  # Default to 0 if no latency data found
            
            # Plot latency data
            if graph_type == 'bar':
                # For bar charts, show latency as a line overlay
                ax2.plot(all_positions, latency_values, 
                        marker='s', linewidth=2, markersize=3, 
                        color='orange', markerfacecolor='orange', 
                        markeredgecolor='darkorange', markeredgewidth=1,
                        label='Average Latency (ms)')
            else:  # line graph
                ax2.plot(all_positions, latency_values, 
                        marker='s', linewidth=2, markersize=3, 
                        color='orange', markerfacecolor='orange', 
                        markeredgecolor='darkorange', markeredgewidth=1,
                        label='Average Latency (ms)')
            
            # Calculate average latency
            avg_latency = sum(latency_values) / len(latency_values) if latency_values else 0
            ax2.axhline(y=avg_latency, color='darkorange', linestyle=':', linewidth=2, alpha=0.8,
                       label=f'Avg Latency: {avg_latency:.2f} ms')
            
            # Customize secondary axis
            ax2.set_ylabel('Average Latency (ms)', fontsize=10, fontweight='bold', color='orange')
            ax2.tick_params(axis='y', labelcolor='orange')
            
            # Set latency axis limits
            if latency_values:
                ax2.set_ylim(0, max(latency_values) * 1.1)
        
        # Extract block size and operation from filename for title
        if 'block_size_' in csv_file and '_operation_' in csv_file:
            filename = os.path.basename(csv_file)
            parts = filename.replace('_job_summary.csv', '').split('_')
            if len(parts) >= 7:
                block_size = parts[4]  # 4k
                operation = parts[6]   # read
                
                # Get FIO configuration for subtitle
                config_key = (operation, block_size)
                subtitle = ""
                if config_key in FIO_CONFIGS:
                    subtitle = format_fio_subtitle(FIO_CONFIGS[config_key])
                
                # Customize the plot
                num_machines = len(df)
                chart_type = "Bar Chart" if graph_type == 'bar' else "Line Chart"
                plt.title(f'{data_type} Performance ({chart_type}): {operation.upper()} - {block_size.upper()} Block Size ({num_machines} Machines)', 
                         fontsize=16, fontweight='bold', pad=30)
                
                # Add subtitle with FIO configuration
                if subtitle:
                    # Position subtitle in the center of the graph area (not the entire figure)
                    # Since we have right=0.65, the graph area is from 0 to 0.65, so center is at 0.325
                    plt.figtext(0.325, 0.92, subtitle, fontsize=10, ha='center', va='top')
                else:
                    plt.title(csv_file.replace('_job_summary.csv', ''), fontsize=14, fontweight='bold')
        else:
            # For per-machine files like: vm-1_bw_mean_job_summary.csv
            plt.title(csv_file.replace('_bw_mean_job_summary.csv', ''), fontsize=14, fontweight='bold')

        # Set axis limits to include zero
        ax1.set_xlim(-0.5, len(machines) - 0.5)
        ax1.set_ylim(0, max(total_data) * 1.1)
        
        # Customize grid and layout
        if graph_type == 'bar':
            ax1.grid(axis='y', alpha=0.3, linestyle='--')
        else:  # line graph
            ax1.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
        
        # Add legend for both axes (positioned outside the graph area)
        lines1, labels1 = ax1.get_legend_handles_labels()
        if latency_data and len(latency_data) > 0:
            lines2, labels2 = ax2.get_legend_handles_labels()
            ax1.legend(lines1 + lines2, labels1 + labels2, loc='center left', bbox_to_anchor=(1.15, 0.5), fontsize=9)
        else:
            ax1.legend(loc='center left', bbox_to_anchor=(1.15, 0.5), fontsize=9)
        
        # Adjust layout to accommodate legend outside the graph
        plt.tight_layout()
        plt.subplots_adjust(right=0.65)  # Make more room for the legend on the right
                
        # Create output filename with data type included
        csv_basename = os.path.basename(csv_file)
        data_type_suffix = 'bw' if data_type == 'Bandwidth' else 'iops'
        if graph_type == 'bar':
            output_filename = csv_basename.replace('.csv', f'_{data_type_suffix}.png')
        else:  # line graph
            output_filename = csv_basename.replace('.csv', f'_{graph_type}_{data_type_suffix}.png')
        output_file = os.path.join(output_dir, output_filename)
                
        # Save the plot
        plt.savefig(output_file, dpi=300, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        plt.close()
                
        print(f"Created {graph_type} graph: {output_file}")
        return True
        
    except Exception as e:
        print(f"Error creating {graph_type} graph for {csv_file}: {e}")
        return False


def create_operation_summary_graphs(csv_files, graph_type='bar', output_dir='.', data_type='bandwidth'):
    """
    Create graphs for operation summary CSV files (all block sizes combined).
    Supports both bar and line graphs with individual block size averages.
    
    Args:
        csv_files: List of CSV files to process
        graph_type: Type of graph ('bar', 'line', or 'both')
        output_dir: Directory to save output files
        data_type: Type of data ('iops' or 'bandwidth')
    """
    try:
        import matplotlib.pyplot as plt
        import pandas as pd
        import numpy as np
        import re
        
        # Set matplotlib to use a non-interactive backend
        import matplotlib
        matplotlib.use('Agg')
        
        success_count = 0
        
        # Handle 'both' option by creating both bar and line graphs
        graph_types = ['bar', 'line'] if graph_type == 'both' else [graph_type]
        
        for csv_file in csv_files:
            for current_graph_type in graph_types:
                try:
                    # Read CSV file
                    df = pd.read_csv(csv_file)
                    
                    # Extract operation from filename
                    filename = os.path.basename(csv_file)
                    match = re.search(r'summary-(\w+)-all-blocks\.csv', filename)
                    if not match:
                        continue
                    operation = match.group(1)
                    
                    # Get block sizes (all columns except vm_name)
                    block_sizes = [col for col in df.columns if col != 'vm_name']
                    
                    # Create the plot with dual axes - increased size for better spacing
                    fig, ax1 = plt.subplots(figsize=(20, 12))
                    
                    # Sort by vm_name for consistent ordering
                    df_sorted = df.sort_values('vm_name')
                    
                    # Get X-axis labels and positions based on number of Machines
                    x_positions, x_labels = get_x_axis_labels_and_positions(df_sorted)
                    
                    # Create numeric x-axis positions for all data points
                    all_positions = range(len(df_sorted))
                    
                    # Create plot based on graph type - use more readable colors
                    # Define a set of colors that provide good contrast and readability
                    readable_colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']
                    colors = [readable_colors[i % len(readable_colors)] for i in range(len(block_sizes))]
                    
                    if current_graph_type == 'bar':
                        # Create bar plot for each block size
                        width = 0.8 / len(block_sizes)  # Width of each bar group
                        
                        for i, block_size in enumerate(block_sizes):
                            offset = (i - len(block_sizes)/2 + 0.5) * width
                            display_name = get_block_size_display_name(block_size)
                            bars = ax1.bar([pos + offset for pos in all_positions], 
                                          df_sorted[block_size], 
                                          width=width, 
                                          label=display_name,
                                          color=colors[i], 
                                          alpha=0.8,
                                          edgecolor='black',
                                          linewidth=0.5)
                    else:  # line graph
                        # Create line plot for each block size
                        for i, block_size in enumerate(block_sizes):
                            display_name = get_block_size_display_name(block_size)
                            ax1.plot(all_positions, df_sorted[block_size], 
                                    marker='o', linewidth=2, markersize=3,
                                    label=display_name,
                                    color=colors[i], 
                                    markerfacecolor=colors[i],
                                    markeredgecolor='black',
                                    markeredgewidth=1)
                
                    # Calculate and display average for each individual block size
                    for i, block_size in enumerate(block_sizes):
                        block_data = df_sorted[block_size]
                        # Calculate average for this specific block size
                        block_average = block_data.mean()
                        
                        # Add horizontal line for this block size's average
                        ax1.axhline(y=block_average, color=colors[i], linestyle='--', linewidth=2, alpha=0.7)
                
                    # Get FIO configuration for subtitle (use first block size as reference)
                    # For comparison graphs, exclude block size since multiple block sizes are shown
                    subtitle = ""
                    if block_sizes:
                        # Use the first block size to get FIO config for subtitle
                        first_block_size = block_sizes[0]
                        config_key = (operation, first_block_size)
                        if config_key in FIO_CONFIGS:
                            subtitle = format_fio_subtitle(FIO_CONFIGS[config_key], exclude_bs=True)
                    
                    # Customize the plot
                    num_vms = len(df_sorted)
                    chart_type = "Bar Chart" if current_graph_type == 'bar' else "Line Chart"
                    
                    # Set title and Y-axis label based on data type
                    if data_type == 'iops':
                        ax1.set_title(f'Total IOPS per Machine Performance Comparison ({chart_type}): {operation.upper()} - Selected Block Sizes ({num_vms} Machines)', 
                                     fontsize=14, fontweight='bold', pad=40)
                        ax1.set_ylabel('Total IOPS per Machine (sum of all jobs)', fontsize=12, fontweight='bold', color='blue')
                        data_type_for_latency = 'iops'
                    else:
                        ax1.set_title(f'Total Bandwidth per Machine Performance Comparison ({chart_type}): {operation.upper()} - Selected Block Sizes ({num_vms} Machines)', 
                                     fontsize=14, fontweight='bold', pad=40)
                        ax1.set_ylabel('Total bw_mean per Machine (sum of all jobs) [KB]', fontsize=12, fontweight='bold', color='blue')
                        data_type_for_latency = 'iops'  # Use IOPS latency data since it's the same source
                    
                    # Add latency data on secondary axis if available
                    latency_data_available = False
                    if block_sizes:
                        # Create secondary y-axis for latency
                        ax2 = ax1.twinx()
                        
                        # Define colors for different block sizes
                        latency_colors = ['orange', 'red', 'purple', 'brown', 'pink', 'gray', 'olive', 'cyan']
                        latency_markers = ['s', 'o', '^', 'D', 'v', '<', '>', 'p']
                        
                        all_latency_values = []
                        
                        # Plot latency data for each block size
                        for i, block_size in enumerate(block_sizes):
                            latency_data = extract_latency_data_for_graph(operation, block_size, output_dir, data_type_for_latency)
                            
                            if latency_data and len(latency_data) > 0:
                                latency_data_available = True
                                
                                # Match latency data to machines in the same order
                                latency_values = []
                                for machine in df_sorted['vm_name']:
                                    # Try to find matching latency data (handle different machine name formats and case)
                                    found_latency = None
                                    
                                    # Extract machine name from full path (e.g., /path/to/machine-name -> machine-name)
                                    machine_basename = os.path.basename(machine)
                                    
                                    # Try exact match first (case-insensitive)
                                    for lat_machine, lat_value in latency_data.items():
                                        if machine_basename.lower() == lat_machine.lower():
                                            found_latency = lat_value
                                            break
                                    
                                    # If no exact match, try partial matching (case-insensitive)
                                    if found_latency is None:
                                        for lat_machine, lat_value in latency_data.items():
                                            # Check if machine name contains latency machine name or vice versa
                                            if (lat_machine.lower() in machine_basename.lower() or 
                                                machine_basename.lower() in lat_machine.lower()):
                                                found_latency = lat_value
                                                break
                                    
                                    # If still no match, try path-based matching
                                    if found_latency is None:
                                        for lat_machine, lat_value in latency_data.items():
                                            # Check if machine path ends with latency machine name
                                            if machine.rstrip('/').endswith('/' + lat_machine):
                                                found_latency = lat_value
                                                break
                                    
                                    if found_latency is not None:
                                        latency_values.append(found_latency)
                                    else:
                                        latency_values.append(0)  # Default to 0 if no latency data found
                                
                                # Store all latency values for axis limits
                                all_latency_values.extend([v for v in latency_values if v > 0])
                                
                                # Plot latency data as a line overlay for this block size
                                color = latency_colors[i % len(latency_colors)]
                                marker = latency_markers[i % len(latency_markers)]
                                
                                ax2.plot(all_positions, latency_values, 
                                        marker=marker, linewidth=2, markersize=2, 
                                        color=color, markerfacecolor=color, 
                                        markeredgecolor=color, markeredgewidth=1,
                                        label=f'Latency {block_size.upper()}', alpha=0.8)
                                
                                # Calculate and plot average latency for this block size
                                avg_latency = sum(latency_values) / len(latency_values) if latency_values else 0
                                ax2.axhline(y=avg_latency, color=color, linestyle=':', linewidth=2, alpha=0.8,
                                           label=f'Avg Latency {block_size.upper()}: {avg_latency:.2f} ms')
                        
                        if latency_data_available:
                            # Customize secondary axis
                            ax2.set_ylabel('Average Latency (ms)', fontsize=12, fontweight='bold', color='orange')
                            ax2.tick_params(axis='y', labelcolor='orange')
                            
                            # Set latency axis limits based on all latency values
                            if all_latency_values:
                                ax2.set_ylim(0, max(all_latency_values) * 1.1)
                    
                    # Add subtitle with FIO configuration
                    if subtitle:
                        # Position subtitle in the center of the graph area (not the entire figure)
                        # Since we have right=0.55, the graph area is from 0 to 0.55, so center is at 0.275
                        # Position below the title (which has pad=40), so subtitle goes at 0.88 to be below title
                        plt.figtext(0.275, 0.88, subtitle, fontsize=10, ha='center', va='top')
                    
                    # Set X-axis label
                    ax1.set_xlabel('Machine Index', fontsize=12, fontweight='bold')
                    
                    # Set x-axis ticks based on visibility rules
                    ax1.set_xticks(x_positions)
                    ax1.set_xticklabels(x_labels, rotation=45, ha='right')
                    
                    # Set axis limits
                    ax1.set_xlim(-0.5, len(df_sorted) - 0.5)
                    max_value = df_sorted[block_sizes].max().max()
                    ax1.set_ylim(0, max_value * 1.1)
                    
                    # Add legend on the right side of the graph (aligned with top of graph)
                    if latency_data_available:
                        # Combine legends from both axes
                        lines1, labels1 = ax1.get_legend_handles_labels()
                        lines2, labels2 = ax2.get_legend_handles_labels()
                        ax1.legend(lines1 + lines2, labels1 + labels2, loc='center left', bbox_to_anchor=(1.15, 0.95), fontsize=9)
                    else:
                        ax1.legend(loc='center left', bbox_to_anchor=(1.15, 0.95), fontsize=9)
                    
                    # Add average, total per Machine, and total all Machines value text boxes below the legend
                    for i, block_size in enumerate(block_sizes):
                        block_data = df_sorted[block_size]
                        block_average = block_data.mean()
                        block_total = block_data.sum()
                        display_name = get_block_size_display_name(block_size)
                        
                        # Format text based on data type
                        if data_type == 'iops':
                            avg_text = f'{display_name} Average: {block_average:.1f} IOPS'
                            total_per_machine_text = f'{display_name} Total per Machine: {block_average:.1f} IOPS'
                            total_all_machines_text = f'{display_name} Total All Machines: {block_total:.0f} IOPS'
                        else:
                            avg_text = f'{display_name} Average: {block_average:.1f} KB'
                            total_per_machine_text = f'{display_name} Total per Machine: {block_average:.1f} KB'
                            total_all_machines_text = f'{display_name} Total All Machines: {block_total:.0f} KB'
                        
                        # Position average text box (top) - white background for better readability
                        ax1.text(1.15, 0.75 - (i * 0.16), avg_text, 
                                transform=ax1.transAxes, fontsize=9, fontweight='bold',
                                verticalalignment='top', bbox=dict(boxstyle='round', facecolor='white', alpha=0.9, edgecolor=colors[i], linewidth=1),
                                color=colors[i])
                        
                        # Position total per machine text box (middle) - white background for better readability
                        ax1.text(1.15, 0.75 - (i * 0.16) - 0.04, total_per_machine_text, 
                                transform=ax1.transAxes, fontsize=9, fontweight='bold',
                                verticalalignment='top', bbox=dict(boxstyle='round', facecolor='white', alpha=0.9, edgecolor=colors[i], linewidth=1),
                                color=colors[i])
                        
                        # Position total all machines text box (bottom) - white background for better readability
                        ax1.text(1.15, 0.75 - (i * 0.16) - 0.08, total_all_machines_text, 
                                transform=ax1.transAxes, fontsize=9, fontweight='bold',
                                verticalalignment='top', bbox=dict(boxstyle='round', facecolor='white', alpha=0.9, edgecolor=colors[i], linewidth=1),
                                color=colors[i])
                    
                    # Add grid for better readability
                    if current_graph_type == 'bar':
                        ax1.grid(axis='y', alpha=0.3, linestyle='--')
                    else:  # line graph
                        ax1.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
                
                    # Adjust layout to accommodate legend and text boxes on the right
                    plt.tight_layout()
                    plt.subplots_adjust(right=0.55, top=0.85)  # Increased space on right for latency axis label and legend boxes
                    
                    # Generate PNG filename with number of machines, block sizes, graph type, and data type included
                    block_sizes_str = '-'.join(block_sizes)
                    csv_basename = os.path.basename(csv_file)
                    graph_suffix = 'bar' if current_graph_type == 'bar' else 'line'
                    data_type_suffix = 'bw' if data_type == 'bandwidth' else 'iops'
                    png_filename = f"{num_vms}_machines_{csv_basename.replace('.csv', f'_comparison-{block_sizes_str}_average_{graph_suffix}_{data_type_suffix}.png')}"
                    png_filepath = os.path.join(output_dir, png_filename)
                    
                    # Save the plot
                    plt.savefig(png_filepath, dpi=300, bbox_inches='tight', 
                               facecolor='white', edgecolor='none')
                    plt.close()  # Close the figure to free memory
                    
                    print(f"Created operation summary graph: {png_filename}")
                    success_count += 1
                    
                    # Create summary text file for this operation
                    create_operation_summary_file(df_sorted, operation, block_sizes, data_type, output_dir)
                    
                except Exception as e:
                    print(f"Error creating operation summary graph for {csv_file}: {e}")
        
        return success_count
        
    except ImportError as e:
        print(f"Error importing required libraries: {e}")
        print("Please install required dependencies: pip install matplotlib pandas numpy")
        return 0
    except Exception as e:
        print(f"Error in operation summary graph creation: {e}")
        return 0


def create_latency_performance_correlation_graph(all_machines_results, output_dir='.', data_type='iops'):
    """
    Create latency vs performance correlation graphs.
    Creates separate graphs for each operation, with subplots for different block sizes.
    X-axis: Performance (IOPS or bandwidth) per Machine
    Y-axis: Average latency in milliseconds
    Each dot represents a Machine's performance vs latency.
    
    Args:
        all_machines_results: Dictionary with aggregated results from all machines
        output_dir: Output directory for the graph
        data_type: 'iops' or 'bandwidth'
    
    Returns:
        Success count (number of graphs created successfully)
    """
    try:
        import matplotlib.pyplot as plt
        import numpy as np
        
        
        # Set matplotlib to use a non-interactive backend
        plt.switch_backend('Agg')
        
        if not all_machines_results:
            print("ERROR: No data available for latency correlation graph")
            return 0
        
        print(f"Creating latency correlation graphs for {data_type} data...")
        
        # Group data by operation and block size
        operations = set()
        block_sizes = set()
        
        for operation, block_data in all_machines_results.items():
            operations.add(operation)
            for block_size in block_data.keys():
                block_sizes.add(block_size)
        
        operations = sorted(operations)
        block_sizes = sorted(block_sizes, key=lambda x: int(x.replace('k', '')))
        
        print(f"Found operations: {operations}")
        print(f"Found block sizes: {block_sizes}")
        
        if len(operations) == 0:
            print("ERROR: No operations found in data")
            return 0
        
        success_count = 0
        
        # Create a separate graph for each operation
        for operation in operations:
            print(f"Processing operation: {operation}")
            
            # Get block sizes available for this operation
            op_block_sizes = [bs for bs in block_sizes 
                            if operation in all_machines_results and bs in all_machines_results[operation]]
            
            if not op_block_sizes:
                print(f"No block sizes found for operation {operation}")
                continue
            
            print(f"Block sizes for {operation}: {op_block_sizes}")
            
            # Create subplots for each block size of this operation
            num_block_sizes = len(op_block_sizes)
            cols = min(3, num_block_sizes)  # Max 3 columns
            rows = (num_block_sizes + cols - 1) // cols
            
            print(f"Creating subplot layout: {rows}x{cols} for {num_block_sizes} block sizes")
            
            # Increase figure height to accommodate title and subtitle
            fig_height = 4*rows + 1.5  # Add extra height for title and subtitle
            fig, axes = plt.subplots(rows, cols, figsize=(5*cols, fig_height))
            
            # Ensure axes is always a list for consistent indexing
            if num_block_sizes == 1:
                axes = [axes]
            elif rows == 1 and cols == 1:
                axes = [axes]
            elif rows == 1:
                # Single row, multiple columns - axes is already a list
                axes = list(axes) if hasattr(axes, '__iter__') else [axes]
            else:
                # Multiple rows - flatten the 2D array
                axes = axes.flatten()
            
            # Use a single color for this operation (since all subplots are the same operation)
            color = plt.cm.Set1(operations.index(operation) / len(operations))
            
            for bs_idx, block_size in enumerate(op_block_sizes):
                print(f"Processing block size: {block_size}")
                ax = axes[bs_idx]
                
                data = all_machines_results[operation][block_size]
                print(f"Data points for {operation}-{block_size}: {len(data)}")
                
                # Extract performance and latency data from the list of items
                performance_values = []
                latency_values = []
                
                for item in data:
                    if data_type == 'iops':
                        if isinstance(item, dict) and 'total_iops' in item and 'avg_latency_ms' in item:
                            performance_values.append(item['total_iops'])
                            latency_values.append(item['avg_latency_ms'])
                    else:  # bandwidth
                        if isinstance(item, dict) and 'bw_mean' in item and 'avg_latency_ms' in item:
                            performance_values.append(item['bw_mean'])
                            latency_values.append(item['avg_latency_ms'])
                
                if performance_values and latency_values:
                    # For large datasets (>1000 points), use smaller markers and lower alpha for better performance
                    num_points = len(performance_values)
                    if num_points > 1000:
                        marker_size = max(10, 60 - (num_points - 1000) // 100)  # Reduce size for large datasets
                        alpha = max(0.3, 0.7 - (num_points - 1000) // 2000)  # Reduce alpha for large datasets
                        
                        # For very large datasets (>5000 points), sample the data to prevent memory issues
                        if num_points > 5000:
                            step = num_points // 5000
                            performance_values = performance_values[::step]
                            latency_values = latency_values[::step]
                            num_points = len(performance_values)
                    else:
                        marker_size = 60
                        alpha = 0.7
                    
                    # Create scatter plot
                    ax.scatter(performance_values, latency_values, 
                              color=color, alpha=alpha, s=marker_size,
                              edgecolors='black', linewidth=0.5)
                    
                    # Set reasonable axis limits
                    if performance_values:
                        ax.set_xlim(0, max(performance_values) * 1.1)
                    if latency_values:
                        ax.set_ylim(0, max(latency_values) * 1.1)
                    
                    # Format X-axis labels for better readability with large numbers
                    def format_large_numbers(x, pos):
                        if x >= 1e6:
                            return f'{x/1e6:.1f}M'
                        elif x >= 1e4:  # Only use K format for numbers >= 10,000
                            return f'{x/1e3:.0f}K'
                        else:
                            return f'{x:.0f}'
                    
                    ax.xaxis.set_major_formatter(plt.FuncFormatter(format_large_numbers))
                
                # Customize subplot
                metric_name = 'IOPS' if data_type == 'iops' else 'Bandwidth (KB/s)'
                ax.set_xlabel(f'Total {metric_name} per Machine', fontsize=10, fontweight='bold')
                ax.set_ylabel('Average Latency (ms)', fontsize=10, fontweight='bold')
                ax.set_title(f'Block Size: {block_size}', fontsize=12, fontweight='bold')
                ax.grid(True, alpha=0.3)
            
            # Hide unused subplots
            for idx in range(num_block_sizes, len(axes)):
                axes[idx].set_visible(False)
            
            # Calculate number of machines from the data
            num_machines = 0
            if op_block_sizes and operation in all_machines_results:
                # Get the first available block size to count unique machines
                first_bs = op_block_sizes[0]
                if first_bs in all_machines_results[operation]:
                    # Count unique machine names, not total data points
                    unique_machines = set()
                    for data_point in all_machines_results[operation][first_bs]:
                        if 'machine' in data_point:
                            unique_machines.add(data_point['machine'])
                    num_machines = len(unique_machines)
            
            print(f"Calculated {num_machines} unique machines for {operation}")
            
            # Get FIO configuration for subtitle (use first available block size)
            subtitle = ""
            if op_block_sizes:
                first_bs = op_block_sizes[0]
                config_key = (operation, first_bs)
                if config_key in FIO_CONFIGS:
                    subtitle = format_fio_subtitle(FIO_CONFIGS[config_key])
            
            # Set main title
            metric_name = 'IOPS' if data_type == 'iops' else 'Bandwidth'
            fig.suptitle(f'{operation.upper()} - Latency vs {metric_name} Performance Correlation ({num_machines} Machines)',
                        fontsize=16, fontweight='bold', y=0.95)
            
            # Add subtitle with FIO configuration
            if subtitle:
                plt.figtext(0.5, 0.85, subtitle, fontsize=10, ha='center', va='top')
            
            # Adjust layout to ensure title and subtitle don't overlap
            plt.tight_layout()
            # Add more space between rows when there are multiple rows
            if rows > 1:
                plt.subplots_adjust(top=0.75, hspace=0.4)  # Increased space for title/subtitle and between rows
            else:
                plt.subplots_adjust(top=0.75)  # Increased space for title and subtitle
            
            # Save the plot
            graph_suffix = 'latency_correlation'
            data_type_suffix = 'bw' if data_type == 'bandwidth' else 'iops'
            png_filename = f"{graph_suffix}_{operation}_{data_type_suffix}.png"
            png_filepath = os.path.join(output_dir, png_filename)
            
            # Ensure output directory exists
            os.makedirs(output_dir, exist_ok=True)
            
            print(f"Saving graph to: {png_filepath}")
            plt.savefig(png_filepath, dpi=300, bbox_inches='tight', 
                       facecolor='white', edgecolor='none')
            plt.close()
            
            print(f"Successfully created latency correlation graph: {png_filename}")
            success_count += 1
        
        print(f"Latency correlation graph creation complete. Created {success_count} graphs.")
        return success_count
        
    except ImportError as e:
        print(f"Error importing required libraries: {e}")
        print("Please install required dependencies: pip install matplotlib pandas numpy")
        return 0
    except Exception as e:
        print(f"Error creating latency correlation graph: {e}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        return 0


def save_latency_data_to_files(all_machines_results, output_dir='.', data_type='iops'):
    """
    Save latency data for each machine to text files in a latencydata directory.
    Creates separate files for each operation and block size combination.
    
    Args:
        all_machines_results: Dictionary with aggregated results from all machines
        output_dir: Output directory for the latency data files
        data_type: 'iops' or 'bandwidth'
    
    Returns:
        Success count (number of files created successfully)
    """
    try:
        import os
        
        # Create latencydata directory
        latency_dir = os.path.join(output_dir, 'latencydata')
        os.makedirs(latency_dir, exist_ok=True)
        
        success_count = 0
        
        if not all_machines_results:
            print("No data available for latency extraction")
            return 0
        
        operations = sorted(set(all_machines_results.keys()))
        print(f"Extracting latency data for operations: {operations}")
        
        for operation in operations:
            op_block_sizes = sorted([bs for bs in all_machines_results[operation].keys() if all_machines_results[operation][bs]], 
                                   key=lambda x: int(x.replace('k', '')))
            
            if not op_block_sizes:
                print(f"No block sizes found for operation {operation}")
                continue
            
            print(f"Processing operation: {operation}")
            print(f"Block sizes for {operation}: {op_block_sizes}")
            
            for block_size in op_block_sizes:
                data = all_machines_results[operation][block_size]
                
                if not data:
                    print(f"No data found for {operation}-{block_size}")
                    continue
                
                # Extract latency data for each machine
                machine_latency_data = []
                for item in data:
                    if isinstance(item, dict) and 'avg_latency_ms' in item and 'machine' in item:
                        machine_latency_data.append({
                            'machine': item['machine'],
                            'avg_latency_ms': item['avg_latency_ms']
                        })
                
                if not machine_latency_data:
                    print(f"No valid latency data found for {operation}-{block_size}")
                    continue
                
                # Sort by machine name for consistent output
                machine_latency_data.sort(key=lambda x: x['machine'])
                
                # Create nested directory structure: latencydata/operation/block_size.txt
                operation_dir = os.path.join(latency_dir, operation)
                os.makedirs(operation_dir, exist_ok=True)
                
                # Create filename
                data_type_suffix = 'bw' if data_type == 'bandwidth' else 'iops'
                filename = f"{block_size}_{data_type_suffix}.txt"
                filepath = os.path.join(operation_dir, filename)
                
                # Write latency data to file
                try:
                    with open(filepath, 'w') as f:
                        f.write(f"Latency Data for {operation.upper()} - Block Size: {block_size.upper()}\n")
                        f.write(f"Data Type: {data_type.upper()}\n")
                        f.write(f"Number of machines: {len(machine_latency_data)}\n")
                        f.write("=" * 60 + "\n\n")
                        
                        for item in machine_latency_data:
                            f.write(f"Machine: {item['machine']}\n")
                            f.write(f"Average Latency: {item['avg_latency_ms']:.3f} ms\n")
                            f.write("-" * 40 + "\n")
                    
                    print(f"Saved latency data to: {filepath}")
                    success_count += 1
                    
                except Exception as e:
                    print(f"Error writing file {filepath}: {e}")
        
        print(f"Latency data extraction complete. Created {success_count} files.")
        return success_count
        
    except Exception as e:
        print(f"Error in save_latency_data_to_files: {e}")
        return 0


def create_operation_summary_file(df_sorted, operation, block_sizes, data_type, output_dir):
    """
    Create a text file with summary values for an operation.
    
    Args:
        df_sorted: DataFrame with sorted data
        operation: Operation name (read, write, randread, randwrite)
        block_sizes: List of block sizes
        data_type: 'iops' or 'bandwidth'
        output_dir: Output directory path
    """
    try:
        # Get number of machines from DataFrame
        num_machines = len(df_sorted)
        
        # Create filename with number of machines prefix
        data_type_suffix = 'bw' if data_type == 'bandwidth' else 'iops'
        txt_filename = f"{num_machines}machines_{operation}_{data_type_suffix}.txt"
        txt_filepath = os.path.join(output_dir, txt_filename)
        
        # Open file for writing
        with open(txt_filepath, 'w') as f:
            f.write(f"Summary for {operation.upper()} operation ({data_type.upper()})\n")
            f.write("=" * 50 + "\n")
            f.write(f"Tested: {num_machines} machines in test\n\n")
            
            # Write values for each block size
            for block_size in block_sizes:
                block_data = df_sorted[block_size]
                block_average = block_data.mean()
                block_total = block_data.sum()
                display_name = get_block_size_display_name(block_size)
                
                f.write(f"Block size: {display_name}\n")
                
                # Format values based on data type
                if data_type == 'iops':
                    f.write(f"  Average: {block_average:.1f} IOPS\n")
                    f.write(f"  Total per Machine: {block_average:.1f} IOPS\n")
                    f.write(f"  Total All Machines: {block_total:.0f} IOPS\n")
                else:
                    f.write(f"  Average: {block_average:.1f} KB\n")
                    f.write(f"  Total per Machine: {block_average:.1f} KB\n")
                    f.write(f"  Total All Machines: {block_total:.0f} KB\n")
                
                f.write("\n")  # Empty line between block sizes
        
        print(f"Created summary file: {txt_filename}")
        
    except Exception as e:
        print(f"Error creating summary file for {operation}: {e}")


if __name__ == "__main__":
    main()
