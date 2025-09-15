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
    --summary-only         Generate only summary graphs (skip per-VM comparison graphs)
    --help                 Show this help message

Examples:
    python3 analyze_bw_mean_with_graphs.py --iops
    python3 analyze_bw_mean_with_graphs.py --bw
    python3 analyze_bw_mean_with_graphs.py --iops --input-dir /path/to/data --output-dir /path/to/results
    python3 analyze_bw_mean_with_graphs.py --bw --graph-type line --block-sizes 4k,8k,128k
    python3 analyze_bw_mean_with_graphs.py --iops --operation-summary --graph-type both
    python3 analyze_bw_mean_with_graphs.py --iops --summary-only
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
    Determine X-axis labels and positions based on number of VMs.
    Show every VM if <= 20, otherwise show every 30th VM.
    """
    num_vms = len(df_sorted)
    # If num_vms is <= 20, show every VM - check this 
    if num_vms <= 20:
        # Show every VM
        x_positions = range(num_vms)
        x_labels = [f'VM {i+1}' for i in x_positions]
        return x_positions, x_labels
    else:
        # Show every 30th VM for better visibility
        x_positions = range(0, num_vms, 30)
        x_labels = [f'VM {i+1}' for i in x_positions]
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

def format_fio_subtitle(config_data):
    """
    Format FIO configuration data into a subtitle string.
    """
    subtitle_parts = []
    
    if config_data.get('size') != 'N/A':
        subtitle_parts.append(f"Size: {config_data['size']}")
    if config_data.get('bs') != 'N/A':
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
        if 'randread' in filename:
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
        
        # Collect all IOPS values for this operation across all jobs
        iops_values = []
        
        for job in data['jobs']:
            # For randread, the data is stored under 'read' key
            # For randwrite, the data is stored under 'write' key
            data_key = 'read' if operation in ['read', 'randread'] else 'write'
            
            if data_key in job and 'iops_mean' in job[data_key]:
                iops_value = job[data_key]['iops_mean']
                if iops_value > 0:  # Only include non-zero IOPS
                    iops_values.append(iops_value)
        
        # Aggregate IOPS values (average them since they represent different jobs)
        if iops_values:
            # Convert to integer to remove decimal places
            iops_data[(operation, block_size)] = int(sum(iops_values) / len(iops_values))
    
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

        # Extract FIO parameters for subtitle
        fio_params = {}
        
        # Determine operation type from filename (same logic as IOPS extraction)
        filename = os.path.basename(file_path)
        if 'randread' in filename:
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
                # For randread, the data is stored under 'read' key
                # For randwrite, the data is stored under 'write' key
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
        
        return bw_values, config_data
    
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return [], {}

def parse_filename_info(filename):
    """Parse operation and block size from filename."""
    # Pattern: fio-test-{operation}-bs-{blocksize}.json
    match = re.match(r'fio-test-(\w+)-bs-(\w+)\.json', filename)
    if match:
        operation = match.group(1)
        block_size = match.group(2).lower()  # Convert to lowercase for consistency
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
                        'job_name': bw_data['job_name']
                    })
                    
                    # Store for all machines aggregation
                    all_machines_results[operation][block_size].append({
                        'machine': directory,
                        'bw_mean': bw_data['bw_mean'],
                        'job_name': bw_data['job_name']
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
                # IOPS data - items are direct values, need to create VM names
                for i, iops_value in enumerate(items):
                    vm_name = f"VM-{i+1}"  # Create VM names for IOPS data
                    if vm_name not in operation_results[operation]:
                        operation_results[operation][vm_name] = {}
                    operation_results[operation][vm_name][block_size] = iops_value
            else:
                # Bandwidth data - items are dictionaries with machine and bw_mean
                for item in items:
                    vm_name = item['machine']
                    bw_mean = item['bw_mean']
                    
                    if vm_name not in operation_results[operation]:
                        operation_results[operation][vm_name] = {}
                    operation_results[operation][vm_name][block_size] = bw_mean
    
    # Write CSV files for each operation
    csv_files_created = []
    
    for operation, vm_data in operation_results.items():
        filename = f"summary-{operation}-all-blocks.csv"
        filepath = os.path.join(output_dir, filename)
        
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
        print(f"Created {filepath} with {len(sorted_vms)} VMs and {len(all_block_sizes)} block sizes: {', '.join(display_names)}")
        csv_files_created.append(filepath)
    
    return csv_files_created

def save_results_to_files(results, all_machines_results, output_dir='.', selected_block_sizes=None):
    """Save results to CSV files for further analysis."""
    
    # Save per-machine results
    for machine in results.keys():
        filename = f"{os.path.basename(machine)}_bw_mean_results.csv"
        filepath = os.path.join(output_dir, filename)
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
        filepath = os.path.join(output_dir, filename)
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
            filepath = os.path.join(output_dir, filename)
            with open(filepath, 'w') as f:
                f.write("Machine,JobName,BwMean\n")
                for item in all_machines_results[operation][block_size]:
                    f.write(f"{item['machine']},{item['job_name']},{item['bw_mean']}\n")
            print(f"Saved block size {block_size} operation {operation} results to: {filepath}")
    
    # Also save combined results for backward compatibility
    filename = "all_machines_bw_mean_results.csv"
    filepath = os.path.join(output_dir, filename)
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
    for machine in results.keys():
        filename = f"{os.path.basename(machine)}_bw_mean_job_summary.csv"
        filepath = os.path.join(output_dir, filename)
        with open(filepath, 'w') as f:
            f.write("Operation,BlockSize,TotalBwMean\n")
            for operation in results[machine].keys():
                for block_size in results[machine][operation].keys():
                    items = results[machine][operation][block_size]
                    total_bw = sum(item['bw_mean'] for item in items) / len(items) if items else 0
                    
                    f.write(f"{operation},{block_size},{total_bw}\n")
        print(f"Saved job-summarized results to: {filepath}")
    
    # Save all machines job-summarized results
    filename = "all_machines_bw_mean_job_summary.csv"
    filepath = os.path.join(output_dir, filename)
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
                    total_bw = sum(item['bw_mean'] for item in items) / len(items) if items else 0
                    
                    f.write(f"{operation},{block_size},{machine},{total_bw}\n")
    
    print(f"Saved all machines job-summarized results to: {filepath}")
    
    # Save block size and operation specific job-summarized results
    for operation in all_machines_results.keys():
        for block_size in all_machines_results[operation].keys():
            # Skip if block size filtering is enabled and this block size is not selected
            if selected_block_sizes and block_size not in selected_block_sizes:
                continue
                
            filename = f"all_machines_block_size_{block_size}_operation_{operation}_job_summary.csv"
            filepath = os.path.join(output_dir, filename)
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
                    total_bw = sum(item['bw_mean'] for item in items) / len(items) if items else 0
                    
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
                       help='Generate only summary graphs (skip per-VM comparison graphs)')
    
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
                        print(f"\nCreating {args.graph_type} summary graphs only (per-VM graphs skipped)...")
                    else:
                        print(f"\nCreating {args.graph_type} graphs...")
                    create_graphs_from_job_summaries(output_dir, args.graph_type, args.summary_only)
            else:
                # Generate report
                generate_report(results, all_machines_results)
                
                # Save results to files
                save_results_to_files_iops(results, all_machines_results, output_dir, selected_block_sizes)
                
                # Save job summarized results
                save_job_summarized_results_iops(results, all_machines_results, output_dir, selected_block_sizes)
                
                # Create graphs from job summaries
                if args.summary_only:
                    print(f"\nCreating {args.graph_type} summary graphs only (per-VM graphs skipped)...")
                else:
                    print(f"\nCreating {args.graph_type} graphs...")
                create_graphs_from_job_summaries(output_dir, args.graph_type, args.summary_only)
    
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
                        print(f"\nCreating {args.graph_type} summary graphs only (per-VM graphs skipped)...")
                    else:
                        print(f"\nCreating {args.graph_type} graphs...")
                    create_graphs_from_job_summaries(output_dir, args.graph_type, args.summary_only)
            else:
                # Generate report
                generate_report(results, all_machines_results)
                
                # Save results to files
                save_results_to_files(results, all_machines_results, output_dir, selected_block_sizes)
                
                # Save job summarized results
                save_job_summarized_results(results, all_machines_results, output_dir, selected_block_sizes)
                
                # Create graphs from job summaries
                if args.summary_only:
                    print(f"\nCreating {args.graph_type} summary graphs only (per-VM graphs skipped)...")
                else:
                    print(f"\nCreating {args.graph_type} graphs...")
                create_graphs_from_job_summaries(output_dir, args.graph_type, args.summary_only)
    
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
            all_machines_results[operation][block_size].append(iops)
            print(f"  Processing: {vm_name} (op: {operation}, bs: {block_size})")
    
    return all_results, all_machines_results

def save_results_to_files_iops(results, all_machines_results, output_dir='.', selected_block_sizes=None):
    """Save IOPS results to CSV files."""
    
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
    # Group results by VM name
    vm_results = defaultdict(lambda: defaultdict(dict))
    for (vm_name, operation, block_size), iops in results.items():
        vm_results[vm_name][operation][block_size] = iops
    
    for vm_name in vm_results.keys():
        filename = f"{vm_name}_iops_results.csv"
        filepath = os.path.join(output_dir, filename)
        
        with open(filepath, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Operation', 'BlockSize', 'IOPS'])
            
            for operation in sorted(vm_results[vm_name].keys()):
                for block_size in sorted(vm_results[vm_name][operation].keys()):
                    writer.writerow([operation, block_size, vm_results[vm_name][operation][block_size]])
        
        print(f"Saved per-machine results to: {filepath}")
    
    # Save aggregated results by operation
    for operation, block_sizes in all_machines_results.items():
        filename = f"{operation}_all_machines_iops_results.csv"
        filepath = os.path.join(output_dir, filename)
        
        with open(filepath, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['BlockSize', 'TotalIOPS', 'MinIOPS', 'MaxIOPS', 'MeanIOPS'])
            
            for block_size, iops_list in block_sizes.items():
                total_iops = sum(iops_list)
                min_iops = min(iops_list)
                max_iops = max(iops_list)
                mean_iops = sum(iops_list) / len(iops_list)
                writer.writerow([block_size, total_iops, min_iops, max_iops, mean_iops])
        
        print(f"Saved {operation} aggregated results to: {filepath}")
    
    # Save combined aggregated results
    filename = "all_machines_iops_results.csv"
    filepath = os.path.join(output_dir, filename)
    
    with open(filepath, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Operation', 'BlockSize', 'TotalIOPS', 'MinIOPS', 'MaxIOPS', 'MeanIOPS'])
        
        for operation, block_sizes in all_machines_results.items():
            for block_size, iops_list in block_sizes.items():
                total_iops = sum(iops_list)
                min_iops = min(iops_list)
                max_iops = max(iops_list)
                mean_iops = sum(iops_list) / len(iops_list)
                writer.writerow([operation, block_size, total_iops, min_iops, max_iops, mean_iops])
    
    print(f"Saved combined aggregated results to: {filepath}")

def save_job_summarized_results_iops(results, all_machines_results, output_dir='.', selected_block_sizes=None):
    """Save IOPS job summarized results to CSV files."""
    
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
    # Group results by VM name
    vm_results = defaultdict(lambda: defaultdict(dict))
    for (vm_name, operation, block_size), iops in results.items():
        vm_results[vm_name][operation][block_size] = iops
    
    for vm_name in vm_results.keys():
        filename = f"{vm_name}_iops_job_summary.csv"
        filepath = os.path.join(output_dir, filename)
        
        with open(filepath, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Operation', 'BlockSize', 'TotalIOPS'])
            
            for operation in sorted(vm_results[vm_name].keys()):
                for block_size in sorted(vm_results[vm_name][operation].keys()):
                    writer.writerow([operation, block_size, vm_results[vm_name][operation][block_size]])
        
        print(f"Saved job-summarized results to: {filepath}")
    
    # Save all machines job summarized results
    filename = "all_machines_iops_job_summary.csv"
    filepath = os.path.join(output_dir, filename)
    
    with open(filepath, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Machine', 'TotalIOPS'])
        
        for (vm_name, operation, block_size), iops in results.items():
            writer.writerow([vm_name, iops])
    
    print(f"Saved all machines job-summarized results to: {filepath}")
    
    # Save block size + operation job summarized results
    for operation, block_sizes in all_machines_results.items():
        for block_size, iops_list in block_sizes.items():
            filename = f"all_machines_block_size_{block_size}_operation_{operation}_job_summary.csv"
            filepath = os.path.join(output_dir, filename)
            
            with open(filepath, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['Machine', 'TotalIOPS'])
                
                for i, iops in enumerate(iops_list):
                    writer.writerow([f"VM-{i+1}", iops])
            
            print(f"Saved job-summarized results to: {filepath}")

def create_graphs_from_job_summaries(output_dir='.', graph_type='bar', summary_only=False):
    """
    Create graphs from block size + operation job summary CSV files.
    
    Args:
        output_dir: Directory to save output files
        graph_type: Type of graphs ('bar', 'line', or 'both')
        summary_only: If True, skip per-VM graphs and only create summary graphs
    """
    try:
        import matplotlib.pyplot as plt
        import pandas as pd
        
        # Set matplotlib to use a non-interactive backend
        plt.switch_backend('Agg')
        
        # Find all job summary CSV files
        job_summary_files = glob.glob(os.path.join(output_dir, "all_machines_block_size_*_operation_*_job_summary.csv"))
        
        if not job_summary_files:
            print("No block size + operation job summary files found to create graphs from.")
            return
        
        if summary_only:
            print(f"\nSkipping per-VM graphs (summary-only mode enabled)")
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
        
    except ImportError as e:
        print(f"Error importing required libraries: {e}")
        print("Please install required dependencies: pip install matplotlib pandas")
    except Exception as e:
        print(f"Error in graph creation: {e}")


def create_single_graph(csv_file, graph_type, output_dir):
    """Create a single graph from a CSV file."""
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
            y_label = 'Total IOPS per machine'
        elif 'TotalBwMean' in df.columns:
            data_column = 'TotalBwMean'
            data_type = 'Bandwidth'
            y_label = 'Total bw_mean per machine [KB]'
        else:
            print(f"Skipping {csv_file}: Missing TotalIOPS or TotalBwMean column")
            return
        
        # Create the plot
        plt.figure(figsize=(10, 6))
        
        # Create numeric x-axis positions for all data points
        machines = df['Machine'].tolist()
        total_data = df[data_column].tolist()
        all_positions = range(len(machines))
        
        # Get X-axis labels and positions based on number of VMs
        x_positions, x_labels = get_x_axis_labels_and_positions(df)
        
        if graph_type == 'bar':
            # Create bar chart
            bars = plt.bar(all_positions, total_data, color='skyblue', edgecolor='navy', alpha=0.7)
        else:  # line graph
            # Create line plot
            plt.plot(all_positions, total_data, 
                    marker='o', linewidth=3, markersize=8, 
                    color='steelblue', markerfacecolor='lightblue', 
                    markeredgecolor='navy', markeredgewidth=2)

        # Set x-axis ticks based on visibility rules
        plt.xticks(x_positions, x_labels, rotation=45, ha='right')
        
        # Calculate average (Total / number of machines)
        total_data_sum = sum(total_data)
        num_machines = len(machines)
        average_data = total_data_sum / num_machines if num_machines > 0 else 0
        
        # Add horizontal line for average
        if data_type == 'IOPS':
            plt.axhline(y=average_data, color='red', linestyle='--', linewidth=2, alpha=0.8, 
                       label=f'Average: {average_data:.1f} IOPS')
            # Add text annotation for the average value
            plt.text(0.02, 0.98, f'Average IOPS: {average_data:.1f}', 
                    transform=plt.gca().transAxes, fontsize=10, fontweight='bold',
                    verticalalignment='top', bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
        else:
            plt.axhline(y=average_data, color='red', linestyle='--', linewidth=2, alpha=0.8, 
                       label=f'Average: {average_data:.1f} KB')
            # Add text annotation for the average value
            plt.text(0.02, 0.98, f'Average BW: {average_data:.1f} KB', 
                    transform=plt.gca().transAxes, fontsize=10, fontweight='bold',
                    verticalalignment='top', bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
                
        # Customize the plot
        plt.ylabel(y_label, fontsize=10, fontweight='bold')
        plt.xlabel('VM Index', fontsize=10, fontweight='bold')
        
        # Extract block size and operation from filename for title
        if 'block_size_' in csv_file and '_operation_' in csv_file:
            # Extract from filename like: all_machines_block_size_4k_operation_read_job_summary.csv
            
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
                num_vms = len(df)
                chart_type = "Bar Chart" if graph_type == 'bar' else "Line Chart"
                plt.title(f'{data_type} Performance ({chart_type}): {operation.upper()} - {block_size.upper()} Block Size ({num_vms} VMs)', 
                         fontsize=16, fontweight='bold', pad=20)
                
                # Add subtitle with FIO configuration
                if subtitle:
                    plt.suptitle(subtitle, fontsize=10, y=0.93, ha='center', va='top')
                else:
                    plt.title(csv_file.replace('_job_summary.csv', ''), fontsize=14, fontweight='bold')
        else:
            # For per-machine files like: vm-1_bw_mean_job_summary.csv
            plt.title(csv_file.replace('_bw_mean_job_summary.csv', ''), fontsize=14, fontweight='bold')

        # Set axis limits to include zero
        plt.xlim(-0.5, len(machines) - 0.5)
        plt.ylim(0, max(total_data) * 1.1)
        
        # Customize grid and layout
        if graph_type == 'bar':
            plt.grid(axis='y', alpha=0.3, linestyle='--')
        else:  # line graph
            plt.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
        
        # Add legend for the average line
        plt.legend(loc='upper right', fontsize=9)
        
        plt.tight_layout()
                
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
                    
                    # Create the plot
                    plt.figure(figsize=(14, 10))
                    
                    # Sort by vm_name for consistent ordering
                    df_sorted = df.sort_values('vm_name')
                    
                    # Get X-axis labels and positions based on number of VMs
                    x_positions, x_labels = get_x_axis_labels_and_positions(df_sorted)
                    
                    # Create numeric x-axis positions for all data points
                    all_positions = range(len(df_sorted))
                    
                    # Create plot based on graph type
                    colors = plt.cm.Set3(np.linspace(0, 1, len(block_sizes)))
                    
                    if current_graph_type == 'bar':
                        # Create bar plot for each block size
                        width = 0.8 / len(block_sizes)  # Width of each bar group
                        
                        for i, block_size in enumerate(block_sizes):
                            offset = (i - len(block_sizes)/2 + 0.5) * width
                            display_name = get_block_size_display_name(block_size)
                            bars = plt.bar([pos + offset for pos in all_positions], 
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
                            plt.plot(all_positions, df_sorted[block_size], 
                                    marker='o', linewidth=2, markersize=6,
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
                        plt.axhline(y=block_average, color=colors[i], linestyle='--', linewidth=2, alpha=0.7)
                
                    # Get FIO configuration for subtitle (use first block size as reference)
                    subtitle = ""
                    if block_sizes:
                        # Use the first block size to get FIO config for subtitle
                        first_block_size = block_sizes[0]
                        config_key = (operation, first_block_size)
                        if config_key in FIO_CONFIGS:
                            subtitle = format_fio_subtitle(FIO_CONFIGS[config_key])
                    
                    # Customize the plot
                    num_vms = len(df_sorted)
                    chart_type = "Bar Chart" if current_graph_type == 'bar' else "Line Chart"
                    
                    # Set title and Y-axis label based on data type
                    if data_type == 'iops':
                        plt.title(f'IOPS Performance Comparison ({chart_type}): {operation.upper()} - Selected Block Sizes ({num_vms} VMs)', 
                                 fontsize=16, fontweight='bold', pad=20)
                        plt.ylabel('Total IOPS per machine', fontsize=12, fontweight='bold')
                    else:
                        plt.title(f'Bandwidth Performance Comparison ({chart_type}): {operation.upper()} - Selected Block Sizes ({num_vms} VMs)', 
                                 fontsize=16, fontweight='bold', pad=20)
                        plt.ylabel('Total bw_mean per machine [KB]', fontsize=12, fontweight='bold')
                    
                    # Add subtitle with FIO configuration
                    if subtitle:
                        plt.suptitle(subtitle, fontsize=10, y=0.93, ha='center', va='top')
                    
                    # Set X-axis label
                    plt.xlabel('VM Index', fontsize=12, fontweight='bold')
                    
                    # Set x-axis ticks based on visibility rules
                    plt.xticks(x_positions, x_labels, rotation=45, ha='right')
                    
                    # Set axis limits
                    plt.xlim(-0.5, len(df_sorted) - 0.5)
                    max_value = df_sorted[block_sizes].max().max()
                    plt.ylim(0, max_value * 1.1)
                    
                    # Add legend on the right side of the graph
                    plt.legend(loc='center left', bbox_to_anchor=(1.02, 0.5), fontsize=10)
                    
                    # Add average value text boxes below the legend
                    for i, block_size in enumerate(block_sizes):
                        block_data = df_sorted[block_size]
                        block_average = block_data.mean()
                        display_name = get_block_size_display_name(block_size)
                        
                        # Set unit based on data type
                        if data_type == 'iops':
                            unit_text = f'{display_name} Avg: {block_average:.1f} IOPS'
                        else:
                            unit_text = f'{display_name} Avg: {block_average:.1f} KB'
                        
                        # Position text boxes below the legend (right side)
                        plt.text(1.02, 0.3 - (i * 0.08), unit_text, 
                                transform=plt.gca().transAxes, fontsize=10, fontweight='bold',
                                verticalalignment='top', bbox=dict(boxstyle='round', facecolor='white', alpha=0.8),
                                color=colors[i])
                    
                    # Add grid for better readability
                    if current_graph_type == 'bar':
                        plt.grid(axis='y', alpha=0.3, linestyle='--')
                    else:  # line graph
                        plt.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
                
                    # Adjust layout to accommodate legend and text boxes on the right
                    plt.tight_layout()
                    plt.subplots_adjust(right=0.75)  # Make room for legend and text boxes
                    
                    # Generate PNG filename with block sizes, graph type, and data type included
                    block_sizes_str = '-'.join(block_sizes)
                    csv_basename = os.path.basename(csv_file)
                    graph_suffix = 'bar' if current_graph_type == 'bar' else 'line'
                    data_type_suffix = 'bw' if data_type == 'bandwidth' else 'iops'
                    png_filename = csv_basename.replace('.csv', f'_comparison-{block_sizes_str}_average_{graph_suffix}_{data_type_suffix}.png')
                    png_filepath = os.path.join(output_dir, png_filename)
                    
                    # Save the plot
                    plt.savefig(png_filepath, dpi=300, bbox_inches='tight', 
                               facecolor='white', edgecolor='none')
                    plt.close()  # Close the figure to free memory
                    
                    print(f"Created operation summary graph: {png_filename}")
                    success_count += 1
                    
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


if __name__ == "__main__":
    main()
