#!/usr/bin/env python3
"""
Test Comparison Analyzer for FIO Results
========================================

This script compares IOPS results from different test runs.
It can compare results from test1/(vm-1,vm-2) vs test2/(vm-1,vm-2) structure.

Usage:
    python3 test_comparison_analyzer.py test1_dir test2_dir [options]

Options:
    --graphs bar|line|both    Type of graphs to generate (default: both)
    --output-dir DIR         Output directory for results (default: current directory)
    --summary-only           Generate only summary graphs (skip per-VM comparison graphs)
    --iops                   Analyze IOPS performance (default if no metric specified)
    --bw                     Analyze bandwidth performance
    --help                   Show this help message

Examples:
    python3 test_comparison_analyzer.py test1/ test2/
    python3 test_comparison_analyzer.py test1/ test2/ --graphs bar
    python3 test_comparison_analyzer.py test1/ test2/ --output-dir results/
    python3 test_comparison_analyzer.py test1/ test2/ --summary-only
    python3 test_comparison_analyzer.py test1/ test2/ --bw
    python3 test_comparison_analyzer.py test1/ test2/ --iops --bw
"""

import json
import csv
import os
import glob
import re
import argparse
import sys
import subprocess
from pathlib import Path

# Try to import optional dependencies
try:
    import pandas as pd
    import matplotlib
    matplotlib.use('Agg')  # Use non-interactive backend for consistent rendering
    import matplotlib.pyplot as plt
    import numpy as np
    HAS_PLOTTING = True
except ImportError as e:
    HAS_PLOTTING = False
    print(f"Warning: Plotting libraries not available: {e}")
    print("Install with: pip install pandas matplotlib numpy")


def extract_block_size_from_filename(filename):
    """
    Extract block size from filename (e.g., 'fio-test-read-bs-4k.json' -> '4k')
    """
    match = re.search(r'bs-(\d+[kmg]?)', filename, re.IGNORECASE)
    if match:
        return match.group(1).lower()
    return 'unknown'

def extract_operation_from_filename(filename):
    """
    Extract operation from filename (e.g., 'fio-test-read-bs-4k.json' -> 'read')
    """
    match = re.search(r'fio-test-(\w+)-bs-', filename, re.IGNORECASE)
    if match:
        return match.group(1).lower()
    return 'unknown'

def get_block_size_display_name(block_size):
    """
    Convert block size to display name (e.g., '4k' -> '4KB', '1024k' -> '1MB')
    """
    if block_size.endswith('k'):
        size_num = int(block_size[:-1])
        if size_num >= 1024:
            return f"{size_num // 1024}MB"
        else:
            return f"{size_num}KB"
    elif block_size.endswith('m'):
        return f"{block_size[:-1]}MB"
    elif block_size.endswith('g'):
        return f"{block_size[:-1]}GB"
    else:
        return f"{block_size}B"

def get_block_size_kb_display(block_size):
    """
    Keep block size in original k format for legend display (e.g., '4k' -> '4k', '1024k' -> '1024k')
    Preserves the original k format as used in FIO testing.
    """
    if block_size.endswith('k'):
        # Keep original k format
        return block_size
    elif block_size.endswith('m'):
        # Convert m to k format
        size_num = int(block_size[:-1])
        return f"{size_num * 1024}k"
    elif block_size.endswith('g'):
        # Convert g to k format
        size_num = int(block_size[:-1])
        return f"{size_num * 1024 * 1024}k"
    else:
        # For numeric values without suffix, add k
        return f"{block_size}k"

def extract_metrics_from_json(json_file_path, metric_type='iops'):
    """
    Extract IOPS or bandwidth data from a single FIO JSON file.
    Returns a dictionary with operation, block_size, and metric values.
    """
    
    try:
        with open(json_file_path, 'r') as f:
            data = json.load(f)
        
        # Extract operation and block size from filename
        filename = os.path.basename(json_file_path)
        operation = extract_operation_from_filename(filename)
        block_size = extract_block_size_from_filename(filename)
        
        # Skip files that don't have block size in their filename
        if block_size == 'unknown':
            print(f"    Skipping {filename}: No block size found in filename")
            return None
        
        # Sum up metrics across all jobs (in case multiple FIO jobs are running)
        total_read_iops = 0
        total_write_iops = 0
        total_read_bw = 0
        total_write_bw = 0
        total_read_lat_ns = 0
        total_write_lat_ns = 0
        job_count = 0
        
        for job in data['jobs']:
            # Extract IOPS data
            read_iops = job['read']['iops'] if 'read' in job else 0
            write_iops = job['write']['iops'] if 'write' in job else 0
            
            # Extract bandwidth data
            read_bw = job['read']['bw_mean'] if 'read' in job else 0
            write_bw = job['write']['bw_mean'] if 'write' in job else 0
            
            # Extract latency data
            read_lat_ns = job['read']['lat_ns']['mean'] if 'read' in job and 'lat_ns' in job['read'] else 0
            write_lat_ns = job['write']['lat_ns']['mean'] if 'write' in job and 'lat_ns' in job['write'] else 0
            
            # Sum up all metrics
            total_read_iops += read_iops
            total_write_iops += write_iops
            total_read_bw += read_bw
            total_write_bw += write_bw
            total_read_lat_ns += read_lat_ns
            total_write_lat_ns += write_lat_ns
            job_count += 1
        
        # Calculate average latency (since latency should be averaged, not summed)
        avg_read_lat_ns = total_read_lat_ns / job_count if job_count > 0 else 0
        avg_write_lat_ns = total_write_lat_ns / job_count if job_count > 0 else 0
        
        # Convert latency from nanoseconds to milliseconds
        read_lat_ms = avg_read_lat_ns / 1000000 if avg_read_lat_ns > 0 else 0
        write_lat_ms = avg_write_lat_ns / 1000000 if avg_write_lat_ns > 0 else 0
        
        # Return data based on metric type
        if metric_type == 'bw':
            return {
                'operation': operation,
                'block_size': block_size,
                'read_metric': total_read_bw,  # bandwidth in KB/s (sum of all jobs)
                'write_metric': total_write_bw,  # bandwidth in KB/s (sum of all jobs)
                'read_iops': int(total_read_iops),  # keep for context (sum of all jobs)
                'write_iops': int(total_write_iops),  # keep for context (sum of all jobs)
                'read_bw_kbps': total_read_bw,
                'write_bw_kbps': total_write_bw,
                'read_lat_ms': read_lat_ms,
                'write_lat_ms': write_lat_ms,
                'file': filename
            }
        else:  # iops (default)
            return {
                'operation': operation,
                'block_size': block_size,
                'read_metric': int(total_read_iops),  # IOPS (sum of all jobs)
                'write_metric': int(total_write_iops),  # IOPS (sum of all jobs)
                'read_iops': int(total_read_iops),
                'write_iops': int(total_write_iops),
                'read_bw_kbps': total_read_bw,
                'write_bw_kbps': total_write_bw,
                'read_lat_ms': read_lat_ms,
                'write_lat_ms': write_lat_ms,
                'file': filename
            }
        
    except Exception as e:
        print(f"Error processing {json_file_path}: {e}")
        return None

# Keep the old function name for backward compatibility
def extract_iops_from_json(json_file_path):
    """Backward compatibility wrapper for extract_metrics_from_json"""
    return extract_metrics_from_json(json_file_path, 'iops')

def process_test_directory(test_dir, test_name=None, metric_type='iops'):
    """
    Process all VM directories in a test directory.
    Returns a dictionary with results organized by (test_name, vm_name, operation, block_size).
    """
    results = {}
    test_path = Path(test_dir)
    
    if not test_path.exists():
        print(f"Warning: Test directory {test_dir} does not exist")
        return results
    
    # Find all host directories (any subdir that contains at least one JSON file)
    vm_dirs = []
    for d in test_path.iterdir():
        if not d.is_dir():
            continue
        try:
            has_json = any(child.suffix == '.json' for child in d.iterdir())
        except Exception:
            has_json = False
        if has_json:
            vm_dirs.append(d)
    
    if not vm_dirs:
        print(f"Warning: No host directories with JSON files found in {test_dir}")
        return results
    
    # Use directory name as test name if not provided
    if test_name is None:
        test_name = test_path.name
    
    print(f"Processing {test_name}: Found {len(vm_dirs)} VM directories")
    
    for vm_dir in vm_dirs:
        vm_name = vm_dir.name
        print(f"  Processing {vm_name}...")
        
        # Find all JSON files in the VM directory
        json_files = list(vm_dir.glob('*.json'))
        
        if not json_files:
            print(f"    Warning: No JSON files found in {vm_dir}")
            continue
        
        print(f"    Found {len(json_files)} JSON files")
        
        for json_file in json_files:
            data = extract_metrics_from_json(json_file, metric_type)
            if data:
                key = (test_name, vm_name, data['operation'], data['block_size'])
                results[key] = {
                    'test_name': test_name,
                    'vm_name': vm_name,
                    'operation': data['operation'],
                    'block_size': data['block_size'],
                    'read_metric': data['read_metric'],
                    'write_metric': data['write_metric'],
                    'read_iops': data['read_iops'],
                    'write_iops': data['write_iops'],
                    'read_bw_kbps': data['read_bw_kbps'],
                    'write_bw_kbps': data['write_bw_kbps'],
                    'read_lat_ms': data['read_lat_ms'],
                    'write_lat_ms': data['write_lat_ms'],
                    'file': data['file']
                }
    
    return results

def create_comparison_dataframe(all_results):
    """
    Create a pandas DataFrame for comparison analysis.
    """
    if not all_results:
        return pd.DataFrame()
    
    # Convert to list of dictionaries
    data_list = list(all_results.values())
    df = pd.DataFrame(data_list)
    
    # Add display names
    df['block_size_display'] = df['block_size'].apply(get_block_size_display_name)
    df['test_vm'] = df['test_name'] + '_' + df['vm_name']
    
    # Sort by operation, block size, and test
    df = df.sort_values(['operation', 'block_size', 'test_name', 'vm_name'])
    
    return df

def create_comparison_summary_csv(df, output_dir, metric_type='iops'):
    """
    Create summary CSV files for comparison analysis.
    """
    if df.empty:
        return []
    
    csv_files = []
    
    # Create overall comparison summary
    summary_file = os.path.join(output_dir, 'test_comparison_summary.csv')
    df.to_csv(summary_file, index=False)
    csv_files.append(summary_file)
    print(f"Created comparison summary: {summary_file}")
    
    # Create summary by operation and block size
    for operation in df['operation'].unique():
        op_data = df[df['operation'] == operation]
        
        # Create pivot table for comparison (using sum instead of mean)
        metric_col = 'read_metric' if metric_type == 'bw' else 'read_iops'
        pivot_read = op_data.pivot_table(
            values=metric_col, 
            index=['vm_name', 'block_size_display'], 
            columns='test_name', 
            aggfunc='sum'
        ).round(0)
        
        metric_col = 'write_metric' if metric_type == 'bw' else 'write_iops'
        pivot_write = op_data.pivot_table(
            values=metric_col, 
            index=['vm_name', 'block_size_display'], 
            columns='test_name',
            aggfunc='sum'
        ).round(0)
        
        # Save read comparison
        metric_suffix = 'bw' if metric_type == 'bw' else 'iops'
        read_file = os.path.join(output_dir, f'comparison_{operation}_read_{metric_suffix}.csv')
        pivot_read.to_csv(read_file)
        csv_files.append(read_file)
        print(f"Created read comparison: {read_file}")
        
        # Save write comparison
        write_file = os.path.join(output_dir, f'comparison_{operation}_write_{metric_suffix}.csv')
        pivot_write.to_csv(write_file)
        csv_files.append(write_file)
        print(f"Created write comparison: {write_file}")
    
    return csv_files


def create_comparison_graphs(df, output_dir, graph_type='both', metric_type='iops', block_size_filter=None, summary_only=False):
    """
    Create summary comparison graphs showing test1 vs test2 results for each operation.
    Similar to iops_analyzer.py operation summary graphs.
    
    Args:
        df: DataFrame with comparison data
        output_dir: Directory to save graphs
        graph_type: Type of graphs ('bar', 'line', 'both')
        metric_type: Type of metric ('iops' or 'bw')
        block_size_filter: List of block sizes to filter
        summary_only: If True, skip per-VM graphs and only create summary graphs
    """
    if not HAS_PLOTTING:
        print("Cannot create graphs: plotting libraries not available")
        return 0
    
    if df.empty:
        print("No data available for graph creation")
        return 0
    
    success_count = 0
    
    # Set up the plotting style
    plt.style.use('default')
    
    # Create summary graphs for each operation
    for operation in df['operation'].unique():
        op_data = df[df['operation'] == operation]
        
        # Get unique block sizes and test names
        all_block_sizes = sorted(op_data['block_size'].unique(), key=lambda x: int(x.replace('k', '')))
        
        # Apply block size filter if specified
        if block_size_filter:
            # Convert filter to lowercase for case-insensitive matching
            filter_lower = [bs.lower() for bs in block_size_filter]
            block_sizes = [bs for bs in all_block_sizes if bs.lower() in filter_lower]
            if not block_sizes:
                print(f"Skipping {operation}: No block sizes match the filter {block_size_filter}")
                continue
            print(f"Filtering {operation} to block sizes: {block_sizes}")
        else:
            block_sizes = all_block_sizes
            
        test_names = sorted(op_data['test_name'].unique())
        
        if len(test_names) < 2:
            print(f"Skipping {operation}: Need at least 2 tests for comparison")
            continue
        
        # Create summary comparison graphs (Average and Total separately, using same approach as analyze_bw_mean_with_graphs.py)
        if graph_type in ['bar', 'both']:
            success_count += create_operation_summary_comparison_avg_bar_chart(op_data, operation, block_sizes, test_names, output_dir, metric_type)
            success_count += create_operation_summary_comparison_total_bar_chart(op_data, operation, block_sizes, test_names, output_dir, metric_type)
            # Create new total comparison graphs showing Total All VMs for each test run
            success_count += create_total_all_vms_comparison_bar_chart(op_data, operation, block_sizes, test_names, output_dir, metric_type)
        
        if graph_type in ['line', 'both']:
            success_count += create_operation_summary_comparison_avg_line_chart(op_data, operation, block_sizes, test_names, output_dir, metric_type)
            success_count += create_operation_summary_comparison_total_line_chart(op_data, operation, block_sizes, test_names, output_dir, metric_type)
            # Create new total comparison graphs showing Total All VMs for each test run
            success_count += create_total_all_vms_comparison_line_chart(op_data, operation, block_sizes, test_names, output_dir, metric_type)
        
        # Create per-VM comparison graphs (like iops_analyzer.py) - skip if summary_only is True
        if not summary_only:
            if graph_type in ['bar', 'both']:
                success_count += create_per_vm_comparison_bar_chart(op_data, operation, block_sizes, test_names, output_dir, metric_type)
            
            if graph_type in ['line', 'both']:
                success_count += create_per_vm_comparison_line_chart(op_data, operation, block_sizes, test_names, output_dir, metric_type)
    
    return success_count

def create_operation_summary_comparison_bar_chart(df, operation, block_sizes, test_names, output_dir, metric_type='iops'):
    """
    Create a summary bar chart comparing test results for an operation.
    Shows the AVERAGE of all VMs for each test directory.
    """
    try:
        # Create figure - increased height for better spacing
        plt.figure(figsize=(14, 12))
        
        # Prepare data for plotting
        x_pos = np.arange(len(block_sizes))
        width = 0.8 / len(test_names)  # Width of each bar group
        
        colors = plt.cm.Set3(np.linspace(0, 1, len(test_names)))
        
        # Create bar plot for each test
        for i, test_name in enumerate(test_names):
            test_data = df[df['test_name'] == test_name]
            avg_values = []
            
            legend_label = get_clean_test_name(test_name)
            
            for block_size in block_sizes:
                bs_data = test_data[test_data['block_size'] == block_size]
                # For read operations, use read_metric; for write operations, use write_metric
                if operation in ['read', 'randread']:
                    metric_col = 'read_metric' if metric_type == 'bw' else 'read_iops'
                    avg_metric = bs_data[metric_col].mean() if not bs_data.empty else 0
                else:
                    metric_col = 'write_metric' if metric_type == 'bw' else 'write_iops'
                    avg_metric = bs_data[metric_col].mean() if not bs_data.empty else 0
                avg_values.append(avg_metric)
            
            offset = (i - len(test_names)/2 + 0.5) * width
            bar_positions = [pos + offset for pos in x_pos]
            bars = plt.bar(bar_positions, avg_values, 
                          width=width, label=legend_label, color=colors[i], alpha=0.8,
                          edgecolor='black', linewidth=0.5)
            
        # Build right-side summary box with values per block size
        metric_name = 'Bandwidth (bw_mean)' if metric_type == 'bw' else 'IOPS (iops_mean)'
        summary_lines = [f"Values per block size ({metric_name}):"]
        for bs in block_sizes:
            bs_label = get_block_size_display_name(bs)
            summary_lines.append(f"\n{bs_label}:")
            for i, test_name in enumerate(test_names):
                test_data = df[df['test_name'] == test_name]
                bs_data = test_data[test_data['block_size'] == bs]
                if operation in ['read', 'randread']:
                    metric_col = 'read_metric' if metric_type == 'bw' else 'read_iops'
                else:
                    metric_col = 'write_metric' if metric_type == 'bw' else 'write_iops'
                value = bs_data[metric_col].mean() if not bs_data.empty else 0
                summary_lines.append(f"  {get_clean_test_name(test_name)}: {value:.0f}")
        
        
        # Customize the plot
        plt.title(f'Average {metric_name} Performance Comparison (Bar Chart): {operation.upper()} - Block Size Comparison', 
                 fontsize=16, fontweight='bold', pad=20)
        
        
        plt.xlabel('Block Size', fontsize=12, fontweight='bold')
        y_label = f'{metric_name} [KB]' if metric_type == 'bw' else metric_name
        plt.ylabel(y_label, fontsize=12, fontweight='bold')
        
        # Set x-axis ticks
        plt.xticks(x_pos, [get_block_size_display_name(bs) for bs in block_sizes])
        
        # Add legend outside the graph on the right side (consistent with bar charts)
        plt.legend(title='Test', bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=10)
        
        # Add grid for better readability
        plt.grid(axis='y', alpha=0.3, linestyle='--')
        
        # Place summary box on the right
        plt.figtext(1.33, 0.15, "\n".join(summary_lines).strip(),
                   ha='right', va='bottom', fontsize=10, fontweight='bold',
                   bbox=dict(boxstyle='round,pad=0.5', facecolor='lightgray', alpha=0.8))
        
        # Build right-side summary box with values per block size (TOTAL)
        metric_name = 'Bandwidth (bw_mean)' if metric_type == 'bw' else 'IOPS (iops_mean)'
        summary_lines = [f"Values per block size ({metric_name}):"]
        for bs in block_sizes:
            bs_label = get_block_size_display_name(bs)
            summary_lines.append(f"\n{bs_label}:")
            for i, test_name in enumerate(test_names):
                test_data = df[df['test_name'] == test_name]
                bs_data = test_data[test_data['block_size'] == bs]
                if operation in ['read', 'randread']:
                    metric_col = 'read_metric' if metric_type == 'bw' else 'read_iops'
                else:
                    metric_col = 'write_metric' if metric_type == 'bw' else 'write_iops'
                value = bs_data[metric_col].sum() if not bs_data.empty else 0
                summary_lines.append(f"  {get_clean_test_name(test_name)}: {value:.0f}")

        # Place summary box on the right
        plt.figtext(1.25, 0.15, "\n".join(summary_lines).strip(), 
                   ha='right', va='bottom', fontsize=10, fontweight='bold',
                   bbox=dict(boxstyle='round,pad=0.5', facecolor='lightgray', alpha=0.8))

        # Adjust layout to accommodate legend and summary
        plt.tight_layout()
        plt.subplots_adjust(right=0.65)
        
        # Save the chart
        metric_suffix = 'bw' if metric_type == 'bw' else 'iops'
        output_file = os.path.join(output_dir, f'comparison_{operation}_summary_bar_{metric_suffix}.png')
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Created summary bar chart: {output_file}")
        
        plt.close()
        return 1
        
    except Exception as e:
        print(f"Error creating summary bar chart for {operation}: {e}")
        return 0

def create_operation_summary_comparison_avg_bar_chart(df, operation, block_sizes, test_names, output_dir, metric_type='iops'):
    """
    Create a summary bar chart comparing AVERAGE test results for an operation.
    Uses the same calculation approach as analyze_bw_mean_with_graphs.py.
    Shows the AVERAGE of all VMs for each test directory.
    """
    try:
        # Create figure - increased height for better spacing
        plt.figure(figsize=(14, 12))
        
        # Prepare data for plotting
        x_pos = np.arange(len(block_sizes))
        width = 0.8 / len(test_names)  # Width of each bar group
        
        colors = plt.cm.Set3(np.linspace(0, 1, len(test_names)))
        
        # Create bar plot for each test
        for i, test_name in enumerate(test_names):
            test_data = df[df['test_name'] == test_name]
            avg_values = []
            
            legend_label = get_clean_test_name(test_name)
            
            for block_size in block_sizes:
                bs_data = test_data[test_data['block_size'] == block_size]
                # For read operations, use read_metric; for write operations, use write_metric
                if operation in ['read', 'randread']:
                    metric_col = 'read_metric' if metric_type == 'bw' else 'read_iops'
                    # Use same approach as analyze_bw_mean_with_graphs.py: sum all jobs per machine, then average across machines
                    avg_metric = bs_data[metric_col].mean() if not bs_data.empty else 0
                else:
                    metric_col = 'write_metric' if metric_type == 'bw' else 'write_iops'
                    # Use same approach as analyze_bw_mean_with_graphs.py: sum all jobs per machine, then average across machines
                    avg_metric = bs_data[metric_col].mean() if not bs_data.empty else 0
                avg_values.append(avg_metric)
            
            offset = (i - len(test_names)/2 + 0.5) * width
            bar_positions = [pos + offset for pos in x_pos]
            bars = plt.bar(bar_positions, avg_values, 
                          width=width, label=legend_label, color=colors[i], alpha=0.8,
                          edgecolor='black', linewidth=0.5)
            
        # Build right-side summary box with values per block size
        metric_name = 'Bandwidth (bw_mean)' if metric_type == 'bw' else 'IOPS (iops_mean)'
        summary_lines = [f"Values per block size ({metric_name}):"]
        for bs in block_sizes:
            bs_label = get_block_size_display_name(bs)
            summary_lines.append(f"\n{bs_label}:")
            for i, test_name in enumerate(test_names):
                test_data = df[df['test_name'] == test_name]
                bs_data = test_data[test_data['block_size'] == bs]
                if operation in ['read', 'randread']:
                    metric_col = 'read_metric' if metric_type == 'bw' else 'read_iops'
                else:
                    metric_col = 'write_metric' if metric_type == 'bw' else 'write_iops'
                value = bs_data[metric_col].mean() if not bs_data.empty else 0
                summary_lines.append(f"  {get_clean_test_name(test_name)}: {value:.0f}")
        
        # Customize the plot
        metric_name = 'Bandwidth (bw_mean)' if metric_type == 'bw' else 'IOPS (iops_mean)'
        plt.title(f'Average {metric_name} Performance Comparison (Bar Chart): {operation.upper()} - Block Size Comparison', 
                 fontsize=16, fontweight='bold', pad=20)
        
        plt.xlabel('Block Size', fontsize=12, fontweight='bold')
        y_label = f'Average {metric_name} [KB]' if metric_type == 'bw' else f'Average {metric_name}'
        plt.ylabel(y_label, fontsize=12, fontweight='bold')
        
        # Set x-axis ticks
        plt.xticks(x_pos, [get_block_size_display_name(bs) for bs in block_sizes])
        
        # Add legend outside the graph on the right side
        plt.legend(title='Test', bbox_to_anchor=(1.15, 1), loc='upper left', fontsize=10)
        
        
        # Add grid for better readability
        plt.grid(axis='y', alpha=0.3, linestyle='--')
        
        # Place summary box on the right
        plt.figtext(1.25, 0.15, "\n".join(summary_lines).strip(), 
                   ha='right', va='bottom', fontsize=10, fontweight='bold',
                   bbox=dict(boxstyle='round,pad=0.5', facecolor='lightgray', alpha=0.8))
        
        # Adjust layout to accommodate legend and summary
        plt.tight_layout()
        plt.subplots_adjust(right=0.65)
        
        # Save the chart
        metric_suffix = 'bw' if metric_type == 'bw' else 'iops'
        output_file = os.path.join(output_dir, f'Avg_{operation}_summary_bar_{metric_suffix}.png')
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Created average summary bar chart: {output_file}")
        
        plt.close()
        return 1
        
    except Exception as e:
        print(f"Error creating average summary bar chart for {operation}: {e}")
        return 0

def create_operation_summary_comparison_total_bar_chart(df, operation, block_sizes, test_names, output_dir, metric_type='iops'):
    """
    Create a summary bar chart comparing TOTAL test results for an operation.
    Uses the same calculation approach as analyze_bw_mean_with_graphs.py.
    Shows the TOTAL of all VMs for each test directory.
    """
    try:
        # Create figure - increased height for better spacing
        plt.figure(figsize=(14, 12))
        
        # Prepare data for plotting
        x_pos = np.arange(len(block_sizes))
        width = 0.8 / len(test_names)  # Width of each bar group
        
        colors = plt.cm.Set3(np.linspace(0, 1, len(test_names)))
        
        # Create bar plot for each test
        for i, test_name in enumerate(test_names):
            test_data = df[df['test_name'] == test_name]
            total_values = []
            
            legend_label = get_clean_test_name(test_name)
            
            for block_size in block_sizes:
                bs_data = test_data[test_data['block_size'] == block_size]
                # For read operations, use read_metric; for write operations, use write_metric
                if operation in ['read', 'randread']:
                    metric_col = 'read_metric' if metric_type == 'bw' else 'read_iops'
                    # Use same approach as analyze_bw_mean_with_graphs.py: sum all jobs per machine, then sum across all machines
                    total_metric = bs_data[metric_col].sum() if not bs_data.empty else 0
                else:
                    metric_col = 'write_metric' if metric_type == 'bw' else 'write_iops'
                    # Use same approach as analyze_bw_mean_with_graphs.py: sum all jobs per machine, then sum across all machines
                    total_metric = bs_data[metric_col].sum() if not bs_data.empty else 0
                total_values.append(total_metric)
            
            offset = (i - len(test_names)/2 + 0.5) * width
            bar_positions = [pos + offset for pos in x_pos]
            bars = plt.bar(bar_positions, total_values, 
                          width=width, label=legend_label, color=colors[i], alpha=0.8,
                          edgecolor='black', linewidth=0.5)
            
        # Build right-side summary box with values per block size
        metric_name = 'Bandwidth (bw_mean)' if metric_type == 'bw' else 'IOPS (iops_mean)'
        summary_lines = [f"Values per block size ({metric_name}):"]
        for bs in block_sizes:
            bs_label = get_block_size_display_name(bs)
            summary_lines.append(f"\n{bs_label}:")
            for i, test_name in enumerate(test_names):
                test_data = df[df['test_name'] == test_name]
                bs_data = test_data[test_data['block_size'] == bs]
                if operation in ['read', 'randread']:
                    metric_col = 'read_metric' if metric_type == 'bw' else 'read_iops'
                else:
                    metric_col = 'write_metric' if metric_type == 'bw' else 'write_iops'
                value = bs_data[metric_col].sum() if not bs_data.empty else 0
                summary_lines.append(f"  {get_clean_test_name(test_name)}: {value:.0f}")
        
        # Customize the plot
        metric_name = 'Bandwidth (bw_mean)' if metric_type == 'bw' else 'IOPS (iops_mean)'
        plt.title(f'Total {metric_name} Performance Comparison (Bar Chart): {operation.upper()} - Block Size Comparison', 
                 fontsize=16, fontweight='bold', pad=20)
        
        plt.xlabel('Block Size', fontsize=12, fontweight='bold')
        y_label = f'Total {metric_name} [KB]' if metric_type == 'bw' else f'Total {metric_name}'
        plt.ylabel(y_label, fontsize=12, fontweight='bold')
        
        # Set x-axis ticks
        plt.xticks(x_pos, [get_block_size_display_name(bs) for bs in block_sizes])
        
        # Add legend outside the graph on the right side
        plt.legend(title='Test', bbox_to_anchor=(1.15, 1), loc='upper left', fontsize=10)
        
        
        # Add grid for better readability
        plt.grid(axis='y', alpha=0.3, linestyle='--')
        
        # Place summary box on the right
        plt.figtext(1.25, 0.15, "\n".join(summary_lines).strip(), 
                   ha='right', va='bottom', fontsize=10, fontweight='bold',
                   bbox=dict(boxstyle='round,pad=0.5', facecolor='lightgray', alpha=0.8))
        
        # Adjust layout to accommodate legend and summary
        plt.tight_layout()
        plt.subplots_adjust(right=0.65)
        
        # Save the chart
        metric_suffix = 'bw' if metric_type == 'bw' else 'iops'
        output_file = os.path.join(output_dir, f'Total_{operation}_summary_bar_{metric_suffix}.png')
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Created total summary bar chart: {output_file}")
        
        plt.close()
        return 1
        
    except Exception as e:
        print(f"Error creating total summary bar chart for {operation}: {e}")
        return 0

def create_operation_summary_comparison_line_chart(df, operation, block_sizes, test_names, output_dir, metric_type='iops'):
    """
    Create a summary line chart comparing test results for an operation.
    Shows the AVERAGE of all VMs for each test directory.
    """
    try:
        # Create figure - increased height for better spacing
        plt.figure(figsize=(14, 12))
        
        colors = plt.cm.Set3(np.linspace(0, 1, len(test_names)))
        
        # Create line plot for each test
        for i, test_name in enumerate(test_names):
            test_data = df[df['test_name'] == test_name]
            avg_values = []
            
            legend_label = get_clean_test_name(test_name)
            
            for block_size in block_sizes:
                bs_data = test_data[test_data['block_size'] == block_size]
                # For read operations, use read_metric; for write operations, use write_metric
                if operation in ['read', 'randread']:
                    metric_col = 'read_metric' if metric_type == 'bw' else 'read_iops'
                    avg_metric = bs_data[metric_col].mean() if not bs_data.empty else 0
                else:
                    metric_col = 'write_metric' if metric_type == 'bw' else 'write_iops'
                    avg_metric = bs_data[metric_col].mean() if not bs_data.empty else 0
                avg_values.append(avg_metric)
            
            plt.plot(block_sizes, avg_values, marker='o', linewidth=2, markersize=6,
                    label=legend_label, color=colors[i], 
                    markerfacecolor=colors[i], markeredgecolor='black', markeredgewidth=1)
            
            # Store data for summary display below legend
            if not hasattr(plt.gca(), '_line_data_summary'):
                plt.gca()._line_data_summary = []
            
            for j, (block_size, avg_val) in enumerate(zip(block_sizes, avg_values)):
                if avg_val > 0:  # Only show values for non-zero averages
                    # Store original block size, not the converted display name
                    plt.gca()._line_data_summary.append({
                        'test_name': test_name,
                        'block_size': block_size,
                        'value': avg_val,
                        'color': colors[i]
                    })
        
        # Customize the plot
        metric_name = 'Bandwidth (bw_mean)' if metric_type == 'bw' else 'IOPS (iops_mean)'
        plt.title(f'Average {metric_name} Performance Comparison (Line Chart): {operation.upper()} - Block Size Comparison', 
                 fontsize=16, fontweight='bold', pad=20)
        
        plt.xlabel('Block Size', fontsize=12, fontweight='bold')
        y_label = f'{metric_name} [KB]' if metric_type == 'bw' else metric_name
        plt.ylabel(y_label, fontsize=12, fontweight='bold')
        
        # Set x-axis ticks
        plt.xticks(block_sizes, [get_block_size_display_name(bs) for bs in block_sizes])
        
        # Add legend outside the graph on the right side (same as bar charts)
        plt.legend(title='Test', bbox_to_anchor=(1.15, 1), loc='upper left', fontsize=10)
        
        # Add grid for better readability
        plt.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
        
        # Set y-axis to start from 0 for better visualization
        plt.ylim(bottom=0)
        
        # Build right-side summary box with values per block size
        summary_lines = [f"Values per block size ({metric_name}):"]
        for bs in block_sizes:
            bs_label = get_block_size_display_name(bs)
            summary_lines.append(f"\n{bs_label}:")
            for i, test_name in enumerate(test_names):
                test_data = df[df['test_name'] == test_name]
                bs_data = test_data[test_data['block_size'] == bs]
                if operation in ['read', 'randread']:
                    metric_col = 'read_metric' if metric_type == 'bw' else 'read_iops'
                else:
                    metric_col = 'write_metric' if metric_type == 'bw' else 'write_iops'
                value = bs_data[metric_col].mean() if not bs_data.empty else 0
                summary_lines.append(f"  {get_clean_test_name(test_name)}: {value:.0f}")
        
        # Place summary box on the right
        plt.figtext(1.45, 0.15, "\n".join(summary_lines).strip(),
                   ha='right', va='bottom', fontsize=10, fontweight='bold',
                   bbox=dict(boxstyle='round,pad=0.5', facecolor='lightgray', alpha=0.8))

        # Adjust layout to accommodate legend and summary
        plt.tight_layout()
        plt.subplots_adjust(right=0.65)
        
        # Save the chart
        metric_suffix = 'bw' if metric_type == 'bw' else 'iops'
        output_file = os.path.join(output_dir, f'comparison_{operation}_summary_line_{metric_suffix}.png')
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Created summary line chart: {output_file}")
        
        plt.close()
        return 1
        
    except Exception as e:
        print(f"Error creating summary line chart for {operation}: {e}")
        return 0


def generate_comparison_report(df, output_dir, metric_type='iops'):
    """
    Generate a text report comparing the test results.
    """
    if df.empty:
        return
    
    report_file = os.path.join(output_dir, 'comparison_report.txt')
    
    with open(report_file, 'w') as f:
        f.write("FIO Test Comparison Report\n")
        f.write("=" * 50 + "\n\n")
        
        # Overall statistics
        test_names = sorted(df['test_name'].unique())
        vm_names = sorted(df['vm_name'].unique())
        operations = sorted(df['operation'].unique())
        block_sizes = sorted(df['block_size'].unique(), key=lambda x: int(x.replace('k', '')))
        
        f.write(f"Tests compared: {', '.join(test_names)}\n")
        f.write(f"VMs analyzed: {', '.join(vm_names)}\n")
        f.write(f"Operations: {', '.join(operations)}\n")
        f.write(f"Block sizes: {', '.join([get_block_size_display_name(bs) for bs in block_sizes])}\n")
        f.write(f"Metric types: IOPS (iops_mean), Bandwidth (bw_mean)\n\n")
        
        # Performance comparison by operation
        for operation in operations:
            f.write(f"{operation.upper()} Operation Analysis\n")
            f.write("-" * 30 + "\n")
            
            op_data = df[df['operation'] == operation]
            
            for block_size in block_sizes:
                bs_data = op_data[op_data['block_size'] == block_size]
                
                if bs_data.empty:
                    continue
                
                f.write(f"\nBlock Size: {get_block_size_display_name(block_size)}\n")
                
                # Read IOPS comparison (sum across all VMs)
                read_iops_comparison = bs_data.groupby('test_name')['read_iops'].sum().round(0)
                f.write(f"Read IOPS (Total across all VMs):\n")
                for test_name, value in read_iops_comparison.items():
                    f.write(f"  {test_name}: {value:,.0f} IOPS\n")
                
                # Read IOPS comparison (average per VM)
                read_iops_avg_comparison = bs_data.groupby('test_name')['read_iops'].mean().round(0)
                f.write(f"Read IOPS (Average per VM):\n")
                for test_name, value in read_iops_avg_comparison.items():
                    f.write(f"  {test_name}: {value:,.0f} IOPS\n")
                
                # Read Bandwidth comparison (sum across all VMs)
                read_bw_comparison = bs_data.groupby('test_name')['read_bw_kbps'].sum().round(0)
                f.write(f"Read Bandwidth (Total across all VMs):\n")
                for test_name, value in read_bw_comparison.items():
                    f.write(f"  {test_name}: {value:,.0f} KB/s\n")
                
                # Read Bandwidth comparison (average per VM)
                read_bw_avg_comparison = bs_data.groupby('test_name')['read_bw_kbps'].mean().round(0)
                f.write(f"Read Bandwidth (Average per VM):\n")
                for test_name, value in read_bw_avg_comparison.items():
                    f.write(f"  {test_name}: {value:,.0f} KB/s\n")
                
                # Write IOPS comparison (sum across all VMs)
                write_iops_comparison = bs_data.groupby('test_name')['write_iops'].sum().round(0)
                f.write(f"Write IOPS (Total across all VMs):\n")
                for test_name, value in write_iops_comparison.items():
                    f.write(f"  {test_name}: {value:,.0f} IOPS\n")
                
                # Write IOPS comparison (average per VM)
                write_iops_avg_comparison = bs_data.groupby('test_name')['write_iops'].mean().round(0)
                f.write(f"Write IOPS (Average per VM):\n")
                for test_name, value in write_iops_avg_comparison.items():
                    f.write(f"  {test_name}: {value:,.0f} IOPS\n")
                
                # Write Bandwidth comparison (sum across all VMs)
                write_bw_comparison = bs_data.groupby('test_name')['write_bw_kbps'].sum().round(0)
                f.write(f"Write Bandwidth (Total across all VMs):\n")
                for test_name, value in write_bw_comparison.items():
                    f.write(f"  {test_name}: {value:,.0f} KB/s\n")
                
                # Write Bandwidth comparison (average per VM)
                write_bw_avg_comparison = bs_data.groupby('test_name')['write_bw_kbps'].mean().round(0)
                f.write(f"Write Bandwidth (Average per VM):\n")
                for test_name, value in write_bw_avg_comparison.items():
                    f.write(f"  {test_name}: {value:,.0f} KB/s\n")
                
                # Calculate improvement percentages for multiple tests
                if len(test_names) >= 2:
                    # Calculate improvement from first to last test
                    first_test = test_names[0]
                    last_test = test_names[-1]
                    
                    # Read IOPS improvements
                    first_read_iops_total = read_iops_comparison.get(first_test, 0)
                    last_read_iops_total = read_iops_comparison.get(last_test, 0)
                    first_read_iops_avg = read_iops_avg_comparison.get(first_test, 0)
                    last_read_iops_avg = read_iops_avg_comparison.get(last_test, 0)
                    
                    if first_read_iops_total > 0 and last_read_iops_total > 0:
                        read_iops_improvement_total = ((last_read_iops_total - first_read_iops_total) / first_read_iops_total) * 100
                        f.write(f"Read IOPS improvement - Total ({first_test} to {last_test}): {read_iops_improvement_total:+.1f}%\n")
                    
                    if first_read_iops_avg > 0 and last_read_iops_avg > 0:
                        read_iops_improvement_avg = ((last_read_iops_avg - first_read_iops_avg) / first_read_iops_avg) * 100
                        f.write(f"Read IOPS improvement - Average per VM ({first_test} to {last_test}): {read_iops_improvement_avg:+.1f}%\n")
                    
                    # Read Bandwidth improvements
                    first_read_bw_total = read_bw_comparison.get(first_test, 0)
                    last_read_bw_total = read_bw_comparison.get(last_test, 0)
                    first_read_bw_avg = read_bw_avg_comparison.get(first_test, 0)
                    last_read_bw_avg = read_bw_avg_comparison.get(last_test, 0)
                    
                    if first_read_bw_total > 0 and last_read_bw_total > 0:
                        read_bw_improvement_total = ((last_read_bw_total - first_read_bw_total) / first_read_bw_total) * 100
                        f.write(f"Read Bandwidth improvement - Total ({first_test} to {last_test}): {read_bw_improvement_total:+.1f}%\n")
                    
                    if first_read_bw_avg > 0 and last_read_bw_avg > 0:
                        read_bw_improvement_avg = ((last_read_bw_avg - first_read_bw_avg) / first_read_bw_avg) * 100
                        f.write(f"Read Bandwidth improvement - Average per VM ({first_test} to {last_test}): {read_bw_improvement_avg:+.1f}%\n")
                    
                    # Write IOPS improvements
                    first_write_iops_total = write_iops_comparison.get(first_test, 0)
                    last_write_iops_total = write_iops_comparison.get(last_test, 0)
                    first_write_iops_avg = write_iops_avg_comparison.get(first_test, 0)
                    last_write_iops_avg = write_iops_avg_comparison.get(last_test, 0)
                    
                    if first_write_iops_total > 0 and last_write_iops_total > 0:
                        write_iops_improvement_total = ((last_write_iops_total - first_write_iops_total) / first_write_iops_total) * 100
                        f.write(f"Write IOPS improvement - Total ({first_test} to {last_test}): {write_iops_improvement_total:+.1f}%\n")
                    
                    if first_write_iops_avg > 0 and last_write_iops_avg > 0:
                        write_iops_improvement_avg = ((last_write_iops_avg - first_write_iops_avg) / first_write_iops_avg) * 100
                        f.write(f"Write IOPS improvement - Average per VM ({first_test} to {last_test}): {write_iops_improvement_avg:+.1f}%\n")
                    
                    # Write Bandwidth improvements
                    first_write_bw_total = write_bw_comparison.get(first_test, 0)
                    last_write_bw_total = write_bw_comparison.get(last_test, 0)
                    first_write_bw_avg = write_bw_avg_comparison.get(first_test, 0)
                    last_write_bw_avg = write_bw_avg_comparison.get(last_test, 0)
                    
                    if first_write_bw_total > 0 and last_write_bw_total > 0:
                        write_bw_improvement_total = ((last_write_bw_total - first_write_bw_total) / first_write_bw_total) * 100
                        f.write(f"Write Bandwidth improvement - Total ({first_test} to {last_test}): {write_bw_improvement_total:+.1f}%\n")
                    
                    if first_write_bw_avg > 0 and last_write_bw_avg > 0:
                        write_bw_improvement_avg = ((last_write_bw_avg - first_write_bw_avg) / first_write_bw_avg) * 100
                        f.write(f"Write Bandwidth improvement - Average per VM ({first_test} to {last_test}): {write_bw_improvement_avg:+.1f}%\n")
                
                f.write("\n")
    
    print(f"Created comparison report: {report_file}")

def create_operation_summary_comparison_avg_line_chart(df, operation, block_sizes, test_names, output_dir, metric_type='iops'):
    """
    Create a summary line chart comparing AVERAGE test results for an operation.
    Uses the same calculation approach as analyze_bw_mean_with_graphs.py.
    Shows the AVERAGE of all VMs for each test directory.
    """
    try:
        # Create figure - increased height for better spacing
        plt.figure(figsize=(14, 12))
        
        colors = plt.cm.Set3(np.linspace(0, 1, len(test_names)))
        
        # Create line plot for each test
        for i, test_name in enumerate(test_names):
            test_data = df[df['test_name'] == test_name]
            avg_values = []
            
            legend_label = get_clean_test_name(test_name)
            
            for block_size in block_sizes:
                bs_data = test_data[test_data['block_size'] == block_size]
                # For read operations, use read_metric; for write operations, use write_metric
                if operation in ['read', 'randread']:
                    metric_col = 'read_metric' if metric_type == 'bw' else 'read_iops'
                    # Use same approach as analyze_bw_mean_with_graphs.py: sum all jobs per machine, then average across machines
                    avg_metric = bs_data[metric_col].mean() if not bs_data.empty else 0
                else:
                    metric_col = 'write_metric' if metric_type == 'bw' else 'write_iops'
                    # Use same approach as analyze_bw_mean_with_graphs.py: sum all jobs per machine, then average across machines
                    avg_metric = bs_data[metric_col].mean() if not bs_data.empty else 0
                avg_values.append(avg_metric)
            
            plt.plot(block_sizes, avg_values, marker='o', linewidth=2, markersize=6,
                    label=legend_label, color=colors[i], 
                    markerfacecolor=colors[i], markeredgecolor='black', markeredgewidth=1)
            
            # Store data for summary display below legend
            if not hasattr(plt.gca(), '_line_data_summary'):
                plt.gca()._line_data_summary = []
            
            for j, (block_size, avg_val) in enumerate(zip(block_sizes, avg_values)):
                if avg_val > 0:  # Only show values for non-zero averages
                    # Store original block size, not the converted display name
                    plt.gca()._line_data_summary.append({
                        'test_name': test_name,
                        'block_size': block_size,
                        'value': avg_val,
                        'color': colors[i]
                    })
        
        # Customize the plot
        metric_name = 'Bandwidth (bw_mean)' if metric_type == 'bw' else 'IOPS (iops_mean)'
        plt.title(f'Average {metric_name} Performance Comparison (Line Chart): {operation.upper()} - Block Size Comparison', 
                 fontsize=16, fontweight='bold', pad=20)
        
        plt.xlabel('Block Size', fontsize=12, fontweight='bold')
        y_label = f'Average {metric_name} [KB]' if metric_type == 'bw' else f'Average {metric_name}'
        plt.ylabel(y_label, fontsize=12, fontweight='bold')
        
        # Set x-axis ticks
        plt.xticks(block_sizes, [get_block_size_display_name(bs) for bs in block_sizes])
        
        # Add legend outside the graph on the right side (same as bar charts)
        plt.legend(title='Test', bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=10)
        
        # Add grid for better readability
        plt.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
        
        # Set y-axis to start from 0 for better visualization
        plt.ylim(bottom=0)
        
        # Build right-side summary box with values per block size
        summary_lines = [f"Values per block size ({metric_name}):"]
        for bs in block_sizes:
            bs_label = get_block_size_display_name(bs)
            summary_lines.append(f"\n{bs_label}:")
            for i, test_name in enumerate(test_names):
                test_data = df[df['test_name'] == test_name]
                bs_data = test_data[test_data['block_size'] == bs]
                if operation in ['read', 'randread']:
                    metric_col = 'read_metric' if metric_type == 'bw' else 'read_iops'
                else:
                    metric_col = 'write_metric' if metric_type == 'bw' else 'write_iops'
                value = bs_data[metric_col].mean() if not bs_data.empty else 0
                summary_lines.append(f"  {get_clean_test_name(test_name)}: {value:.0f}")
        
        # Place summary box on the right
        plt.figtext(1.25, 0.15, "\n".join(summary_lines).strip(), 
                   ha='right', va='bottom', fontsize=10, fontweight='bold',
                   bbox=dict(boxstyle='round,pad=0.5', facecolor='lightgray', alpha=0.8))

        # Adjust layout to accommodate legend and summary
        plt.tight_layout()
        plt.subplots_adjust(right=0.65)
        
        # Save the chart
        metric_suffix = 'bw' if metric_type == 'bw' else 'iops'
        output_file = os.path.join(output_dir, f'Avg_{operation}_summary_line_{metric_suffix}.png')
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Created average summary line chart: {output_file}")
        
        plt.close()
        return 1
        
    except Exception as e:
        print(f"Error creating average summary line chart for {operation}: {e}")
        return 0

def create_operation_summary_comparison_total_line_chart(df, operation, block_sizes, test_names, output_dir, metric_type='iops'):
    """
    Create a summary line chart comparing TOTAL test results for an operation.
    Uses the same calculation approach as analyze_bw_mean_with_graphs.py.
    Shows the TOTAL of all VMs for each test directory.
    """
    try:
        # Create figure - increased height for better spacing
        plt.figure(figsize=(14, 12))
        
        colors = plt.cm.Set3(np.linspace(0, 1, len(test_names)))
        
        # Create line plot for each test
        for i, test_name in enumerate(test_names):
            test_data = df[df['test_name'] == test_name]
            total_values = []
            
            legend_label = get_clean_test_name(test_name)
            
            for block_size in block_sizes:
                bs_data = test_data[test_data['block_size'] == block_size]
                # For read operations, use read_metric; for write operations, use write_metric
                if operation in ['read', 'randread']:
                    metric_col = 'read_metric' if metric_type == 'bw' else 'read_iops'
                    # Use same approach as analyze_bw_mean_with_graphs.py: sum all jobs per machine, then sum across all machines
                    total_metric = bs_data[metric_col].sum() if not bs_data.empty else 0
                else:
                    metric_col = 'write_metric' if metric_type == 'bw' else 'write_iops'
                    # Use same approach as analyze_bw_mean_with_graphs.py: sum all jobs per machine, then sum across all machines
                    total_metric = bs_data[metric_col].sum() if not bs_data.empty else 0
                total_values.append(total_metric)
            
            plt.plot(block_sizes, total_values, marker='o', linewidth=2, markersize=6,
                    label=legend_label, color=colors[i], 
                    markerfacecolor=colors[i], markeredgecolor='black', markeredgewidth=1)
            
            # Store data for summary display below legend
            if not hasattr(plt.gca(), '_line_data_summary'):
                plt.gca()._line_data_summary = []
            
            for j, (block_size, total_val) in enumerate(zip(block_sizes, total_values)):
                if total_val > 0:  # Only show values for non-zero totals
                    # Store original block size, not the converted display name
                    plt.gca()._line_data_summary.append({
                        'test_name': test_name,
                        'block_size': block_size,
                        'value': total_val,
                        'color': colors[i]
                    })
        
        # Customize the plot
        metric_name = 'Bandwidth (bw_mean)' if metric_type == 'bw' else 'IOPS (iops_mean)'
        plt.title(f'Total {metric_name} Performance Comparison (Line Chart): {operation.upper()} - Block Size Comparison', 
                 fontsize=16, fontweight='bold', pad=20)
        
        plt.xlabel('Block Size', fontsize=12, fontweight='bold')
        y_label = f'Total {metric_name} [KB]' if metric_type == 'bw' else f'Total {metric_name}'
        plt.ylabel(y_label, fontsize=12, fontweight='bold')
        
        # Set x-axis ticks
        plt.xticks(block_sizes, [get_block_size_display_name(bs) for bs in block_sizes])
        
        # Add legend outside the graph on the right side (same as bar charts)
        plt.legend(title='Test', bbox_to_anchor=(1.15, 1), loc='upper left', fontsize=10)
        
        # Add grid for better readability
        plt.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
        
        # Set y-axis to start from 0 for better visualization
        plt.ylim(bottom=0)
        
        # Build right-side summary box with values per block size (TOTAL)
        metric_name = 'Bandwidth (bw_mean)' if metric_type == 'bw' else 'IOPS (iops_mean)'
        summary_lines = [f"Values per block size ({metric_name}):"]
        for bs in block_sizes:
            bs_label = get_block_size_display_name(bs)
            summary_lines.append(f"\n{bs_label}:")
            for i, test_name in enumerate(test_names):
                test_data = df[df['test_name'] == test_name]
                bs_data = test_data[test_data['block_size'] == bs]
                if operation in ['read', 'randread']:
                    metric_col = 'read_metric' if metric_type == 'bw' else 'read_iops'
                else:
                    metric_col = 'write_metric' if metric_type == 'bw' else 'write_iops'
                value = bs_data[metric_col].sum() if not bs_data.empty else 0
                summary_lines.append(f"  {get_clean_test_name(test_name)}: {value:.0f}")

        # Place summary box on the right
        plt.figtext(1.25, 0.15, "\n".join(summary_lines).strip(), 
                   ha='right', va='bottom', fontsize=10, fontweight='bold',
                   bbox=dict(boxstyle='round,pad=0.5', facecolor='lightgray', alpha=0.8))
        
        # Adjust layout to accommodate legend and summary
        plt.tight_layout()
        plt.subplots_adjust(right=0.65)
        
        # Save the chart
        metric_suffix = 'bw' if metric_type == 'bw' else 'iops'
        output_file = os.path.join(output_dir, f'Total_{operation}_summary_line_{metric_suffix}.png')
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Created total summary line chart: {output_file}")
        
        plt.close()
        return 1
        
    except Exception as e:
        print(f"Error creating total summary line chart for {operation}: {e}")
        return 0

def create_total_all_vms_comparison_bar_chart(df, operation, block_sizes, test_names, output_dir, metric_type='iops'):
    """
    Create a bar chart comparing TOTAL ALL VMs values from different test runs for an operation.
    Shows the sum of all VMs for each test run on a single graph.
    """
    try:
        # Create figure - increased height for better spacing
        plt.figure(figsize=(14, 12))
        
        # Prepare data for plotting
        x_pos = np.arange(len(block_sizes))
        width = 0.8 / len(test_names)  # Adjust width based on number of tests
        
        colors = plt.cm.Set3(np.linspace(0, 1, len(test_names)))
        
        # Calculate total values for each test and block size
        test_totals = {}
        for test_name in test_names:
            test_totals[test_name] = []
            for block_size in block_sizes:
                # Get all VMs for this test and block size
                test_block_data = df[(df['test_name'] == test_name) & (df['block_size'] == block_size)]
                if not test_block_data.empty:
                    # For read operations, use read_metric; for write operations, use write_metric
                    if operation in ['read', 'randread']:
                        metric_col = 'read_metric' if metric_type == 'bw' else 'read_iops'
                    else:
                        metric_col = 'write_metric' if metric_type == 'bw' else 'write_iops'
                    # Sum all VMs for this test and block size
                    total_value = test_block_data[metric_col].sum()
                    test_totals[test_name].append(total_value)
                else:
                    test_totals[test_name].append(0)
        
        # Create bars for each test
        for i, test_name in enumerate(test_names):
            plt.bar(x_pos + i * width, test_totals[test_name], width, 
                   label=get_clean_test_name(test_name), color=colors[i], alpha=0.8)
        
        # Customize the plot
        plt.xlabel('Block Size', fontsize=12, fontweight='bold')
        if metric_type == 'iops':
            plt.ylabel('Total IOPS (All VMs Combined)', fontsize=12, fontweight='bold')
            plt.title(f'Total IOPS Comparison (All VMs): {operation.upper()} - {len(test_names)} Test Runs', 
                     fontsize=16, fontweight='bold', pad=20)
        else:
            plt.ylabel('Total Bandwidth (All VMs Combined) [KB]', fontsize=12, fontweight='bold')
            plt.title(f'Total Bandwidth Comparison (All VMs): {operation.upper()} - {len(test_names)} Test Runs', 
                     fontsize=16, fontweight='bold', pad=20)
        
        # Set x-axis labels
        block_size_labels = [get_block_size_display_name(bs) for bs in block_sizes]
        plt.xticks(x_pos + width * (len(test_names) - 1) / 2, block_size_labels, rotation=45, ha='right')
        
        # Add legend outside the graph on the right side
        plt.legend(title='Test Run', bbox_to_anchor=(1.15, 1), loc='upper left', fontsize=10)
        
        # Add grid
        plt.grid(axis='y', alpha=0.3, linestyle='--')
        
        # Build right-side summary box with values per block size
        metric_name = 'Total IOPS' if metric_type == 'iops' else 'Total Bandwidth [KB]'
        summary_lines = [f"Values per block size ({metric_name}):"]
        for bs_idx, bs in enumerate(block_sizes):
            bs_label = get_block_size_display_name(bs)
            summary_lines.append(f"\n{bs_label}:")
            for i, test_name in enumerate(test_names):
                value = test_totals[test_name][bs_idx] if bs_idx < len(test_totals[test_name]) else 0
                summary_lines.append(f"  {get_clean_test_name(test_name)}: {value:.0f}")
        
        # Place summary box on the right
        plt.figtext(1.25, 0.15, "\n".join(summary_lines).strip(), 
                   ha='right', va='bottom', fontsize=10, fontweight='bold',
                   bbox=dict(boxstyle='round,pad=0.5', facecolor='lightgray', alpha=0.8))
        
        # Adjust layout to accommodate legend and summary
        plt.tight_layout()
        plt.subplots_adjust(right=0.65)
        
        # Save the graph
        metric_suffix = 'iops' if metric_type == 'iops' else 'bw'
        filename = f'Total_All_VMs_{operation}_comparison_bar_{metric_suffix}.png'
        filepath = os.path.join(output_dir, filename)
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"Created total all VMs comparison bar chart: {filename}")
        return 1
        
    except Exception as e:
        print(f"Error creating total all VMs comparison bar chart for {operation}: {e}")
        return 0

def create_total_all_vms_comparison_line_chart(df, operation, block_sizes, test_names, output_dir, metric_type='iops'):
    """
    Create a line chart comparing TOTAL ALL VMs values from different test runs for an operation.
    Shows the sum of all VMs for each test run on a single graph.
    """
    try:
        # Create figure - increased height for better spacing
        plt.figure(figsize=(14, 12))
        
        colors = plt.cm.Set3(np.linspace(0, 1, len(test_names)))
        
        # Calculate total values for each test and block size
        test_totals = {}
        for test_name in test_names:
            test_totals[test_name] = []
            for block_size in block_sizes:
                # Get all VMs for this test and block size
                test_block_data = df[(df['test_name'] == test_name) & (df['block_size'] == block_size)]
                if not test_block_data.empty:
                    # For read operations, use read_metric; for write operations, use write_metric
                    if operation in ['read', 'randread']:
                        metric_col = 'read_metric' if metric_type == 'bw' else 'read_iops'
                    else:
                        metric_col = 'write_metric' if metric_type == 'bw' else 'write_iops'
                    # Sum all VMs for this test and block size
                    total_value = test_block_data[metric_col].sum()
                    test_totals[test_name].append(total_value)
                else:
                    test_totals[test_name].append(0)
        
        # Create line plot for each test
        for i, test_name in enumerate(test_names):
            plt.plot(block_sizes, test_totals[test_name], 
                    marker='o', linewidth=2, markersize=8, 
                    label=get_clean_test_name(test_name), color=colors[i])
            
            # Store data for summary display below legend
            if not hasattr(plt.gca(), '_line_data_summary'):
                plt.gca()._line_data_summary = []
            
            for j, (block_size, value) in enumerate(zip(block_sizes, test_totals[test_name])):
                if value > 0:  # Only show values for non-zero values
                    # Store original block size, not the converted display name
                    plt.gca()._line_data_summary.append({
                        'test_name': test_name,
                        'block_size': block_size,
                        'value': value,
                        'color': colors[i]
                    })
        
        # Customize the plot
        plt.xlabel('Block Size', fontsize=12, fontweight='bold')
        if metric_type == 'iops':
            plt.ylabel('Total IOPS (All VMs Combined)', fontsize=12, fontweight='bold')
            plt.title(f'Total IOPS Comparison (All VMs): {operation.upper()} - {len(test_names)} Test Runs', 
                     fontsize=16, fontweight='bold', pad=20)
        else:
            plt.ylabel('Total Bandwidth (All VMs Combined) [KB]', fontsize=12, fontweight='bold')
            plt.title(f'Total Bandwidth Comparison (All VMs): {operation.upper()} - {len(test_names)} Test Runs', 
                     fontsize=16, fontweight='bold', pad=20)
        
        # Set x-axis labels
        block_size_labels = [get_block_size_display_name(bs) for bs in block_sizes]
        plt.xticks(block_sizes, block_size_labels, rotation=45, ha='right')
        
        # Add legend outside the graph on the right side (same as bar charts)
        plt.legend(title='Test Run', bbox_to_anchor=(1.15, 1), loc='upper left', fontsize=10)
        
        # Add grid
        plt.grid(True, alpha=0.3, linestyle='--')

        # Build right-side summary box with values per block size (TOTAL ALL VMs)
        metric_name = 'Total IOPS' if metric_type == 'iops' else 'Total Bandwidth [KB]'
        summary_lines = [f"Values per block size ({metric_name}):"]
        for bs_idx, bs in enumerate(block_sizes):
            bs_label = get_block_size_display_name(bs)
            summary_lines.append(f"\n{bs_label}:")
            for i, test_name in enumerate(test_names):
                value = test_totals[test_name][bs_idx] if bs_idx < len(test_totals[test_name]) else 0
                summary_lines.append(f"  {get_clean_test_name(test_name)}: {value:.0f}")

        # Place summary box on the right
        plt.figtext(1.25, 0.15, "\n".join(summary_lines).strip(), 
                   ha='right', va='bottom', fontsize=10, fontweight='bold',
                   bbox=dict(boxstyle='round,pad=0.5', facecolor='lightgray', alpha=0.8))

        # Adjust layout to accommodate legend and summary
        plt.tight_layout()
        plt.subplots_adjust(right=0.65)
        
        # Save the graph
        metric_suffix = 'iops' if metric_type == 'iops' else 'bw'
        filename = f'Total_All_VMs_{operation}_comparison_line_{metric_suffix}.png'
        filepath = os.path.join(output_dir, filename)
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"Created total all VMs comparison line chart: {filename}")
        return 1
        
    except Exception as e:
        print(f"Error creating total all VMs comparison line chart for {operation}: {e}")
        return 0

def create_per_vm_comparison_bar_chart(df, operation, block_sizes, test_names, output_dir, metric_type='iops'):
    """
    Create per-VM bar charts comparing test results for an operation.
    Creates one graph per block size showing test1 vs test2 vs test3 for that block size.
    """
    success_count = 0
    
    # Create one graph per block size
    for block_size in block_sizes:
        try:
            # Create figure - increased height for better spacing
            plt.figure(figsize=(16, 12))
            
            # Get all VMs and sort them numerically
            vm_names = df['vm_name'].unique()
            # Natural sort hostnames to handle embedded numbers while working for arbitrary names
            def natural_sort_key(s):
                return [int(text) if text.isdigit() else text.lower() for text in re.split(r'(\d+)', str(s))]
            all_vms = sorted(vm_names, key=natural_sort_key)
            
            # Create numeric x-axis positions for all VMs
            all_positions = range(len(all_vms))
            
            # Create plot for each test (only for this block size)
            colors = plt.cm.Set3(np.linspace(0, 1, len(test_names)))
            
            for i, test_name in enumerate(test_names):
                test_data = df[df['test_name'] == test_name]
                bs_data = test_data[test_data['block_size'] == block_size]
                
                # Get rate_iops for this test (from first record)
                rate_iops = test_data['rate_iops'].iloc[0] if not test_data.empty and 'rate_iops' in test_data.columns else None
                legend_label = get_clean_test_name(test_name)
                
                # Get metric values for each VM
                vm_values = []
                for vm_name in all_vms:
                    vm_data = bs_data[bs_data['vm_name'] == vm_name]
                    if not vm_data.empty:
                        if operation in ['read', 'randread']:
                            metric_col = 'read_metric' if metric_type == 'bw' else 'read_iops'
                        else:
                            metric_col = 'write_metric' if metric_type == 'bw' else 'write_iops'
                        vm_values.append(vm_data[metric_col].iloc[0])
                    else:
                        vm_values.append(0)
                
                # Create bar plot for this test
                width = 0.8 / len(test_names)
                offset = (i - len(test_names)/2 + 0.5) * width
                bar_positions = [pos + offset for pos in all_positions]
                
                bars = plt.bar(bar_positions, vm_values, 
                              width=width, label=legend_label, 
                              color=colors[i], alpha=0.8,
                              edgecolor='black', linewidth=0.5)
            
            
            # Customize the plot
            metric_name = 'Bandwidth (bw_mean)' if metric_type == 'bw' else 'IOPS (iops_mean)'
            block_display = get_block_size_display_name(block_size)
            plt.title(f'{metric_name} Performance Comparison (Per-VM Bar Chart): {operation.upper()} - {block_display} Block Size', 
                     fontsize=16, fontweight='bold', pad=20)
            
            plt.xlabel('Test Machines', fontsize=12, fontweight='bold')
            y_label = f'{metric_name} [KB]' if metric_type == 'bw' else metric_name
            plt.ylabel(y_label, fontsize=12, fontweight='bold')
            
            # Set x-axis ticks (show every machine if <=20, otherwise every 10th machine)
            if len(all_vms) <= 20:
                x_positions = range(len(all_vms))
                x_labels = [all_vms[i] for i in x_positions]
            else:
                x_positions = range(0, len(all_vms), 10)
                x_labels = [all_vms[i] for i in x_positions]
            
            plt.xticks(x_positions, x_labels, rotation=45, ha='right')
            
            # Improve x-axis spacing and appearance
            plt.tick_params(axis='x', which='major', labelsize=8, pad=8)
            plt.tick_params(axis='x', which='minor', labelsize=6)
            
            # Force all labels to be shown (prevent matplotlib from hiding labels)
            plt.gca().set_xticks(x_positions)
            plt.gca().set_xticklabels(x_labels, rotation=45, ha='right')
            
            # Ensure all ticks are visible
            plt.gca().tick_params(axis='x', which='both', bottom=True, top=False)
            plt.gca().set_xlim(-0.5, len(all_vms) - 0.5)
            
            # Add more spacing between major ticks for better readability (only for large datasets)
            if len(all_vms) > 20:
                plt.gca().xaxis.set_major_locator(plt.MultipleLocator(10))
                plt.gca().xaxis.set_minor_locator(plt.MultipleLocator(5))
            # For ≤20 VMs, don't set locators to allow all labels to show
            
            # Add legend
            plt.legend(title='Test Directory', bbox_to_anchor=(1.15, 1), loc='upper left')
            
            # Add total sum values under the legend on the right side
            # Calculate total sums for each test
            avg_values = []
            for test_name in test_names:
                test_data = df[df['test_name'] == test_name]
                bs_data = test_data[test_data['block_size'] == block_size]
                
                # Calculate total sum for this test and block size
                if operation in ['read', 'randread']:
                    metric_col = 'read_metric' if metric_type == 'bw' else 'read_iops'
                else:
                    metric_col = 'write_metric' if metric_type == 'bw' else 'write_iops'
                
                avg_value = bs_data[metric_col].mean() if not bs_data.empty else 0
                avg_values.append(avg_value)
            
            # Add total sum text under the legend
            metric_name = 'Bandwidth (bw_mean)' if metric_type == 'bw' else 'IOPS (iops_mean)'
            avg_text = f"Average {metric_name}:\n"
            for i, (test_name, avg_value) in enumerate(zip(test_names, avg_values)):
                avg_text += f"{get_clean_test_name(test_name)}: {avg_value:.0f}\n"
            
            # Position text under the legend
            plt.figtext(0.98, 0.15, avg_text.strip(), 
                       ha='right', va='bottom', fontsize=10, fontweight='bold',
                       bbox=dict(boxstyle='round,pad=0.5', facecolor='lightgray', alpha=0.8))
            
            
            # Add individual horizontal lines for each test's average
            for i, (test_name, avg_value) in enumerate(zip(test_names, avg_values)):
                # Add horizontal line for this test's average
                plt.axhline(y=avg_value, color=colors[i], linestyle='-', linewidth=3, alpha=0.9,
                           label=f'{get_clean_test_name(test_name)} Avg: {avg_value:.0f}')
            
            # Add grid for better readability
            plt.grid(axis='y', alpha=0.3, linestyle='--')
            
            # Adjust layout to accommodate legend and summary
            plt.tight_layout()
            plt.subplots_adjust(right=0.65)

            # Generate PNG filename with block size and metric type
            metric_suffix = 'bw' if metric_type == 'bw' else 'iops'
            png_filename = os.path.join(output_dir, f'comparison_{operation}_{block_size}_per_vm_bar_{metric_suffix}.png')
            
            # Save the plot
            plt.savefig(png_filename, dpi=300, bbox_inches='tight', 
                       facecolor='white', edgecolor='none')
            plt.close()  # Close the figure to free memory
            
            print(f"Created per-VM bar chart: {png_filename}")
            success_count += 1
            
        except Exception as e:
            print(f"Error creating per-VM bar chart for {operation} {block_size}: {e}")
    
    return success_count

def create_per_vm_comparison_line_chart(df, operation, block_sizes, test_names, output_dir, metric_type='iops'):
    """
    Create per-VM line charts comparing test results for an operation.
    Creates one graph per block size showing test1 vs test2 vs test3 for that block size.
    """
    success_count = 0
    
    # Create one graph per block size
    for block_size in block_sizes:
        try:
            # Create figure - increased height for better spacing
            plt.figure(figsize=(16, 12))
            
            # Get all VMs and sort them numerically
            vm_names = df['vm_name'].unique()
            # Natural sort hostnames to handle embedded numbers while working for arbitrary names
            def natural_sort_key(s):
                return [int(text) if text.isdigit() else text.lower() for text in re.split(r'(\d+)', str(s))]
            all_vms = sorted(vm_names, key=natural_sort_key)
            
            # Create numeric x-axis positions for all VMs
            all_positions = range(len(all_vms))
            
            # Create plot for each test (only for this block size)
            colors = plt.cm.Set3(np.linspace(0, 1, len(test_names)))
            
            for i, test_name in enumerate(test_names):
                test_data = df[df['test_name'] == test_name]
                bs_data = test_data[test_data['block_size'] == block_size]
                
                # Get rate_iops for this test (from first record)
                rate_iops = test_data['rate_iops'].iloc[0] if not test_data.empty and 'rate_iops' in test_data.columns else None
                legend_label = get_clean_test_name(test_name)
                
                # Get metric values for each VM
                vm_values = []
                for vm_name in all_vms:
                    vm_data = bs_data[bs_data['vm_name'] == vm_name]
                    if not vm_data.empty:
                        if operation in ['read', 'randread']:
                            metric_col = 'read_metric' if metric_type == 'bw' else 'read_iops'
                        else:
                            metric_col = 'write_metric' if metric_type == 'bw' else 'write_iops'
                        vm_values.append(vm_data[metric_col].iloc[0])
                    else:
                        vm_values.append(0)
                
                # Create line plot for this test
                plt.plot(all_positions, vm_values, 
                        marker='o', linewidth=2, markersize=4,
                        label=legend_label, 
                        color=colors[i], 
                        markerfacecolor=colors[i],
                        markeredgecolor='black', markeredgewidth=1)
            
            
            # Customize the plot
            metric_name = 'Bandwidth (bw_mean)' if metric_type == 'bw' else 'IOPS (iops_mean)'
            block_display = get_block_size_display_name(block_size)
            plt.title(f'{metric_name} Performance Comparison (Per-VM Line Chart): {operation.upper()} - {block_display} Block Size', 
                     fontsize=16, fontweight='bold', pad=20)
            
            
            plt.xlabel('Test Machines', fontsize=12, fontweight='bold')
            y_label = f'{metric_name} [KB]' if metric_type == 'bw' else metric_name
            plt.ylabel(y_label, fontsize=12, fontweight='bold')
            
            # Set x-axis ticks (show every machine if <=20, otherwise every 10th machine)
            if len(all_vms) <= 20:
                x_positions = range(len(all_vms))
                x_labels = [all_vms[i] for i in x_positions]
            else:
                x_positions = range(0, len(all_vms), 10)
                x_labels = [all_vms[i] for i in x_positions]
            
            plt.xticks(x_positions, x_labels, rotation=45, ha='right')
            
            # Improve x-axis spacing and appearance
            plt.tick_params(axis='x', which='major', labelsize=8, pad=8)
            plt.tick_params(axis='x', which='minor', labelsize=6)
            
            # Force all labels to be shown (prevent matplotlib from hiding labels)
            plt.gca().set_xticks(x_positions)
            plt.gca().set_xticklabels(x_labels, rotation=45, ha='right')
            
            # Ensure all ticks are visible
            plt.gca().tick_params(axis='x', which='both', bottom=True, top=False)
            plt.gca().set_xlim(-0.5, len(all_vms) - 0.5)
            
            # Add more spacing between major ticks for better readability (only for large datasets)
            if len(all_vms) > 20:
                plt.gca().xaxis.set_major_locator(plt.MultipleLocator(10))
                plt.gca().xaxis.set_minor_locator(plt.MultipleLocator(5))
            # For ≤20 VMs, don't set locators to allow all labels to show
            
            # Add legend
            plt.legend(title='Test Directory', bbox_to_anchor=(1.15, 1), loc='upper left')
            
            # Add total sum values under the legend on the right side
            # Calculate total sums for each test
            avg_values = []
            for test_name in test_names:
                test_data = df[df['test_name'] == test_name]
                bs_data = test_data[test_data['block_size'] == block_size]
                
                # Calculate total sum for this test and block size
                if operation in ['read', 'randread']:
                    metric_col = 'read_metric' if metric_type == 'bw' else 'read_iops'
                else:
                    metric_col = 'write_metric' if metric_type == 'bw' else 'write_iops'
                
                avg_value = bs_data[metric_col].mean() if not bs_data.empty else 0
                avg_values.append(avg_value)
            
            # Add total sum text under the legend
            metric_name = 'Bandwidth (bw_mean)' if metric_type == 'bw' else 'IOPS (iops_mean)'
            avg_text = f"Average {metric_name}:\n"
            for i, (test_name, avg_value) in enumerate(zip(test_names, avg_values)):
                avg_text += f"{get_clean_test_name(test_name)}: {avg_value:.0f}\n"
            
            # Position text under the legend
            plt.figtext(0.98, 0.15, avg_text.strip(), 
                       ha='right', va='bottom', fontsize=10, fontweight='bold',
                       bbox=dict(boxstyle='round,pad=0.5', facecolor='lightgray', alpha=0.8))
            
            
            # Add individual horizontal lines for each test's average
            for i, (test_name, avg_value) in enumerate(zip(test_names, avg_values)):
                # Add horizontal line for this test's average
                plt.axhline(y=avg_value, color=colors[i], linestyle='-', linewidth=3, alpha=0.9,
                           label=f'{get_clean_test_name(test_name)} Avg: {avg_value:.0f}')
            
            # Add grid for better readability
            plt.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
            
            # Set y-axis to start from 0 for better visualization
            plt.ylim(bottom=0)
            
            # Adjust layout to accommodate legend
            plt.tight_layout()
            plt.subplots_adjust(right=0.65)  # Less space on right since legend is inside
            
            # Generate PNG filename with block size and metric type
            metric_suffix = 'bw' if metric_type == 'bw' else 'iops'
            png_filename = os.path.join(output_dir, f'comparison_{operation}_{block_size}_per_vm_line_{metric_suffix}.png')
            
            # Save the plot
            plt.savefig(png_filename, dpi=300, bbox_inches='tight', 
                       facecolor='white', edgecolor='none')
            plt.close()  # Close the figure to free memory
            
            print(f"Created per-VM line chart: {png_filename}")
            success_count += 1
            
        except Exception as e:
            print(f"Error creating per-VM line chart for {operation} {block_size}: {e}")
    
    return success_count

def get_clean_test_name(name):
    """
    Return a display-friendly test name by removing '-YYYYMMDD-HHMMSS-' segments.
    Example: 'fio-results-20250905-152136-100vm-rate-iops-10' -> 'fio-results-100vm-rate-iops-10'
    """
    return re.sub(r'-\d{8}-\d{6}-', '-', str(name))

def main():
    parser = argparse.ArgumentParser(description='Compare FIO test results from different test runs')
    parser.add_argument('directories', nargs='+', help='Test directories to compare (e.g., dir1/ dir2/ dir3/ dir4/)')
    parser.add_argument('--graphs', choices=['bar', 'line', 'both', 'none'], default='both',
                       help='Type of graphs to generate (default: both)')
    parser.add_argument('--output-dir', default='.', help='Output directory for results (default: current directory)')
    parser.add_argument('--block-sizes', nargs='*', default=None,
                       help='Specify block sizes to analyze (e.g., --block-sizes 4k 8k 128k). If not specified, all available block sizes will be used.')
    parser.add_argument('--summary-only', action='store_true',
                       help='Generate only summary graphs (skip per-VM comparison graphs)')
    
    # Add metric selection options (no longer mutually exclusive)
    parser.add_argument('--iops', action='store_true',
                       help='Analyze IOPS performance')
    parser.add_argument('--bw', action='store_true',
                       help='Analyze bandwidth performance')
    
    args = parser.parse_args()
    
    # Determine the metric types to process
    metric_types = []
    metric_names = []
    
    if args.iops:
        metric_types.append('iops')
        metric_names.append('IOPS (iops_mean)')
    
    if args.bw:
        metric_types.append('bw')
        metric_names.append('Bandwidth (bw_mean)')
    
    # If neither is specified, default to IOPS
    if not metric_types:
        metric_types = ['iops']
        metric_names = ['IOPS (iops_mean)']
    
    # Validate that we have at least 2 directories
    if len(args.directories) < 2:
        print("Error: At least 2 directories are required for comparison")
        return 1
    
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    
    print("FIO Test Comparison Analyzer")
    print("=" * 50)
    print(f"Directories to compare: {', '.join(args.directories)}")
    print(f"Output directory: {args.output_dir}")
    print(f"Graph type: {args.graphs}")
    print(f"Metric types: {', '.join(metric_names)}")
    print()
    
    # Process each metric type
    total_success_count = 0
    
    for metric_idx, metric_type in enumerate(metric_types, 1):
        metric_name = metric_names[metric_idx - 1]
        
        print(f"\n{'='*60}")
        print(f"PROCESSING METRIC TYPE {metric_idx}/{len(metric_types)}: {metric_name}")
        print(f"{'='*60}")
        
        # Process all test directories for this metric type
        print(f"Step 1: Processing test directories for {metric_name}...")
        print("-" * 40)
        
        all_results = {}
        directory_names = []
        
        for i, directory in enumerate(args.directories, 1):
            # Extract directory name for test identification
            dir_name = Path(directory).name
            directory_names.append(dir_name)
            
            print(f"Processing directory {i}: {directory} (name: {dir_name})")
            results = process_test_directory(directory, dir_name, metric_type)
            all_results.update(results)
            print(f"{dir_name} results: {len(results)} records")
        
        print(f"\nTotal directories processed: {len(args.directories)}")
        print(f"Total records: {len(all_results)}")
        
        if not all_results:
            print(f"No data found in any test directory for {metric_name}")
            continue
        
        # Create comparison DataFrame
        print(f"\nStep 2: Creating comparison analysis for {metric_name}...")
        print("-" * 40)
        
        df = create_comparison_dataframe(all_results)
        
        if df.empty:
            print(f"No data available for comparison for {metric_name}")
            continue
        
        # Create summary CSV files
        csv_files = create_comparison_summary_csv(df, args.output_dir, metric_type)
        
        # Generate comparison report
        generate_comparison_report(df, args.output_dir, metric_type)
        
        # Create comparison graphs
        if args.graphs != 'none':
            if args.summary_only:
                print(f"\nStep 3: Creating {args.graphs} summary graphs only (per-VM graphs skipped) for {metric_name}...")
            else:
                print(f"\nStep 3: Creating {args.graphs} comparison graphs for {metric_name}...")
            print("-" * 40)
            
            success_count = create_comparison_graphs(df, args.output_dir, args.graphs, metric_type, args.block_sizes, args.summary_only)
            print(f"Successfully created {success_count} comparison graphs for {metric_name}")
            total_success_count += success_count
        
    
    print("\n" + "=" * 50)
    print("COMPARISON ANALYSIS COMPLETE!")
    print("=" * 50)
    print(f"Total graphs created: {total_success_count}")
    print(f"Metric types processed: {', '.join(metric_names)}")
    
    # List generated files
    generated_files = [f for f in os.listdir(args.output_dir) if f.startswith('comparison_') or f.startswith('test_comparison_')]
    if generated_files:
        print(f"\nGenerated files:")
        for file in sorted(generated_files):
            print(f"  - {file}")
    
    # List summary graphs specifically
    summary_graphs = [f for f in os.listdir(args.output_dir) if f.startswith('comparison_') and ('_summary_bar_' in f or '_summary_line_' in f) and f.endswith('.png')]
    if summary_graphs:
        print(f"\nSummary comparison graphs:")
        for file in sorted(summary_graphs):
            print(f"  - {file}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
