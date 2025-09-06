#!/usr/bin/env python3
"""
Comprehensive IOPS Analysis Tool
===============================

This script combines all IOPS analysis functionality:
1. Extract IOPS data from FIO JSON files in vm-* directories
2. Generate CSV files with integer IOPS values
3. Create PNG graphs (bar charts, line graphs, or both)
4. Support for all operation types: read, write, randread, randwrite
5. Automatic block size detection and separate files per operation/block size

Usage:
    python3 iops_analyzer.py [options]

Options:
    --graphs bar|line|both    Type of graphs to generate (default: bar)
    --help                   Show this help message

Examples:
    python3 iops_analyzer.py                    # Generate bar charts
    python3 iops_analyzer.py --graphs line      # Generate line graphs
    python3 iops_analyzer.py --graphs both      # Generate both types
"""

import json
import csv
import os
import glob
import re
import argparse
import sys
import subprocess

# Try to import optional dependencies
try:
    import pandas as pd
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

# Global variable to store FIO configurations for subtitles
FIO_CONFIGS = {}

def extract_fio_config_from_json(json_file_path):
    """
    Extract FIO configuration from JSON file for subtitle display.
    Returns a dictionary with configuration parameters.
    """
    config_data = {}
    
    try:
        import json
        with open(json_file_path, 'r') as f:
            data = json.load(f)
        
        # Check if jobs exist
        if 'jobs' not in data or len(data['jobs']) == 0:
            return config_data
        
        # Extract from first job's options (assuming all jobs have similar config)
        job_options = data['jobs'][0].get('job options', {})
        
        # Extract the requested parameters
        config_data['size'] = job_options.get('size', 'N/A')
        config_data['bs'] = job_options.get('bs', 'N/A')
        config_data['runtime'] = job_options.get('runtime', 'N/A')
        config_data['direct'] = job_options.get('direct', 'N/A')
        config_data['numjobs'] = job_options.get('numjobs', 'N/A')
        config_data['iodepth'] = job_options.get('iodepth', 'N/A')
        config_data['rate_iops'] = job_options.get('rate_iops', None)  # May not be present
        
    except (json.JSONDecodeError, KeyError, FileNotFoundError) as e:
        print(f"Error extracting config from {json_file_path}: {e}")
    
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
    if config_data.get('rate_iops'):
        subtitle_parts.append(f"Rate IOPS: {config_data['rate_iops']}")
    
    return " | ".join(subtitle_parts)
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
        
        # Aggregate IOPS values (sum them up since they represent different jobs)
        if iops_values:
            # Convert to integer to remove decimal places
            iops_data[(operation, block_size)] = int(sum(iops_values))
    
    except (json.JSONDecodeError, KeyError, FileNotFoundError) as e:
        print(f"Error processing {json_file_path}: {e}")
    
    return iops_data

def process_vm_directory(vm_dir):
    """
    Process all JSON files in a vm-* directory.
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

def write_csv_files(all_results):
    """
    Write separate CSV files for each operation and block size combination.
    """
    # Group results by operation and block size
    grouped_results = {}
    
    for (vm_name, operation, block_size), iops in all_results.items():
        key = (operation, block_size)
        if key not in grouped_results:
            grouped_results[key] = []
        grouped_results[key].append((vm_name, iops))
    
    # Write separate CSV files
    for (operation, block_size), data in grouped_results.items():
        filename = f"summary-{operation}-{block_size}.csv"
        
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header
            writer.writerow(['vm_name', 'iops'])
            
            # Sort by vm_name for consistent output
            data.sort(key=lambda x: x[0])
            
            # Write data
            for vm_name, iops in data:
                writer.writerow([vm_name, iops])
        
        print(f"Created {filename} with {len(data)} records")
    
    return len(grouped_results)

def get_block_size_display_name(block_size):
    """
    Convert block size to a more readable display name.
    """
    block_size_map = {
        '4k': '4KB',
        '8k': '8KB', 
        '128k': '128KB',
        '1024k': '1MB',
        '4096k': '4MB',
        'unknown': 'Unknown'
    }
    return block_size_map.get(block_size.lower(), block_size.upper())

def write_operation_summary_csv_files(all_results, selected_block_sizes=None):
    """
    Write CSV files that combine all block sizes for each operation type.
    Creates files like: summary-write-all-blocks.csv, summary-read-all-blocks.csv, etc.
    """
    # Group results by operation only (combining all block sizes)
    operation_results = {}
    
    for (vm_name, operation, block_size), iops in all_results.items():
        if operation not in operation_results:
            operation_results[operation] = {}
        if vm_name not in operation_results[operation]:
            operation_results[operation][vm_name] = {}
        operation_results[operation][vm_name][block_size] = iops
    
    # Write CSV files for each operation
    csv_files_created = []
    
    for operation, vm_data in operation_results.items():
        filename = f"summary-{operation}-all-blocks.csv"
        
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
        
        with open(filename, 'w', newline='') as csvfile:
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
                    iops_value = vm_data[vm_name].get(block_size, 0)  # 0 if not found
                    row.append(iops_value)
                writer.writerow(row)
        
        display_names = [get_block_size_display_name(bs) for bs in all_block_sizes]
        print(f"Created {filename} with {len(sorted_vms)} VMs and {len(all_block_sizes)} block sizes: {', '.join(display_names)}")
        csv_files_created.append(filename)
    
    return csv_files_created

def extract_operation_and_blocksize_from_filename(filename):
    """
    Extract operation and block size from CSV filename.
    Example: 'summary-write-4k.csv' -> ('write', '4k')
    """
    match = re.search(r'summary-(\w+)-(\w+)\.csv', filename)
    if match:
        operation = match.group(1)
        block_size = match.group(2)
        return operation, block_size
    return None, None

def get_x_axis_labels_and_positions(df_sorted):
    """
    Determine X-axis labels and positions based on number of VMs.
    Show every VM if <= 100, otherwise show every 10th VM.
    """
    num_vms = len(df_sorted)
    # If num_vms is <= 20, show every VM - check this 
    if num_vms <= 20:
        # Show every VM
        x_positions = range(num_vms)
        x_labels = [f'VM {i+1}' for i in x_positions]
        return x_positions, x_labels
    else:
        # Show every 10th VM for better visibility
        x_positions = range(0, num_vms, 10)
        x_labels = [f'VM {i+1}' for i in x_positions]
        return x_positions, x_labels

def create_bar_graph(csv_file):
    """
    Create a PNG bar graph from a CSV file.
    """
    try:
        # Read CSV file
        df = pd.read_csv(csv_file)
        
        # Extract operation and block size from filename
        filename = os.path.basename(csv_file)
        operation, block_size = extract_operation_and_blocksize_from_filename(filename)
        
        if not operation or not block_size:
            print(f"Could not parse filename: {filename}")
            return False
        
        # Create the plot
        plt.figure(figsize=(12, 8))
        
        # Sort by vm_name for consistent ordering
        df_sorted = df.sort_values('vm_name')
        
        # Get X-axis labels and positions based on number of VMs
        x_positions, x_labels = get_x_axis_labels_and_positions(df_sorted)
        
        # Create bar plot with all data points
        all_positions = range(len(df_sorted))
        bars = plt.bar(all_positions, df_sorted['iops'], 
                      color='skyblue', edgecolor='navy', alpha=0.7)
        
        # Add IOPS values on bars if number of VMs is less than 20
        if len(df_sorted) < 20:
            for i, (pos, iops) in enumerate(zip(all_positions, df_sorted['iops'])):
                plt.text(pos, iops + max(df_sorted['iops']) * 0.01, 
                        f'{iops:,}', ha='center', va='bottom', fontsize=8, fontweight='bold')
        
        # Get FIO configuration for subtitle
        config_key = (operation, block_size)
        subtitle = ""
        if config_key in FIO_CONFIGS:
            subtitle = format_fio_subtitle(FIO_CONFIGS[config_key])
        
        # Customize the plot
        num_vms = len(df_sorted)
        plt.title(f'IOPS Performance (Bar Chart): {operation.upper()} - {block_size.upper()} Block Size ({num_vms} VMs)', 
                 fontsize=16, fontweight='bold', pad=20)
        
        # Add subtitle with FIO configuration
        if subtitle:
            plt.suptitle(subtitle, fontsize=10, y=0.88, ha='center', va='top')
        
        plt.xlabel('VM Index', fontsize=12, fontweight='bold')
        plt.ylabel('IOPS', fontsize=12, fontweight='bold')
        
        # Set x-axis ticks based on visibility rules
        plt.xticks(x_positions, x_labels, rotation=45, ha='right')
        
        # Add grid for better readability
        plt.grid(axis='y', alpha=0.3, linestyle='--')
        
        # Adjust layout to prevent label cutoff
        plt.tight_layout()
        
        # Generate PNG filename
        png_filename = csv_file.replace('.csv', '_bar.png')
        
        # Save the plot
        plt.savefig(png_filename, dpi=300, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        plt.close()  # Close the figure to free memory
        
        print(f"Created bar graph: {png_filename}")
        return True
        
    except Exception as e:
        print(f"Error creating bar graph for {csv_file}: {e}")
        return False

def create_line_graph(csv_file):
    """
    Create a PNG line graph from a CSV file.
    """
    try:
        # Read CSV file
        df = pd.read_csv(csv_file)
        
        # Extract operation and block size from filename
        filename = os.path.basename(csv_file)
        operation, block_size = extract_operation_and_blocksize_from_filename(filename)
        
        if not operation or not block_size:
            print(f"Could not parse filename: {filename}")
            return False
        
        # Create the plot
        plt.figure(figsize=(12, 8))
        
        # Sort by vm_name for consistent ordering
        df_sorted = df.sort_values('vm_name')
        
        # Get X-axis labels and positions based on number of VMs
        x_positions, x_labels = get_x_axis_labels_and_positions(df_sorted)
        
        # Create line plot with all data points
        all_positions = range(len(df_sorted))
        plt.plot(all_positions, df_sorted['iops'], 
                marker='o', linewidth=1, markersize=2, 
                color='steelblue', markerfacecolor='lightblue', 
                markeredgecolor='navy', markeredgewidth=2)
        
        # Add IOPS values on dots if number of VMs is less than 20
        if len(df_sorted) < 20:
            for i, (pos, iops) in enumerate(zip(all_positions, df_sorted['iops'])):
                plt.text(pos, iops + max(df_sorted['iops']) * 0.02, 
                        f'{iops:,}', ha='center', va='bottom', fontsize=8, fontweight='bold')
        
        # Get FIO configuration for subtitle
        config_key = (operation, block_size)
        subtitle = ""
        if config_key in FIO_CONFIGS:
            subtitle = format_fio_subtitle(FIO_CONFIGS[config_key])
        
        # Customize the plot
        num_vms = len(df_sorted)
        plt.title(f'IOPS Performance (Line Chart): {operation.upper()} - {block_size.upper()} Block Size ({num_vms} VMs)', 
                 fontsize=16, fontweight='bold', pad=20)
        
        # Add subtitle with FIO configuration
        if subtitle:
            plt.suptitle(subtitle, fontsize=10, ha='center', va='top')
        
        plt.xlabel('VM Index', fontsize=12, fontweight='bold')
        plt.ylabel('IOPS', fontsize=12, fontweight='bold')
        
        # Set x-axis ticks based on visibility rules
        plt.xticks(x_positions, x_labels, rotation=45, ha='right')
        
        # Add grid for better readability
        plt.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
        
        # Set y-axis to start from 0 for better visualization
        plt.ylim(bottom=0)
        
        # Add some padding to y-axis for label visibility
        y_max = df_sorted['iops'].max()
        plt.ylim(top=y_max * 1.1)
        
        # Adjust layout to prevent label cutoff
        plt.tight_layout()
        
        # Generate PNG filename with line-chart suffix
        png_filename = csv_file.replace('.csv', '_line-chart.png')
        
        # Save the plot
        plt.savefig(png_filename, dpi=300, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        plt.close()  # Close the figure to free memory
        
        print(f"Created line graph: {png_filename}")
        return True
        
    except Exception as e:
        print(f"Error creating line graph for {csv_file}: {e}")
        return False

def create_simple_graphs(csv_file, graph_type):
    """
    Create simple graphs without suffixes (for backward compatibility).
    """
    try:
        # Read CSV file
        df = pd.read_csv(csv_file)
        
        # Extract operation and block size from filename
        filename = os.path.basename(csv_file)
        operation, block_size = extract_operation_and_blocksize_from_filename(filename)
        
        if not operation or not block_size:
            print(f"Could not parse filename: {filename}")
            return False
        
        # Create the plot
        plt.figure(figsize=(12, 8))
        
        # Sort by vm_name for consistent ordering
        df_sorted = df.sort_values('vm_name')
        
        # Get X-axis labels and positions based on number of VMs
        x_positions, x_labels = get_x_axis_labels_and_positions(df_sorted)
        
        # Create numeric x-axis positions for all data points
        all_positions = range(len(df_sorted))
        
        if graph_type == 'bar':
            # Create bar plot
            bars = plt.bar(all_positions, df_sorted['iops'], 
                          color='skyblue', edgecolor='navy', alpha=0.7)
            
            # Add IOPS values on bars if number of VMs is less than 20
            # does not make sense to add this for more than 20 VMs - simly not readable! 
            if len(df_sorted) < 20:
                for i, (pos, iops) in enumerate(zip(all_positions, df_sorted['iops'])):
                    plt.text(pos, iops + max(df_sorted['iops']) * 0.01, 
                            f'{iops:,}', ha='center', va='bottom', fontsize=8, fontweight='bold')
            
            # Add grid for better readability
            plt.grid(axis='y', alpha=0.3, linestyle='--')
            
        else:  # line
            # Create line plot
            plt.plot(all_positions, df_sorted['iops'], 
                    marker='o', linewidth=3, markersize=8, 
                    color='steelblue', markerfacecolor='lightblue', 
                    markeredgecolor='navy', markeredgewidth=2)
            
            # Add IOPS values on dots if number of VMs is less than 100
            # not making sense to add this for more than 20 VMs - simly not readable! 
            if len(df_sorted) < 100:
                for i, (pos, iops) in enumerate(zip(all_positions, df_sorted['iops'])):
                    plt.text(pos, iops + max(df_sorted['iops']) * 0.02, 
                            f'{iops:,}', ha='center', va='bottom', fontsize=8, fontweight='bold')
            
            # Add grid for better readability
            plt.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
            plt.ylim(bottom=0)
            y_max = df_sorted['iops'].max()
            plt.ylim(top=y_max * 1.1)
        
        # Get FIO configuration for subtitle
        config_key = (operation, block_size)
        subtitle = ""
        if config_key in FIO_CONFIGS:
            subtitle = format_fio_subtitle(FIO_CONFIGS[config_key])
        
        # Customize the plot
        chart_type = "Bar Chart" if graph_type == 'bar' else "Line Chart"
        num_vms = len(df_sorted)
        plt.title(f'IOPS Performance ({chart_type}): {operation.upper()} - {block_size.upper()} Block Size (Total Tested {num_vms} VMs)', 
                 fontsize=16, fontweight='bold', pad=20)
        
        # Add subtitle with FIO configuration
        # y=0.91 is the best position for the subtitle - it is not cut off and it is not too high 
        if subtitle:
            plt.suptitle(subtitle, fontsize=10, y=0.91, ha='center', va='top')
        
        plt.xlabel('VM Index', fontsize=12, fontweight='bold')
        plt.ylabel('IOPS', fontsize=12, fontweight='bold')
        
        # Set x-axis ticks based on visibility rules
        plt.xticks(x_positions, x_labels, rotation=45, ha='right')
        
        # Adjust layout to prevent label cutoff
        plt.tight_layout()
        
        # Generate PNG filename (same as CSV but with .png extension)
        png_filename = csv_file.replace('.csv', '.png')
        
        # Save the plot
        plt.savefig(png_filename, dpi=300, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        plt.close()  # Close the figure to free memory
        
        print(f"Created {graph_type} graph: {png_filename}")
        return True
        
    except Exception as e:
        print(f"Error creating {graph_type} graph for {csv_file}: {e}")
        return False

def create_operation_summary_graphs(csv_files):
    """
    Create graphs for operation summary CSV files (all block sizes combined).
    """
    if not HAS_PLOTTING:
        print("Cannot generate operation summary graphs: plotting libraries not available")
        return 0
    
    success_count = 0
    
    for csv_file in csv_files:
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
            
            # Create bar plot for each block size
            width = 0.8 / len(block_sizes)  # Width of each bar group
            colors = plt.cm.Set3(np.linspace(0, 1, len(block_sizes)))
            
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
            
            # Customize the plot
            plt.title(f'IOPS Performance Comparison: {operation.upper()} - Selected Block Sizes', 
                     fontsize=16, fontweight='bold', pad=20)
            plt.xlabel('VM Index', fontsize=12, fontweight='bold')
            plt.ylabel('IOPS', fontsize=12, fontweight='bold')
            
            # Set x-axis ticks based on visibility rules
            plt.xticks(x_positions, x_labels, rotation=45, ha='right')
            
            # Add legend with better block size names
            plt.legend(title='Block Size', bbox_to_anchor=(1.05, 1), loc='upper left')
            
            # Add grid for better readability
            plt.grid(axis='y', alpha=0.3, linestyle='--')
            
            # Adjust layout to prevent label cutoff
            plt.tight_layout()
            
            # Generate PNG filename with block sizes included
            block_sizes_str = '-'.join(block_sizes)
            png_filename = csv_file.replace('.csv', f'_comparison-{block_sizes_str}.png')
            
            # Save the plot
            plt.savefig(png_filename, dpi=300, bbox_inches='tight', 
                       facecolor='white', edgecolor='none')
            plt.close()  # Close the figure to free memory
            
            print(f"Created operation summary graph: {png_filename}")
            success_count += 1
            
        except Exception as e:
            print(f"Error creating operation summary graph for {csv_file}: {e}")
    
    return success_count

def generate_graphs(csv_files, graph_type):
    """
    Generate graphs based on the specified type.
    """
    if not HAS_PLOTTING:
        print("Cannot generate graphs: plotting libraries not available")
        return 0
    
    success_count = 0
    
    if graph_type == 'both':
        # Generate both bar and line graphs with suffixes
        for csv_file in csv_files:
            if create_bar_graph(csv_file):
                success_count += 1
            if create_line_graph(csv_file):
                success_count += 1
    else:
        # Generate simple graphs without suffixes
        for csv_file in csv_files:
            if create_simple_graphs(csv_file, graph_type):
                success_count += 1
    
    return success_count

def main():
    """
    Main function to process all vm-* directories and create CSV/graph output.
    """
    parser = argparse.ArgumentParser(
        description='Comprehensive IOPS Analysis Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 iops_analyzer.py                                    # Generate bar charts
  python3 iops_analyzer.py --graphs line                      # Generate line graphs
  python3 iops_analyzer.py --graphs both                      # Generate both types
  python3 iops_analyzer.py --operation-summary                # Generate operation summary
  python3 iops_analyzer.py --operation-summary --block-sizes 4k,8k,128k  # Select specific block sizes
        """
    )
    
    parser.add_argument('--graphs', 
                       choices=['bar', 'line', 'both'], 
                       default='bar',
                       help='Type of graphs to generate (default: bar)')
    
    parser.add_argument('--operation-summary', 
                       action='store_true',
                       help='Generate operation summary files (all block sizes combined)')
    
    parser.add_argument('--block-sizes',
                       type=str,
                       help='Comma-separated list of block sizes to include in operation summary (e.g., "4k,8k,128k")')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("COMPREHENSIVE IOPS ANALYSIS TOOL")
    print("=" * 60)
    
    # Get the current directory
    current_dir = os.getcwd()
    
    # Find all vm-* directories
    vm_dirs = glob.glob(os.path.join(current_dir, "vm-*"))
    vm_dirs.sort()  # Sort to ensure consistent order
    
    if not vm_dirs:
        print("No vm-* directories found!")
        return
    
    print(f"Found {len(vm_dirs)} vm-* directories")
    
    # Step 1: Extract IOPS data from JSON files
    print(f"\nStep 1: Extracting IOPS data from JSON files...")
    print("-" * 50)
    
    # Process all directories
    all_results = {}
    for vm_dir in vm_dirs:
        print(f"Processing {vm_dir}...")
        results = process_vm_directory(vm_dir)
        all_results.update(results)
    
    # Write CSV files
    csv_count = write_csv_files(all_results)
    
    print(f"\nTotal records processed: {len(all_results)}")
    
    # Print summary with display names
    operations = set(op for _, op, _ in all_results.keys())
    block_sizes = set(bs for _, _, bs in all_results.keys())
    display_block_sizes = [get_block_size_display_name(bs) for bs in sorted(block_sizes)]
    print(f"Operations found: {', '.join(sorted(operations))}")
    print(f"Block sizes found: {', '.join(display_block_sizes)}")
    
    # Parse selected block sizes if provided
    selected_block_sizes = None
    if args.block_sizes:
        selected_block_sizes = [bs.strip().lower() for bs in args.block_sizes.split(',')]
        display_selected = [get_block_size_display_name(bs) for bs in selected_block_sizes]
        print(f"Selected block sizes for operation summary: {', '.join(display_selected)}")
    
    # Step 2: Generate operation summary files (if requested)
    operation_summary_files = []
    if args.operation_summary:
        print(f"\nStep 2: Generating operation summary files...")
        print("-" * 50)
        operation_summary_files = write_operation_summary_csv_files(all_results, selected_block_sizes)
    
    # Step 3: Generate graphs
    if args.graphs != 'none':
        step_num = 3 if args.operation_summary else 2
        print(f"\nStep {step_num}: Generating {args.graphs} graphs...")
        print("-" * 50)
        
        # Find all CSV files
        csv_files = glob.glob("summary-*.csv")
        
        if csv_files:
            success_count = generate_graphs(csv_files, args.graphs)
            print(f"\nSuccessfully created {success_count} graphs")
            
            # List generated PNG files
            if args.graphs == 'both':
                png_files = glob.glob("summary-*_bar.png") + glob.glob("summary-*_line-chart.png")
            else:
                png_files = glob.glob("summary-*.png")
            
            if png_files:
                print(f"\nGenerated PNG files:")
                for png_file in sorted(png_files):
                    print(f"  - {png_file}")
        else:
            print("No CSV files found for graph generation")
    
    # Step 4: Generate operation summary graphs (if requested)
    if args.operation_summary and operation_summary_files:
        step_num = 4 if args.graphs != 'none' else 3
        print(f"\nStep {step_num}: Generating operation summary graphs...")
        print("-" * 50)
        
        summary_success_count = create_operation_summary_graphs(operation_summary_files)
        print(f"\nSuccessfully created {summary_success_count} operation summary graphs")
        
        # List generated operation summary PNG files
        summary_png_files = glob.glob("summary-*-all-blocks_comparison-*.png")
        if summary_png_files:
            print(f"\nGenerated operation summary PNG files:")
            for png_file in sorted(summary_png_files):
                print(f"  - {png_file}")
    
    print("\n" + "=" * 60)
    print("PROCESSING COMPLETE!")
    print("=" * 60)
    
    # Final summary
    csv_files = [f for f in os.listdir('.') if f.startswith('summary-') and f.endswith('.csv')]
    png_files = [f for f in os.listdir('.') if f.startswith('summary-') and f.endswith('.png')]
    
    print(f"Generated {len(csv_files)} CSV files")
    if args.graphs != 'none' or args.operation_summary:
        print(f"Generated {len(png_files)} PNG graphs")

if __name__ == "__main__":
    main()
