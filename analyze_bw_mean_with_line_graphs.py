#!/usr/bin/env python3
"""
Script to analyze bw_mean values from FIO JSON files across multiple directories.
Extracts bw_mean values for every operation and block size, per machine and aggregated.
"""

import json
import os
import matplotlib.pyplot as plt
import pandas as pd
import glob
import re
from collections import defaultdict
from pathlib import Path

def extract_bw_mean_from_json(file_path):
    """Extract bw_mean values from a JSON file, filtering out zero values."""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        bw_values = []

        # Extract FIO parameters for subtitle
        fio_params = {}
        
        # Look for bw_mean in the jobs section (filtering out zero values)
        if 'jobs' in data:
            for job in data['jobs']:
                # Check read operations
                if 'read' in job and 'bw_mean' in job['read']:
                    bw_mean_val = job['read']['bw_mean']
                    # Skip zero values
                    if bw_mean_val > 0:
                        bw_values.append({
                            'operation': 'read',
                            'bw_mean': int(bw_mean_val),
                            'job_name': job.get('jobname', 'unknown')
                        })
                
                # Check write operations
                if 'write' in job and 'bw_mean' in job['write']:
                    bw_mean_val = job['write']['bw_mean']
                    # Skip zero values
                    if bw_mean_val > 0:
                        bw_values.append({
                            'operation': 'write',
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
        

        # Extract FIO parameters from first job
        if 'jobs' in data and len(data['jobs']) > 0:
            job_options = data['jobs'][0].get('job options', {})
            fio_params = {
                'size': job_options.get('size', 'N/A'),
                'bs': job_options.get('bs', 'N/A'),
                'runtime': job_options.get('runtime', 'N/A'),
                'direct': job_options.get('direct', 'N/A'),
                'numjobs': job_options.get('numjobs', 'N/A'),
                'iodepth': job_options.get('iodepth', 'N/A')
            }
        
        return bw_values, fio_params
    
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return [], {}

def parse_filename_info(filename):
    """Parse operation and block size from filename."""
    # Pattern: fio-test-{operation}-bs-{blocksize}.json
    match = re.match(r'fio-test-(\w+)-bs-(\w+)\.json', filename)
    if match:
        return match.group(1), match.group(2)
    return None, None

def analyze_all_directories():
    """Analyze all directories for JSON files and extract bw_mean values."""
    
    # Get all directories
    directories = [d for d in os.listdir('.') if os.path.isdir(d) and not d.startswith('.')]
    
    # Results storage
    results = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    all_machines_results = defaultdict(lambda: defaultdict(list))
    
    print(f"Found directories: {directories}")
    
    for directory in directories:
        print(f"\nAnalyzing directory: {directory}")
        
        # Find all JSON files in this directory
        json_files = glob.glob(os.path.join(directory, "*.json"))
        
        for json_file in json_files:
            filename = os.path.basename(json_file)
            operation, block_size = parse_filename_info(filename)
            
            if operation and block_size:
                print(f"  Processing: {filename} (op: {operation}, bs: {block_size})")
                
                # Extract bw_mean values
                bw_values, fio_params = extract_bw_mean_from_json(json_file)
                
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
    """Calculate statistics for a list of bw_mean values."""
    if not values:
        return {}
    
    bw_values = [v['bw_mean'] for v in values if isinstance(v['bw_mean'], (int, float))]
    
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
                    machine_stats[item['machine']].append(item['bw_mean'])
                
                print(f"    Per Machine:")
                for machine in sorted(machine_stats.keys()):
                    machine_mean = sum(machine_stats[machine]) / len(machine_stats[machine])
                    print(f"      {machine}: {machine_mean:.2f} (n={len(machine_stats[machine])})")

def save_results_to_files(results, all_machines_results):
    """Save results to CSV files for further analysis."""
    
    # Save per-machine results
    for machine in results.keys():
        filename = f"{machine}_bw_mean_results.csv"
        with open(filename, 'w') as f:
            f.write("Operation,BlockSize,JobName,BwMean\n")
            for operation in results[machine].keys():
                for block_size in results[machine][operation].keys():
                    for item in results[machine][operation][block_size]:
                        f.write(f"{operation},{block_size},{item['job_name']},{item['bw_mean']}\n")
        print(f"Saved per-machine results to: {filename}")
    
    # Save aggregated results per operation
    for operation in all_machines_results.keys():
        filename = f"{operation}_all_machines_bw_mean_results.csv"
        with open(filename, 'w') as f:
            f.write("BlockSize,Machine,JobName,BwMean\n")
            for block_size in all_machines_results[operation].keys():
                for item in all_machines_results[operation][block_size]:
                    f.write(f"{block_size},{item['machine']},{item['job_name']},{item['bw_mean']}\n")
        print(f"Saved {operation} aggregated results to: {filename}")
    
    # Save results per block size and operation combination
    for operation in all_machines_results.keys():
        for block_size in all_machines_results[operation].keys():
            # Create filename with block size and operation
            # Replace 'k' with 'k' and format the filename
            if block_size.endswith('k'):
                size_part = block_size
            else:

                size_part = block_size
            
            filename = f"all_machines_block_size_{size_part}_operation_{operation}.csv"
            with open(filename, 'w') as f:
                f.write("Machine,JobName,BwMean\n")
                for item in all_machines_results[operation][block_size]:
                    f.write(f"{item['machine']},{item['job_name']},{item['bw_mean']}\n")
            print(f"Saved block size {block_size} operation {operation} results to: {filename}")
    
    # Also save combined results for backward compatibility
    filename = "all_machines_bw_mean_results.csv"
    with open(filename, 'w') as f:
        f.write("Operation,BlockSize,Machine,JobName,BwMean\n")
        for operation in all_machines_results.keys():
            for block_size in all_machines_results[operation].keys():
                for item in all_machines_results[operation][block_size]:
                    f.write(f"{operation},{block_size},{item['machine']},{item['job_name']},{item['bw_mean']}\n")
    print(f"Saved combined aggregated results to: {filename}")
    
    # Save job-summarized results (sum of all jobs per machine)
    save_job_summarized_results(results, all_machines_results)

def save_job_summarized_results(results, all_machines_results):
    """Save results with sum of all jobs per machine, operation, and block size."""
    
    # Save per-machine job-summarized results
    for machine in results.keys():
        filename = f"{machine}_bw_mean_job_summary.csv"
        with open(filename, 'w') as f:
            f.write("Operation,BlockSize,TotalBwMean\n")
            for operation in results[machine].keys():
                for block_size in results[machine][operation].keys():
                    items = results[machine][operation][block_size]
                    total_bw = sum(item['bw_mean'] for item in items)
                    
                    f.write(f"{operation},{block_size},{total_bw}\n")
        print(f"Saved job-summarized results to: {filename}")
    
    # Save all machines job-summarized results
    filename = "all_machines_bw_mean_job_summary.csv"
    with open(filename, 'w') as f:
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
                    total_bw = sum(item['bw_mean'] for item in items)
                    
                    f.write(f"{operation},{block_size},{machine},{total_bw}\n")
    
    print(f"Saved all machines job-summarized results to: {filename}")
    
    # Save block size and operation specific job-summarized results
    for operation in all_machines_results.keys():
        for block_size in all_machines_results[operation].keys():
            filename = f"all_machines_block_size_{block_size}_operation_{operation}_job_summary.csv"
            with open(filename, 'w') as f:
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
                    total_bw = sum(item['bw_mean'] for item in items)
                    
                    f.write(f"{machine},{total_bw}\n")
            
            print(f"Saved job-summarized results to: {filename}")






def main():

    """Main function."""
    print("Starting bw_mean analysis...")
    
    # Analyze all directories
    results, all_machines_results = analyze_all_directories()
    
    # Generate report
    generate_report(results, all_machines_results)
    
    # Save results to files
    save_results_to_files(results, all_machines_results)
    
    # Create graphs from job summaries
    create_graphs_from_job_summaries()
    
    print("\nAnalysis complete!")


def extract_fio_params_from_csv_filename(csv_filename):
    """Extract FIO parameters from the original JSON file based on CSV filename."""
    try:
        # Parse the CSV filename to get operation and block_size
        # Format: all_machines_block_size_4k_operation_read_job_summary.csv
        parts = csv_filename.replace('_job_summary.csv', '').split('_')
        if len(parts) >= 7:
            block_size = parts[4]  # 4k
            operation = parts[6]   # read
            
            # Find the original JSON file
            json_pattern = f"fio-test-{operation}-bs-{block_size}.json"
            json_files = glob.glob(f"*/{json_pattern}")
            
            if json_files:
                # Read the first JSON file found
                with open(json_files[0], 'r') as f:
                    data = json.load(f)
                
                # Extract FIO parameters from first job
                if 'jobs' in data and len(data['jobs']) > 0:
                    job_options = data['jobs'][0].get('job options', {})
                    return {
                        'size': job_options.get('size', 'N/A'),
                        'bs': job_options.get('bs', 'N/A'),
                        'runtime': job_options.get('runtime', 'N/A'),
                        'direct': job_options.get('direct', 'N/A'),
                        'numjobs': job_options.get('numjobs', 'N/A'),
                        'iodepth': job_options.get('iodepth', 'N/A')
                    }
    except Exception as e:
        print(f"Error extracting FIO parameters: {e}")
    
    return {}

def create_graphs_from_job_summaries():
    """Create graphs from block size + operation job summary CSV files."""
    try:
        import matplotlib.pyplot as plt
        import pandas as pd
        
        # Set matplotlib to use a non-interactive backend
        plt.switch_backend('Agg')
        
        # Find all job summary CSV files
        job_summary_files = glob.glob("all_machines_block_size_*_operation_*_job_summary.csv")
        
        if not job_summary_files:
            print("No block size + operation job summary files found to create graphs from.")
            return
        
        print(f"\nCreating graphs from {len(job_summary_files)} block size + operation job summary files...")
        
        for csv_file in job_summary_files:
            try:
                # Read the CSV file
                df = pd.read_csv(csv_file)
                
                # Skip if file doesn't have the expected columns
                if 'Machine' not in df.columns or 'TotalBwMean' not in df.columns:
                    print(f"Skipping {csv_file}: Missing required columns")
                    continue
                
                # Create the plot
                plt.figure(figsize=(10, 6))
                
                # Create bar chart
                machines = df['Machine'].tolist()
                total_bw = df['TotalBwMean'].tolist()
                
                bars = plt.plot(range(len(machines)), total_bw, marker='o', linewidth=1, markersize=2, color='navy', markerfacecolor='skyblue', markeredgecolor='navy')

                # Remove X-axis labels completely
                plt.xticks(range(len(machines)), [])
                
                # Customize the plot
                plt.ylabel('Total bw_mean per machine [KB]', fontsize=10, fontweight='bold')
                plt.xlabel('Test machines', fontsize=10, fontweight='bold')
                
                # Extract block size and operation from filename for title
                if 'block_size_' in csv_file and '_operation_' in csv_file:
                    # Extract from filename like: all_machines_block_size_4k_operation_read_job_summary.csv
                    parts = csv_file.replace('_job_summary.csv', '').split('_')
                    if len(parts) >= 7:
                        block_size = parts[4]  # 128k
                        operation = parts[6]   # read
                        title = f"size_{block_size}_operation_{operation}"
                    else:
                        title = csv_file.replace('_job_summary.csv', '')
                else:
                    # For per-machine files like: vm-1_bw_mean_job_summary.csv
                    title = csv_file.replace('_bw_mean_job_summary.csv', '')
                
                # Extract FIO parameters and create subtitle
                fio_params = extract_fio_params_from_csv_filename(csv_file)
                if fio_params:
                    subtitle = f"size={fio_params.get('size', 'N/A')}, bs={fio_params.get('bs', 'N/A')}, runtime={fio_params.get('runtime', 'N/A')}s, direct={fio_params.get('direct', 'N/A')}, numjobs={fio_params.get('numjobs', 'N/A')}, iodepth={fio_params.get('iodepth', 'N/A')}"
                    plt.suptitle(title, fontsize=14, fontweight='bold')
                    plt.title(subtitle, fontsize=10, style='italic')
                else:
                    plt.title(title, fontsize=14, fontweight='bold')

                # Set axis limits to include zero
                plt.xlim(-0.5, len(machines) - 0.5)
                plt.ylim(0, max(total_bw) * 1.1)
                
                # Customize grid and layout
                plt.grid(True, alpha=0.3, axis='y')
                plt.tight_layout()
                
                # Create output filename
                output_file = csv_file.replace('.csv', '.png')
                
                # Save the plot
                plt.savefig(output_file, dpi=300, bbox_inches='tight')
                plt.close()
                
                print(f"Created graph: {output_file}")
                
            except Exception as e:
                print(f"Error creating graph for {csv_file}: {e}")
                continue
        
        print(f"\nGraph creation complete! Created {len(job_summary_files)} graphs.")
        
    except ImportError as e:
        print(f"Error importing required libraries: {e}")
        print("Please install required dependencies: pip install matplotlib pandas")
    except Exception as e:
        print(f"Error in graph creation: {e}")


if __name__ == "__main__":
    main()
