#!/usr/bin/env python3
"""
Script to analyze bw_mean values from FIO JSON files across multiple directories.
Extracts bw_mean values for every operation and block size, per machine and aggregated.
"""

import json
import os
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
        
        return bw_values
    
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return []

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
                bw_values = extract_bw_mean_from_json(json_file)
                
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

def main():
    """Main function."""
    print("Starting bw_mean analysis...")
    
    # Analyze all directories
    results, all_machines_results = analyze_all_directories()
    
    # Generate report
    generate_report(results, all_machines_results)
    
    # Save results to files
    save_results_to_files(results, all_machines_results)
    
    print("\nAnalysis complete!")

if __name__ == "__main__":
    main() 