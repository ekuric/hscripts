# IOPS Analysis Tools

A comprehensive suite of Python tools for analyzing FIO (Flexible I/O Tester) performance results, with support for IOPS and bandwidth analysis, graph generation, and test comparison.

## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Tools](#tools)
  - [iops_analyzer.py](#iops_analyzerpy)
  - [analyze_bw_mean_with_graphs.py](#analyze_bw_mean_with_graphspy)
  - [analyze_bw_mean_with_line_graphs.py](#analyze_bw_mean_with_line_graphspy)
  - [test_comparison_analyzer.py](#test_comparison_analyzerpy)
  - [compare_tests.sh](#compare_testssh)
- [Usage Examples](#usage-examples)
- [Output Files](#output-files)
- [Requirements](#requirements)
- [File Structure](#file-structure)

## Overview

This toolkit provides comprehensive analysis of FIO test results with the following capabilities:

- **IOPS Analysis**: Extract and analyze IOPS metrics from FIO JSON files
- **Bandwidth Analysis**: Extract and analyze bandwidth (bw_mean) metrics
- **Graph Generation**: Create bar charts and line graphs for visualization
- **Test Comparison**: Compare results between different test runs
- **Flexible Output**: Save results to custom directories
- **Multiple Operations**: Support for read, write, randread, randwrite operations
- **Block Size Analysis**: Automatic detection and analysis of different block sizes

## Installation

### Prerequisites

- Python 3.6 or higher
- FIO test results in JSON format

### Install Dependencies

```bash
pip install -r requirements.txt
```

Or install manually:

```bash
pip install pandas matplotlib numpy
```

## Tools

### iops_analyzer.py

**Main IOPS analysis tool** that processes FIO JSON files and generates comprehensive analysis reports.

#### Features

- Extracts IOPS data from FIO JSON files in in structure `test-fio/*` directories - this means if we put inside `test-fio` directories with fio `.json` results it will analyze these results 
run `python iops_analyzer.py --help` for comprehensive help output. 

- Generates CSV files with integer IOPS values
- Creates PNG graphs (bar charts, line graphs, or both)
- Supports all operation types: read, write, randread, randwrite
- Automatic block size detection and separate files per operation/block size
- Operation summary files combining all block sizes
- Custom results directory support

#### Usage

```bash
python3 iops_analyzer.py [options]

python3 iops_analyzer.py [-h] [--graphs {bar,line,both}] [--operation-summary] [--block-sizes BLOCK_SIZES]
                        [--results RESULTS] [--input-dir INPUT_DIR]
```

#### Options

- `--graphs {bar,line,both}`: Type of graphs to generate (default: bar)
- `--operation-summary`: Generate operation summary files (all block sizes combined)
- `--block-sizes BLOCK_SIZES`: Comma-separated list of block sizes to include in operation summary (e.g., "4k,8k,128k")
- `--results RESULTS`: Directory to save results (CSV and PNG files). Default: current directory
- `--input-dir`: INPUT_DIR Directory containing FIO JSON files in subdirectories (any name). Default: current directory

- `--help`: Show help message

#### Examples

```bash
# Generate bar charts in current directory
python3 iops_analyzer.py

# Generate line graphs
python3 iops_analyzer.py --graphs line --input-dir location_of_fio_directories

# Generate both bar and line graphs
python3 iops_analyzer.py --graphs both --input-dir location_of_fio_directories

# Generate operation summary with specific block sizes
python3 iops_analyzer.py --operation-summary --block-sizes 4k,8k,128k --input-dir location_of_fio_directories

# Save results to specific directory
python3 iops_analyzer.py --results /path/to/results --input-dir location_of_fio_directories

# Combined example
python3 iops_analyzer.py --results results/run1 --graphs both --operation-summary --block-sizes 4k,8k,128k --input-dir location_of_fio_directories
```

### analyze_bw_mean_with_graphs.py

**Bandwidth analysis tool** that extracts and analyzes bandwidth (bw_mean) metrics from FIO JSON files.

#### Features

- Extracts bw_mean values from FIO JSON files
- Filters out zero values for accurate analysis
- Generates bar charts for bandwidth visualization
- Supports multiple test directories
- Per-machine and aggregated analysis
- Automatic operation and block size detection

#### Usage

```bash
usage: analyze_bw_mean_with_graphs.py [-h] [--input-dir INPUT_DIR] [--output-dir OUTPUT_DIR]
                                      [--graph-type {bar,line,both}] [--block-sizes BLOCK_SIZES]

Bandwidth Analysis Tool for FIO Results

options:
  -h, --help            show this help message and exit
  --input-dir INPUT_DIR
                        Directory containing FIO JSON files in subdirectories (any name). Default: current directory
  --output-dir OUTPUT_DIR
                        Directory to save output files (CSV and PNG). Default: current directory
  --graph-type {bar,line,both}
                        Type of graphs to generate (default: bar)
  --block-sizes BLOCK_SIZES
                        Comma-separated list of block sizes to analyze (e.g., "4k,8k,128k")

Examples:
  python3 analyze_bw_mean_with_graphs.py                    # Analyze current directory
  python3 analyze_bw_mean_with_graphs.py --input-dir /path/to/data  # Analyze specific directory
  python3 analyze_bw_mean_with_graphs.py --output-dir /path/to/results  # Save results to specific directory
  python3 analyze_bw_mean_with_graphs.py --graph-type line  # Generate line graphs
  python3 analyze_bw_mean_with_graphs.py --graph-type both  # Generate both bar and line graphs
  python3 analyze_bw_mean_with_graphs.py --block-sizes 4k,8k,128k  # Analyze specific block sizes
  python3 analyze_bw_mean_with_graphs.py --input-dir /data --output-dir /results --graph-type line --block-sizes 4k,8k  # All options
```

#### Examples

```bash
# Analyze current directory
python3 analyze_bw_mean_with_graphs.py 

# Analyze specific directories in /path/to/results

 python3 analyze_bw_mean_with_graphs.py --output-dir /path/to/results 

 # Analyze fio data saved in /data and sve results in /results 
 python3 analyze_bw_mean_with_graphs.py --input-dir /data --output-dir /results --graph-type both 

 # Analyze fio data - only 4k, 8k block sizes 

 python3 analyze_bw_mean_with_graphs.py --input-dir /data --output-dir /results --graph-type line --block-sizes 4k,8k

```

### test_comparison_analyzer.py

**Test comparison tool** that compares IOPS and bandwidth results between different test runs.

#### Features

- Compares results from different test directories
- Generates comparison graphs (bar and line charts)
- Creates comprehensive comparison reports
- Supports both IOPS and bandwidth metrics
- Block size filtering for focused analysis
- Rate IOPS information in legends
- Average per VM and total metrics

#### Usage

```bash
python3 test_comparison_analyzer.py test1_dir test2_dir [options]
```

#### Options

- `--graphs {bar,line,both,none}`: Type of graphs to generate (default: both)
- `--output-dir DIR`: Output directory for results (default: current directory)
- `--bw`: Analyze bandwidth instead of IOPS
- `--block-sizes SIZE [SIZE ...]`: Specify block sizes to analyze
- `--help`: Show help message

#### Examples

```bash
# Compare two test directories
python3 test_comparison_analyzer.py test1/ test2/

# Compare with specific graph type
python3 test_comparison_analyzer.py test1/ test2/ --graphs bar

# Compare bandwidth results
python3 test_comparison_analyzer.py test1/ test2/ --bw

# Compare specific block sizes
python3 test_comparison_analyzer.py test1/ test2/ --block-sizes 4k 8k 128k

# Save results to specific directory
python3 test_comparison_analyzer.py test1/ test2/ --output-dir comparison_results/
```

### compare_tests.sh

**Shell script wrapper** for the test comparison analyzer with additional convenience features.

#### Features

- Simplified command-line interface
- Automatic directory validation
- Status reporting
- Support for all comparison analyzer options

#### Usage

```bash
./compare_tests.sh [options] test1_dir test2_dir
```

#### Options

- `-g, --graphs TYPE`: Graph type (bar, line, both, none)
- `-o, --output-dir DIR`: Output directory
- `-b, --bandwidth`: Analyze bandwidth instead of IOPS
- `-s, --block-sizes SIZE [SIZE ...]`: Block sizes to analyze
- `-h, --help`: Show help message

#### Examples

```bash
# Basic comparison
./compare_tests.sh test1/ test2/

# Compare with specific options
./compare_tests.sh -g bar -o results/ test1/ test2/

# Compare bandwidth with specific block sizes
./compare_tests.sh -b -s 4k 8k 128k test1/ test2/
```

## Usage Examples

### Basic IOPS Analysis

```bash
# Analyze IOPS in current directory
python3 iops_analyzer.py

# Generate line graphs
python3 iops_analyzer.py --graphs line

# Save to results directory
python3 iops_analyzer.py --results results/baseline
```

### Bandwidth Analysis

```bash
# Analyze bandwidth with bar charts
python3 analyze_bw_mean_with_graphs.py

# Analyze bandwidth with line charts
python3 analyze_bw_mean_with_line_graphs.py

# Analyze specific test directories
python3 analyze_bw_mean_with_graphs.py test1/ test2/ test3/
```

### Test Comparison

```bash
# Compare two test runs
python3 test_comparison_analyzer.py baseline/ optimized/

# Compare bandwidth results
python3 test_comparison_analyzer.py baseline/ optimized/ --bw

# Compare specific block sizes
python3 test_comparison_analyzer.py baseline/ optimized/ --block-sizes 4k 8k 128k

# Using shell script wrapper
./compare_tests.sh -g both -o comparison_results/ baseline/ optimized/
```

### Advanced Usage

```bash
# Comprehensive analysis with operation summary
python3 iops_analyzer.py --results results/comprehensive --graphs both --operation-summary --block-sizes 4k,8k,128k,1024k

# Compare multiple block sizes with bandwidth analysis
python3 test_comparison_analyzer.py test1/ test2/ --bw --block-sizes 4k 8k 128k 1024k --output-dir bw_comparison/

# Generate only CSV files (no graphs)
python3 test_comparison_analyzer.py test1/ test2/ --graphs none
```

## Output Files

### IOPS Analyzer Output

- **CSV Files**: `summary-{operation}-{block_size}.csv`
- **Bar Charts**: `summary-{operation}-{block_size}_bar.png`
- **Line Charts**: `summary-{operation}-{block_size}_line-chart.png`
- **Simple Graphs**: `summary-{operation}-{block_size}.png`
- **Operation Summary**: `summary-{operation}-all-blocks.csv`
- **Operation Summary Graphs**: `summary-{operation}-all-blocks_comparison-{block_sizes}_{graph_type}.png`

### Bandwidth Analyzer Output

- **CSV Files**: `bw_mean_{operation}_{block_size}.csv`
- **Bar Charts**: `bw_mean_{operation}_{block_size}_bar.png`
- **Line Charts**: `bw_mean_{operation}_{block_size}_line.png`

### Comparison Analyzer Output

- **Comparison CSV**: `comparison_{operation}_{metric}.csv`
- **Comparison Graphs**: `comparison_{operation}_{block_size}_per_vm_{graph_type}_{metric}.png`
- **Summary Graphs**: `comparison_{operation}_summary_{graph_type}_{metric}.png`
- **Comparison Report**: `comparison_report.txt`

## Requirements

### Python Dependencies

- **pandas** (>=1.3.0): Data manipulation and analysis
- **matplotlib** (>=3.5.0): Graph and chart generation
- **numpy** (>=1.21.0): Numerical operations

### System Requirements

- Python 3.6 or higher
- FIO test results in JSON format
- Sufficient disk space for output files

## File Structure

### Expected Input Structure

```
project_directory/
├── vm-1/
│   ├── fio-test-read-bs-4k.json
│   ├── fio-test-write-bs-4k.json
│   ├── fio-test-randread-bs-8k.json
│   └── ...
├── vm-2/
│   ├── fio-test-read-bs-4k.json
│   └── ...
└── vm-N/
    └── ...
```

### Output Structure

```
results_directory/
├── summary-read-4k.csv
├── summary-read-4k_bar.png
├── summary-read-4k_line-chart.png
├── summary-write-8k.csv
├── summary-write-8k_bar.png
├── comparison_report.txt
└── ...
```

## Tips and Best Practices

### 1. Organize Your Results

```bash
# Create organized directory structure
mkdir -p results/{baseline,optimized,comparison}
python3 iops_analyzer.py --results results/baseline
python3 iops_analyzer.py --results results/optimized
python3 test_comparison_analyzer.py results/baseline/ results/optimized/ --output-dir results/comparison/
```

### 2. Focus on Specific Block Sizes

```bash
# Analyze only relevant block sizes
python3 iops_analyzer.py --operation-summary --block-sizes 4k,8k,128k
python3 test_comparison_analyzer.py test1/ test2/ --block-sizes 4k 8k 128k
```

### 3. Generate Both Graph Types

```bash
# Generate both bar and line charts for comprehensive analysis
python3 iops_analyzer.py --graphs both
python3 test_comparison_analyzer.py test1/ test2/ --graphs both
```

### 4. Use Descriptive Directory Names

```bash
# Use descriptive names for different test runs
python3 iops_analyzer.py --results results/2024-01-15_baseline_100vm
python3 iops_analyzer.py --results results/2024-01-15_optimized_100vm
```

## Troubleshooting

### Common Issues

1. **No vm-* directories found**
   - Ensure FIO test results are in the correct directory structure
   - Check that vm-* directories contain JSON files

2. **Missing dependencies**
   - Install required packages: `pip install -r requirements.txt`
   - Check Python version: `python3 --version`

3. **Permission errors**
   - Ensure write permissions for output directories
   - Use absolute paths if relative paths cause issues

4. **Empty or zero results**
   - Check FIO JSON files for valid data
   - Verify that tests completed successfully
   - Check for rate limiting or other FIO configuration issues

### Getting Help

- Use `--help` flag with any tool for detailed usage information
- Check the script docstrings for additional details
- Verify input file format and structure

## Contributing

When adding new features or fixing issues:

1. Maintain backward compatibility
2. Update this README.md with new features
3. Add appropriate help text and examples
4. Test with various input configurations
5. Follow the existing code style and structure

## License

This project is provided as-is for FIO performance analysis. Please ensure you have appropriate permissions for any test data and results.
