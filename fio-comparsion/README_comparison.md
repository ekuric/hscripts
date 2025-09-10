# FIO Test Comparison Tool

This tool allows you to compare IOPS ( iops_mean )  and Bandwidth ( bw_mean ) results from multiple test runs using any directory names, such as comparing baseline/, optimized/, test1/, test2/, etc. You can compare 2 or more test directories in a single analysis.

## Files Created

- `test_comparison_analyzer.py` - Main Python script for comparison analysis
- `compare_tests.sh` - Shell script wrapper for easier usage

## Directory Structure Expected

```
baseline/ (or any directory name)
├── vm-1/
│   ├── fio-test-read-bs-4k.json
│   ├── fio-test-write-bs-4k.json
│   ├── fio-test-randread-bs-4k.json
│   ├── fio-test-randwrite-bs-4k.json
│   └── ... (other block sizes)
└── vm-2/
    ├── fio-test-read-bs-4k.json
    ├── fio-test-write-bs-4k.json
    └── ... (other block sizes)

optimized/ (or any directory name)
├── vm-1/
│   ├── fio-test-read-bs-4k.json
│   ├── fio-test-write-bs-4k.json
│   └── ... (other block sizes)
└── vm-2/
    ├── fio-test-read-bs-4k.json
    ├── fio-test-write-bs-4k.json
    └── ... (other block sizes)
```

This tool expect below

- fio output json files must have fio `operation` name and `block size` in its name, for example `fio-read-4k.json` or `write-fio-1024k.json` are valid names
- it expect fio output to be in `json` format so ensure your fio results are save in json format
- directory structure as stated above

In future releases we will make it to work without these limitations. 

## Usage

### Using the Shell Script (Recommended)

```bash
# Compare 2 directories (IOPS analysis - default)
./compare_tests.sh baseline/ optimized/

# Compare 4 directories
./compare_tests.sh test1/ test2/ test3/ test4/

# Bandwidth analysis
./compare_tests.sh baseline/ optimized/ --bw

# Generate only CSV files (no graphs) - useful for per-machine analysis
./compare_tests.sh test1/ test2/ --graphs none

# Generate per-machine CSV files with bandwidth analysis
./compare_tests.sh test1/ test2/ --bw --graphs none

# Bar charts only
./compare_tests.sh before/ after/ --graphs bar

# Output to specific directory
./compare_tests.sh old_config/ new_config/ --output results/

# Clean previous results and run
./compare_tests.sh baseline/ optimized/ --clean
```

### Using Python Script Directly

```bash
# Compare 2 directories (IOPS analysis - default)
python3 test_comparison_analyzer.py baseline/ optimized/

# Compare 4 directories
python3 test_comparison_analyzer.py test1/ test2/ test3/ test4/

# Bandwidth analysis
python3 test_comparison_analyzer.py baseline/ optimized/ --bw

# Bar charts only
python3 test_comparison_analyzer.py before/ after/ --graphs bar

# Output to specific directory
python3 test_comparison_analyzer.py old_config/ new_config/ --output-dir results/
```

## Output Files

The tool generates several types of output files:

### CSV Files
- `test_comparison_summary.csv` - Complete comparison data
- `comparison_{operation}_read_{metric}.csv` - Read metric comparison by operation (iops or bw)
- `comparison_{operation}_write_{metric}.csv` - Write metric comparison by operation (iops or bw)
- `{vm_name}_detailed_metrics.csv` - Individual machine detailed metrics (one file per VM)

### PNG Graphs
- `comparison_{operation}_summary_bar_{metric}.png` - Summary bar chart comparison with total metric values (sum across all VMs) displayed on bars
- `comparison_{operation}_summary_line_{metric}.png` - Summary line chart comparison with total metric values (sum across all VMs) displayed on data points
- `comparison_{operation}_{block_size}_per_vm_bar_{metric}.png` - Per-VM bar chart showing individual VM performance for specific block size (test1 vs test2 vs test3) with total sum annotations
- `comparison_{operation}_{block_size}_per_vm_line_{metric}.png` - Per-VM line chart showing individual VM performance for specific block size (test1 vs test2 vs test3) with total sum annotations

**Note**: `{metric}` is either `iops` or `bw` depending on the analysis type (--iops or --bw option)

### Text Report
- `comparison_report.txt` - Human-readable comparison report with improvement percentages

## Features

1. **Automatic Data Extraction**: Extracts IOPS, bandwidth, and latency data from FIO JSON files
   - **Multi-Job Support**: Automatically sums metrics across multiple FIO jobs running on the same machine
2. **Multiple Directory Support**: Compare 2 or more test directories in a single analysis
3. **Metric Selection**: Choose between IOPS analysis (default) or bandwidth analysis (--bw option)
4. **Sum Analysis**: Shows the total sum of all VMs for each FIO operation (e.g., sum of read IOPS/bandwidth from vm-1 + vm-2 + vm-3 + ...)
5. **Summary Comparison Graphs**: Creates single graphs comparing all test directories for each operation (sum across all VMs)
6. **Per-VM Comparison Graphs**: Creates detailed graphs showing individual VM performance across different test directories (like iops_analyzer.py)
   - **One graph per block size**: Each graph shows test1 vs test2 vs test3 for a specific block size only
   - **Clean comparison**: Easier to read and compare test results for each block size separately
   - **Total sum annotations**: Shows "Total across all VMs" values under the legend on the right side for organized display
7. **Per-Machine CSV Files**: Creates individual CSV files for each machine with detailed metrics
   - **One CSV per machine**: Each VM gets its own detailed metrics file (e.g., `vm-1_detailed_metrics.csv`)
   - **Complete metrics**: Includes IOPS, bandwidth, latency, and file information for all operations and block sizes
   - **No graphs needed**: Use `--graphs none` to generate only CSV files without any graphs
8. **FIO Configuration Subtitles**: Displays FIO job configuration on all graphs
   - **Complete configuration**: Shows size, block size, runtime, direct, numjobs, iodepth, and rate_iops (if used)
   - **Automatic extraction**: Extracts configuration from FIO JSON files automatically
   - **All graph types**: Subtitles appear on summary graphs, per-VM graphs, bar charts, and line charts
9. **Data Points Display**: Shows metric values directly on graphs (on bars for bar charts, on data points for line charts)
10. **Multiple Graph Types**: Supports bar charts, line charts, or both
11. **Performance Metrics**: Calculates improvement percentages between first and last test directories
12. **Comprehensive Reports**: Generates both CSV data and human-readable reports
13. **Flexible Output**: Supports custom output directories
14. **Flexible Directory Naming**: Works with any directory names (baseline/optimized, before/after, etc.)

## Example Output

The tool will show:
- Overall performance comparison between all test directories
- **Multi-job aggregation**: Automatically sums IOPS and bandwidth across multiple FIO jobs per machine
- IOPS or bandwidth improvements/degradations by operation and block size
- **Sum of all VMs** for each FIO operation (total performance across all machines)
- **Individual VM performance** across different test directories and block sizes
- Summary comparison graphs with data points showing exact metric values
- Per-VM comparison graphs showing detailed individual machine performance
- **Total sum annotations**: "Total across all VMs" values displayed under the legend on the right side of each per-block-size graph
- **Individual machine CSV files**: Detailed metrics for each VM in separate CSV files
- Performance differences across all block sizes for each operation
- Visual comparison with precise numerical values displayed on graphs
- Improvement percentages from first to last test directory

## Requirements

- Python 3
- pandas
- matplotlib
- numpy
- seaborn (optional, for enhanced styling)

Install requirements:
```bash
pip install pandas matplotlib numpy seaborn
```
