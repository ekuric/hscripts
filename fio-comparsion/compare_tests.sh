#!/bin/bash

# Test Comparison Script for FIO Results
# This script compares results from different test runs

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo -e "${BLUE}================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}================================${NC}"
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS] DIR1 DIR2 [DIR3 DIR4 ...]"
    echo ""
    echo "Options:"
    echo "  -h, --help              Show this help message"
    echo "  -g, --graphs TYPE       Graph type: bar, line, both (default: both)"
    echo "  -o, --output DIR        Output directory (default: current directory)"
    echo "  --iops                  Analyze IOPS performance (default)"
    echo "  --bw                    Analyze bandwidth performance"
    echo "  --clean                 Clean previous results before analysis"
    echo ""
    echo "Examples:"
    echo "  $0 baseline/ optimized/                    # Compare baseline vs optimized"
    echo "  $0 test1/ test2/ test3/ test4/             # Compare 4 test runs"
    echo "  $0 before/ after/ --graphs bar             # Bar charts only"
    echo "  $0 dir1/ dir2/ dir3/ --output results/     # Output to results directory"
    echo ""
    echo "Directory Structure Expected:"
    echo "  baseline/ (or any directory name)"
    echo "    ├── vm-1/"
    echo "    │   ├── fio-test-read-bs-4k.json"
    echo "    │   ├── fio-test-write-bs-4k.json"
    echo "    │   └── ..."
    echo "    └── vm-2/"
    echo "        ├── fio-test-read-bs-4k.json"
    echo "        └── ..."
    echo "  optimized/ (or any directory name)"
    echo "    ├── vm-1/"
    echo "    └── vm-2/"
}

# Default values
GRAPHS="both"
OUTPUT_DIR="."
CLEAN=false
METRIC_TYPE=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_usage
            exit 0
            ;;
        -g|--graphs)
            GRAPHS="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --clean)
            CLEAN=true
            shift
            ;;
        --iops)
            METRIC_TYPE="--iops"
            shift
            ;;
        --bw)
            METRIC_TYPE="--bw"
            shift
            ;;
        -*)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
        *)
            # Add directory to the list
            DIRECTORIES+=("$1")
            shift
            ;;
    esac
done

# Check if at least 2 directories are specified
if [[ ${#DIRECTORIES[@]} -lt 2 ]]; then
    print_error "At least 2 directories must be specified"
    show_usage
    exit 1
fi

# Check if all directories exist
for dir in "${DIRECTORIES[@]}"; do
    if [[ ! -d "$dir" ]]; then
        print_error "Directory '$dir' does not exist"
        exit 1
    fi
done

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Clean previous results if requested
if [[ "$CLEAN" == true ]]; then
    print_status "Cleaning previous results..."
    rm -f "${OUTPUT_DIR}/comparison_*.csv"
    rm -f "${OUTPUT_DIR}/comparison_*.png"
    rm -f "${OUTPUT_DIR}/test_comparison_*.csv"
    rm -f "${OUTPUT_DIR}/comparison_report.txt"
fi

# Check if Python 3 is available
if ! command -v python3 &> /dev/null; then
    print_error "Python 3 is not installed or not in PATH"
    exit 1
fi

# Check if required Python packages are available
python3 -c "import pandas, matplotlib, numpy" 2>/dev/null || {
    print_error "Required Python packages not found. Please install: pandas, matplotlib, numpy"
    exit 1
}

# Check for optional seaborn package
python3 -c "import seaborn" 2>/dev/null || {
    print_warning "Optional package seaborn not found. Some styling features may be limited."
}

# Check if the comparison script exists
if [[ ! -f "test_comparison_analyzer.py" ]]; then
    print_error "test_comparison_analyzer.py not found in current directory"
    exit 1
fi

# Main execution
print_header "FIO Test Comparison Tool"
print_status "Directories to compare: ${#DIRECTORIES[@]} directories"
for i in "${!DIRECTORIES[@]}"; do
    print_status "  Directory $((i+1)): ${DIRECTORIES[i]}"
done
print_status "Output directory: $OUTPUT_DIR"
print_status "Graph type: $GRAPHS"

# Build the command
CMD="python3 test_comparison_analyzer.py"
for dir in "${DIRECTORIES[@]}"; do
    CMD="$CMD \"$dir\""
done
CMD="$CMD --graphs \"$GRAPHS\" --output-dir \"$OUTPUT_DIR\""
if [[ -n "$METRIC_TYPE" ]]; then
    CMD="$CMD $METRIC_TYPE"
fi


# Run the comparison
print_header "Running Test Comparison"
print_status "Executing: $CMD"

eval $CMD

if [[ $? -eq 0 ]]; then
    print_status "Test comparison completed successfully!"
    
    # Show results
    print_header "Generated Files"
    if [[ -d "$OUTPUT_DIR" ]]; then
        cd "$OUTPUT_DIR"
        print_status "Files in $OUTPUT_DIR:"
        ls -la comparison_* test_comparison_* 2>/dev/null || print_warning "No comparison files found"
    fi
else
    print_error "Test comparison failed"
    exit 1
fi
