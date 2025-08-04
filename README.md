# Modular Query Analysis Pipeline

A modular version of the complete query analysis pipeline that processes parquet files directly in memory, filters PowerBI queries, and analyzes unsupported functions and UDFs.

## Directory Structure

```
modular_pipeline/
├── __init__.py          # Package initialization
├── config.py            # Configuration settings and runtime variables
├── data_loader.py       # Parquet file loading functionality
├── query_filter.py      # Query filtering and comparison with result files
├── function_utils.py    # Utility functions for parsing function lists
├── analyzer.py          # Comprehensive analysis of functions and UDFs
├── main.py             # Main pipeline orchestration and CLI
└── README.md           # This file
```

## Modules Overview

### 1. `config.py`
- Contains all configuration settings and default values
- Provides functions to update runtime configuration
- Manages global variables for parquet path, columns, chunk size, etc.

### 2. `data_loader.py`
- Handles loading parquet files into memory
- Creates directory structures for output
- Calculates chunk information for processing

### 3. `query_filter.py`
- Filters PowerBI queries based on configurable criteria
- Compares filtered queries with result files
- Creates final matched datasets

### 4. `function_utils.py`
- Utility functions for parsing JSON-formatted function lists
- Analyzes individual files for unsupported functions and UDFs
- Handles different formats of function list data

### 5. `analyzer.py`
- Performs comprehensive analysis across all processed files
- Creates summary reports and statistics
- Generates detailed reports with function occurrences

### 6. `main.py`
- Main entry point and command-line interface
- Orchestrates the entire pipeline execution
- Handles argument parsing and configuration

## Usage

### Basic Usage

```bash
# Use default settings
python main.py

# Run from parent directory
python modular_pipeline/main.py
```

### Advanced Usage

```bash
# Custom parquet path
python main.py /path/to/parquet/folder

# Custom columns (must include equivalent filtering columns)
python main.py /data/parquet/ statement_type,client_application,execution_status,query_Hash

# All custom parameters
python main.py /data/parquet/ stmt_type,app_name,status,hash_id 1000
```

### Running as a Module

```bash
# From the parent directory
python -m modular_pipeline.main

# With arguments
python -m modular_pipeline.main /data/parquet/ statement_type,client_application,execution_status,query_Hash 5000
```

## Configuration

### Default Settings
- **Parquet Path**: `/Users/niranjgaurav/Downloads/`
- **Output Directory**: `Inmobi_queries`
- **Chunk Size**: `2000`
- **Required Columns**: `["statement_type", "client_application", "execution_status", "query_Hash"]`

### Command Line Arguments
1. **parquet_path** (optional): Path to folder containing parquet files
2. **required_columns** (optional): Comma-separated list of column names
3. **chunk_size** (optional): Number of rows per processing chunk

## Output Files

For each parquet file processed, the pipeline creates:
- `*_filtered.csv`: Unique query hashes that match filter criteria
- `*_final.csv`: Complete records from result files matching filtered hashes
- `*_final_unsupported.csv`: Records with unsupported functions
- `*_final_udfs.csv`: Records with user-defined functions
- `unsupported_functions_summary.csv`: Summary of all unsupported functions
- `udf_summary.csv`: Summary of all user-defined functions
- `pipeline_report.txt`: Comprehensive analysis report

## Filtering Logic

The pipeline filters rows where:
- `statement_type == 'SELECT'`
- `client_application == 'PowerBI'`
- `execution_status == 'FINISHED'`

When using custom column names, ensure they contain equivalent data for filtering.

## Advantages of Modular Design

1. **Maintainability**: Each module has a specific responsibility
2. **Reusability**: Individual modules can be imported and used separately
3. **Testability**: Each module can be tested independently
4. **Readability**: Code is organized logically and easier to understand
5. **Extensibility**: New features can be added by creating new modules

## Examples

### Using Individual Modules

```python
from modular_pipeline import config, data_loader, query_filter

# Configure settings
config.set_config(parquet_path="/my/data/path", chunk_size=5000)

# Load data
parquet_data = data_loader.load_parquet_data()

# Filter and compare
results = query_filter.filter_and_compare(parquet_data)
```

### Custom Configuration

```python
from modular_pipeline import config

# Update configuration
config.set_config(
    parquet_path="/custom/path",
    columns=["stmt", "app", "status", "hash"],
    chunk_size=1500
)

# Get current configuration
current_config = config.get_config()
print(current_config)
```

## Requirements

- Python 3.6+
- pandas
- Standard library modules: os, glob, sys, json, math, collections, datetime

## Error Handling

The modular pipeline includes comprehensive error handling:
- Validates command line arguments
- Checks for required columns in data
- Handles missing files gracefully
- Provides detailed error messages
- Continues processing when individual files fail
