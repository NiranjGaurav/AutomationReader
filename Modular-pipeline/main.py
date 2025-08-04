#!/usr/bin/env python3
"""
MODULAR QUERY ANALYSIS PIPELINE (PARQUET ONLY)

This modular pipeline processes parquet files directly in memory without creating CSV chunks.
It filters PowerBI queries, compares with result files, and analyzes unsupported functions and UDFs.

USAGE:
------
Basic usage (uses default columns):
    python main.py

With custom parquet path:
    python main.py /path/to/parquet/folder

With custom parquet path and required columns:
    python main.py /path/to/parquet/folder statement_type,client_application,execution_status,query_Hash

With custom parquet path, required columns, and chunk size:
    python main.py /path/to/parquet/folder statement_type,client_application,execution_status,query_Hash 5000

ARGUMENTS:
----------
1. parquet_path (optional): Path to folder containing parquet files
   Default: "/Users/niranjgaurav/Downloads/"

2. required_columns (optional): Comma-separated list of column names required for filtering
   Default: "statement_type,client_application,execution_status,query_Hash"
   
3. chunk_size (optional): Number of rows per processing chunk
   Default: 2000

EXAMPLES:
---------
# Use defaults
python main.py

# Custom parquet path
python main.py /data/parquet_files/

# Custom columns (must include filtering columns)
python main.py /data/parquet_files/ query_id,app_name,status,hash_value

# All custom parameters
python main.py /data/parquet_files/ statement_type,client_application,execution_status,query_Hash 1000

FILTERING LOGIC:
---------------
The pipeline filters rows where:
- statement_type == 'SELECT'
- client_application == 'PowerBI' 
- execution_status == 'FINISHED'

Note: If you specify custom column names, make sure they contain equivalent columns for filtering.

OUTPUT FILES:
------------
For each parquet file processed, creates:
- *_filtered.csv: Unique query hashes that match filter criteria
- *_final.csv: Complete records from result files matching filtered hashes
- *_final_unsupported.csv: Records with unsupported functions
- *_final_udfs.csv: Records with user-defined functions
- unsupported_functions_summary.csv: Summary of all unsupported functions
- udf_summary.csv: Summary of all user-defined functions
- pipeline_report.txt: Comprehensive analysis report
"""

import sys
import os
from datetime import datetime

# Import pipeline modules
try:
    from . import config
    from .data_loader import load_parquet_data
    from .query_filter import filter_and_compare
    from .analyzer import comprehensive_analysis
except ImportError:
    import config
    from data_loader import load_parquet_data
    from query_filter import filter_and_compare
    from analyzer import comprehensive_analysis

def parse_arguments():
    """Parse command line arguments and update configuration"""
    
    # Parse command line arguments
    if len(sys.argv) > 1:
        config.set_config(parquet_path=sys.argv[1])
        print(f"Using custom parquet path: {config.PARQUET_FOLDER_PATH}")
    
    if len(sys.argv) > 2:
        # Parse required columns
        columns_arg = sys.argv[2]
        columns = [col.strip() for col in columns_arg.split(',')]
        config.set_config(columns=columns)
        print(f"Using custom required columns: {config.REQUIRED_COLUMNS}")
        
        # Validate that we have at least 4 columns for filtering
        if len(config.REQUIRED_COLUMNS) < 4:
            print(f"❌ Error: At least 4 columns required for filtering (statement_type, client_application, execution_status, query_Hash)")
            print(f"   Provided: {len(config.REQUIRED_COLUMNS)} columns")
            sys.exit(1)
    
    if len(sys.argv) > 3:
        try:
            chunk_size = int(sys.argv[3])
            config.set_config(chunk_size=chunk_size)
            print(f"Using custom chunk size: {config.CHUNK_SIZE}")
        except ValueError:
            print(f"❌ Error: Chunk size must be a valid integer, got: {sys.argv[3]}")
            sys.exit(1)

def show_configuration():
    """Display current configuration summary"""
    print(f"\n📋 CONFIGURATION SUMMARY:")
    print(f"   Parquet folder: {config.PARQUET_FOLDER_PATH}")
    print(f"   Output directory: {config.OUTPUT_DIR}")
    print(f"   Required columns: {config.REQUIRED_COLUMNS}")
    print(f"   Chunk size: {config.CHUNK_SIZE}")
    print(f"   Expected filtering on: {config.REQUIRED_COLUMNS[0]}=='SELECT', {config.REQUIRED_COLUMNS[1]}=='PowerBI', {config.REQUIRED_COLUMNS[2]}=='FINISHED'")

def run_pipeline():
    """Execute the complete pipeline"""
    print("\n")
    print("*" * 80)
    print("MODULAR QUERY ANALYSIS PIPELINE (PARQUET ONLY)")
    print("*" * 80)
    print(f"Parquet source: {config.PARQUET_FOLDER_PATH}")
    print(f"Output directory: {config.OUTPUT_DIR}")
    print(f"Chunk size: {config.CHUNK_SIZE}")
    
    start_time = datetime.now()
    
    try:
        # Step 1: Load parquet data into memory
        parquet_data = load_parquet_data()
        if not parquet_data:
            print("\n❌ Pipeline failed at Step 1: No parquet files to process")
            return
        
        # Step 2: Filter and compare (if result files exist)
        filter_and_compare(parquet_data)
        
        # Step 3: Comprehensive analysis
        comprehensive_analysis(parquet_data)
        
        # Final summary
        end_time = datetime.now()
        duration = end_time - start_time
        
        print("\n" + "*" * 80)
        print("✅ PIPELINE COMPLETED SUCCESSFULLY")
        print("*" * 80)
        print(f"Total execution time: {duration}")
        print(f"Output locations:")
        for filename in parquet_data.keys():
            print(f"  - {os.path.join(config.OUTPUT_DIR, filename)}")
        print("\nGenerated files (per dataset):")
        print("  - Filtered queries: *_filtered.csv")
        print("  - Final matched queries: *_final.csv")
        print("  - Unsupported functions: *_final_unsupported.csv")
        print("  - UDF records: *_final_udfs.csv")
        print("  - Summary files: unsupported_functions_summary.csv, udf_summary.csv")
        print("  - Report: pipeline_report.txt")
        print("\nNote: Data processed entirely in memory without creating CSV chunks")
        
    except Exception as e:
        print(f"\n❌ Pipeline failed with error: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main entry point for the modular pipeline"""
    # Parse command line arguments
    parse_arguments()
    
    # Show configuration summary
    show_configuration()
    
    # Run the pipeline
    run_pipeline()

if __name__ == "__main__":
    main()