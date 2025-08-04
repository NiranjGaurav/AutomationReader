#!/usr/bin/env python3
"""
COMPLETE QUERY ANALYSIS PIPELINE (PARQUET ONLY)

This pipeline processes parquet files directly in memory without creating CSV chunks.
It filters PowerBI queries, compares with result files, and analyzes unsupported functions and UDFs.

USAGE:
------
Basic usage (uses default columns):
    python complete_pipeline.py

With custom parquet path:
    python complete_pipeline.py /path/to/parquet/folder

With custom parquet path and required columns:
    python complete_pipeline.py /path/to/parquet/folder statement_type,client_application,execution_status,query_Hash

With custom parquet path, required columns, and chunk size:
    python complete_pipeline.py /path/to/parquet/folder statement_type,client_application,execution_status,query_Hash 5000

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
python complete_pipeline.py

# Custom parquet path
python complete_pipeline.py /data/parquet_files/

# Custom columns (must include filtering columns)
python complete_pipeline.py /data/parquet_files/ query_id,app_name,status,hash_value

# All custom parameters
python complete_pipeline.py /data/parquet_files/ statement_type,client_application,execution_status,query_Hash 1000

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

import pandas as pd
import math
import os
import glob
import sys
import json
from collections import defaultdict
from datetime import datetime

# ========== DEFAULT CONFIGURATION ==========
DEFAULT_PARQUET_FOLDER_PATH = "/Users/niranjgaurav/Downloads/"
DEFAULT_OUTPUT_DIR = "Inmobi_queries"
DEFAULT_CHUNK_SIZE = 2000
DEFAULT_REQUIRED_COLUMNS = ["statement_type", "client_application", "execution_status", "query_Hash"]

# ========== RUNTIME CONFIGURATION ==========
PARQUET_FOLDER_PATH = DEFAULT_PARQUET_FOLDER_PATH
OUTPUT_DIR = DEFAULT_OUTPUT_DIR
CHUNK_SIZE = DEFAULT_CHUNK_SIZE
REQUIRED_COLUMNS = DEFAULT_REQUIRED_COLUMNS

def step1_load_parquet_data():
    """Step 1: Load parquet files into memory"""
    print("\n" + "=" * 80)
    print("STEP 1: LOADING PARQUET FILES")
    print("=" * 80)
    
    # Find all parquet files
    search_pattern = os.path.join(PARQUET_FOLDER_PATH, "*.parquet")
    parquet_files = glob.glob(search_pattern, recursive=True)
    
    if not parquet_files:
        print(f"No parquet files found in {PARQUET_FOLDER_PATH}")
        return None
    
    print(f"Found {len(parquet_files)} parquet file(s)")
    
    # Create the main output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    parquet_data = {}
    
    # Process each parquet file
    for file_path in parquet_files:
        print(f"\nProcessing: {os.path.basename(file_path)}")
        
        # Get base filename
        base_filename = os.path.splitext(os.path.basename(file_path))[0]
        
        # Create output folder for this file
        file_specific_output_dir = os.path.join(OUTPUT_DIR, base_filename)
        os.makedirs(file_specific_output_dir, exist_ok=True)
        
        # Read parquet file
        df = pd.read_parquet(file_path)
        print(f"   Total rows: {len(df):,}")
        print(f"   Columns: {list(df.columns)}")
        
        # Calculate chunks for processing
        num_chunks = math.ceil(len(df) / CHUNK_SIZE)
        print(f"   Will process in {num_chunks} chunks of {CHUNK_SIZE} rows each")
        
        # Store data for processing
        parquet_data[base_filename] = {
            'dataframe': df,
            'num_chunks': num_chunks,
            'output_dir': file_specific_output_dir
        }
        
        print(f"   ✓ Loaded into memory")
    
    return parquet_data

def filter_and_compare_chunk(chunk_df, chunk_num, base_dir, base_filename):
    """Process a single chunk - filter and compare with result"""
    
    # File paths for result and output files
    result_file = os.path.join(base_dir, f"{base_filename}_part_{chunk_num}_result.csv")
    filtered_file = os.path.join(base_dir, f"{base_filename}_part_{chunk_num}_filtered.csv")
    final_file = os.path.join(base_dir, f"{base_filename}_part_{chunk_num}_final.csv")
    
    # Check if result file exists
    if not os.path.exists(result_file):
        return False, f"Result file not found: {os.path.basename(result_file)}"
    
    try:
        # Check if required columns exist
        missing_cols = [col for col in REQUIRED_COLUMNS if col not in chunk_df.columns]
        if missing_cols:
            return False, f"Missing required columns: {missing_cols}"
        
        # Apply filters using the configured column names
        # Assume first 4 columns are: statement_type, client_application, execution_status, query_Hash
        stmt_col, app_col, status_col, hash_col = REQUIRED_COLUMNS[0], REQUIRED_COLUMNS[1], REQUIRED_COLUMNS[2], REQUIRED_COLUMNS[3]
        
        filtered_df = chunk_df[
            (chunk_df[stmt_col] == 'SELECT') &
            (chunk_df[app_col] == 'PowerBI') &
            (chunk_df[status_col] == 'FINISHED')
        ]
        
        if len(filtered_df) == 0:
            return False, "No rows matched the filter criteria"
        
        # Save only unique query_Hash values from filtered data using the configured hash column
        unique_hashes_df = filtered_df[[hash_col]].drop_duplicates()
        unique_hashes_df.to_csv(filtered_file, index=False)
        
        # Step 2: Read result file and compare
        result_df = pd.read_csv(result_file)
        
        # Check if result file has required columns
        if 'original_query' not in result_df.columns:
            return False, "Result file missing 'original_query' column"
        
        # Get query hashes from filtered data
        filtered_hashes = set(unique_hashes_df[hash_col].dropna())
        
        # Extract hash from result file's original_query column
        result_df['extracted_hash'] = result_df['original_query'].str.extract(r'inmobi::([a-f0-9]{64})')
        matching_results = result_df[result_df['extracted_hash'].isin(filtered_hashes)]
        
        # Keep only matching records from result file (all columns)
        final_df = matching_results.copy()
        
        # Save final data
        if len(final_df) > 0:
            final_df.to_csv(final_file, index=False)
            return True, f"Success: {len(final_df)} records"
        else:
            return False, "No matching records found between filtered and result files"
        
    except Exception as e:
        return False, f"Error: {str(e)}"

def step2_filter_and_compare(parquet_data):
    """Step 2: Filter PowerBI queries and compare with results"""
    print("\n" + "=" * 80)
    print("STEP 2: FILTERING POWERBI QUERIES AND COMPARING WITH RESULTS")
    print("=" * 80)
    
    total_success = 0
    
    for base_filename, data in parquet_data.items():
        print(f"\nProcessing dataset: {base_filename}")
        base_dir = data['output_dir']
        df = data['dataframe']
        num_chunks = data['num_chunks']
        
        # Check if we have result files
        result_pattern = os.path.join(base_dir, "*_result.csv")
        result_files = glob.glob(result_pattern)
        
        if not result_files:
            print("   ⚠ No result files found. Skipping this dataset.")
            continue
        
        print(f"   Found {len(result_files)} result files")
        print("   Processing chunks...")
        
        success_count = 0
        failed_files = []
        
        # Process chunks
        for i in range(num_chunks):
            chunk_num = i + 1
            if chunk_num % 10 == 0:
                print(f"   Progress: {chunk_num}/{num_chunks}")
            
            # Get chunk data
            start_index = i * CHUNK_SIZE
            end_index = start_index + CHUNK_SIZE
            chunk_df = df.iloc[start_index:end_index]
            
            success, message = filter_and_compare_chunk(chunk_df, chunk_num, base_dir, base_filename)
            if success:
                success_count += 1
            else:
                failed_files.append((chunk_num, message))
        
        print(f"   ✓ Completed: {success_count}/{num_chunks} chunks successful")
        
        if failed_files and len(failed_files) < 10:
            print(f"   Failed chunks: {[f[0] for f in failed_files[:5]]}")
        
        total_success += success_count
    
    return total_success

def parse_function_list(func_string):
    """Parse function list strings (works for both unsupported_functions and udf_list)"""
    if pd.isna(func_string) or func_string == '' or func_string == '[]':
        return []
    
    try:
        if func_string.startswith('[') and func_string.endswith(']'):
            cleaned = func_string.replace("'", '"')
            functions = json.loads(cleaned)
            return functions if isinstance(functions, list) else [functions]
        else:
            return [func_string.strip()]
    except:
        cleaned = func_string.strip('[]').replace("'", "").replace('"', '')
        if cleaned:
            return [f.strip() for f in cleaned.split(',') if f.strip()]
        return []

def analyze_single_file(file_num, base_dir):
    """Analyze a single final CSV file for unsupported functions and UDFs"""
    final_file = os.path.join(base_dir, f"queries-hashed.snappy_part_{file_num}_final.csv")
    unsupported_file = os.path.join(base_dir, f"queries-hashed.snappy_part_{file_num}_final_unsupported.csv")
    udf_file = os.path.join(base_dir, f"queries-hashed.snappy_part_{file_num}_final_udfs.csv")
    
    if not os.path.exists(final_file):
        return {
            'success': False,
            'error': f"Final file not found",
            'unsupported_functions': {},
            'udfs': {},
            'unsupported_records': 0,
            'udf_records': 0
        }
    
    try:
        # Read the final file
        df = pd.read_csv(final_file)
        
        # Check required columns
        required_cols = ['unsupported_functions', 'udf_list']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            return {
                'success': False,
                'error': f"Missing columns: {missing_cols}",
                'unsupported_functions': {},
                'udfs': {},
                'unsupported_records': 0,
                'udf_records': 0
            }
        
        # Process unsupported functions
        unsupported_df = df[
            df['unsupported_functions'].notna() & 
            (df['unsupported_functions'] != '') & 
            (df['unsupported_functions'] != '[]') &
            (df['unsupported_functions'].str.strip() != '[]')
        ]
        
        # Process UDFs
        udf_df = df[
            df['udf_list'].notna() & 
            (df['udf_list'] != '') & 
            (df['udf_list'] != '[]') &
            (df['udf_list'].str.strip() != '[]')
        ]
        
        # Count individual unsupported functions
        unsupported_counts = defaultdict(int)
        if len(unsupported_df) > 0:
            for _, row in unsupported_df.iterrows():
                functions = parse_function_list(row['unsupported_functions'])
                for func in functions:
                    if func:
                        unsupported_counts[func] += 1
            
            # Save unsupported records
            unsupported_df.to_csv(unsupported_file, index=False)
        
        # Count individual UDFs
        udf_counts = defaultdict(int)
        if len(udf_df) > 0:
            for _, row in udf_df.iterrows():
                udfs = parse_function_list(row['udf_list'])
                for udf in udfs:
                    if udf:
                        udf_counts[udf] += 1
            
            # Save UDF records
            udf_df.to_csv(udf_file, index=False)
        
        return {
            'success': True,
            'error': None,
            'unsupported_functions': dict(unsupported_counts),
            'udfs': dict(udf_counts),
            'unsupported_records': len(unsupported_df),
            'udf_records': len(udf_df)
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': f"Error: {str(e)}",
            'unsupported_functions': {},
            'udfs': {},
            'unsupported_records': 0,
            'udf_records': 0
        }

def step3_comprehensive_analysis(parquet_data):
    """Step 3: Comprehensive analysis of unsupported functions and UDFs"""
    print("\n" + "=" * 80)
    print("STEP 3: COMPREHENSIVE ANALYSIS (UNSUPPORTED FUNCTIONS & UDFs)")
    print("=" * 80)
    
    for base_filename, data in parquet_data.items():
        print(f"\nAnalyzing dataset: {base_filename}")
        base_dir = data['output_dir']
        
        # Check how many final files we actually have
        final_pattern = os.path.join(base_dir, "*_final.csv")
        final_files = glob.glob(final_pattern)
        
        if not final_files:
            print("   ⚠ No final files found. Skipping analysis for this dataset.")
            continue
        
        print(f"   Found {len(final_files)} final files to analyze")
        
        # Process all files
        all_results = {}
        success_count = 0
        total_unsupported_records = 0
        total_udf_records = 0
        files_with_unsupported = 0
        files_with_udfs = 0
        
        # Get file numbers from final files
        file_numbers = []
        for f in final_files:
            try:
                # Extract number from filename like "queries-hashed.snappy_part_140_final.csv"
                parts = os.path.basename(f).split('_')
                for i, part in enumerate(parts):
                    if part == 'part' and i + 1 < len(parts):
                        file_numbers.append(int(parts[i + 1]))
                        break
            except:
                continue
        
        file_numbers.sort()
        
        for i, num in enumerate(file_numbers):
            if i % 10 == 0:
                print(f"   Progress: {i+1}/{len(file_numbers)}")
            
            result = analyze_single_file(num, base_dir)
            all_results[num] = result
            
            if result['success']:
                success_count += 1
                total_unsupported_records += result['unsupported_records']
                total_udf_records += result['udf_records']
                
                if result['unsupported_records'] > 0:
                    files_with_unsupported += 1
                if result['udf_records'] > 0:
                    files_with_udfs += 1
        
        print(f"   ✓ Analysis completed: {success_count}/{len(file_numbers)} files processed")
        
        # Create summaries
        merged_unsupported = defaultdict(lambda: {'total_count': 0, 'chunks': set()})
        merged_udfs = defaultdict(lambda: {'total_count': 0, 'chunks': set()})
        
        for file_num, result in all_results.items():
            if result['success']:
                # Add unsupported functions
                for func_name, count in result['unsupported_functions'].items():
                    merged_unsupported[func_name]['total_count'] += count
                    merged_unsupported[func_name]['chunks'].add(file_num)
                
                # Add UDFs
                for udf_name, count in result['udfs'].items():
                    merged_udfs[udf_name]['total_count'] += count
                    merged_udfs[udf_name]['chunks'].add(file_num)
        
        # Save summary files in base directory
        if merged_unsupported:
            unsupported_data = []
            for func_name, stats in merged_unsupported.items():
                unsupported_data.append({
                    'function_name': func_name,
                    'total_occurrences': stats['total_count'],
                    'number_of_chunks': len(stats['chunks']),
                    'chunk_list': ','.join(map(str, sorted(stats['chunks'])))
                })
            
            unsupported_data.sort(key=lambda x: x['total_occurrences'], reverse=True)
            unsupported_df = pd.DataFrame(unsupported_data)
            unsupported_df.to_csv(os.path.join(base_dir, "unsupported_functions_summary.csv"), index=False)
        
        if merged_udfs:
            udf_data = []
            for udf_name, stats in merged_udfs.items():
                udf_data.append({
                    'udf_name': udf_name,
                    'total_occurrences': stats['total_count'],
                    'number_of_chunks': len(stats['chunks']),
                    'chunk_list': ','.join(map(str, sorted(stats['chunks'])))
                })
            
            udf_data.sort(key=lambda x: x['total_occurrences'], reverse=True)
            udf_df = pd.DataFrame(udf_data)
            udf_df.to_csv(os.path.join(base_dir, "udf_summary.csv"), index=False)
        
        # Print summary
        print(f"\n   📊 ANALYSIS SUMMARY FOR {base_filename}:")
        print(f"      Files with unsupported functions: {files_with_unsupported}")
        print(f"      Files with UDFs: {files_with_udfs}")
        print(f"      Total records with unsupported functions: {total_unsupported_records}")
        print(f"      Total records with UDFs: {total_udf_records}")
        
        if merged_unsupported:
            print(f"\n      Top 5 Unsupported Functions:")
            sorted_unsupported = sorted(merged_unsupported.items(), key=lambda x: x[1]['total_count'], reverse=True)
            for i, (func_name, stats) in enumerate(sorted_unsupported[:5]):
                print(f"        {i+1}. {func_name}: {stats['total_count']} occurrences in {len(stats['chunks'])} files")
        
        if merged_udfs:
            print(f"\n      User Defined Functions:")
            sorted_udfs = sorted(merged_udfs.items(), key=lambda x: x[1]['total_count'], reverse=True)
            for func_name, stats in sorted_udfs:
                print(f"        - {func_name}: {stats['total_count']} occurrences in {len(stats['chunks'])} files")
        
        # Save comprehensive report
        report_file = os.path.join(base_dir, "pipeline_report.txt")
        with open(report_file, 'w') as f:
            f.write(f"Complete Pipeline Report\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Base Directory: {base_dir}\n")
            f.write(f"Files analyzed: {success_count}\n")
            f.write(f"Files with unsupported functions: {files_with_unsupported}\n")
            f.write(f"Files with UDFs: {files_with_udfs}\n")
            f.write(f"Total unsupported records: {total_unsupported_records}\n")
            f.write(f"Total UDF records: {total_udf_records}\n\n")
            
            if merged_unsupported:
                f.write("UNSUPPORTED FUNCTIONS:\n")
                f.write("-" * 80 + "\n")
                sorted_unsupported = sorted(merged_unsupported.items(), key=lambda x: x[1]['total_count'], reverse=True)
                for func_name, stats in sorted_unsupported:
                    f.write(f"{func_name}: {stats['total_count']} occurrences in {len(stats['chunks'])} chunks\n")
                    f.write(f"   Chunks: {','.join(map(str, sorted(stats['chunks'])))}\n\n")
            
            if merged_udfs:
                f.write("\nUSER DEFINED FUNCTIONS:\n")
                f.write("-" * 80 + "\n")
                sorted_udfs = sorted(merged_udfs.items(), key=lambda x: x[1]['total_count'], reverse=True)
                for udf_name, stats in sorted_udfs:
                    f.write(f"{udf_name}: {stats['total_count']} occurrences in {len(stats['chunks'])} chunks\n")
                    f.write(f"   Chunks: {','.join(map(str, sorted(stats['chunks'])))}\n\n")
        
        print(f"      ✓ Report saved to: {report_file}")

def main():
    """Main pipeline execution"""
    print("\n")
    print("*" * 80)
    print("COMPLETE QUERY ANALYSIS PIPELINE (PARQUET ONLY)")
    print("*" * 80)
    print(f"Parquet source: {PARQUET_FOLDER_PATH}")
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Chunk size: {CHUNK_SIZE}")
    
    start_time = datetime.now()
    
    try:
        # Step 1: Load parquet data into memory
        parquet_data = step1_load_parquet_data()
        if not parquet_data:
            print("\n❌ Pipeline failed at Step 1: No parquet files to process")
            return
        
        # Step 2: Filter and compare (if result files exist)
        step2_filter_and_compare(parquet_data)
        
        # Step 3: Comprehensive analysis
        step3_comprehensive_analysis(parquet_data)
        
        # Final summary
        end_time = datetime.now()
        duration = end_time - start_time
        
        print("\n" + "*" * 80)
        print("✅ PIPELINE COMPLETED SUCCESSFULLY")
        print("*" * 80)
        print(f"Total execution time: {duration}")
        print(f"Output locations:")
        for filename in parquet_data.keys():
            print(f"  - {os.path.join(OUTPUT_DIR, filename)}")
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

if __name__ == "__main__":
    # Parse command line arguments
    if len(sys.argv) > 1:
        PARQUET_FOLDER_PATH = sys.argv[1]
        print(f"Using custom parquet path: {PARQUET_FOLDER_PATH}")
    
    if len(sys.argv) > 2:
        # Parse required columns
        columns_arg = sys.argv[2]
        REQUIRED_COLUMNS = [col.strip() for col in columns_arg.split(',')]
        print(f"Using custom required columns: {REQUIRED_COLUMNS}")
        
        # Validate that we have at least 4 columns for filtering
        if len(REQUIRED_COLUMNS) < 4:
            print(f"❌ Error: At least 4 columns required for filtering (statement_type, client_application, execution_status, query_Hash)")
            print(f"   Provided: {len(REQUIRED_COLUMNS)} columns")
            sys.exit(1)
    
    if len(sys.argv) > 3:
        try:
            CHUNK_SIZE = int(sys.argv[3])
            print(f"Using custom chunk size: {CHUNK_SIZE}")
        except ValueError:
            print(f"❌ Error: Chunk size must be a valid integer, got: {sys.argv[3]}")
            sys.exit(1)
    
    # Show configuration summary
    print(f"\n📋 CONFIGURATION SUMMARY:")
    print(f"   Parquet folder: {PARQUET_FOLDER_PATH}")
    print(f"   Output directory: {OUTPUT_DIR}")
    print(f"   Required columns: {REQUIRED_COLUMNS}")
    print(f"   Chunk size: {CHUNK_SIZE}")
    print(f"   Expected filtering on: {REQUIRED_COLUMNS[0]}=='SELECT', {REQUIRED_COLUMNS[1]}=='PowerBI', {REQUIRED_COLUMNS[2]}=='FINISHED'")
    
    main()