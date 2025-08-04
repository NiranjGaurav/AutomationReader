#!/usr/bin/env python3
"""
Query filtering and comparison module for the pipeline.
Handles filtering PowerBI queries and comparing with result files.
"""

import pandas as pd
import os
import glob
try:
    from . import config
except ImportError:
    import config

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
        missing_cols = [col for col in config.REQUIRED_COLUMNS if col not in chunk_df.columns]
        if missing_cols:
            return False, f"Missing required columns: {missing_cols}"
        
        # Apply filters using the configured column names
        # Assume first 4 columns are: statement_type, client_application, execution_status, query_Hash
        stmt_col, app_col, status_col, hash_col = config.REQUIRED_COLUMNS[0], config.REQUIRED_COLUMNS[1], config.REQUIRED_COLUMNS[2], config.REQUIRED_COLUMNS[3]
        
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

def filter_and_compare(parquet_data):
    """Filter PowerBI queries and compare with results"""
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
            start_index = i * config.CHUNK_SIZE
            end_index = start_index + config.CHUNK_SIZE
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