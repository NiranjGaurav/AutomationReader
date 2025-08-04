#!/usr/bin/env python3
"""
Data loading module for the query analysis pipeline.
Handles loading parquet files into memory and preparing data structures.
"""

import pandas as pd
import math
import os
import glob
try:
    from . import config
except ImportError:
    import config

def load_parquet_data():
    """Load parquet files into memory and prepare data structures"""
    print("\n" + "=" * 80)
    print("STEP 1: LOADING PARQUET FILES")
    print("=" * 80)
    
    # Find all parquet files
    search_pattern = os.path.join(config.PARQUET_FOLDER_PATH, "*.parquet")
    parquet_files = glob.glob(search_pattern, recursive=True)
    
    if not parquet_files:
        print(f"No parquet files found in {config.PARQUET_FOLDER_PATH}")
        return None
    
    print(f"Found {len(parquet_files)} parquet file(s)")
    
    # Create the main output directory
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    
    parquet_data = {}
    
    # Process each parquet file
    for file_path in parquet_files:
        print(f"\nProcessing: {os.path.basename(file_path)}")
        
        # Get base filename
        base_filename = os.path.splitext(os.path.basename(file_path))[0]
        
        # Create output folder for this file
        file_specific_output_dir = os.path.join(config.OUTPUT_DIR, base_filename)
        os.makedirs(file_specific_output_dir, exist_ok=True)
        
        # Read parquet file
        df = pd.read_parquet(file_path)
        print(f"   Total rows: {len(df):,}")
        print(f"   Columns: {list(df.columns)}")
        
        # Calculate chunks for processing
        num_chunks = math.ceil(len(df) / config.CHUNK_SIZE)
        print(f"   Will process in {num_chunks} chunks of {config.CHUNK_SIZE} rows each")
        
        # Store data for processing
        parquet_data[base_filename] = {
            'dataframe': df,
            'num_chunks': num_chunks,
            'output_dir': file_specific_output_dir
        }
        
        print(f"   ✓ Loaded into memory")
    
    return parquet_data