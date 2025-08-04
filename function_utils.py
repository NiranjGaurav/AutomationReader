#!/usr/bin/env python3
"""
Function analysis utilities for the pipeline.
Contains helper functions for parsing and analyzing unsupported functions and UDFs.
"""
import os

import pandas as pd
import json
from collections import defaultdict

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