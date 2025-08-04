#!/usr/bin/env python3
"""
Configuration module for the query analysis pipeline.
Contains all default settings and runtime configuration variables.
"""

# ========== DEFAULT CONFIGURATION ==========
DEFAULT_PARQUET_FOLDER_PATH = "/Users/niranjgaurav/Downloads/"
DEFAULT_OUTPUT_DIR = "Inmobi_queries"
DEFAULT_CHUNK_SIZE = 2000
DEFAULT_REQUIRED_COLUMNS = ["statement_type", "client_application", "execution_status", "query_Hash"]

# ========== RUNTIME CONFIGURATION (Modified by main.py) ==========
PARQUET_FOLDER_PATH = DEFAULT_PARQUET_FOLDER_PATH
OUTPUT_DIR = DEFAULT_OUTPUT_DIR
CHUNK_SIZE = DEFAULT_CHUNK_SIZE
REQUIRED_COLUMNS = DEFAULT_REQUIRED_COLUMNS

def set_config(parquet_path=None, columns=None, chunk_size=None):
    """Update runtime configuration"""
    global PARQUET_FOLDER_PATH, REQUIRED_COLUMNS, CHUNK_SIZE
    
    if parquet_path:
        PARQUET_FOLDER_PATH = parquet_path
    
    if columns:
        REQUIRED_COLUMNS = columns
    
    if chunk_size:
        CHUNK_SIZE = chunk_size

def get_config():
    """Get current configuration as dictionary"""
    return {
        'parquet_folder_path': PARQUET_FOLDER_PATH,
        'output_dir': OUTPUT_DIR,
        'chunk_size': CHUNK_SIZE,
        'required_columns': REQUIRED_COLUMNS
    }