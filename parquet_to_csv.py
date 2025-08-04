import pandas as pd
import math
import os
import glob

# Configuration
parquet_folder_path = "/Users/niranjgaurav/Downloads/"
output_dir = "Inmobi_queries"
chunk_size = 2000

# Find all parquet files
search_pattern = os.path.join(parquet_folder_path, "*.parquet")
parquet_files = glob.glob(search_pattern, recursive=True)

# Create the main output directory
os.makedirs(output_dir, exist_ok=True)

# Process each parquet file
for file_path in parquet_files:
    # Get base filename
    base_filename = os.path.splitext(os.path.basename(file_path))[0]
    
    # Create output folder for this file
    file_specific_output_dir = os.path.join(output_dir, base_filename)
    os.makedirs(file_specific_output_dir, exist_ok=True)
    
    # Read parquet file
    df = pd.read_parquet(file_path)
    
    # Keep only query_hash and hashed_query columns
    # df = df[['query_Hash', 'hashed_query']]
    
    # Save chunks
    num_chunks = math.ceil(len(df) / chunk_size)
    for i in range(num_chunks):
        start_index = i * chunk_size
        end_index = start_index + chunk_size
        chunk_df = df.iloc[start_index:end_index]
        
        output_path = os.path.join(file_specific_output_dir, f"{base_filename}_part_{i+1}.csv")
        chunk_df.to_csv(output_path, index=False)
