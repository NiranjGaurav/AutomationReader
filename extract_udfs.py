#!/usr/bin/env python3
import pandas as pd
import os
import sys
import json
from collections import defaultdict
from datetime import datetime

def parse_udf_list(udf_string):
    """Parse the udf_list string and extract individual UDFs"""
    if pd.isna(udf_string) or udf_string == '' or udf_string == '[]':
        return []
    
    try:
        # Try to parse as JSON list
        if udf_string.startswith('[') and udf_string.endswith(']'):
            # Remove extra quotes and parse
            cleaned = udf_string.replace("'", '"')
            udfs = json.loads(cleaned)
            return udfs if isinstance(udfs, list) else [udfs]
        else:
            # Handle other formats
            return [udf_string.strip()]
    except:
        # Fallback: extract UDFs manually
        # Remove brackets and split by comma
        cleaned = udf_string.strip('[]').replace("'", "").replace('"', '')
        if cleaned:
            return [u.strip() for u in cleaned.split(',') if u.strip()]
        return []

def process_final_file(file_num):
    """Extract records with UDFs from _final files"""
    input_dir = "/Users/niranjgaurav/Desktop/Parquet_to_CSV/Inmobi_queries/queries-hashed.snappy"
    
    # File paths
    final_file = os.path.join(input_dir, f"queries-hashed.snappy_part_{file_num}_final.csv")
    udf_file = os.path.join(input_dir, f"queries-hashed.snappy_part_{file_num}_final_udfs.csv")
    
    print(f"\n📋 Processing final file {file_num}:")
    
    # Check if final file exists
    if not os.path.exists(final_file):
        return False, f"Final file not found: {os.path.basename(final_file)}", {}
    
    try:
        # Read the final file
        print("   Reading final file...")
        df = pd.read_csv(final_file)
        print(f"   Total records: {len(df)}")
        
        # Check if udf_list column exists
        if 'udf_list' not in df.columns:
            return False, "Column 'udf_list' not found", {}
        
        # Filter for records with UDFs
        # Check for non-empty udf_list (not null, not empty string, not just '[]')
        udf_df = df[
            df['udf_list'].notna() & 
            (df['udf_list'] != '') & 
            (df['udf_list'] != '[]') &
            (df['udf_list'].str.strip() != '[]')
        ]
        
        print(f"   Records with UDFs: {len(udf_df)}")
        
        # Count individual UDFs
        udf_counts = defaultdict(int)
        
        if len(udf_df) == 0:
            print("   ✓ No UDFs found - all queries use standard functions!")
            return True, "No UDFs found", {}
        
        # Process each record to count UDFs
        for _, row in udf_df.iterrows():
            udfs = parse_udf_list(row['udf_list'])
            for udf in udfs:
                if udf:  # Skip empty UDFs
                    udf_counts[udf] += 1
        
        # Save records with UDFs
        udf_df.to_csv(udf_file, index=False)
        print(f"   ✓ Saved UDF records: {os.path.basename(udf_file)}")
        
        # Show sample of UDFs
        unique_udfs = list(udf_counts.keys())
        print(f"   Unique UDFs found: {len(unique_udfs)}")
        
        # Show top 5 most common UDFs
        if len(unique_udfs) > 0:
            print("   Most common UDFs:")
            sorted_udfs = sorted(udf_counts.items(), key=lambda x: x[1], reverse=True)
            for i, (udf, count) in enumerate(sorted_udfs[:5]):
                print(f"     {i+1}. {udf} (appears {count} times)")
        
        return True, f"Found {len(udf_df)} records with UDFs", udf_counts
        
    except Exception as e:
        return False, f"Error: {str(e)}", {}

def update_udf_summary(all_udf_counts, output_file):
    """Update UDF summary CSV file"""
    
    existing_stats = defaultdict(lambda: {'total_count': 0, 'chunks': set()})
    
    # Read existing summary if it exists
    if os.path.exists(output_file):
        try:
            existing_df = pd.read_csv(output_file)
            print(f"   📁 Found existing UDF summary with {len(existing_df)} UDFs")
            
            for _, row in existing_df.iterrows():
                udf_name = row['udf_name']
                existing_stats[udf_name]['total_count'] = row['total_occurrences']
                # Parse chunk list
                if pd.notna(row['chunk_list']) and row['chunk_list']:
                    chunks = [int(x.strip()) for x in str(row['chunk_list']).split(',') if x.strip()]
                    existing_stats[udf_name]['chunks'] = set(chunks)
        except Exception as e:
            print(f"   ⚠ Error reading existing UDF summary: {e}")
            print("   Creating new UDF summary file...")
    else:
        print("   📝 Creating new UDF summary file...")
    
    # Merge existing stats with new stats
    merged_stats = defaultdict(lambda: {'total_count': 0, 'chunks': set()})
    
    # Add existing stats
    for udf_name, stats in existing_stats.items():
        merged_stats[udf_name]['total_count'] += stats['total_count']
        merged_stats[udf_name]['chunks'].update(stats['chunks'])
    
    # Add new stats
    for file_num, udf_counts in all_udf_counts.items():
        for udf_name, count in udf_counts.items():
            merged_stats[udf_name]['total_count'] += count
            merged_stats[udf_name]['chunks'].add(file_num)
    
    # Convert to list of dictionaries for DataFrame
    summary_data = []
    
    for udf_name, stats in merged_stats.items():
        summary_data.append({
            'udf_name': udf_name,
            'total_occurrences': stats['total_count'],
            'number_of_chunks': len(stats['chunks']),
            'chunk_list': ','.join(map(str, sorted(stats['chunks'])))
        })
    
    # Sort by total occurrences (descending)
    summary_data.sort(key=lambda x: x['total_occurrences'], reverse=True)
    
    # Create DataFrame and save
    df = pd.DataFrame(summary_data)
    df.to_csv(output_file, index=False)
    
    return df

def main():
    if len(sys.argv) != 3:
        print("Usage: python extract_udfs.py <start_num> <end_num>")
        print("Example: python extract_udfs.py 140 145")
        return
    
    try:
        start_num = int(sys.argv[1])
        end_num = int(sys.argv[2])
        
        if start_num > end_num:
            print("Error: Starting number must be less than or equal to ending number")
            return
        
        print(f"Extracting UDFs from files {start_num} to {end_num}")
        print("=" * 70)
        
        success_count = 0
        failed_files = []
        total_udf_records = 0
        files_with_udfs = 0
        all_udf_counts = {}  # {file_num: {udf_name: count}}
        
        for num in range(start_num, end_num + 1):
            success, message, udf_counts = process_final_file(num)
            if success:
                success_count += 1
                if udf_counts:  # If UDFs were found
                    all_udf_counts[num] = udf_counts
                    record_count = int(message.split()[1]) if "Found" in message else 0
                    total_udf_records += record_count
                    files_with_udfs += 1
            else:
                failed_files.append((num, message))
        
        # Update UDF summary
        if all_udf_counts:
            summary_file = "udf_summary.csv"
            summary_df = update_udf_summary(all_udf_counts, summary_file)
        
        # Summary
        print("\n" + "=" * 70)
        print(f"📊 UDF EXTRACTION SUMMARY")
        print("=" * 70)
        print(f"Files processed: {success_count}/{end_num - start_num + 1}")
        print(f"Files with UDFs: {files_with_udfs}")
        print(f"Total records with UDFs: {total_udf_records}")
        
        if all_udf_counts:
            total_unique_udfs = len(summary_df)
            print(f"Unique UDFs found: {total_unique_udfs}")
            print(f"UDF summary saved to: udf_summary.csv")
            
            print("\n🔝 TOP 10 MOST COMMON UDFs:")
            print("-" * 70)
            print(f"{'UDF Name':<40} {'Count':<8} {'Chunks':<8}")
            print("-" * 70)
            
            for i, row in summary_df.head(10).iterrows():
                udf_name = row['udf_name']
                if len(udf_name) > 37:
                    udf_name = udf_name[:34] + "..."
                print(f"{udf_name:<40} {row['total_occurrences']:<8} {row['number_of_chunks']:<8}")
        
        if failed_files:
            print(f"\n❌ Failed files ({len(failed_files)}):")
            for file_num, reason in failed_files:
                print(f"   File {file_num}: {reason}")
        
        # Save detailed report
        report_file = "udf_extraction_report.txt"
        with open(report_file, 'w') as f:
            f.write(f"UDF Extraction Report\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Range: {start_num} to {end_num}\n")
            f.write(f"Success: {success_count}/{end_num - start_num + 1}\n")
            f.write(f"Files with UDFs: {files_with_udfs}\n")
            f.write(f"Total UDF records: {total_udf_records}\n\n")
            
            if all_udf_counts and not summary_df.empty:
                f.write("All UDFs (sorted by frequency):\n")
                f.write("-" * 80 + "\n")
                for _, row in summary_df.iterrows():
                    f.write(f"{row['udf_name']}: {row['total_occurrences']} occurrences in {row['number_of_chunks']} chunks\n")
                    f.write(f"   Chunks: {row['chunk_list']}\n\n")
            
            if failed_files:
                f.write("Failed Files:\n")
                for file_num, reason in failed_files:
                    f.write(f"File {file_num}: {reason}\n")
        
        print(f"\n📄 Detailed report saved to: {report_file}")
        
    except ValueError:
        print("Error: Please provide valid numbers")

if __name__ == "__main__":
    main()