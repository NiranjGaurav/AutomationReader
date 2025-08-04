#!/usr/bin/env python3
import pandas as pd
import os
import sys
import json
from collections import defaultdict
from datetime import datetime

def parse_function_list(func_string):
    """Parse function list strings (works for both unsupported_functions and udf_list)"""
    if pd.isna(func_string) or func_string == '' or func_string == '[]':
        return []
    
    try:
        # Try to parse as JSON list
        if func_string.startswith('[') and func_string.endswith(']'):
            # Remove extra quotes and parse
            cleaned = func_string.replace("'", '"')
            functions = json.loads(cleaned)
            return functions if isinstance(functions, list) else [functions]
        else:
            # Handle other formats
            return [func_string.strip()]
    except:
        # Fallback: extract functions manually
        # Remove brackets and split by comma
        cleaned = func_string.strip('[]').replace("'", "").replace('"', '')
        if cleaned:
            return [f.strip() for f in cleaned.split(',') if f.strip()]
        return []

def process_single_file(file_num):
    """Process a single final CSV file and extract both unsupported functions and UDFs"""
    input_dir = "/Users/niranjgaurav/Desktop/Parquet_to_CSV/Inmobi_queries/queries-hashed.snappy"
    
    # File paths
    final_file = os.path.join(input_dir, f"queries-hashed.snappy_part_{file_num}_final.csv")
    unsupported_file = os.path.join(input_dir, f"queries-hashed.snappy_part_{file_num}_final_unsupported.csv")
    udf_file = os.path.join(input_dir, f"queries-hashed.snappy_part_{file_num}_final_udfs.csv")
    
    print(f"\n📋 Processing file {file_num}:")
    
    # Check if final file exists
    if not os.path.exists(final_file):
        return {
            'success': False,
            'error': f"Final file not found: {os.path.basename(final_file)}",
            'unsupported_functions': {},
            'udfs': {},
            'unsupported_records': 0,
            'udf_records': 0
        }
    
    try:
        # Read the final file
        print("   Reading final file...")
        df = pd.read_csv(final_file)
        print(f"   Total records: {len(df)}")
        
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
        
        print(f"   Records with unsupported functions: {len(unsupported_df)}")
        print(f"   Records with UDFs: {len(udf_df)}")
        
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
            print(f"   ✓ Saved unsupported records: {os.path.basename(unsupported_file)}")
            
            # Show top unsupported functions
            sorted_unsupported = sorted(unsupported_counts.items(), key=lambda x: x[1], reverse=True)
            print("   Top unsupported functions:")
            for i, (func, count) in enumerate(sorted_unsupported[:3]):
                print(f"     {i+1}. {func} (appears {count} times)")
        
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
            print(f"   ✓ Saved UDF records: {os.path.basename(udf_file)}")
            
            # Show UDFs
            sorted_udfs = sorted(udf_counts.items(), key=lambda x: x[1], reverse=True)
            print("   UDFs found:")
            for i, (udf, count) in enumerate(sorted_udfs):
                print(f"     {i+1}. {udf} (appears {count} times)")
        
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

def update_summary_files(all_results):
    """Update both unsupported functions and UDF summary files"""
    
    # Update unsupported functions summary
    unsupported_file = "unsupported_functions_summary.csv"
    existing_unsupported = defaultdict(lambda: {'total_count': 0, 'chunks': set()})
    
    # Read existing unsupported summary
    if os.path.exists(unsupported_file):
        try:
            existing_df = pd.read_csv(unsupported_file)
            print(f"   📁 Found existing unsupported summary with {len(existing_df)} functions")
            
            for _, row in existing_df.iterrows():
                func_name = row['function_name']
                existing_unsupported[func_name]['total_count'] = row['total_occurrences']
                if pd.notna(row['chunk_list']) and row['chunk_list']:
                    chunks = [int(x.strip()) for x in str(row['chunk_list']).split(',') if x.strip()]
                    existing_unsupported[func_name]['chunks'] = set(chunks)
        except Exception as e:
            print(f"   ⚠ Error reading existing unsupported summary: {e}")
    
    # Update UDF summary
    udf_file = "udf_summary.csv"
    existing_udfs = defaultdict(lambda: {'total_count': 0, 'chunks': set()})
    
    # Read existing UDF summary
    if os.path.exists(udf_file):
        try:
            existing_df = pd.read_csv(udf_file)
            print(f"   📁 Found existing UDF summary with {len(existing_df)} UDFs")
            
            for _, row in existing_df.iterrows():
                udf_name = row['udf_name']
                existing_udfs[udf_name]['total_count'] = row['total_occurrences']
                if pd.notna(row['chunk_list']) and row['chunk_list']:
                    chunks = [int(x.strip()) for x in str(row['chunk_list']).split(',') if x.strip()]
                    existing_udfs[udf_name]['chunks'] = set(chunks)
        except Exception as e:
            print(f"   ⚠ Error reading existing UDF summary: {e}")
    
    # Merge new data
    merged_unsupported = defaultdict(lambda: {'total_count': 0, 'chunks': set()})
    merged_udfs = defaultdict(lambda: {'total_count': 0, 'chunks': set()})
    
    # Add existing data
    for func_name, stats in existing_unsupported.items():
        merged_unsupported[func_name]['total_count'] += stats['total_count']
        merged_unsupported[func_name]['chunks'].update(stats['chunks'])
    
    for udf_name, stats in existing_udfs.items():
        merged_udfs[udf_name]['total_count'] += stats['total_count']
        merged_udfs[udf_name]['chunks'].update(stats['chunks'])
    
    # Add new data
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
    
    # Save unsupported functions summary
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
        unsupported_df.to_csv(unsupported_file, index=False)
        print(f"   ✓ Updated unsupported functions summary: {unsupported_file}")
    
    # Save UDF summary
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
        udf_df.to_csv(udf_file, index=False)
        print(f"   ✓ Updated UDF summary: {udf_file}")
    
    return merged_unsupported, merged_udfs

def main():
    if len(sys.argv) != 3:
        print("Usage: python comprehensive_query_analyzer.py <start_num> <end_num>")
        print("Example: python comprehensive_query_analyzer.py 140 200")
        return
    
    try:
        start_num = int(sys.argv[1])
        end_num = int(sys.argv[2])
        
        if start_num > end_num:
            print("Error: Starting number must be less than or equal to ending number")
            return
        
        print("🔍 COMPREHENSIVE QUERY ANALYSIS")
        print("=" * 80)
        print(f"Analyzing files {start_num} to {end_num}")
        print("Processing: Unsupported Functions + UDFs + Summaries")
        print("=" * 80)
        
        # Process all files
        all_results = {}
        success_count = 0
        failed_files = []
        total_unsupported_records = 0
        total_udf_records = 0
        files_with_unsupported = 0
        files_with_udfs = 0
        
        for num in range(start_num, end_num + 1):
            result = process_single_file(num)
            all_results[num] = result
            
            if result['success']:
                success_count += 1
                total_unsupported_records += result['unsupported_records']
                total_udf_records += result['udf_records']
                
                if result['unsupported_records'] > 0:
                    files_with_unsupported += 1
                if result['udf_records'] > 0:
                    files_with_udfs += 1
            else:
                failed_files.append((num, result['error']))
        
        # Update summary files
        print(f"\n📊 UPDATING SUMMARY FILES")
        print("=" * 80)
        merged_unsupported, merged_udfs = update_summary_files(all_results)
        
        # Final summary
        print(f"\n📈 COMPREHENSIVE ANALYSIS SUMMARY")
        print("=" * 80)
        print(f"Files processed: {success_count}/{end_num - start_num + 1}")
        print(f"Files with unsupported functions: {files_with_unsupported}")
        print(f"Files with UDFs: {files_with_udfs}")
        print(f"Total records with unsupported functions: {total_unsupported_records}")
        print(f"Total records with UDFs: {total_udf_records}")
        
        if merged_unsupported:
            print(f"\n🚫 TOP 10 UNSUPPORTED FUNCTIONS:")
            print("-" * 80)
            print(f"{'Function Name':<40} {'Count':<8} {'Chunks':<8}")
            print("-" * 80)
            
            sorted_unsupported = sorted(merged_unsupported.items(), key=lambda x: x[1]['total_count'], reverse=True)
            for i, (func_name, stats) in enumerate(sorted_unsupported[:10]):
                display_name = func_name[:37] + "..." if len(func_name) > 37 else func_name
                print(f"{display_name:<40} {stats['total_count']:<8} {len(stats['chunks']):<8}")
        
        if merged_udfs:
            print(f"\n🔧 USER DEFINED FUNCTIONS:")
            print("-" * 80)
            print(f"{'UDF Name':<40} {'Count':<8} {'Chunks':<8}")
            print("-" * 80)
            
            sorted_udfs = sorted(merged_udfs.items(), key=lambda x: x[1]['total_count'], reverse=True)
            for func_name, stats in sorted_udfs:
                display_name = func_name[:37] + "..." if len(func_name) > 37 else func_name
                print(f"{display_name:<40} {stats['total_count']:<8} {len(stats['chunks']):<8}")
        
        if failed_files:
            print(f"\n❌ FAILED FILES ({len(failed_files)}):")
            print("-" * 80)
            for file_num, reason in failed_files:
                print(f"   File {file_num}: {reason}")
        
        # Save comprehensive report
        report_file = "comprehensive_analysis_report.txt"
        with open(report_file, 'w') as f:
            f.write(f"Comprehensive Query Analysis Report\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Range: {start_num} to {end_num}\n")
            f.write(f"Files processed: {success_count}/{end_num - start_num + 1}\n")
            f.write(f"Files with unsupported functions: {files_with_unsupported}\n")
            f.write(f"Files with UDFs: {files_with_udfs}\n")
            f.write(f"Total unsupported records: {total_unsupported_records}\n")
            f.write(f"Total UDF records: {total_udf_records}\n\n")
            
            if merged_unsupported:
                f.write("UNSUPPORTED FUNCTIONS (sorted by frequency):\n")
                f.write("-" * 80 + "\n")
                sorted_unsupported = sorted(merged_unsupported.items(), key=lambda x: x[1]['total_count'], reverse=True)
                for func_name, stats in sorted_unsupported:
                    f.write(f"{func_name}: {stats['total_count']} occurrences in {len(stats['chunks'])} chunks\n")
                    f.write(f"   Chunks: {','.join(map(str, sorted(stats['chunks'])))}\n\n")
            
            if merged_udfs:
                f.write("USER DEFINED FUNCTIONS (sorted by frequency):\n")
                f.write("-" * 80 + "\n")
                sorted_udfs = sorted(merged_udfs.items(), key=lambda x: x[1]['total_count'], reverse=True)
                for udf_name, stats in sorted_udfs:
                    f.write(f"{udf_name}: {stats['total_count']} occurrences in {len(stats['chunks'])} chunks\n")
                    f.write(f"   Chunks: {','.join(map(str, sorted(stats['chunks'])))}\n\n")
            
            if failed_files:
                f.write("FAILED FILES:\n")
                f.write("-" * 80 + "\n")
                for file_num, reason in failed_files:
                    f.write(f"File {file_num}: {reason}\n")
        
        print(f"\n📄 Comprehensive report saved to: {report_file}")
        print("\n✅ Analysis complete!")
        
    except ValueError:
        print("Error: Please provide valid numbers")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()