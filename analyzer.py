#!/usr/bin/env python3
"""
Analysis module for the query analysis pipeline.
Handles comprehensive analysis of unsupported functions and UDFs.
"""

import pandas as pd
import os
import glob
from collections import defaultdict
from datetime import datetime
try:
    from .function_utils import analyze_single_file
except ImportError:
    from function_utils import analyze_single_file

def comprehensive_analysis(parquet_data):
    """Comprehensive analysis of unsupported functions and UDFs"""
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
        
        # Save summary files and generate reports
        _save_summaries_and_reports(base_dir, base_filename, merged_unsupported, merged_udfs, 
                                   success_count, files_with_unsupported, files_with_udfs,
                                   total_unsupported_records, total_udf_records)

def _save_summaries_and_reports(base_dir, base_filename, merged_unsupported, merged_udfs, 
                               success_count, files_with_unsupported, files_with_udfs,
                               total_unsupported_records, total_udf_records):
    """Save summary files and generate reports (private helper function)"""
    
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