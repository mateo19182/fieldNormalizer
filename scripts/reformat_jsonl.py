#!/usr/bin/env python3
"""
Script to reformat large JSONL files by removing the outer key.
Processes files line by line to handle large files efficiently.
"""

import json
import os
import argparse
import time
from pathlib import Path
from tqdm import tqdm


def count_lines(filename):
    """Efficiently count lines in a file without loading it all into memory"""
    with open(filename, 'rb') as f:
        lines = 0
        buf_size = 1024 * 1024
        read_f = f.raw.read

        buf = read_f(buf_size)
        while buf:
            lines += buf.count(b'\n')
            buf = read_f(buf_size)

    return lines

def process_jsonl_file(input_file, output_file):
    """
    Process a JSONL file line by line, removing the outer key.
    
    Args:
        input_file (str): Path to input JSONL file
        output_file (str): Path to output JSONL file
    """
    start_time = time.time()
    file_size = os.path.getsize(input_file)
    
    print(f"Processing file: {input_file} ({file_size / (1024 * 1024 * 1024):.2f} GB)")
    
    # Count total lines for tqdm (with a small sample if file is very large)
    print("Estimating total lines...")
    if file_size > 1 * 1024 * 1024 * 1024:  # If larger than 1GB, estimate lines
        with open(input_file, 'r', encoding='utf-8') as f:
            sample_lines = 1000
            sample_size = 0
            for i in range(sample_lines):
                line = f.readline()
                if not line:
                    break
                sample_size += len(line.encode('utf-8'))
            avg_line_size = sample_size / sample_lines if sample_lines > 0 else 0
            total_lines = int(file_size / avg_line_size) if avg_line_size > 0 else 0
            print(f"Estimated {total_lines:,} lines based on sampling")
    else:
        total_lines = count_lines(input_file)
        print(f"Counted {total_lines:,} lines in file")
    
    with open(input_file, 'r', encoding='utf-8') as f_in, \
         open(output_file, 'w', encoding='utf-8') as f_out:
        
        # Create tqdm progress bar
        pbar = tqdm(total=total_lines, unit='lines', desc=f"Processing {Path(input_file).name}")
        errors = 0
        
        for line in f_in:
            try:
                # Parse the line as JSON
                data = json.loads(line.strip())
                
                # Extract the inner object (first value in the dictionary)
                if len(data) == 1:
                    inner_object = next(iter(data.values()))
                    # Write only the inner object to the output
                    f_out.write(json.dumps(inner_object) + '\n')
                else:
                    # If structure is different, log and count errors
                    errors += 1
                    if errors <= 10:  # Only show the first 10 errors
                        print(f"Warning: Unexpected structure. Skipping.")
            except json.JSONDecodeError:
                errors += 1
                if errors <= 10:  # Only show the first 10 errors
                    print(f"Error: Failed to parse JSON. Skipping.")
            
            # Update progress bar
            pbar.update(1)
        
        # Close progress bar
        pbar.close()
    
    total_time = time.time() - start_time
    print(f"Completed processing in {total_time:.1f} seconds")
    print(f"Output written to {output_file}")
    if errors > 0:
        print(f"Encountered {errors} errors during processing")


def main():
    parser = argparse.ArgumentParser(description='Reformat JSONL files by removing the outer key')
    parser.add_argument('input_files', nargs='+', help='Input JSONL file(s)')
    parser.add_argument('-o', '--output-dir', help='Output directory (default: same as input with _reformatted suffix)')
    args = parser.parse_args()
    
    total_start_time = time.time()
    
    for input_file in args.input_files:
        input_path = Path(input_file)
        
        if not input_path.exists():
            print(f"Error: File not found - {input_file}")
            continue
            
        if args.output_dir:
            output_dir = Path(args.output_dir)
            output_dir.mkdir(exist_ok=True, parents=True)
            output_file = output_dir / f"{input_path.stem}_reformatted{input_path.suffix}"
        else:
            output_file = input_path.with_stem(f"{input_path.stem}_reformatted")
        
        file_start_time = time.time()
        process_jsonl_file(str(input_path), str(output_file))
        file_time = time.time() - file_start_time
        print(f"Processed {input_path.name} in {file_time:.1f} seconds")
    
    total_time = time.time() - total_start_time
    print(f"Total processing time: {total_time:.1f} seconds")


if __name__ == "__main__":
    main()
