#!/usr/bin/env python3

import os
import re
import sys
from pathlib import Path

def extract_rejected_files(analysis_file):
    """Extract filenames of files rejected by AI from the analysis file."""
    rejected_files = []
    in_rejected_section = False
    
    with open(analysis_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            
            # Skip separator lines
            if line.startswith('-') and all(c in '-=' for c in line):
                continue
                
            # Check for section header
            if line == "Files Rejected by AI:":
                in_rejected_section = True
                continue
            elif line.startswith("Mapping Outcomes Summary:") or line.startswith("Files with Too Few Mappings"):
                in_rejected_section = False
                continue
            
            # If we're in the rejected section and the line starts with a filename
            if in_rejected_section and line and not line.startswith("Reason:"):
                # Only add if it looks like a filename (contains a dot and no special characters)
                if '.' in line and not any(c in '=-' for c in line):
                    filename = line.strip()
                    if filename:
                        rejected_files.append(filename)
    
    return rejected_files

def delete_files(filenames, base_dir):
    """Delete the specified files if they exist in the base directory."""
    deleted = []
    not_found = []
    
    # Ensure base_dir exists and is a directory
    if not os.path.isdir(base_dir):
        print(f"Error: Base directory '{base_dir}' does not exist or is not a directory")
        return deleted, not_found
    
    for filename in filenames:
        try:
            # Construct full path by joining base_dir and filename
            full_path = os.path.join(base_dir, filename)
            if os.path.exists(full_path):
                os.remove(full_path)
                deleted.append(filename)
                print(f"Deleted: {filename}")
            else:
                not_found.append(filename)
                print(f"File not found: {filename}")
        except Exception as e:
            print(f"Error deleting {filename}: {str(e)}")
    
    return deleted, not_found

def main():
    if len(sys.argv) != 3:
        print("Usage: python delete_rejected_files.py <analysis_file> <base_directory>")
        print("Example: python delete_rejected_files.py a.txt ./data")
        sys.exit(1)
    
    analysis_file = sys.argv[1]
    base_dir = sys.argv[2]
    
    if not os.path.exists(analysis_file):
        print(f"Error: Analysis file '{analysis_file}' not found")
        sys.exit(1)
    
    if not os.path.isdir(base_dir):
        print(f"Error: Base directory '{base_dir}' does not exist or is not a directory")
        sys.exit(1)
    
    print(f"Reading analysis file: {analysis_file}")
    print(f"Base directory for files: {base_dir}")
    
    rejected_files = extract_rejected_files(analysis_file)
    
    if not rejected_files:
        print("No rejected files found in the analysis file.")
        sys.exit(0)
    
    print(f"\nFound {len(rejected_files)} rejected files:")
    for f in rejected_files:
        print(f"- {f}")
    
    print("\nProceeding to delete files...")
    deleted, not_found = delete_files(rejected_files, base_dir)
    
    print("\nSummary:")
    print(f"Successfully deleted: {len(deleted)} files")
    print(f"Files not found: {len(not_found)} files")
    
    if not_found:
        print("\nFiles not found:")
        for f in not_found:
            print(f"- {f}")

if __name__ == "__main__":
    main() 