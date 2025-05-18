#!/usr/bin/env python3
"""
Field Normalizer CLI
A tool to extract and normalize headers from various data files (CSV, JSON, SQL, etc.)
"""
import argparse
import os
import sys
from typing import Dict, List, Set, Tuple, Any
from tqdm import tqdm

from src.extractors import extract_headers_from_file
from src.field_normalizer import analyze_field_variations, group_fields
from src.field_mapper import create_field_mappings, format_mappings_report
from src.data_extractor import extract_all_data, write_jsonl


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Field Normalizer: Extract and process data from various file formats."
    )
    
    # Create subparsers for the two main operations
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Common arguments for both commands
    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument(
        "paths",
        nargs="+",
        help="One or more directories or files to process",
    )
    common_parser.add_argument(
        "--file-types",
        nargs="+",
        default=["csv", "json", "sql"],
        help="File extensions to process (default: csv, json, sql)",
    )
    common_parser.add_argument(
        "--max-files",
        "-n",
        type=int,
        help="Maximum number of files to process (only applies to directories)",
    )
    
    # 1. Analyze command - for analyzing headers and creating mappings
    analyze_parser = subparsers.add_parser(
        "analyze", 
        parents=[common_parser],
        help="Analyze files and create field mappings"
    )
    analyze_parser.add_argument(
        "--output",
        "-o",
        help="Output file for analysis report (default: stdout)",
    )
    analyze_parser.add_argument(
        "--no-normalize",
        action="store_true",
        help="Disable field normalization (enabled by default)",
    )
    analyze_parser.add_argument(
        "--no-variations",
        action="store_true",
        help="Disable showing field variations (enabled by default)",
    )
    analyze_parser.add_argument(
        "--mappings-output",
        default="mappings.json",
        help="Output file for field mappings (JSON format)",
    )
    
    # 2. Extract command - for extracting data using mappings
    extract_parser = subparsers.add_parser(
        "extract", 
        help="Extract data using field mappings"
    )
    extract_parser.add_argument(
        "--mappings",
        default="mappings.json",
        help="Field mappings file (JSON format, default: mappings.json)",
    )
    extract_parser.add_argument(
        "paths",
        nargs="*",
        help="Optional paths to process (if not specified, uses paths from mappings file)",
    )
    extract_parser.add_argument(
        "--output",
        "-o",
        default="extracted_data.jsonl",
        help="Output file for extracted data (JSONL format)",
    )
    extract_parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size for writing records to output file",
    )
    
    return parser.parse_args()


def find_data_files(paths: List[str], file_types: List[str], max_files: int = None) -> List[str]:
    """
    Find all data files with the specified extensions in the given paths.
    Paths can be directories or individual files.
    
    Args:
        paths: List of directory or file paths to process
        file_types: List of file extensions to include
        max_files: Maximum number of files to process (only applies to directories)
        
    Returns:
        List of absolute paths to matching data files
    """
    data_files = []
    
    for path in paths:
        # Check if the path is a directory
        if os.path.isdir(path):
            dir_files = []
            for root, _, files in os.walk(path):
                for file in files:
                    if any(file.endswith(f".{ext}") for ext in file_types):
                        dir_files.append(os.path.join(root, file))
            
            # Limit the number of files from this directory if max_files is specified
            if max_files is not None and max_files > 0:
                dir_files = dir_files[:max_files]
                
            data_files.extend(dir_files)
        
        # Check if the path is a file
        elif os.path.isfile(path):
            _, ext = os.path.splitext(path)
            ext = ext.lower().lstrip('.')
            
            if ext in file_types:
                data_files.append(path)
            else:
                print(f"Warning: {path} is not a supported file type ({', '.join(file_types)}), skipping.", file=sys.stderr)
        
        # Path is neither a file nor a directory
        else:
            print(f"Warning: {path} is not a valid file or directory, skipping.", file=sys.stderr)
        
    return data_files


def process_files(file_paths: List[str]) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]], Dict[str, str]]:
    """
    Process all files and extract headers.
    
    Args:
        file_paths: List of file paths to process
        
    Returns:
        Tuple of (header_stats, file_metadata, failed_files)
            header_stats: Dict mapping headers to their statistics
            file_metadata: List of dicts with metadata about processed files
            failed_files: Dict mapping failed files to error messages
    """
    header_stats = {}
    file_metadata = []
    failed_files = {}
    
    # Add progress bar
    for file_path in tqdm(file_paths, desc="Processing files", unit="file"):
        try:
            headers, headers_inferred = extract_headers_from_file(file_path)
            
            # Update header statistics
            for header in headers:
                if header not in header_stats:
                    header_stats[header] = {'count': 0, 'files': []}
                header_stats[header]['count'] += 1
                header_stats[header]['files'].append(os.path.basename(file_path))
            
            file_metadata.append({
                'path': file_path,
                'headers': headers,
                'headers_inferred': headers_inferred
            })
            
        except Exception as e:
            failed_files[file_path] = str(e)
    
    return header_stats, file_metadata, failed_files


def main():
    """Main entry point for the CLI."""
    args = parse_args()
    
    # Process based on command
    if args.command == "analyze":
        # Find all data files in the specified paths
        data_files = find_data_files(args.paths, args.file_types, args.max_files)
        print(f"Found {len(data_files)} data files to process")
    
        # Process all files and extract headers
        # Process all files and extract headers
        header_stats, file_metadata, failed_files = process_files(data_files)
        
        # Create field mappings
        field_mapper = create_field_mappings(file_metadata)
        
        # Save mappings to file
        field_mapper.save_mappings(args.mappings_output)
        print(f"\nWrote field mappings to {args.mappings_output}")
        
        # Apply field normalization (enabled by default)
        all_headers = list(header_stats.keys())
        if not args.no_normalize:
            if not args.no_variations:
                field_variations = analyze_field_variations(all_headers, header_stats)
                normalized_output = format_field_variations(field_variations, header_stats)
            else:
                field_groups = group_fields(all_headers)
                normalized_output = format_field_groups(field_groups, header_stats)
        
        # Output analysis results
        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                # Add header with processing summary
                summary = [
                    f"Field Normalization Report",
                    "=" * 80,
                    f"Total files processed: {len(file_metadata)}",
                    f"Files with inferred headers: {sum(1 for m in file_metadata if m['headers_inferred'])}",
                    f"Files with standard headers: {sum(1 for m in file_metadata if not m['headers_inferred'])}",
                    ""
                ]
                
                if not args.no_normalize:
                    if not args.no_variations:
                        content = format_field_variations(field_variations, header_stats)
                    else:
                        content = format_field_groups(field_groups, header_stats)
                else:
                    content = format_header_stats(header_stats)
                
                # Add files with inferred headers section
                inferred_files = [m['path'] for m in file_metadata if m['headers_inferred']]
                if inferred_files:
                    summary.append("\nFILES WITH INFERRED HEADERS:")
                    summary.append("=" * 80)
                    for file_path in sorted(inferred_files):
                        summary.append(f"- {os.path.basename(file_path)}")
                    summary.append("")
                
                summary.append(content)
                f.write("\n".join(summary))
            print(f"\nWrote analysis report to {args.output}")
        else:
            if not args.no_normalize:
                print("\nNormalized Field Groups:")
                print(normalized_output)
            else:
                print("\nExtracted Headers:")
                print("\nHeader | Occurrences | Files")
                print("-" * 80)
                for header in sorted(header_stats.keys()):
                    stats = header_stats[header]
                    # Truncate the files list if it's too long for display
                    files_str = ", ".join(stats['files'])
                    if len(files_str) > 50:
                        files_str = files_str[:47] + "..."
                    print(f"{header} | {stats['count']} | {files_str}")
    elif args.command == "extract":
        # Load field mappings
        if os.path.isfile(args.mappings):
            field_mapper = create_field_mappings([])  # Create empty mapper
            field_mapper.load_mappings(args.mappings)
            print(f"Loaded field mappings from {args.mappings}")
        else:
            print(f"Error: Mappings file {args.mappings} not found", file=sys.stderr)
            sys.exit(1)
        
        # Get file paths from the mappings file if not specified
        if not args.paths:
            # Extract file paths from the mappings
            mapping_paths = field_mapper.get_all_file_paths()
            if not mapping_paths:
                print("Error: No file paths found in mappings file", file=sys.stderr)
                sys.exit(1)
            print(f"Using {len(mapping_paths)} file paths from mappings file")
            data_files = mapping_paths
        else:
            # Find data files in the specified paths
            # Use default file types if not specified
            file_types = getattr(args, 'file_types', ["csv", "json", "sql"])
            max_files = getattr(args, 'max_files', None)
            data_files = find_data_files(args.paths, file_types, max_files)
            print(f"Found {len(data_files)} data files to process")
        
        # Extract data using the field mappings
        print(f"Extracting data from {len(data_files)} files...")
        records = extract_all_data(data_files, field_mapper)
        
        # Write to JSONL file
        print(f"Writing records to {args.output}...")
        total_records = write_jsonl(records, args.output, args.batch_size)
        print(f"Extracted {total_records} records to {args.output}")
    
    else:
        # No command specified, show help
        print("Error: No command specified. Use 'analyze' or 'extract'.")
        sys.exit(1)
    # This section is now handled within the command-specific blocks
    
    # Print processing statistics
    if args.command == "analyze":
        print(f"\nProcessing Statistics:")
        print(f"Total files: {len(data_files)}")
        print(f"Successfully processed: {len(file_metadata)}")
        print(f"Files with inferred headers: {sum(1 for m in file_metadata if m['headers_inferred'])}")
        print(f"Failed: {len(failed_files)}")
    elif args.command == "extract" and 'total_records' in locals():
        print(f"\nProcessing Statistics:")
        print(f"Total files: {len(data_files)}")
        print(f"Records extracted: {total_records}")
    
    # Print details about failed files if any (only in analyze mode)
    if args.command == "analyze" and failed_files:
        print("\nFiles with errors:")
        for file_path, error_msg in failed_files.items():
            print(f"  - {os.path.basename(file_path)}: {error_msg}")
    
    if args.command == "analyze":
        print(f"\nTotal unique headers found: {len(header_stats)}")
        
        # Print the most common headers (only if not showing normalized output)
        if args.no_normalize:
            print("\nMost common headers:")
            sorted_headers = sorted(header_stats.items(), key=lambda x: x[1]['count'], reverse=True)
            for header, stats in sorted_headers[:10]:  # Show top 10
                print(f"{header}: {stats['count']} occurrences")


def format_field_groups(field_groups: Dict[str, Set[str]], header_stats: Dict[str, Dict[str, Any]] = None) -> str:
    """Format field groups for display with file sources.
    
    Args:
        field_groups: Dictionary mapping field types to sets of field names
        header_stats: Dictionary mapping headers to their statistics (count and files)
        
    Returns:
        Formatted string representation of field groups with sources and unmatched headers
    """
    output = []
    matched_headers = set()
    
    # Process all field groups first
    for field_type, fields in field_groups.items():
        if fields:  # Only show non-empty groups
            output.append(f"\n{field_type.upper()} FIELDS:")
            output.append("=" * 80)
            for field in sorted(fields):
                matched_headers.add(field)
                output.append(f"  - {field}")
    
    # Add file headers section if we have file metadata
    if header_stats and any('files' in data for data in header_stats.values()):
        # Group headers by file
        file_headers = {}
        for header, data in header_stats.items():
            for filename in data.get('files', []):
                if filename not in file_headers:
                    file_headers[filename] = set()
                file_headers[filename].add(header)
        
        if file_headers:
            output.append("\n\nFILE HEADERS:")
            output.append("=" * 80)
            for filename, headers in sorted(file_headers.items()):
                output.append(f"\n{filename}:")
                output.append("  " + ", ".join(sorted(headers)))
    
    # Add unmatched headers section
    if header_stats:
        all_headers = set(header_stats.keys())
        unmatched_headers = all_headers - matched_headers
        
        if unmatched_headers:
            output.append("\n\nUNMATCHED HEADERS:")
            output.append("=" * 80)
            for header in sorted(unmatched_headers):
                files = header_stats[header].get('files', [])
                output.append(f"\n{header}")
                if files:
                    output.append(f"  Found in: {', '.join(files[:3])}" + ("..." if len(files) > 3 else ""))
    
    return "\n".join(output)


def format_field_variations(field_variations: Dict[str, Dict[str, Dict[str, List[str]]]], header_stats: Dict[str, Dict[str, Any]] = None) -> str:
    """Format field variations for display with file sources.
    
    Args:
        field_variations: Dictionary of field variations by type with sources
        header_stats: Dictionary mapping headers to their statistics (count and files)
        
    Returns:
        Formatted string representation of field variations with sources and unmatched headers
    """
    output = []
    matched_headers = set()
    
    # First, collect all fields by type
    fields_by_type = {}
    for field_type, patterns in field_variations.items():
        fields_by_type[field_type] = set()
        for fields in patterns.values():
            fields_by_type[field_type].update(fields.keys())
    
    # Show fields grouped by type
    for field_type, fields in fields_by_type.items():
        if fields:  # Only show non-empty groups
            output.append(f"\n{field_type.upper()} FIELDS:")
            output.append("=" * 80)
            for field in sorted(fields):
                matched_headers.add(field)
                output.append(f"  - {field}")
    
    # Add file headers section if we have file metadata
    if header_stats and any('files' in data for data in header_stats.values()):
        # Group headers by file
        file_headers = {}
        for header, data in header_stats.items():
            for filename in data.get('files', []):
                if filename not in file_headers:
                    file_headers[filename] = set()
                file_headers[filename].add(header)
        
        if file_headers:
            output.append("\n\nFILE HEADERS:")
            output.append("=" * 80)
            for filename, headers in sorted(file_headers.items()):
                output.append(f"\n{filename}:")
                output.append("  " + ", ".join(sorted(headers)))
    
    # Add unmatched headers section
    if header_stats:
        all_headers = set(header_stats.keys())
        unmatched_headers = all_headers - matched_headers
        
        if unmatched_headers:
            output.append("\n\nUNMATCHED HEADERS:")
            output.append("=" * 80)
            for header in sorted(unmatched_headers):
                files = header_stats[header].get('files', [])
                output.append(f"\n{header}")
                if files:
                    output.append(f"  Found in: {', '.join(files[:3])}" + ("..." if len(files) > 3 else ""))
    
    return "\n".join(output)


if __name__ == "__main__":
    main()
