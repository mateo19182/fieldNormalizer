#!/usr/bin/env python3
"""
Field Normalizer CLI
A tool to extract and normalize headers from various data files (CSV, JSON, SQL, etc.)
"""
import argparse
import os
import sys
import json
import asyncio
import concurrent.futures
from typing import Dict, List, Set, Tuple, Any
from tqdm import tqdm

from src.extractors import extract_headers_from_file
from src.field_normalizer import analyze_field_variations, group_fields
from src.field_mapper import create_field_mappings, format_mappings_report, DEFAULT_TARGET_FIELDS, FieldMapper
from src.ai_field_mapper import create_ai_field_mappings, format_ai_mappings_report, AIFieldMapper
from src.data_extractor import extract_all_data, write_jsonl


def load_config(config_file: str) -> Dict[str, Any]:
    """
    Load configuration from a JSON file.
    
    Args:
        config_file: Path to the configuration file
        
    Returns:
        Dictionary containing configuration settings
    """
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        return config
    except Exception as e:
        print(f"Error loading configuration file: {str(e)}", file=sys.stderr)
        sys.exit(1)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Field Normalizer - Extract and normalize fields from various data sources"
    )
    
    # Add common arguments to the main parser
    parser.add_argument(
        "--config",
        help="Path to configuration file (JSON format)",
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # 1. Analyze command - for analyzing files and creating mappings
    analyze_parser = subparsers.add_parser(
        "analyze",
        help="Analyze files and create field mappings"
    )
    analyze_parser.add_argument(
        "--max-files",
        "-n",
        type=int,
        help="Maximum number of files to process per directory",
    )
    analyze_parser.add_argument(
        "--file-types",
        nargs="+",
        default=["csv", "json", "jsonl"],
        help="File types to process (default: csv json, optional: txt, sql)",
    )
    analyze_parser.add_argument(
        "paths",
        nargs="+",
        help="Paths to analyze (files or directories)",
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
    analyze_parser.add_argument(
        "--use-ai",
        action="store_true",
        help="Use AI to create field mappings (requires OPENROUTER_API_KEY in .env file)",
    )
    analyze_parser.add_argument(
        "--target-fields",
        nargs="+",
        help=f"Custom target fields to map to (default: {', '.join(DEFAULT_TARGET_FIELDS)})",
    )
    analyze_parser.add_argument(
        "--data-description",
        help="Description of the data you are looking for (helps AI determine file relevance)",
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
    extract_parser.add_argument(
        "--group-by-email",
        action="store_true",
        help="Group records by email address (disabled by default)",
    )
    extract_parser.add_argument(
        "--use-ai",
        action="store_true",
        help="Use AI-based field mappings (requires OPENROUTER_API_KEY in .env file)",
    )
    
    # 3. Process command - combines analyze and extract in one step
    process_parser = subparsers.add_parser(
        "process",
        help="Analyze files and extract data in one step"
    )
    process_parser.add_argument(
        "--max-files",
        "-n",
        type=int,
        help="Maximum number of files to process per directory",
    )
    process_parser.add_argument(
        "--file-types",
        nargs="+",
        default=["csv", "json"],
        help="File types to process (default: csv json, optional: txt, sql)",
    )
    process_parser.add_argument(
        "paths",
        nargs="+",
        help="Paths to process (files or directories)",
    )
    process_parser.add_argument(
        "--analysis-output",
        default=None,
        help="Output file for analysis report (default: no file output)",
    )
    process_parser.add_argument(
        "--mappings-output",
        default="mappings.json",
        help="Output file for field mappings (default: mappings.json)",
    )
    process_parser.add_argument(
        "--extract-output",
        "-o",
        default="extracted_data.jsonl",
        help="Output file for extracted data (default: extracted_data.jsonl)",
    )
    process_parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size for writing records to output file",
    )
    process_parser.add_argument(
        "--no-normalize",
        action="store_true",
        help="Disable field normalization (enabled by default)",
    )
    process_parser.add_argument(
        "--no-variations",
        action="store_true",
        help="Don't show field variations in the output (shown by default)",
    )
    process_parser.add_argument(
        "--group-by-email",
        action="store_true",
        help="Group records by email address (disabled by default)",
    )
    process_parser.add_argument(
        "--use-ai",
        action="store_true",
        help="Use AI to create field mappings (requires OPENROUTER_API_KEY in .env file)",
    )
    process_parser.add_argument(
        "--target-fields",
        nargs="+",
        help=f"Custom target fields to map to (default: {', '.join(DEFAULT_TARGET_FIELDS)})",
    )
    process_parser.add_argument(
        "--data-description",
        help="Description of the data you are looking for (helps AI determine file relevance)",
    )
    
    return parser.parse_args()


# Helper function for directory processing - must be at module level for pickability
def process_directory(directory, file_types, max_files):
    dir_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if any(file.endswith(f".{ext}") for ext in file_types):
                dir_files.append(os.path.join(root, file))
    
    # Limit the number of files from this directory if max_files is specified
    if max_files is not None and max_files > 0:
        dir_files = dir_files[:max_files]
        
    return dir_files


def find_data_files(paths: List[str], file_types: List[str], max_files: int = None) -> List[str]:
    """
    Find all data files with the specified extensions in the given paths using parallel processing.
    Paths can be directories or individual files.
    
    Args:
        paths: List of directory or file paths to process
        file_types: List of file extensions to include
        max_files: Maximum number of files to process (only applies to directories)
        
    Returns:
        List of absolute paths to matching data files
    """
    data_files = []
    directories = []
    
    # First separate directories from individual files
    for path in paths:
        # Check if the path is a directory
        if os.path.isdir(path):
            directories.append(path)
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
    
    # Process directories in parallel if there are more than one
    if directories:
        # Use parallel processing for multiple directories
        if len(directories) > 1:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = [executor.submit(process_directory, directory, file_types, max_files) 
                          for directory in directories]
                for future in tqdm(concurrent.futures.as_completed(futures), 
                                  total=len(futures), 
                                  desc="Scanning directories", 
                                  unit="dir"):
                    dir_files = future.result()
                    data_files.extend(dir_files)
        else:
            # Just process a single directory directly
            dir_files = process_directory(directories[0], file_types, max_files)
            data_files.extend(dir_files)
            
    return data_files


# Helper function for parallel processing - must be at module level for pickability
def extract_headers_worker(file_path):
    try:
        # Extract headers from file
        headers, headers_inferred = extract_headers_from_file(file_path)
        return (file_path, headers, headers_inferred, None)
    except Exception as e:
        return (file_path, [], False, str(e))


def process_files(file_paths: List[str]) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]], List[str]]:
    """
    Process all files and extract headers using parallel processing.
    
    Args:
        file_paths: List of file paths to process
        
    Returns:
        Tuple of (header_stats, file_metadata, all_headers)
    """
    header_stats = {}
    file_metadata = []
    all_headers = []
    failed_files = {}
    
    # Process files in parallel
    with concurrent.futures.ProcessPoolExecutor() as executor:
        # Submit all tasks
        futures = [executor.submit(extract_headers_worker, file_path) for file_path in file_paths]
        
        # Process results as they complete
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), 
                          desc="Processing files", unit="file"):
            file_path, headers, headers_inferred, error = future.result()
            
            if error:
                failed_files[file_path] = error
                print(f"Error processing {file_path}: {error}", file=sys.stderr)
                continue
            
            # Update header statistics
            for header in headers:
                if header not in header_stats:
                    header_stats[header] = {
                        "count": 0,
                        "files": []
                    }
                header_stats[header]["count"] += 1
                header_stats[header]["files"].append(os.path.basename(file_path))
            
            # Add to file metadata
            file_metadata.append({
                "path": file_path,
                "headers": headers,
                "headers_inferred": headers_inferred
            })
            
            # Add to all headers list
            all_headers.extend(headers)
    
    # Remove duplicates from all_headers
    all_headers = list(set(all_headers))
    
    return header_stats, file_metadata, all_headers


async def async_main():
    """Async main entry point for the CLI."""
    args = parse_args()
    
    if not args.command:
        print("Error: No command specified. Use 'analyze', 'extract', or 'process'.", file=sys.stderr)
        sys.exit(1)
    
    # Load configuration if specified
    config = {}
    if args.config:
        config = load_config(args.config)
    
    # Handle the analyze command
    if args.command == "analyze":
        # Find data files
        data_files = find_data_files(args.paths, args.file_types, args.max_files)
        if not data_files:
            print("Error: No matching data files found.", file=sys.stderr)
            sys.exit(1)
        
        print(f"Found {len(data_files)} data files to analyze.")
        
        # Process files to extract headers
        header_stats, file_metadata, all_headers = process_files(data_files)
        
        # Group fields by type
        field_groups = group_fields(all_headers)
        
        # Get target fields and data description from config or command line
        target_fields = args.target_fields or config.get('target_fields') or DEFAULT_TARGET_FIELDS
        data_description = args.data_description or config.get('data_description', "")
        
        # Create field mappings
        if args.use_ai:
            # Use AI-based field mapping with custom target fields
            print(f"Using AI to create field mappings with target fields: {', '.join(target_fields)}")
            if data_description:
                print(f"Using data description: \"{data_description}\"")
            mapper = await create_ai_field_mappings(file_metadata, target_fields, data_description)
            mappings_report = format_ai_mappings_report(mapper)
        else:
            # Use traditional regex-based field mapping
            print(f"Creating field mappings with target fields: {', '.join(target_fields)}")
            # If using config file and not AI, use custom field patterns
            if config and 'field_patterns' in config:
                mapper = create_field_mappings(file_metadata, target_fields, custom_patterns=config['field_patterns'])
            else:
                mapper = create_field_mappings(file_metadata, target_fields)
            mappings_report = format_mappings_report(mapper)
        
        # Save mappings to file
        mapper.save_mappings(args.mappings_output)
        print(f"Field mappings saved to {args.mappings_output}")
        
        # Prepare extra info for report
        import datetime
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        file_list = [os.path.basename(f) for f in data_files]
        file_list_str = "\n  - ".join(file_list[:10])
        if len(file_list) > 10:
            file_list_str += f"\n  ... and {len(file_list) - 10} more files"
        # Build headers_per_file_str using finalized mappings
        file_mappings = mapper.get_all_mappings()
        headers_per_file_lines = []
        for file_path in data_files:
            mapping = file_mappings.get(file_path)
            if mapping is None:
                for k in file_mappings:
                    if os.path.basename(k) == os.path.basename(file_path):
                        mapping = file_mappings[k]
                        break
            mapped_count = len(mapping) if mapping else 0
            # Find total headers for this file from file_metadata
            total_headers = 0
            for meta in file_metadata:
                if meta.get('path') == file_path or os.path.basename(meta.get('path', '')) == os.path.basename(file_path):
                    total_headers = len(meta.get('headers', []))
                    break
            headers_per_file_lines.append(f"{os.path.basename(file_path)}: {mapped_count}/{total_headers} headers")
        headers_per_file_str = "\n  - " + "\n  - ".join(headers_per_file_lines[:10])
        if len(headers_per_file_lines) > 10:
            headers_per_file_str += f"\n  ... and {len(headers_per_file_lines) - 10} more files"
        # Generate analysis report
        report = format_analysis_report(
            total_files=len(data_files),
            total_headers=len(all_headers),
            analyzed_files=file_list_str,
            headers_per_file=headers_per_file_str,
            datetime_str=now
        )
        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                f.write(report)
            print(f"Analysis report saved to {args.output}")
        else:
            print(report)
    
    # Handle the extract command
    elif args.command == "extract":
        # Load field mappings
        if args.use_ai:
            mapper = AIFieldMapper([])  # Initialize with empty target fields
            mapper.load_mappings(args.mappings)
        else:
            # First load the mappings file to get target fields
            try:
                with open(args.mappings, 'r') as f:
                    mappings_data = json.load(f)
                target_fields = mappings_data.get('target_fields', DEFAULT_TARGET_FIELDS)
                mapper = FieldMapper(target_fields)
                mapper.load_mappings(args.mappings)
            except Exception as e:
                print(f"Error loading mappings file: {str(e)}", file=sys.stderr)
                sys.exit(1)
        
        # Get file paths from mappings
        file_paths = mapper.get_all_file_paths()
        if not file_paths:
            print("Error: No file paths found in mappings.", file=sys.stderr)
            sys.exit(1)
        
        print(f"Extracting data from {len(file_paths)} files using mappings in {args.mappings}")
        
        # Extract data
        record_count = write_jsonl(
            extract_all_data(file_paths, mapper),
            args.output,
            args.batch_size,
            args.group_by_email
        )
        
        print(f"Extracted {record_count} records to {args.output}")

        # Summarize lines written from each file into the output
        try:
            from collections import Counter
            file_counts = Counter()
            with open(args.output, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        obj = json.loads(line)
                        src = obj.get('_source_file', 'UNKNOWN')
                        file_counts[src] += 1
                    except Exception:
                        continue
            print("\nLines written per source file:")
            print("=" * 80)
            for fname, count in file_counts.most_common():
                print(f"  {fname}: {count}")
        except Exception as e:
            print(f"Warning: Could not summarize per-file line counts: {e}")

    
    # Handle the process command (analyze + extract)
    elif args.command == "process":
        # Find data files
        data_files = find_data_files(args.paths, args.file_types, args.max_files)
        if not data_files:
            print("Error: No matching data files found.", file=sys.stderr)
            sys.exit(1)
        
        print(f"Found {len(data_files)} data files to process.")
        
        # Process files to extract headers
        header_stats, file_metadata, all_headers = process_files(data_files)
        
        # Create field mappings
        if args.use_ai:
            # Use AI-based field mapping with custom target fields
            target_fields = args.target_fields or DEFAULT_TARGET_FIELDS
            data_description = args.data_description or ""
            print(f"Using AI to create field mappings with target fields: {', '.join(target_fields)}")
            if data_description:
                print(f"Using data description: \"{data_description}\"")
            mapper = await create_ai_field_mappings(file_metadata, target_fields, data_description)
            mappings_report = format_ai_mappings_report(mapper)
        else:
            # Use traditional regex-based field mapping
            target_fields = args.target_fields or DEFAULT_TARGET_FIELDS
            print(f"Creating field mappings with target fields: {', '.join(target_fields)}")
            mapper = create_field_mappings(file_metadata, target_fields)
            mappings_report = format_mappings_report(mapper)
        
        # Save mappings to file
        mapper.save_mappings(args.mappings_output)
        print(f"Field mappings saved to {args.mappings_output}")
        
        # Prepare extra info for report
        import datetime
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        file_list = [os.path.basename(f) for f in data_files]
        file_list_str = "\n  - ".join(file_list[:10])
        if len(file_list) > 10:
            file_list_str += f"\n  ... and {len(file_list) - 10} more files"
        headers_per_file_lines = []
        file_mappings = mapper.get_all_mappings()  # Dict[file_path, Dict[original_header, normalized_field]]
        for file_path in data_files:
            # Try to match mapping by full path, fallback to basename
            mapping = file_mappings.get(file_path)
            if mapping is None:
                for k in file_mappings:
                    if os.path.basename(k) == os.path.basename(file_path):
                        mapping = file_mappings[k]
                        break
            count = len(mapping) if mapping else 0
            headers_per_file_lines.append(f"{os.path.basename(file_path)}: {count} headers")
        headers_per_file_str = "\n  - " + "\n  - ".join(headers_per_file_lines[:10])
        if len(headers_per_file_lines) > 10:
            headers_per_file_str += f"\n  ... and {len(headers_per_file_lines) - 10} more files"
        # Generate analysis report if requested
        if args.analysis_output:
            report = format_analysis_report(
                total_files=len(data_files),
                total_headers=len(all_headers),
                analyzed_files=file_list_str,
                headers_per_file=headers_per_file_str,
                datetime_str=now
            )
            with open(args.analysis_output, 'w', encoding='utf-8') as f:
                f.write(report)
            print(f"Analysis report saved to {args.analysis_output}")

        
        # Extract data
        print(f"Extracting data from {len(data_files)} files...")
        record_count = write_jsonl(
            extract_all_data(data_files, mapper),
            args.extract_output,
            args.batch_size,
            args.group_by_email
        )
        
        print(f"Extracted {record_count} records to {args.extract_output}")


def format_field_groups(field_groups: Dict[str, Set[str]], header_stats: Dict[str, Dict[str, Any]] = None) -> str:
    """
    Format field groups for display.
    
    Args:
        field_groups: Dictionary mapping field types to sets of field names
        header_stats: Optional dictionary containing header statistics
        
    Returns:
        Formatted string representation of field groups
    """
    lines = [
        "Field Groups by Type",
        "=" * 80,
        ""
    ]
    
    for field_type, fields in sorted(field_groups.items()):
        if not fields:
            continue
            
        lines.append(f"{field_type.upper()} FIELDS:")
        lines.append("-" * 40)
        
        # Sort fields by frequency if header_stats is provided
        if header_stats:
            sorted_fields = sorted(fields, key=lambda x: header_stats.get(x, {}).get('count', 0), reverse=True)
        else:
            sorted_fields = sorted(fields)
        
        for field in sorted_fields:
            if header_stats and field in header_stats:
                count = header_stats[field]['count']
                files = header_stats[field]['files']
                files_str = ", ".join(files[:3])
                if len(files) > 3:
                    files_str += f" and {len(files) - 3} more"
                lines.append(f"  {field} ({count} occurrences in {len(files)} files: {files_str})")
            else:
                lines.append(f"  {field}")
        
        lines.append("")
    
    return "\n".join(lines)


def format_field_variations(field_variations: Dict[str, Dict[str, Dict[str, List[str]]]], header_stats: Dict[str, Dict[str, Any]] = None) -> str:
    """
    Format field variations for display.
    
    Args:
        field_variations: Dictionary mapping field types to dictionaries of patterns and matching fields
        header_stats: Optional dictionary containing header statistics
        
    Returns:
        Formatted string representation of field variations
    """
    lines = [
        "Field Variations by Type",
        "=" * 80,
        ""
    ]
    
    for field_type, patterns in sorted(field_variations.items()):
        lines.append(f"{field_type.upper()} FIELDS:")
        lines.append("-" * 40)
        
        for pattern, fields in sorted(patterns.items()):
            if isinstance(fields, dict):
                # New format with file sources
                field_list = list(fields.keys())
                lines.append(f"  Pattern: {pattern}")
                for field in sorted(field_list):
                    files = fields[field]
                    files_str = ", ".join(files[:3])
                    if len(files) > 3:
                        files_str += f" and {len(files) - 3} more"
                    lines.append(f"    - {field} (in {len(files)} files: {files_str})")
            else:
                # Old format without file sources
                lines.append(f"  Pattern: {pattern}")
                for field in sorted(fields):
                    lines.append(f"    - {field}")
        
        lines.append("")
    
    return "\n".join(lines)

def format_analysis_report(
    total_files: int,
    total_headers: int,
    analyzed_files: str = None,
    headers_per_file: str = None,
    datetime_str: str = None
) -> str:
    """
    Unified formatting for field normalizer analysis report (with useful extra info).
    """
    lines = [
        "Field Normalizer Analysis Report",
        "=" * 80,
        f"Analysis run at: {datetime_str}" if datetime_str else None,
        "",
        f"Total files analyzed: {total_files}",
        f"Total unique headers found: {total_headers}",
        ""
    ]
    if analyzed_files:
        lines.append("Files analyzed:")
        lines.append(f"  - {analyzed_files}")
        lines.append("")
    if headers_per_file:
        lines.append("Unique headers per file:")
        lines.append(f"{headers_per_file}")
        lines.append("")
    # Remove None entries
    lines = [l for l in lines if l is not None]
    return "\n".join(lines)

def main():
    """Main entry point that runs the async main function."""
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
