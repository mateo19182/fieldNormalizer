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
    
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Common arguments for all commands
    for cmd_parser in [parser, subparsers.add_parser("analyze"), subparsers.add_parser("extract"), subparsers.add_parser("process")]:
        cmd_parser.add_argument(
            "--config",
            help="Path to configuration file (JSON format)",
        )
    
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
        default=["csv", "json"],
        help="File types to process (default: csv json sql)",
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


def process_files(file_paths: List[str]) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]], List[str]]:
    """
    Process all files and extract headers.
    
    Args:
        file_paths: List of file paths to process
        
    Returns:
        Tuple of (header_stats, file_metadata, all_headers)
    """
    header_stats = {}
    file_metadata = []
    all_headers = []
    failed_files = {}
    
    for file_path in tqdm(file_paths, desc="Processing files", unit="file"):
        try:
            # Extract headers from file
            headers, headers_inferred = extract_headers_from_file(file_path)
            
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
            
        except Exception as e:
            failed_files[file_path] = str(e)
            print(f"Error processing {file_path}: {str(e)}", file=sys.stderr)
    
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
        
        # Generate analysis report
        report_lines = [
            "Field Normalizer Analysis Report",
            "=" * 80,
            "",
            f"Total files analyzed: {len(data_files)}",
            f"Total unique headers found: {len(all_headers)}",
            ""
        ]
        
        # Add field groups to report
        if not args.no_normalize:
            report_lines.append(format_field_groups(field_groups, header_stats))
        
        # Add field variations to report
        if not args.no_variations and not args.no_normalize:
            field_variations = analyze_field_variations(all_headers, header_stats)
            report_lines.append(format_field_variations(field_variations, header_stats))
        
        # Add mappings report
        report_lines.append("")
        report_lines.append(mappings_report)
        
        # Add AI analysis section if AI was used
        if args.use_ai:
            report_lines.append("")
            report_lines.append("AI Analysis Details")
            report_lines.append("=" * 80)
            report_lines.append("")
            
            for file_name, api_data in mapper.api_responses.items():
                report_lines.append(f"File: {file_name}")
                report_lines.append("-" * (len(file_name) + 6))
                report_lines.append("")
                
                # Add headers
                report_lines.append("Headers:")
                report_lines.append("```")
                report_lines.append(", ".join(api_data["headers"]))
                report_lines.append("```")
                report_lines.append("")
                
                # Add sample data in native format
                report_lines.append(f"Sample Data ({api_data.get('sample_format', 'unknown').upper()}):")
                report_lines.append("```")
                if "sample_display" in api_data:
                    report_lines.append(api_data["sample_display"])
                else:
                    report_lines.append(json.dumps(api_data["sample_data"], indent=2))
                report_lines.append("```")
                report_lines.append("")
                
                # Add prompt
                report_lines.append("AI Prompt:")
                report_lines.append("```")
                report_lines.append(api_data["prompt"])
                report_lines.append("```")
                report_lines.append("")
                
                # Add response
                report_lines.append("AI Response:")
                report_lines.append("```")
                report_lines.append(api_data["response"])
                report_lines.append("```")
                report_lines.append("")
        
        # Output the report
        report = "\n".join(report_lines)
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
            mapper = FieldMapper()
            mapper.load_mappings(args.mappings)
        
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
        
        # Generate analysis report if requested
        if args.analysis_output:
            report_lines = [
                "Field Normalizer Analysis Report",
                "=" * 80,
                "",
                f"Total files analyzed: {len(data_files)}",
                f"Total unique headers found: {len(all_headers)}",
                ""
            ]
            
            # Add field groups to report
            if not args.no_normalize:
                field_groups = group_fields(all_headers)
                report_lines.append(format_field_groups(field_groups, header_stats))
            
            # Add field variations to report
            if not args.no_variations and not args.no_normalize:
                field_variations = analyze_field_variations(all_headers, header_stats)
                report_lines.append(format_field_variations(field_variations, header_stats))
            
            # Add mappings report
            report_lines.append("")
            report_lines.append(mappings_report)
            
            # Add AI analysis section if AI was used
            if args.use_ai:
                report_lines.append("")
                report_lines.append("AI Analysis Details")
                report_lines.append("=" * 80)
                report_lines.append("")
                
                for file_name, api_data in mapper.api_responses.items():
                    report_lines.append(f"File: {file_name}")
                    report_lines.append("-" * (len(file_name) + 6))
                    report_lines.append("")
                    
                    # Add headers
                    report_lines.append("Headers:")
                    report_lines.append("```")
                    report_lines.append(", ".join(api_data["headers"]))
                    report_lines.append("```")
                    report_lines.append("")
                    
                    # Add sample data in native format
                    report_lines.append(f"Sample Data ({api_data.get('sample_format', 'unknown').upper()}):")
                    report_lines.append("```")
                    if "sample_display" in api_data:
                        report_lines.append(api_data["sample_display"])
                    else:
                        report_lines.append(json.dumps(api_data["sample_data"], indent=2))
                    report_lines.append("```")
                    report_lines.append("")
                    
                    # Add prompt
                    report_lines.append("AI Prompt:")
                    report_lines.append("```")
                    report_lines.append(api_data["prompt"])
                    report_lines.append("```")
                    report_lines.append("")
                    
                    # Add response
                    report_lines.append("AI Response:")
                    report_lines.append("```")
                    report_lines.append(api_data["response"])
                    report_lines.append("```")
                    report_lines.append("")
                    
                    # Add final mappings if available
                    if file_name in [os.path.basename(path) for path in mapper.file_mappings]:
                        file_path = next(path for path in mapper.file_mappings if os.path.basename(path) == file_name)
                        file_mappings = mapper.file_mappings[file_path]
                        
                        report_lines.append("Final Mappings:")
                        report_lines.append("```json")
                        report_lines.append(json.dumps(file_mappings, indent=2))
                        report_lines.append("```")
                        report_lines.append("")
                    
                    report_lines.append("-" * 80)
                    report_lines.append("")
            
            # Output the report
            report = "\n".join(report_lines)
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


def main():
    """Main entry point that runs the async main function."""
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
