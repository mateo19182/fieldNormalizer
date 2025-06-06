"""
Data extraction module for Ultimate Parser.
Handles extracting data from files based on field mappings.
"""
import csv
import json
import os
import sys
from typing import Dict, List, Set, Any, Iterator, Optional, Tuple
from tqdm import tqdm
import concurrent.futures
from src.field_mapper import FieldMapper, DEFAULT_TARGET_FIELDS
from src.field_utilities import validate_field_value
from src.header_extractors import find_header_row

def extract_data_from_file(file_path: str, field_mapping: Dict[str, List[str]]) -> Iterator[Dict[str, Any]]:
    """
    Extract data from a file based on field mappings.
    
    Args:
        file_path: Path to the file
        field_mapping: Mapping of normalized field types to lists of original headers
        
    Yields:
        Dictionaries containing extracted data with normalized field names
    """
    _, ext = os.path.splitext(file_path)
    ext = ext.lower().lstrip('.')
    
    if ext in ['csv', 'txt']:
        yield from extract_data_from_csv(file_path, field_mapping, is_txt=(ext == 'txt'))
    elif ext == 'json':
        yield from extract_data_from_json(file_path, field_mapping)
    elif ext == 'jsonl':
        yield from extract_data_from_jsonl(file_path, field_mapping)
    elif ext == 'sql':
        yield from extract_data_from_sql(file_path, field_mapping)
    else:
        print(f"Warning: Unsupported file type: {ext}, skipping {file_path}", file=sys.stderr)

def extract_data_from_csv(file_path: str, field_mapping: Dict[str, List[str]], is_txt: bool = False) -> Iterator[Dict[str, Any]]:
    """
    Extract data from a CSV or TXT file based on field mappings.
    
    Args:
        file_path: Path to the CSV/TXT file
        field_mapping: Mapping of normalized field types to lists of original headers
        is_txt: Whether this is a txt file (affects header detection)
        
    Yields:
        Dictionaries containing extracted data with normalized field names
    """
    try:
        with open(file_path, 'r', newline='', encoding='utf-8', errors='replace') as f:
            # For txt files, first try to find a valid header row
            header_line = 0
            if is_txt:
                header_line, _ = find_header_row(file_path)
                if header_line < 0:
                    print(f"Warning: No valid headers found in {file_path}, skipping", file=sys.stderr)
                    return
                # Skip to header line
                for _ in range(header_line):
                    next(f)
            
            # Try to detect dialect
            sample = f.read(4096)
            f.seek(0)
            if is_txt:
                # Skip to header line again after reading sample
                for _ in range(header_line):
                    next(f)
            
            try:
                dialect = csv.Sniffer().sniff(sample)
                has_header = True  # We already found headers for txt files
            except:
                # If dialect detection fails, use default
                dialect = csv.excel
                has_header = True
            
            # Create CSV reader
            reader = csv.reader(f, dialect)
            
            # Read headers
            if has_header:
                headers = next(reader)
            else:
                # If no headers, use column indices
                headers = [f"column_{i}" for i in range(len(next(reader)))]
                f.seek(0)
                if is_txt:
                    # Skip to header line again
                    for _ in range(header_line):
                        next(f)
                reader = csv.reader(f, dialect)
            
            # Create a mapping from column index to normalized field
            column_mapping = {}
            for field_type, original_headers in field_mapping.items():
                for header in original_headers:
                    if header in headers:
                        column_idx = headers.index(header)
                        column_mapping[column_idx] = (field_type, header)
            
            # Process each row
            for row in tqdm(reader, desc=f"Extracting rows from {file_path}", unit="row"):
                if not row or all(cell.strip() == '' for cell in row):
                    continue  # Skip empty rows
                
                record = {}
                
                # Extract data based on mappings
                for col_idx, (field_type, original_header) in column_mapping.items():
                    if col_idx < len(row):
                        value = row[col_idx].strip()
                        
                        # Skip NULL values for this field, but continue processing the record
                        if not value or value.upper() in ('NULL', 'N/A', 'NONE', ''):
                            continue
                        
                        # Validate field value based on field type
                        validated_value = validate_field_value(field_type, value)
                        if validated_value is None:
                            continue
                        
                        # Handle multiple fields mapping to the same normalized field
                        if field_type in record:
                            # If we already have a value for this field type
                            if isinstance(record[field_type], list):
                                # If it's already a list, append the new value if not already present
                                if validated_value not in record[field_type]:
                                    record[field_type].append(validated_value)
                            else:
                                # Only convert to list if the values are different
                                if validated_value != record[field_type]:
                                    record[field_type] = [record[field_type], validated_value]
                        else:
                            # First occurrence of this field type
                            record[field_type] = validated_value
                
                # Only yield records that have at least one of our target fields
                if record:
                    # Add source file information
                    record['_source_file'] = os.path.basename(file_path)
                    yield record
                    
    except Exception as e:
        print(f"Error extracting data from CSV file {file_path}: {str(e)}", file=sys.stderr)

def extract_data_from_json(file_path: str, field_mapping: Dict[str, List[str]]) -> Iterator[Dict[str, Any]]:
    """
    Extract data from a JSON file based on field mappings.
    
    Args:
        file_path: Path to the JSON file
        field_mapping: Mapping of normalized field types to lists of original headers
        
    Yields:
        Dictionaries containing extracted data with normalized field names
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Handle different JSON structures
        if isinstance(data, dict):
            # Single object
            yield from _process_json_object(data, field_mapping, file_path)
        elif isinstance(data, list):
            # Array of objects
            for item in tqdm(data, desc="Processing JSON objects", unit="object"):
                if isinstance(item, dict):
                    yield from _process_json_object(item, field_mapping, file_path)
        
    except Exception as e:
        print(f"Error extracting data from JSON file {file_path}: {str(e)}", file=sys.stderr)

def extract_data_from_jsonl(file_path: str, field_mapping: Dict[str, List[str]]) -> Iterator[Dict[str, Any]]:
    """
    Extract data from a JSONL (JSON Lines) file based on field mappings.
    
    Args:
        file_path: Path to the JSONL file
        field_mapping: Mapping of normalized field types to lists of original headers
        
    Yields:
        Dictionaries containing extracted data with normalized field names
    """
    try:
        # Process JSONL files line by line to avoid loading everything into memory
        with open(file_path, 'r', encoding='utf-8') as f:
            # Use tqdm to show progress - estimate total lines for large files
            total_lines = None
            try:
                # Try to get file size and estimate number of lines
                file_size = os.path.getsize(file_path)
                if file_size > 1_000_000_000:  # If file > 1GB
                    # Sample first 1000 lines to estimate average line size
                    sample_size = 0
                    sample_lines = 0
                    for _ in range(1000):
                        line = f.readline()
                        if not line:
                            break
                        sample_size += len(line)
                        sample_lines += 1
                    if sample_lines > 0:
                        avg_line_size = sample_size / sample_lines
                        total_lines = int(file_size / avg_line_size)
                f.seek(0)  # Reset file position after sampling
            except:
                pass  # If estimation fails, proceed without total
                
            # Process each line as a separate JSON object with progress bar
            for line_num, line in enumerate(tqdm(f, desc=f"Processing JSONL from {os.path.basename(file_path)}", 
                                                 unit="line", total=total_lines)):
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    obj = json.loads(line)
                    if isinstance(obj, dict):
                        yield from _process_json_object(obj, field_mapping, file_path)
                except json.JSONDecodeError as e:
                    print(f"Error parsing JSONL at line {line_num+1} in {file_path}: {str(e)}", file=sys.stderr)
                    continue  # Skip problematic lines
                    
    except Exception as e:
        print(f"Error extracting data from JSONL file {file_path}: {str(e)}", file=sys.stderr)

def extract_data_from_sql(file_path: str, field_mapping: Dict[str, List[str]]) -> Iterator[Dict[str, Any]]:
    """
    Extract data from a SQL file using the robust SQL parser.
    
    Args:
        file_path: Path to the SQL file
        field_mapping: Mapping of normalized field types to lists of original headers
        
    Yields:
        Dictionaries containing extracted data with normalized field names
    """
    from .sql_parser import sql_parser
    yield from sql_parser.extract_data_from_sql(file_path, field_mapping)

def _process_json_object(obj: Dict[str, Any], field_mapping: Dict[str, List[str]], file_path: str) -> Iterator[Dict[str, Any]]:
    """
    Process a JSON object and extract fields based on mappings.
    
    Args:
        obj: JSON object (dictionary)
        field_mapping: Mapping of normalized field types to lists of original headers
        file_path: Source file path
        
    Yields:
        Dictionaries containing extracted data with normalized field names
    """
    record = {}
    
    # Extract data based on mappings
    for field_type, original_headers in field_mapping.items():
        for header in original_headers:
            if header in obj:
                value = obj[header]
                
                # Skip empty containers (lists, dicts, etc.) and null values
                if value is None or (hasattr(value, '__len__') and len(value) == 0):
                    continue
                
                # Convert non-string values to strings
                if not isinstance(value, str):
                    value = str(value)
                
                # Skip NULL values
                if not value or value.upper() in ('NULL', 'N/A', 'NONE', '', "<blank>", "<BLANK>", "null"):
                    continue
                
                # Validate field value based on field type
                validated_value = validate_field_value(field_type, value)
                if validated_value is None:
                    continue
                
                # Handle multiple fields mapping to the same normalized field
                if field_type in record:
                    # If we already have a value for this field type
                    if isinstance(record[field_type], list):
                        # If it's already a list, append the new value if not already present
                        if validated_value not in record[field_type]:
                            record[field_type].append(validated_value)
                    else:
                        # Only convert to list if the values are different
                        if validated_value != record[field_type]:
                            record[field_type] = [record[field_type], validated_value]
                else:
                    # First occurrence of this field type
                    record[field_type] = validated_value
    
    # Only yield records that have at least one of our target fields
    if record:
        # Add source file information
        record['_source_file'] = os.path.basename(file_path)
        yield record

def _extract_data_from_file_wrapper(file_path: str, inverse_mapping: Dict[str, List[str]]) -> List[Dict[str, Any]]:
    """
    Wrapper function for parallel processing that collects all records from a file.
    
    Args:
        file_path: Path to the file
        inverse_mapping: Mapping of field types to original headers
        
    Returns:
        List of extracted records
    """
    try:
        # Extract data and collect all records from the generator
        return list(extract_data_from_file(file_path, inverse_mapping))
    except Exception as e:
        print(f"Error extracting data from {file_path}: {str(e)}", file=sys.stderr)
        return []

def extract_all_data(file_paths: List[str], field_mapper: Any) -> Iterator[Dict[str, Any]]:
    """
    Extract data from all files based on field mappings using parallel processing.
    
    Args:
        file_paths: List of file paths to process
        field_mapper: FieldMapper or AIFieldMapper instance with mappings
        
    Yields:
        Dictionaries containing extracted data with normalized field names
    """
    # Get all mappings
    all_mappings = field_mapper.get_all_mappings()
    
    # Track files with only one mapping (these are often not useful)
    single_mapping_files = []
    files_to_process = []
    
    # Process files with mappings
    for file_path in file_paths:
        # Check if we have mappings for this file
        basename = os.path.basename(file_path)
        found_mapping = False
        
        # Try direct path match
        if file_path in all_mappings:
            inverse_mapping = field_mapper.get_inverse_mapping(file_path)
            files_to_process.append((file_path, inverse_mapping))
            found_mapping = True
        
        # Try basename match
        elif basename in all_mappings:
            inverse_mapping = field_mapper.get_inverse_mapping(basename)
            files_to_process.append((file_path, inverse_mapping))
            found_mapping = True
        
        # Try with relative paths that might be in the mappings
        else:
            for mapping_path in all_mappings.keys():
                if mapping_path.endswith('/' + basename) or mapping_path.endswith('\\' + basename):
                    inverse_mapping = field_mapper.get_inverse_mapping(mapping_path)
                    files_to_process.append((file_path, inverse_mapping))
                    found_mapping = True
                    break
        
        # If no mapping found, check if it has only one mapping (often not useful)
        if not found_mapping:
            try:
                inverse_mapping = field_mapper.get_inverse_mapping(file_path)
                total_mappings = sum(len(headers) for headers in inverse_mapping.values())
                if total_mappings <= 1:
                    single_mapping_files.append(file_path)
                else:
                    files_to_process.append((file_path, inverse_mapping))
            except:
                # No mapping found at all
                print(f"Warning: No mapping found for {file_path}", file=sys.stderr)
    
    # Process files sequentially to avoid memory/resource issues with large files
    for file_path, inverse_mapping in tqdm(files_to_process, desc="Processing files", unit="file"):
        try:
            for record in extract_data_from_file(file_path, inverse_mapping):
                if record:
                    yield record
        except Exception as e:
            print(f"Error extracting data from {file_path}: {str(e)}", file=sys.stderr)
    
    # Report files with only one mapping
    if single_mapping_files:
        print(f"\nSkipped {len(single_mapping_files)} files with only one mapping:", file=sys.stderr)
        for file_path in single_mapping_files[:5]:
            print(f"  - {os.path.basename(file_path)}", file=sys.stderr)
        if len(single_mapping_files) > 5:
            print(f"  - ... and {len(single_mapping_files) - 5} more", file=sys.stderr)

def merge_records_by_email(records: Iterator[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
    """
    Merge records with the same email address.
    
    Args:
        records: Iterator of record dictionaries
        
    Yields:
        Merged record dictionaries
    """
    # Buffer to store records by email
    email_records: Dict[str, Dict[str, Any]] = {}
    record_count = 0
    
    # Process each record
    for record in records:
        record_count += 1
        
        # Skip records without email
        if 'email' not in record:
            yield record
            continue
        
        email = record['email']
        
        # Skip empty emails
        if not email:
            yield record
            continue
        
        # If this is the first record with this email, store it
        if email not in email_records:
            email_records[email] = record
        else:
            # Merge with existing record
            existing = email_records[email]
            
            # Merge all fields
            for field, value in record.items():
                if field == '_source_file':
                    # Combine source files
                    if field in existing:
                        if isinstance(existing[field], list):
                            if value not in existing[field]:
                                existing[field].append(value)
                        else:
                            if existing[field] != value:
                                existing[field] = [existing[field], value]
                    else:
                        existing[field] = value
                elif field not in existing:
                    # Add new field
                    existing[field] = value
                else:
                    # Merge field values
                    if isinstance(existing[field], list):
                        if isinstance(value, list):
                            # Both are lists, extend
                            for item in value:
                                if item not in existing[field]:
                                    existing[field].append(item)
                        else:
                            # Existing is list, value is scalar
                            if value not in existing[field]:
                                existing[field].append(value)
                    else:
                        if isinstance(value, list):
                            # Existing is scalar, value is list
                            if existing[field] not in value:
                                value.insert(0, existing[field])
                            existing[field] = value
                        else:
                            # Both are scalars
                            if existing[field] != value:
                                existing[field] = [existing[field], value]
        
        # Periodically yield records to avoid memory buildup
        if record_count % 10000 == 0:
            # Yield records for emails that haven't been seen recently
            emails_to_yield = list(email_records.keys())[:len(email_records) // 2]
            for email in emails_to_yield:
                yield email_records.pop(email)
    
    # Yield remaining records
    for record in email_records.values():
        yield record

def deduplicate_record_values(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deduplicate values within a single record.
    
    Args:
        record: Dictionary containing record data
        
    Returns:
        Record with deduplicated values
    """
    deduplicated = {}
    for field, value in record.items():
        if isinstance(value, list):
            # Convert to set and back to list to deduplicate
            # Preserve order by using dict.fromkeys
            deduplicated[field] = list(dict.fromkeys(value))
        else:
            deduplicated[field] = value
    return deduplicated

def deduplicate_records(records: Iterator[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
    """
    Deduplicate records based on their content.
    
    Args:
        records: Iterator of record dictionaries
        
    Yields:
        Deduplicated records
    """
    import json
    seen_records = set()

    for record in records:
        # First deduplicate values within the record
        record = deduplicate_record_values(record)
        # Exclude '_source_file' from deduplication key
        record_no_source = {k: v for k, v in record.items() if k != '_source_file'}
        # Use canonical JSON serialization for robust, order-insensitive hashing
        try:
            record_key = json.dumps(record_no_source, sort_keys=True, separators=(',', ':'))
        except TypeError:
            # Fallback: convert non-serializable objects to string
            record_key = json.dumps({k: str(v) for k, v in record_no_source.items()}, sort_keys=True, separators=(',', ':'))
        if record_key not in seen_records:
            seen_records.add(record_key)
            yield record

def standardize_record_format(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Standardize record format to ensure consistent field order and list values.
    Excludes empty fields from the output.
    
    Args:
        record: Dictionary containing record data
        
    Returns:
        Standardized record with consistent field order and all values as lists,
        with empty fields excluded
    """
    # Use the standard field order from DEFAULT_TARGET_FIELDS
    standard_fields = DEFAULT_TARGET_FIELDS
    
    # Create a new record with standardized format
    standardized = {}
    
    # Process each field in the standard order
    for field in standard_fields:
        # Convert camelCase to lowercase for backward compatibility
        legacy_field = field.lower()
        
        # Check if the field exists in either format
        if field in record:
            value = record[field]
        elif legacy_field in record:
            value = record[legacy_field]
        else:
            # Field doesn't exist in this record - skip it entirely
            continue
        
        # Ensure value is a list
        if not isinstance(value, list):
            value = [value]
        
        # Only add non-empty values
        if value:  # This checks if the list is not empty
            standardized[field] = value
    
    # Add any additional fields not in the standard list (like _source_file)
    for field, value in record.items():
        if field not in standard_fields and field.lower() not in standard_fields and field not in standardized:
            # For non-standard fields, keep them as is (don't filter empty values)
            standardized[field] = value
    
    return standardized

def process_batch(batch: List[Dict[str, Any]], group_by_email: bool, batch_num: int) -> List[Dict[str, Any]]:
    """
    Process a batch of records with optional email grouping and deduplication.
    
    Args:
        batch: List of records to process
        group_by_email: Whether to group records by email
        batch_num: Batch number (for logging)
        
    Returns:
        Processed batch of records
    """
    # Optionally merge records by email
    if group_by_email:
        batch = list(merge_records_by_email(batch))
    
    # Deduplicate records
    deduplicated = list(deduplicate_records(batch))
    
    # Standardize record format
    return [standardize_record_format(record) for record in deduplicated]

def write_jsonl(records: Iterator[Dict[str, Any]], output_path: str, batch_size: int = 1000, group_by_email: bool = False, include_source: bool = True) -> int:
    """
    Write records to a JSONL file using optimized batch processing with true cross-batch deduplication.
    Uses a two-pass approach to ensure complete deduplication without excessive memory usage.
    
    Args:
        records: Iterator of record dictionaries
        output_path: Path to output JSONL file
        batch_size: Number of records to write in each batch
        group_by_email: Whether to group records by email
        include_source: Whether to include source file information in output
        
    Returns:
        Total number of records written
    """
    import tempfile
    import os
    import json
    
    # Create a temporary directory for our intermediate files
    with tempfile.TemporaryDirectory() as temp_dir:
        # First pass: Process records in batches and write to temp file with hashes
        temp_file_path = os.path.join(temp_dir, "records_with_hashes.jsonl")
        
        print("Pass 1: Processing records and generating hashes...")
        total_processed = 0
        batch_records = []
        batch_count = 0
        
        with open(temp_file_path, 'w', encoding='utf-8') as temp_f:
            # Process the records in batches
            for record in tqdm(records, desc="Processing records (pass 1)", unit="record"):
                batch_records.append(record)
                
                # Process when we reach the batch size
                if len(batch_records) >= batch_size:
                    batch_count += 1
                    # Process this batch (deduplicates within batch)
                    processed_batch = process_batch(batch_records, group_by_email, batch_count)
                    
                    # Generate hash for each record and write to temp file
                    for r in processed_batch:
                        # Create a hash key for the record (same logic as in deduplicate_records)
                        record_no_source = {k: v for k, v in r.items() if k != '_source_file'}
                        try:
                            record_hash = json.dumps(record_no_source, sort_keys=True, separators=(',', ':'))
                        except TypeError:
                            record_hash = json.dumps({k: str(v) for k, v in record_no_source.items()}, sort_keys=True, separators=(',', ':'))
                        
                        # Write record with its hash
                        temp_f.write(json.dumps({"hash": record_hash, "record": r}) + '\n')
                    
                    total_processed += len(processed_batch)
                    batch_records = []
            
            # Process any remaining records
            if batch_records:
                batch_count += 1
                processed_batch = process_batch(batch_records, group_by_email, batch_count)
                
                for r in processed_batch:
                    # Create a hash key for the record
                    record_no_source = {k: v for k, v in r.items() if k != '_source_file'}
                    try:
                        record_hash = json.dumps(record_no_source, sort_keys=True, separators=(',', ':'))
                    except TypeError:
                        record_hash = json.dumps({k: str(v) for k, v in record_no_source.items()}, sort_keys=True, separators=(',', ':'))
                    
                    # Write record with its hash
                    temp_f.write(json.dumps({"hash": record_hash, "record": r}) + '\n')
                
                total_processed += len(processed_batch)
        
        # Second pass: Read the temp file and deduplicate across all batches
        print(f"Pass 1 complete: Processed {total_processed} records")
        print("Pass 2: Deduplicating across all batches...")
        
        seen_hashes = set()
        total_records = 0
        
        with open(temp_file_path, 'r', encoding='utf-8') as temp_f, \
             open(output_path, 'w', encoding='utf-8') as out_f:
            
            for line in tqdm(temp_f, desc="Deduplicating (pass 2)", unit="record"):
                data = json.loads(line)
                record_hash = data["hash"]
                record = data["record"]
                
                # Only write if we haven't seen this hash before
                if record_hash not in seen_hashes:
                    seen_hashes.add(record_hash)
                    # Write record with or without source file based on include_source flag
                    record_array = [record.get(field, []) for field in DEFAULT_TARGET_FIELDS]
                    if include_source:
                        record_array.append(record.get('_source_file', ''))
                    out_f.write(json.dumps(record_array) + '\n')
                    total_records += 1
    
    print(f"Deduplication complete: {total_processed} records processed, {total_records} unique records written")
    return total_records

def write_csv(records: Iterator[Dict[str, Any]], output_path: str, batch_size: int = 1000, group_by_email: bool = False, include_source: bool = True) -> int:
    """
    Write records to a CSV file using optimized batch processing with deduplication.
    
    Args:
        records: Iterator of record dictionaries
        output_path: Path to output CSV file
        batch_size: Number of records to process in each batch
        group_by_email: Whether to group records by email
        include_source: Whether to include source file information in output
        
    Returns:
        Total number of records written
    """
    import tempfile
    import os
    import json
    
    # Create a temporary directory for our intermediate files
    with tempfile.TemporaryDirectory() as temp_dir:
        # First pass: Process records in batches and write to temp file with hashes
        temp_file_path = os.path.join(temp_dir, "records_with_hashes.jsonl")
        
        print("Pass 1: Processing records and generating hashes...")
        total_processed = 0
        batch_records = []
        batch_count = 0
        
        with open(temp_file_path, 'w', encoding='utf-8') as temp_f:
            # Process the records in batches
            for record in tqdm(records, desc="Processing records (pass 1)", unit="record"):
                batch_records.append(record)
                
                # Process when we reach the batch size
                if len(batch_records) >= batch_size:
                    batch_count += 1
                    # Process this batch (deduplicates within batch)
                    processed_batch = process_batch(batch_records, group_by_email, batch_count)
                    
                    # Generate hash for each record and write to temp file
                    for r in processed_batch:
                        # Create a hash key for the record (same logic as in deduplicate_records)
                        record_no_source = {k: v for k, v in r.items() if k != '_source_file'}
                        try:
                            record_hash = json.dumps(record_no_source, sort_keys=True, separators=(',', ':'))
                        except TypeError:
                            record_hash = json.dumps({k: str(v) for k, v in record_no_source.items()}, sort_keys=True, separators=(',', ':'))
                        
                        # Write record with its hash
                        temp_f.write(json.dumps({"hash": record_hash, "record": r}) + '\n')
                    
                    total_processed += len(processed_batch)
                    batch_records = []
            
            # Process any remaining records
            if batch_records:
                batch_count += 1
                processed_batch = process_batch(batch_records, group_by_email, batch_count)
                
                for r in processed_batch:
                    # Create a hash key for the record
                    record_no_source = {k: v for k, v in r.items() if k != '_source_file'}
                    try:
                        record_hash = json.dumps(record_no_source, sort_keys=True, separators=(',', ':'))
                    except TypeError:
                        record_hash = json.dumps({k: str(v) for k, v in record_no_source.items()}, sort_keys=True, separators=(',', ':'))
                    
                    # Write record with its hash
                    temp_f.write(json.dumps({"hash": record_hash, "record": r}) + '\n')
                
                total_processed += len(processed_batch)
        
        # Second pass: Read the temp file and deduplicate across all batches, write to CSV
        print(f"Pass 1 complete: Processed {total_processed} records")
        print("Pass 2: Deduplicating and writing to CSV...")
        
        seen_hashes = set()
        total_records = 0
        
        # Prepare CSV headers
        csv_headers = list(DEFAULT_TARGET_FIELDS)
        if include_source:
            csv_headers.append('_source_file')
        
        with open(temp_file_path, 'r', encoding='utf-8') as temp_f, \
             open(output_path, 'w', encoding='utf-8', newline='') as out_f:
            
            writer = csv.writer(out_f)
            # Write header row
            writer.writerow(csv_headers)
            
            for line in tqdm(temp_f, desc="Deduplicating and writing CSV (pass 2)", unit="record"):
                data = json.loads(line)
                record_hash = data["hash"]
                record = data["record"]
                
                # Only write if we haven't seen this hash before
                if record_hash not in seen_hashes:
                    seen_hashes.add(record_hash)
                    
                    # Convert record to CSV row
                    csv_row = []
                    for field in DEFAULT_TARGET_FIELDS:
                        value = record.get(field, [])
                        if isinstance(value, list):
                            # Join list values with semicolon
                            csv_row.append('; '.join(str(v) for v in value) if value else '')
                        else:
                            csv_row.append(str(value) if value else '')
                    
                    # Add source file if requested
                    if include_source:
                        csv_row.append(record.get('_source_file', ''))
                    
                    writer.writerow(csv_row)
                    total_records += 1
    
    print(f"Deduplication complete: {total_processed} records processed, {total_records} unique records written to CSV")
    return total_records

def write_json(records: Iterator[Dict[str, Any]], output_path: str, batch_size: int = 1000, group_by_email: bool = False, include_source: bool = True) -> int:
    """
    Write records to a JSON file using optimized batch processing with deduplication.
    
    Args:
        records: Iterator of record dictionaries
        output_path: Path to output JSON file
        batch_size: Number of records to process in each batch
        group_by_email: Whether to group records by email
        include_source: Whether to include source file information in output
        
    Returns:
        Total number of records written
    """
    import tempfile
    import os
    import json
    
    # Create a temporary directory for our intermediate files
    with tempfile.TemporaryDirectory() as temp_dir:
        # First pass: Process records in batches and write to temp file with hashes
        temp_file_path = os.path.join(temp_dir, "records_with_hashes.jsonl")
        
        print("Pass 1: Processing records and generating hashes...")
        total_processed = 0
        batch_records = []
        batch_count = 0
        
        with open(temp_file_path, 'w', encoding='utf-8') as temp_f:
            # Process the records in batches
            for record in tqdm(records, desc="Processing records (pass 1)", unit="record"):
                batch_records.append(record)
                
                # Process when we reach the batch size
                if len(batch_records) >= batch_size:
                    batch_count += 1
                    # Process this batch (deduplicates within batch)
                    processed_batch = process_batch(batch_records, group_by_email, batch_count)
                    
                    # Generate hash for each record and write to temp file
                    for r in processed_batch:
                        # Create a hash key for the record (same logic as in deduplicate_records)
                        record_no_source = {k: v for k, v in r.items() if k != '_source_file'}
                        try:
                            record_hash = json.dumps(record_no_source, sort_keys=True, separators=(',', ':'))
                        except TypeError:
                            record_hash = json.dumps({k: str(v) for k, v in record_no_source.items()}, sort_keys=True, separators=(',', ':'))
                        
                        # Write record with its hash
                        temp_f.write(json.dumps({"hash": record_hash, "record": r}) + '\n')
                    
                    total_processed += len(processed_batch)
                    batch_records = []
            
            # Process any remaining records
            if batch_records:
                batch_count += 1
                processed_batch = process_batch(batch_records, group_by_email, batch_count)
                
                for r in processed_batch:
                    # Create a hash key for the record
                    record_no_source = {k: v for k, v in r.items() if k != '_source_file'}
                    try:
                        record_hash = json.dumps(record_no_source, sort_keys=True, separators=(',', ':'))
                    except TypeError:
                        record_hash = json.dumps({k: str(v) for k, v in record_no_source.items()}, sort_keys=True, separators=(',', ':'))
                    
                    # Write record with its hash
                    temp_f.write(json.dumps({"hash": record_hash, "record": r}) + '\n')
                
                total_processed += len(processed_batch)
        
        # Second pass: Read the temp file and deduplicate across all batches, write to JSON
        print(f"Pass 1 complete: Processed {total_processed} records")
        print("Pass 2: Deduplicating and writing to JSON...")
        
        seen_hashes = set()
        all_records = []
        
        with open(temp_file_path, 'r', encoding='utf-8') as temp_f:
            for line in tqdm(temp_f, desc="Deduplicating (pass 2)", unit="record"):
                data = json.loads(line)
                record_hash = data["hash"]
                record = data["record"]
                
                # Only add if we haven't seen this hash before
                if record_hash not in seen_hashes:
                    seen_hashes.add(record_hash)
                    # Remove source file if not requested
                    if not include_source and '_source_file' in record:
                        record = {k: v for k, v in record.items() if k != '_source_file'}
                    all_records.append(record)
        
        # Write all records to JSON file
        print("Writing JSON file...")
        with open(output_path, 'w', encoding='utf-8') as out_f:
            json.dump(all_records, out_f, indent=2, ensure_ascii=False)
        
        total_records = len(all_records)
    
    print(f"Deduplication complete: {total_processed} records processed, {total_records} unique records written to JSON")
    return total_records

def write_data(records: Iterator[Dict[str, Any]], output_path: str, output_format: str = "jsonl", 
               batch_size: int = 1000, group_by_email: bool = False, include_source: bool = True) -> int:
    """
    Write records to a file in the specified format.
    
    Args:
        records: Iterator of record dictionaries
        output_path: Path to output file
        output_format: Output format ('jsonl', 'csv', or 'json')
        batch_size: Number of records to process in each batch
        group_by_email: Whether to group records by email
        include_source: Whether to include source file information in output
        
    Returns:
        Total number of records written
    """
    if output_format == "csv":
        return write_csv(records, output_path, batch_size, group_by_email, include_source)
    elif output_format == "json":
        return write_json(records, output_path, batch_size, group_by_email, include_source)
    elif output_format == "jsonl":
        return write_jsonl(records, output_path, batch_size, group_by_email, include_source)
    else:
        raise ValueError(f"Unsupported output format: {output_format}")
