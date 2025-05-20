"""
Data extraction module for Field Normalizer.
Handles extracting data from files based on field mappings.
"""
import csv
import json
import os
import sys
from typing import Dict, List, Set, Any, Iterator, Optional, Tuple
from tqdm import tqdm
from src.field_mapper import FieldMapper
from src.field_normalizer import validate_field_value

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
    
    if ext == 'csv':
        yield from extract_data_from_csv(file_path, field_mapping)
    elif ext == 'json':
        yield from extract_data_from_json(file_path, field_mapping)
    elif ext == 'sql':
        yield from extract_data_from_sql(file_path, field_mapping)
    else:
        print(f"Warning: Unsupported file type: {ext}, skipping {file_path}", file=sys.stderr)

def extract_data_from_csv(file_path: str, field_mapping: Dict[str, List[str]]) -> Iterator[Dict[str, Any]]:
    """
    Extract data from a CSV file based on field mappings.
    
    Args:
        file_path: Path to the CSV file
        field_mapping: Mapping of normalized field types to lists of original headers
        
    Yields:
        Dictionaries containing extracted data with normalized field names
    """
    try:
        with open(file_path, 'r', newline='', encoding='utf-8', errors='replace') as f:
            # Try to detect dialect
            sample = f.read(4096)
            f.seek(0)
            
            try:
                dialect = csv.Sniffer().sniff(sample)
                has_header = csv.Sniffer().has_header(sample)
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
                        
                        # Skip NULL values
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

def extract_data_from_sql(file_path: str, field_mapping: Dict[str, List[str]]) -> Iterator[Dict[str, Any]]:
    """
    Extract data from a SQL file based on field mappings.
    
    Args:
        file_path: Path to the SQL file
        field_mapping: Mapping of normalized field types to lists of original headers
        
    Yields:
        Dictionaries containing extracted data with normalized field names
    """
    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()
            
            # Find all INSERT statements
            import re
            insert_pattern = r"INSERT\s+INTO\s+`?(\w+)`?\s*\(([^)]+)\)\s*VALUES\s*(.+?);"
            matches = re.finditer(insert_pattern, content, re.IGNORECASE | re.MULTILINE)
            
            for match in tqdm(matches, desc="Processing SQL INSERT statements", unit="statement"):
                table_name = match.group(1)
                columns_str = match.group(2)
                values_str = match.group(3)
                
                # Parse column names
                columns = [col.strip().strip('`') for col in columns_str.split(',')]
                
                # Create a mapping from column index to normalized field
                column_mapping = {}
                for field_type, original_headers in field_mapping.items():
                    for header in original_headers:
                        if header in columns:
                            column_idx = columns.index(header)
                            column_mapping[column_idx] = (field_type, header)
                
                # Parse values - handle both single and multi-row inserts
                # First, split into individual value sets
                value_sets = []
                current_set = []
                in_quotes = False
                current_value = ""
                
                for char in values_str:
                    if char == "'" and (len(current_value) == 0 or current_value[-1] != '\\'):
                        in_quotes = not in_quotes
                        current_value += char
                    elif char == ',' and not in_quotes:
                        current_set.append(current_value.strip())
                        current_value = ""
                    elif char == '(' and not in_quotes and not current_value:
                        # Start of new value set
                        if current_set:
                            value_sets.append(current_set)
                            current_set = []
                    elif char == ')' and not in_quotes:
                        # End of value set
                        if current_value.strip():
                            current_set.append(current_value.strip())
                        if current_set:
                            value_sets.append(current_set)
                            current_set = []
                        current_value = ""
                    else:
                        current_value += char
                
                # Process each set of values
                for value_set in value_sets:
                    if len(value_set) != len(columns):
                        continue  # Skip malformed rows
                    
                    record = {}
                    
                    # Extract data based on mappings
                    for col_idx, (field_type, original_header) in column_mapping.items():
                        if col_idx < len(value_set):
                            value = value_set[col_idx].strip()
                            
                            # Remove surrounding quotes if present
                            if value.startswith("'") and value.endswith("'"):
                                value = value[1:-1]
                            
                            # Skip NULL values
                            if not value or value.upper() in ('NULL', 'N/A', 'NONE', ''):
                                continue
                            
                            # Validate field value based on field type
                            validated_value = validate_field_value(field_type, value)
                            if validated_value is None:
                                continue
                            
                            # Handle multiple fields mapping to the same normalized field
                            if field_type in record:
                                if isinstance(record[field_type], list):
                                    if validated_value not in record[field_type]:
                                        record[field_type].append(validated_value)
                                else:
                                    if validated_value != record[field_type]:
                                        record[field_type] = [record[field_type], validated_value]
                            else:
                                record[field_type] = validated_value
                    
                    # Only yield records that have at least one of our target fields
                    if record:
                        # Add source file information
                        record['_source_file'] = os.path.basename(file_path)
                        yield record
                        
    except Exception as e:
        print(f"Error extracting data from SQL file {file_path}: {str(e)}", file=sys.stderr)

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
                
                # Convert non-string values to strings
                if not isinstance(value, str):
                    value = str(value)
                
                # Skip NULL values
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

def extract_all_data(file_paths: List[str], field_mapper: Any) -> Iterator[Dict[str, Any]]:
    """
    Extract data from all files based on field mappings.
    
    Args:
        file_paths: List of file paths to process
        field_mapper: FieldMapper or AIFieldMapper instance with mappings
        
    Yields:
        Dictionaries containing extracted data with normalized field names
    """
    # Track files with only one mapping (these are often not useful)
    single_mapping_files = []
    
    # Process each file
    for file_path in file_paths:
        # Get inverse mapping for this file (field type -> list of original headers)
        inverse_mapping = field_mapper.get_inverse_mapping(file_path)
        
        # Skip files with only one mapping (these are often not useful)
        total_mappings = sum(len(headers) for headers in inverse_mapping.values())
        if total_mappings <= 1:
            single_mapping_files.append(file_path)
            continue
        
        # Extract data from this file
        try:
            for record in extract_data_from_file(file_path, inverse_mapping):
                if record:  # Only yield non-empty records
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
    seen_records = set()
    
    for record in records:
        # First deduplicate values within the record
        record = deduplicate_record_values(record)
        
        # Create a hashable representation of the record for deduplication
        # We exclude the _source_file field from the hash to deduplicate across files
        record_hash = hash(frozenset({
            k: tuple(v) if isinstance(v, list) else v 
            for k, v in record.items() 
            if k != '_source_file'
        }.items()))
        
        if record_hash not in seen_records:
            seen_records.add(record_hash)
            yield record

def write_jsonl(records: Iterator[Dict[str, Any]], output_path: str, batch_size: int = 1000, group_by_email: bool = False) -> int:
    """
    Write records to a JSONL file in batches.
    
    Args:
        records: Iterator of record dictionaries
        output_path: Path to output JSONL file
        batch_size: Number of records to write in each batch
        
    Returns:
        Total number of records written
    """
    # Convert records to a list for processing
    records_list = list(records)
    
    # Optionally merge records by email
    if group_by_email:
        print("Merging records by email...")
        records_list = list(merge_records_by_email(records_list))
    
    # Then deduplicate any remaining duplicates
    print(f"Deduplicating {len(records_list)} records...")
    deduplicated_records = list(deduplicate_records(records_list))
    
    total_records = len(deduplicated_records)
    batch = []
    
    # Add progress bar for writing records
    with open(output_path, 'w', encoding='utf-8') as f:
        for record in tqdm(deduplicated_records, desc="Writing records", unit="record"):
            batch.append(json.dumps(record))
            
            if len(batch) >= batch_size:
                f.write('\n'.join(batch) + '\n')
                batch = []
        
        # Write any remaining records
        if batch:
            f.write('\n'.join(batch) + '\n')
    
    return total_records
