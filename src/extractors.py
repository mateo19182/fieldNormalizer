"""
Header extractors for various file types
"""
import csv
import json
import os
import sys
import re
from typing import List, Set, Tuple
from .infer_headers import sample_csv_data, generate_headers_with_openrouter, update_csv_with_headers


def find_header_row(file_path: str) -> Tuple[int, List[str]]:
    """
    Scan a text file to find the first row that looks like valid headers.
    
    Args:
        file_path: Path to the text file
        
    Returns:
        Tuple of (line number where headers were found (0-based), list of headers)
        If no valid headers found, returns (-1, [])
    """
    with open(file_path, 'r', newline='', encoding='utf-8', errors='replace') as f:
        for i, line in enumerate(f):
            # Try to parse the line as CSV
            try:
                # Try different delimiters
                for delimiter in [',', '\t', ';', '|']:
                    headers = [h.strip() for h in line.strip().split(delimiter)]
                    if has_valid_headers(headers):
                        return i, headers
            except:
                continue
    return -1, []

def extract_headers_from_file(file_path: str) -> Tuple[List[str], bool]:
    """
    Extract headers from a data file based on its extension.
    
    Args:
        file_path: Path to the data file
        
    Returns:
        Tuple of (list of headers found in the file, were_headers_inferred)
    """
    _, ext = os.path.splitext(file_path)
    ext = ext.lower().lstrip('.')
    
    if ext == 'csv':
        return extract_headers_from_csv(file_path)
    elif ext in ('json', 'jsonl'):
        headers = extract_headers_from_json(file_path)
        return headers, False  # JSON files don't need header inference
    elif ext == 'sql':
        headers = extract_headers_from_sql(file_path)
        return headers, False
    elif ext == 'txt':
        # For txt files, first try to find a valid header row
        header_line, headers = find_header_row(file_path)
        if header_line >= 0:
            # Found valid headers, process like CSV but skip to header line
            with open(file_path, 'r', newline='', encoding='utf-8', errors='replace') as f:
                # Skip to header line
                for _ in range(header_line):
                    next(f)
                # Read the header line
                line = next(f)
                # Try different delimiters
                for delimiter in [',', '\t', ';', '|']:
                    try:
                        headers = [h.strip() for h in line.strip().split(delimiter)]
                        if has_valid_headers(headers):
                            return headers, False
                    except:
                        continue
        # If no valid headers found or processing failed, try CSV processing
        return extract_headers_from_csv(file_path)
    else:
        raise ValueError(f"Unsupported file type: {ext}")


def has_valid_headers(headers: List[str]) -> bool:
    """Check if the headers appear to be valid (not null, not numeric, etc.)."""
    if not headers:
        return False
    
    # Common header names that should be allowed even if they'd fail other checks
    allowed_headers = {'email', 'id', 'email address', 'emailaddress', 'mail', 'e-mail'}
    
    for header in headers:
        header_lower = header.lower()
        
        # Skip if it's in our allowed list
        if header_lower in allowed_headers:
            continue
            
        # Skip empty or whitespace-only headers or null / NULL
        if not header or not header.strip() or header.upper() in ('NULL', 'null'):
            return False
        # Skip headers that are just numbers or spaces
        if header.strip().isdigit() or header.strip() == ' ':
            return False
        # Skip very long headers (likely data rows)
        if len(header) > 1000:
            return False
        # Skip headers that have urls - but not email which is a common header
        if ("http" in header_lower or "www" in header_lower):
            return False
        # Skip headers that look like phone numbers (standalone digits)
        # Allow digits within variable names (like userId, field2, etc.)
        if re.match(r'^\d+$', header.strip()):
            return False
    return True

def extract_headers_from_csv(file_path: str) -> Tuple[List[str], bool]:
    """
    Extract headers from a CSV file or JSON-formatted content in a .csv file.
    
    Args:
        file_path: Path to the file (may be CSV or JSON with .csv extension)
        
    Returns:
        Tuple of (list of column headers or JSON keys, were_headers_inferred)
    """
    with open(file_path, 'r', newline='', encoding='utf-8', errors='replace') as f:
        first_line = f.readline().strip()
        
        # Check if the first line looks like a JSON object
        if first_line.startswith('{') and first_line.endswith('}'):
            try:
                # It's a JSON file with .csv extension
                f.seek(0)
                # Read all lines and join them as a JSON array
                json_content = '[' + ','.join(line.strip() for line in f if line.strip()) + ']'
                data = json.loads(json_content)
                
                # Extract all unique keys from JSON objects while preserving order
                headers = []
                seen = set()
                for item in data:
                    if isinstance(item, dict):
                        for key in item.keys():
                            if key not in seen:
                                headers.append(key)
                                seen.add(key)
                return headers, False  # JSON files don't need header inference
            except (json.JSONDecodeError, UnicodeDecodeError):
                # If JSON parsing fails, continue with CSV processing
                f.seek(0)
        
        # If not JSON or JSON parsing failed, try processing as CSV
        f.seek(0)
        try:
            # Try to detect dialect with a larger sample
            sample = f.read(8192)  # Increased sample size
            f.seek(0)
            
            # Skip empty lines before trying to detect dialect
            while True:
                pos = f.tell()
                line = f.readline()
                if not line.strip():
                    continue
                f.seek(pos)
                break
            
            # Special handling for common formats
            first_line = sample.split('\n')[0].strip()
            
            # Check for tab-separated values
            if '\t' in first_line:
                # Handle tab-delimited files (TSV)
                headers = [h.strip() for h in first_line.split('\t') if h.strip()]
                if has_valid_headers(headers):
                    return headers, False
                
                # If simple split didn't work, try with CSV module
                f.seek(0)
                dialect = csv.excel_tab
                reader = csv.reader(f, dialect)
                headers = next(reader)
                headers = [h.strip() for h in headers if h.strip()]
                if has_valid_headers(headers):
                    return headers, False
            
            # Special handling for semicolon-delimited files with quoted fields
            elif ';' in first_line:
                # Handle semicolon-delimited files with quoted fields
                if first_line.endswith(';'):  # Handle trailing semicolon
                    first_line = first_line[:-1]
                
                # Check if fields are quoted
                if '"' in first_line:
                    # Use the csv module with proper dialect
                    f.seek(0)
                    dialect = csv.excel()
                    dialect.delimiter = ';'
                    dialect.quotechar = '"'
                    dialect.doublequote = True
                    
                    reader = csv.reader(f, dialect)
                    headers = next(reader)
                    
                    # Clean up headers
                    headers = [h.strip().strip('"\'') for h in headers if h]
                    
                    if has_valid_headers(headers):
                        return headers, False
                else:
                    # Simple split for non-quoted fields
                    headers = [h.strip() for h in first_line.split(';') if h]
                    if has_valid_headers(headers):
                        return headers, False
            
            # Try with CSV Sniffer for other formats
            try:
                dialect = csv.Sniffer().sniff(sample)
            except:
                # If dialect detection fails, try common delimiters
                for delimiter in [',', '\t', ';', '|']:
                    if delimiter in first_line:
                        if delimiter == '\t':
                            dialect = csv.excel_tab
                        else:
                            dialect = csv.excel()
                            dialect.delimiter = delimiter
                        dialect.quotechar = '"'
                        dialect.doublequote = True
                        break
                else:
                    # If no delimiter found, default to comma
                    dialect = csv.excel()
            
            # Read the first row as potential headers
            f.seek(0)
            reader = csv.reader(f, dialect)
            try:
                headers = next(reader)
                
                # Clean up headers - remove quotes and whitespace
                headers = [h.strip().strip('"\'') for h in headers if h]
                
                # Check if these look like valid headers
                if has_valid_headers(headers):
                    return headers, False  # Valid headers found, no inference needed
                
                # Debug output for header validation failures
                print(f"Headers in {os.path.basename(file_path)} failed validation: {headers}", file=sys.stderr)
                
                # If headers don't look valid, try to infer them
                # Get sample data for inference
                f.seek(0)
                sample_data, num_columns = sample_csv_data(file_path)
                if sample_data and num_columns == len(headers):
                    try:
                        inferred_headers = generate_headers_with_openrouter(sample_data, num_columns, os.path.basename(file_path))
                        if inferred_headers and len(inferred_headers) == num_columns:
                            print(f"Info: Inferred headers for {os.path.basename(file_path)} using AI", file=sys.stderr)
                            # Save the inferred headers back to the file
                            try:
                                update_csv_with_headers(file_path, inferred_headers)
                                print(f"Info: Updated {os.path.basename(file_path)} with inferred headers", file=sys.stderr)
                            except Exception as update_error:
                                print(f"Warning: Failed to update {file_path} with inferred headers: {str(update_error)}", file=sys.stderr)
                            return inferred_headers, True  # Headers were inferred
                    except Exception as e:
                        print(f"Warning: Failed to infer headers for {file_path}: {str(e)}", file=sys.stderr)
                
                # If inference failed, return the original headers with a warning
                print(f"Warning: No valid headers found in {os.path.basename(file_path)}, using default column names", file=sys.stderr)
                return headers if headers else [], False
                
            except StopIteration:
                # Empty file
                return [], False
                
        except Exception as e:
            print(f"Error processing {file_path}: {str(e)}", file=sys.stderr)
            # If all else fails, try with default dialect
            f.seek(0)
            # Try tab-separated as a last resort if it's in the filename
            if '\t' in first_line or 'tab' in file_path.lower():
                reader = csv.reader(f, csv.excel_tab)
            else:
                reader = csv.reader(f)
            try:
                headers = next(reader)
                headers = [h.strip().strip('"\'') for h in headers if h]  # Clean up headers
                return (headers, False) if has_valid_headers(headers) else ([], False)
            except StopIteration:
                # Empty file or no headers found
                return [], False

def extract_headers_from_json(file_path: str) -> List[str]:
    """
    Extract headers (keys) from a JSON file or JSONL (JSON Lines) file.
    
    Args:
        file_path: Path to the JSON/JSONL file
        
    Returns:
        List of all keys found in the JSON structure, preserving order
    """
    headers = []
    seen = set()
    
    # First try parsing as regular JSON
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        def extract_keys(obj, prefix=''):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    if key not in seen:
                        headers.append(key)
                        seen.add(key)
                    if isinstance(value, (dict, list)):
                        extract_keys(value, f"{prefix}{key}.")
            elif isinstance(obj, list) and obj:
                # Process the first item as a representative
                extract_keys(obj[0], prefix)
        
        extract_keys(data)
        
    except json.JSONDecodeError:
        # If regular JSON parsing fails, try parsing as JSONL (JSON Lines)
        # print(f"JSON parsing failed for {file_path}, trying JSONL format", file=sys.stderr)
        with open(file_path, 'r', encoding='utf-8') as f:
            # Process each line as a separate JSON object
            for line_num, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    obj = json.loads(line)
                    if isinstance(obj, dict):
                        for key in obj.keys():
                            if key not in seen:
                                headers.append(key)
                                seen.add(key)
                except json.JSONDecodeError as e:
                    print(f"Error parsing JSONL at line {line_num+1}: {e}", file=sys.stderr)
                    # Continue to the next line even if this one failed
                    continue
                    
        if not headers:
            print(f"Failed to extract headers from {file_path} as JSON or JSONL", file=sys.stderr)
            
    return headers

def extract_headers_from_sql(file_path: str) -> List[str]:
    """
    Extract column names from SQL CREATE TABLE statements.
    
    Args:
        file_path: Path to the SQL file
        
    Returns:
        List of column names found in CREATE TABLE statements, preserving order
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        sql_content = f.read()
    
    # Find CREATE TABLE statements with backticks for table names and column names
    create_table_pattern = r'CREATE\s+TABLE\s+(?:`[^`]+`|\w+)\s*\((.*?)\)(?:\s*ENGINE|\s*;)'
    create_table_matches = re.finditer(create_table_pattern, sql_content, re.IGNORECASE | re.DOTALL)
    
    headers = []
    seen = set()
    
    for match in create_table_matches:
        # Extract column definitions
        column_defs = match.group(1)
        
        # Extract column names using regex that handles backtick quotes
        # This pattern looks for `column_name` or column_name followed by type definition
        column_pattern = r'\s*(?:`([^`]+)`|(\w+))\s+\w+'
        column_matches = re.finditer(column_pattern, column_defs, re.MULTILINE)
        
        for col_match in column_matches:
            # Get the column name (either from backtick group or regular group)
            col_name = col_match.group(1) if col_match.group(1) else col_match.group(2)
            if col_name and not col_name.upper() in ('PRIMARY', 'FOREIGN', 'UNIQUE', 'CHECK', 'CONSTRAINT', 'KEY'):
                if col_name not in seen:
                    headers.append(col_name)
                    seen.add(col_name)
    
    return headers