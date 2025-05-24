"""
Header extractors for various file types
"""
import csv
import json
import os
import sys
import re
from typing import List, Set, Tuple, Optional
from .infer_headers import sample_csv_data, generate_headers_with_openrouter, update_csv_with_headers


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
        return extract_headers_from_txt(file_path)
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
    Extract headers from a CSV file with clear, ordered logic.
    
    Args:
        file_path: Path to the CSV file
        
    Returns:
        Tuple of (list of column headers, were_headers_inferred)
    """
    try:
        with open(file_path, 'r', newline='', encoding='utf-8', errors='replace') as f:
            # Step 1: Check if it's actually JSON content in a .csv file
            headers = _try_json_in_csv(f)
            if headers:
                return headers, False
            
            # Step 2: Try ordered delimiter strategies
            f.seek(0)
            headers = _try_delimiter_strategies(f)
            if headers and has_valid_headers(headers):
                return headers, False
            
            # Step 3: Use CSV sniffer as fallback
            f.seek(0)
            headers = _try_csv_sniffer(f)
            if headers and has_valid_headers(headers):
                return headers, False
            
            # Step 4: Final fallback - use simple CSV reader
            f.seek(0)
            headers = _try_simple_csv_reader(f)
            if headers and has_valid_headers(headers):
                return headers, False
            
            # Step 5: If headers look invalid, try AI inference
            if headers:  # We have headers but they failed validation
                print(f"Headers in {os.path.basename(file_path)} failed validation: {headers[:5]}...", file=sys.stderr)
                inferred_headers = _try_ai_header_inference(file_path, headers)
                if inferred_headers:
                    return inferred_headers, True
            
            # Step 6: Return whatever we found or empty list
            print(f"Warning: No valid headers found in {os.path.basename(file_path)}", file=sys.stderr)
            return headers if headers else [], False
            
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}", file=sys.stderr)
        return [], False


def _try_json_in_csv(f) -> Optional[List[str]]:
    """Check if the file contains JSON content despite .csv extension."""
    first_line = f.readline().strip()
    
    if first_line.startswith('{') and first_line.endswith('}'):
        try:
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
            return headers
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass  # Not JSON, continue with CSV processing
    return None


def _try_delimiter_strategies(f) -> Optional[List[str]]:
    """Try different delimiter strategies in order of preference."""
    # Read sample to analyze
    sample = f.read(8192)
    f.seek(0)
    
    if not sample.strip():
        return None
    
    first_line = sample.split('\n')[0].strip()
    if not first_line:
        return None
    
    # Strategy 1: Tab-separated (most explicit)
    if '\t' in first_line:
        headers = _try_delimiter(f, '\t', handle_quotes=True)
        if headers:
            return headers
    
    # Strategy 2: Semicolon-separated (common in European CSVs)
    if ';' in first_line:
        headers = _try_delimiter(f, ';', handle_quotes=True)
        if headers:
            return headers
    
    # Strategy 3: Pipe-separated
    if '|' in first_line:
        headers = _try_delimiter(f, '|', handle_quotes=True)
        if headers:
            return headers
    
    # Strategy 4: Comma-separated (most common)
    if ',' in first_line:
        headers = _try_delimiter(f, ',', handle_quotes=True)
        if headers:
            return headers
    
    return None


def _try_delimiter(f, delimiter: str, handle_quotes: bool = True) -> Optional[List[str]]:
    """Try parsing with a specific delimiter."""
    f.seek(0)
    
    try:
        if handle_quotes:
            # Use CSV module for proper quote handling
            dialect = csv.excel()
            dialect.delimiter = delimiter
            reader = csv.reader(f, dialect)
            headers = next(reader)
        else:
            # Simple split for non-quoted fields
            first_line = f.readline().strip()
            if first_line.endswith(delimiter):  # Handle trailing delimiter
                first_line = first_line[:-1]
            headers = [h.strip() for h in first_line.split(delimiter)]
        
        # Clean up headers
        headers = [h.strip().strip('"\'') for h in headers if h.strip()]
        
        return headers if headers else None
        
    except Exception:
        return None


def _try_csv_sniffer(f) -> Optional[List[str]]:
    """Use CSV sniffer to detect dialect."""
    try:
        sample = f.read(8192)
        f.seek(0)
        
        dialect = csv.Sniffer().sniff(sample)
        reader = csv.reader(f, dialect)
        headers = next(reader)
        
        # Clean up headers
        headers = [h.strip().strip('"\'') for h in headers if h.strip()]
        return headers if headers else None
        
    except Exception:
        return None


def _try_simple_csv_reader(f) -> Optional[List[str]]:
    """Use default CSV reader as final fallback."""
    try:
        reader = csv.reader(f)
        headers = next(reader)
        
        # Clean up headers
        headers = [h.strip().strip('"\'') for h in headers if h.strip()]
        return headers if headers else None
        
    except Exception:
        return None


def _try_ai_header_inference(file_path: str, original_headers: List[str]) -> Optional[List[str]]:
    """Try to infer headers using AI if available."""
    try:
        sample_data, num_columns = sample_csv_data(file_path)
        if sample_data and num_columns == len(original_headers):
            inferred_headers = generate_headers_with_openrouter(
                sample_data, num_columns, os.path.basename(file_path)
            )
            if inferred_headers and len(inferred_headers) == num_columns:
                print(f"Info: Inferred headers for {os.path.basename(file_path)} using AI", file=sys.stderr)
                # Save the inferred headers back to the file
                try:
                    update_csv_with_headers(file_path, inferred_headers)
                    print(f"Info: Updated {os.path.basename(file_path)} with inferred headers", file=sys.stderr)
                except Exception as update_error:
                    print(f"Warning: Failed to update {file_path} with inferred headers: {str(update_error)}", file=sys.stderr)
                return inferred_headers
    except Exception as e:
        print(f"Warning: Failed to infer headers for {file_path}: {str(e)}", file=sys.stderr)
    
    return None


def extract_headers_from_txt(file_path: str) -> Tuple[List[str], bool]:
    """
    Extract headers from a text file by finding the first valid header row.
    
    Args:
        file_path: Path to the text file
        
    Returns:
        Tuple of (list of headers, were_headers_inferred)
    """
    header_line, headers = find_header_row(file_path)
    if header_line >= 0 and headers:
        return headers, False
    
    # If no valid headers found, try processing as CSV
    return extract_headers_from_csv(file_path)


def find_header_row(file_path: str) -> Tuple[int, List[str]]:
    """
    Scan a text file to find the first row that looks like valid headers.
    
    Args:
        file_path: Path to the text file
        
    Returns:
        Tuple of (line number where headers were found (0-based), list of headers)
        If no valid headers found, returns (-1, [])
    """
    try:
        with open(file_path, 'r', newline='', encoding='utf-8', errors='replace') as f:
            for i, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                
                # Try different delimiters
                for delimiter in [',', '\t', ';', '|']:
                    headers = [h.strip() for h in line.split(delimiter) if h.strip()]
                    if len(headers) > 1 and has_valid_headers(headers):
                        return i, headers
    except Exception:
        pass
    
    return -1, []


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
    
    # Check if file is JSONL based on extension
    _, ext = os.path.splitext(file_path)
    is_jsonl = ext.lower() == '.jsonl'
    
    if is_jsonl:
        headers = _extract_from_jsonl(file_path, seen)
    else:
        headers = _extract_from_json(file_path, seen)
    
    return headers


def _extract_from_jsonl(file_path: str, seen: set) -> List[str]:
    """Extract headers from JSONL file with sampling for large files."""
    headers = []
    
    try:
        file_size = os.path.getsize(file_path)
        
        if file_size > 100_000_000:  # > 100MB, use sampling
            headers = _sample_large_jsonl(file_path, seen)
        else:
            # For smaller files, process all lines
            with open(file_path, 'r', encoding='utf-8') as f:
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
                        continue
                        
    except Exception as e:
        print(f"Error processing JSONL file {file_path}: {str(e)}", file=sys.stderr)
    
    return headers


def _sample_large_jsonl(file_path: str, seen: set) -> List[str]:
    """Sample headers from a large JSONL file."""
    headers = []
    sample_limit = 10000
    
    with open(file_path, 'r', encoding='utf-8') as f:
        # Sample first 1000 lines
        for _ in range(min(1000, sample_limit // 3)):
            line = f.readline()
            if not line:
                break
            line = line.strip()
            if line:
                try:
                    obj = json.loads(line)
                    if isinstance(obj, dict):
                        for key in obj.keys():
                            if key not in seen:
                                headers.append(key)
                                seen.add(key)
                except json.JSONDecodeError:
                    continue
        
        # Sample from middle (approximate)
        try:
            file_size = os.path.getsize(file_path)
            mid_pos = file_size // 2
            f.seek(mid_pos)
            f.readline()  # Skip partial line
            for _ in range(min(1000, sample_limit // 3)):
                line = f.readline()
                if not line:
                    break
                line = line.strip()
                if line:
                    try:
                        obj = json.loads(line)
                        if isinstance(obj, dict):
                            for key in obj.keys():
                                if key not in seen:
                                    headers.append(key)
                                    seen.add(key)
                    except json.JSONDecodeError:
                        continue
        except:
            pass  # Skip middle sampling if it fails
        
        # Sample from end (approximate)
        try:
            end_pos = max(0, file_size - 50000)  # Last ~50KB
            f.seek(end_pos)
            f.readline()  # Skip partial line
            for _ in range(min(1000, sample_limit // 3)):
                line = f.readline()
                if not line:
                    break
                line = line.strip()
                if line:
                    try:
                        obj = json.loads(line)
                        if isinstance(obj, dict):
                            for key in obj.keys():
                                if key not in seen:
                                    headers.append(key)
                                    seen.add(key)
                    except json.JSONDecodeError:
                        continue
        except:
            pass  # Skip end sampling if it fails
    
    return headers


def _extract_from_json(file_path: str, seen: set) -> List[str]:
    """Extract headers from standard JSON file."""
    headers = []
    
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
        headers = _extract_from_jsonl(file_path, seen)
        
    except Exception as e:
        print(f"Error processing JSON file {file_path}: {str(e)}", file=sys.stderr)
    
    return headers


def extract_headers_from_sql(file_path: str) -> List[str]:
    """
    Extract column names from SQL CREATE TABLE statements.
    
    Args:
        file_path: Path to the SQL file
        
    Returns:
        List of column names found in CREATE TABLE statements, preserving order
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
    except Exception as e:
        print(f"Error reading SQL file {file_path}: {str(e)}", file=sys.stderr)
        return []
    
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