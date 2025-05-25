"""
Robust SQL parser for various SQL dump formats.
Handles MySQL, PostgreSQL, SQLite dumps and various statement types.
"""
import re
import os
import sys
import json
import sqlparse
from typing import List, Dict, Any, Iterator, Tuple, Optional, Set
from tqdm import tqdm
import io


class SQLParser:
    """
    Robust SQL parser that handles various SQL dump formats and statement types.
    """
    
    def __init__(self):
        # Patterns for different SQL dialects and formats
        self.insert_patterns = [
            # Standard INSERT INTO
            r'INSERT\s+INTO\s+(?:`?([^`\s]+)`?)\s*\(([^)]+)\)\s*VALUES\s*(.+?)(?:;|\s*$)',
            # MySQL REPLACE INTO
            r'REPLACE\s+INTO\s+(?:`?([^`\s]+)`?)\s*\(([^)]+)\)\s*VALUES\s*(.+?)(?:;|\s*$)',
            # SQLite INSERT OR REPLACE
            r'INSERT\s+OR\s+REPLACE\s+INTO\s+(?:`?([^`\s]+)`?)\s*\(([^)]+)\)\s*VALUES\s*(.+?)(?:;|\s*$)',
            # INSERT with ON DUPLICATE KEY UPDATE (MySQL)
            r'INSERT\s+INTO\s+(?:`?([^`\s]+)`?)\s*\(([^)]+)\)\s*VALUES\s*(.+?)\s*ON\s+DUPLICATE\s+KEY\s+UPDATE.*?(?:;|\s*$)',
        ]
        
        # PostgreSQL COPY statement pattern
        self.copy_pattern = r'COPY\s+(?:`?([^`\s]+)`?)\s*\(([^)]+)\)\s+FROM\s+stdin'
        
        # CREATE TABLE patterns for schema extraction
        self.create_table_patterns = [
            # Standard CREATE TABLE
            r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:`?([^`\s]+)`?)\s*\((.*?)\)(?:\s*ENGINE|\s*;|\s*$)',
            # PostgreSQL CREATE TABLE with additional options
            r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:`?([^`\s]+)`?)\s*\((.*?)\)(?:\s*WITH|\s*INHERITS|\s*;|\s*$)',
        ]
    
    def extract_headers_from_sql(self, file_path: str) -> List[str]:
        """
        Extract column names from SQL file using multiple strategies.
        
        Args:
            file_path: Path to the SQL file
            
        Returns:
            List of column names found in the SQL file
        """
        headers = []
        seen = set()
        
        try:
            # Try to extract from CREATE TABLE statements first
            create_headers = self._extract_headers_from_create_statements(file_path)
            for header in create_headers:
                if header not in seen:
                    headers.append(header)
                    seen.add(header)
            
            # If no CREATE TABLE found, extract from INSERT/COPY statements
            if not headers:
                insert_headers = self._extract_headers_from_data_statements(file_path)
                for header in insert_headers:
                    if header not in seen:
                        headers.append(header)
                        seen.add(header)
                        
        except Exception as e:
            print(f"Error extracting headers from SQL file {file_path}: {str(e)}", file=sys.stderr)
        
        return headers
    
    def _extract_headers_from_create_statements(self, file_path: str) -> List[str]:
        """Extract headers from CREATE TABLE statements."""
        headers = []
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                # Read file in chunks to handle large files
                chunk_size = 1024 * 1024  # 1MB chunks
                content = ""
                
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    
                    content += chunk
                    
                    # Process complete statements
                    statements = content.split(';')
                    content = statements[-1]  # Keep incomplete statement for next iteration
                    
                    for statement in statements[:-1]:
                        headers.extend(self._parse_create_table_statement(statement + ';'))
                
                # Process final incomplete statement
                if content.strip():
                    headers.extend(self._parse_create_table_statement(content))
                    
        except Exception as e:
            print(f"Error reading CREATE statements from {file_path}: {str(e)}", file=sys.stderr)
        
        return headers
    
    def _parse_create_table_statement(self, statement: str) -> List[str]:
        """Parse a single CREATE TABLE statement to extract column names."""
        headers = []
        
        for pattern in self.create_table_patterns:
            match = re.search(pattern, statement, re.IGNORECASE | re.DOTALL)
            if match:
                table_name = match.group(1)
                column_defs = match.group(2)
                
                # Use sqlparse for better parsing
                try:
                    parsed = sqlparse.parse(f"CREATE TABLE {table_name} ({column_defs})")[0]
                    headers.extend(self._extract_columns_from_parsed_create(parsed))
                except:
                    # Fallback to regex parsing
                    headers.extend(self._extract_columns_with_regex(column_defs))
                
                break
        
        return headers
    
    def _extract_columns_from_parsed_create(self, parsed_statement) -> List[str]:
        """Extract column names from sqlparse parsed CREATE TABLE statement."""
        columns = []
        
        def extract_identifiers(token):
            if hasattr(token, 'tokens'):
                for subtoken in token.tokens:
                    if subtoken.ttype is sqlparse.tokens.Name:
                        # Clean up the identifier
                        name = str(subtoken).strip('`"[]')
                        if name and not name.upper() in ('PRIMARY', 'FOREIGN', 'UNIQUE', 'CHECK', 'CONSTRAINT', 'KEY', 'INDEX'):
                            columns.append(name)
                    elif hasattr(subtoken, 'tokens'):
                        extract_identifiers(subtoken)
        
        extract_identifiers(parsed_statement)
        return columns
    
    def _extract_columns_with_regex(self, column_defs: str) -> List[str]:
        """Fallback regex-based column extraction."""
        columns = []
        
        # Enhanced pattern to handle various identifier formats
        column_pattern = r'(?:^|,)\s*(?:`([^`]+)`|"([^"]+)"|(\w+))\s+(?:\w+|[^,\)]+)'
        matches = re.finditer(column_pattern, column_defs, re.MULTILINE)
        
        for match in matches:
            # Get the column name from whichever group matched
            col_name = match.group(1) or match.group(2) or match.group(3)
            if col_name and not col_name.upper() in ('PRIMARY', 'FOREIGN', 'UNIQUE', 'CHECK', 'CONSTRAINT', 'KEY'):
                columns.append(col_name)
        
        return columns
    
    def _extract_headers_from_data_statements(self, file_path: str) -> List[str]:
        """Extract headers from INSERT/COPY statements when CREATE TABLE is not available."""
        headers = []
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                # Sample first few statements to get column names
                sample_size = 10
                statements_found = 0
                
                for line in f:
                    if statements_found >= sample_size:
                        break
                    
                    # Check for INSERT patterns
                    for pattern in self.insert_patterns:
                        match = re.search(pattern, line, re.IGNORECASE)
                        if match:
                            columns_str = match.group(2)
                            columns = [col.strip().strip('`"[]') for col in columns_str.split(',')]
                            headers.extend(col for col in columns if col not in headers)
                            statements_found += 1
                            break
                    
                    # Check for COPY pattern (PostgreSQL)
                    copy_match = re.search(self.copy_pattern, line, re.IGNORECASE)
                    if copy_match:
                        columns_str = copy_match.group(2)
                        columns = [col.strip().strip('`"[]') for col in columns_str.split(',')]
                        headers.extend(col for col in columns if col not in headers)
                        statements_found += 1
                        
        except Exception as e:
            print(f"Error extracting headers from data statements in {file_path}: {str(e)}", file=sys.stderr)
        
        return headers
    
    def extract_data_from_sql(self, file_path: str, field_mapping: Dict[str, List[str]]) -> Iterator[Dict[str, Any]]:
        """
        Extract data from SQL file using robust parsing for various formats.
        
        Args:
            file_path: Path to the SQL file
            field_mapping: Mapping of normalized field types to lists of original headers
            
        Yields:
            Dictionaries containing extracted data with normalized field names
        """
        try:
            file_size = os.path.getsize(file_path)
            
            # Use streaming for large files
            if file_size > 100 * 1024 * 1024:  # > 100MB
                yield from self._stream_extract_data(file_path, field_mapping)
            else:
                yield from self._extract_data_standard(file_path, field_mapping)
                
        except Exception as e:
            print(f"Error extracting data from SQL file {file_path}: {str(e)}", file=sys.stderr)
    
    def _stream_extract_data(self, file_path: str, field_mapping: Dict[str, List[str]]) -> Iterator[Dict[str, Any]]:
        """Stream-based extraction for large SQL files."""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                statement_buffer = ""
                in_copy_data = False
                copy_columns = []
                
                # Progress bar for large files
                total_size = os.path.getsize(file_path)
                pbar = tqdm(total=total_size, desc=f"Processing {os.path.basename(file_path)}", unit='B', unit_scale=True)
                
                for line in f:
                    pbar.update(len(line.encode('utf-8')))
                    
                    # Handle PostgreSQL COPY data
                    if in_copy_data:
                        if line.strip() == '\\.':
                            in_copy_data = False
                            continue
                        
                        # Process COPY data line
                        yield from self._process_copy_data_line(line, copy_columns, field_mapping, file_path)
                        continue
                    
                    # Check for COPY statement start
                    copy_match = re.search(self.copy_pattern, line, re.IGNORECASE)
                    if copy_match:
                        in_copy_data = True
                        columns_str = copy_match.group(2)
                        copy_columns = [col.strip().strip('`"[]') for col in columns_str.split(',')]
                        continue
                    
                    # Accumulate statement
                    statement_buffer += line
                    
                    # Process complete statements (ending with semicolon)
                    if line.strip().endswith(';'):
                        yield from self._process_sql_statement(statement_buffer, field_mapping, file_path)
                        statement_buffer = ""
                
                # Process any remaining statement
                if statement_buffer.strip():
                    yield from self._process_sql_statement(statement_buffer, field_mapping, file_path)
                
                pbar.close()
                
        except Exception as e:
            print(f"Error in stream extraction from {file_path}: {str(e)}", file=sys.stderr)
    
    def _extract_data_standard(self, file_path: str, field_mapping: Dict[str, List[str]]) -> Iterator[Dict[str, Any]]:
        """Standard extraction for smaller SQL files."""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()
            
            # Split into statements
            statements = sqlparse.split(content)
            
            for statement in tqdm(statements, desc="Processing SQL statements", unit="stmt"):
                if statement.strip():
                    yield from self._process_sql_statement(statement, field_mapping, file_path)
                    
        except Exception as e:
            print(f"Error in standard extraction from {file_path}: {str(e)}", file=sys.stderr)
    
    def _process_sql_statement(self, statement: str, field_mapping: Dict[str, List[str]], file_path: str) -> Iterator[Dict[str, Any]]:
        """Process a single SQL statement to extract data."""
        # Try INSERT patterns
        for pattern in self.insert_patterns:
            matches = re.finditer(pattern, statement, re.IGNORECASE | re.DOTALL)
            for match in matches:
                table_name = match.group(1)
                columns_str = match.group(2)
                values_str = match.group(3)
                
                yield from self._process_insert_statement(columns_str, values_str, field_mapping, file_path)
    
    def _process_insert_statement(self, columns_str: str, values_str: str, field_mapping: Dict[str, List[str]], file_path: str) -> Iterator[Dict[str, Any]]:
        """Process INSERT statement values."""
        try:
            # Parse column names
            columns = [col.strip().strip('`"[]') for col in columns_str.split(',')]
            
            # Create column mapping
            column_mapping = {}
            for field_type, original_headers in field_mapping.items():
                for header in original_headers:
                    if header in columns:
                        column_idx = columns.index(header)
                        column_mapping[column_idx] = (field_type, header)
            
            # Parse values using improved parser
            value_sets = self._parse_values_robust(values_str)
            
            for value_set in value_sets:
                if len(value_set) == len(columns):
                    record = self._create_record_from_values(value_set, column_mapping, file_path)
                    if record:
                        yield record
                        
        except Exception as e:
            print(f"Error processing INSERT statement: {str(e)}", file=sys.stderr)
    
    def _parse_values_robust(self, values_str: str) -> List[List[str]]:
        """Robust parsing of VALUES clause handling various formats."""
        value_sets = []
        
        try:
            # Use sqlparse for better parsing
            parsed = sqlparse.parse(f"SELECT {values_str}")[0]
            
            # Extract value sets from parsed tokens
            current_set = []
            in_parentheses = False
            current_value = ""
            
            def extract_values(token):
                nonlocal current_set, in_parentheses, current_value, value_sets
                
                if hasattr(token, 'tokens'):
                    for subtoken in token.tokens:
                        extract_values(subtoken)
                else:
                    token_str = str(token).strip()
                    
                    if token_str == '(':
                        in_parentheses = True
                        if current_set:
                            value_sets.append(current_set)
                            current_set = []
                    elif token_str == ')':
                        if current_value.strip():
                            current_set.append(self._clean_sql_value(current_value))
                            current_value = ""
                        in_parentheses = False
                    elif token_str == ',' and in_parentheses:
                        if current_value.strip():
                            current_set.append(self._clean_sql_value(current_value))
                            current_value = ""
                    elif in_parentheses and token_str not in ('(', ')', ','):
                        current_value += token_str + " "
            
            extract_values(parsed)
            
            # Add final set if exists
            if current_set:
                value_sets.append(current_set)
                
        except:
            # Fallback to manual parsing
            value_sets = self._parse_values_manual(values_str)
        
        return value_sets
    
    def _parse_values_manual(self, values_str: str) -> List[List[str]]:
        """Manual fallback parsing for VALUES clause."""
        value_sets = []
        current_set = []
        in_quotes = False
        quote_char = None
        current_value = ""
        paren_depth = 0
        
        i = 0
        while i < len(values_str):
            char = values_str[i]
            
            if not in_quotes:
                if char in ("'", '"'):
                    in_quotes = True
                    quote_char = char
                    current_value += char
                elif char == '(':
                    paren_depth += 1
                    if paren_depth == 1:
                        # Start of new value set
                        if current_set:
                            value_sets.append(current_set)
                            current_set = []
                elif char == ')':
                    paren_depth -= 1
                    if paren_depth == 0:
                        # End of value set
                        if current_value.strip():
                            current_set.append(self._clean_sql_value(current_value))
                            current_value = ""
                elif char == ',' and paren_depth == 1:
                    # Value separator within parentheses
                    if current_value.strip():
                        current_set.append(self._clean_sql_value(current_value))
                        current_value = ""
                else:
                    current_value += char
            else:
                current_value += char
                if char == quote_char:
                    # Check for escaped quote
                    if i + 1 < len(values_str) and values_str[i + 1] == quote_char:
                        current_value += quote_char
                        i += 1  # Skip next quote
                    else:
                        in_quotes = False
                        quote_char = None
            
            i += 1
        
        # Add final set
        if current_set:
            value_sets.append(current_set)
        
        return value_sets
    
    def _clean_sql_value(self, value: str) -> str:
        """Clean and normalize SQL value."""
        value = value.strip()
        
        # Remove surrounding quotes
        if len(value) >= 2:
            if (value.startswith("'") and value.endswith("'")) or \
               (value.startswith('"') and value.endswith('"')):
                value = value[1:-1]
        
        # Handle escaped quotes
        value = value.replace("''", "'").replace('""', '"')
        
        return value
    
    def _process_copy_data_line(self, line: str, columns: List[str], field_mapping: Dict[str, List[str]], file_path: str) -> Iterator[Dict[str, Any]]:
        """Process a line of PostgreSQL COPY data."""
        try:
            # Split by tabs (PostgreSQL COPY default)
            values = line.strip().split('\t')
            
            if len(values) == len(columns):
                # Create column mapping
                column_mapping = {}
                for field_type, original_headers in field_mapping.items():
                    for header in original_headers:
                        if header in columns:
                            column_idx = columns.index(header)
                            column_mapping[column_idx] = (field_type, header)
                
                record = self._create_record_from_values(values, column_mapping, file_path)
                if record:
                    yield record
                    
        except Exception as e:
            print(f"Error processing COPY data line: {str(e)}", file=sys.stderr)
    
    def _create_record_from_values(self, values: List[str], column_mapping: Dict[int, Tuple[str, str]], file_path: str) -> Optional[Dict[str, Any]]:
        """Create a record from parsed values and column mapping."""
        from .field_normalizer import validate_field_value
        
        record = {}
        
        for col_idx, (field_type, original_header) in column_mapping.items():
            if col_idx < len(values):
                value = values[col_idx].strip()
                
                # Skip NULL values
                if not value or value.upper() in ('NULL', 'N/A', 'NONE', '', '\\N'):
                    continue
                
                # Validate field value
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
        
        # Only return records that have at least one target field
        if record:
            record['_source_file'] = os.path.basename(file_path)
            return record
        
        return None


# Global instance for use by other modules
sql_parser = SQLParser() 