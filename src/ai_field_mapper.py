"""
AI-based field mapper for creating and managing field mappings.
This module uses AI to map source fields to arbitrary target fields specified by the user.
"""
import json
import os
import random
import requests
import aiohttp
import asyncio
from typing import Dict, List, Set, Any, Optional, Tuple
import dotenv
import csv
from tqdm import tqdm
from src.field_normalizer import normalize_field_name

dotenv.load_dotenv(".env")

class AIFieldMapper:
    """
    Uses AI to map source fields to arbitrary target fields specified by the user.
    """
    
    def __init__(self, target_fields: List[str], data_description: str = ""):
        """
        Initialize the AI Field Mapper with user-specified target fields.
        
        Args:
            target_fields: List of target fields to map source fields to
            data_description: User-provided description of the data they care about
        """
        self.target_fields = target_fields
        self.data_description = data_description
        self.file_mappings: Dict[str, Dict[str, str]] = {}
        self.api_responses: Dict[str, Any] = {}  # Store API responses for logging
        self._session: Optional[aiohttp.ClientSession] = None
        
    async def __aenter__(self):
        """Async context manager entry."""
        self._session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self._session:
            await self._session.close()
            self._session = None
    
    async def build_mappings(self, file_metadata: List[Dict[str, Any]]) -> None:
        """
        Build field mappings for all files based on their headers using AI.
        
        Args:
            file_metadata: List of dicts with file metadata including headers
        """
        if not self._session:
            self._session = aiohttp.ClientSession()
        
        # Create progress bar
        pbar = tqdm(total=len(file_metadata), desc="Processing files")
        
        try:
            tasks = []
            for file_info in file_metadata:
                file_path = file_info['path']
                headers = file_info['headers']
                
                # Get sample data from the file
                sample_data, sample_format = self._get_sample_data(file_path, headers)
                
                # Create mapping for this file
                self.file_mappings[file_path] = {}
                
                # Create task for mapping headers
                task = asyncio.create_task(
                    self._map_headers_with_ai(headers, file_path, sample_data, sample_format)
                )
                tasks.append((file_path, task))
            
            # Process all tasks and update progress
            for file_path, task in tasks:
                try:
                    header_mappings, is_relevant = await task
                    
                    # Only store mappings if the file is relevant
                    if is_relevant:
                        for header, target_field in header_mappings.items():
                            if target_field in self.target_fields:
                                self.file_mappings[file_path][header] = target_field
                    else:
                        # Create an empty mapping to indicate the file was processed but deemed irrelevant
                        print(f"File {os.path.basename(file_path)} was deemed irrelevant to the target fields and will be skipped.")
                except Exception as e:
                    print(f"Error processing {file_path}: {str(e)}")
                finally:
                    pbar.update(1)
        finally:
            pbar.close()
    
    def _get_sample_data(self, file_path: str, headers: List[str], max_samples: int = 2) -> Tuple[Any, str]:
        """
        Extract sample data from a file to help determine relevance.
        Returns the data in its native format along with format type.
        
        Args:
            file_path: Path to the file
            headers: List of headers in the file
            max_samples: Maximum number of sample rows to extract
            
        Returns:
            Tuple of (sample_data, format_type)
            - sample_data: Sample data in appropriate format for the file type
            - format_type: String indicating the format ('csv', 'json', etc.)
        """
        _, ext = os.path.splitext(file_path)
        ext = ext.lower().lstrip('.')
        
        try:
            if ext == 'csv':
                with open(file_path, 'r', newline='', encoding='utf-8', errors='replace') as f:
                    # Try to detect dialect
                    sample_text = f.read(4096)
                    f.seek(0)
                    
                    try:
                        dialect = csv.Sniffer().sniff(sample_text)
                        has_header = csv.Sniffer().has_header(sample_text)
                    except:
                        dialect = csv.excel
                        has_header = True
                    
                    reader = csv.reader(f, dialect)
                    
                    # Skip header if present
                    if has_header:
                        next(reader)
                    
                    # Get sample rows as arrays (native CSV format)
                    rows = []
                    for i, row in enumerate(reader):
                        if i >= max_samples:
                            break
                        rows.append(row)
                    
                    # Also create a list of dictionaries for internal use
                    sample_dicts = []
                    for row in rows:
                        if len(row) >= min(len(headers), 1):
                            sample = {}
                            for i, header in enumerate(headers):
                                if i < len(row):
                                    sample[header] = row[i]
                            if sample:
                                sample_dicts.append(sample)
                    
                    # Return both the raw rows and header mapping for CSV
                    return {
                        "headers": headers,
                        "rows": rows,
                        "dict_format": sample_dicts
                    }, "csv"
            
            elif ext == 'json':
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if isinstance(data, list) and data:
                    # Get random samples from the list
                    if len(data) > max_samples:
                        sample_items = random.sample(data, max_samples)
                    else:
                        sample_items = data[:max_samples]
                    
                    # For JSON, the native format is already dictionaries
                    # But we still want to ensure we have the headers we're looking for
                    sample_dicts = []
                    for item in sample_items:
                        if isinstance(item, dict):
                            sample = {}
                            for header in headers:
                                if header in item:
                                    sample[header] = item[header]
                            if sample:
                                sample_dicts.append(sample)
                    
                    return {
                        "raw_data": sample_items,
                        "filtered_data": sample_dicts
                    }, "json"
                
                elif isinstance(data, dict):
                    # Single object
                    sample = {}
                    for header in headers:
                        if header in data:
                            sample[header] = data[header]
                    
                    return {
                        "raw_data": data,
                        "filtered_data": [sample] if sample else []
                    }, "json"
            
            elif ext == 'sql':
                # For SQL files, use the robust SQL parser to get sample data
                from .sql_parser import sql_parser
                
                try:
                    # Get a small sample of data using the SQL parser
                    sample_records = []
                    record_count = 0
                    
                    # Create a dummy field mapping to extract any data
                    dummy_mapping = {header: [header] for header in headers[:10]}  # Limit to first 10 headers
                    
                    for record in sql_parser.extract_data_from_sql(file_path, dummy_mapping):
                        sample_records.append(record)
                        record_count += 1
                        if record_count >= 3:  # Get up to 3 sample records
                            break
                    
                    return {
                        "headers": headers,
                        "sample_records": sample_records,
                        "record_count": record_count,
                        "file_type": "sql"
                    }, "sql"
                    
                except Exception as e:
                    # Fallback to basic file info
                    return {
                        "headers": headers,
                        "error": f"Could not extract sample data: {str(e)}",
                        "file_type": "sql"
                    }, "sql"
        
        except Exception as e:
            print(f"Warning: Could not extract sample data from {file_path}: {str(e)}")
        
        # If no samples were extracted successfully, create a sample with empty values
        return {
            "error": "Could not extract sample data in native format",
            "headers": headers,
            "empty_sample": {header: "" for header in headers[:min(5, len(headers))]}
        }, "unknown"
    
    async def _map_headers_with_ai(self, headers: List[str], file_path: str, sample_data: Any, sample_format: str) -> Tuple[Dict[str, str], bool]:
        """
        Use AI to map headers to target fields and determine if the file is relevant.
        
        Args:
            headers: List of headers to map
            file_path: Path to the file
            sample_data: Sample data from the file in native format
            sample_format: Format of the sample data ('csv', 'json', etc.)
            
        Returns:
            Tuple of (header_mappings, is_relevant)
                header_mappings: Dictionary mapping original headers to target fields
                is_relevant: Boolean indicating if the file is relevant to the target fields
        """
        api_key = os.getenv("OPENROUTER_API_KEY")
        if not api_key:
            print(f"Error: OPENROUTER_API_KEY not found in environment. Cannot process {file_path}.")
            return {}, False
        
        # Format sample data for display based on file format
        sample_display = self._format_sample_for_display(sample_data, sample_format)
        
        # Prepare the prompt for the API
        user_description = f"\n\nUser description of relevant data: {self.data_description}" if self.data_description else ""
        
        prompt = f"""Analyze this file and determine if it contains relevant data for the specified target fields.

File name: {os.path.basename(file_path)}
File format: {sample_format.upper()}
Source fields: {", ".join(headers)}
Target fields: {", ".join(self.target_fields)}{user_description}

Here are some sample rows from the file in its native {sample_format.upper()} format:
{sample_display}

Task 1: Determine if this file is relevant to the target fields. A file is relevant if it contains data that can be mapped to at least 2 of the target fields.

Task 2: For each source field, determine which target field it should map to. If a source field doesn't clearly map to any target field, don't map it.

Return your answer as a JSON object with the following structure:
{{
    "is_relevant": true/false,
    "reason": "Short and concise explanation of why the file is relevant or not",
    "mappings": {{
        "source_field1": "target_field1",
        "source_field2": "target_field2",
        ...
    }}
}}

JSON response:
"""
        
        # Make the API request
        headers_dict = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        data = {
            "model": "google/gemini-2.0-flash-001",
            "messages": [
                {"role": "system", "content": "You are a data mapping assistant that helps map source fields to target fields and determine file relevance."},
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 800
        }
        
        try:
            async with self._session.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers=headers_dict,
                json=data
            ) as response:
                response.raise_for_status()
                result = await response.json()
                content = result['choices'][0]['message']['content']
            
            # Store the API response for logging
            self.api_responses[os.path.basename(file_path)] = {
                "prompt": prompt,
                "response": content,
                "headers": headers,
                "sample_data": sample_data,
                "sample_format": sample_format,
                "sample_display": sample_display
            }
            
            # Try to parse the JSON object from the response
            try:
                # First, try to find JSON object in the response if it's not a clean JSON
                import re
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    content = json_match.group(0)
                
                response_data = json.loads(content)
                
                # Extract relevance determination
                is_relevant = response_data.get("is_relevant", False)
                reason = response_data.get("reason", "No reason provided")
                
                # If the file is deemed irrelevant, log the reason
                if not is_relevant:
                    print(f"File {os.path.basename(file_path)} deemed irrelevant: {reason}")
                
                # Extract mappings
                mappings = response_data.get("mappings", {})
                
                # Validate the mappings
                validated_mappings = {}
                for source, target in mappings.items():
                    if source in headers and target in self.target_fields:
                        validated_mappings[source] = target
                
                # Check if we have at least 2 valid mappings (if the file is deemed relevant)
                if is_relevant and len(validated_mappings) < 2:
                    print(f"Warning: File {os.path.basename(file_path)} was marked as relevant but has fewer than 2 valid mappings. It will be skipped.")
                    is_relevant = False
                
                return validated_mappings, is_relevant
                
            except json.JSONDecodeError:
                print(f"Error parsing API response as JSON for {file_path}: {content}")
                return {}, False
                
        except aiohttp.ClientError as e:
            print(f"API request error for {file_path}: {e}")
            return {}, False
    
    def _format_sample_for_display(self, sample_data: Any, sample_format: str) -> str:
        """
        Format sample data for display in the prompt based on file format.
        
        Args:
            sample_data: Sample data in native format
            sample_format: Format of the sample data
            
        Returns:
            Formatted string representation of the sample data
        """
        if sample_format == "csv":
            # Format as CSV table
            result = []
            
            # Add header row
            if "headers" in sample_data and sample_data["headers"]:
                result.append("Header row: " + ", ".join(sample_data["headers"]))
            if "rows" in sample_data and sample_data["rows"]:
                for i, row in enumerate(sample_data["rows"]):
                    result.append(f"{i+1}: " + ", ".join(str(cell).replace('\n', ' ').replace('\r', '') for cell in row))
            return "\n".join(result)
        
        elif sample_format == "json":
            # Format JSON sample nicely
            if "raw_data" in sample_data:
                if isinstance(sample_data["raw_data"], list):
                    return json.dumps(sample_data["raw_data"][:3], indent=2)
                else:
                    return json.dumps(sample_data["raw_data"], indent=2)
            elif "filtered_data" in sample_data:
                return json.dumps(sample_data["filtered_data"], indent=2)
            else:
                return "Could not format JSON sample data."
        
        elif sample_format == "sql":
            # Format SQL sample
            result = []
            
            if "table" in sample_data:
                result.append(f"Table: {sample_data['table']}")
            
            if "columns" in sample_data:
                result.append(f"Columns: {', '.join(sample_data['columns'])}")
            
            if "sample_sql" in sample_data:
                result.append("\nSample SQL:")
                result.append(sample_data["sample_sql"])
            
            return "\n".join(result)
        
        else:
            # Default format as JSON for unknown types
            return json.dumps(sample_data, indent=2)
    
    def save_mappings(self, output_path: str) -> None:
        """
        Save the field mappings to a JSON file.
        
        Args:
            output_path: Path to save the mappings file
        """
        # Convert file paths to relative paths if possible
        formatted_mappings = {}
        
        for file_path, mappings in self.file_mappings.items():
            # Skip files with empty mappings (deemed irrelevant)
            if not mappings:
                continue
                
            # Skip files with only one mapping
            if len(mappings) < 2:
                continue
                
            # Use basename for cleaner output, but keep full path as a comment
            basename = os.path.basename(file_path)
            formatted_mappings[basename] = {
                "_full_path": file_path,  # Keep the full path as metadata
                "mappings": mappings
            }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(formatted_mappings, f, indent=2)
    
    def save_analysis_report(self, output_path: str) -> None:
        """
        Save a detailed analysis report including API calls.
        
        Args:
            output_path: Path to save the analysis report
        """
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("# Field Normalizer AI Analysis Report\n\n")
            
            for file_name, api_data in self.api_responses.items():
                f.write(f"## File: {file_name}\n\n")
                
                # Write headers
                f.write("### Headers\n")
                f.write("```\n")
                f.write(", ".join(api_data["headers"]))
                f.write("\n```\n\n")
                
                # Write sample data in native format
                f.write(f"### Sample Data ({api_data.get('sample_format', 'unknown').upper()})\n")
                f.write("```\n")
                if "sample_display" in api_data:
                    f.write(api_data["sample_display"])
                else:
                    f.write(json.dumps(api_data["sample_data"], indent=2))
                f.write("\n```\n\n")
                
                # Write prompt
                f.write("### API Prompt\n")
                f.write("```\n")
                f.write(api_data["prompt"])
                f.write("\n```\n\n")
                
                # Write response
                f.write("### API Response\n")
                f.write("```\n")
                f.write(api_data["response"])
                f.write("\n```\n\n")
                
                # Write mappings if available
                if file_name in [os.path.basename(path) for path in self.file_mappings]:
                    file_path = next(path for path in self.file_mappings if os.path.basename(path) == file_name)
                    mappings = self.file_mappings[file_path]
                    
                    f.write("### Final Mappings\n")
                    f.write("```json\n")
                    f.write(json.dumps(mappings, indent=2))
                    f.write("\n```\n\n")
                
                f.write("---\n\n")
    
    def load_mappings(self, input_path: str) -> None:
        """
        Load field mappings from a JSON file.
        
        Args:
            input_path: Path to the mappings file
        """
        with open(input_path, 'r', encoding='utf-8') as f:
            formatted_mappings = json.load(f)
        
        # Convert back to our internal format
        self.file_mappings = {}
        for basename, data in formatted_mappings.items():
            full_path = data.get("_full_path", basename)
            self.file_mappings[full_path] = data["mappings"]
            
            # Collect all unique target fields from the loaded mappings
            unique_targets = set()
            for _, target in data["mappings"].items():
                unique_targets.add(target)
            
            # Update target_fields to include all fields found in the mappings
            self.target_fields = list(set(self.target_fields) | unique_targets)
    
    def get_field_mapping(self, file_path: str) -> Dict[str, str]:
        """
        Get the field mapping for a specific file.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Dictionary mapping original headers to target fields
        """
        return self.file_mappings.get(file_path, {})
    
    def get_inverse_mapping(self, file_path: str) -> Dict[str, List[str]]:
        """
        Get the inverse mapping (target field to list of original headers).
        This handles multiple original fields mapping to the same target field.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Dictionary mapping target fields to lists of original headers
        """
        mapping = self.get_field_mapping(file_path)
        inverse_mapping: Dict[str, List[str]] = {field: [] for field in self.target_fields}
        
        for header, target_field in mapping.items():
            if target_field in inverse_mapping:
                inverse_mapping[target_field].append(header)
        
        return inverse_mapping
    
    def get_all_mappings(self) -> Dict[str, Dict[str, str]]:
        """
        Get all file mappings.
        
        Returns:
            Dictionary mapping file paths to their header mappings
        """
        return {k: v for k, v in self.file_mappings.items() if v}  # Filter out empty mappings
    
    def get_all_file_paths(self) -> List[str]:
        """
        Get all file paths from the mappings.
        
        Returns:
            List of file paths found in the mappings
        """
        return [path for path, mappings in self.file_mappings.items() if mappings]  # Filter out empty mappings
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the mappings.
        
        Returns:
            Dictionary with mapping statistics
        """
        # Filter out files with empty mappings or fewer than 2 mappings
        valid_mappings = {path: mappings for path, mappings in self.file_mappings.items() 
                         if mappings and len(mappings) >= 2}
        
        stats = {
            "total_files": len(valid_mappings),
            "field_counts": {field: 0 for field in self.target_fields}
        }
        
        for file_path, mappings in valid_mappings.items():
            # Count fields by type
            for header, target_field in mappings.items():
                if target_field in stats["field_counts"]:
                    stats["field_counts"][target_field] += 1
        
        return stats


async def create_ai_field_mappings(file_metadata: List[Dict[str, Any]], target_fields: List[str], data_description: str = "") -> AIFieldMapper:
    """
    Create AI-based field mappings from file metadata.
    
    Args:
        file_metadata: List of dicts with file metadata including headers
        target_fields: List of target fields to map to
        data_description: User-provided description of the data they care about
        
    Returns:
        AIFieldMapper instance with the mappings
    """
    async with AIFieldMapper(target_fields, data_description) as mapper:
        await mapper.build_mappings(file_metadata)
        return mapper


def format_ai_mappings_report(mapper: AIFieldMapper) -> str:
    """
    Format a human-readable report of the AI field mappings.
    
    Args:
        mapper: AIFieldMapper instance with mappings
        
    Returns:
        Formatted string representation of the mappings
    """
    lines = [
        "AI Field Mappings Report",
        "=" * 80,
        ""
    ]
    
    stats = mapper.get_stats()
    lines.append(f"Total files with mappings: {stats['total_files']}")
    lines.append("Field counts:")
    for field, count in stats['field_counts'].items():
        lines.append(f"  - {field}: {count}")
    lines.append("")
    
    # # Show mappings by file
    # lines.append("Mappings by File:")
    # lines.append("=" * 80)
    
    # for file_path, mappings in mapper.get_all_mappings().items():
    #     # Skip files with fewer than 2 mappings
    #     if len(mappings) < 2:
    #         continue
            
    #     basename = os.path.basename(file_path)
    #     lines.append(f"\n{basename} ({file_path})")
    #     lines.append("-" * len(basename))
        
    #     # Group by target field
    #     inverse_mapping = mapper.get_inverse_mapping(file_path)
        
    #     for target_field in mapper.target_fields:
    #         headers = inverse_mapping.get(target_field, [])
    #         if headers:
    #             lines.append(f"{target_field}: {', '.join(headers)}")
    
    # Add detailed mapping outcomes section
    lines.append("\n\nDetailed Mapping Outcomes:")
    lines.append("=" * 80)
    
    # Track different categories of files
    mapped_files = []
    too_few_mappings = []
    ai_rejected = []
    
    # Process each file that was analyzed
    for file_name, api_data in mapper.api_responses.items():
        try:
            # First try to find JSON object in the response if it's not a clean JSON
            import re
            content = api_data["response"]
            json_match = re.search(r'\{.*\}', content, re.DOTALL)
            if json_match:
                content = json_match.group(0)
            
            response_data = json.loads(content)
            is_relevant = response_data.get("is_relevant", False)
            reason = response_data.get("reason", "No reason provided")
            
            # Get the file path if it exists in mappings
            file_path = next((path for path in mapper.file_mappings if os.path.basename(path) == file_name), None)
            
            if not is_relevant:
                # File was rejected by AI
                ai_rejected.append((file_name, reason))
            elif file_path:
                mappings = mapper.file_mappings[file_path]
                if len(mappings) >= 2:
                    mapped_files.append((file_name, mappings))
                else:
                    too_few_mappings.append((file_name, reason))
            else:
                # This shouldn't happen, but just in case
                too_few_mappings.append((file_name, "No mappings found despite being marked as relevant"))
                
        except (json.JSONDecodeError, KeyError) as e:
            # If we can't parse the response, try to extract the reason from the raw response
            content = api_data["response"]
            # Look for common patterns in the response
            if "deemed irrelevant" in content.lower():
                # Try to extract the reason after "deemed irrelevant:"
                match = re.search(r'deemed irrelevant:\s*(.*?)(?:\n|$)', content, re.IGNORECASE)
                if match:
                    reason = match.group(1).strip()
                    ai_rejected.append((file_name, reason))
                else:
                    ai_rejected.append((file_name, "File was deemed irrelevant but reason could not be extracted"))
            else:
                ai_rejected.append((file_name, f"Error parsing response: {str(e)}"))
    
    # Report successfully mapped files
    if mapped_files:
        lines.append("\nSuccessfully Mapped Files:")
        lines.append("-" * 40)
        for file_name, mappings in mapped_files:
            lines.append(f"\n{file_name}")
            lines.append(f"  Mapped {len(mappings)} fields:")
            for source, target in mappings.items():
                lines.append(f"    - {source} -> {target}")
    
    # Report files with too few mappings
    if too_few_mappings:
        lines.append("\nFiles with Too Few Mappings (< 2):")
        lines.append("-" * 40)
        for file_name, reason in too_few_mappings:
            lines.append(f"\n{file_name}")
            lines.append(f"  Reason: {reason}")
    
    # Report files rejected by AI
    if ai_rejected:
        lines.append("\nFiles Rejected by AI:")
        lines.append("-" * 40)
        for file_name, reason in ai_rejected:
            lines.append(f"\n{file_name}")
            lines.append(f"  Reason: {reason}")
    
    # Add summary statistics
    lines.append("\nMapping Outcomes Summary:")
    lines.append("-" * 40)
    lines.append(f"Total files processed: {len(mapper.api_responses)}")
    lines.append(f"Successfully mapped: {len(mapped_files)}")
    lines.append(f"Too few mappings: {len(too_few_mappings)}")
    lines.append(f"Rejected by AI: {len(ai_rejected)}")
    
    return "\n".join(lines) 