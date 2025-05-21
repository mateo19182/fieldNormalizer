"""
Field mapper for creating and managing field mappings.
This module handles the creation, saving, and loading of field mappings
between original file headers and normalized output fields.
"""
import json
import os
import re
from typing import Dict, List, Set, Any, Optional
from src.field_normalizer import get_field_type, normalize_field_name, FIELD_PATTERNS

# Default target field categories
DEFAULT_TARGET_FIELDS = ['firstname', 'lastname', 'email', 'phone', 'address', 'username']

class FieldMapper:
    """
    Manages mappings between original file headers and normalized output fields.
    Handles cases where multiple input fields map to the same output field.
    """
    
    def __init__(self, target_fields: List[str], custom_patterns: Optional[Dict[str, List[str]]] = None):
        """
        Initialize the Field Mapper with target fields.
        
        Args:
            target_fields: List of target fields to map source fields to
            custom_patterns: Optional dictionary of custom field patterns to use instead of defaults
        """
        self.target_fields = target_fields
        self.file_mappings: Dict[str, Dict[str, str]] = {}
        self.field_patterns = custom_patterns or FIELD_PATTERNS
        
    def get_field_type(self, field_name: str) -> str:
        """
        Determine the field type based on the field name patterns.
        
        Args:
            field_name: The field name to analyze
            
        Returns:
            Field type from target_fields or 'other' if no match found
        """
        normalized = normalize_field_name(field_name)
        
        for field_type, patterns in self.field_patterns.items():
            if field_type not in self.target_fields:
                continue
            for pattern in patterns:
                if re.search(pattern, normalized, re.IGNORECASE):
                    return field_type
        
        return 'other'
    
    def create_mappings(self, file_metadata: List[Dict[str, Any]]) -> None:
        """
        Create mappings for each file based on its headers.
        
        Args:
            file_metadata: List of dictionaries containing file metadata including headers
        """
        for metadata in file_metadata:
            file_path = metadata['path']
            headers = metadata['headers']
            
            # Create mappings for this file
            mappings = {}
            for header in headers:
                field_type = self.get_field_type(header)
                if field_type != 'other':
                    mappings[header] = field_type
            
            # Only store mappings if we found at least 2 fields
            if len(mappings) >= 2:
                self.file_mappings[file_path] = mappings
    
    def save_mappings(self, output_file: str) -> None:
        """
        Save mappings to a JSON file.
        
        Args:
            output_file: Path to the output file
        """
        with open(output_file, 'w') as f:
            json.dump({
                'target_fields': self.target_fields,
                'field_patterns': self.field_patterns,
                'mappings': self.file_mappings
            }, f, indent=2)
    
    def load_mappings(self, input_file: str) -> None:
        """
        Load mappings from a JSON file.
        
        Args:
            input_file: Path to the input file
        """
        with open(input_file, 'r') as f:
            data = json.load(f)
            self.target_fields = data.get('target_fields', [])
            self.field_patterns = data.get('field_patterns', FIELD_PATTERNS)
            self.file_mappings = data.get('mappings', {})
    
    def get_mappings(self) -> Dict[str, Dict[str, str]]:
        """
        Get the current mappings.
        
        Returns:
            Dictionary mapping file paths to field mappings
        """
        return self.file_mappings
    
    def get_inverse_mapping(self, file_path: str) -> Dict[str, List[str]]:
        """
        Get the inverse mapping (normalized field type to list of original headers).
        This handles multiple original fields mapping to the same normalized field.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Dictionary mapping normalized field types to lists of original headers
        """
        mapping = self.get_mappings().get(file_path, {})
        inverse_mapping: Dict[str, List[str]] = {field: [] for field in self.target_fields}
        
        for header, field_type in mapping.items():
            if field_type in inverse_mapping:
                inverse_mapping[field_type].append(header)
        
        return inverse_mapping
    
    def get_all_mappings(self) -> Dict[str, Dict[str, str]]:
        """
        Get all file mappings.
        
        Returns:
            Dictionary mapping file paths to their header mappings
        """
        return self.file_mappings
    
    def get_all_file_paths(self) -> List[str]:
        """
        Get all file paths from the mappings.
        
        Returns:
            List of file paths found in the mappings
        """
        return list(self.file_mappings.keys())
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the mappings.
        
        Returns:
            Dictionary with mapping statistics
        """
        stats = {
            "total_files": len(self.file_mappings),
            "field_counts": {field: 0 for field in self.target_fields},
            "unmapped_headers": 0
        }
        
        for file_path, mappings in self.file_mappings.items():
            # Count fields by type
            for header, field_type in mappings.items():
                if field_type in stats["field_counts"]:
                    stats["field_counts"][field_type] += 1
        
        return stats


def create_field_mappings(file_metadata: List[Dict[str, Any]], target_fields: List[str], custom_patterns: Optional[Dict[str, List[str]]] = None) -> FieldMapper:
    """
    Create field mappings from file metadata.
    
    Args:
        file_metadata: List of dictionaries containing file metadata including headers
        target_fields: List of target fields to map to
        custom_patterns: Optional dictionary of custom field patterns to use
        
    Returns:
        FieldMapper instance with the mappings
    """
    mapper = FieldMapper(target_fields, custom_patterns)
    mapper.create_mappings(file_metadata)
    return mapper


def format_mappings_report(mapper: FieldMapper) -> str:
    """
    Format a human-readable report of the field mappings.
    
    Args:
        mapper: FieldMapper instance with mappings
        
    Returns:
        Formatted string representation of the mappings
    """
    lines = [
        "Field Mappings Report",
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
    #     basename = os.path.basename(file_path)
    #     lines.append(f"\n{basename} ({file_path})")
    #     lines.append("-" * len(basename))
        
    #     # Group by normalized field type
    #     inverse_mapping = mapper.get_inverse_mapping(file_path)
        
    #     for field_type in mapper.target_fields:
    #         headers = inverse_mapping.get(field_type, [])
    #         if headers:
    #             lines.append(f"{field_type}: {', '.join(headers)}")
    
    return "\n".join(lines)
