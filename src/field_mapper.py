"""
Field mapper for creating and managing field mappings.
This module handles the creation, saving, and loading of field mappings
between original file headers and normalized output fields.
"""
import json
import os
from typing import Dict, List, Set, Any, Optional
from src.field_normalizer import get_field_type, normalize_field_name

# Default target field categories
DEFAULT_TARGET_FIELDS = ['name', 'lastname', 'email', 'phone', 'address', 'username']

class FieldMapper:
    """
    Manages mappings between original file headers and normalized output fields.
    Handles cases where multiple input fields map to the same output field.
    """
    
    def __init__(self, target_fields: List[str] = None):
        """
        Initialize the Field Mapper.
        
        Args:
            target_fields: Optional list of target fields to use (defaults to DEFAULT_TARGET_FIELDS)
        """
        self.target_fields = target_fields or DEFAULT_TARGET_FIELDS
        self.file_mappings: Dict[str, Dict[str, str]] = {}
        
    def build_mappings(self, file_metadata: List[Dict[str, Any]]) -> None:
        """
        Build field mappings for all files based on their headers.
        
        Args:
            file_metadata: List of dicts with file metadata including headers
        """
        for file_info in file_metadata:
            file_path = file_info['path']
            headers = file_info['headers']
            
            # Create mapping for this file
            self.file_mappings[file_path] = {}
            
            # Map each header to a normalized field type
            for header in headers:
                field_type = get_field_type(header)
                
                # Only include fields we care about (those in target_fields)
                if field_type in self.target_fields:
                    self.file_mappings[file_path][header] = field_type
    
    def save_mappings(self, output_path: str) -> None:
        """
        Save the field mappings to a JSON file.
        
        Args:
            output_path: Path to save the mappings file
        """
        # Convert file paths to relative paths if possible
        formatted_mappings = {}
        
        for file_path, mappings in self.file_mappings.items():
            # Use basename for cleaner output, but keep full path as a comment
            basename = os.path.basename(file_path)
            formatted_mappings[basename] = {
                "_full_path": file_path,  # Keep the full path as metadata
                "mappings": mappings
            }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(formatted_mappings, f, indent=2)
    
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
            Dictionary mapping original headers to normalized field types
        """
        return self.file_mappings.get(file_path, {})
    
    def get_inverse_mapping(self, file_path: str) -> Dict[str, List[str]]:
        """
        Get the inverse mapping (normalized field type to list of original headers).
        This handles multiple original fields mapping to the same normalized field.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Dictionary mapping normalized field types to lists of original headers
        """
        mapping = self.get_field_mapping(file_path)
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


def create_field_mappings(file_metadata: List[Dict[str, Any]], target_fields: List[str] = None) -> FieldMapper:
    """
    Create field mappings from file metadata.
    
    Args:
        file_metadata: List of dicts with file metadata including headers
        target_fields: Optional list of target fields to use (defaults to DEFAULT_TARGET_FIELDS)
        
    Returns:
        FieldMapper instance with the mappings
    """
    mapper = FieldMapper(target_fields)
    mapper.build_mappings(file_metadata)
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
    
    # Show mappings by file
    lines.append("Mappings by File:")
    lines.append("=" * 80)
    
    for file_path, mappings in mapper.get_all_mappings().items():
        basename = os.path.basename(file_path)
        lines.append(f"\n{basename} ({file_path})")
        lines.append("-" * len(basename))
        
        # Group by normalized field type
        inverse_mapping = mapper.get_inverse_mapping(file_path)
        
        for field_type in mapper.target_fields:
            headers = inverse_mapping.get(field_type, [])
            if headers:
                lines.append(f"{field_type}: {', '.join(headers)}")
    
    return "\n".join(lines)
