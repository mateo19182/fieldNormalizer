"""
AI-based mapping validator for validating and correcting existing field mappings.
This module uses AI to validate and correct existing mappings.json files to ensure they make sense
and properly match the target fields and data description.
"""
import json
import os
import aiohttp
import asyncio
from typing import Dict, List, Any, Optional, Tuple
import dotenv
from tqdm import tqdm
import datetime

dotenv.load_dotenv(".env")


class AIMappingValidator:
    """
    Uses AI to validate and correct existing field mappings from a mappings.json file.
    """
    
    def __init__(self, target_fields: List[str], data_description: str = "", debug: bool = True):
        """
        Initialize the AI Mapping Validator.
        
        Args:
            target_fields: List of target fields that mappings should map to
            data_description: User-provided description of the data they care about
            debug: Whether to enable debug logging to file
        """
        self.target_fields = target_fields
        self.data_description = data_description
        self.original_mappings: Dict[str, Any] = {}
        self.corrected_mappings: Dict[str, Any] = {}
        self._session: Optional[aiohttp.ClientSession] = None
        self.debug = debug
        self.debug_log = []
        
    def _log_debug(self, message: str, data: Any = None):
        """Add debug message to log."""
        if self.debug:
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            log_entry = f"[{timestamp}] {message}"
            if data is not None:
                log_entry += f"\n{json.dumps(data, indent=2)}"
            self.debug_log.append(log_entry)
            
    def save_debug_log(self, output_path: str = "validator_debug.log"):
        """Save debug log to file."""
        if self.debug and self.debug_log:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write("AI Mapping Validator Debug Log\n")
                f.write("=" * 50 + "\n\n")
                for entry in self.debug_log:
                    f.write(entry + "\n\n")
        
    async def __aenter__(self):
        """Async context manager entry."""
        self._session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self._session:
            await self._session.close()
            self._session = None
    
    def load_mappings(self, mappings_path: str) -> None:
        """
        Load existing field mappings from a JSON file.
        
        Args:
            mappings_path: Path to the mappings.json file
        """
        with open(mappings_path, 'r', encoding='utf-8') as f:
            self.original_mappings = json.load(f)
        
        self._log_debug(f"Loaded mappings from {mappings_path} with {len(self.original_mappings)} entries")
    
    async def validate_and_correct_mappings(self) -> None:
        """
        Validate and correct all mappings using AI in a single request.
        """
        if not self._session:
            self._session = aiohttp.ClientSession()
        
        # Prepare the mappings for validation
        # Handle both AI and traditional mapping formats
        mappings_to_validate = {}
        
        if "mappings" in self.original_mappings:
            # Traditional format: {"mappings": {file_path: {header: target_field}}, "target_fields": [...]}
            mappings_to_validate = self.original_mappings["mappings"]
        else:
            # AI format: {filename: {"_full_path": path, "mappings": {header: target_field}}}
            for filename, file_data in self.original_mappings.items():
                if isinstance(file_data, dict) and "mappings" in file_data:
                    full_path = file_data.get("_full_path", filename)
                    mappings_to_validate[full_path] = file_data["mappings"]
        
        # self._log_debug(f"Extracted {len(mappings_to_validate)} files to validate")
        
        if not mappings_to_validate:
            print("No mappings found in the provided file.")
            self._log_debug("ERROR: No mappings found to validate")
            return
        
        print(f"Validating and correcting all mappings for {len(mappings_to_validate)} files...")
        
        try:
            self.corrected_mappings = await self._validate_all_mappings(mappings_to_validate)
            self._log_debug(f"Validation completed. Got {len(self.corrected_mappings)} corrected files")
            
            # Add diff to debug log
            diff_summary = self._generate_diff_summary()
            self._log_debug("MAPPING CHANGES SUMMARY", diff_summary)
            
        except Exception as e:
            error_msg = f"Error validating mappings: {str(e)}"
            print(error_msg)
            self._log_debug(f"ERROR: {error_msg}")
    
    async def _validate_all_mappings(self, all_mappings: Dict[str, Dict[str, str]]) -> Dict[str, Dict[str, str]]:
        """
        Use AI to validate and correct all mappings in batches.
        
        Args:
            all_mappings: Dictionary mapping file paths to their field mappings
            
        Returns:
            Corrected mappings dictionary
        """
        # Split mappings into batches of 20
        batch_size = 20
        mapping_items = list(all_mappings.items())
        batches = [mapping_items[i:i + batch_size] for i in range(0, len(mapping_items), batch_size)]
        
        # self._log_debug(f"Processing {len(all_mappings)} files in {len(batches)} batches of up to {batch_size} files each")
        
        corrected_mappings = {}
        
        # Process each batch
        for batch_idx, batch in enumerate(batches):
            batch_mappings = dict(batch)
            print(f"Processing batch {batch_idx + 1}/{len(batches)} ({len(batch_mappings)} files)...")
            
            try:
                batch_corrected = await self._validate_mapping_batch(batch_mappings, batch_idx + 1)
                corrected_mappings.update(batch_corrected)
                self._log_debug(f"Batch {batch_idx + 1} completed successfully with {len(batch_corrected)} files")
            except Exception as e:
                error_msg = f"Error processing batch {batch_idx + 1}: {str(e)}"
                print(f"Warning: {error_msg}, keeping original mappings for this batch")
                self._log_debug(f"BATCH ERROR: {error_msg}")
                
                # Keep original mappings for this batch
                corrected_mappings.update(batch_mappings)
        
        self._log_debug(f"All batches processed. Final result: {len(corrected_mappings)} files")
        return corrected_mappings
    
    async def _validate_mapping_batch(self, batch_mappings: Dict[str, Dict[str, str]], batch_num: int) -> Dict[str, Dict[str, str]]:
        """
        Validate a single batch of mappings.
        
        Args:
            batch_mappings: Dictionary of file paths to mappings for this batch
            batch_num: Batch number for logging
            
        Returns:
            Corrected mappings for this batch
        """
        api_key = os.getenv("OPENROUTER_API_KEY")
        if not api_key:
            error_msg = f"OPENROUTER_API_KEY not found in environment for batch {batch_num}"
            print(f"Error: {error_msg}")
            self._log_debug(f"ERROR: {error_msg}")
            return batch_mappings  # Return original mappings if no API key
        
        # Prepare the mappings data for the prompt
        mappings_for_prompt = {}
        for file_path, file_mappings in batch_mappings.items():
            filename = os.path.basename(file_path)
            mappings_for_prompt[filename] = file_mappings
        
        # self._log_debug(f"Batch {batch_num}: prepared {len(mappings_for_prompt)} files for AI validation")
        
        # Prepare the prompt for validation
        user_description = f"\n\nUser description of relevant data: {self.data_description}" if self.data_description else ""
        
        prompt = f"""Review and correct the following field mappings to ensure they are accurate and logical.

Target fields available: {", ".join(self.target_fields)}{user_description}

Current mappings for batch {batch_num}:
{json.dumps(mappings_for_prompt, indent=2)}

Task: Return the corrected mappings for all files as a JSON object. Only include mappings that make logical sense and use the available target fields. Remove any incorrect mappings. Keep the same file structure.

Return only a JSON object in this exact format:
{{
    "filename1.csv": {{
        "source_field1": "target_field1",
        "source_field2": "target_field2"
    }},
    "filename2.json": {{
        "source_field3": "target_field3",
        "source_field4": "target_field4"
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
                {"role": "system", "content": "You are a data mapping expert. Return only valid JSON mappings with no additional text or explanations."},
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 2000
        }
        
        # self._log_debug(f"Batch {batch_num}: sending request to AI")
        
        try:
            async with self._session.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers=headers_dict,
                json=data
            ) as response:
                response.raise_for_status()
                result = await response.json()
                content = result['choices'][0]['message']['content']
            
            # self._log_debug(f"Batch {batch_num}: received AI response")
            
            # Try to parse the JSON object from the response
            try:
                # First, try to find JSON object in the response if it's not a clean JSON
                import re
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    content = json_match.group(0)
                
                corrected_mappings_by_filename = json.loads(content)
                
                # self._log_debug(f"Batch {batch_num}: successfully parsed JSON response with {len(corrected_mappings_by_filename)} files")
                
                # Convert back to full file paths and validate target fields
                corrected_mappings = {}
                for filename, file_mappings in corrected_mappings_by_filename.items():
                    # Find the original full path for this filename
                    full_path = None
                    for original_path in batch_mappings.keys():
                        if os.path.basename(original_path) == filename:
                            full_path = original_path
                            break
                    
                    if not full_path:
                        warning_msg = f"Could not find original path for {filename} in batch {batch_num}, skipping"
                        print(f"Warning: {warning_msg}")
                        self._log_debug(f"WARNING: {warning_msg}")
                        continue
                    
                    # Validate that mappings only use allowed target fields
                    validated_mappings = {}
                    for source, target in file_mappings.items():
                        if target in self.target_fields:
                            validated_mappings[source] = target
                        else:
                            warning_msg = f"Target field '{target}' not in allowed target fields, skipping mapping for '{source}' in {filename} (batch {batch_num})"
                            print(f"Warning: {warning_msg}")
                            self._log_debug(f"WARNING: Invalid target field '{target}' for '{source}' in {filename}")
                    
                    if validated_mappings:
                        corrected_mappings[full_path] = validated_mappings
                    else:
                        self._log_debug(f"No valid mappings for {filename} in batch {batch_num} after validation")
                
                # self._log_debug(f"Batch {batch_num}: validation completed with {len(corrected_mappings)} valid files")
                return corrected_mappings
                
            except json.JSONDecodeError as e:
                error_msg = f"Error parsing AI response as JSON for batch {batch_num}: {str(e)}"
                print(f"Warning: {error_msg}, keeping original mappings for this batch")
                self._log_debug(f"JSON DECODE ERROR for batch {batch_num}: {error_msg}")
                
                # Return original mappings for this batch
                return batch_mappings
                
        except aiohttp.ClientError as e:
            error_msg = f"API request error for batch {batch_num}: {str(e)}"
            print(f"Warning: {error_msg}, keeping original mappings for this batch")
            self._log_debug(f"API ERROR for batch {batch_num}: {error_msg}")
            
            # Return original mappings for this batch
            return batch_mappings
    
    def save_corrected_mappings(self, output_path: str) -> None:
        """
        Save the corrected mappings to a JSON file in the same format as the original.
        
        Args:
            output_path: Path to save the corrected mappings file
        """
        # Determine the original format and maintain it
        if "mappings" in self.original_mappings:
            # Traditional format: {"mappings": {file_path: {header: target_field}}, "target_fields": [...]}
            output_data = {
                "target_fields": self.target_fields,
                "mappings": {}
            }
            
            for file_path, mappings in self.corrected_mappings.items():
                if mappings and len(mappings) >= 2:  # Only include files with at least 2 mappings
                    output_data["mappings"][file_path] = mappings
        else:
            # AI format: {filename: {"_full_path": path, "mappings": {header: target_field}}}
            output_data = {}
            
            for file_path, mappings in self.corrected_mappings.items():
                if mappings and len(mappings) >= 2:  # Only include files with at least 2 mappings
                    basename = os.path.basename(file_path)
                    output_data[basename] = {
                        "_full_path": file_path,
                        "mappings": mappings
                    }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2)
    
    def get_changes_diff(self) -> List[Dict[str, Any]]:
        """
        Get a list of changes made during validation.
        
        Returns:
            List of dictionaries describing changes made to each file
        """
        changes = []
        
        # Get original mappings in consistent format
        original_mappings = {}
        if "mappings" in self.original_mappings:
            original_mappings = self.original_mappings["mappings"]
        else:
            for filename, file_data in self.original_mappings.items():
                if isinstance(file_data, dict) and "mappings" in file_data:
                    full_path = file_data.get("_full_path", filename)
                    original_mappings[full_path] = file_data["mappings"]
        
        # Compare original vs corrected
        all_files = set(original_mappings.keys()) | set(self.corrected_mappings.keys())
        
        for file_path in all_files:
            original = original_mappings.get(file_path, {})
            corrected = self.corrected_mappings.get(file_path, {})
            
            file_changes = {
                "file": os.path.basename(file_path),
                "full_path": file_path,
                "added": {},
                "removed": {},
                "changed": {},
                "unchanged": {}
            }
            
            # Find added, removed, and changed mappings
            all_keys = set(original.keys()) | set(corrected.keys())
            
            for key in all_keys:
                if key in original and key in corrected:
                    if original[key] != corrected[key]:
                        file_changes["changed"][key] = {
                            "from": original[key],
                            "to": corrected[key]
                        }
                    else:
                        file_changes["unchanged"][key] = corrected[key]
                elif key in corrected:
                    file_changes["added"][key] = corrected[key]
                elif key in original:
                    file_changes["removed"][key] = original[key]
            
            # Only include files that have changes
            if file_changes["added"] or file_changes["removed"] or file_changes["changed"]:
                changes.append(file_changes)
        
        return changes

    def _generate_diff_summary(self) -> Dict[str, Any]:
        """Generate a summary of changes for debug logging."""
        changes = self.get_changes_diff()
        
        if not changes:
            return {"message": "No changes were made to the mappings"}
        
        summary = {
            "files_with_changes": len(changes),
            "total_added": sum(len(f["added"]) for f in changes),
            "total_removed": sum(len(f["removed"]) for f in changes),
            "total_changed": sum(len(f["changed"]) for f in changes),
            "detailed_changes": []
        }
        
        for file_change in changes:
            file_detail = {
                "file": file_change["file"],
                "added": file_change["added"],
                "removed": file_change["removed"], 
                "changed": file_change["changed"],
                "unchanged_count": len(file_change["unchanged"])
            }
            summary["detailed_changes"].append(file_detail)
            
        return summary


async def validate_mappings_with_ai(mappings_path: str, target_fields: List[str], data_description: str = "") -> AIMappingValidator:
    """
    Validate and correct existing field mappings using AI.
    
    Args:
        mappings_path: Path to the existing mappings.json file
        target_fields: List of target fields that mappings should map to
        data_description: User-provided description of the data they care about
        
    Returns:
        AIMappingValidator instance with validation results
    """
    async with AIMappingValidator(target_fields, data_description) as validator:
        validator.load_mappings(mappings_path)
        await validator.validate_and_correct_mappings()
        return validator


def format_changes_diff(validator: AIMappingValidator) -> str:
    """
    Format a human-readable diff of the changes made.
    
    Args:
        validator: AIMappingValidator instance with validation results
        
    Returns:
        Formatted string representation of the changes
    """
    changes = validator.get_changes_diff()
    
    if not changes:
        return "No changes were made to the mappings."
    
    lines = [
        "Mapping Changes Summary",
        "=" * 50,
        ""
    ]
    
    for file_change in changes:
        lines.append(f"File: {file_change['file']}")
        lines.append("-" * 30)
        
        # Added mappings
        if file_change["added"]:
            lines.append("  Added:")
            for key, value in file_change["added"].items():
                lines.append(f"    + {key} → {value}")
        
        # Removed mappings
        if file_change["removed"]:
            lines.append("  Removed:")
            for key, value in file_change["removed"].items():
                lines.append(f"    - {key} → {value}")
        
        # Changed mappings
        if file_change["changed"]:
            lines.append("  Changed:")
            for key, change in file_change["changed"].items():
                lines.append(f"    ~ {key}: {change['from']} → {change['to']}")
        
        # Unchanged count
        unchanged_count = len(file_change["unchanged"])
        if unchanged_count > 0:
            lines.append(f"  Unchanged: {unchanged_count} mappings")
        
        lines.append("")
    
    # Summary
    total_files = len(changes)
    total_added = sum(len(f["added"]) for f in changes)
    total_removed = sum(len(f["removed"]) for f in changes)
    total_changed = sum(len(f["changed"]) for f in changes)
    
    lines.extend([
        "Summary:",
        f"  Files with changes: {total_files}",
        f"  Total added: {total_added}",
        f"  Total removed: {total_removed}",
        f"  Total changed: {total_changed}",
    ])
    
    return "\n".join(lines) 