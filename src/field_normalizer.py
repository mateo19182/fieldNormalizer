"""
Field normalization and grouping logic.
"""
import re
from typing import Any, Dict, List, Set, Tuple

# Define field patterns for different types of fields
FIELD_PATTERNS = {
    'email': [
        r'email',
        r'e.?mail',
        r'email.?address',
        r'indirizzo.?email',
        r'user.?email',
        r'contact.?email',
        r'email.?contact',
        r'^e.?mail$',
        r'mail',
        r'^e$',
    ],
    'phone': [
        r'phone',
        r'tel',
        r'telephone',
        r'mobile',
        r'cell',
        r'cellulare',
        r'telefono',
        r'phone.?number',
        r'tel.?number',
        r'mobile.?number',
        r'cell.?number',
        r'telefono',
        r'telefono.?cellulare',
        r'number',
        r'numero',
        r'numero.?de.?telefono',
        r'numero.?di.?telefono',
        r'contact.?number',
        r'contact.?phone',
        r'contact.?tel',
        r'contact.?telephone',
        r'contact.?mobile',
    ],
    'firstname': [
        r'^name$',
        r'^nome$',
        r'first.?name',
        r'firstname',
        r'display.?name',
        r'full.?name',
        r'nombre',
        r'given.?name',
        r'givenname',
        r'first',
        r'first.?name',
    ],
    'lastname': [
        r'cognome',
        r'last.?name',
        r'lastname',
        r'surname',
        r'apellido',
    ],
    'username': [
        r'^user.?name$',
        r'^username$',
    ],
    'address': [
        # General address patterns
        r'address',
        r'indirizzo',
        r'street',
        r'via',
        r'strada',
        r'road',
        r'avenue',
        r'viale',
        r'corso',
        r'piazza',
        r'piazzale',
        
        # Address components
        r'street.?address',
        r'street.?name',
        r'street.?number',
        r'building',
        r'edificio',
        r'civico',
        r'civic',
        r'number',
        r'numero',
        
        # City/town
        r'city',
        r'città',
        r'citta',
        r'town',
        r'paese',
        r'località',
        r'localita',
        
        # Region/State/Province
        r'region',
        r'regione',
        r'state',
        r'stato',
        r'province',
        r'provincia',
        r'county',
        r'contea',
        
        # Postal/Zip code
        r'zip',
        r'zip.?code',
        r'postal',
        r'postal.?code',
        r'cap',
        r'codice.?postale',
        r'posta',
        
        # Country
        r'country',
        r'paese',
        r'nazione',
        
        # Special address types
        r'shipping',
        r'billing',
        r'delivery',
        r'spedizione',
        r'fatturazione',
        r'fattura',
        r'residenza',
        r'residence',
        
        # Compound patterns
        r'street',
        r'address.?line',
        r'indirizzo',
        r'via',
        r'strada',
        r'civico',
        
        # Additional international patterns
        r'p\.?o\.?\s*box',
        r'post\s*office\s*box',
        r'apt',
        r'apartment',
        r'appartamento',
        r'floor',
        r'piano'
    ]
}

def normalize_field_name(field_name: str) -> str:
    """
    Normalize a field name by converting to lowercase and removing non-alphanumeric characters.
    
    Args:
        field_name: The field name to normalize
        
    Returns:
        Normalized field name
    """
    if not field_name:
        return ""
    # Convert to lowercase and replace common separators with spaces
    normalized = field_name.lower()
    normalized = re.sub(r'[^a-z0-9]', ' ', normalized)
    # Replace multiple spaces with a single space
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    return normalized

def get_field_type(field_name: str) -> str:
    """
    Determine the field type based on the field name patterns.
    
    Args:
        field_name: The field name to analyze
        
    Returns:
        Field type ('email', 'phone', 'name', 'address') or 'other' if no match found
    """
    normalized = normalize_field_name(field_name)
    
    for field_type, patterns in FIELD_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, normalized, re.IGNORECASE):
                return field_type
    
    return 'other'

def group_fields(headers: List[str]) -> Dict[str, Set[str]]:
    """
    Group field names by their type.
    
    Args:
        headers: List of field names to group
        
    Returns:
        Dictionary mapping field types to sets of field names
    """
    field_groups = {
        'email': set(),
        'phone': set(),
        'firstname': set(),
        'lastname': set(),
        'username': set(),
        'address': set(),
        'other': set()
    }
    
    for header in headers:
        field_type = get_field_type(header)
        field_groups[field_type].add(header)
    
    return field_groups

def validate_field_value(field_type: str, value: str) -> Any:
    """
    Validate and clean field values based on their type.
    
    Args:
        field_type: Type of the field ('email', 'phone', 'name', etc.)
        value: The value to validate
        
    Returns:
        Validated value or None if the value should be discarded
    """
    if not value:
        return None
        
    # Trim whitespace
    value = value.strip()
    
    # Discard names with more than 50 characters
    if field_type == 'firstname' and len(value) > 50:
        return None
        
    # Discard phone numbers that contain alphabetic characters (words)
    if field_type == 'phone':
        # Check if the phone number contains alphabetic characters
        # Allow common phone number characters: digits, +, -, (, ), spaces, dots
        if re.search(r'[a-zA-Z]', value):
            return None
            
    return value


def analyze_field_variations(headers: List[str], header_stats: Dict[str, Dict[str, Any]] = None) -> Dict[str, Dict[str, Dict[str, List[str]]]]:
    """
    Analyze and group field variations by type, including file sources.
    
    Args:
        headers: List of field names to analyze
        header_stats: Optional dictionary containing header statistics with file sources
        
    Returns:
        Dictionary mapping field types to dictionaries of patterns and matching fields with sources
    """
    field_groups = group_fields(headers)
    result = {}
    
    for field_type, fields in field_groups.items():
        if field_type == 'other' or not fields:
            continue
            
        pattern_matches = {}
        
        # For each pattern, find all fields that match it
        for pattern in FIELD_PATTERNS[field_type]:
            matched_fields = {}
            for field in fields:
                normalized = normalize_field_name(field)
                if re.search(pattern, normalized, re.IGNORECASE):
                    if header_stats and field in header_stats:
                        matched_fields[field] = header_stats[field]['files']
                    else:
                        matched_fields[field] = ['unknown']
            
            if matched_fields:
                pattern_matches[pattern] = matched_fields
        
        # Add any unmatched fields to an 'other' category
        matched_fields = set()
        for fields_dict in pattern_matches.values():
            matched_fields.update(fields_dict.keys())
        
        if header_stats:
            unmatched_fields = {}
            for field in fields:
                if field not in matched_fields and field in header_stats:
                    unmatched_fields[field] = header_stats[field]['files']
            if unmatched_fields:
                pattern_matches['other'] = unmatched_fields
        
        result[field_type] = pattern_matches
    
    return result
