# Field Normalizer

A powerful command-line tool for extracting, normalizing, and processing structured data from various file formats (CSV, JSON, SQL). The tool specializes in identifying and categorizing fields related to personal information such as names, email addresses, phone numbers, and physical addresses.

## Overview

Field Normalizer provides a two-step workflow:
1. **Analyze**: Extract and normalize headers from data files, categorizing them by field type and creating field mappings
2. **Extract**: Generate a JSONL file containing the actual data from fields matching specific categories

This tool is designed to handle large files and process data in a memory-efficient manner, making it suitable for batch processing of substantial datasets. It also provides smart features like deduplication, merging records with the same email, and ignoring NULL values.

## Installation

### Prerequisites
- Python 3.8 or higher
- pip (Python package installer)

### Install from Source
```bash
git clone https://github.com/yourusername/fieldNormalizer.git
cd fieldNormalizer
pip install -e .
```

Create a .env file in the root directory of the project with the following content if you want to infer headers with AI for files missing them:

```bash
OPENROUTER_API_KEY=your_api_key_here
```

## Usage

The tool now uses a two-command structure for clarity and ease of use:

### Step 1: Analyze Command
```bash
field-normalizer analyze [OPTIONS] PATHS...
```

#### Analyze Command Options
- `--file-types` - Specify file types to process (default: csv, json, sql)
- `--max-files, -n` - Maximum number of files to process per directory
- `--output, -o` - Output file for analysis report (default: stdout)
- `--no-normalize` - Disable field normalization (enabled by default)
- `--no-variations` - Disable showing field variations (enabled by default)
- `--mappings-output` - Output file for field mappings (default: mappings.json)

### Step 2: Extract Command
```bash
field-normalizer extract [OPTIONS] [PATHS...]
```

#### Extract Command Options
- `--mappings` - Field mappings file (default: mappings.json)
- `--output, -o` - Output file for extracted data (default: extracted_data.jsonl)
- `--batch-size` - Batch size for writing records (default: 1000)

### Examples

#### Analyze Files and Create Mappings
```bash
# Analyze all supported files in a directory (with default normalization)
field-normalizer analyze /path/to/data

# Process specific file types with a limit
field-normalizer analyze /path/to/data --file-types csv json --max-files 50

# Analyze files and save results to a file
field-normalizer analyze /path/to/data --output analysis_report.txt

# Disable normalization or variations
field-normalizer analyze /path/to/data --no-normalize --output raw_headers.txt
```

#### Extract Data Using Mappings
```bash
# Extract data using mappings.json (uses paths from the mappings file)
field-normalizer extract

# Extract data to a specific output file
field-normalizer extract --output contacts.jsonl

# Use a different mappings file
field-normalizer extract --mappings custom_mappings.json

# Extract from specific paths (overriding mappings file paths)
field-normalizer extract /path/to/data --output contacts.jsonl
```

## Internal Architecture

### Core Components

The Field Normalizer is built around four main components:

1. **CLI Interface** (`cli.py`): 
   - Handles command-line arguments and orchestrates the workflow
   - Manages file discovery and processing
   - Implements the two-command structure (analyze and extract)

2. **Header Extractors** (`extractors.py`):
   - Contains file-specific logic for extracting headers from different file types
   - Supports CSV, JSON, and SQL file formats
   - Handles special cases like inferring headers when they're missing

3. **Field Normalizer** (`field_normalizer.py`):
   - Provides algorithms for normalizing field names
   - Contains pattern-matching logic to categorize fields
   - Groups similar fields by type (name, email, phone, address)

4. **Field Mapper** (`field_mapper.py`):
   - Creates and manages mappings between original headers and normalized categories
   - Handles saving/loading mappings to/from JSON files
   - Provides inverse mappings for data extraction

5. **Data Extractor** (`data_extractor.py`):
   - Extracts data from files based on field mappings
   - Handles multiple fields mapping to the same category
   - Merges records with the same email address
   - Deduplicates records and ignores NULL values

### Data Processing Workflow

#### Step 1: Analyze (Header Analysis & Mapping)
1. **File Discovery**: The tool scans specified directories for matching file types
2. **Header Extraction**: Each file is processed to extract its headers/field names
3. **Normalization**: Headers are normalized (lowercase, spaces standardized)
4. **Categorization**: Headers are matched against pattern libraries to identify field types
5. **Mapping Creation**: Mappings between original headers and normalized categories are created
6. **Mapping Storage**: Mappings are saved to a JSON file for manual review/editing

#### Step 2: Extract (Data Extraction)
1. **Mapping Loading**: Field mappings are loaded from the JSON file
2. **Stream Processing**: Files are processed one record at a time to minimize memory usage
3. **Field Mapping**: Original headers are mapped to their normalized categories
4. **Data Extraction**: For each record, relevant fields are extracted based on mappings
5. **Value Handling**: Multiple fields mapping to the same category are combined
6. **Record Merging**: Records with the same email address are merged
7. **Deduplication**: Duplicate records are removed
8. **NULL Handling**: NULL values are ignored
9. **JSONL Output**: Records are written to output in JSONL format in batches

### Memory-Efficient Processing

The tool is designed to handle large datasets by:

1. **Two-Pass Approach**: Separating header analysis from data extraction for better control
2. **Streaming Approach**: Reading and processing files line by line instead of loading entire files in memory
3. **Generator Functions**: Using Python generators to yield records incrementally
4. **Batched Writing**: Accumulating a configurable number of records before writing to the output file
5. **Field Mapping Cache**: Reusing field classification results to avoid redundant processing

### Smart Data Processing Features

1. **Multiple Field Handling**: Multiple fields mapping to the same category (e.g., telephone1 and telephone2 both map to phone) are handled intelligently
2. **NULL Value Filtering**: NULL, N/A, and empty values are automatically ignored
3. **Deduplication**: Duplicate records are removed to ensure data quality
4. **Email-Based Merging**: Records with the same email address are merged to consolidate information
5. **Source Tracking**: Each record includes information about its source file

This architecture allows Field Normalizer to handle data files of virtually any size with a fixed memory footprint while providing smart data processing capabilities.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.