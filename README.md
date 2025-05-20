# Field Normalizer

A powerful command-line tool for extracting, normalizing, and processing structured data from various file formats (CSV, JSON, SQL). The tool specializes in identifying and categorizing fields related to personal information such as names, email addresses, phone numbers, and physical addresses.

## Overview

Field Normalizer provides a two-step workflow:
1. **Analyze**: Extract and normalize headers from data files, categorizing them by field type and creating field mappings
2. **Extract**: Generate a JSONL file containing the actual data from fields matching specific categories

This tool is designed to handle large files and process data in a memory-efficient manner, making it suitable for batch processing of substantial datasets. It also provides smart features like deduplication, merging records with the same email, and ignoring NULL values. Files with only one mapping are automatically ignored during the extract phase to ensure data quality.

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

Create a .env file in the root directory of the project with the following content if you want to use AI-based field mapping:

```bash
OPENROUTER_API_KEY=your_api_key_here
```

## Usage

The tool offers three command options for flexibility:

### Option 1: Two-Step Workflow (Analyze then Extract)

#### Step 1: Analyze Command
```bash
field-normalizer analyze [OPTIONS] PATHS...
```

##### Analyze Command Options
- `--file-types` - Specify file types to process (default: csv, json, sql)
- `--max-files, -n` - Maximum number of files to process per directory
- `--output, -o` - Output file for analysis report (default: stdout)
- `--no-normalize` - Disable field normalization (enabled by default)
- `--no-variations` - Disable showing field variations (enabled by default)
- `--mappings-output` - Output file for field mappings (default: mappings.json)
- `--use-ai` - Use AI to create field mappings (requires OPENROUTER_API_KEY in .env file)
- `--target-fields` - Custom target fields to map to (default: name, email, phone, address)
- `--data-description` - Description of the data you are looking for (helps AI determine file relevance)

#### Step 2: Extract Command
```bash
field-normalizer extract [OPTIONS]
```

##### Extract Command Options
- `--mappings` - Field mappings file (default: mappings.json)
- `--output, -o` - Output file for extracted data (default: extracted_data.jsonl)
- `--batch-size` - Batch size for writing records (default: 1000)
- `--group-by-email` - Group records by email address (disabled by default)
- `--use-ai` - Use AI-based field mappings (requires OPENROUTER_API_KEY in .env file)

### Option 2: One-Step Workflow (Process)

```bash
field-normalizer process [OPTIONS] PATHS...
```

#### Process Command Options
- `--file-types` - Specify file types to process (default: csv, json, sql)
- `--max-files, -n` - Maximum number of files to process per directory
- `--analysis-output` - Output file for analysis report (default: no file output)
- `--mappings-output` - Output file for field mappings (default: mappings.json)
- `--extract-output, -o` - Output file for extracted data (default: extracted_data.jsonl)
- `--batch-size` - Batch size for writing records (default: 1000)
- `--no-normalize` - Disable field normalization (enabled by default)
- `--no-variations` - Disable showing field variations (enabled by default)
- `--group-by-email` - Group records by email address (disabled by default)
- `--use-ai` - Use AI to create field mappings (requires OPENROUTER_API_KEY in .env file)
- `--target-fields` - Custom target fields to map to (default: name, email, phone, address)
- `--data-description` - Description of the data you are looking for (helps AI determine file relevance)

### Examples

#### Two-Step Workflow

##### Step 1: Analyze Files and Create Mappings
```bash
# Analyze all supported files in a directory (with default normalization)
field-normalizer analyze /path/to/data

# Process specific file types with a limit
field-normalizer analyze /path/to/data --file-types csv json --max-files 50

# Analyze files and save results to a file
field-normalizer analyze /path/to/data --output analysis_report.txt

# Disable normalization or variations
field-normalizer analyze /path/to/data --no-normalize --output raw_headers.txt

# Use AI-based field mapping with default target fields
field-normalizer analyze /path/to/data --use-ai

# Use AI-based field mapping with custom target fields
field-normalizer analyze /path/to/data --use-ai --target-fields name email phone address company title

# Use AI-based field mapping with data description to improve relevance detection
field-normalizer analyze /path/to/data --use-ai --data-description "Looking for customer contact data from e-commerce transactions"
```

##### Step 2: Extract Data Using Mappings
```bash
# Extract data using mappings.json (automatically uses paths from the mappings file)
field-normalizer extract

# Extract data to a specific output file
field-normalizer extract --output contacts.jsonl

# Use a different mappings file
field-normalizer extract --mappings custom_mappings.json

# Extract data using AI-based field mappings
field-normalizer extract --use-ai
```

#### One-Step Workflow
```bash
# Process all files in one step (analyze and extract)
field-normalizer process /path/to/data

# Specify output files for each step
field-normalizer process /path/to/data --analysis-output report.txt --extract-output contacts.jsonl

# Process specific file types with a limit
field-normalizer process /path/to/data --file-types csv json --max-files 50

# Use AI-based field mapping with custom target fields and data description
field-normalizer process /path/to/data --use-ai --target-fields name email phone address company title --data-description "Looking for customer support tickets with contact information"
```

## Field Mapping Strategies

Field Normalizer supports two strategies for mapping source fields to target fields:

### 1. Regex-Based Mapping (Default)

The default strategy uses regular expressions to match field names against predefined patterns. This approach is fast and works without any external dependencies.

- **Pros**: Works offline, consistent results, no API costs
- **Cons**: Limited to predefined patterns, may miss complex or unusual field names

### 2. AI-Based Mapping

When enabled with the `--use-ai` flag, Field Normalizer uses AI to map source fields to target fields. This approach can handle a wider variety of field names and is especially useful when working with arbitrary target fields.

- **Pros**: More accurate mapping, handles arbitrary target fields, understands context
- **Cons**: Requires an API key, may incur costs, requires internet connection

The AI-based mapping strategy analyzes:
1. **Field names** - Maps source fields to target fields based on semantic understanding
2. **Sample data** - Examines actual values to determine field relevance
3. **File context** - Uses filename and data patterns to determine if a file is relevant
4. **User description** - Uses your description of the data you're looking for to improve relevance detection

Files with fewer than 2 mappings or deemed irrelevant to your target data are automatically skipped.

To use AI-based mapping, you must provide an OpenRouter API key in a `.env` file in the project root:

```bash
OPENROUTER_API_KEY=your_api_key_here
```

## Custom Target Fields

By default, Field Normalizer maps source fields to four predefined target fields: name, email, phone, and address. However, you can specify custom target fields using the `--target-fields` option:

```bash
field-normalizer analyze /path/to/data --target-fields name email phone address company title department salary
```

This is especially powerful when combined with AI-based mapping, as the AI can intelligently map source fields to any arbitrary target fields you specify.

## Specifying Data Description

When using AI-based mapping, you can provide a description of the data you're looking for to help the AI determine file relevance:

```bash
field-normalizer analyze /path/to/data --use-ai --data-description "Looking for customer support tickets with contact information"
```

This description helps the AI understand:
- What type of data you're interested in
- The context or domain of the data
- Specific attributes that make a file relevant to your needs

This can significantly improve the accuracy of file relevance determination and field mapping.

## Configuration File

Field Normalizer supports loading configuration from a JSON file. This allows you to specify target fields, data descriptions, and custom field patterns in a reusable format. To use a configuration file, specify it with the `--config` option:

```bash
field-normalizer analyze /path/to/data --config config.json
```

### Configuration File Format

The configuration file should be a JSON file with the following structure:

```json
{
    "target_fields": [
        "name",
        "email",
        "phone",
        "address",
        "company",
        "title"
    ],
    "data_description": "Looking for customer contact information from various data sources",
    "field_patterns": {
        "name": [
            "^name$",
            "first.?name",
            "full.?name"
        ],
        "email": [
            "email",
            "e.?mail"
        ],
        "phone": [
            "phone",
            "tel",
            "mobile"
        ],
        "address": [
            "address",
            "street",
            "city"
        ],
        "company": [
            "company",
            "organization",
            "employer"
        ],
        "title": [
            "title",
            "job.?title",
            "position"
        ]
    }
}
```

#### Configuration Options

- `target_fields`: List of target fields to map source fields to
- `data_description`: Description of the data you are looking for (used by AI-based mapping)
- `field_patterns`: Dictionary mapping field types to lists of regex patterns for matching field names

When using a configuration file:
1. If `--use-ai` is specified, the AI will use the `target_fields` and `data_description` from the config file
2. If not using AI, the tool will use the `field_patterns` from the config file for regex-based matching
3. Command line arguments take precedence over config file settings

### Examples

#### Using Configuration File with AI
```bash
# Use AI with settings from config file
field-normalizer analyze /path/to/data --config config.json --use-ai

# Override config file settings with command line arguments
field-normalizer analyze /path/to/data --config config.json --use-ai --target-fields name email phone
```

#### Using Configuration File without AI
```bash
# Use regex patterns from config file
field-normalizer analyze /path/to/data --config config.json

# Use custom patterns for specific fields
field-normalizer analyze /path/to/data --config custom_patterns.json
```

## Internal Architecture

### Core Components

The Field Normalizer is built around five main components:

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

4. **Field Mapper** (`field_mapper.py` and `ai_field_mapper.py`):
   - Creates and manages mappings between original headers and normalized categories
   - Handles saving/loading mappings to/from JSON files
   - Provides inverse mappings for data extraction
   - Supports both regex-based and AI-based mapping strategies
   - Allows for arbitrary target fields
   - Analyzes sample data for relevance determination (AI-based)

5. **Data Extractor** (`data_extractor.py`):
   - Extracts data from files based on field mappings
   - Handles multiple fields mapping to the same category
   - Merges records with the same email address
   - Deduplicates records and ignores NULL values

### Data Processing Workflow

#### Step 1: Analyze (Header Analysis & Mapping)
1. **File Discovery**: The tool scans specified directories for matching file types
2. **Header Extraction**: Each file is processed to extract its headers/field names
3. **Sample Data Extraction**: For AI-based mapping, sample data is extracted from each file
4. **Relevance Determination**: AI evaluates if each file contains relevant data based on headers, sample data, and user description
5. **Field Mapping**: Source fields are mapped to target fields using regex patterns or AI
6. **Mapping Filtering**: Files with fewer than 2 mappings or deemed irrelevant are filtered out
7. **Mapping Storage**: Mappings are saved to a JSON file for manual review/editing

#### Step 2: Extract (Data Extraction)
1. **Mapping Loading**: Field mappings are loaded from the JSON file
2. **Path Extraction**: File paths are automatically extracted from the mappings file
3. **Stream Processing**: Files are processed one record at a time to minimize memory usage
4. **Field Mapping**: Original headers are mapped to their normalized categories
5. **Data Extraction**: For each record, relevant fields are extracted based on mappings
6. **Value Handling**: Multiple fields mapping to the same category are combined
7. **Record Merging**: Records with the same email address are merged
8. **Deduplication**: Duplicate records are removed
9. **NULL Handling**: NULL values are ignored
10. **JSONL Output**: Records are written to output in JSONL format in batches

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
6. **File Relevance Determination**: Files that don't contain relevant data are automatically skipped
7. **Single Mapping Filtering**: Files with only one mapping are automatically ignored to ensure data quality

This architecture allows Field Normalizer to handle data files of virtually any size with a fixed memory footprint while providing smart data processing capabilities.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Utility Scripts

### Delete Rejected Files

The `delete_rejected_files.py` script helps clean up files that were rejected by the AI during the analysis phase. This is useful for removing irrelevant files from your dataset.

#### Usage
```bash
python delete_rejected_files.py <analysis_file> <base_directory>
```

#### Arguments
- `analysis_file`: Path to the analysis report file (e.g., a.txt)
- `base_directory`: Directory containing the files to be deleted

#### Example
```bash
# Delete rejected files from the analysis report a.txt, looking for files in the it/ directory
python delete_rejected_files.py a.txt it/
```

The script will:
1. Read the analysis file to identify files rejected by AI
2. Look for these files in the specified base directory
3. Delete any matching files it finds
4. Provide a summary of deleted and not-found files

#### Output Example
```
Reading analysis file: a.txt
Base directory for files: it/

Found 2 rejected files:
- shopping-Rome-C+Italy.csv
- corpo_mail.csv

Proceeding to delete files...
Deleted: shopping-Rome-C+Italy.csv
Deleted: corpo_mail.csv

Summary:
Successfully deleted: 2 files
Files not found: 0 files
```