#!/usr/bin/env python3

import argparse
import csv
import json
import os
import random
import requests
import sys
from collections import defaultdict
from typing import List, Dict, Any, Optional, Tuple
import os
import dotenv

dotenv.load_dotenv(".env")

def sample_csv_data(file_path: str, min_examples: int = 5) -> Tuple[List[List[str]], int]:
    """Sample data from a CSV file to get representative examples of each field.
    
    Returns:
        Tuple of (sampled_data, num_columns)
    """
    with open(file_path, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        all_rows = list(reader)
        
        if not all_rows:
            return [], 0
        
        num_columns = len(all_rows[0])
        
        # Track examples for each column
        column_examples = defaultdict(set)
        sampled_rows = []
        
        # First, get a random sampling of rows to increase diversity
        if len(all_rows) > 20:
            # Get some random rows first
            random_sample = random.sample(all_rows, min(20, len(all_rows)))
            for row in random_sample:
                sampled_rows.append(row)
                for i, value in enumerate(row):
                    if i < num_columns:  # Ensure we don't go out of bounds
                        column_examples[i].add(value)
        
        # Then, specifically look for examples we're missing
        for row in all_rows:
            # Check if we have enough examples for each column
            if all(len(column_examples[i]) >= min_examples for i in range(num_columns)):
                break
                
            # Find columns that need more examples
            needed = False
            for i, value in enumerate(row):
                if i < num_columns and len(column_examples[i]) < min_examples and value not in column_examples[i]:
                    column_examples[i].add(value)
                    needed = True
            
            if needed and row not in sampled_rows:
                sampled_rows.append(row)
        
        return sampled_rows, num_columns

def generate_headers_with_openrouter(sample_data: List[List[str]], num_columns: int, filename: Optional[str] = None) -> List[str]:
    """Use OpenRouter API to generate headers based on sample data."""
    # Format the sample data as a readable string
    data_str = "\n".join([','.join(row) for row in sample_data])
    
    # Prepare the prompt for the API
    prompt = f"""Below is sample data from a CSV file without headers. Based on this data, generate appropriate column headers.
    The filename is {filename}. It might be useful to understand the context of the data.
    The CSV has {num_columns} columns. Please respond with ONLY a JSON array of {num_columns} header names, nothing else.
    
    Sample data:
    {data_str}
    
    JSON array of header names:
    """
    
    # Hardcoded API key
    api_key = os.getenv("OPENROUTER_API_KEY")
    # Make the API request
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    data = {
        "model": "google/gemini-2.0-flash-001",
        "messages": [
            {"role": "system", "content": "You are a data analysis assistant that helps identify appropriate CSV headers from the data."},
            {"role": "user", "content": prompt}
        ],
        "max_tokens": 150
    }
    print(f"Generating headers for {filename}...")

    try:
        response = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers=headers,
            json=data
        )
        response.raise_for_status()
        
        # Extract the generated headers from the response
        result = response.json()
        content = result['choices'][0]['message']['content']
        
        # Try to parse the JSON array from the response
        try:
            # First, try to find JSON array in the response if it's not a clean JSON
            import re
            json_match = re.search(r'\[.*\]', content, re.DOTALL)
            if json_match:
                content = json_match.group(0)
            
            headers = json.loads(content)
            
            # Ensure we have the right number of headers
            if len(headers) != num_columns:
                print(f"Warning: Generated {len(headers)} headers but expected {num_columns}")
                # Pad or truncate as needed
                if len(headers) < num_columns:
                    headers.extend([f"Column_{i+1}" for i in range(len(headers), num_columns)])
                else:
                    headers = headers[:num_columns]
            return headers
        except json.JSONDecodeError:
            print(f"Error parsing API response as JSON: {content}")
            # Fallback to generic headers
            return [f"Column_{i+1}" for i in range(num_columns)]
            
    except requests.exceptions.RequestException as e:
        print(f"API request error: {e}")
        # Fallback to generic headers
        return [f"Column_{i+1}" for i in range(num_columns)]

def update_csv_with_headers(file_path: str, headers: List[str]):
    """Update the CSV file with the generated headers."""
    # Read the existing data
    with open(file_path, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        data = list(reader)
    
    # Write the data back with headers
    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        writer.writerows(data)

def main():
    parser = argparse.ArgumentParser(description='Extract headers for a CSV file using AI')
    parser.add_argument('file_path', help='Path to the CSV file')
    args = parser.parse_args()
    
    file_path = args.file_path
    
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} does not exist")
        sys.exit(1)
    
    print(f"Processing {file_path}...")
    
    # Sample data from the CSV
    print("Sampling data to generate headers...")
    sample_data, num_columns = sample_csv_data(file_path)
    
    if not sample_data:
        print("Error: Could not extract sample data from the CSV file")
        sys.exit(1)
    
    # Generate headers using OpenRouter
    print("Generating headers using AI...")
    headers = generate_headers_with_openrouter(sample_data, num_columns)
    
    # Update the CSV with the new headers
    print(f"Adding headers to {file_path}...")
    update_csv_with_headers(file_path, headers)
    
    print(f"Successfully added headers: {', '.join(headers)}")


if __name__ == "__main__":
    main()
