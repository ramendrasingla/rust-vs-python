import pandas as pd
import gzip
import time
import os
from pathlib import Path

# Function to process a single file with additional complex operations
def process_file(file_path):
    with gzip.open(file_path, 'rt') as f:
        df = pd.read_csv(f, sep=' ', header=None, names=['project', 'title', 'views', 'bytes'])
    
    # Simulate complex processing
    df['processed_views'] = df['views'] ** 2  # Example of a complex operation
    
    # Perform additional computations
    grouped = df.groupby('project').agg({
        'views': 'sum',
        'processed_views': 'mean'
    })
    
    return grouped

# Function to handle multiple files
def process_files(file_paths):
    results = []
    for file_path in file_paths:
        results.append(process_file(file_path))
    combined_result = pd.concat(results).groupby('project').agg({
        'views': 'sum',
        'processed_views': 'mean'
    })
    return combined_result

def main():

    directory = "../dataset"
    output_file = "../output/python_result.csv"
    file_paths = list(Path(directory).glob("pageviews-20240801-*.gz"))

    start_time = time.time()
    combined_result = process_files(file_paths)
    end_time = time.time()

    # Write results to CSV
    combined_result.to_csv(output_file)

    # Print results and performance metrics
    print("Combined processing result:", combined_result.head())
    print(f"Execution time: {end_time - start_time} seconds")

if __name__ == "__main__":
    main()
