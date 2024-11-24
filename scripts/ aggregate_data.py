"""
Script Name: aggregate_cleaned_data.py

Description:
    This script aggregates cleaned annual datasets of night-time light (NTL) data 
    into a single Parquet file. The aggregated dataset contains summed DN values 
    for each country across all years.

Author:
    Shamayla Durrin Islam
    shamayla.islam@mail.utoronto.ca

Date:
    Created: November 23, 2024

Dependencies:
    - Python 3.8 or higher
    - pandas >= 1.3

Inputs:
    - Cleaned annual CSV files located in the directory: `analysis_data/cleaned_data/`.

Outputs:
    - A single aggregated Parquet file saved as: `analysis_data/final_aggregated_data.parquet`.

Usage:
    1. Ensure cleaned data files are located in the `analysis_data/cleaned_data/` directory.
    2. Install required dependencies using pip:
        pip install pandas pyarrow
    3. Run the script:
        python aggregate_cleaned_data.py
"""

import os
import pandas as pd

# Define input and output paths
input_dir = "analysis_data/cleaned_data/"  # Directory with cleaned annual data files
output_file = "analysis_data/final_aggregated_data.parquet"  # Final aggregated Parquet file

# Get a list of all cleaned CSV files
file_list = [f for f in os.listdir(input_dir) if f.endswith("_cleaned.csv")]

# Initialize an empty DataFrame to store the aggregated data
final_df = pd.DataFrame()

# Process each cleaned file
for file_name in file_list:
    year = file_name.split("_")[0]  # Extract the year from the file name
    file_path = os.path.join(input_dir, file_name)
    
    # Load the cleaned dataset
    print(f"Processing cleaned file: {file_name}")
    df = pd.read_csv(file_path)
    
    # Add the year as a column
    df["Year"] = int(year)
    
    # Append to the final DataFrame
    final_df = pd.concat([final_df, df], ignore_index=True)

# Save the aggregated data as a Parquet file
final_df.to_parquet(output_file, index=False)
print(f"Saved aggregated data to: {output_file}")
