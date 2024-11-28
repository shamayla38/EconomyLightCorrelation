"""
Script Name: data_cleaning.py

Description:
    This script processes annual datasets of night-time light (NTL) data by:
    1. Removing observations without a valid 'Country'.
    2. Grouping the data by 'Country' and summing the DN values.
    3. Saving the cleaned data for each year into a specified directory.
    The cleaned datasets will have as many rows as there are unique countries, with summed DN values.

Author:
    Shamayla Durrin Islam
    shamayla.islam@mail.utoronto.ca

Date:
    Created: November 23, 2024

Dependencies:
    - Python 3.8 or higher
    - pandas >= 1.3

Inputs:
    - Annual CSV files located in the directory: `analysis_data/finaldata/`
    - Each CSV contains columns: Latitude, Longitude, DN, Country.

Outputs:
    - Cleaned CSV files, one for each year, saved in: `analysis_data/cleaned_data/`.

Usage:
    1. Place the annual raw data files in the `analysis_data/finaldata/` directory.
    2. Install required dependencies using pip:
        pip install pandas
    3. Run the script:
        python data_cleaning.py
"""

import os
import pandas as pd

# Define input and output directories
input_dir = "data/01-raw_data/extracted/"  # Directory with raw annual data files
output_dir = "data/02-analysis_data/yearwise"  # Directory to save cleaned data

# Ensure the output directory exists
os.makedirs(output_dir, exist_ok=True)

# Get a list of all CSV files in the input directory
file_list = [f for f in os.listdir(input_dir) if f.endswith(".csv")]

# Process each file
for file_name in file_list:
    year = file_name.split(".")[0]  # Extract the year from the file name
    file_path = os.path.join(input_dir, file_name)
    
    # Load the dataset
    print(f"Processing file: {file_name}")
    df = pd.read_csv(file_path)

    # Remove observations without a valid 'Country'
    df = df.dropna(subset=["COUNTRY"])
    
    # Group by 'Country' and sum the DN values
    grouped_df = df.groupby("COUNTRY", as_index=False)["DN"].sum()
    
    # Save the cleaned dataset to the output directory
    output_file_path = os.path.join(output_dir, f"{year}_cleaned.csv")
    grouped_df.to_csv(output_file_path, index=False)
    print(f"Saved cleaned data for {year} to: {output_file_path}")

# Cleaning Worldbank Data 
# Please adjust this following code for each files

file_path = 'data/01-raw_data/worldbankdata/GDP.csv'  # Update with your file path
df = pd.read_csv(file_path)

# Melt the DataFrame to make it long
df_long = pd.melt(
    df,
    id_vars=["Country", "Country_Code"],  # Columns to keep
    var_name="Year",                     # Name of the new 'Year' column
    value_name="GDP_PPP"                 # Name of the values column
)

# Ensure 'Year' is an integer (if applicable)
df_long["Year"] = df_long["Year"].astype(int)

# Display the transformed DataFrame
print(df_long.head())

# Save the long-format data to a new CSV
output_file = 'gdplong.csv'
df_long.to_csv(output_file, index=False)
print(f"Long format data saved to {output_file}")
