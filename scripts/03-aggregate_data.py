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

# Define input and output paths
input_dir = "/Users/shamayladurrin/Desktop/STA304/NTLGDPCorr/data/02-analysis_data/yearwise/"  # Directory with cleaned annual data files

# Get a list of all cleaned CSV files
file_list = [f for f in os.listdir(input_dir) if f.endswith(".csv")]

# Initialize an empty DataFrame to store the aggregated data
final_df = pd.DataFrame()

# Process each cleaned file
for file_name in file_list:
    # Extract the year from the file name (removing the .csv extension)
    year = os.path.splitext(file_name.split("_")[0])[0]
    file_path = os.path.join(input_dir, file_name)
    
    # Load the cleaned dataset
    print(f"Processing cleaned file: {file_name}")
    df = pd.read_csv(file_path)
    
    # Normalize column names to lowercase
    df.columns = df.columns.str.lower()
    
    # Rename columns to standard names
    column_mapping = {
        "country": "country",  # Ensure consistency for country
        "dn": "dn",            # Normalize 'DN' to 'dn'
        "dn_value": "dn"       # Normalize 'DN_value' to 'dn'
    }
    df.rename(columns=column_mapping, inplace=True)
    
    # Ensure the required columns exist
    if "country" not in df.columns or "dn" not in df.columns:
        print(f"Skipping file {file_name}: Required columns ('country' and 'dn') not found.")
        continue

    # Add the year as a column
    try:
        df["year"] = int(year)  # Convert the year to an integer
    except ValueError:
        print(f"Skipping file {file_name}: Unable to parse year '{year}'.")
        continue
    
    # Append to the final DataFrame
    final_df = pd.concat([final_df, df], ignore_index=True)

rename_mapping = {
    # Rename to 'France'
    'Guadeloupe': 'France',
    'Réunion': 'France',
    'Mayotte': 'France',
    'French Guiana': 'France',
    'Martinique': 'France',
    'French Southern Territories': 'France',
    'Wallis and Futuna': 'France',
    # Rename to 'Portugal'
    'Madeira': 'Portugal',
    'Azores': 'Portugal',
    # Rename to 'United Kingdom'
    'Guernsey': 'United Kingdom',
    'Jersey': 'United Kingdom',
    'Anguilla': 'United Kingdom',
    'Montserrat': 'United Kingdom',
    'Saint Helena': 'United Kingdom',
    'Falkland Islands': 'United Kingdom',
    'South Georgia and South Sandwich Islands': 'United Kingdom',
    'British Indian Ocean Territory': 'United Kingdom',
    # Rename to 'Australia'
    'Norfolk Island': 'Australia',
    'Christmas Island': 'Australia',
    'Heard Island and McDonald Islands': 'Australia',
    # Rename to 'Netherlands'
    'Sint Maarten': 'Netherlands',
    'Bonaire': 'Netherlands',
    'Saba': 'Netherlands',
    'Saint Eustatius': 'Netherlands',
    # Rename to 'New Zealand'
    'Niue': 'New Zealand',
    # Rename to 'Spain'
    'Canarias': 'Spain',
    #rename to norway
    'Svalbard': 'Norway',
    #Rename to France
    'Saint Barthelemy': 'France',
    'Saint Pierre and Miquelon': 'France',
    #Rename to Australia
    'Cocos Islands': 'Australia',
    # Rename to fix spelling
    "Côte d'Ivoire" : "Cote d'Ivoire"
}
# Apply renaming
final_df['country'] = final_df['country'].replace(rename_mapping)

#Now we will prepare the world bank data for merge 

