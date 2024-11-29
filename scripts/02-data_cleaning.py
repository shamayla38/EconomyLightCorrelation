"""
Script Name: data_cleaning.py

Description:
    This script processes and cleans annual datasets related to night-time light (NTL) data, GDP, manufacturing share of GDP, population, 
    and SPI (Statistical Performance Indicator). The script standardizes, aggregates, renames, and merges datasets for analysis purposes. 
    Key steps include:
    1. Cleaning and aggregating NTL data by country and year.
    2. Transforming and standardizing World Bank data (GDP, Manufacturing, Population, SPI).
    3. Assigning SPI-based grades to countries based on statistical performance.
    4. Merging cleaned datasets into a final unified dataset for analysis.

Author:
    Shamayla Durrin Islam
    shamayla.islam@mail.utoronto.ca

Date:
    28/11/2024

Dependencies:
    - Python 3.8 or higher
    - pandas >= 1.3

Inputs:
    - Extracted NTL datasets (CSV format)
    - World Bank datasets: GDP, Manufacturing, Population, SPI (CSV format)

Outputs:
    - Cleaned and aggregated NTL data (by country and year)
    - Standardized World Bank datasets (GDP, Manufacturing, Population, SPI)
    - Unified dataset with merged variables for analysis (CSV and Parquet format)

Usage:
    1. Set the input and output directories for raw and processed data.
    2. Run the script to clean, process, and merge datasets.
    3. Access the final dataset for further statistical or econometric analysis.
"""


# First we simply take each extracted file that contains the DN values, Country and Year and we will remove all the observations that do not have a valid country identified.
# Then we will sum all the DN values for each country as we will be needing the aggregate luminosity.  
# Define input and output directories

input_dir = "data/01-raw_data/extracted"  # Directory with raw annual data files
output_dir = "data/02-analysis_data/01-aggregatedbycountry"  # Directory to save cleaned data

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

    # Remove observations without a valid 'country'
    df = df.dropna(subset=["country"])
    
    # Group by 'country' and sum the DN values
    grouped_df = df.groupby("country", as_index=False)["dn"].sum()
    
    # Save the cleaned dataset to the output directory
    output_file_path = os.path.join(output_dir, f"{year}.csv")
    grouped_df.to_csv(output_file_path, index=False)
    print(f"Saved cleaned data for {year} to: {output_file_path}")



#Here we will concatenate into a single data frame. 
#Also, some of the regions are part of coutries but named independently in the shapefile we will rename them as the parent country and sum their dn values again. 

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


# Define countries to remove
remove_countries = [
    'Vatican City', 'Saint Martin', 'Cook Islands', 'Bouvet Island'
]

# Apply renaming
final_df['country'] = final_df['country'].replace(rename_mapping)

# Remove specified countries
final_df = final_df[~final_df['country'].isin(remove_countries)]

# Group by 'country' and sum the DN values
final_df = final_df.groupby(["country", "year"], as_index=False)["dn"].sum()

# Save the concatenated DF

file_path = '/analysis_data/03-concatenated/concatenated.csv'

# Save the DataFrame to a CSV file
final_df.to_csv(file_path, index=False)

#standertise the column names for merging later

final_df.columns = final_df.columns.str.lower().str.strip() 

# Section 2 : Clean World Bank Data 

#First we will fix the gdp data set 

# Load your dataset (replace 'your_file.csv' with the actual file name)
file_path = '/Users/shamayladurrin/Desktop/GDPNTL/raw_data/gdpdata/GDP.csv'  # Update this to your file path
gdp = pd.read_csv(file_path)

# Convert from wide to long format
gdp = pd.melt(
    gdp,
    id_vars=['Country', 'Country_Code'],  # Columns to keep fixed
    var_name='Year',                     # Name for the years column
    value_name='GDP'                     # Name for the GDP column
)

# Define renaming rules
rename_mapping = {
    'West Bank and Gaza': 'Palestinian Territory',
    'Viet Nam': 'Vietnam',
    "Korea, Dem. People's Rep.": 'North Korea',
    'Channel Islands': 'Jersey',
    'Congo, Dem. Rep.': 'Congo DRC',
    'St. Lucia': 'Saint Lucia',
    'Micronesia, Fed. Sts.': 'Micronesia',
    'Venezuela, RB': 'Venezuela',
    'Yemen, Rep.': 'Yemen',
    'St. Kitts and Nevis': 'Saint Kitts and Nevis',
    'Korea, Rep.': 'South Korea',
    'Virgin Islands (U.S.)': 'US Virgin Islands',
    'Bahamas, The': 'Bahamas',
    'Czechia': 'Czech Republic',
    'Syrian Arab Republic': 'Syria',
    'Egypt, Arab Rep.': 'Egypt',
    'Lao PDR': 'Laos',
    'Kyrgyz Republic': 'Kyrgyzstan'
}

# Apply renaming
gdp['Country'] = gdp['Country'].replace(rename_mapping)
gdp.columns = gdp.columns.str.lower().str.strip()

# Here we fix the manufacturing data set

file_path = '/Users/shamayladurrin/Desktop/GDPNTL/analysis_data/04-worldbankdataprocessed/gdp.csv'
gdp.to_csv(file_path, index=False)

# Load your dataset (replace 'your_file.csv' with the actual file name)
file_path = '/Users/shamayladurrin/Desktop/GDPNTL/raw_data/manufacturingData/manufacturing.csv'  # Update this to your file path
manu = pd.read_csv(file_path)

# Convert from wide to long format
manu = pd.melt(
    manu,
    id_vars=['Country', 'Country_Code'],  # Columns to keep fixed
    var_name='Year',                     # Name for the years column
    value_name='ManufacturingShareGDP'                     
)

# Define renaming rules
rename_mapping = {
    'West Bank and Gaza': 'Palestinian Territory',
    'Viet Nam': 'Vietnam',
    "Korea, Dem. People's Rep.": 'North Korea',
    'Channel Islands': 'Jersey',
    'Congo, Dem. Rep.': 'Congo DRC',
    'St. Lucia': 'Saint Lucia',
    'Micronesia, Fed. Sts.': 'Micronesia',
    'Venezuela, RB': 'Venezuela',
    'Yemen, Rep.': 'Yemen',
    'St. Kitts and Nevis': 'Saint Kitts and Nevis',
    'Korea, Rep.': 'South Korea',
    'Virgin Islands (U.S.)': 'US Virgin Islands',
    'Bahamas, The': 'Bahamas',
    'Czechia': 'Czech Republic',
    'Syrian Arab Republic': 'Syria',
    'Egypt, Arab Rep.': 'Egypt',
    'Lao PDR': 'Laos',
    'Kyrgyz Republic': 'Kyrgyzstan'
}

# Apply renaming
manu['Country'] = manu['Country'].replace(rename_mapping)
manu.columns = manu.columns.str.lower().str.strip()

file_path = '/Users/shamayladurrin/Desktop/GDPNTL/analysis_data/04-worldbankdataprocessed/manufacturing.csv'
manu.to_csv(file_path, index=False)

# Now we will fix the population data set

# Load your dataset (replace 'your_file.csv' with the actual file name)
file_path = '/Users/shamayladurrin/Desktop/GDPNTL/raw_data/manufacturingData/manufacturing.csv'  # Update this to your file path
pop = pd.read_csv(file_path)

# Convert from wide to long format
pop = pd.melt(
    pop,
    id_vars=['Country', 'Country_Code'],  # Columns to keep fixed
    var_name='Year',                     # Name for the years column
    value_name='Population'                     
)

# Define renaming rules
rename_mapping = {
    'West Bank and Gaza': 'Palestinian Territory',
    'Viet Nam': 'Vietnam',
    "Korea, Dem. People's Rep.": 'North Korea',
    'Channel Islands': 'Jersey',
    'Congo, Dem. Rep.': 'Congo DRC',
    'St. Lucia': 'Saint Lucia',
    'Micronesia, Fed. Sts.': 'Micronesia',
    'Venezuela, RB': 'Venezuela',
    'Yemen, Rep.': 'Yemen',
    'St. Kitts and Nevis': 'Saint Kitts and Nevis',
    'Korea, Rep.': 'South Korea',
    'Virgin Islands (U.S.)': 'US Virgin Islands',
    'Bahamas, The': 'Bahamas',
    'Czechia': 'Czech Republic',
    'Syrian Arab Republic': 'Syria',
    'Egypt, Arab Rep.': 'Egypt',
    'Lao PDR': 'Laos',
    'Kyrgyz Republic': 'Kyrgyzstan'
}

# Apply renaming
pop['Country'] = pop['Country'].replace(rename_mapping)
pop.columns = pop.columns.str.lower().str.strip()

# Save it 
file_path = '/Users/shamayladurrin/Desktop/GDPNTL/analysis_data/04-worldbankdataprocessed/population.csv'
pop.to_csv(file_path, index=False)

file_path = '/Users/shamayladurrin/Desktop/STA304/NTLGDPCorr/data/01-raw_data/worldbankdata/SPI.csv'
spi = pd.read_csv(file_path)

# Now we will prepare the rating data set 

#SPI 

file_path = '/Users/shamayladurrin/Desktop/STA304/NTLGDPCorr/data/01-raw_data/worldbankdata/SPI.csv'
spi = pd.read_csv(file_path)

# Convert the year column to numeric (if it's not already)
spi['year'] = pd.to_numeric(spi['year'], errors='coerce')

# Remove rows with years after 2020
spi = spi[spi['year'] <= 2020]

# Group by 'year' (or 'date') and calculate the average SPI for each country or globally
spi = spi.groupby(['country'], as_index=False)['SPI'].mean()

# Define a function to assign grades based on SPI
def assign_grade(spi_value):
    if spi_value >= 80:
        return 'A'
    elif spi_value >= 60:
        return 'B'
    elif spi_value >= 40:
        return 'C'
    elif spi_value >= 20:
        return 'D'
    else:
        return 'F'

# Apply the grading function to create a new column
spi['grade'] = spi['SPI'].apply(assign_grade)

file_path = '/Users/shamayladurrin/Desktop/GDPNTL/analysis_data/04-worldbankdataprocessed/ratingonSPI.csv'
spi.to_csv(file_path, index=False)

#Section 3: Merging

# final_df + GDP
# Convert 'year' column to string in both datasets
final_df['year'] = final_df['year'].astype(str)
gdp['year'] = gdp['year'].astype(str)

# Now merge the datasets
merged = pd.merge(final_df, gdp, on=['year', 'country'], how='inner')

# merged + manu
manu['year'] = manu['year'].astype(str)

# Now merge the datasets
merged = pd.merge(merged, manu, on=['year', 'country'], how='inner')

# merged + pop

pop['year'] = pop['year'].astype(str)

# Now merge the datasets
merged = pd.merge(merged, pop, on=['year', 'country'], how='inner')

# Drop unnecessary columns
merged = merged.drop(columns=['country_code_x', 'country_code_y'])

# Now merge the datasets
merged = pd.merge(merged, spi, on=['country'], how='inner')

# Save the DataFrame to a Parquet file
parquet_file_path = '/Users/shamayladurrin/Desktop/GDPNTL/analysis_data/05-analysis/analysis.csv'
merged.to_csv(parquet_file_path, index=False)