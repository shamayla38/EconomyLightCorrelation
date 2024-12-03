"""
Script Name: synsimulat_data.py

Description:
    This script generates a synthetic dataset containing the following columns:
    - Country: Randomly selected country names.
    - Year: Randomly selected years within a specified range.
    - Population: Random population figures within a defined range.
    - Digital Number (DN): Random floating-point values representing DN.
    - Manufacturing as a Portion of GDP: Random percentages representing the manufacturing sector's contribution to GDP.

    The generated dataset is saved as a CSV file for further analysis or testing purposes.

Author:
    Shamayla Durrin Islam
    shamayla.islam@mail.utoronto.ca

Date:
    Created: November 24, 2024

Dependencies:
    - Python 3.8 or higher
    - pandas >= 1.3
    - faker >= 8.0

Inputs:
    - None. The script generates data programmatically.

Outputs:
    - A CSV file named 'synthetic_data.csv' containing the generated dataset.

Usage:
    1. Ensure that the required dependencies are installed:
        pip install pandas faker
    2. Run the script:
        python synthetic_data_generator.py
"""

import pandas as pd
import random
from faker import Faker

# Initialize the Faker object
fake = Faker()

# Set the number of records to generate
num_records = 1000

# Define the range of years for the dataset
start_year = 1990
end_year = 2020

# Initialize an empty list to store the generated data
data = []

# Loop to generate synthetic data records
for _ in range(num_records):
    # Generate a random country name
    country = fake.country()
    
    # Generate a random year within the specified range
    year = random.randint(start_year, end_year)
    
    # Generate a random population between 1 million and 100 million
    population = random.randint(1_000_000, 100_000_000)
    
    # Generate a random Digital Number (DN) value between 0 and 100
    dn = round(random.uniform(0, 100), 2)
    
    # Generate a random manufacturing GDP percentage between 5% and 40%
    manufacturing_gdp = round(random.uniform(5, 40), 2)
    
    # Append the generated record to the data list
    data.append({
        'Country': country,
        'Year': year,
        'Population': population,
        'DN': dn,
        'ManufacturingAsPortionOfGDP': manufacturing_gdp
    })

# Convert the list of dictionaries into a pandas DataFrame
df = pd.DataFrame(data)

# Save the DataFrame to a CSV file
df.to_csv('data/00-simulated_data/simulated_data.csv', index=False)

