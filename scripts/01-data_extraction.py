"""
Script Name: data_extraction.py

Description:
    This script extracts data from a GeoTIFF file (e.g., Night-Time Light (NTL) data),
    converts it into latitude, longitude, and DN value format, and then merges the data 
    with a shapefile of world countries to assign a 'COUNTRY' column. The output is 
    saved as a CSV file.

Author:
    Shamayla Durrin Islam
    shamayla.islam@mail.utoronto.ca

Date:
    Created: November 23, 2024

Dependencies:
    - Python 3.8 or higher
    - pandas >= 1.3
    - geopandas >= 0.9
    - rasterio >= 1.2
    - shapely >= 1.7

Inputs:
    - GeoTIFF file containing raster data (e.g., NTL data).
    - Shapefile of world countries with CRS EPSG:4326 (or compatible).

Outputs:
    - A CSV file containing latitude, longitude, DN value, and assigned country.

Usage:
    1. Update `file_path` and `shapefile_path` variables with appropriate file paths.
    2. Install required dependencies using pip:
        pip install pandas geopandas rasterio shapely
    3. Run the script:
        python data_extraction.py
"""

import pandas as pd
import geopandas as gpd
import rasterio
import numpy as np
from shapely.geometry import Point

# Define file paths
file_path = "raw_data/tiffiles/Harmonized_DN_NTL_2019_simVIIRS.tif"  # GeoTIFF file path
shapefile_path = "raw_data/shapefile"  # Shapefile path

# Load the shapefile
shapefile = gpd.read_file(shapefile_path)

# Reproject the shapefile to EPSG:4326 if necessary
if shapefile.crs.to_epsg() != 4326:
    print("Reprojecting shapefile to EPSG:4326...")
    shapefile = shapefile.to_crs(epsg=4326)
else:
    print("Shapefile is already in EPSG:4326.")

# Open the GeoTIFF file with rasterio
print("Reading GeoTIFF data...")
with rasterio.open(file_path) as dataset:
    # Read the first band
    first_band = dataset.read(1)
    transform = dataset.transform  # Get the affine transformation
    
    # Get dimensions
    rows, cols = first_band.shape

# Create a list to store the data
data = []

# Loop through each pixel to extract data
print("Extracting raster data...")
for row in range(rows):
    for col in range(cols):
        # Get the DN value for the pixel
        dn_value = first_band[row, col]
        
        # Skip no-data or invalid values
        if np.isnan(dn_value) or dn_value == 0:  # Adjust '0' if your no-data value is different
            continue
        
        # Convert pixel indices to geographic coordinates
        lon, lat = rasterio.transform.xy(transform, row, col, offset='center')
        
        # Append the data as a tuple (latitude, longitude, DN value)
        data.append((lat, lon, dn_value))

# Convert the extracted data to a Pandas DataFrame
df = pd.DataFrame(data, columns=["Latitude", "Longitude", "DN"])
print(f"Extracted {len(df)} valid data points.")

# Create geometry for each point using latitude and longitude
geometry = [Point(xy) for xy in zip(df["Longitude"], df["Latitude"])]
geo_df = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

# Perform a spatial join with the shapefile to assign country names
print("Performing spatial join...")
result = gpd.sjoin(geo_df, shapefile, how="left", predicate="intersects")

# Select relevant columns and rename as needed
result = result[["Latitude", "Longitude", "DN", "COUNTRY"]]

# Save the result to a CSV file
output_path = "raw_data/extracted/1993.csv"  # Output CSV file path
result.to_csv(output_path, index=False)
print(f"Processed data saved to {output_path}.")

# Display the first few rows of the result
print(result.head())
