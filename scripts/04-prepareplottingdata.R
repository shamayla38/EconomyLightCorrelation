### Preamble ####
# Purpose: Prepares data for plotting by cleaning and organizing analysis data, assigning regions based on SDG classification, and calculating average metrics such as GDP, population, and manufacturing share.
# Author: Shamayla Durrin Islam
# Date: 30 November 2024
# Contact: shamayla.islam@mil.utoronto.ca
# License: MIT
# Pre-requisites: Requires the analysis dataset with columns for country, year, GDP, population, and manufacturing share. Ensure tidyverse and arrow packages are installed for data manipulation and saving.
# Any other information: This script assigns regions to countries, computes averages over defined intervals, and outputs a cleaned dataset for plotting in Parquet format.
# Data Preparation for Plotting
# 
# Description:
# This script prepares a dataset for plotting by:
# - Assigning regions to countries based on the SDG (Sustainable Development Goals) region classification.
# - Calculating average GDP, population, and manufacturing share for each country across specified 7-year intervals.
# - Saving the processed dataset to a Parquet file for efficient use in plotting and further analysis.
# 
# Inputs:
# - A dataset named `analysis_data` containing columns: country, year, gdp, population, and manufacturingsharegdp.
# 
# Outputs:
# - A processed dataset saved as a Parquet file: "plotting.parquet".
# 
# Author:
# [Your Name]
# 
# Date:
# [Today's Date]

# Load necessary libraries
library(dplyr)
library(arrow)

# Step 1: Define the SDG region mapping
region_mapping <- list(
  "Central and Southern Asia" = c("Afghanistan", "Bangladesh", "Bhutan", "India", "Iran", "Kazakhstan", 
                                  "Kyrgyzstan", "Maldives", "Nepal", "Pakistan", "Sri Lanka", "Tajikistan", 
                                  "Turkmenistan", "Uzbekistan"),
  "Europe and Northern America" = c("Albania", "Bermuda", "Andorra", "Austria", "Belarus", "Belgium", 
                                    "Bosnia and Herzegovina", "Bulgaria", "Canada", "Croatia", "Czechia", 
                                    "Denmark", "Estonia", "Faroe Islands", "Finland", "France", "Germany", 
                                    "Gibraltar", "Greece", "Greenland", "Guernsey", "Hungary", "Iceland", 
                                    "Ireland", "Isle of Man", "Italy", "Jersey", "Latvia", "Liechtenstein", 
                                    "Lithuania", "Luxembourg", "Malta", "Moldova", "Monaco", "Montenegro", 
                                    "Netherlands", "North Macedonia", "Norway", "Poland", "Portugal", 
                                    "Romania", "Russian Federation", "San Marino", "Serbia", "Slovakia", 
                                    "Slovenia", "Spain", "Svalbard and Jan Mayen", "Sweden", "Switzerland", 
                                    "Ukraine", "United Kingdom", "United States", "Vatican"),
  "Northern Africa and Western Asia" = c("Algeria", "Armenia", "Azerbaijan", "Bahrain", "Cyprus", "Egypt", 
                                         "Georgia", "Iraq", "Israel", "Jordan", "Kuwait", "Lebanon", "Libya", 
                                         "Morocco", "Oman", "Palestine", "Qatar", "Saudi Arabia", "Sudan", 
                                         "Syria", "Tunisia", "Turkey", "United Arab Emirates", "Yemen"),
  "Sub-Saharan Africa" = c("Angola", "Benin", "Botswana", "Burkina Faso", "Burundi", "Cabo Verde", 
                           "Cameroon", "Central African Republic", "Chad", "Comoros", "Congo", 
                           "Cote d'Ivoire", "Djibouti", "Equatorial Guinea", "Eritrea", "Eswatini", 
                           "Ethiopia", "Gabon", "Gambia", "Ghana", "Guinea", "Guinea-Bissau", "Kenya", 
                           "Lesotho", "Liberia", "Madagascar", "Malawi", "Mali", "Mauritania", "Mauritius", 
                           "Mozambique", "Namibia", "Niger", "Nigeria", "Rwanda", "Sao Tome and Principe", 
                           "Senegal", "Seychelles", "Sierra Leone", "Somalia", "South Africa", "South Sudan", 
                           "Tanzania", "Togo", "Uganda", "Zambia", "Zimbabwe"),
  "Eastern and South-Eastern Asia" = c("Brunei Darussalam", "Timor-Leste", "Cambodia", "China", "East Timor", 
                                       "Hong Kong", "Indonesia", "Japan", "Laos", "Macao", "Malaysia", 
                                       "Mongolia", "Myanmar", "North Korea", "Philippines", "Singapore", 
                                       "South Korea", "Thailand", "Vietnam"),
  "Latin America and the Caribbean" = c("Antigua and Barbuda", "Argentina", "Aruba", "Bahamas", "Barbados", 
                                        "Belize", "Bolivia", "Brazil", "British Virgin Islands", 
                                        "Cayman Islands", "Chile", "Colombia", "Costa Rica", "Cuba", 
                                        "Curacao", "Dominica", "Dominican Republic", "Ecuador", 
                                        "El Salvador", "Falkland Islands", "French Guiana", "Grenada", 
                                        "Guadeloupe", "Guatemala", "Guyana", "Haiti", "Honduras", 
                                        "Jamaica", "Martinique", "Mexico", "Montserrat", "Nicaragua", 
                                        "Panama", "Paraguay", "Peru", "Puerto Rico", "Saint Barthelemy", 
                                        "Saint Kitts and Nevis", "Saint Lucia", "Saint Martin (French part)", 
                                        "Saint Vincent and the Grenadines", "Sint Maarten (Dutch part)", 
                                        "Suriname", "Trinidad and Tobago", "Turks and Caicos Islands", 
                                        "Uruguay", "Venezuela"),
  "Oceania" = c("American Samoa", "Australia", "Cook Islands", "Fiji", "French Polynesia", "Guam", "Kiribati", 
                "Marshall Islands", "Micronesia", "Nauru", "New Caledonia", "New Zealand", "Niue", 
                "Norfolk Island", "Northern Mariana Islands", "Palau", "Papua New Guinea", "Pitcairn", 
                "Samoa", "Solomon Islands", "Tokelau", "Tonga", "Tuvalu", "Vanuatu", "Wallis and Futuna"),
  "Australia and New Zealand" = c("Australia", "New Zealand", "Christmas Island", "Cocos Islands", 
                                  "Heard Island and McDonald Islands", "Norfolk Island")
)

# Step 2: Assign regions to countries
analysis_data_with_regions <- analysis_data
analysis_data_with_regions$region <- NA

for (region in names(region_mapping)) {
  analysis_data_with_regions$region[analysis_data_with_regions$country %in% region_mapping[[region]]] <- region
}

# Step 3: Add 7-year intervals and calculate averages
average_metrics <- analysis_data_with_regions %>%
  mutate(interval = case_when(
    year >= 1991 & year <= 1997 ~ "1991-1997",
    year >= 1998 & year <= 2004 ~ "1998-2004",
    year >= 2005 & year <= 2011 ~ "2005-2011",
    year >= 2012 & year <= 2020 ~ "2012-2020",
    TRUE ~ NA_character_
  )) %>%
  group_by(country, interval) %>%  # Group only by country and interval
  summarise(
    region = first(region),  # Retain the region column
    avg_gdp = mean(gdp, na.rm = TRUE),
    avg_population = mean(population, na.rm = TRUE),
    avg_manufacturing_share = mean(manufacturingsharegdp, na.rm = TRUE)
  ) %>%
  ungroup()

# Step 4: Save the resulting dataset
write_parquet(average_metrics, "plotting.parquet")
