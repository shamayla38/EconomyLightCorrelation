#### Preamble ####
# Purpose: Models the percentage of support for Kamala Harris in each poll as a function of various predictors.
# Author: Shamayla Durrin
# Date: 30-10-2024
# Contact: shamayla.islam@mail.utoronto.ca
# License: MIT
# Pre-requisites:
# - 00-install_packages.R
# - 02-clean_data.R
# Any other information needed? Data should include variables for support, pollster, sample size, state, and recency.

#### Workspace setup ####
library(tidyverse)
library(here)
library(arrow)

#### Read data ####
analysis_data <-
  read_parquet(here("data/02-analysis_data/analysis_data.parquet"))

# Filter all the observations regarding Harris. Model 1,2,3 will be about Harris.
harris_data <- analysis_data %>%
  filter(candidate_name %in% c("Kamala Harris"))

# Model 1: Only pollster and sample size as predictors
model1 <- lm(pct ~ pollster + sample_size, data = harris_data)

saveRDS(model1, here("models", "model1.rds"))

# Model 2: Adding state as a predictor
model2 <-
  lm(pct ~ pollster + sample_size + state, data = harris_data)
saveRDS(model2, here("models", "model2.rds"))

# Model 3: Adding recency as a predictor
model3 <-
  lm(pct ~ pollster + sample_size + state + recency, data = harris_data)
saveRDS(model3, here("models", "model3.rds"))

# Filter all the observations regarding Trump. Model 4,5,6 will be about Trump.
trump_data <- analysis_data %>%
  filter(candidate_name %in% c("Donald Trump"))

# Model 4: Only pollster and sample size as predictors
model4 <- lm(pct ~ pollster + sample_size, data = trump_data)

saveRDS(model4, here("models", "model4.rds"))

# Model 5: Adding state as a predictor
model5 <-
  lm(pct ~ pollster + sample_size + state, data = trump_data)
saveRDS(model5, here("models", "model5.rds"))

# Model 6: Adding recency as a predictor
model6 <-
  lm(pct ~ pollster + sample_size + state + recency, data = trump_data)
saveRDS(model6, here("models", "model6.rds"))