#### Preamble ####
# Purpose: Models the percentage of support for Kamala Harris in each poll as a function of various predictors.
# Author: Shamayla Durrin
# Date: 01/12/2024
# Contact: shamayla.islam@mail.utoronto.ca
# License: MIT
# Pre-requisites:

#### Workspace setup ####
library(tidyverse)
library(here)
library(arrow)
library(car)          # For VIF and residual plots
library(ggplot2)      # For plotting
library(broom)        # For tidying model outputs
library(performance)  # For model diagnostics

#### Read data ####
analysis_data <- read_parquet(here("data/02-analysis_data/04-analysis/analysis.parquet"))

# Model 1: DN ~ GDP + Country (Fixed Effects) + Year (Fixed Effects)

model1_data <- analysis_data %>%
  filter(!is.na(dn) & !is.na(gdp) & !is.na(country) & !is.na(year))

model1 <- lm(log(dn) ~ log(gdp) + factor(country) + factor(year), data = model1_data)
saveRDS(model1, file = "models/model1.rds") # Save the model

# Model 2: DN ~ GDP*grade
model2_data <- analysis_data %>%
  filter(!is.na(dn) & !is.na(gdp))

model2 <- lm(log(dn) ~ log(gdp):grade + factor(year) , data = model2_data )
saveRDS(model2, file = "models/model2.rds") # Save the model

# Model 3: DN ~ Population + country(Fixed Effects) + Year (Fixed Effects)
model3_data <- analysis_data %>%
  filter(!is.na(dn) &  !is.na(population))

model3 <- lm(log(dn) ~ log(population) + factor(country) + factor(year)  , data = model3_data)
saveRDS(model3, file = "models/model3.rds") # Save the model

# Model 4: DN ~  Manufacturing Share + Grade + country(Fixed Effects) + Year (Fixed Effects)
model4_data <- analysis_data %>%
  filter(!is.na(dn) & !is.na(manufacturingsharegdp))

model4 <- lm(log(dn) ~ manufacturingsharegdp +  factor(country) + factor(year) , data = model4_data)
saveRDS(model4, file = "models/model4.rds") # Save the model


#### Models by Grade ####

# Model for Grade A
model_a_data <- analysis_data %>%
  filter(!is.na(dn) & !is.na(gdp) & !is.na(country) & !is.na(year) & grade == "A")

model_a <- lm(log(dn) ~ log(gdp) + factor(country) + factor(year), data = model_a_data)
saveRDS(model_a, file = "models/model_a.rds") # Save the model

# Model for Grade B
model_b_data <- analysis_data %>%
  filter(!is.na(dn) & !is.na(gdp) & !is.na(country) & !is.na(year) & grade == "B")

model_b <- lm(log(dn) ~ log(gdp) + factor(country) + factor(year), data = model_b_data)
saveRDS(model_b, file = "models/model_b.rds") # Save the model

# Model for Grade C
model_c_data <- analysis_data %>%
  filter(!is.na(dn) & !is.na(gdp) & !is.na(country) & !is.na(year) & grade == "C")

model_c <- lm(log(dn) ~ log(gdp) + factor(country) + factor(year), data = model_c_data)
saveRDS(model_c, file = "models/model_c.rds") # Save the model

# Model for Grade D
model_d_data <- analysis_data %>%
  filter(!is.na(dn) & !is.na(gdp) & !is.na(country) & !is.na(year) & grade == "D")

model_d <- lm(log(dn) ~ log(gdp) + factor(country) + factor(year), data = model_d_data)
saveRDS(model_d, file = "models/model_d.rds") # Save the model

# Model for Grade F
model_f_data <- analysis_data %>%
  filter(!is.na(dn) & !is.na(gdp) & !is.na(country) & !is.na(year) & grade == "F")

model_f <- lm(log(dn) ~ log(gdp) + factor(country) + factor(year), data = model_f_data)
saveRDS(model_f, file = "models/model_f.rds")

#### Model Diagnostics ####


# Function to perform diagnostics for a given model
run_diagnostics <- function(model, model_name) {
  cat("### Diagnostics for", model_name, "###\n\n")
  
  # Linearity and Residual Plots
  par(mfrow = c(2, 2))
  plot(model, main = paste("Diagnostics for", model_name))
  par(mfrow = c(1, 1))
  
  # Normality of Residuals
  shapiro_test <- shapiro.test(residuals(model))
  cat("Shapiro-Wilk Test for Normality of Residuals:\n")
  print(shapiro_test)
  
  # Homoscedasticity (Breusch-Pagan Test)
  bp_test <- bptest(model)
  cat("\nBreusch-Pagan Test for Homoscedasticity:\n")
  print(bp_test)
  
  # Variance Inflation Factor (VIF) for Multicollinearity
  if ("lm" %in% class(model)) { # Check if model is linear
    vif_values <- vif(model)
    cat("\nVariance Inflation Factor (VIF):\n")
    print(vif_values)
  }
  
  # Cook's Distance for Influence
  cooks <- cooks.distance(model)
  cat("\nInfluential Observations (Cook's Distance > 0.5):\n")
  print(which(cooks > 0.5))
  
  # Model Performance Summary
  performance_metrics <- performance::check_model(model)
  cat("\nModel Performance Summary:\n")
  print(performance_metrics)
}

# Run diagnostics for each model
models <- list(
  Model1 = readRDS(here("models/model1.rds")),
  Model2 = readRDS(here("models/model2.rds")),
  Model3 = readRDS(here("models/model3.rds")),
  Model4 = readRDS(here("models/model4.rds")),
  ModelA = readRDS(here("models/model_a.rds")),
  ModelB = readRDS(here("models/model_b.rds")),
  ModelC = readRDS(here("models/model_c.rds")),
  ModelD = readRDS(here("models/model_d.rds")),
  ModelF = readRDS(here("models/model_f.rds"))
)

for (model_name in names(models)) {
  run_diagnostics(models[[model_name]], model_name)
}



