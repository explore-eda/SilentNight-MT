# Set Main file as Working Directory
# Install the package
install.packages("readxl")
install.packages("xportr")

#install.packages(c('dplyr', 'labelled', 'xportr', 'admiral', 'rlang', 'readxl'))
# Load the package
library(readxl)
library(xportr)
library(tidyverse)

# Clean up environment - remove all objects
rm(list=ls())

# Read the excel
TS <- read_xlsx("data/Trial Design Domains.xlsx", sheet = 1) 
# %>%
#  rename(type = "Data Type") %>%
#  set_names(tolower)
TE <- read_xlsx("data/Trial Design Domains.xlsx", sheet = 2)
TA <- read_xlsx("data/Trial Design Domains.xlsx", sheet = 3)
TV <- read_xlsx("data/Trial Design Domains.xlsx", sheet = 4)
TI <- read_xlsx("data/Trial Design Domains.xlsx", sheet = 5)

########
# Final outputs

# Create dataset level metadata dataframe
ts_dlm <- tribble(
  ~dataset, ~label,
  "ts",   "Trial Summary"
)

# Create variable level metadata dataframe
vlm <- tribble(
  ~dataset, ~variable, ~label, ~type, ~format,
  'ts', 'STUDYID', 'Study Identifier', 'character', NA_character_,
  'ts', 'DOMAIN', 'Domain Abbreviation', 'character', NA_character_,
  'ts', 'TSSEQ', 'Sequence Number', 'numeric', NA_character_,
  'ts', 'TSGRPID', 'Group ID', 'character', NA_character_,
  'ts', 'TSPARMCD', 'Trial Summary Parameter Short Name', 'character', NA_character_,
  'ts', 'TSPARM', 'Trial Summary Parameter', 'character', NA_character_,
  'ts', 'TSVAL', 'Parameter Value', 'character', NA_character_,
  'ts', 'TSVALCD', 'Parameter Value Code', 'character', NA_character_,
  'ts', 'TSVCDREF', 'Name of the Reference Terminology', 'character', NA_character_,
  'ts', 'TSVCDVER', 'Version of the Reference Terminology', 'character', NA_character_,
)


xTS <- TS %>%
  select(STUDYID, DOMAIN, TSSEQ, TSGRPID, TSPARMCD, 
         TSPARM, TSVAL, TSVALCD, TSVCDREF, TSVCDVER) %>%
  # Apply dataframe label
  xportr_df_label(ts_dlm, domain = "ts") %>%
  # Apply variable labels
  xportr_label(vlm, domain = "ts") %>%
  # Apply variable types
  xportr_type(vlm, domain = "ts") %>%
  # Apply variable formats
  xportr_format(vlm, domain = "ts") %>%
  # Change any numeric NAs to empty strings ("")
  mutate(across(where(is.character), ~replace(., is.na(.), ""))) 

########
# Export as xpt

ts_metadata <- data.frame(
  dataset = "Trial Summary",
  variable = c("Subj", "Param", "Val", "NotUsed"),
  type = c("numeric", "character", "numeric", "character"),
  format = NA
)

install.packages(c('dplyr', 'labelled', 'xportr', 'admiral', 'rlang', 'readxl'))
