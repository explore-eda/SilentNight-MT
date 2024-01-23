# Set Main file as Working Directory
# Install the package
install.packages("readxl")
install.packages("xportr")

#install.packages(c('dplyr', 'labelled', 'xportr', 'admiral', 'rlang', 'readxl'))
# Load the package
library(readxl)
library(xportr)
library(tidyverse)

library(tibble)

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

#Check dataframe label
attr(xTS, "label")



# Function to check dataframe metadata
contents <- function(dat) {
  
  # Define a function to give the contents of a row
  row_contents <- function(m, dat) {
    # Pull out the column
    var <- dat[[m]]
    # Make a data frame of the content of interest
    as.data.frame(
      list(
        # Variable name was passed in as a string
        Variable = m,
        # Variable class
        Class =  class(var),
        # Label 
        Label = ifelse(is.null(attr(var, 'label')), '', attr(var, 'label')),
        #Format
        Format = ifelse(is.null(attr(var, 'SASformat')), '', attr(var, 'SASformat'))
      )
    )
  }
  
  # Map the function over all contents and build a data.frame
  purrr::map_dfr(names(dat), row_contents, dat=dat)
}

# Check metadata
xTS %>%
  contents()

# Output to specified path
xportr_write(xTS, "dev/output/ts.xpt")

########

