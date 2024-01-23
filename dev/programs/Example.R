####----
# Program Name: adsl
# Protocol Name: ADaM Code Library
# Author Name: Atorus Academy
# Creation Date: 19OCT2021
# Modification: <DDMMMYYYY> by <programmer initials>: <summary of changes>
####----

library(tidyverse)
library(lubridate)
library(xportr)

# Clean up environment - remove all objects
rm(list=ls())

# function to group together missing strings and NAs
str_missing <- function(s) {
  is.na(s) | str_trim(s) == ""
}

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

# load files from the phuse test data a factory 
dm <- haven::read_xpt(url("https://github.com/phuse-org/TestDataFactory/raw/main/Updated/TDF_SDTM/dm.xpt"))
ex <- haven::read_xpt(url("https://github.com/phuse-org/TestDataFactory/raw/main/Updated/TDF_SDTM/ex.xpt"))
vs <- haven::read_xpt(url("https://github.com/phuse-org/TestDataFactory/raw/main/Updated/TDF_SDTM/vs.xpt"))
ds <- haven::read_xpt(url("https://github.com/phuse-org/TestDataFactory/raw/main/Updated/TDF_SDTM/ds.xpt"))

# Get records/columns from DM and derive associated variables 
demo <- dm %>%
  # Keep only necessary columns
  select(STUDYID, USUBJID, SUBJID, SITEID, AGE, AGEU, SEX, RACE, ETHNIC, 
         DTHDTC, ARM, ACTARM) %>%
  mutate(
    # Use conditions to create age group variables
    AGEGR1 = case_when(
      AGE < 65 ~ "<65",
      AGE >= 65 & AGE <= 70 ~ "65-70",
      AGE >70 ~ ">70"
    ),
    AGEGR1N = case_when(
      AGEGR1 == "<65" ~ 1,
      AGEGR1 == "65-70" ~ 2,
      AGEGR1 == ">70" ~ 3
    ),
    # Use conditions to create race numeric variable   
    RACEN = case_when(
      RACE == "WHITE" ~ 1,
      RACE == "BLACK OR AFRICAN AMERICAN" ~ 2,
      RACE == "ASIAN" ~ 3,
      RACE == "AMERICAN INDIAN OR ALASKA NATIVE" ~ 4,
      RACE == "NATIVE HAWAIIAN OR OTHER PACIFIC ISLANDER" ~ 5,
      RACE == "UNKNOWN" ~ 6
    ),
    # Use conditions to create treatment variables  
    TRT01P = ARM, 
    TRT01PN = case_when(
      TRT01P == "Placebo" ~ 0,
      TRT01P == "Xanomeline Low Dose" ~ 6,
      TRT01P == "Xanomeline High Dose" ~ 9
    ),
    TRT01A = ACTARM,
    TRT01AN = case_when(
      TRT01A == "Placebo" ~ 0,
      TRT01A == "Xanomeline Low Dose" ~ 6,
      TRT01A == "Xanomeline High Dose" ~ 9 
    ),
    # Set ITTFL to Y if ARM is not missing
    ITTFL = if_else(str_missing(ARM) == FALSE, "Y", "N"),
    # Convert the DTHDTC variable into a date
    DTHDT = as_date(DTHDTC)
  ) %>%
  # Drop columns that are no longer needed
  select(-DTHDTC)

# Add variables from EX
# Treatment dates
treat <- ex %>% 
  # Keep only necessary columns
  select(USUBJID, EXSTDTC, EXENDTC) %>% 
  # Convert the DTC variables into dates
  mutate(
    across(ends_with("DTC"), ~ as_date(.))
  ) %>% 
  # Within a subject, 
  # get the first treatment start date and the last treatment end date
  group_by(USUBJID) %>% 
  summarize(
    TRTSDT = min(EXSTDTC, na.rm = TRUE), 
    TRTEDT = max(EXENDTC, na.rm = TRUE)  
  ) %>% 
  mutate(
    # min() and max() can create Inf values, if created change to NA
    TRTSDT = replace(TRTSDT, is.infinite(TRTSDT), NA_Date_),
    TRTEDT = replace(TRTEDT, is.infinite(TRTEDT), NA_Date_),
    # Calculate treatment duration
    TRTDURD = as.numeric(TRTEDT - TRTSDT + (TRTEDT >= TRTSDT)) 
  )

# Dosing information
dose <- ex %>%
  # Within a subject, get the sum of doses
  group_by(USUBJID) %>%
  summarize(DOSE01A=sum(EXDOSE)) %>%
  # Set DOSE01U to mg
  mutate(DOSE01U = "mg") %>%
  # Keep only necessary columns
  select(USUBJID, DOSE01A, DOSE01U)

# Add the EX variables to the main dataframe 
# `reduce()` can be used to merge more than 2 dataframes
demo1 <- reduce(list(demo, treat, dose), left_join, by = "USUBJID") %>%
  # Set SAFFL to Y if ITTFL=Y and TRTSDT is not missing
  mutate(SAFFL = if_else(ITTFL == "Y" & str_missing(TRTSDT) == FALSE, "Y", "N"))

# Add variables from DS
# Add the DS variables to the main dataframe
demo2 <- left_join(demo1, 
                   ds %>% 
                     # Keep only necessary records and columns 
                     filter(DSCAT == "DISPOSITION EVENT") %>%
                     select(USUBJID, DSCAT, DSDECOD, DSTERM), 
                   by = "USUBJID") %>%
  mutate(
    # Set EOSSTT to ONGOING if no DS record, COMPLETED if DSDECOD = COMPLETED,
    # otherwise DISCONTINUED
    EOSSTT = case_when(
      str_missing(DSDECOD) == TRUE ~ "ONGOING",
      DSDECOD == "COMPLETED" ~ "COMPLETED",
      TRUE ~ "DISCONTINUED"
    ),
    # Set DCSREAS/ DCSRESP to DSDECOD/DSTERM when DSDECOD is not = COMPLETED
    DCSREAS = if_else(DSDECOD != "COMPLETED", DSDECOD, "", missing=""),
    DCSREASP = if_else(DSDECOD != "COMPLETED", DSTERM, "", missing=""),
    # Set SCRNFL = Y for DSDECOD = SCREEN FAILURE, otherwise N
    # Consider missing values
    SCRNFL = if_else(DSDECOD == "SCREEN FAILURE", "Y", "N", missing="N")
  ) %>%
  # Drop columns that are no longer needed
  select(-c(DSCAT, DSDECOD, DSTERM))

# Add variables from VS
weight <- vs %>%
  # Keep only necessary records and columns for baseline weight
  filter(VSTESTCD == "WEIGHT", VISITNUM == 3) %>%
  select(USUBJID, WEIGHTBL = VSSTRESN)

height <- vs %>%
  # Keep only necessary records and columns for baseline height
  filter(VSTESTCD == "HEIGHT", VISITNUM == 1) %>%
  select(USUBJID, HEIGHTBL = VSSTRESN)

# Add the VS variables to the main dataframe 
# `reduce()` can be used to merge more than 2 dataframes
demo3 <- reduce(list(demo2, weight, height), left_join, by = "USUBJID") %>%
  mutate(
    # Calculate BMIBL
    BMIBL = (WEIGHTBL / HEIGHTBL / HEIGHTBL) * 10000,
    # Use conditions to create BMI group variable
    BMIGR1 = case_when(
      BMIBL < 25 ~ "<25",
      BMIBL >= 25 ~ ">=25"
    )
  )

# Finalize and output
# Create dataset level metadata dataframe
dlm <- tribble(
  ~dataset, ~label,
  "adsl",   "Subject-Level Analysis Dataset"
)

# Create variable level metadata dataframe
vlm <- tribble(
  ~dataset, ~variable, ~label, ~type, ~format,
  'adsl', 'STUDYID', 'Study Identifier', 'character', NA_character_,
  'adsl', 'USUBJID', 'Unique Subject Identifier', 'character', NA_character_,
  'adsl', 'SUBJID', 'Subject Identifier for the Study', 'character', NA_character_,
  'adsl', 'SITEID', 'Study Site Identifier', 'character', NA_character_,
  'adsl', 'AGE', 'Age', 'numeric', NA_character_,
  'adsl', 'AGEU', 'Age Units', 'character', NA_character_,
  'adsl', 'AGEGR1', 'Pooled Age Group 1', 'character', NA_character_,
  'adsl', 'AGEGR1N', 'Pooled Age Group 1 (N)', 'numeric', NA_character_,
  'adsl', 'SEX', 'Sex', 'character', NA_character_,
  'adsl', 'RACE', 'Race', 'character', NA_character_,
  'adsl', 'RACEN', 'Race (N)', 'numeric', NA_character_,
  'adsl', 'ETHNIC', 'Ethnicity', 'character', NA_character_,
  'adsl', 'SAFFL', 'Safety Population Flag', 'character', NA_character_,
  'adsl', 'ITTFL', 'Intent-To-Treat Population Flag', 'character', NA_character_,
  'adsl', 'SCRNFL', 'Screen Failure Population Flag', 'character', NA_character_,
  'adsl', 'ARM', 'Description of Planned Arm', 'character', NA_character_,
  'adsl', 'ACTARM', 'Description of Actual Arm', 'character', NA_character_,
  'adsl', 'TRT01P', 'Planned Treatment for Period 01', 'character', NA_character_,
  'adsl', 'TRT01PN', 'Planned Treatment for Period 01 (N)', 'numeric', NA_character_,
  'adsl', 'TRT01A', 'Actual Treatment for Period 01', 'character', NA_character_,
  'adsl', 'TRT01AN', 'Actual Treatment for Period 01 (N)', 'numeric', NA_character_,
  'adsl', 'DOSE01A', 'Actual Treatment Dose for Period 01', 'numeric', NA_character_,
  'adsl', 'DOSE01U', 'Units for Dose for Period 01', 'character', NA_character_,
  'adsl', 'TRTSDT', 'Date of First Exposure to Treatment', 'Date', "DATE9.",
  'adsl', 'TRTEDT', 'Date of Last Exposure to Treatment', 'Date', "DATE9.",
  'adsl', 'TRTDURD', 'Total Treatment Duration (Days)', 'numeric', NA_character_,
  'adsl', 'EOSSTT', 'End of Study Status', 'character', NA_character_,
  'adsl', 'DCSREAS', 'Reason for Discontinuation from Study', 'character', NA_character_,
  'adsl', 'DCSREASP', 'Reason Spec for Discont from Study', 'character', NA_character_,
  'adsl', 'DTHDT', 'Date of Death', 'Date', "DATE9.",
  'adsl', 'WEIGHTBL', 'Weight (kg) at Baseline', 'numeric', NA_character_,
  'adsl', 'HEIGHTBL', 'Height (cm) at Baseline', 'numeric', NA_character_,
  'adsl', 'BMIBL', 'Body Mass Index (kg/m2) at Baseline ', 'numeric', NA_character_,
  'adsl', 'BMIGR1', 'Pooled BMI Group 1', 'character', NA_character_
)

adsl <- demo3 %>%
  # Order variables
  select(STUDYID, USUBJID, SUBJID, SITEID, AGE, AGEU, AGEGR1, AGEGR1N, SEX, RACE, 
         RACEN, ETHNIC, SAFFL, ITTFL, SCRNFL, ARM, ACTARM, TRT01P, TRT01PN, TRT01A,
         TRT01AN, DOSE01A, DOSE01U, TRTSDT, TRTEDT, TRTDURD, EOSSTT, DCSREAS,
         DCSREASP, DTHDT, WEIGHTBL, HEIGHTBL, BMIBL, BMIGR1) %>%
  # Sort by keys
  arrange(USUBJID) %>%
  # Apply dataframe label
  xportr_df_label(dlm, domain = "adsl") %>%
  # Apply variable labels
  xportr_label(vlm, domain = "adsl") %>%
  # Apply variable types
  xportr_type(vlm, domain = "adsl") %>%
  # Apply variable formats
  xportr_format(vlm, domain = "adsl") %>%
  # Change any numeric NAs to empty strings ("")
  mutate(across(where(is.character), ~replace(., is.na(.), ""))) 

#Check dataframe label
attr(adsl, "label")

# Check metadata
adsl %>%
  contents()

# Output to specified path
xportr_write(adsl, "./adam/adsl.xpt")