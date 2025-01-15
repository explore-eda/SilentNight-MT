import pandas as pd
import os
import json
import sys
from datetime import datetime

# Paths
DM_raw_data_path = '/Users/haoxiang/Desktop/SilentNight-MT/raw_data/Demographic.xlsx'
DM_path = '/Users/haoxiang/Desktop/SilentNight-MT/transformed_files/Transformed_DM.xlsx'
SC_path = '/Users/haoxiang/Desktop/SilentNight-MT/transformed_files/Transformed_SC.xlsx'
PR_path = '/Users/haoxiang/Desktop/SilentNight-MT/transformed_files/Transformed_PR.xlsx'
DS_path = '/Users/haoxiang/Desktop/SilentNight-MT/transformed_files/Transformed_DS.xlsx'
SE_path = '/Users/haoxiang/Desktop/SilentNight-MT/transformed_files/Transformed_SE.xlsx'
spec_path = '/Users/haoxiang/Desktop/SilentNight-MT/XYZ_ADaM_Spec_v1.xlsx'

passed_sheet_name = 'ADSL'

# Load data
DM_raw_data = pd.read_excel(DM_raw_data_path, sheet_name='Sheet1')
DM_data = pd.read_excel(DM_path, sheet_name='Sheet1')
SC_data = pd.read_excel(SC_path, sheet_name='Sheet1')
PR_data = pd.read_excel(PR_path, sheet_name='Sheet1')
DS_data = pd.read_excel(DS_path, sheet_name='Sheet1')
SE_data = pd.read_excel(SE_path, sheet_name='Sheet1')
spec_data = pd.read_excel(spec_path, sheet_name=passed_sheet_name)

transformed_data = pd.DataFrame(index=DM_data.index)

merged_data = pd.merge(DM_data, DM_raw_data[['Subject_ID', 'Subject_DOB']], left_on='SUBJID', right_on='Subject_ID', how='left')

transformed_data['STUDYID'] = DM_data['STUDYID']
transformed_data['DOMAIN'] = 'ADSL'
transformed_data['USUBJID'] = DM_data['USUBJID']
transformed_data['SUBJID'] = DM_data['SUBJID']
transformed_data['SITEID'] = DM_data['SITEID']
transformed_data['AGE'] = DM_data['AGE']
transformed_data['AGEU'] = DM_data['AGEU']
transformed_data['BRTHDTC'] = merged_data['Subject_DOB']
transformed_data['BRTHDT'] = pd.to_datetime(transformed_data['BRTHDTC']).dt.strftime('%m/%d/%Y')
transformed_data['SEX'] = DM_data['SEX']
transformed_data['RACE'] = DM_data['RACE']
transformed_data['ETHNIC'] = DM_data['ETHNIC']

# Mapping OCCUPTN (Employee Job) and EDULVL (Education Level) based on SCTESTCD values
sc_empjob = SC_data[SC_data['SCTESTCD'] == 'EMPJOB'][['USUBJID', 'SCORRES']].rename(columns={'SCORRES': 'OCCUPTN'})
sc_edulevel = SC_data[SC_data['SCTESTCD'] == 'EDULEVEL'][['USUBJID', 'SCORRES']].rename(columns={'SCORRES': 'EDULVL'})

transformed_data = pd.merge(transformed_data, sc_empjob, on='USUBJID', how='left')
transformed_data = pd.merge(transformed_data, sc_edulevel, on='USUBJID', how='left')
transformed_data['COUNTRY'] = DM_data['COUNTRY']
transformed_data['FASFL'] = 'null'
transformed_data['SAFFL'] = DM_data['ACTARMCD'].notna().apply(lambda x: 'Y' if x else None)
transformed_data['ITTFL'] = DM_data['ARMCD'].notna().apply(lambda x: 'Y' if x else None)
transformed_data['COMPLFL'] = 'Y'
transformed_data['ENRLFL'] = DM_data['RFICDTC'].notna().apply(lambda x: 'Y' if x else None)
transformed_data['DTHFL'] = DM_data['DTHFL']

transformed_data['RFICDT'] = pd.to_datetime(DM_data['RFICDTC'], errors='coerce').dt.strftime('%m/%d/%Y')
transformed_data['RFPENDT'] = pd.to_datetime(DM_data['RFPENDTC'], errors='coerce').dt.strftime('%m/%d/%Y')

transformed_data['ARM'] = DM_data['ARM']
transformed_data['TRT01P'] = 'Mute'
transformed_data['TRT02P'] = 'MyTAP'
transformed_data['TRT03P'] = 'SPT'
transformed_data['TRT01A'] = 'Mute'
transformed_data['TRT02A'] = 'MyTAP'
transformed_data['TRT03A'] = 'SPT'
transformed_data['TRTSDT'] = pd.to_datetime(PR_data.loc[PR_data['PRTRT'] == 'MUTE', 'PRSTDTC'], errors='coerce').dt.strftime('%m/%d/%Y')
transformed_data['TRTSDTM'] = transformed_data['TRTSDT']
transformed_data['TR01SDT'] = pd.to_datetime(PR_data.loc[PR_data['PRTRT'] == 'MUTE', 'PRSTDTC'], errors='coerce').dt.strftime('%m/%d/%Y')
transformed_data['TR02SDT'] = pd.to_datetime(PR_data.loc[PR_data['PRTRT'] == 'MYTAP', 'PRSTDTC'], errors='coerce').dt.strftime('%m/%d/%Y')
transformed_data['TR03SDT'] = pd.to_datetime(PR_data.loc[PR_data['PRTRT'] == 'SPT', 'PRSTDTC'], errors='coerce').dt.strftime('%m/%d/%Y')
transformed_data['TR01EDT'] = pd.to_datetime(PR_data.loc[PR_data['PRTRT'] == 'MUTE', 'PRENDTC'], errors='coerce').dt.strftime('%m/%d/%Y')
transformed_data['TR02EDT'] = pd.to_datetime(PR_data.loc[PR_data['PRTRT'] == 'MYTAP', 'PRENDTC'], errors='coerce').dt.strftime('%m/%d/%Y')
transformed_data['TR03EDT'] = pd.to_datetime(PR_data.loc[PR_data['PRTRT'] == 'SPT', 'PRENDTC'], errors='coerce').dt.strftime('%m/%d/%Y')

transformed_data['TRT01PN'] = 1
transformed_data['TRT02PN'] = 2
transformed_data['TRT03PN'] = 3
transformed_data['TRT01AN'] = transformed_data['TRT01A'].apply(lambda x: 1 if x == 'Mute' else None)
transformed_data['TRT02AN'] = transformed_data['TRT02A'].apply(lambda x: 2 if x == 'MyTAP' else None)
transformed_data['TRT03AN'] = transformed_data['TRT03A'].apply(lambda x: 3 if x == 'SPT' else None)
ds_decod_merged = pd.merge(transformed_data[['USUBJID']], DS_data[['USUBJID', 'DSDECOD', 'DSTERM']], 
                           on='USUBJID', how='left')
transformed_data['DCSREASP'] = ds_decod_merged['DSTERM']
transformed_data['DCSREAS'] = ds_decod_merged['DSDECOD']

transformed_data['ASEX'] = transformed_data['SEX'].apply(lambda x: 1 if x == 'M' else 2 if x == 'F' else None)
transformed_data['ARACE'] = transformed_data['RACE'].map({
    "American Indian or Alaska Native": 1,
    "Asian": 2,
    "Black or African American": 3,
    "Native Hawaiian or Other Pacific Islander": 4,
    "White": 5,
    "Not Reported": 6,
    "Unknown": 7,
    "Other": 8
})

# # Fill unmapped variables with "null"
spec_data['Variable Name'] = spec_data['Variable Name'].str.strip()
required_columns = spec_data['Variable Name'].dropna().tolist()
for column in required_columns:
    if column not in transformed_data.columns:
        transformed_data[column] = "null"

# Reorder columns to match the specification order
transformed_data = transformed_data[required_columns]

output_path = f'/Users/haoxiang/Desktop/SilentNight-MT/transformed_files/Transformed_{passed_sheet_name}.xlsx'
os.makedirs(os.path.dirname(output_path), exist_ok=True)
transformed_data.to_excel(output_path, index=False)

print(f"Transformed data saved to {output_path}")


# Begin conversion to JSON
xlsx_path = output_path
specification_path = spec_path

xls = pd.ExcelFile(xlsx_path)
spec_xls = pd.ExcelFile(specification_path)
spec_data = pd.read_excel(specification_path, sheet_name=passed_sheet_name)

domain_names = {passed_sheet_name: "Analysis Dataset of Subject-Level"}
required_columns = spec_data['Variable Name'].dropna().tolist()
sdtm_domains = {passed_sheet_name: required_columns}

sheet_name_mapping = {"Sheet1": passed_sheet_name}

spec_data = pd.concat([spec_xls.parse(sheet) for sheet in spec_xls.sheet_names])
variable_labels = {}
for sheet_name in domain_names:
    sheet_data = spec_xls.parse(sheet_name)
    if "Variable Name" in sheet_data.columns and "Variable Label" in sheet_data.columns:
        variable_labels.update(
            sheet_data.set_index("Variable Name")["Variable Label"].to_dict()
        )

output_directory = f"/Users/haoxiang/Desktop/SilentNight-MT/dataset_json_{passed_sheet_name}/"
os.makedirs(output_directory, exist_ok=True)

python_version = sys.version.split()[0]
last_modified_timestamp = os.path.getmtime(xlsx_path)
db_last_modified_datetime = datetime.fromtimestamp(last_modified_timestamp).strftime("%Y-%m-%dT%H:%M:%S")
sheet_row_counts = {sheet_name: pd.read_excel(xlsx_path, sheet_name=sheet_name).shape[0] for sheet_name in xls.sheet_names}

for sheet_name in xls.sheet_names:
    mapped_domain = sheet_name_mapping.get(sheet_name)
    if mapped_domain in sdtm_domains:
        df = xls.parse(sheet_name)

        required_columns = sdtm_domains[mapped_domain]
        for col in required_columns:
            if col not in df.columns:
                df[col] = None

        # Ensure correct column order
        df = df[required_columns].where(pd.notnull(df), None)
        for col in df.select_dtypes(include=['datetime64', 'datetime']):
            df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')  # Convert datetime to ISO 8601 format

        # Metadata
        dataset_metadata = {
            "datasetJSONCreationDateTime": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "datasetJSONVersion": "1.1.0",
            "fileOID": f"XYZ_SDTM_{mapped_domain}",
            "dbLastModifiedDateTime": db_last_modified_datetime,
            "originator": "EDA Clinical",
            "sourceSystem": {"name": "Python", "version": python_version},
            "studyOID": "SilentNight",
            "metaDataVersionOID": "1.0",
            "metaDataRef": "define.xml",
            "itemGroupOID": f"IG.{mapped_domain}",
            "records": sheet_row_counts[sheet_name],
            "name": mapped_domain,
            "label": domain_names.get(mapped_domain, "Unknown Domain"),
        }

        # Build JSON structure
        json_data = dataset_metadata.copy()
        json_data["columns"] = [
            {
                "itemOID": f"IT.{mapped_domain}.{col}",
                "name": col,
                "label": variable_labels.get(col, "No Label"),
                "dataType": "string" if df[col].dtype == "object" else "integer",
                "length": int(df[col].astype(str).str.len().max() or 0),
                "keySequence": i + 1,
            }
            for i, col in enumerate(df.columns)
        ]

        json_data["rows"] = [
            [None if pd.isna(val) else val for val in row]
            for row in df.values.tolist()
        ]
        
        json_path = os.path.join(output_directory, f"{mapped_domain}.json")
        class DateTimeEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, datetime):
                    return obj.strftime('%Y-%m-%dT%H:%M:%S')  # Convert datetime to ISO 8601 string
                return super().default(obj)

        try:
            with open(json_path, 'w') as json_file:
                json.dump(json_data, json_file, indent=4, cls=DateTimeEncoder)
            print(f"JSON file written: {json_path}")
        except Exception as e:
            print(f"Error writing JSON file: {json_path}, Error: {str(e)}")