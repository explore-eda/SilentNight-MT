import pandas as pd
import os
import json
import sys
from datetime import datetime

CM_path = '/Users/haoxiang/Desktop/SilentNight-MT/transformed_files/Transformed_CM.xlsx'
DM_path = '/Users/haoxiang/Desktop/SilentNight-MT/transformed_files/Transformed_DM.xlsx'
ADSL_path = '/Users/haoxiang/Desktop/SilentNight-MT/transformed_files/Transformed_ADSL.xlsx'
spec_path = '/Users/haoxiang/Desktop/SilentNight-MT/XYZ_ADaM_Spec_v1.xlsx'

passed_sheet_name = 'ADCM'

CM_data = pd.read_excel(CM_path, sheet_name='Sheet1')
DM_data = pd.read_excel(DM_path, sheet_name='Sheet1')
ADSL_data = pd.read_excel(ADSL_path, sheet_name='Sheet1')
spec_data = pd.read_excel(spec_path, sheet_name=passed_sheet_name)

transformed_data = pd.DataFrame(index=CM_data.index)

transformed_data['STUDYID'] = CM_data['STUDYID']
transformed_data['SITEID'] = DM_data.set_index('USUBJID').loc[CM_data['USUBJID'], 'SITEID'].values
transformed_data['DOMAIN'] = passed_sheet_name
transformed_data['SUBJID'] = DM_data.set_index('USUBJID').loc[CM_data['USUBJID'], 'SUBJID'].values
transformed_data['USUBJID'] = CM_data['USUBJID']
transformed_data['CMTRT'] = CM_data['CMTRT']
transformed_data['CMDECOD'] = CM_data['CMDECOD']
transformed_data['CMINDC'] = CM_data['CMINDC']
transformed_data['CMDOSE'] = CM_data['CMDOSE']
transformed_data['CMDOSTXT'] = CM_data['CMDOSTXT']
transformed_data['CMDOSU'] = CM_data['CMDOSU']
transformed_data['CMDOSFRM'] = CM_data['CMDOSFRM']
transformed_data['CMDOSFRQ'] = CM_data['CMDOSFRQ']
transformed_data['CMROUTE'] = CM_data['CMROUTE']
transformed_data['CMONGO'] = CM_data['CMENRTPT'].apply(lambda x: 'Y' if x == 'ONGOING' else None)
transformed_data['CMSTDTC'] = pd.to_datetime(CM_data['CMSTDTC']).dt.strftime('%m/%d/%Y')
transformed_data['CMENDTC'] = pd.to_datetime(CM_data['CMENDTC']).dt.strftime('%m/%d/%Y')

# TODO: No such 'CMSTDTC' and 'CMENDTC' in the ADSL_data, so we will fill it with "null"
# transformed_data['ASTDT'] = pd.to_datetime(ADSL_data['CMSTDTC'], errors='coerce').dt.strftime('%Y-%m-%d')
# transformed_data['AENDT'] = pd.to_datetime(ADSL_data['CMENDTC'], errors='coerce').dt.strftime('%Y-%m-%d')

# TODO: No such 'ASTDT' and 'AENDT' in the ADSL_data, so we will fill it with "null"
# transformed_data['ADUR'] = (pd.to_datetime(ADSL_data['AENDT']) - pd.to_datetime(ADSL_data['ASTDT'])).dt.days

# TODO: No such a variable called "SCRNFL" in the ADSL_data, so we will fill it with "null"
required_flags = ['ENRLFL', 'ITTFL', 'SAFFL', 'FASFL', 'DTHFL']
for flag in required_flags:
    if flag in ADSL_data.columns:
        transformed_data[flag] = ADSL_data.set_index('USUBJID').loc[transformed_data['USUBJID'], flag].values
    else:
        print(f"Column {flag} is missing in ADSL_data. Filling with 'null'.")
        transformed_data[flag] = 'null'

transformed_data['TRT01A'] = ADSL_data.set_index('USUBJID').loc[transformed_data['USUBJID'], 'TRT01A'].values
transformed_data['TRT02A'] = ADSL_data.set_index('USUBJID').loc[transformed_data['USUBJID'], 'TRT02A'].values
transformed_data['TRT03A'] = ADSL_data.set_index('USUBJID').loc[transformed_data['USUBJID'], 'TRT03A'].values
transformed_data['TRT01P'] = ADSL_data.set_index('USUBJID').loc[transformed_data['USUBJID'], 'TRT01P'].values
transformed_data['TRT02P'] = ADSL_data.set_index('USUBJID').loc[transformed_data['USUBJID'], 'TRT02P'].values
transformed_data['TRT03P'] = ADSL_data.set_index('USUBJID').loc[transformed_data['USUBJID'], 'TRT03P'].values


# TODO: Since 'ASTDT' does not exist in the ADSL_data, I could not process the 'CONCOMFL', 'PRIORFL', and 'FUPFL' columns.
# transformed_data['CONCOMFL'] = transformed_data.apply(
#     lambda row: 'Y' if pd.notnull(row['ASTDT']) and pd.notnull(ADSL_data.loc[ADSL_data['USUBJID'] == row['USUBJID'], 'TR01SDT'].values[0]) and 
#                 pd.to_datetime(row['ASTDT']) >= pd.to_datetime(ADSL_data.loc[ADSL_data['USUBJID'] == row['USUBJID'], 'TR01SDT'].values[0]) 
#                 else 'N', axis=1
# )

# transformed_data['PRIORFL'] = transformed_data.apply(
#     lambda row: 'Y' if pd.notnull(row['ASTDT']) and pd.notnull(ADSL_data.loc[ADSL_data['USUBJID'] == row['USUBJID'], 'TR01SDT'].values[0]) and 
#                 pd.to_datetime(row['ASTDT']) < pd.to_datetime(ADSL_data.loc[ADSL_data['USUBJID'] == row['USUBJID'], 'TR01SDT'].values[0]) 
#                 else 'N', axis=1
# )

# transformed_data['FUPFL'] = transformed_data.apply(
#     lambda row: 'Y' if pd.notnull(row['ASTDT']) and pd.notnull(ADSL_data.loc[ADSL_data['USUBJID'] == row['USUBJID'], 'TR03EDT'].values[0]) and 
#                 pd.to_datetime(row['ASTDT']) > pd.to_datetime(ADSL_data.loc[ADSL_data['USUBJID'] == row['USUBJID'], 'TR03EDT'].values[0]) 
#                 else 'N', axis=1
# )

transformed_data['CMSEQ'] = CM_data['CMSEQ']

# Fill unmapped variables with "null"
spec_data['Variable Name'] = spec_data['Variable Name'].str.strip()
required_columns = spec_data['Variable Name'].dropna().tolist()
for column in required_columns:
    if column not in transformed_data.columns:
        transformed_data[column] = "null"

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

domain_names = {passed_sheet_name: "Analysis Dataset of Concomitant Medication"}
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