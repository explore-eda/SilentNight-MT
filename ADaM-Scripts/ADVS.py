import pandas as pd
import os
import json
import sys
from datetime import datetime

VS_path = '/Users/haoxiang/Desktop/SilentNight-MT/transformed_files/Transformed_VS.xlsx'
DM_path = '/Users/haoxiang/Desktop/SilentNight-MT/transformed_files/Transformed_DM.xlsx'
ADSL_path = '/Users/haoxiang/Desktop/SilentNight-MT/transformed_files/Transformed_ADSL.xlsx'
spec_path = '/Users/haoxiang/Desktop/SilentNight-MT/XYZ_ADaM_Spec_v1.xlsx'

passed_sheet_name = 'ADVS'

VS_data = pd.read_excel(VS_path, sheet_name='Sheet1')
DM_data = pd.read_excel(DM_path, sheet_name='Sheet1')
ADSL_data = pd.read_excel(ADSL_path, sheet_name='Sheet1')
spec_data = pd.read_excel(spec_path, sheet_name=passed_sheet_name)

transformed_data = pd.DataFrame(index=VS_data.index)

transformed_data['STUDYID'] = VS_data['STUDYID']
transformed_data['DOMAIN'] = passed_sheet_name
transformed_data['USUBJID'] = VS_data['USUBJID']
transformed_data['SUBJID'] = DM_data.set_index('USUBJID').loc[transformed_data['USUBJID'], 'SUBJID'].values
transformed_data['VSSEQ'] = VS_data['VSSEQ']
transformed_data['PARAM'] = VS_data['VSTEST']
transformed_data['PARAMCD'] = VS_data['VSTESTCD']
transformed_data['AVAL'] = VS_data['VSORRES']
transformed_data['AVALC'] = VS_data['VSSTRESC']
transformed_data['VISIT'] = VS_data['VISIT']
transformed_data['VISITNUM'] = VS_data['VISITNUM']
# transformed_data['DTYPE'] = '?'
# transformed_data['AVISIT'] = 'AVISIT' 
# transformed_data['AVISITN'] = 'null'
# transformed_data['TRTC'] = 'null' 
# transformed_data['ANL01FL'] = '?'

for flag in ['FASFL', 'SAFFL', 'ITTFL', 'COMPLFL', 'ENRLFL']:
    transformed_data[flag] = ADSL_data.set_index('USUBJID').loc[transformed_data['USUBJID'], flag].values


# Fill unmapped variables with "null"
spec_data['Variable Name'] = spec_data['Variable Name'].str.strip()
required_columns = spec_data['Variable Name'].dropna().tolist()

# Temporarily remove variables/columns that are not hidden in the specification file
excluded_columns = ['DTYPE', 'AVISIT', 'AVISITN', 'TRTC', 'ANL01FL']
required_columns = [col for col in required_columns if col not in excluded_columns]
transformed_data = transformed_data[required_columns]

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

domain_names = {passed_sheet_name: "Analysis Dataset of Vital Signs"}
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

output_directory = f"/Users/haoxiang/Desktop/SilentNight-MT/ADaM_JSON_files/dataset_json_{passed_sheet_name}/"
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