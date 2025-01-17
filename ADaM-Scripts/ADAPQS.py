import pandas as pd
import os
import json
import sys
from datetime import datetime

APQS_path = '/Users/haoxiang/Desktop/SilentNight-MT/transformed_files/Transformed_APQS.xlsx'
ADSL_path = '/Users/haoxiang/Desktop/SilentNight-MT/transformed_files/Transformed_ADSL.xlsx'
spec_path = '/Users/haoxiang/Desktop/SilentNight-MT/XYZ_ADaM_Spec_v1.xlsx'

passed_sheet_name = 'ADAPQS'

APQS_data = pd.read_excel(APQS_path, sheet_name='Sheet1')
ADSL_data = pd.read_excel(ADSL_path, sheet_name='Sheet1')
spec_data = pd.read_excel(spec_path, sheet_name=passed_sheet_name)

transformed_data = pd.DataFrame(index=APQS_data.index)

transformed_data['STUDYID'] = APQS_data['STUDYID']
transformed_data['DOMAIN'] = passed_sheet_name
transformed_data['APID'] = APQS_data['APID']
transformed_data['RSUBJID'] = APQS_data['RSUBJID']
transformed_data['QSSEQ'] = APQS_data['QSSEQ']
transformed_data['PARAM'] = APQS_data['QSTEST']
transformed_data['PARAMCD'] = APQS_data['QSTESTCD']
transformed_data['PARCAT1'] = APQS_data['QSCAT']
transformed_data['AVAL'] = APQS_data['QSORRES']
transformed_data['AVALC'] = APQS_data['QSSTRESC']

transformed_data['QSDT'] = pd.to_datetime(APQS_data['QSDTC'], errors='coerce').dt.strftime('%m/%d/%Y')
transformed_data['ADT'] = transformed_data['QSDT']

transformed_data['VISITNUM'] = APQS_data['VISITNUM']

for flag in ['FASFL', 'SAFFL', 'ITTFL', 'COMPLFL', 'ENRLFL']:
    if flag in ADSL_data.columns:
        transformed_data[flag] = transformed_data['RSUBJID'].map(
            ADSL_data.set_index('SUBJID')[flag]
        ).fillna('null')
    else:
        print(f"Warning: Column {flag} is missing in ADSL_data. Filling with 'null'.")
        transformed_data[flag] = 'null'

# Fill unmapped variables with "null"
spec_data['Variable Name'] = spec_data['Variable Name'].str.strip()
required_columns = spec_data['Variable Name'].dropna().tolist()

excluded_columns = ['VISIT', 'DTYPE', 'AVISIT', 'AVISITN', 'TRTC', 'ANL01FL']
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

domain_names = {passed_sheet_name: "Analysis Dataset of Associated Person of Questionnaires"}
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