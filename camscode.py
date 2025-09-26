import base64, json
import vertexai
from vertexai.generative_models import GenerativeModel, Part, SafetySetting
import pandas as pd
from google.cloud import storage
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

### Variables ########

GEMINI_MODEL = "gemini-1.5-pro-002"
PROJECT_ID = "prj-cams-playground-vertexai"
LOCATION_ID = "asia-south1"
BATCH_SIZE = 10
RERUN = False
CHECK_ID = 'FATFCountryIdentification2FUNDSNET'
REPORT_TYPE = 'ETL'


##### Input Files #######

MAPPING_FILE = "table_name_mapping.csv"
COLUMNS_ALL = "ColumnsFull.csv"


#### Output Files #######
OUTPUT_CSV_FILE = "SQLQueriesGenAIExtract001.csv"  ##Change the name of the file as requried
CHECKPOINT_FILE = f"""{OUTPUT_CSV_FILE}_checkpoint.json"""
OUTPUT_TABLE_LIST = "TablesFromGenAI.csv"
OUTPUT_COLUMNS_LIST = "ColumnsFromGenAI.csv"
OUTPUT_TABLES_PER_REPORT = "TablesListPerReport.csv"
OUTPUT_COLUMNS_PER_REPORT = "ColumnsListPerReport.csv"




safety_settings = [
    SafetySetting(
        category=SafetySetting.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
        threshold=SafetySetting.HarmBlockThreshold.BLOCK_NONE
    ),
    SafetySetting(
        category=SafetySetting.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
        threshold=SafetySetting.HarmBlockThreshold.BLOCK_NONE
    ),
    SafetySetting(
        category=SafetySetting.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
        threshold=SafetySetting.HarmBlockThreshold.BLOCK_NONE
    ),
    SafetySetting(
        category=SafetySetting.HarmCategory.HARM_CATEGORY_HARASSMENT,
        threshold=SafetySetting.HarmBlockThreshold.BLOCK_NONE
    ),
]

def extract_details(sql_query):
    try:
        
        if len(sql_query) < 10:
            sql_query = 'No SQL'
            
        vertexai.init(project=PROJECT_ID, location=LOCATION_ID)
        model = GenerativeModel(
            GEMINI_MODEL,
            system_instruction=["""You a database analyst expert in writing and understanding sql queries. 
            You are also a business expert in financial investments like  mutual funds (MFs), 
            alternative investment funds (AIFs), insurance companies and other financial institutions."""])
        
        
        ##Change the prompt accordinly to extract various details
        
        extraction_prompt = f"""
        Analyze the provided Oracle SQL query and extract the following information (TableNames, Filters, Joins,UdfUsed, CaseStatements ) as JSON Formatted Output.
Make sure you extract all the details including from subqueries and CTEs. Return Empty if not valid SQLs.

Output JSON: 

{{
  "TableNamesAllTables"[
                  {{"Name":"table1",
                    "Alias": "If no alias add NA"
                    "Columns : [List of Columns name from this table that used anywhere in the SQL Text (SELECT or JOINS or FILTERS or SUBQUERIES)]}},
                    {{"Name": "table2",
                    "Alias": "If no alias add NA",
                    "Columns : [List of Columns name from this table that used anywhere in the SQL Text (SELECT or JOINS or FILTERS or SUBQUERIES)]}}.
                    ...List all the tables
                    {{"Name": "UNKNOWN", #Last value should be for those columns which cannot be identified
                    "Alias": "If no alias add NA",
                    "Columns : [List of Columns names which you cannot understand which table they are from and used anywhere in the SQL Text (SELECT or JOINS or FILTERS or SUBQUERIES)]}}
                ],
  "TableNamesDerivedTables": [List of Derived tables or CTE Names.],
  
  "Filters": [
    {{
      "Field": "filter_field",
      "Condition": "filter_condition (e.g., =, >, <)",
      "Value": "filter_value"
    }},
    // ... more filters
  ],
  "Joins": [
    {{
      "Type": "join_type" (Valid List: INNER JOIN, LEFT JOIN, RIGHT JOIN, OUTER JOIN),
      "Tables": ["table1", "table2"],
      "Condition": "Join Condition" (Include EVERY Join Condition between 
                  all the tables present in any part of the query. Names along with full Table Name
                  [DO NOT USE Alais] and full Column names 
                  e.g., "TABLE_NAME_1.COLUMN_NAME = TABLE_NAME_2.COLUMN_NAME
                  In case the join alias is from a CTE include the table name 
                  from the CTE that has the joined column. 
                  Dot not add the CTE alias as Table Name.")
    }},
    // ... more joins
  ],
  "UdfUsed": [function/udf used-1, function/udf used-2...],
  "CaseStatements": [ 
    {{
      "CaseLogic": "Case statement logic",
      "FinalAlias": "Final alias for the case statement"
      "ColumnListUsedInCaseLogic":[Column1, Column2,...]
    }}, ##If more case logics
  ]
}}

       ORACLE SQL:
            {sql_query.replace("{","{{").replace("}","}}")}

"""
            
        # print("R")
        # print(extraction_prompt)
    
        generation_config = {
        "max_output_tokens": 8192,
        "temperature": 1,
        "top_p": 0.9,
    }
        # print(extraction_prompt)
        responses = model.generate_content(
            [extraction_prompt],
            generation_config=generation_config,
            # safety_settings=safety_settings,
            stream=False,
        )
        
        return responses.text.replace("```","").replace("json","")
    except Exception as e:
        return "Response Generation Error"


import os
current_dir = os.getcwd()
def process_sql_from_gcs(RType,RId):
    try:
        if RType=="MIS":
            filepath=os.path.join(current_dir, "../SourceQueries/MISReports",f'''{RId}.txt''')     
            with open(filepath, 'r') as f:  # Use 'r' for reading
                content_text = f.read()
        elif RType=="ROR":
            filepath=os.path.join(current_dir, "../SourceQueries","Consolidated Source Reports.csv")
            ror_df = pd.read_csv(filepath)
            ror_row = ror_df[ror_df['LinkID'] == RId]
            content_text=ror_row["SQL"].iloc[0]
        else:
            filepath=os.path.join(current_dir, "../SourceQueries","ETLQueries.csv")
            etl_df = pd.read_csv(filepath)
            etl_row = etl_df[etl_df['LinkID'] == RId]
            # print(etl_row)
            content_text=etl_row["RF Query"].iloc[0]
        # print("SQL:::  " + content_text)
        response = extract_details(str(content_text))
        response_json = json.loads(response)
        # print(response_json)
        response_json["LinkID"] = RId
        return response_json
    except Exception as e:
        
        print("Error is processing sql:"+str(e))
        return None
    

def process_file_future(row):
    response = process_sql_from_gcs(row["Report Type"],row["LinkID"])
    if response:
        df = pd.json_normalize(response)
        df["LinkID"] = row["LinkID"]
        return df, row["LinkID"]
    else:
        print("No Reponse from Gemini" + str(row))
    return None


def main():
    try:
        final_df = pd.DataFrame()
        processed_files = set()
        skip_count=0
        
        if os.path.exists(OUTPUT_CSV_FILE):
            try:
                final_df = pd.read_csv(OUTPUT_CSV_FILE)
                    
            except pd.errors.EmptyDataError:
                print("Output CSV is empty, starting fresh.")
            except pd.errors.ParserError:
                print("Error parsing existing CSV. Starting fresh.")
        else:
            print("Output CSV not found!!! Creating one..")
                
        # Load checkpoint or initialize
        if os.path.exists(CHECKPOINT_FILE):
            with open(CHECKPOINT_FILE, "r") as f:
                checkpoint = json.load(f)
                processed_files = set(checkpoint.get("processed_files", []))

        # # Read GCS paths from the text file
        # with open(GCS_PATHS_LIST_FILE, "r") as f:
        #     all_gcs_paths = [line.strip() for line in f]
        
        reports_df = pd.read_csv("InputIDsForQueries.csv")

        process_df = reports_df.head(20)
        
        # print(process_df.head())
        
        # process_df = pd.DataFrame({
        #     "LinkID": ["0CFCDED1486E4253DA7FA687174558FB",
        #                "AuditPMOSwitchNAVapplicabilityaspernewcriteriaisbeingadhered"],
        #     "Report Type": ["MIS","ROR"]
        # })
#         process_df = pd.DataFrame({
#             "LinkID": [CHECK_ID],
#             "Report Type": [REPORT_TYPE]
#         })
    
        # process_df = process_df.head(5)

        # Process files in batches
        with ThreadPoolExecutor(max_workers=20) as executor:
            for i in range(0, len(process_df), BATCH_SIZE):
                batch = process_df[i:i + BATCH_SIZE]
                batch_df = pd.DataFrame()
                newly_processed = set()
                
                futures = []
                for _, row in batch.iterrows():
                  if ((row["LinkID"] not in processed_files)):
                      # print(batch)
                      futures.append(executor.submit(process_file_future, row))
                      
                  else:
                    if RERUN:
                          
                          final_df = final_df[final_df['LinkID'] != row["LinkID"]]
                          processed_files.remove(row["LinkID"])
                          # print(batch)
                          futures.append(executor.submit(process_file_future, row))
                      
    
                for future in as_completed(futures):
                    result = future.result()
                    
                    if result is not None:
                        df, gcs_path_from_future = result
                        batch_df = pd.concat([batch_df, df], ignore_index=True)
                        newly_processed.add(gcs_path_from_future)
                    else:
                        skip_count += 1
                # for gcs_path in batch:
                #     if gcs_path not in processed_files:
                #         try:
                #             if ".txt" in gcs_path:
                #                 response = process_sql_from_gcs(gcs_path)
                #                 if response:
                #                     temp_df = pd.json_normalize(response, meta=['Purpose'])
                #                     batch_df = pd.concat([batch_df, temp_df])
                #                     newly_processed.add(gcs_path)
                #         except Exception as e:
                #             print(f"Error processing {gcs_path} in batch: {e}")
                #     else:
                #         skip_count+=1  
                    

                if not batch_df.empty: #only append if there are successful processes
                    final_df = pd.concat([final_df, batch_df])
                    processed_files.update(newly_processed)

                # Update checkpoint after each batch (more frequent checkpoints)
                checkpoint = {"processed_files": list(processed_files)}
                with open(CHECKPOINT_FILE, "w") as f:
                    json.dump(checkpoint, f)
                final_df.to_csv(OUTPUT_CSV_FILE, index=False)


                # print(f"Completed batch. Processed {len(processed_files)} files.")

        print(f"Processing complete. Total processed: {len(processed_files)} files. Skipped {skip_count} files ")


    except Exception as e:
        print(f"An error occurred processing: {e}")


if __name__ == "__main__":
    main()


import pandas as pd
import ast
from itertools import combinations
import re

mapping_df = pd.read_csv(MAPPING_FILE)
mapping_dict = dict(zip(mapping_df['Name'], mapping_df['CorrectedName']))
df_tables = pd.read_csv("FullTableList.csv")
table_names_in_df_tables = set(df_tables['TableName'].str.upper().tolist())

def process_tables_with_columns(row):
    try:
        table_list = ast.literal_eval(str(row['TableNamesAllTables']))
        results = []
        for table_data in table_list:
            if isinstance(table_data, dict) and 'Name' in table_data:
                table_name = table_data['Name']
                alias = table_data.get('Alias', 'NA').upper()  # Handle missing aliases
                columns = table_data.get('Columns', [])  # Handle missing column lists

                

                # Process and map the table name
                if "." in table_name:
                    parts = table_name.split('.')
                    schema = parts[0]
                    table = parts[-1]
                    if "@" in table:
                        table = table.split("@")[0]
                else:
                    schema = ""
                    table = table_name
                mapped_table = mapping_dict.get(table, table)

                # Append results with LinkID and columns
                results.append({'LinkID': row['LinkID'], 'TableName': mapped_table, 'Columns': columns})
        return results
    except Exception as e:
        print(f"Error processing tables and columns: {e}")
        return []
    
def process_tables(tables, alias_to_name):
    processed_tables = []
    if isinstance(tables, list):
        for _t in tables:
            if _t:
                # print("Processing: " + _t)
                # Alias replacement happens here, before further processing
                # print(alias_to_name)
                for alias, name in alias_to_name.items():
                    _t = re.sub(rf'\b{alias}\b', name, _t.upper()) #replaced '\b\b' with '\b{alias}\b'
                    # print("Alias: "+_t)
                if len(_t.split(' ')) == 2:
                    _t = _t.split(' ')[0]
                formatted_table_name = _t.upper()
                if "." in formatted_table_name:
                    parts = formatted_table_name.split('.')
                    schema = parts[0]
                    table = parts[-1]
                    if "@" in table:
                        table = table.split("@")[0]
                else:
                    schema = ""
                    table = formatted_table_name
                    for alias, name in alias_to_name.items():
                        table = re.sub(rf'\b{alias}\b', name, table)

                mapped_table = mapping_dict.get(table, table)
                processed_tables.append((schema, mapped_table))

        return (list(set(processed_tables))) 
    else:
        if tables:
          parts = tables.split('.')
          if len(parts) > 1: #check we have schema
            schema = parts[0].upper()
            for part in parts[1:]:
              processed_tables.append((schema, part.upper()))
          else:
            processed_tables.append(("", parts[0].upper()))
          return processed_tables
        return []
    
# df_full = pd.read_csv("Extract0801.csv")
df_full = pd.read_csv(OUTPUT_CSV_FILE)
df = df_full.copy()

def extract_table_names(row):
  try:
    table_list = ast.literal_eval(row)
    return [d['Name'] for d in table_list if isinstance(d, dict) and 'Name' in d]
  except Exception as e:
    print(f"Error parsing table string - ")
    return []


def extract_aliases(row):
    try:
        tables_alias_str = row
        tables_alias = ast.literal_eval(tables_alias_str)
        alias_to_name = {item['Alias'].upper(): item['Name'].upper() for item in tables_alias if 'Alias' in item and 'Name' in item}
        return alias_to_name
    except Exception as e:
      print(f"Error parsing alias string - ")
      return {}
    

df['TableNamesList'] = df['TableNamesAllTables'].apply(extract_table_names)
df['AliasToName'] = df['TableNamesAllTables'].apply(extract_aliases)



df['TableNamesProcessed'] = df.apply(lambda row: process_tables(row['TableNamesList'], row['AliasToName']), axis=1)


df_exploded = df.explode('TableNamesProcessed')
df_exploded[['SchemaName', 'TableName']] = pd.DataFrame(df_exploded['TableNamesProcessed'].tolist(), index=df_exploded.index)


df_output = df_exploded[['LinkID', 'SchemaName', 'TableName']]
df_output = df_output[df_output["TableName"]!="UNKNOWN"]

# df_output.loc[:, 'AlreadyInList'] = df_output['TableName'].apply(lambda x: "Yes" if x in table_names_in_df_tables else "No")

# print(df_output[df_output["TableName"]!=""])
    
df_output.to_csv(OUTPUT_TABLE_LIST, index=False)

print(f"Tables extracted and outputted to {OUTPUT_TABLE_LIST}")

df['TableNamesAndColumns'] = df.apply(process_tables_with_columns, axis=1)
df_exploded_columns = df.explode('TableNamesAndColumns')

df_output_columns = pd.json_normalize(df_exploded_columns['TableNamesAndColumns'])
# df_output_columns = pd.concat([df_exploded_columns[['LinkID']], df_output_columns], axis=1)


df_output_columns.to_csv(OUTPUT_COLUMNS_LIST, index=False)
print(f"Table names and columns extracted and outputted to {OUTPUT_COLUMNS_LIST}")


# display(df_output[df_output['AlreadyInList']=='No']['TableName'].dropna())

# df_output[df_output['AlreadyInList'] == 'No'][['SchemaName', 'TableName']].drop_duplicates().dropna(subset=['TableName'])

# df_output_yes = df_output[df_output['AlreadyInList'] == 'Yes']

df_grouped = df_output[df_output['TableName'].notna()].groupby('LinkID')['TableName'].apply(lambda x: sorted(list(set(x)))).reset_index()
df_grouped.rename(columns={'TableName': 'TableNames'}, inplace=True)

df_grouped.to_csv(OUTPUT_TABLES_PER_REPORT, index=False)
print(f"Tables extracted and outputted to {OUTPUT_TABLES_PER_REPORT}")


import pandas as pd
import ast
from pandas._typing import SequenceNotStr

try:
    columns_df = pd.read_csv(COLUMNS_ALL)
    columns_df["TableColumnDB"] = columns_df["TableNameDB"] + "." + columns_df["ColumnNameDB"]
    columns_set = set(columns_df["TableColumnDB"])
    mapping_df = pd.read_csv(MAPPING_FILE)
    mapping_dict = dict(zip(mapping_df['Name'], mapping_df['CorrectedName']))
    table_per_id_df = pd.read_csv(OUTPUT_TABLES_PER_REPORT)
    
    df = pd.read_csv(OUTPUT_COLUMNS_LIST)
    
    def get_valid_column(column_name, valid_tables_str, columns_df):
        try:
            table_valid_list = ast.literal_eval(str(valid_tables_str[0]))
        except Exception as e:
            print("Error parsing the table list " + str(e))
            table_valid_list = []
        
        if len(column_name.split('.'))==2:
            column_name=column_name.split('.')[1]
        for table in table_valid_list:
          if f'{table}.{column_name}' in columns_set:
             return f'{table}.{column_name}'
        return None

    def map_and_uppercase(table):
        schema = ''
        formatted_table_name = str(table).upper()
        if "." in formatted_table_name:
            table = formatted_table_name.rsplit(".", 1)[-1] #Correct rsplit usage
            schema = formatted_table_name.split(".")[0]
            if "@" in table:
                table = table.split("@")[0]
        elif "@" in str(table):
                table = table.split("@")[0]
        else:
            table = formatted_table_name #Handle cases without "."
        mapped_table = mapping_dict.get(table, table)
        return mapped_table.upper() if isinstance(mapped_table, str) else mapped_table

    df[['TableName']] = df['TableName'].apply(lambda x: pd.Series(map_and_uppercase(x))) #Correct: Apply to 'table' column and create two new columns
    
    #Improved error handling for ast.literal_eval
    def safe_literal_eval(x):
        try:
            return ast.literal_eval(x) if isinstance(x, str) and x.startswith('[') else []
        except (ValueError, SyntaxError):
            print(x)
            print(f"Warning: Skipping invalid list literal: {x}")
            return []

    df['Columns'] = df['Columns'].apply(safe_literal_eval)
    df = df.explode('Columns')


    #Handle potential None values more gracefully.
    # df = df.dropna(subset=['column_name'], how='all') #Only drop if ALL values are NaN.
    df['Columns'] = df['Columns'].astype(str).str.upper()
    df['ValidColumn'] = df.apply(lambda x: "Yes" if f"{x['TableName']}.{x['Columns']}" in columns_set else "No", axis=1)

    df = df.drop_duplicates().reset_index(drop=True) # Correct column selection
    no_valid_rows = df[df['ValidColumn'] == 'No']
   
    v_count=0
    for index, row in no_valid_rows.iterrows():
        link_id = row['LinkID'] 
        valid_tables = table_per_id_df[table_per_id_df['LinkID'] == link_id]['TableNames']
        corrected_column = get_valid_column(row['Columns'], valid_tables.values, columns_df)
        if corrected_column:
            v_count+=1
            df.loc[index,"TableName"] = corrected_column.split('.')[0]
            df.loc[index, 'Columns'] = corrected_column.split('.')[1]
            df.loc[index, 'ValidColumn'] = "Yes"    
            
    print(f"Updated {v_count} invalid columns as Valid through lazy selection")
    df = df.drop_duplicates().reset_index(drop=True)
    print(len(df))
    df.to_csv(OUTPUT_COLUMNS_PER_REPORT, index=False)

except FileNotFoundError:
    print("Error: One or both CSV files not found.")
except KeyError as e:
    print(f"Error: Missing column in CSV file: {e}")
except pd.errors.EmptyDataError:
    print("Error: One or both CSV files are empty.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")


from __future__ import annotations
import json, pandas as pd, os
import time
import vertexai
from vertexai.language_models import TextEmbeddingInput, TextEmbeddingModel
from vertexai.generative_models import GenerativeModel, Part, SafetySetting

PROJECT_ID = "prj-cams-playground-vertexai"
LOCATION_ID = "asia-south1"

EMBEDDING_FILE = 'cols_embeddings.json'
CHECKPOINT_FILE = f'checkpoint_{EMBEDDING_FILE}'
SIMILARITY_MATRIX_FILE = 'TableColumnsSimilarityMatrix.csv'

processed_files = set()

safety_settings = [
    SafetySetting(
        category=SafetySetting.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
        threshold=SafetySetting.HarmBlockThreshold.BLOCK_NONE
    ),
    SafetySetting(
        category=SafetySetting.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
        threshold=SafetySetting.HarmBlockThreshold.BLOCK_NONE
    ),
    SafetySetting(
        category=SafetySetting.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
        threshold=SafetySetting.HarmBlockThreshold.BLOCK_NONE
    ),
    SafetySetting(
        category=SafetySetting.HarmCategory.HARM_CATEGORY_HARASSMENT,
        threshold=SafetySetting.HarmBlockThreshold.BLOCK_NONE
    ),
]


if os.path.exists(CHECKPOINT_FILE):
    with open(CHECKPOINT_FILE, "r") as f:
        checkpoint = json.load(f)
        processed_files = set(checkpoint.get("processed_files", []))
else:
    processed_files = set()

try:
    with open(EMBEDDING_FILE, 'r') as f:
        embeddings_dict = json.load(f)
except (FileNotFoundError, json.JSONDecodeError):
    embeddings_dict = {}


def embed_text(texts) -> list[list[float]]:
    dimensionality = 768
    task = "SEMANTIC_SIMILARITY"
    model = TextEmbeddingModel.from_pretrained("text-embedding-005")
    inputs = [TextEmbeddingInput(text, task) for text in texts]
    kwargs = dict(output_dimensionality=dimensionality) if dimensionality else {}
    embeddings = model.get_embeddings(inputs, **kwargs)
    return [embedding.values for embedding in embeddings]


df_full = pd.read_csv(COLUMNS_ALL)
df = df_full.head(1000)

_c = 0

for i, _row in df.iterrows():
    table_name = _row['TableNameDB']
    column_name = _row['ColumnNameDB']

    # Create a composite key (e.g., "TABLE_NAME.COLUMN_NAME")
    composite_key = f"{table_name}.{column_name}"

    if composite_key in processed_files:
        _c += 1
        continue

    _text = f'''
    Column Name: {column_name}
    Table Name : {table_name}
    '''
    # print(_text)

    try:
        time.sleep(2)
        embedding = embed_text([_row['ColumnNameDB']]) # Pass in the contextually rich text
       
        if embedding:
            embeddings_dict[composite_key] = embedding[0]
            processed_files.add(composite_key)
        else:
            print(f"Warning: Embedding generation failed for {composite_key} in table {table_name}")
    except Exception as e:
        print(f"Error processing row {composite_key}: {e}")

    _c += 1
    if _c % 20 == 0:
        print(str(_c) + " Processed...")
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump({"processed_files": list(processed_files)}, f)
        with open(EMBEDDING_FILE, 'w') as file:
            json.dump(embeddings_dict, file, indent=4)

print(str(_c) + " Processed...")
with open(CHECKPOINT_FILE, "w") as f:
    json.dump({"processed_files": list(processed_files)}, f)

with open(EMBEDDING_FILE, 'w') as file:
    json.dump(embeddings_dict, file, indent=4)

print(str(_c) + f" Embeddings saved to {EMBEDDING_FILE}")



import numpy as np
import csv
import pandas as pd
import json

def cosine_similarity(a, b):
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))

try:
    with open(EMBEDDING_FILE, 'r') as f:
        embeddings_dict = json.load(f)
except (FileNotFoundError, json.JSONDecodeError) as e:
    print(f"Error loading embeddings: {e}")
    embeddings_dict = {}

if embeddings_dict:
    link_ids = list(embeddings_dict.keys())
    num_links = len(link_ids)

    # Calculate similarity matrix only once
    table_dict = {link_id: link_id.split(".")[0] for link_id in link_ids}
    
    similarity_matrix = np.zeros((num_links, num_links))
    # for i, link_id1 in enumerate(link_ids):
    #     embedding1 = np.array(embeddings_dict[link_id1])
    #     for j, link_id2 in enumerate(link_ids):
    #         embedding2 = np.array(embeddings_dict[link_id2])
    #         similarity_matrix[i, j] = cosine_similarity(embedding1, embedding2)
    
    similarity_matrix = np.zeros((num_links, num_links))
    for i, link_id1 in enumerate(link_ids):
      print(table1)
      table1 = table_dict[link_id1]
      embedding1 = np.array(embeddings_dict[link_id1])
      for j, link_id2 in enumerate(link_ids):
        table2 = table_dict[link_id2]
        if table1 and table2 and table1 != table2: # Only compare if tables are different and valid table name exist
          embedding2 = np.array(embeddings_dict[link_id2])
          similarity_matrix[i, j] = cosine_similarity(embedding1, embedding2)
        else:
          similarity_matrix[i, j] = -1

    # Get similarity threshold from the user
    while True:
        try:
            similarity_threshold = float(input("Enter the similarity threshold (between 0 and 1): "))
            if 0 <= similarity_threshold <= 1:
                break
            else:
                print("Threshold must be between 0 and 1.")
        except ValueError:
            print("Invalid input. Please enter a number.")


    #Find nearest neighbors based on threshold
    results = []
    for i, link_id in enumerate(link_ids):
        similar_links = []
        for j, similarity in enumerate(similarity_matrix[i]):
            if i != j and similarity >= similarity_threshold:  #Exclude self-similarity
                similar_links.append(link_ids[j])
        results.append({"LinkID": link_id, "SimilarLinks": similar_links})

    # Output to CSV
    df = pd.DataFrame(results)
    rows = []
    for _, row in df.iterrows():
        link_table, link_column = row['LinkID'].split('.')
        # print(row['SimilarLinks'])
        similar_links = eval(str(row['SimilarLinks']))  # Safely evaluate the string representation of the list

        if similar_links:  # Check if similar_links is not empty
            for similar_link in similar_links:
                similar_table, similar_column = similar_link.split('.')
                rows.append([link_table, link_column, similar_table, similar_column])
        else:
             rows.append([link_table, link_column, None, None])  # Handle empty SimilarLinks


    s_df = pd.DataFrame(rows, columns=['TableName', 'ColumnName', 'SimilarTable', 'SimilarColumn'])


    output_filename = f"NearestNeighbours_{similarity_threshold*100}_percent.csv"
    s_df.to_csv(output_filename, index=False)
    print(f"Nearest neighbors saved to {output_filename}")

else:
    print(f"No embeddings loaded. Please check your {EMBEDDING_FILE}")


    