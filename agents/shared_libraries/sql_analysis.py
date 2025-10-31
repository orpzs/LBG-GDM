import base64, json, hashlib
import vertexai
from vertexai.generative_models import GenerativeModel, Part, SafetySetting
import pandas as pd
from google.cloud import storage
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from config.settings import Settings
import uuid
from agents.shared_libraries.bq_utils import insert_sql_extract_to_bq, delete_analysis_data

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

def extract_sql_details(sql_query, file_path=None):
    if file_path:
        file_name = os.path.basename(file_path)
        hash_input = file_name
    else:
        hash_input = sql_query
    sql_id = hashlib.sha256(hash_input.encode()).hexdigest()
    
    # Delete existing analysis data for this q_id
    delete_analysis_data(sql_id)

    try:
        
        if len(sql_query) < 10:
            sql_query = 'No SQL'
        
        config = Settings.get_settings()
        
        model = GenerativeModel(
            config.LLM_MODEL,
            system_instruction=["""You are a database analyst expert in writing and understanding SQL queries. 
            You are also a business expert in the UK banking sector, with a deep understanding of retail and commercial banking products and services."""],
        )
        
        
        ##Change the prompt accordinly to extract various details
        
        extraction_prompt = f"""
You are an expert SQL parser. Your task is to analyze a multi-statement SQL script and extract a detailed, structured JSON representation of all its DML (Data Manipulation Language) operations. These SQLs are from a Banking & Financial Institution.

Instructions:

  - Scan the entire script and identify all DML statements: INSERT, UPDATE, DELETE, CREATE TABLE AS SELECT, and MERGE.
  - Ignore simple SELECT statements that are not part of a DML operation (e.g., SELECT * FROM... at the start of a script for a check).
  - The output must be a single JSON object.
  - The root of the JSON will contain a file_summary and a list called statements.
  - Each item in the statements list represents one DML operation you found, in sequential order.
  - Throughout your entire response, you must resolve all table aliases (e.g., `T1`, `A`, `B`) back to their full, original table names database.table_name for the transformation logics.
  - Always resolve the SELECT * as well. if the DDL is present in the script resolve each of the column if not 
  - **You to need uppercase all the relevant SQL statement related data like Column name, transformation logic expect inferred logic details**

Core Logic: Handling the SELECT part of a DML (Flattening Lineage)

This is your most important task. When you find a DML statement like INSERT ... SELECT ... or UPDATE ... FROM (SELECT ...), you must not treat the SELECT as a single "black-box" source.Instead, you must "flatten" the lineage by recursively analyzing the SELECT statement and its subqueries/CTEs.

How to Flatten:

  1. Analyze FROM/JOIN: For a DML statement (e.g., s1), parse its SELECT block (including all subqueries and CTEs) to find the true, "grandparent" tables/views.

  2. Populate sources: The sources array for s1 must contain only these true tables/views. An alias for a subquery (e.g., derived) must never be listed as a source.

  3. Trace column_lineage: For each output_column_name in the DML, you must trace its logic back through all aliases (e.g., derived.CUST_NAME) to find its ultimate origin (e.g., C.FIRST_NAME, C.LAST_NAME). The source_references array must point to the true source tables identified in step 2.

  4. Promote JOINs/WHEREs: All JOIN and WHERE clauses from inside the subqueries must be "promoted" and logged in the main DML statement's joins and filters arrays. Their source_ids must map to the true source tables.

Core Logic: Handling UNION / UNION ALL. When the DML's SELECT statement contains a UNION or UNION ALL, you must trace all data paths.

  - One Statement: The INSERT (or other DML) is still one single statement (e.g., s5). Do not create multiple statements entries for it.

  - All Sources: The sources array for this DML statement must include all unique "true" source tables from all branches of the UNION unqiuely. If firs select give src1, src2, src3 repeat them for second UNION select as well i.e. src4, src5, src6

  - Branching Lineage: The column_lineage is where the UNION logic becomes visible. For a single output_column_name, transformation logic should be fully resolves but use the inferred_logic_detail to provide full detail with whole understanding of the statement with UNION itself.

If the SQL script is invalid or contains no DML, return an empty JSON object: {{}}

**ALways be consistent with the JSON key name mentioned in the below format** 

Output JSON:

JSON
{{
  "file_summary": {{
    "inferred_detail": "A high-level, natural language summary of the entire script's purpose.",
    "dependencies": [
      "A list of any explicit dependencies, like job IDs, mentioned in comments (e.g., 'C01J01')."
    ]
  }},
  "statements": [
    {{
      "s_id": "A unique, sequential ID for this statement (e.g., 's1', 's2', 's3').",
      "inferred_detail": "A natural language summary or inferred purpose of the statement.",
      "statement_type": "The DML command (e.g., 'UPDATE', 'INSERT', 'DELETE').",
      "target_table": {{
        "database_name": "The database of the table being written to.",
        "schema_name": "The schema of the table being written to (or null).",
        "table_name": "The name of the table being written to.",
        "alias": "The alias used for the target table (e.g., in an UPDATE T... statement).",
        "inferred_target_type": "The inferred role of this table ['BASE_TABLE', 'WORK_TABLE', 'LOG_TABLE'] (Details of what they mean is displayed here: 'BASE_TABLE ( The primary table(s) the script is designed to populate. This is the final destination of the main data flow)', 
                  'WORK_TABLE(Temporary/interim tables, often prefixed with WK_ or TEMP_. They are used for staging, are written to, then read from, and often deleted at the end )', 
                  'LOG_TABLE (Tables used for logging and status, often named ..._LOG or ..._STATUS and typically targeted by UPDATE statements to track progress.)')
      }},
      "sources": [
        {{
          "source_id": "A unique ID for this *true* source *within this statement* (e.g., 'src1', 'src2').",
          "database_name": "The database of the *true* source table.",
          "schema_name": "The schema of the *true* source table (or null).",
          "table_name": "The name of the *true* source table. **This MUST be an actual table/view, NEVER a subquery string.**",
          "alias": "The alias used for this source table (e.g., 'A', 'CCA', 'B').",
          "source_type": "The type of source (e.g., 'BASE_TABLE'). **Do NOT use 'SUBQUERY' or 'CTE' here; they must be flattened as described above**"
        }}
      ],
      // This should repeat for the same column when its for UNION
     "column_lineage": [
        {{
          "output_column_name": "The name of the column in the target table being populated. Do not assume any different column name you need to use the same name what is present for the INSERT.**This field is MANDATORY and MUST NOT be null for INSERTs or UPDATEs.**. ",
          "output_column_ordinal": "The 1-based integer position (1, 2, 3...) for 'INSERT' columns. Should be null for 'UPDATE' columns.",
          "transformation_logic": "The full expression or logic used to derive the column with all aliases resolved. Always resolve the aliases in the logic with corresponding source database and table name e.g.table_database.table_name,
          "inferred_logic_detail": "Based on the overall statement how is this column populated business logic wise?. For Non direct one to one mapping just mention as direct mapping from source column. For Sub query or CTE just talk about what kind of joins and filter logic is actually making this",
          "source_references": [
            // This array should be empty if the transformation_logic is a constant (e.g., '0' or 'I').
            // It MUST point to the 'source_id' of a 'true' source from the 'sources' array above.
            {{
              "source_id": ""The 'source_id' (from the 'sources' list) that maps to the *true* source table.",
              "column_name": "The name of the *true* source column from that table."
            }}
          ]
        }}
      ],
      // This array MUST also include joins "promoted" from any flattened subqueries.
      "joins": [
        {{
          "join_type": "The join type (e.g., 'JOIN', 'LEFT JOIN').",
          "left_source_id": "The 'source_id' of the table on the left.",
          "right_source_id": "The 'source_id' of the table on the right.",
          "join_conditions": [
          // Below contiion should cover all the multiple conditions e.g. ON A.COLUMN = B.COLUMN AND C.COLUMN = D.COLUMN
            {{
              "left_source_id": "The 'source_id' for the left side of the condition.",
              "left_column": "The column name from the left-side table.",
              "operator": "The comparison operator (e.g., '=', '<=').",
              "right_source_id": "The 'source_id' for the right side of the condition.",
              "right_column": "The column name from the right-side table."
              // repeat this for each condtion between sane table.
            }}
          ]
        }}
      ],
      "filters": [
        {{
          "clause": "The clause where the filter is applied (WHERE condition filter only) **Note that 'ON' clause is not for filters and ON clause would go on into the join coditions above**",
          "filter_expression": "The full text of the filter condition.",
          "involved_columns": [
            {{
              "source_id": "The 'source_id' this column belongs to. **Use the special value 'target'** to refer to columns from the 'target_table' (e.g., in an UPDATE's WHERE clause).",
              "column_name": "The name of the column used in the filter."
            }}
          ]
        }}
      ]
    }}
  ]
}}

       SQL:
            {sql_query.replace("{","{{").replace("}","}}")}
"""
            
        generation_config = {
            "max_output_tokens": 8192,
            "temperature": 1,
            "top_p": 0.9,
        }
        print("Starting Extraction")
        responses = model.generate_content(
            [extraction_prompt],
            stream=False,
        )
        
        response_text = responses.text.replace("```","" ).replace("json","")
        print("Completed Extraction")
        try:
            # Validate JSON
            parser_output = json.loads(response_text)
            processing_status = "NEW"
        except json.JSONDecodeError:
            print("Invalid JSON response from model")
            parser_output = {{"error": "Invalid JSON response from model", "response_text": response_text}}
            processing_status = "ERROR"

        # Insert into BigQuery
        insert_sql_extract_to_bq(
            q_id=sql_id,
            raw_sql_path=file_path,
            parser_output=parser_output,
            processing_status=processing_status
        )
        
        return response_text

    except Exception as e:
        print(e)
        # Insert error into BigQuery
        insert_sql_extract_to_bq(
            q_id=sql_id,
            raw_sql_path=file_path,
            parser_output={"error": str(e)},
            processing_status="ERROR"
        )
        return "Response Generation Error"