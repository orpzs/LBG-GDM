import base64, json
import vertexai
from vertexai.generative_models import GenerativeModel, Part, SafetySetting
import pandas as pd
from google.cloud import storage
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from config.settings import Settings
import uuid
from agents.shared_libraries.bq_utils import insert_sql_extract_to_bq

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

def extract_sql_details(sql_query):
    sql_id = str(uuid.uuid4())
    try:
        
        if len(sql_query) < 10:
            sql_query = 'No SQL'
        
        config = Settings.get_settings()
        
        vertexai.init(project=config.PROJECT_ID, location=config.REGION)
        model = GenerativeModel(
            config.LLM_MODEL,
            system_instruction=["""You are a database analyst expert in writing and understanding SQL queries. 
            You are also a business expert in the UK banking sector, with a deep understanding of retail and commercial banking products and services."""],
        )
        
        
        ##Change the prompt accordinly to extract various details
        
        extraction_prompt = f"""
        Analyze the provided SQL query and extract the following information (TableNames, Filters, Joins,UdfUsed, CaseStatements ) as JSON Formatted Output.
Make sure you extract all the details including from subqueries and CTEs. Return Empty if not valid SQLs.

Output JSON: 

Of course. Based on your goal and my previous recommendations, I've revised your prompt to extract a much more structured and useful JSON that will directly support your lineage analysis.

The key changes introduce dedicated sections for column-level lineage (output_columns), provide fully qualified table names, and replace simple strings with structured objects for joins and filters. This new prompt is designed to give you a clean, predictable output that requires minimal post-processing.

Revised Prompt for Your Agent
Here is the enhanced prompt. I've kept the double braces for your formatting needs.

Analyze the provided SQL query to build a detailed, structured representation of its components for lineage and metadata analysis. Extract the information into the following JSON format.

Ensure all identifiers (tables, columns, aliases) are extracted accurately.

Recurse into all CTEs (Common Table Expressions) and subqueries.

If the SQL is invalid, return an empty JSON object: {{}}.

Output JSON:

JSON

{{
  "query_type": "The type of query (e.g., SELECT, CREATE_TABLE_AS_SELECT, INSERT_SELECT)",
  "target_table": {{
    "database": "The database/project of the table being created, or null",
    "schema": "The schema/dataset of the table being created, or null",
    "name": "The name of the table being created, or null"
  }},
  "output_columns": [ // This is the core of column-level lineage. One entry for each column in the final SELECT list.
    {{
      "output_name": "The final column name or alias",
      "ordinal_position": "The integer position of the column in the SELECT list (1, 2, 3...)",
      "transformation_logic": "The full expression or logic used to derive the column (e.g., 'SUM(t1.sales)', 'CAST(t2.id AS STRING)', 't1.name')",
      "source_columns": [ // A list of all base columns that this output column is derived from.
        {{
          "table_alias": "The alias of the source table",
          "column_name": "The name of the source column"
        }}
      ]
    }}
  ],
  "tables": [ // A unified list of all tables, including base tables and CTEs.
    {{
      "database": "The database/project of the table, or null for a CTE",
      "schema": "The schema/dataset of the table, or null for a CTE",
      "name": "The name of the table or CTE",
      "alias": "The alias used in the query (use the name itself if no alias)",
      "table_type": "The type of table: BASE_TABLE or CTE",
      "used_columns": ["A list of column names from this table that are used anywhere in the query."]
    }}
  ],
  "joins": [
    {{
      "type": "The join type (e.g., INNER, LEFT, RIGHT, FULL OUTER)",
      "left_table_alias": "The alias of the table on the left side",
      "right_table_alias": "The alias of the table on the right side",
      "conditions": [ // A list of structured join conditions.
        {{
          "left_column": {{ "table_alias": "Alias of the left column's table", "column_name": "Name of the left column" }},
          "operator": "The join operator (e.g., =, <, >)",
          "right_column": {{ "table_alias": "Alias of the right column's table", "column_name": "Name of the right column" }}
        }}
      ]
    }}
  ],
  "filters": [ // Captures both WHERE and HAVING clauses.
    {{
      "clause": "The clause name: WHERE or HAVING",
      "filter_expression": "The full, raw text of the filter condition (e.g., 'status = \'active\' AND order_date > CURRENT_DATE()')",
      "involved_columns": [
        {{
          "table_alias": "Alias of the table for the column in the filter",
          "column_name": "Name of the column used in the filter"
        }}
      ]
    }}
  ],
  "udfs_used": ["A list of all User-Defined Functions or built-in functions used."],
  "unresolved_columns": [ // Columns that could not be mapped to a specific table.
    {{
      "column_name": "The name of the column whose source table is unknown."
    }}
  ]
}}

       SQL:
            {sql_query.replace("{","{{").replace("}","}}")}
"""
            
        generation_config = {{
            "max_output_tokens": 8192,
            "temperature": 1,
            "top_p": 0.9,
        }}
        responses = model.generate_content(
            [extraction_prompt],
            stream=False,
        )
        
        response_text = responses.text.replace("```","" ).replace("json","")
        
        try:
            # Validate JSON
            parser_output = json.loads(response_text)
            processing_status = "NEW"
        except json.JSONDecodeError:
            parser_output = {{"error": "Invalid JSON response from model", "response_text": response_text}}
            processing_status = "ERROR"

        # Insert into BigQuery
        # insert_sql_extract_to_bq(
        #     sql_id=sql_id,
        #     raw_sql_text=sql_query,
        #     parser_output=parser_output,
        #     processing_status=processing_status
        # )
        
        return response_text

    except Exception as e:
        print(e)
        # Insert error into BigQuery
        insert_sql_extract_to_bq(
            sql_id=sql_id,
            raw_sql_text=sql_query,
            parser_output={"error": str(e)},
            processing_status="ERROR"
        )
        return "Response Generation Error"
