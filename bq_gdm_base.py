from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError


def run_bigquery_ddl():
    """
    Initializes the BigQuery client and executes the DDL statements
    for the new statement-level lineage model.
    """
    try:
        client = bigquery.Client()
        print(f"✅ BigQuery client initialized. Using project: {client.project}")
    except Exception as e:
        print(f"❌ Could not initialize BigQuery client. Error: {e}")
        print(
            "Please ensure you are authenticated (e.g., `gcloud auth application-default login`)"
        )
        return

    ddl_statements = [
        """
        CREATE OR REPLACE TABLE `r2d2-00.gdm.raw_sql_extracts`(
            q_id STRING OPTIONS ( description = "Primary Key. A unique identifier for the entire SQL file/script."),
            raw_sql_path STRING OPTIONS ( description = "Raw SQL Path"),
            file_name STRING OPTIONS (description = "File Name"),
            parser_output JSON OPTIONS ( description = "The complete, raw JSON output from the SQL parsing agent."),
            processing_status STRING OPTIONS ( description = "The status of post-processing (e.g., NEW, PROCESSED, ERROR)."),
            query_inferred_detail STRING OPTIONS(description="A natural language summary or inferred purpose of the query file."),
            dependencies ARRAY<STRING> OPTIONS(description="A list of explicit dependencies for the entire file (e.g., upstream job IDs)."),
            inserted_at TIMESTAMP OPTIONS (description = "The timestamp when the file was first ingested."),
            processed_at TIMESTAMP OPTIONS ( description = "The timestamp when the file was last successfully processed into statement tables.")
        ) OPTIONS (
            description = "Master log of all SQL files/scripts that have been ingested for parsing.",
            labels = [("agent", "reverse_agent")]
        )
        """,
        """
       CREATE OR REPLACE TABLE `r2d2-00.gdm.query_statements` (
            q_id STRING OPTIONS (description = "Foreign Key. Links to the parent file in raw_sql_extracts."),
            s_id STRING OPTIONS (description = "Primary Key (composite). A unique ID for this specific statement within the file (e.g., s1, s2)."),
            inferred_detail STRING OPTIONS (description = "A natural language summary or inferred purpose of the statement."),
            statement_type STRING OPTIONS (description = "The DML type (INSERT, UPDATE, DELETE, CREATE_TABLE_AS_SELECT)."),
            target_database_name STRING OPTIONS (description = "The database of the table being modified."),
            target_schema_name STRING OPTIONS (description = "The schema of the table being modified."),
            target_table_name STRING OPTIONS (description = "The name of the table being modified."),
            target_table_alias STRING OPTIONS (description = "The alias used for the target table in the DML statement (if any)."),
            inferred_target_type STRING OPTIONS (description = "Inferred table role based on script-wide analysis: BASE_TABLE, WORK_TABLE, LOG_TABLE.")
        ) OPTIONS (
            description = "Tracks each individual DML (INSERT, UPDATE, etc.) statement within a SQL file.",
            labels = [("agent", "reverse_agent")]
        )
        """,
        """
        CREATE OR REPLACE TABLE `r2d2-00.gdm.statement_sources` (
            q_id STRING OPTIONS (description = "Foreign Key. Links to the parent file."),
            s_id STRING OPTIONS (description = "Foreign Key. Links to the specific statement."),
            source_id STRING OPTIONS (description = "Primary Key (composite). A unique ID for this source table *within this statement* (e.g., src1, src2)."),
            source_database_name STRING OPTIONS (description = "The database of the source table."),
            source_schema_name STRING OPTIONS (description = "The schema of the source table."),
            source_table_name STRING OPTIONS (description = "The name of the source table (or '(Subquery)')."),
            source_alias STRING OPTIONS (description = "The alias used for this source table in the statement."),
            source_type STRING OPTIONS (description = "The type of source (BASE_TABLE, CTE, SUBQUERY).")
        ) OPTIONS (
            description = "Catalogs every source table (FROM/JOIN) used by a specific statement.",
            labels = [("agent", "reverse_agent")]
        )
        """,
        """
        CREATE OR REPLACE TABLE `r2d2-00.gdm.column_lineage` (
            q_id STRING OPTIONS (description = "Foreign Key. Links to the parent file."),
            s_id STRING OPTIONS (description = "Foreign Key. Links to the specific statement."),
            output_column_name STRING OPTIONS (description = "The final name of the column being inserted or updated."),
            output_column_ordinal INT64 OPTIONS (description = "The position of the column in the SELECT list (1, 2, 3, ...)."),
            transformation_logic STRING OPTIONS (description = "The full expression or function used to create the column."),
            inferred_logic_detail STRING OPTIONS (description = "A natural language summary or inferred purpose how this column is populated"),
            source_references ARRAY<STRUCT<source_id STRING, column_name STRING>> OPTIONS (description = "Links to the specific source table (via source_id) and column name that feeds this output column.")
        ) OPTIONS (
            description = "Core column-level lineage, mapping statement outputs to specific statement sources.",
            labels = [("agent", "reverse_agent")]
        )
        """,
        """
        CREATE OR REPLACE TABLE `r2d2-00.gdm.statement_joins` (
            q_id STRING OPTIONS (description = "Foreign Key. Links to the parent file."),
            s_id STRING OPTIONS (description = "Foreign Key. Links to the specific statement."),
            join_type STRING OPTIONS (description = "The type of join (INNER, LEFT, RIGHT, FULL OUTER, CROSS)."),
            left_source_id STRING OPTIONS (description = "The source_id (from statement_sources) of the table on the left side."),
            right_source_id STRING OPTIONS (description = "The source_id (from statement_sources) of the table on the right side."),
            join_conditions ARRAY<STRUCT<left_column STRING, operator STRING, right_column STRING>> OPTIONS (description = "An array of structs detailing the join conditions.")
        ) OPTIONS (
            description = "Stores structured information about all join operations in a specific statement.",
            labels = [("agent", "reverse_agent")]
        )
        """,
        """
        CREATE OR REPLACE TABLE `r2d2-00.gdm.statement_filters` (
            q_id STRING OPTIONS (description = "Foreign Key. Links to the parent file."),
            s_id STRING OPTIONS (description = "Foreign Key. Links to the specific statement."),
            clause STRING OPTIONS (description = "The clause where the filter is applied (WHERE, HAVING, ON)."),
            filter_expression STRING OPTIONS (description = "The full text of the filter condition."),
            involved_columns ARRAY<STRUCT<source_id STRING, column_name STRING>> OPTIONS (description = "An array identifying all source columns (via source_id) part of the filter.")
        ) OPTIONS (
            description = "Stores conditions from WHERE, HAVING, and ON clauses for a specific statement.",
            labels = [("agent", "reverse_agent")]
        )
        """,
    ]

    print(f"\nFound {len(ddl_statements)} DDL commands to execute for the new model.\n")
    print("-" * 80)

    for i, sql in enumerate(ddl_statements, 1):
        # A simple way to get the table name from the DDL for a cleaner log
        try:
            table_name = sql.split("`")[1]
            print(f"Executing command {i}/{len(ddl_statements)} for: `{table_name}`")
        except IndexError:
            print(f"Executing command {i}/{len(ddl_statements)}:")
        
        # print(f"Executing command {i}/{len(ddl_statements)}:\n{sql}\n") # Uncomment for full SQL

        try:
            query_job = client.query(sql)
            query_job.result()
            print(f"✅ Command {i} executed successfully.\n")

        except GoogleAPIError as e:
            print(f"❌ Error executing command {i}:\n{sql}\n")
            print(f"   Error details: {e}\n")

        except Exception as e:
            print(f"❌ An unexpected error occurred with command {i}:\n{sql}\n")
            print(f"   Error details: {e}\n")

        print("-" * 80)

    print("All DDL commands have been processed.")


if __name__ == "__main__":
    run_bigquery_ddl()