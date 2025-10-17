import glob
import base64, json
import vertexai
from vertexai.generative_models import GenerativeModel, Part, SafetySetting
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.agents.config.settings import Settings
import uuid, json
from src.agents.shared_libraries.bq_utils import (
    insert_sql_extract_to_bq,
    get_completed_sql_files_from_bq,
)
import sys

safety_settings = [
    SafetySetting(
        category=SafetySetting.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
        threshold=SafetySetting.HarmBlockThreshold.BLOCK_NONE,
    ),
    SafetySetting(
        category=SafetySetting.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
        threshold=SafetySetting.HarmBlockThreshold.BLOCK_NONE,
    ),
    SafetySetting(
        category=SafetySetting.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
        threshold=SafetySetting.HarmBlockThreshold.BLOCK_NONE,
    ),
    SafetySetting(
        category=SafetySetting.HarmCategory.HARM_CATEGORY_HARASSMENT,
        threshold=SafetySetting.HarmBlockThreshold.BLOCK_NONE,
    ),
]


def extract_sql_details(sql_query, application_name: str, sql_file_name: str):
    # Check if the file has already been processed for this application
    completed_files = get_completed_sql_files_from_bq(application_name)
    if sql_file_name in completed_files:
        message = f"Skipping already processed file '{sql_file_name}' for application '{application_name}'."
        # Return a clear message to the frontend
        return {"status": "skipped", "message": message, "sql_file_name": sql_file_name}

    sql_id = str(uuid.uuid4())
    # try:

    if len(sql_query) < 10:
        sql_query = "No SQL"
    config = Settings.get_settings()

    vertexai.init(project=config.PROJECT_ID, location=config.REGION)
    model = GenerativeModel(
        config.LLM_MODEL,
        system_instruction=[
            """You are an expert data architect specializing in reverse-engineering data models from SQL code. 
        Your sole purpose is to analyze the structure of SQL scripts to identify entities (tables), 
        attributes (columns), and relationships (joins). You must ignore the business context and focus
        exclusively on the technical DDL and DML structure to build an accurate data model.""",
        ],
    )

    ##Change the prompt accordinly to extract various details

    extraction_prompt_json = f"""You are a meticulous and highly accurate data lineage analysis agent. Your task is to analyze the provided SQL script and generate **Comprehensive outputs in a single response**: a machine-readable JSON.

**CORE ANALYSIS INSTRUCTIONS (Applies to BOTH outputs):**

1.  **Analyze the Entire Script**: Process all SQL commands. You must **ignore vendor-specific control commands** (e.g., `.IF`, `.GOTO`, `.LABEL`, `.SET`, `.QUIT`).
2.  **Identify Schema**: Find all table definitions. Prioritize `CREATE TABLE` statements for schema details,Infer table structures from DML if no DDL is present.
3.  **Universal Alias Resolution (CRITICAL)**: Throughout your entire response, for the JSON, you must resolve all table aliases (e.g., `T1`, `A`, `B`) back to their full, original table names (e.g., `your_schema.your_table_name`).
4.  **Rewrite Transformation Logic (CRITICAL)**: When populating the `transformation_logic` in the JSON, you must **rewrite the original SQL expression**, replacing all aliases with their fully qualified table names. **Do not simply copy the original code snippet.**
5.  **Focus on Data Movement**: Document every `INSERT` and `UPDATE` statement as a distinct data flow.
6.  **Ground Your Analysis**: Your entire output must be based **exclusively** on the information present in the script provided. Do not invent or infer any information.
7.  Delimit by terminal semicolon only. Ignore any other semicolons.
8. Ignore the code which are commented out like -- or /* */

**ENTITY TYPE DEFINITIONS:**

You must classify each table into one of the following three types for the `entity_type` field:
* **SOURCE_TABLE**: A table that is only ever read from (SELECT or JOIN) and is **never** the subject of an INSERT, UPDATE, or DELETE operation within the script.
* **TARGET_TABLE**: A table that is modified (`INSERT`, `UPDATE`, `DELETE`). It can be read from in other steps, but its primary role involves being written to. This is typically a final output or persistent log table.
* **WORK_TABLE**: A table whose name begins with the prefix **WK_**. These are considered intermediate or staging tables, regardless of their usage.

---

**OUTPUT STRUCTURE:**

** Machine-Readable JSON Data Map**
*(Enclose this entire section in a single ```json code block)*

```json
{{
  "job_metadata": {{
    "job_name": "Extract the job name, e.g., 'Daily Sales Aggregation'",
    "version": "Extract the version, e.g., 'v1.2'",
    "default_database": "The database set by a 'USE' or 'DATABASE' command, e.g., 'PROD_DB'"
  }},
  "entities": [
    {{
      "entity_name": "Fully qualified table name, e.g., PROD_DB.MY_TARGET_TABLE",
      "entity_type": "WORK_TABLE, BASE_TABLE, VIEW, etc.",
      "creation_source": "The source of the schema definition ('CREATE TABLE DDL' or 'Inferred from DML')",
      "primary_key": ["List of columns from the PRIMARY KEY or UNIQUE INDEX, if available"],
      "attributes": [
        {{
          "attribute_name": "Column name, e.g., TOTAL_SALES",
          "data_type": "Data type from DDL, e.g., DECIMAL(18,2)",
          "is_nullable": "Boolean (true/false) based on DDL, default to true if inferred"
        }}
      ]
    }}
  ],
  "relationships": [
    {{
      "type": "The join type, e.g., LEFT, INNER, CROSS",
      "left_entity": "The full name of the table on the left side",
      "right_entity": "The full name of the table on the right side",
      "join_conditions": [
        "A list of string expressions from the ON clause with aliases fully resolved, e.g., 'PROD_DB.SOURCE_TABLE_A.ID = STAGING_DB.SOURCE_TABLE_B.ID'"
      ]
    }}
  ],
  "data_flows": [
    {{
      "flow_description": "A brief, human-readable summary, e.g., 'Populate the daily sales summary table.'",
      "operation_type": "The DML command, e.g., INSERT, UPDATE",
      "target_entity": "The table being modified, e.g., PROD_DB.MY_TARGET_TABLE",
      "source_entities": [
          "List of all tables used for sourcing data, e.g., ['PROD_DB.SOURCE_TABLE_A', 'STAGING_DB.SOURCE_TABLE_B']"
      ],
      "attribute_mappings": [
        {{
          "target_attribute": "The column in the target table, e.g., PRODUCT_CATEGORY",
          "source_attributes": ["List of all source columns used, with aliases resolved, e.g., ['PROD_DB.SOURCE_TABLE_A.CATEGORY_CODE']"],
          "transformation_logic": "The exact SQL expression with all aliases resolved. Example: 'CASE WHEN PROD_DB.SOURCE_TABLE_A.CATEGORY_CODE IN ('A', 'B') THEN 'Premium' ELSE 'Standard' END'"
        }}
      ]
    }}
  ]
}}
````

**SQL SCRIPT TO ANALYZE:**

```sql
    SQL:
        {sql_query}
        
```
"""

    extraction_prompt_tbl = f"""You are a meticulous and highly accurate data lineage analysis agent. Your task is to analyze the provided SQL script and generate **One, comprehensive output in a single response**: a human-readable Markdown report.

**CORE ANALYSIS INSTRUCTIONS (Applies to BOTH outputs):**

1.  **Analyze the Entire Script**: Process all SQL commands. You must **ignore vendor-specific control commands** (e.g., `.IF`, `.GOTO`, `.LABEL`, `.SET`, `.QUIT`).
2.  **Identify Schema**: Find all table definitions. Prioritize `CREATE TABLE` statements for schema details,Infer table structures from DML if no DDL is present.
3.  **Universal Alias Resolution (CRITICAL)**: Throughout your entire response, you must resolve all table aliases (e.g., `T1`, `A`, `B`) back to their full, original table names (e.g., `your_schema.your_table_name`).
4.  **Rewrite Transformation Logic (CRITICAL)**: When populating the `Transformation Logic` column in the Markdown table, you must **rewrite the original SQL expression**, replacing all aliases with their fully qualified table names. **Do not simply copy the original code snippet.**
5.  **Focus on Data Movement**: Document every `INSERT` and `UPDATE` statement as a distinct data flow.
6.  **Ground Your Analysis**: Your entire output must be based **exclusively** on the information present in the script provided. Do not invent or infer any information.
7.  Delimit by terminal semicolon only. Ignore any other semicolons.
8. Ignore the code which are commented out like -- or /* */

**ENTITY TYPE DEFINITIONS:**

You must classify each table into one of the following three types for the `entity_type` field:
* **SOURCE_TABLE**: A table that is only ever read from (SELECT or JOIN) and is **never** the subject of an INSERT, UPDATE, or DELETE operation within the script.
* **TARGET_TABLE**: A table that is modified (`INSERT`, `UPDATE`, `DELETE`). It can be read from in other steps, but its primary role involves being written to. This is typically a final output or persistent log table.
* **WORK_TABLE**: A table whose name begins with the prefix **WK_**. These are considered intermediate or staging tables, regardless of their usage.

---

**OUTPUT STRUCTURE:**


**Human-Readable Markdown Report**

# Data Lineage Report: [Job Name]

## 1\. Job Summary

  - **Job Name**: Extract the job name from comments (e.g., Daily Sales Aggregation).
  - **Version**: Extract the job version (e.g., v1.2).
  - **Default Database**: The database set by the `USE` or `DATABASE` command.

## 2\. Schema Overview

Provide a bulleted list of all entities (tables/views) involved in the script.

  - **`schema.table_name`** (Type: [WORK\_TABLE/TARGET\_TABLE/SOURCE\_TABLE], Source: [DDL/Inferred])
  - **`schema.table_name_2`** (Type: [WORK\_TABLE/TARGET\_TABLE/SOURCE\_TABLE], Source: [DDL/Inferred])
  - ...and so on for all entities.

## 3\. Data Transformation Flows

For **each `INSERT` or `UPDATE` operation** found in the script, create a separate subsection.

-----

### Flow X: Populating the `[Target Table Name]` Table

  - **Operation Type**: INSERT / UPDATE
  - **Source Entities**: List all fully-resolved source table names used in this operation.
  - **Target Entity**: The target table of the operation.

**Attribute-Level Lineage:**

| Target Column | Transformation Logic | Source Table(s) | Source Column(s) |
| :--- | :--- | :--- | :--- |
| `product_id` | `schema.source_table.product_code` | `schema.source_table` | `product_code` |
| `category` | `CASE WHEN schema.source_table.status = 'X' THEN 'Active' ELSE 'Inactive' END` | `schema.source_table` | `status` |
| `total_revenue` | `SUM(schema.source_table.price * schema.source_table.quantity)` | `schema.source_table` | `price`, `quantity` |
| `record_type`| `'FINAL' --(Literal)` | `N/A (Literal Value)` | `N/A` |

-----

*(Repeat the "Flow X" section for every subsequent `INSERT` or `UPDATE` statement)*

## 4\. Brief Functional Overview

Provide a short, high-level summary of the script's overall purpose. Describe what data it reads, the main transformations it performs, and what data it ultimately produces. For example: "This script aggregates transactional data into a daily summary table. It begins by joining sales data with product dimension tables to create a staging table. It then aggregates this data by region and product category into a final reporting table, deriving new metrics like total revenue and average sale price. Finally, it flags records based on their status and inserts a load timestamp."

-----

**SQL SCRIPT TO ANALYZE:**

```sql
    SQL:
        {sql_query}
        
```
"""

    generation_config = {"temperature": 1, "top_p": 0.9}
    
    report_markdown = ""
    json_string = ""

    try:
        # It's safer to wrap each API call in its own try/except block
        # to handle potential failures, like timeouts or empty responses from the model.
        responses_tbl = model.generate_content(
            [extraction_prompt_tbl],
            generation_config=generation_config,
            safety_settings=safety_settings,
            stream=False,
        )
        report_markdown = responses_tbl.text

        responses_json = model.generate_content(
            [extraction_prompt_json],
            generation_config=generation_config,
            safety_settings=safety_settings,
            stream=False,
        )
        json_string = responses_json.text

    except Exception as e:
        # If the LLM call fails, create a structured error to send back.
        # This prevents the frontend from getting an empty response.
        error_payload = {"error": "LLM content generation failed", "details": str(e)}
        # Return a dictionary that the FastAPI endpoint can serialize to JSON
        return {"parser_output": error_payload, "report_markdown": ""}

    # Split the response into JSON and Markdown parts

    # Clean up the JSON string
    json_string = json_string.strip().lstrip("```json").lstrip("```").rstrip("```")

    try:
        # Validate JSON
        parser_output = json.loads(json_string)
        processing_status = "NEW"
        # Insert into BigQuery
        insert_sql_extract_to_bq(
            sql_id=sql_id,
            sql_file_name=sql_file_name,
            raw_sql_text=sql_query,
            parser_output=parser_output,
            processing_status=processing_status,
            application_name=application_name,
            parser_output_tables=report_markdown,
        )
    except json.JSONDecodeError as e:
        parser_output = {
            "error": "Invalid JSON response from model",
            "details": str(e),
            "response_text": json_string,
        }
        processing_status = "ERROR"

    return {
        "parser_output": parser_output,
        "report_markdown": report_markdown,
    }
