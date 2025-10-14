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

    extraction_prompt_tbl = """
You are a meticulous and highly accurate data lineage analysis agent. Your task is to analyze the provided Teradata BTEQ script and generate **TWO distinct, comprehensive outputs in a single response**: a machine-readable JSON object AND a human-readable Markdown report.

**CORE ANALYSIS INSTRUCTIONS (Applies to BOTH outputs):**

1.  **Analyze the Entire Script**: Process all SQL commands. You must **ignore Teradata BTEQ control commands** (e.g., `.IF`, `.GOTO`, `.LABEL`, `.SET`, `.QUIT`).
2.  **Identify Schema**: Find all table definitions. Prioritize `CREATE TABLE` statements for schema details, **even if they are commented out (`/* ... */`)**. Infer table structures from DML if no DDL is present.
3.  **Universal Alias Resolution (CRITICAL)**: Throughout your entire response, for both the JSON and the Markdown outputs, you must resolve all table aliases (e.g., T1, A, RD) back to their full, original table names (e.g., `CC_COBRA.CC_ACCOUNT_FEATURE_DAILY`).
4.  **Rewrite Transformation Logic (CRITICAL)**: When populating the `transformation_logic` in the JSON and the `Transformation Logic` column in the Markdown table, you must **rewrite the original SQL expression**, replacing all aliases with their fully qualified table names. **Do not simply copy the original code snippet.**
5.  **Focus on Data Movement**: Document every `INSERT` and `UPDATE` statement as a distinct data flow.
6.  **Ground Your Analysis**: Your entire output must be based **exclusively** on the information present in the script provided. Do not invent or infer any information.
7.  Delimit by terminal semicolon only. Ignore any other semicolons.

---

**OUTPUT STRUCTURE:**

Your response must contain two parts, presented in this exact order:

**Part 1: Machine-Readable JSON Data Map**
*(Enclose this entire section in a single ```json code block)*

```json
{{
  "job_metadata": {{
    "job_name": "Extract the job name, e.g., 'FR01 - ACCRUED INTEREST'",
    "version": "Extract the version, e.g., 'FR01v10'",
    "default_database": "The database set by the 'DATABASE' command, e.g., 'CC_COBRA'"
  }},
  "entities": [
    {{
      "entity_name": "Fully qualified table name, e.g., CC_COBRA.WK_FR01_ACCRUED_INTEREST",
      "entity_type": "WORK_TABLE, BASE_TABLE, VIEW, etc.",
      "creation_source": "The source of the schema definition ('CREATE TABLE DDL' or 'Inferred from DML')",
      "primary_key": ["List of columns from the UNIQUE PRIMARY INDEX, if available"],
      "attributes": [
        {{
          "attribute_name": "Column name, e.g., ACCRUED_INT",
          "data_type": "Data type from DDL, e.g., DECIMAL(15,2)",
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
        "A list of string expressions from the ON clause with aliases fully resolved, e.g., 'CC_COBRA.CC_ACCOUNT_FEATURE_DAILY.AGRMNT_ID = GDW_VIEWS.CREDIT_CARD_AGREEMENT.AGRMNT_ID'"
      ]
    }}
  ],
  "data_flows": [
    {{
      "flow_description": "A brief, human-readable summary, e.g., 'Populate the accrued interest staging table.'",
      "operation_type": "The DML command, e.g., INSERT, UPDATE",
      "target_entity": "The table being modified, e.g., CC_COBRA.WK_FR01_ACCRUED_INTEREST",
      "source_entities": [
          "List of all tables used for sourcing data, e.g., ['CC_COBRA.CC_ACCOUNT_FEATURE_DAILY', 'GDW_VIEWS.CREDIT_CARD_AGREEMENT']"
      ],
      "attribute_mappings": [
        {{
          "target_attribute": "The column in the target table, e.g., PART_NO",
          "source_attributes": ["List of all source columns used, with aliases resolved, e.g., ['CC_COBRA.CC_ACCOUNT_FEATURE_DAILY.PLAN_NO']"],
          "transformation_logic": "The exact SQL expression with all aliases resolved. Example: 'CASE WHEN CC_COBRA.CC_ACCOUNT_FEATURE_DAILY.PLAN_NO IN (10002,10003) THEN 1 ELSE NULL END'"
        }}
      ]
    }}
  ]
}}

**Part 2: Human-Readable Markdown Report**
*(Begin this section immediately after the JSON code block)*

# Data Lineage Report: [Job Name]

## 1\. Job Summary

  - **Job Name**: Extract the job name from comments (e.g., FR01 - ACCRUED INTEREST).
  - **Version**: Extract the job version (e.g., FR01v10).
  - **Default Database**: The database set by the `DATABASE` command.

## 2\. Schema Overview

Provide a bulleted list of all entities (tables/views) involved in the script.

  - **`database.table_name`** (Type: [WORK\_TABLE/TARGET\_TABLE/SOURCE\_TABLE], Source: [DDL/Inferred])
  - **`database.table_name_2`** (Type: [WORK\_TABLE/TARGET\_TABLE/SOURCE\_TABLE], Source: [DDL/Inferred])
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
| `TARGET_COLUMN_1` | `CC_COBRA.CC_ACCOUNT_FEATURE_DAILY.AGRMNT_ID` | `CC_COBRA.CC_ACCOUNT_FEATURE_DAILY` | `CC_COBRA.CC_ACCOUNT_FEATURE_DAILY.AGRMNT_ID` |
| `TARGET_COLUMN_2` | `CASE WHEN CC_COBRA.WK_FR01_ACCRUED_INTEREST.PLAN_NO = 10001 THEN 2 ... END` | `CC_COBRA.WK_FR01_ACCRUED_INTEREST` | `CC_COBRA.WK_FR01_ACCRUED_INTEREST.PLAN_NO` |
| `TARGET_COLUMN_3` | `SUM(CC_COBRA.WK_FR01_ACCRUED_INTEREST.ACCRUED_INT)` | `CC_COBRA.WK_FR01_ACCRUED_INTEREST` | `CC_COBRA.WK_FR01_ACCRUED_INTEREST.ACCRUED_INT` |
| `TARGET_COLUMN_4`| `'A' --ACTUAL` | `N/A (Literal Value)` | `N/A` |

-----

*(Repeat the "Flow X" section for every subsequent `INSERT` or `UPDATE` statement)*

## 4\. Brief Functional Overview

Provide a short, high-level summary of the script's overall purpose. Describe what data it reads, the main transformations it performs, and what data it ultimately produces. For example: "This script calculates daily accrued interest for credit card accounts. It begins by joining daily account features with agreement details to create a staging table. It then aggregates this data into a final reporting table, deriving balance types and part descriptions based on plan numbers. Finally, it calculates monthly conversion metrics and estimates for the next period."

-----

**SQL SCRIPT TO ANALYZE:**

```sql
    SQL:
        {sql_query.replace("{","{{").replace("}","}}")}
"""
    generation_config = {
        "temperature": 1,
        "top_p": 0.9,
    }
    responses_tbl = model.generate_content(
        [extraction_prompt_tbl],
        generation_config=generation_config,
        safety_settings=safety_settings,
        stream=False,
    )
    # print("Response for Combine Format:", responses_tbl.text)

    # responses = model.generate_content(
    #     [extraction_prompt_json],
    #     generation_config=generation_config,
    #     safety_settings=safety_settings,
    #     stream=False,
    # )
    # response 
    # response_json = responses_tbl.text.split("# Data Lineage Report")[0].strip()
    # response_tbl = responses_tbl.text.split("# Data Lineage Report")[1].strip()

    # print(response_json)
    # print(response_tbl)

    full_response_text = responses_tbl.text
    report_markdown = ""
    json_string = full_response_text

    # Split the response into JSON and Markdown parts
    if "# Data Lineage Report" in full_response_text:
        parts = full_response_text.split("# Data Lineage Report", 1)
        json_string = parts[0]
        report_markdown = "# Data Lineage Report" + parts[1]

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
            parser_output_tables = report_markdown
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
        "report_markdown": report_markdown if report_markdown else full_response_text
    }
