import glob
import base64, json
import vertexai
from vertexai.generative_models import GenerativeModel, Part, SafetySetting
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.agents.config.settings import Settings
import uuid, json
from src.agents.shared_libraries.bq_utils import insert_sql_extract_to_bq, get_completed_sql_files_from_bq
import sys

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

def extract_sql_details(sql_query, application_name: str,sql_file_name:str):
    # Check if the file has already been processed for this application
    completed_files = get_completed_sql_files_from_bq(application_name)
    print(completed_files)
    if sql_file_name in completed_files:
        message = f"Skipping already processed file '{sql_file_name}' for application '{application_name}'."
        print(message)
        # Return a clear message to the frontend
        return {"status": "skipped", "message": message, "sql_file_name": sql_file_name}

    sql_id = str(uuid.uuid4())
    # try:
        
    if len(sql_query) < 10:
        sql_query = 'No SQL'
    print("Inside extract_sql_details function")
    print(sql_query)
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
    
    extraction_prompt = f"""
You are an expert data lineage analysis agent. Your mission is to meticulously analyze the provided Teradata BTEQ script and extract a complete **Data Lineage and Transformation Map**. You must capture the static data model, the relationships between entities, and, most importantly, the dynamic flow of data and the business logic used to transform it.

**CORE INSTRUCTIONS:**

1.  **Parse the Entire Script**: Analyze all SQL commands but **ignore Teradata BTEQ control commands** (e.g., `.IF`, `.GOTO`, `.LABEL`, `.SET`, `.QUIT`).
2.  **Extract Schema from DDL**: Identify all tables (entities). Prioritize `CREATE TABLE` statements for extracting attributes and data types. **Crucially, you must find and parse `CREATE TABLE` statements even if they are inside comment blocks (`/* ... */`)**.
3.  **Infer Schema from DML**: If a table's `CREATE` statement is not present, infer its existence and attributes from its usage in `INSERT`, `UPDATE`, `SELECT`, or `JOIN` clauses. Mark its `creation_source` as 'Inferred'.
4.  **Map Data Flows**: For **every** `INSERT ... SELECT` and `UPDATE` statement, create a detailed attribute-level lineage map. This map must show exactly how each attribute in the target table is populated.
5.  **Identify Relationships**: Document all `JOIN` operations, specifying the join type, the entities involved, and the exact join conditions.

**OUTPUT FORMAT:**

Return the complete analysis as a single, well-formed JSON object. Adhere strictly to the structure below.

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
        "A list of string expressions from the ON clause, e.g., 'T1.AGRMNT_ID = T2.AGRMNT_ID'"
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
          "source_attributes": ["List of all source columns used in the derivation, e.g., ['T1.PLAN_NO']"],
          "transformation_logic": "The exact SQL expression or logic used to derive the value. Preserve all details, including functions, CASE statements, and calculations. Example: 'CASE WHEN T1.PLAN_NO IN (10002,10003,10004,10005,10006) THEN 1 ... ELSE NULL END'"
        }},
        {{
          "target_attribute": "ACCRUED_INT",
          "source_attributes": ["T1.ACCRUED_INT"],
          "transformation_logic": "SUM(T1.ACCRUED_INT) AS ACCRUED_INT"
        }}
      ]
    }}
  ]
}}
```
    SQL:
        {sql_query.replace("{","{{").replace("}","}}")}
"""
        
    generation_config = {
        "temperature": 1,
        "top_p": 0.9,
    }
    responses = model.generate_content(
        [extraction_prompt],
        generation_config=generation_config,
        safety_settings=safety_settings,
        stream=False,
    )
    
    response_text = responses.text.replace("```","" ).replace("json","")
    print("Response from model:")
    print(response_text)
    
    try:
        # Validate JSON
        parser_output = json.loads(response_text)
        processing_status = "NEW"
    except json.JSONDecodeError as e:
        parser_output = {"error": "Invalid JSON response from model", "details": str(e), "response_text": response_text}
        processing_status = "ERROR"

    # Insert into BigQuery
    insert_sql_extract_to_bq(
        sql_id=sql_id,
        sql_file_name=sql_file_name,
        raw_sql_text=sql_query,
        parser_output=parser_output,
        processing_status=processing_status,
        application_name=application_name,
    )
    
    return parser_output
