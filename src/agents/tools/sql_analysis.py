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
    Analyze the provided SQL script. Your task is to extract a structured **Data Model**. Focus ONLY on the **entities (tables)**, their **attributes (columns)**, and the **relationships (joins)** between them.

    **IMPORTANT INSTRUCTIONS:**
    1.  **Focus on the Data Model:** Extract the static data model. Do NOT include procedural steps, transformation logic (like CASE statements), or operational commands (DELETE, COLLECT STATS).
    2.  **Prioritize DDL for Schema:** Extract column names and data types from `CREATE TABLE` statements. This is the primary source for attribute information.
    3.  **Identify Relationships:** Accurately capture all `JOIN` operations between entities, including the type of join and the columns used in the `ON` clause.
    4.  **Infer Entities from Usage:** If a table is not explicitly created with a DDL statement, infer its existence and role (e.g., SOURCE_TABLE) from its usage in `FROM` or `JOIN` clauses.

    Return the complete output as a single JSON object with the following structure.

    Output JSON:

    {{
      "job_metadata": {{
        "job_name": "Extracted name from script comments or file name",
        "default_database": "The database set by the DATABASE command"
      }},
      "entities": [ // A list of all tables defined or referenced in the script.
        {{
          "entity_name": "The name of the table/entity (e.g., FR36_TXN_REPORT_SUMMARY)",
          "database": "The database/project where the entity resides",
          "entity_role": "Classification of the entity's use (e.g., TARGET_TABLE, SOURCE_TABLE, LOOKUP_TABLE)",
          "creation_source": "The source of the schema definition (e.g., 'CREATE TABLE DDL', 'Inferred from SELECT')",
          "primary_key": ["List of columns from the Unique Primary Index (UPI)"],
          "attributes": [
            {{
              "attribute_name": "The column name (e.g., RETAIL_AM)",
              "data_type": "The column's data type, extracted from DDL (e.g., DECIMAL(15,2))",
              "is_nullable": "Boolean (true/false) based on DDL"
            }}
          ]
        }}
      ],
      "relationships": [ // Describes how entities are joined in SELECT statements.
        {{
          "type": "The join type (e.g., INNER, LEFT, CROSS)",
          "left_entity": "The full name of the table on the left side of the join",
          "right_entity": "The full name of the table on the right side of the join",
          "join_conditions": [
            "A list of strings, where each string is a single join condition (e.g., 'A.ACCOUNT_KEY = B.ACCOUNT_KEY')"
          ]
        }}
      ]
    }}

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
