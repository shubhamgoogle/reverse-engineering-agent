import glob
import base64, json
import vertexai
from vertexai.generative_models import GenerativeModel, Part, SafetySetting
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.agents.config.settings import Settings
import uuid
from src.agents.shared_libraries.bq_utils import insert_sql_extract_to_bq
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

def analyze_sqls():
    queries_folder = "data/queries"
    # Ensure the queries folder exists before trying to create files in it
    os.makedirs(queries_folder, exist_ok=True)
    completed_file = os.path.join(queries_folder, "completed_sql_files.txt")

    # Ensure completed_sql_files.txt exists
    if not os.path.exists(completed_file):
        with open(completed_file, "w") as f:
            pass  # Create the file if it doesn't exist

    # Load completed SQL filenames
    with open(completed_file, "r") as f:
        completed_sql_files = set(line.strip() for line in f if line.strip())

    sql_files = glob.glob(os.path.join(queries_folder, "*.sql"))
    if sql_files:
        for sql_file in sql_files:
            sql_filename = os.path.basename(sql_file)
            if sql_filename in completed_sql_files:
                print(f"Skipping already processed file: {sql_filename}")
                continue
            try:
                with open(sql_file, "r") as f:
                    file_query = f.read()
            except Exception as e:
                with open(sql_file, "r", encoding='windows-1252') as f:
                    file_query = f.read()
            if file_query:
                try:
                    #extract_sql_details funcition call will store output in BQ
                    sql_analysis_output = extract_sql_details(file_query)
                    # print(sql_analysis_output)  # For debugging purposes
                    with open(completed_file, "a") as f:
                        f.write(sql_filename + "\n")
                    # The 'with' statement handles closing the file, so f.close() is redundant.
                except Exception as e:
                    # If the exception object 'e' is a dictionary (e.g., from a custom error source),
                    # it needs to be converted to a string for printing to avoid 'unhashable type: dict'.
                    if isinstance(e, dict):
                        print(f"Error for {sql_filename}: {json.dumps(e)}")
                    else:
                        print(f"Error for {sql_filename}: {e}")
            else:
                print(f"{sql_filename} is empty.")
    else:
        print("No SQL files found in the queries folder.")

if __name__ == "__main__":
    analyze_sqls()