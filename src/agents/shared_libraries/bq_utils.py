from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from datetime import datetime, timezone
import json
from src.agents.config.settings import Settings

def get_bq_client():
    """Initializes the BigQuery client."""
    try:
        config = Settings.get_settings()
        client = bigquery.Client(project=config.PROJECT_ID)
        return client
    except Exception as e:
        print(f"Could not connect to BigQuery. Please check your GCP authentication. Error: {e}")
        return None

def insert_sql_extract_to_bq(sql_id: str, sql_file_name: str, raw_sql_text: str, parser_output: dict,parser_output_tables:str,application_name:str, processing_status: str):
    """Inserts a record into the raw_sql_extracts table."""
    print("Inserting record into BigQuery...")
    client = get_bq_client()
    if not client:
        print("BigQuery client not available. Skipping insert.")
        return False

    config = Settings.get_settings()
    table_id = f"{config.PROJECT_ID}.{config.REA_SQL_EXTRACTS_DATASET}.{config.REA_SQL_EXTRACTS_TABLE}"

    rows_to_insert = [
        {
            "sql_id": sql_id,
            "sql_file_name": sql_file_name,
            "raw_sql_text": raw_sql_text,
            "parser_output": json.dumps(parser_output),
            "processing_status": processing_status,
            "application_name":application_name,
            "inserted_at": datetime.now(timezone.utc).isoformat(),
            "parser_output_tables" : parser_output_tables
        }
    ]

    try:
        errors = client.insert_rows_json(table_id, rows_to_insert)
        if not errors:
            print(f"Successfully inserted record with sql_id: {sql_id}")
            return True
        else:
            print(f"Failed to insert record into BigQuery: {errors}")
            return False
    except NotFound:
        print(f"Table {table_id} not found. Please create it.")
        # Here you could add logic to create the table if it doesn't exist.
        # For now, we just print an error.
        return False
    except Exception as e:
        print(f"An error occurred during the BigQuery insert operation: {e}")
        return False

def fetch_from_bq(application_name:str):
    """Fetches all records from the raw_sql_extracts table."""
    print(f"Fetching records from BigQuery for application_name:{application_name} ...")
    client = get_bq_client()
    if not client:
        print("BigQuery client not available. Skipping fetch.")
        return []

    config = Settings.get_settings()
    table_id = f"{config.PROJECT_ID}.{config.REA_SQL_EXTRACTS_DATASET}.{config.REA_SQL_EXTRACTS_TABLE}"

    query = f"""
    SELECT sql_file_name,parser_output_tables
    FROM `{table_id}`
    WHERE application_name = @application_name
    ORDER BY inserted_at DESC
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("application_name", "STRING", application_name)
        ]
    )

    try:
        query_job = client.query(query, job_config=job_config)
        results = query_job.result()
        records = [dict(row) for row in results]
        print(f"Fetched {len(records)} records from BigQuery.")
        return records
    except NotFound:
        print(f"Table {table_id} not found. Please create it.")
        return []
    except Exception as e:
        print(f"An error occurred during the BigQuery fetch operation: {e}")
        return []

def fetch_report_data_from_bq(application_name: str) -> list:
    """Fetches sql_file_name and parser_output_tables for a given application."""
    print(f"Fetching report data from BigQuery for application: {application_name}...")
    client = get_bq_client()
    if not client:
        print("BigQuery client not available. Skipping fetch.")
        return []

    config = Settings.get_settings()
    table_id = f"{config.PROJECT_ID}.{config.REA_SQL_EXTRACTS_DATASET}.{config.REA_SQL_EXTRACTS_TABLE}"

    query = f"""
        SELECT sql_file_name, parser_output_tables, parser_output 
        FROM `{table_id}`
        WHERE application_name = @application_name
        ORDER BY sql_file_name
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("application_name", "STRING", application_name)
        ]
    )

    try:
        query_job = client.query(query, job_config=job_config)
        results = query_job.result()
        records = [dict(row) for row in results]
        print(f"Fetched {len(records)} records for the report.")
        return records
    except Exception as e:
        print(f"An error occurred while fetching report data: {e}")
        return []


def get_completed_sql_files_from_bq(application_name: str) -> list:
    """Fetches distinct sql_file_name for a given application from BigQuery."""
    print(f"Fetching completed file names from BigQuery for application: {application_name}...")
    client = get_bq_client()
    if not client:
        print("BigQuery client not available. Skipping fetch.")
        return []

    config = Settings.get_settings()
    table_id = f"{config.PROJECT_ID}.{config.REA_SQL_EXTRACTS_DATASET}.{config.REA_SQL_EXTRACTS_TABLE}"

    query = f"""
        SELECT DISTINCT sql_file_name
        FROM `{table_id}`
        WHERE application_name = @application_name
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("application_name", "STRING", application_name)
        ]
    )

    try:
        query_job = client.query(query, job_config=job_config)
        completed_files = [row.sql_file_name for row in query_job.result()]
        print(f"Found {len(completed_files)} completed files in BigQuery.")
        return completed_files
    except Exception as e:
        print(f"An error occurred while fetching completed files: {e}")
        return []