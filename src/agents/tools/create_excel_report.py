import io
import pandas as pd
import re
import json
from src.agents.shared_libraries.bq_utils import fetch_report_data_from_bq

def sanitize_sheet_name(name: str) -> str:
    """
    Sanitizes a string to be a valid Excel sheet name.
    - Removes invalid characters: [ ] : * ? / \\
    - Truncates to 31 characters.
    """
    # Remove invalid characters
    name = re.sub(r'[\[\]:*?/\\]', '', name)
    # Truncate to 31 characters
    return name[:31]

def create_excel_report(application_name: str) -> bytes:
    """
    Fetches report data from BigQuery and generates an Excel file in memory.

    Args:
        application_name: The name of the application to generate the report for.

    Returns:
        The Excel file as a bytes object.
    """
    report_data = fetch_report_data_from_bq(application_name)

    if not report_data:
        # If no data, return an empty Excel file with a notice.
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_empty = pd.DataFrame([{"message": f"No data found for application: {application_name}"}])
            df_empty.to_excel(writer, sheet_name='Notice', index=False)
        return output.getvalue()

    # --- Logic for the new Entity Summary sheet ---
    summary_data = []
    for record in report_data:
        sql_file_name = record.get("sql_file_name", "Untitled")
        json_content_str = record.get("parser_output", "{}")

        try:
            # The parser_output from BQ might be a string, so we need to load it
            if isinstance(json_content_str, str):
                json_content = json.loads(json_content_str)
            else:
                json_content = json_content_str # It might already be a dict

            entities = json_content.get("entities", [])
            if not entities:
                continue

            for entity in entities:
                summary_data.append({
                    "SQL File Name": sql_file_name,
                    "Table Name": entity.get("entity_name"),
                    "Operation Type": entity.get("entity_type"),
                    "Creation Source": entity.get("creation_source")
                })
        except (json.JSONDecodeError, TypeError):
            # If JSON is malformed or not present, skip this record for the summary
            continue

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # Write the new Entity Summary sheet first
        if summary_data:
            df_summary = pd.DataFrame(summary_data)
            df_summary.to_excel(writer, sheet_name="Report Summary", index=False)

        # --- Existing logic to create a sheet per SQL file ---
        for record in report_data:
            sql_file_name = record.get("sql_file_name", "Untitled")
            markdown_content = record.get("parser_output_tables", "")
            sheet_name = sanitize_sheet_name(sql_file_name)
            df = pd.DataFrame([markdown_content])
            df.to_excel(writer, sheet_name=sheet_name, index=False, header=False)

    return output.getvalue()