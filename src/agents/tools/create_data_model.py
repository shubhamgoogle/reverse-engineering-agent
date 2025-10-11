
import os
import json
from src.agents.shared_libraries.bq_utils import fetch_from_bq
from vertexai.generative_models import GenerativeModel

def get_sql_json_from_bq(application_name: str) -> list:
    return fetch_from_bq(application_name)

def create_data_model_from_bq(application_name: str) -> dict:
    """
    Fetches SQL parser outputs from BigQuery for a given application name.
    For each parser output, it uses a generative model to extract a structured data model
    containing entities, attributes, and relationships.

    Returns:
        dict: A dictionary with the following keys:
            - "status": "success" or "error"
            - "results": List of generated data model JSON objects (on success)
            - "error_message": Error message (on error)
    """
    results = []
    try:
        # Fetch the JSON data from BigQuery using the existing function
        bq_records = get_sql_json_from_bq(application_name)
        if not bq_records:
            return {"status": "success", "results": [], "message": "No records found in BigQuery for the application."}

        model = GenerativeModel("gemini-2.5-pro")

        for record in bq_records:
            parser_output = record.get("parser_output")
            if not parser_output:
                continue

            prompt = f"""
            Given the following JSON data from a Teradata SQL script, extract the core data model.

            **CRITICAL INSTRUCTIONS:**
            1.  **IGNORE AUDIT/METADATA:** You MUST exclude any entities that appear to be for auditing, logging, or metadata purposes. Common patterns to ignore include tables with names containing `_LOG`, `_AUDIT`, `_ERR`, `_TMP`, `_WORK`, or volatile tables. Focus only on entities that represent core business concepts.
            2.  **Extract Core Model:** Identify the primary entities, their attributes, and the relationships between them.
            3.  **Structure Output:** Return a single, clean JSON object with the following structure. Do not include any explanations or surrounding text.

            **Output JSON Structure:**
            {{
              "entities": [
                {{"name": "CoreBusinessEntityName", "attributes": ["attr1", "attr2", ...]}}
              ],
              "relationships": [
                {{"from": "EntityA", "to": "EntityB", "type": "relationship_type", "details": "..."}}
              ],
              "lineage": [
                {{"source": "SourceEntity", "target": "TargetEntity", "transformation": "..."}}
              ]
            }}

            **Input JSON:**
            {parser_output}
            """
            response = model.generate_content(prompt)
            response_text = response.text.strip().strip("` \n")
            if response_text.startswith("json"):
                response_text = response_text[4:].strip()

            try:
                data_model_json = json.loads(response_text)
                results.append(data_model_json)
            except json.JSONDecodeError:
                # If the model response is not valid JSON, skip this record
                continue
    except Exception as e:
        return {"status": "error", "error_message": str(e)}

    return {"status": "success", "results": results}