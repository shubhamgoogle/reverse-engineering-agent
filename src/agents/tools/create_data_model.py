
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

            prompt = (
                "Given the following JSON data representing SQL information, extract all entities, their attributes, "
                "and the relationships among them (including data lineage if possible). "
                "Return a single JSON object with the following structure:\n"
                "{\n"
                "  \"entities\": [\n"
                "    {\"name\": \"EntityName\", \"attributes\": [\"attr1\", \"attr2\", ...]}\n"
                "  ],\n"
                "  \"relationships\": [\n"
                "    {\"from\": \"EntityA\", \"to\": \"EntityB\", \"type\": \"relationship_type\", \"details\": \"...\"}\n"
                "  ],\n"
                "  \"lineage\": [\n"
                "    {\"source\": \"EntityA\", \"target\": \"EntityB\", \"transformation\": \"...\"}\n"
                "  ]\n"
                "}\n"
                "Only return the JSON object, no explanation or extra formatting.\n\n"
                f"Input:\n{parser_output}"
            )
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