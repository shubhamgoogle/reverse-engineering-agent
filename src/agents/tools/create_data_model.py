
import os
import json
from src.agents.shared_libraries.bq_utils import fetch_from_bq
from vertexai.generative_models import GenerativeModel

def get_sql_json_from_bq(application_name: str) -> list:
    return fetch_from_bq(application_name)

def create_data_model_from_jsonl() -> dict:
    """
    Reads a JSONL file where each line contains SQL-related data in JSON format.
    For each line, uses a generative model to extract:
        - All entities and their attributes
        - Relationships among entities (including data lineage if possible)

    The extracted information is structured as a JSON object with the following keys:
        - "entities": List of entities with their attributes
        - "relationships": List of relationships between entities, including type and details
        - "lineage": List of data lineage mappings, including source, target, and transformation

    Each result is written to an output JSONL file, with each line containing only the generated data model JSON.

    Returns:
        dict: A dictionary with the following keys:
            - "status": "success" or "error"
            - "output_file": Path to the output JSONL file (on success)
            - "results": List of generated data model JSON objects (on success)
            - "error_message": Error message (on error)
    """
    file_path = "/Users/shubu/Documents/github_repo/cloudsql-jump-start-solution-for-genai/gke/data-modelling-agent/multi_tool_agent/data2.jsonl"
    output_file_path = "/Users/shubu/Documents/github_repo/cloudsql-jump-start-solution-for-genai/gke/data-modelling-agent/multi_tool_agent/data_model_outputs.jsonl"
    results = []
    try:
        with open(file_path, "r") as f:
            lines = f.readlines()
        model = GenerativeModel("gemini-2.5-pro")
        with open(output_file_path, "w") as out_f:
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                prompt = (
                    "Given the following JSONL data representing SQL information, extract all entities, their attributes, "
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
                    f"Input:\n{line}"
                )
                response = model.generate_content(prompt)
                # Clean up the response to ensure it's a valid JSON object
                response_text = response.text.strip()
                # Remove code block markers if present
                if response_text.startswith("```"):
                    response_text = response_text.strip("` \n")
                    if response_text.startswith("json"):
                        response_text = response_text[4:].strip()
                try:
                    data_model_json = json.loads(response_text)
                except Exception:
                    # If not valid JSON, skip this line
                    continue
                out_f.write(json.dumps(data_model_json) + "\n")
                results.append(data_model_json)
    except Exception as e:
        return {"status": "error", "error_message": str(e)}

    return {"status": "success", "output_file": output_file_path, "results": results} 