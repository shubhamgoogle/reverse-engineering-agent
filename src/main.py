import os
import sys
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# Add project root to the Python path to allow absolute imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.agents.tools.sql_analysis import extract_sql_details
from src.agents.tools.create_data_model import get_sql_json_from_bq, create_data_model_from_bq

# Set a default config path before importing settings
# This is crucial for the settings module to find the configuration file.
os.environ.setdefault("CONFIG_PATH", "config.yaml")


app = FastAPI(
    title="Reverse Engineering Agent API",
    description="An API to analyze SQL scripts and extract data models.",
    version="0.1.0",
)

class SQLQueryRequest(BaseModel):
    """Request model for a single SQL query."""
    sql_query: str
    application_name: str
    sql_file_name: str

class DataModelRequest(BaseModel):
    """Request model for fetching a data model by application name."""
    application_name: str


@app.post("/analyze-sql", summary="Analyze a SQL query")
async def analyze_sql(request: SQLQueryRequest):
    """
    Accepts a SQL query, processes it using the reverse-engineering agent,
    and returns the extracted data model as JSON.
    """
    try:
        analysis_result = extract_sql_details(
            sql_query=request.sql_query,
            application_name=request.application_name,
            sql_file_name=request.sql_file_name
        )
        return analysis_result
    except Exception as e:
        # Catch potential exceptions from the agent and return a proper HTTP error
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/get-data-model", summary="Get data model from BigQuery")
async def get_data_model(request: DataModelRequest):
    """
    Accepts an application name, fetches the corresponding SQL parser outputs
    from BigQuery, and returns them.
    """
    try:
        # This function now fetches data from BQ and returns it
        data_model_result = get_sql_json_from_bq(application_name=request.application_name)
        return data_model_result
    except Exception as e:
        # Catch potential exceptions and return a proper HTTP error
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/create-data-model", summary="Create a consolidated data model from BigQuery records")
async def create_data_model(request: DataModelRequest):
    """
    Accepts an application name, fetches all its SQL parser outputs from BigQuery,
    and uses a generative model to create a consolidated data model.
    """
    try:
        # This function fetches records and generates a new data model
        consolidated_model = create_data_model_from_bq(application_name=request.application_name)
        return consolidated_model
    except Exception as e:
        # Catch potential exceptions and return a proper HTTP error
        raise HTTPException(status_code=500, detail=str(e))
