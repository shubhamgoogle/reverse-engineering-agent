import os
import sys
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# Add project root to the Python path to allow absolute imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.agents.tools.sql_analysis import extract_sql_details

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


@app.post("/analyze-sql", summary="Analyze a SQL query")
async def analyze_sql(request: SQLQueryRequest):
    """
    Accepts a SQL query, processes it using the reverse-engineering agent,
    and returns the extracted data model as JSON.
    """
    try:
        analysis_result = extract_sql_details(request.sql_query)
        return analysis_result
    except Exception as e:
        # Catch potential exceptions from the agent and return a proper HTTP error
        raise HTTPException(status_code=500, detail=str(e))