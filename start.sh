#!/bin/bash

# --- Configuration for local execution ---
export API_BASE_URL="http://localhost:8000"
export PROJECT_ID="semantic-layer-poc-470809"
export PROJECT_NUMBER="semantic-layer-poc-470809" # Note: This is usually a number.
export REGION="us-central1"
export RUN_AGENT_WITH_DEBUG="False"
export ARTIFACT_GCS_BUCKET="rev-eng-bkt"
export SESSION_DB_URL="sqlite:///./sessions.db"
export RAG_DEFAULT_TOP_K="5"
export RAG_DEFAULT_SEARCH_TOP_K="5"
export RAG_DEFAULT_VECTOR_DISTANCE_THRESHOLD="0.5"

# BigQuery settings
export REA_SQL_EXTRACTS_DATASET="reverse_engineering_agent"
export REA_SQL_EXTRACTS_TABLE="sql_extracts"
export PORT="8501" # Default port for Streamlit locally

# --- Process Management ---
BACKEND_PID=0
FRONTEND_PID=0

cleanup() {
    echo "Shutting down servers..."
    if [ $FRONTEND_PID -ne 0 ]; then kill $FRONTEND_PID; fi
    if [ $BACKEND_PID -ne 0 ]; then kill $BACKEND_PID; fi
    exit
}

trap cleanup SIGINT SIGTERM

# --- Start Servers ---
echo "Starting FastAPI backend server..."
poetry run uvicorn src.main:app --host 0.0.0.0 --port 8000 &
BACKEND_PID=$!
 
echo "Waiting for backend to start..."
poetry run python -c 'import time, socket; s = socket.socket(socket.AF_INET, socket.SOCK_STREAM); [(time.sleep(1), s.connect_ex(("localhost", 8000))) for i in range(60)]'
if ! kill -0 $BACKEND_PID 2>/dev/null; then
    echo "Backend server failed to start."
    exit 1
fi
echo "Backend started."
 
echo "Starting Streamlit frontend server..."
poetry run streamlit run src/frontend/app.py --server.port $PORT --server.address=0.0.0.0 &
FRONTEND_PID=$!

wait $FRONTEND_PID
