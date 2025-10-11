#!/bin/bash

# Exit on error
set -e

# --- Configuration ---
export REGION="us-central1"
export PROJECT="semantic-layer-poc-470809"

export API_APP="reverse-engineering-agent-api"
export FRONTEND_APP="reverse-engineering-agent-frontend"
export SERVICE_ACCOUNT="rea-api-sa@${PROJECT}.iam.gserviceaccount.com"

export API_TAG="gcr.io/${PROJECT}/${API_APP}"
export FRONTEND_TAG="gcr.io/${PROJECT}/${FRONTEND_APP}"

gcloud config set project ${PROJECT}

# --- Build Backend API Image ---
echo "Building backend API image using cloudbuild.api.yaml..."
gcloud builds submit . --config=cloudbuild.api.yaml

# --- Deploy Backend API ---
echo "Deploying backend API to Cloud Run..."
gcloud run deploy ${API_APP} \
  --image ${API_TAG} \
  --platform managed \
  --region ${REGION} \
  --port 8000 \
  --service-account ${SERVICE_ACCOUNT} \
  --allow-unauthenticated \
  --set-env-vars="PROJECT_ID=${PROJECT},PROJECT_NUMBER=172009895677,REGION=${REGION},RUN_AGENT_WITH_DEBUG=False,ARTIFACT_GCS_BUCKET=rev-eng-bkt,SESSION_DB_URL=sqlite:///./sessions.db,RAG_DEFAULT_TOP_K=5,RAG_DEFAULT_SEARCH_TOP_K=5,RAG_DEFAULT_VECTOR_DISTANCE_THRESHOLD=0.5,REA_SQL_EXTRACTS_DATASET=reverse_engineering_agent,REA_SQL_EXTRACTS_TABLE=sql_extracts"
  
# --- Get Backend URL ---
echo "Fetching backend URL..."
API_URL=$(gcloud run services describe ${API_APP} --platform managed --region ${REGION} --format 'value(status.url)')
echo "Backend URL: ${API_URL}"

# --- Build Frontend Image ---
echo "Building frontend image using cloudbuild.frontend.yaml..."
gcloud builds submit . --config=cloudbuild.frontend.yaml

# --- Deploy Frontend ---
echo "Deploying frontend to Cloud Run..."
gcloud run deploy ${FRONTEND_APP} \
  --image ${FRONTEND_TAG} \
  --platform managed \
  --region ${REGION} \
  --allow-unauthenticated \
  --port 8501 \
  --set-env-vars="API_BASE_URL=https://reverse-engineering-agent-api-172009895677.us-central1.run.app"

echo "Deployment complete."