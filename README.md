# Reverse Engineering Agent

> **Note**: This is an AI-generated page. If you find any issues or missing information, please create a bug.

This project provides a web-based agent to reverse-engineer SQL files, extract data models, and visualize them. It consists of a Python FastAPI backend for analysis and a Streamlit frontend for user interaction.


## Prerequisites

Before you begin, ensure you have the following installed:
*   [Python 3.8+](https://www.python.org/)
*   [Poetry](https://python-poetry.org/docs/#installation) for dependency management.
*   [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) (for deployment).

## Local Development

Follow these steps to set up and run the application on your local machine.

### 1. Installation

Clone the repository and install the required Python dependencies using Poetry. This command will create a virtual environment inside the project directory.

```shell
make install
```

Alternatively, you can run the poetry command directly:

```shell
poetry install
```

### 2. Running the Application

To run both the backend and frontend services concurrently, use the following command:

```shell
make dev
```

This will execute the `start.sh` script, which:
1.  Sets the necessary environment variables for local execution.
2.  Starts the FastAPI backend on `http://localhost:8000`.
3.  Starts the Streamlit frontend on `http://localhost:8501`.

You can access the web application by navigating to `http://localhost:8501` in your browser.

### Individual Services

You can also run the services individually:

*   **Start only the backend:**
    ```shell
    make dev-backend
    ```

*   **Start only the frontend:**
    ```shell
    make dev-frontend
    ```

## Deployment to Google Cloud Run

The `deploy.sh` script automates the process of building and deploying the application to Google Cloud Run.

### 1. Prerequisites for Deployment

*   Authenticate with Google Cloud:
    ```shell
    gcloud auth login
    gcloud auth application-default login
    ```
*   Enable the required Google Cloud services (Cloud Build, Cloud Run, Artifact Registry).
*   Ensure you have a Service Account with the necessary permissions.

### 2. Configure Deployment Script

Before running the deployment script, you must update the configuration variables at the top of the `deploy.sh` file:

```shellscript
# deploy.sh

# --- Configuration ---
export REGION="us-central1"
export PROJECT="your-gcp-project-id" # <-- UPDATE THIS

export API_APP="reverse-engineering-agent-api"
export FRONTEND_APP="reverse-engineering-agent-frontend"
export SERVICE_ACCOUNT="your-service-account@your-gcp-project-id.iam.gserviceaccount.com" # <-- UPDATE THIS
```

### 3. Run the Deployment Script

Execute the script from the root of the project directory:

```shell
bash deploy.sh
```

The script will perform the following steps:
1.  Build the backend Docker image using Cloud Build and push it to Google Container Registry (GCR).
2.  Deploy the backend image to Cloud Run.
3.  Build the frontend Docker image using Cloud Build and push it to GCR.
4.  Deploy the frontend image to Cloud Run, automatically setting the `API_BASE_URL` to point to the newly deployed backend service.

Once complete, the script will output the URL for the deployed frontend application.