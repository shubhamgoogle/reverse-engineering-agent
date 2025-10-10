import streamlit as st
import requests
import io

# Configuration for the backend API
API_BASE_URL = "http://127.0.0.1:8000"
ANALYZE_API_URL = f"{API_BASE_URL}/analyze-sql"

st.set_page_config(
    page_title="SQL Reverse Engineering Agent",
    layout="wide"
)

def show_sql_analysis_page():
    """
    Displays the page for uploading and analyzing SQL files.
    """
    st.title("SQL File Analysis")
    
    application_name = st.text_input(
        "Enter Application Name (e.g., 'CRM', 'Finance_Reporting')",
        key="application_name_input"
    )

    if not application_name:
        st.warning("Please enter an Application Name to proceed.")
        # Disable file uploader if application name is not provided
        uploaded_files = None 
    else:
        st.write(
            "Upload one or more SQL files to analyze their structure and extract the data model. "
            "The agent will process each file and display the resulting JSON data model."
        )
        # Create a file uploader that accepts multiple .sql files
        uploaded_files = st.file_uploader(
            "Choose SQL files",
            type="sql",
            accept_multiple_files=True
        )

    if uploaded_files:
        st.header("Analysis Results")
        # Iterate through each uploaded file
        for uploaded_file in uploaded_files:
            with st.expander(f"Processing: `{uploaded_file.name}`", expanded=True):
                if application_name: # Double-check application_name is present
                    with st.spinner("Analyzing SQL..."):
                        try:
                            # To read the file content, we use a BytesIO object and decode it
                            stringio = io.StringIO(uploaded_file.getvalue().decode("utf-8"))
                            sql_query = stringio.read()

                            # Prepare the request payload, including application_name
                            payload = {
                                "sql_query": sql_query,
                                "application_name": application_name,
                                "sql_file_name": uploaded_file.name
                            }

                            # Send the request to the FastAPI backend
                            response = requests.post(ANALYZE_API_URL, json=payload)

                            # Check if the request was successful
                            if response.status_code == 200:
                                st.success(f"Successfully analyzed `{uploaded_file.name}`.")
                                st.json(response.json())
                            else:
                                st.error(f"Error analyzing `{uploaded_file.name}`. Status code: {response.status_code}")
                                st.json(response.json())

                        except Exception as e:
                            st.error(f"An unexpected error occurred while processing `{uploaded_file.name}`: {e}")
                else:
                    st.error("Application Name is missing. Please provide it before uploading files.")

def show_data_model_page():
    """
    Displays the page for fetching and viewing data models from BigQuery.
    """
    st.title("View SQL Analysis by Application")

    application_name = st.text_input(
        "Enter Application Name to fetch its data model",
        key="data_model_app_name"
    )

    if st.button("Retrieve SQL Analysis Results"):
        if not application_name:
            st.warning("Please enter an Application Name.")
        else:
            with st.spinner(f"Retrieve SQL Analysis Results for `{application_name}`..."):
                try:
                    payload = {"application_name": application_name}
                    response = requests.post(f"{API_BASE_URL}/get-data-model", json=payload)

                    if response.status_code == 200:
                        results = response.json()
                        if results:
                            st.success(f"Successfully fetched data model for `{application_name}`.")
                            st.write(f"Found {len(results)} parser output(s).")
                            for result in results:
                                sql_file_name = result.get("sql_file_name", "Unknown File")
                                parser_output = result.get("parser_output")

                                with st.expander(f"Data Model from: `{sql_file_name}`"):
                                    if isinstance(parser_output, str):
                                        # Handle escaped JSON strings before displaying
                                        parser_output = io.StringIO(parser_output).read()
                                    st.json(parser_output)
                        else:
                            st.info(f"No data model found for application: `{application_name}`.")
                    else:
                        st.error(f"Error fetching data model. Status code: {response.status_code}")
                        st.json(response.json())
                except Exception as e:
                    st.error(f"An unexpected error occurred: {e}")

# --- Main App Navigation ---
st.sidebar.title("Reverse Engineering Agent")
page = st.sidebar.radio("Choose a page", ["SQL File Analysis", "View Data Model"])

if page == "SQL File Analysis":
    show_sql_analysis_page()
elif page == "View Data Model":
    show_data_model_page()