import streamlit as st
import requests
import io
import os
import json
from pyvis.network import Network
import streamlit.components.v1 as components

# Configuration for the backend API
# Use an environment variable for the backend URL in production,
# with a fallback to localhost for local development.
API_BASE_URL = os.environ.get("API_BASE_URL", "http://127.0.0.1:8000")
# API_BASE_URL = "https://reverse-engineering-agent-api-172009895677.us-central1.run.app"
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

    # Initialize session state for analysis results
    if 'analysis_results' not in st.session_state:
        st.session_state.analysis_results = {}
    
    application_name = st.text_input(
        "Enter Application Name (e.g., 'CRM', 'Finance_Reporting')",
        key="application_name_input"
    )

    if not application_name:
        st.warning("Please enter an Application Name to proceed.")
        # Disable file uploader and clear previous results if application name is removed
        st.session_state.analysis_results = {}
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
        total_files = len(uploaded_files)
        st.info(f"Found {total_files} file(s) to process.")
        
        # Iterate through each uploaded file
        for i, uploaded_file in enumerate(uploaded_files, 1):
            st.markdown("---")
            st.write(f"**Processing file {i} of {total_files}: `{uploaded_file.name}`**")
            
            file_id = f"{application_name}_{uploaded_file.name}"

            with st.expander(f"Analysis details for `{uploaded_file.name}`", expanded=True):
                # Only perform analysis if results are not already in session state
                if file_id not in st.session_state.analysis_results:
                    if application_name: # Double-check application_name is present
                        with st.spinner(f"Analyzing `{uploaded_file.name}`..."):
                            try:
                                # To read the file content, we use a BytesIO object and decode it
                                stringio = io.StringIO(uploaded_file.getvalue().decode("utf-8", errors="ignore"))
                                sql_query = stringio.read()

                                # Prepare the request payload, including application_name
                                payload = {
                                    "sql_query": sql_query,
                                    "application_name": application_name,
                                    "sql_file_name": uploaded_file.name
                                }

                                # Send the request to the FastAPI backend
                                response = requests.post(ANALYZE_API_URL, json=payload)
                                
                                # Store result in session state
                                st.session_state.analysis_results[file_id] = response.json()
                                st.session_state.analysis_results[file_id]['status_code'] = response.status_code

                            except Exception as e:
                                st.session_state.analysis_results[file_id] = {'error': str(e), 'status_code': 500}
                    else:
                        st.error("Application Name is missing. Please provide it before uploading files.")

                # Display results from session state
                result = st.session_state.analysis_results.get(file_id)
                if result:
                    status_code = result.get('status_code')
                    if status_code == 200:
                        st.success(f"Successfully analyzed `{uploaded_file.name}`.")
                        report_markdown = result.get("report_markdown", "")
                        parser_output = result.get("parser_output", {})
                        
                        # The backend now returns markdown and json. We can offer both for download.
                        if report_markdown:
                            st.download_button(
                                label="Download Report",
                                data=report_markdown,
                                file_name=f"{uploaded_file.name}.csv",
                                mime="text/markdown",
                                key=f"download_btn_{i}"
                            )
                        if parser_output and "error" not in parser_output:
                            st.download_button(
                                label="Download JSON",
                                data=json.dumps(parser_output, indent=2),
                                file_name=f"{uploaded_file.name}.json",
                                mime="application/json",
                                key=f"download_btn_{i}_json"
                            )
                        st.json(parser_output)
                    elif 'error' in result:
                         st.error(f"An unexpected error occurred while processing `{uploaded_file.name}`: {result['error']}")
                    else:
                        st.error(f"Error analyzing `{uploaded_file.name}`. Status code: {status_code}")
                        st.json(result)

def show_data_model_page():
    """
    Displays the page for fetching and viewing data models from BigQuery.
    """
    st.title("View SQL Analysis by Application")

    application_name = st.text_input(
        "Enter Application Name to fetch its SQL analysis results",
        key="data_model_app_name"
    )

    # Initialize or clear session state for results
    if 'view_results' not in st.session_state:
        st.session_state.view_results = None
    if 'view_app_name' not in st.session_state:
        st.session_state.view_app_name = None

    # If the application name changes, clear the previous results
    if st.session_state.view_app_name != application_name:
        st.session_state.view_results = None
        st.session_state.view_app_name = application_name

    if st.button("Retrieve SQL Analysis Results"):
        if not application_name:
            st.warning("Please enter an Application Name.")
            st.session_state.view_results = None # Clear results if button is clicked with no name
        else:
            with st.spinner(f"Retrieving SQL Analysis Results for `{application_name}`..."):
                try:
                    payload = {"application_name": application_name}
                    response = requests.post(f"{API_BASE_URL}/get-data-model", json=payload)

                    if response.status_code == 200:
                        st.session_state.view_results = response.json()
                    else:
                        st.error(f"Error fetching data model. Status code: {response.status_code}")
                        st.json(response.json())
                        st.session_state.view_results = None
                except Exception as e:
                    st.error(f"An unexpected error occurred: {e}")
                    st.session_state.view_results = None

    # Always display results if they are in the session state
    if st.session_state.view_results:
        results = st.session_state.view_results
        st.success(f"Successfully fetched data for `{application_name}`.")
        st.write(f"Found {len(results)} parser output(s).")
        for result in results:
            sql_file_name = result.get("sql_file_name", "Unknown File")
            parser_output = result.get("parser_output")
            with st.expander(f"Data Model from: `{sql_file_name}`", expanded=True):
                # The parser_output from BQ might be a string-escaped JSON.
                st.json(json.loads(parser_output) if isinstance(parser_output, str) else parser_output)
    elif st.session_state.view_results == []: # Handle case where fetch was successful but returned no data
        st.info(f"No data model found for application: `{application_name}`.")

def create_interactive_graph(data_models: list):
    """
    Creates an interactive pyvis graph from the consolidated data model.
    """
    net = Network(height="750px", width="100%", bgcolor="#222222", font_color="white", notebook=True, cdn_resources="in_line")

    added_nodes = set()

    for model in data_models:
        # Add entities as nodes
        for entity in model.get("entities", []):
            entity_name = entity.get("name")
            if entity_name and entity_name not in added_nodes:
                attributes = entity.get("attributes", [])
                title = f"<b>{entity_name}</b><br>Attributes: {', '.join(attributes)}"
                net.add_node(entity_name, label=entity_name, title=title)
                added_nodes.add(entity_name)

        # Add relationships as solid edges
        for rel in model.get("relationships", []):
            source = rel.get("from")
            target = rel.get("to")
            if source and target:
                # Ensure nodes exist before adding an edge
                if source not in added_nodes: net.add_node(source, label=source); added_nodes.add(source)
                if target not in added_nodes: net.add_node(target, label=target); added_nodes.add(target)
                
                rel_type = rel.get("type", "RELATIONSHIP")
                details = rel.get("details", "")
                net.add_edge(source, target, title=f"{rel_type}: {details}", label=rel_type)

        # Add lineage as dashed edges
        for lin in model.get("lineage", []):
            source = lin.get("source")
            target = lin.get("target")
            if source and target:
                # Ensure nodes exist before adding an edge
                if source not in added_nodes: net.add_node(source, label=source); added_nodes.add(source)
                if target not in added_nodes: net.add_node(target, label=target); added_nodes.add(target)

                transformation = lin.get("transformation", "LINEAGE")
                net.add_edge(source, target, title=transformation, dashes=True, color="#00ff00", label="lineage")

    # Generate the HTML file in memory
    net.show("graph.html")
    with open("graph.html", "r", encoding="utf-8") as f:
        html_content = f.read()
    
    return html_content

def show_consolidated_model_page():
    """
    Displays the page for generating a consolidated data model from all SQL
    analysis results for a given application.
    """
    st.title("Generate Consolidated Data Model")
    st.write(
        "This page fetches all individual SQL analysis results for an application from BigQuery "
        "and uses a generative model to create a single, consolidated data model."
    )

    application_name = st.text_input(
        "Enter Application Name to generate its consolidated data model",
        key="consolidated_model_app_name"
    )
    
    # Reset state if application name changes
    if 'app_name' not in st.session_state or st.session_state.app_name != application_name:
        st.session_state.app_name = application_name
        st.session_state.data_models = None
        st.session_state.html_content = None
        
    if st.button("Generate Consolidated Model"):
        if not application_name:
            st.warning("Please enter an Application Name.")
        else:
            with st.spinner(f"Generating consolidated model for `{application_name}`... This may take a moment."):
                st.session_state.data_models = None # Clear previous results
                st.session_state.html_content = None
                try:
                    payload = {"application_name": application_name}
                    response = requests.post(f"{API_BASE_URL}/create-data-model", json=payload)

                    if response.status_code == 200:
                        result = response.json()
                        st.session_state.data_models = result.get("results")
                        if st.session_state.data_models:
                            st.success(f"Successfully generated model for `{application_name}`.")
                            st.session_state.html_content = create_interactive_graph(st.session_state.data_models)
                        else:
                            st.info("The model generation resulted in no data. This could be due to no records found or an issue during processing.")
                    else:
                        st.error(f"Error generating model. Status code: {response.status_code}")
                        st.json(response.json())
                except Exception as e:
                    st.error(f"An unexpected error occurred: {e}")

    # Display the results if they exist in the session state
    if st.session_state.get("html_content"):
        # Add a download button for the JSON data
        st.download_button(
            label="Download Data Model as JSON",
            data=json.dumps(st.session_state.data_models, indent=2),
            file_name=f"{st.session_state.app_name}_consolidated_model.json",
            mime="application/json",
        )
        components.html(st.session_state.html_content, height=800)



# --- Main App Navigation ---
st.sidebar.title("Reverse Engineering Agent")
page = st.sidebar.radio("Choose a page", ["SQL File Analysis", "View Data Model", "Generate Consolidated Data Model"])

if page == "SQL File Analysis":
    show_sql_analysis_page()
elif page == "View Data Model":
    show_data_model_page()
elif page == "Generate Consolidated Data Model":
    show_consolidated_model_page()