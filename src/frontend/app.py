import streamlit as st
import requests
import io

# Configuration for the backend API
API_URL = "http://127.0.0.1:8000/analyze-sql"

st.set_page_config(
    page_title="SQL Reverse Engineering Agent",
    layout="wide"
)

def show_sql_analysis_page():
    """
    Displays the page for uploading and analyzing SQL files.
    """
    st.title("SQL File Analysis")
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
                with st.spinner("Analyzing SQL..."):
                    try:
                        # To read the file content, we use a BytesIO object and decode it
                        stringio = io.StringIO(uploaded_file.getvalue().decode("utf-8"))
                        sql_query = stringio.read()

                        # Prepare the request payload
                        payload = {"sql_query": sql_query}

                        # Send the request to the FastAPI backend
                        response = requests.post(API_URL, json=payload)

                        # Check if the request was successful
                        if response.status_code == 200:
                            st.success(f"Successfully analyzed `{uploaded_file.name}`.")
                            st.json(response.json())
                        else:
                            st.error(f"Error analyzing `{uploaded_file.name}`. Status code: {response.status_code}")
                            st.json(response.json())

                    except Exception as e:
                        st.error(f"An unexpected error occurred while processing `{uploaded_file.name}`: {e}")

# --- Main App Navigation ---
st.sidebar.title("Reverse Engineering Agent")
page = st.sidebar.radio("Choose a page", ["SQL File Analysis"])

if page == "SQL File Analysis":
    show_sql_analysis_page()