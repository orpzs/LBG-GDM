import streamlit as st
import requests
import os


st.set_page_config(layout="wide")

st.title("SQL Analysis")

sql_query = st.text_area("Enter your SQL query here", height=300)

if st.button("Analyze SQL"):
    if sql_query:
        try:
            fastapi_url = os.environ.get("API_BASE_URL", "http://localhost:8000")
            response = requests.post(f"{fastapi_url}/sql_analysis", json={"sql_query": sql_query})
            response.raise_for_status()  # Raise an exception for bad status codes
            st.json(response.json())
        except requests.exceptions.RequestException as e:
            st.error(f"An error occurred while communicating with the API: {e}")
    else:
        st.warning("Please enter a SQL query to analyze.")
