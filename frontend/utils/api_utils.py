import time
import requests
import streamlit as st
from google.adk.sessions import Session
from utils.schema import ImageData, ChatRequest, ChatResponse, inlineData, fileUriData
from typing import Any, Dict, List
from dotenv import load_dotenv
import google.oauth2.id_token
import os, uuid, json

load_dotenv()

BACKEND_URL = os.environ.get("API_BASE_URL", "http://localhost:8000")

def _make_request_headers() -> dict:
    try:
        request = google.auth.transport.requests.Request()
        audience = BACKEND_URL
        token = google.oauth2.id_token.fetch_id_token(request, audience)
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }
    except Exception as error:
        return {
            "Content-Type": "application/json",
        }

# @st.cache_resource
# def get_token():
#     cloudrun_token = google.oauth2.id_token.fetch_id_token(google.auth.transport.requests.Request(), os.environ.get("API_BASE_URL", "http://localhost:8000"))
#     return cloudrun_token

# if "google_auth_token" not in st.session_state:
#     st.session_state.google_auth_token = get_token()
# AUTH_TOKEN = st.session_state["google_auth_token"]
# headers = {"Authorization": f"Bearer {AUTH_TOKEN}"}



def _get_api_url(path: str) -> str:
    return f"{st.session_state.fastapi_url}{path}"

def _list_sessions_api(app_name: str, user_id: str) -> List[Session]:
    """Fetches list of sessions for a given app and user."""
    if not app_name or not user_id:
        return []

    path = f"/apps/{app_name}/users/{user_id}/sessions"
    try:
        response = requests.get(_get_api_url(path),headers=_make_request_headers())
        response.raise_for_status()
        sessions_data = response.json()
        return sessions_data
    except requests.exceptions.RequestException as e:
        st.error(f"Error listing sessions: {e}")
        return []
    
def _get_session_history(app_name: str, user_id: str,session_id: str) -> List[Session]:
    """Fetches list of sessions for a given app and user."""
    if not app_name or not user_id:
        return []

    path = f"/apps/{app_name}/users/{user_id}/sessions/{session_id}"
    try:
        response = requests.get(_get_api_url(path),headers=_make_request_headers())
        response.raise_for_status()
        sessions_history = response.json()
        # print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
        # print(sessions_history)
        return sessions_history.get("events",[])
    except requests.exceptions.RequestException as e:
        st.error(f"Error getting history sessions: {e}")
        return []   

def _create_session_api(app_name: str, user_id: str, session_id : str) -> None:
    if not app_name or not user_id:
        st.error("App name and User ID are required to create a session.")
        return None

    path = f"/apps/{app_name}/users/{user_id}/sessions/{session_id}"
    try:
        with st.spinner(f"Creating new session for {user_id} in {app_name}..."):
            response = requests.post(_get_api_url(path), json={}, headers=_make_request_headers())  # Empty state
            response.raise_for_status()
            return True
    except requests.exceptions.RequestException as e:
        st.error(
            f"Error creating session: {e}. Check if the session already exists or if app name is valid."
        )
        return None



def generate_tdd(prompt: str, base64_data,gcs_uri, retry_delay=1):
    app_name = "tdd_generate_app"
    session_id = str(uuid.uuid4())
    user_id = "document_generating_user"
    max_retries = 3
    full_response_content = ""
    
    if base64_data:
        inline_data = base64_data
    else:
        inline_data = []
    if gcs_uri:
        file_data=gcs_uri
    else:
        file_data = []
    print("-----------------------Start with generation---------------------")
    _create_session_api(app_name, user_id, session_id)
    request_body = ChatRequest(
        text=prompt,
        inline=inline_data,
        fileData = file_data,
        user_id=user_id,
        session_id=session_id
        )
    path = "/generate"
    
    print("-----------------------Start ATTEMPTS--------------------")
    for attempt in range(max_retries):
        try:
            print("-----------------------ATTEMPT: " +str(attempt) + "----------------------------------" )
            response = requests.post(
                _get_api_url(path), 
                json=request_body.model_dump(), 
                headers=_make_request_headers()
            )
            response.raise_for_status()
            
            result = ChatResponse(**response.json())
            
            if result.error:
                print(f"API returned a structured error: {result.error}")
                return {"section_name": "API Error", "response_text": str(result.error)}

            # Internally validate if the response content is parsable JSON
            try:
                json.loads(result.response)
                return result.response 
            except json.JSONDecodeError:
                print(f"Response is not parsable JSON on attempt {attempt + 1}.")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)

        
        except requests.exceptions.RequestException as e:
            print(f"Request failed on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)


    print("ERROR: All retries failed. Returning error dictionary.")
    return {
        "section_name": "Error Generating the section",
        "response_text": "Failed to get a valid response from the server after multiple attempts.",
        "document_name": "",
    }

def generate_discovery_questionnaire(description: str, product_type: str, retry_delay=1):
    app_name = "discovery_query_app"
    session_id = str(uuid.uuid4())
    user_id = "discovery_query_user"
    max_retries = 3

    _create_session_api(app_name, user_id, session_id)
    request_body = ChatRequest(
        text=description,
        product_type=product_type,
        user_id=user_id,
        session_id=session_id
    )
    path = "/generate_discovery_questionnaire"

    for attempt in range(max_retries):
        try:
            response = requests.post(
                _get_api_url(path),
                json=request_body.model_dump(),
                headers=_make_request_headers()
            )
            response.raise_for_status()

            result = ChatResponse(**response.json())

            if result.error:
                st.error(f"API returned an error: {result.error}")
                return None

            # The response should be a JSON string.
            try:
                json.loads(result.response)
                return result.response
            except json.JSONDecodeError:
                print(f"Response is not parsable JSON on attempt {attempt + 1}.")
                if attempt >= max_retries - 1:
                    st.error("Failed to get a parsable JSON response from the server.")
                    return None
                time.sleep(retry_delay)

        except requests.exceptions.RequestException as e:
            st.error(f"Request failed on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)

    st.error("All retries failed. Could not generate questionnaire.")
    return None

def generate_config(prompt: str, base64_data,gcs_uri, retry_delay=1):
    app_name = "generate_config_app"
    session_id = str(uuid.uuid4())
    user_id = "config_generating_user"
    max_retries = 3
    full_response_content = ""
    
    if base64_data:
        inline_data = base64_data
    else:
        inline_data = []
    if gcs_uri:
        file_data=gcs_uri
    else:
        file_data = []
    print("-----------------------Start with generation---------------------")
    _create_session_api(app_name, user_id, session_id)
    request_body = ChatRequest(
        text=prompt,
        inline=inline_data,
        fileData = file_data,
        user_id=user_id,
        session_id=session_id
        )
    path = "/generate_config"
    
    print("-----------------------Start ATTEMPTS--------------------")
    for attempt in range(max_retries):
        try:
            print("-----------------------ATTEMPT: " +str(attempt) + "----------------------------------" )
            response = requests.post(
                _get_api_url(path), 
                json=request_body.model_dump(), 
                headers=_make_request_headers()
            )
            response.raise_for_status()
            
            result = ChatResponse(**response.json())
            
            if result.error:
                print(f"API returned a structured error: {result.error}")
                return {"section_name": "API Error", "response_text": str(result.error)}

            # Internally validate if the response content is parsable JSON
            try:
                json.loads(result.response)
                return result.response 
            except json.JSONDecodeError:
                print(f"Response is not parsable JSON on attempt {attempt + 1}.")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)

        
        except requests.exceptions.RequestException as e:
            print(f"Request failed on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)


    print("ERROR: All retries failed. Returning error dictionary.")
    return None

def generate_dtm_review(xml_content: str, retry_delay=1):
    app_name = "dtm_review_app"
    session_id = str(uuid.uuid4())
    user_id = "dtm_review_user"
    max_retries = 3

    _create_session_api(app_name, user_id, session_id)
    request_body = ChatRequest(
        text=xml_content,
        user_id=user_id,
        session_id=session_id
    )
    path = "/dtm_review"

    for attempt in range(max_retries):
        try:
            response = requests.post(
                _get_api_url(path),
                json=request_body.model_dump(),
                headers=_make_request_headers()
            )
            response.raise_for_status()
            result = ChatResponse(**response.json())
            if result.error:
                st.error(f"API returned an error: {result.error}")
                return None
            
            # The response should be a JSON string.
            try:
                json.loads(result.response)
                return result.response 
            except json.JSONDecodeError:
                print(f"Response is not parsable JSON on attempt {attempt + 1}.")
                if attempt >= max_retries - 1:
                    st.error("Failed to get a parsable JSON response from the server.")
                    st.text(result.response) # show the raw response
                    return None
                time.sleep(retry_delay)
        except requests.exceptions.RequestException as e:
            st.error(f"Request failed on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
    
    st.error("All retries failed. Could not generate review.")
    return None