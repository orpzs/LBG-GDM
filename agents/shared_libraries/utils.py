"""
Copyright 2025 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from google.cloud import storage
from agents.shared_libraries.config_load import Settings
import base64
import re
from .schema import ChatRequest, ImageData, inlineData, fileUriData
from google.genai import types
import hashlib
import json
from google.adk.artifacts import GcsArtifactService
from google.oauth2 import id_token
from google.genai.types import Part, HttpOptions
from google import genai
from google.adk.models.google_llm import BaseLlm, Gemini
from functools import cached_property
from typing_extensions import override
from google.genai import Client, types
from google import genai
config = Settings.load_configs()

customAFC = types.GenerateContentConfig(
        automatic_function_calling=types.AutomaticFunctionCallingConfig(
            maximum_remote_calls=100
            )
        )

# class CustomGeminiClient(Gemini):
#     def __init__(self, model: str, api_endpoint: str, credentials):
#         super().__init__(model=model)
#         self._api_endpoint = api_endpoint
#         self._credentials = credentials
    
#     @cached_property
#     @override
#     def api_client(self)->Client:
#         client_options = {"api_endpoint" : self._api_endpoint}

#         return genai.Client(vertexai=True, project=config.PROJECT_ID, location=config.REGION, credentials=credentials, http_options=HttpOptions(base_url=f"{self._api_endpoint}/google-llm", api_version="v1"))



# creds = credentials.Credentials(token=config.ID_TOKEN.get())
# base_url = config.LLM_PROXY_ENDPOINT
# credentials = id_token.fetch_id_token_credentials(audience=base_url)
# client_options = {"api_endpoint": config.LLM_PROXY_ENDPOINT}
# print(client_options)

# proxyModel = CustomGeminiClient(model=config.LLM_MODEL, api_endpoint=config.LLM_PROXY_ENDPOINT, credentials=credentials)
proxyModel = config.LLM_MODEL


def store_uploaded_image_as_artifact(
    artifact_service: GcsArtifactService,
    app_name: str,
    user_id: str,
    session_id: str,
    image_data: ImageData,
) -> tuple[str, bytes]:
    """
    Store an uploaded image as an artifact in Google Cloud Storage.

    Args:
        artifact_service: The artifact service to use for storing artifacts
        app_name: The name of the application
        user_id: The ID of the user
        session_id: The ID of the session
        image_data: The image data to store

    Returns:
        tuple[str, bytes]: A tuple containing the image hash ID and the image byte
    """

    # Decode the base64 image data and use it to generate a hash id
    image_byte = base64.b64decode(image_data.serialized_image)
    hasher = hashlib.sha256(image_byte)
    image_hash_id = hasher.hexdigest()[:12]

    artifact_versions = artifact_service.list_versions(
        app_name=app_name,
        user_id=user_id,
        session_id=session_id,
        filename=image_hash_id,
    )
    if artifact_versions:
        print(f"Image {image_hash_id} already exists in GCS, skipping upload")

        return image_hash_id, image_byte

    artifact_service.save_artifact(
        app_name=app_name,
        user_id=user_id,
        session_id=session_id,
        filename=image_hash_id,
        artifact=types.Part(
            inline_data=types.Blob(mime_type=image_data.mime_type, data=image_byte)
        ),
    )

    return image_hash_id, image_byte

def store_uploaded_pdf_as_artifact(
    artifact_service: GcsArtifactService,
    app_name: str,
    user_id: str,
    session_id: str,
    inline_data: inlineData,
) -> tuple[str, bytes]:
    """
    Store an uploaded image as an artifact in Google Cloud Storage.

    Args:
        artifact_service: The artifact service to use for storing artifacts
        app_name: The name of the application
        user_id: The ID of the user
        session_id: The ID of the session
        pdf_data: The image data to store

    Returns:
        tuple[str, bytes]: A tuple containing the image hash ID and the image byte
    """

    # Decode the base64 image data and use it to generate a hash id
    data_byte = base64.b64decode(inline_data.serialized_data)
    hasher = hashlib.sha256(data_byte)
    data_hash_id = hasher.hexdigest()[:12]

    artifact_versions = artifact_service.list_versions(
        app_name=app_name,
        user_id=user_id,
        session_id=session_id,
        filename=data_hash_id,
    )
    if artifact_versions:
        print(f"Data {data_hash_id} already exists in GCS, skipping upload")

        return data_hash_id, data_byte

    artifact_service.save_artifact(
        app_name=app_name,
        user_id=user_id,
        session_id=session_id,
        filename=data_hash_id,
        artifact=types.Part(
            inline_data=types.Blob(mime_type=inline_data.mime_type, data=data_byte)
        ),
    )

    return data_hash_id, data_byte

def download_image_from_gcs(
    artifact_service: GcsArtifactService,
    app_name: str,
    user_id: str,
    session_id: str,
    image_hash: str,
) -> tuple[str, str] | None:
    """
    Downloads an image artifact from Google Cloud Storage and
    returns it as base64 encoded string with its MIME type.
    Uses local caching to avoid redundant downloads.

    Args:
        artifact_service: The artifact service to use for downloading artifacts
        app_name: The name of the application
        user_id: The ID of the user
        session_id: The ID of the session
        image_hash: The hash identifier of the image to download

    Returns:
        tuple[str, str] | None: A tuple containing (base64_encoded_data, mime_type), or None if download fails
    """
    try:
        artifact = artifact_service.load_artifact(
            app_name=app_name,
            user_id=user_id,
            session_id=session_id,
            filename=image_hash,
        )
        if not artifact:
            print(f"Image {image_hash} does not exist in GCS Artifact Service")
            return None

        # Get the blob and mime type
        image_data = artifact.inline_data.data
        mime_type = artifact.inline_data.mime_type

        print(f"Downloaded image {image_hash} with type {mime_type}")

        return base64.b64encode(image_data).decode("utf-8"), mime_type
    except Exception as e:
        print(f"Error downloading image from GCS: {e}")
        return None

def download_pdf_from_gcs(
    artifact_service: GcsArtifactService,
    app_name: str,
    user_id: str,
    session_id: str,
    pdf_hash: str,
) -> tuple[str, str] | None:
    """
    Downloads an image artifact from Google Cloud Storage and
    returns it as base64 encoded string with its MIME type.
    Uses local caching to avoid redundant downloads.

    Args:
        artifact_service: The artifact service to use for downloading artifacts
        app_name: The name of the application
        user_id: The ID of the user
        session_id: The ID of the session
        image_hash: The hash identifier of the image to download

    Returns:
        tuple[str, str] | None: A tuple containing (base64_encoded_data, mime_type), or None if download fails
    """
    try:
        artifact = artifact_service.load_artifact(
            app_name=app_name,
            user_id=user_id,
            session_id=session_id,
            filename=pdf_hash,
        )
        if not artifact:
            print(f"Image {pdf_hash} does not exist in GCS Artifact Service")
            return None

        # Get the blob and mime type
        pdf_data = artifact.inline_data.data
        mime_type = artifact.inline_data.mime_type

        print(f"Downloaded image {pdf_hash} with type {mime_type}")

        return base64.b64encode(pdf_data).decode("utf-8"), mime_type
    except Exception as e:
        print(f"Error downloading image from GCS: {e}")
        return None
    
# def format_user_request_to_adk_content_and_store_artifacts(
#     request: ChatRequest, app_name: str, artifact_service: GcsArtifactService
# ) -> types.Content:
#     """Format a user request into ADK Content format.

#     Args:
#         request: The chat request object containing text and optional files
#         app_name: The name of the application
#         artifact_service: The artifact service to use for storing artifacts

#     Returns:
#         types.Content: The formatted content for ADK
#     """
#     # Create a list to hold parts
#     parts = []

#     # Handle image files if present
#     for data in request.files:
#         # Process the image and add string placeholder

#         image_hash_id, image_byte = store_uploaded_image_as_artifact(
#             artifact_service=artifact_service,
#             app_name=app_name,
#             user_id=request.user_id,
#             session_id=request.session_id,
#             image_data=data,
#         )

#         # Add inline data part
#         parts.append(
#             types.Part(
#                 inline_data=types.Blob(mime_type=data.mime_type, data=image_byte)
#             )
#         )

#         # Add image placeholder identifier
#         placeholder = f"[IMAGE-ID {image_hash_id}]"
#         parts.append(types.Part(text=placeholder))

#     # Handle if user didn't specify text input
#     if not request.text:
#         request.text = " "

#     parts.append(types.Part(text=request.text))

#     # Create and return the Content object
#     return types.Content(role="user", parts=parts)

def format_user_request_to_adk_content_and_store_artifacts(
    request: ChatRequest, app_name: str, artifact_service: GcsArtifactService
) -> types.Content:
    """Format a user request into ADK Content format.

    Args:
        request: The chat request object containing text and optional files
        app_name: The name of the application
        artifact_service: The artifact service to use for storing artifacts

    Returns:
        types.Content: The formatted content for ADK
    """
    # Create a list to hold parts
    parts = []

    # Handle image files if present
    for data in request.inline:
        print("PROCESSING THE DATA FROM THE BASE64 ENCODED DATA")
        # Process the image and add string placeholder
        
        data_hash_id, data_byte = store_uploaded_pdf_as_artifact(
            artifact_service=artifact_service,
            app_name=app_name,
            user_id=request.user_id,
            session_id=request.session_id,
            inline_data=data,
        )
        
        # Add inline data part
        parts.append(
            types.Part(
                inline_data=types.Blob(mime_type=data.mime_type, data=data_byte)
            )
        )

        # Add image placeholder identifier
        placeholder = f"[Data-ID {data_hash_id}]"
        parts.append(types.Part(text=placeholder))
    
    if request.fileData:
        for data in request.fileData:
            file_data = types.FileData(mime_type=data.mimeType, file_uri=data.fileUri)
            parts.append(types.Part(file_data=file_data))

    # Handle if user didn't specify text input
    if not request.text:
        request.text = " "

    
    
    # if request.fileData:
    #     file_data = types.FileData(mime_type=request.fileUriData.mimeType, file_uri=request.fileUriData.fileUri)
    #     parts.append(types.Part(file_data=file_data))
    
    parts.append(types.Part(text=request.text))
    # print("This is the parts sents to LLM::--" + str(parts))

    # for part in parts:
    #     print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    #     print("CHECKING::::: "+ str(part))
    #     if part.text:
    #         # if len(parts) == 1:
    #         #     return part.text
    #         print("FIRST TEXT::::"+str(part.text))
    #     elif (
    #         part.inline_data
    #         and part.inline_data.data
    #         and part.inline_data.mime_type
    #     ):
    #         # base64_string = base64.b64encode(part.inline_data.data).decode("utf-8")
    #         # data_uri = f"data:{part.inline_data.mime_type};base64,{base64_string}"

    #         print("+++++++++++++++++++++++SECOND DATA URI++++++++++++++++++++++++++++")
    #         # print(part.inline_data.data)
    #     else:
    #         print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
      
    # Create and return the Content object
    return types.Content(role="user", parts=parts)

def sanitize_image_id(image_id: str) -> str:
    """Sanitize image ID by removing any leading/trailing whitespace."""
    if image_id.startswith("[IMAGE-"):
        image_id = image_id.split("ID ")[1].split("]")[0]

    return image_id.strip()


def extract_attachment_ids_and_sanitize_response(
    response_text: str,
) -> tuple[str, list[str]]:
    """Extract image hash IDs from JSON code block in the FINAL RESPONSE section.

    Args:
        response_text: The response text from the LLM in markdown format.

    Returns:
        tuple[str, list[str]]: A tuple containing the sanitized response text and list of image hash IDs.
    """
    # JSON code block pattern, looking for ```json { ... } ```
    json_block_pattern = r"```json\s*({[^`]*?})\s*```"
    json_match = re.search(json_block_pattern, response_text, re.DOTALL)

    all_attachments_hash_ids = []
    sanitized_text = response_text

    if json_match:
        json_str = json_match.group(1).strip()
        try:
            # Try to parse the JSON
            json_data = json.loads(json_str)

            # Extract attachment IDs if they exist in the expected format
            if isinstance(json_data, dict) and "attachments" in json_data:
                attachments = json_data["attachments"]
                if isinstance(attachments, list):
                    # Extract image IDs from each attachment string
                    for attachment_id in attachments:
                        all_attachments_hash_ids.append(
                            sanitize_image_id(attachment_id)
                        )

            # Remove the JSON block from the response
            sanitized_text = response_text.replace(json_match.group(0), "")
        except json.JSONDecodeError:
            # If JSON parsing fails, try to extract image IDs directly using regex
            id_pattern = r"\[IMAGE-ID\s+([^\]]+)\]"
            hash_id_matches = re.findall(id_pattern, json_str)
            all_attachments_hash_ids = [
                sanitize_image_id(match.strip())
                for match in hash_id_matches
                if match.strip()
            ]

            # Remove the JSON block from the response
            sanitized_text = response_text.replace(json_match.group(0), "")

    # Clean up the sanitized text
    sanitized_text = sanitized_text.strip()

    return sanitized_text, all_attachments_hash_ids


def extract_thinking_process(response_text: str) -> tuple[str, str]:
    """Extract thinking process from response text and sanitize the response.

    The response expected should e like this

    # THINKING PROCESS
    <thinking process>

    # FINAL RESPONSE
    <final response>

    Args:
        response_text: The response text from the LLM in markdown format.

    Returns:
        tuple[str, str]: A tuple containing the sanitized response text and extracted thinking process.
    """
    # Match until FINAL RESPONSE heading or end
    thinking_pattern = r"#\s*THINKING PROCESS[\s\S]*?(?=#\s*FINAL RESPONSE|\Z)"
    thinking_match = re.search(thinking_pattern, response_text, re.MULTILINE)

    thinking_process = ""

    if thinking_match:
        # Extract the content without the heading
        thinking_content = thinking_match.group(0)
        # Remove the heading and get just the content
        thinking_process = re.sub(
            r"^#\s*THINKING PROCESS\s*", "", thinking_content, flags=re.MULTILINE
        ).strip()

        # Remove the THINKING PROCESS section from the response
        sanitized_text = response_text.replace(thinking_content, "")
    else:
        sanitized_text = response_text

    # Extract just the FINAL RESPONSE section as the sanitized text if it exists
    final_response_pattern = r"#\s*FINAL RESPONSE[\s\S]*?(?=#\s*ATTACHMENTS|\Z)"  # Match until ATTACHMENTS heading or end
    final_response_match = re.search(
        final_response_pattern, sanitized_text, re.MULTILINE
    )

    if final_response_match:
        # Extract the content without the heading
        final_response_content = final_response_match.group(0)
        # Remove the heading and get just the content
        sanitized_text = re.sub(
            r"^#\s*FINAL RESPONSE\s*", "", final_response_content, flags=re.MULTILINE
        ).strip()

    return sanitized_text, thinking_process