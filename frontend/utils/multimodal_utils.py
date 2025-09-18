# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# pylint: disable=W0718

import base64
import mimetypes
import os
import tempfile
from typing import Any, Dict, List, Optional, Union
from urllib.parse import quote

import docx2txt
import fpdf
from google.cloud import storage

from vertexai.preview.generative_models import Content, Part

from utils.schema import ImageData, fileUriData, inlineData
from utils.content_utils import get_full_content_text, get_full_parts_text

# HELP_MESSAGE_MULTIMODALITY = (
#     "For Gemini models to access the URIs you provide, store them in "
#     "Google Cloud Storage buckets within the same project used by Gemini."
# )

# HELP_GCS_CHECKBOX = (
#     "Enabling GCS upload will increase the app observability by avoiding"
#     " forwarding and logging large byte strings within the app."
# )


# def format_content(content: List[Part]) -> str:
#     """Formats content as a string, handling both text and multimedia inputs."""
#     if isinstance(content, Content):
#         return get_full_content_text(content)
#     if len(content) > 0 and isinstance(content[0], Part):
#         return get_full_parts_text(content)
#     if isinstance(content, str):
#         return content
#     raise ValueError("Unexpected content type)}")


# def get_gcs_blob_mime_type(gcs_uri: str) -> Optional[str]:
#     """Fetches the MIME type (content type) of a Google Cloud Storage blob.

#     Args:
#         gcs_uri (str): The GCS URI of the blob in the format "gs://bucket-name/object-name".

#     Returns:
#         str: The MIME type of the blob (e.g., "image/jpeg", "text/plain") if found,
#              or None if the blob does not exist or an error occurs.
#     """
#     storage_client = storage.Client()

#     try:
#         bucket_name, object_name = gcs_uri.replace("gs://", "").split("/", 1)

#         bucket = storage_client.bucket(bucket_name)
#         blob = bucket.blob(object_name)
#         blob.reload()
#         return blob.content_type
#     except Exception as e:
#         print(f"Error retrieving MIME type for {gcs_uri}: {e}")
#         return None  # Indicate failure


# def get_parts_from_files(
#     upload_gcs_checkbox: bool, uploaded_files: List[Any], gcs_uris: str
# ) -> List[Dict[str, Any]]:
#     """Processes uploaded files and GCS URIs to create a list of content parts."""
#     parts = []
#     # read from local directly
#     if not upload_gcs_checkbox:
#         for uploaded_file in uploaded_files:
#             im_bytes = uploaded_file.read()
#             if "image" in uploaded_file.type:
#                 content = {
#                     "type": "image_url",
#                     "image_url": {
#                         "url": f"data:{uploaded_file.type};base64,"
#                         f"{base64.b64encode(im_bytes).decode('utf-8')}"
#                     },
#                     "file_name": uploaded_file.name,
#                 }
#             else:
#                 content = {
#                     "type": "media",
#                     "data": base64.b64encode(im_bytes).decode("utf-8"),
#                     "file_name": uploaded_file.name,
#                     "mime_type": uploaded_file.type,
#                 }

#             parts.append(content)
#     if gcs_uris != "":
#         for uri in gcs_uris.split(","):
#             content = {
#                 "type": "media",
#                 "file_uri": uri,
#                 "mime_type": get_gcs_blob_mime_type(uri),
#             }
#             parts.append(content)
#     return parts


def upload_bytes_to_gcs(
    bucket_name: str,
    blob_name: str,
    file_bytes: bytes,
    content_type: Optional[str] = None,
) -> str:
    """Uploads a bytes object to Google Cloud Storage and returns the GCS URI.

    Args:
        bucket_name: The name of the GCS bucket.
        blob_name: The desired name for the uploaded file in GCS.
        file_bytes: The file's content as a bytes object.
        content_type (optional): The MIME type of the file (e.g., "image/png").
            If not provided, GCS will try to infer it.

    Returns:
        str: The GCS URI (gs://bucket_name/blob_name) of the uploaded file.

    Raises:
        GoogleCloudError: If there's an issue with the GCS operation.
    """
    storage_client = storage.Client(project="r2d2-00")
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data=file_bytes, content_type=content_type)
    # Construct and return the GCS URI
    gcs_uri = f"gs://{bucket_name}/{blob_name}"
    return gcs_uri


# def gs_uri_to_https_url(gs_uri: str) -> str:
#     """Converts a GS URI to an HTTPS URL without authentication.

#     Args:
#         gs_uri: The GS URI in the format gs://<bucket>/<object>.

#     Returns:
#         The corresponding HTTPS URL, or None if the GS URI is invalid.
#     """

#     if not gs_uri.startswith("gs://"):
#         raise ValueError("Invalid GS URI format")

#     gs_uri = gs_uri[5:]

#     # Extract bucket and object names, then URL encode the object name
#     bucket_name, object_name = gs_uri.split("/", 1)
#     object_name = quote(object_name)

#     # Construct the HTTPS URL
#     https_url = f"https://storage.mtls.cloud.google.com/{bucket_name}/{object_name}"
#     return https_url


# def upload_files_to_gcs(st: Any, bucket_name: str, files_to_upload: List[Any]) -> None:
#     """Upload multiple files to Google Cloud Storage and store URIs in session state."""
#     bucket_name = bucket_name.replace("gs://", "")
#     uploaded_uris = []
#     for file in files_to_upload:
#         if file:
#             file_bytes = file.read()
#             gcs_uri = upload_bytes_to_gcs(
#                 bucket_name=bucket_name,
#                 blob_name=file.name,
#                 file_bytes=file_bytes,
#                 content_type=file.type,
#             )
#             uploaded_uris.append(gcs_uri)
#     st.session_state.uploader_key += 1
#     st.session_state["gcs_uris_to_be_sent"] = ",".join(uploaded_uris)
#     print(st.session_state["gcs_uris_to_be_sent"])
#     print(st.session_state.uploader_key)

def convert_docx_to_pdf_bytes(docx_bytes: bytes) -> bytes:
    try:
        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Write docx bytes to a temporary file
            docx_file_path = os.path.join(temp_dir, "temp.docx")
            with open(docx_file_path, "wb") as f:
                f.write(docx_bytes)

            # Extract text from docx using docx2txt (using file path)
            extracted_text = docx2txt.process(docx_file_path)

            # Create a PDF using FPDF
            pdf = fpdf.FPDF()
            pdf.add_page()
            pdf.set_font("Arial", size=12)  # Reduced size for more content
            pdf.set_auto_page_break(auto=True, margin=15)
            # Split the text into lines and add each line to the PDF
            for line in extracted_text.splitlines():
                pdf.cell(190, 5, txt=line.encode('utf-8').decode('latin-1', 'ignore'), ln=1, align='L')  # Adjusted Cell size and alignment

            # Get the PDF bytes
            pdf_bytes = pdf.output(dest='S').encode('latin-1')
            return pdf_bytes

    except Exception as e:
        print(f"Error during conversion: {e}")
        return b""

     
# def convert_docx_to_pdf_bytes(docx_bytes: bytes) -> bytes:
#     """Converts docx file bytes to pdf file bytes using pypandoc, handling Unicode characters and image conversion issues."""
#     try:
#         # Create temporary files
#         with tempfile.NamedTemporaryFile(suffix=".docx", delete=False) as temp_docx:
#             temp_docx.write(docx_bytes)
#             temp_docx_path = temp_docx.name


#         with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as temp_pdf:
#             temp_pdf_path = temp_pdf.name

#         # Conversion options.  Try several strategies.
#         extra_args = ['--pdf-engine=xelatex']  # Essential for Unicode and some image formats.
#         # Option 1: Try to embed images as PNG (often more compatible)
#         # extra_args.extend(['--extract-media', '.', '--embed-resources'])  #Requires directory for files (see note)

#         # Option 2: Try to convert EMF to SVG. Requires inkscape installed
#         # extra_args.extend(['--extract-media', '.', '--embed-resources'])
#         #extra_args.extend(['--filter', 'pandoc-svg'])

#         # Option 3:  Try to ignore image errors (least preferred, can result in loss of images)
#         # extra_args.append('--wrap=preserve') #Needed if ignoring image read errors

#         try:
#             pypandoc.convert_file(
#                 source_file=temp_docx_path,
#                 to='pdf',
#                 format='docx',
#                 outputfile=temp_pdf_path,
#                 extra_args=extra_args,
#             )

#         except Exception as conversion_error:  # Nested try/except to handle specific conversion problems.
#             st.warning(f"Initial PDF conversion failed.  Trying fallback strategies. Error: {conversion_error}")
#             #Option 3 Attempt: try ignoring errors
#             extra_args = ['--pdf-engine=xelatex', '--wrap=preserve', '--ignore-errors']
#             try:
#               pypandoc.convert_file(
#                 source_file=temp_docx_path,
#                 to='pdf',
#                 format='docx',
#                 outputfile=temp_pdf_path,
#                 extra_args=extra_args,
#             )
#             except Exception as final_error:
#                st.error(f"PDF conversion failed after multiple attempts.  Please check the DOCX for image errors. LaTeX may be unable to load images correctly: {final_error}")
#                os.remove(temp_docx_path)
#                os.remove(temp_pdf_path)
#                return None


#         # Read PDF data
#         with open(temp_pdf_path, "rb") as temp_pdf_file:
#             pdf_bytes = temp_pdf_file.read()

#         # Clean up
#         os.remove(temp_docx_path)
#         os.remove(temp_pdf_path)

#         return pdf_bytes

#     except Exception as e:
#         st.error(f"General error during DOCX to PDF conversion: {e}")
#         return None


def encode_image_to_base64_and_get_mime_type(image_path: str) -> ImageData:

    # Read the image file
    with open(image_path, "rb") as file:
        image_content = file.read()

    # Get the mime type
    mime_type = mimetypes.guess_type(image_path)[0]

    # Base64 encode the image
    base64_data = base64.b64encode(image_content).decode("utf-8")

    # Return as ImageData object
    return ImageData(serialized_image=base64_data, mime_type=mime_type)

def encode_data(file_bytes,file_type) -> inlineData:

    base64_data = base64.b64encode(file_bytes).decode("utf-8")

    return inlineData(serialized_data=base64_data, mime_type=file_type)

def upload_files_to_gcs(st: Any, bucket_name: str, file_bytes,file_type,file_name) -> None:
    """Upload multiple files to Google Cloud Storage and store URIs in session state."""
    bucket_name = bucket_name.replace("gs://", "")
    uploaded_uris = []

    gcs_uri = upload_bytes_to_gcs(
                bucket_name=bucket_name,
                blob_name=file_name,
                file_bytes=file_bytes,
                content_type=file_type,
            )
    
    st.session_state.uploader_key += 1
    st.session_state["gcs_uris_to_be_sent"] = gcs_uri
    print(st.session_state["gcs_uris_to_be_sent"])
    print(st.session_state.uploader_key)

    return fileUriData(mimeType=file_type, fileUri=gcs_uri)