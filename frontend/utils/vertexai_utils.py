import streamlit as st
import vertexai
from vertexai.language_models import TextEmbeddingModel, TextEmbeddingInput

# Initialize Vertex AI (replace with your project and location)
# try:
#     vertexai.init(project="YOUR_PROJECT_ID", location="us-central1")
# except Exception as e:
#     st.warning(f"Vertex AI not initialized: {e}. Embeddings may fail.")

@st.cache_data
def generate_embedding(text: str, task_type: str = "RETRIEVAL_DOCUMENT") -> list[float]:
    """Generates an embedding for a given text using Vertex AI."""
    if not text:
        return []
    try:
        # Use the recommended model
        model = TextEmbeddingModel.from_pretrained("gemini-embedding-001")

        # Prepare the input with a specified task type
        text_input = TextEmbeddingInput(text=text, task_type=task_type)

        # gemini-embedding-001 processes one input at a time
        embeddings = model.get_embeddings([text_input])

        if embeddings:
            return embeddings[0].values
        return []
    except Exception as e:
        st.error(f"Could not generate embedding: {e}")
        return []

# Alias for backward compatibility if other files use `generate_embeddings`
generate_embeddings = generate_embedding