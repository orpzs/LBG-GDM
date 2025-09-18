from functools import cache
from google.adk.agents import Agent
from google.adk.models.lite_llm import LiteLlm
from config.settings import Settings
from typing import Dict, Optional, Any
import google.oauth2.id_token
from google import genai
from vertexai.preview import rag
import logging
logging.basicConfig(level=logging.DEBUG)

settings = Settings().get_settings()

if settings.RUN_AGENT_WITH_DEBUG:
    import litellm

    litellm._turn_on_debug()

def query_rag_corpus(
    corpus_id: str,
    query_text: str,
    top_k: Optional[int] = None,
    vector_distance_threshold: Optional[float] = None
) -> Dict[str, Any]:
    """
    Directly queries a RAG corpus using the Vertex AI RAG API.
    
    Args:
        corpus_id: The ID of the corpus to query
        query_text: The search query text
        top_k: Maximum number of results to return (default: 10)
        vector_distance_threshold: Threshold for vector similarity (default: 0.5)
        
    Returns:
        A dictionary containing the query results
    """
    if top_k is None:
        top_k = settings.RAG_DEFAULT_TOP_K
    if vector_distance_threshold is None:
        vector_distance_threshold = settings.RAG_DEFAULT_VECTOR_DISTANCE_THRESHOLD
    try:
        # Construct full corpus resource path
        corpus_path = f"projects/r2d2-00/locations/us-central1/ragCorpora/{corpus_id}"
        
        # Create the resource config
        rag_resource = rag.RagResource(rag_corpus=corpus_path)
        
        # Configure retrieval parameters
        retrieval_config = rag.RagRetrievalConfig(
            top_k=top_k,
            filter=rag.utils.resources.Filter(vector_distance_threshold=vector_distance_threshold)
        )
        
        # Execute the query directly using the API
        response = rag.retrieval_query(
            rag_resources=[rag_resource],
            text=query_text,
            rag_retrieval_config=retrieval_config
        )
        
        # Process the results
        results = []
        if hasattr(response, "contexts"):
            # Handle different response structures
            contexts = response.contexts
            if hasattr(contexts, "contexts"):
                contexts = contexts.contexts
            
            # Extract text and metadata from each context
            for context in contexts:
                result = {
                    "text": context.text if hasattr(context, "text") else "",
                    "source_uri": context.source_uri if hasattr(context, "source_uri") else None,
                    "relevance_score": context.relevance_score if hasattr(context, "relevance_score") else None
                }
                results.append(result)
        
        return {
            "status": "success",
            "corpus_id": corpus_id,
            "results": results,
            "count": len(results),
            "query": query_text,
            "message": f"Found {len(results)} results for query: '{query_text}'"
        }
        
    except Exception as e:
        return {
            "status": "error",
            "corpus_id": corpus_id,
            "error_message": str(e),
            "message": f"Failed to query corpus: {str(e)}"
        }