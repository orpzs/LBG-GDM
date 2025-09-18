#!/bin/bash

export PROJECT_ID=r2d2-00
export REGION=us-central1
export SERVICE_ACCOUNT=

# -------------------- Notebooks --------------------
# GCS URIs
export RAW_DATA_BUCKET_URI=
export DOCUMENTS_BUCKET_URI=
export EMBEDDINGS_BUCKET_URI=

# Vertex AI Vector Search IDs
export VECTORSEARCH_INDEX_NAME=rfc_rag_index_small
export VECTORSEARCH_INDEX_ENDPOINT_NAME=rfc_rag_endpoint
export VECTORSEARCH_INDEX_ENDPOINT_DEPLOYED_NAME=rfc_rag_endpoint_deployed 
# Deployed Index ID should start with a letter 
# and contain only letters, numbers and underscores

# LLMs
export EMBEDDINGS_LLM=text-embedding-005
export INFERENCE_LLM=gemini-2.5-pro

# -------------------- Load testing --------------------
export PYTHON_INDEX_URL=
export LOAD_TEST_BASE_IMAGE_URI=
export LOAD_TEST_IMAGE_URI=
export LOAD_TEST_SERVICE_ACCOUNT=
export LOAD_TEST_NETWORK=
export LOAD_TEST_SUBNET=
export LOAD_TEST_BUCKET=
export LOAD_TEST_INPUT="{'prompt': 'Give me a recipe for banana bread'}"
# locust conf
export LOCUST_LOCUSTFILE=locustfile.py
export LOCUST_HOST=https://aib-dummy-use-case-app-mchy4gbw4a-ew.a.run.app
export LOCUST_CSV=test_results
export LOCUST_HTML=test_results.html
export LOCUST_HEADLESS=True
export LOCUST_USERS=5
export LOCUST_SPAWN_RATE=1
export LOCUST_EXPECT_WORKERS=5
export LOCUST_RUN_TIME=60

# -------------------- Ingestion pipeline --------------------
export PIPELINE_DISPLAY_NAME=rfc-ingestion-pipeline
export PIPELINE_PACKAGE=ingestion_workflow/payload/rfc-ingestion-pipeline.yaml
export PIPELINE_ROOT=
export PIPELINE_NETWORK=
export COMPUTE_SA=313635801388-compute@developer.gserviceaccount.com
export TRIGGER_BUCKET=${PROJECT_ID}-trigger
export VPC_CONNECTOR=cf-vpc-connector
export WORKER_POOL=

export CONFIG_PATH=config.yaml

export RAG_DEFAULT_EMBEDDING_MODEL=text-embedding-004
export RAG_DEFAULT_TOP_K=10  
export RAG_DEFAULT_SEARCH_TOP_K=5
export RAG_DEFAULT_VECTOR_DISTANCE_THRESHOLD=0.5

export GOOGLE_GENAI_USE_VERTEXAI=1
export GOOGLE_CLOUD_PROJECT=r2d2-00
export GOOGLE_CLOUD_LOCATION=us-central1
