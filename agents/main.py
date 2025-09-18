
from agents.dtm_review_agent.agent import root_agent as dtm_review_agent

from google.adk.sessions import InMemorySessionService, DatabaseSessionService
from google.adk.runners import Runner
from google.adk.events import Event
from fastapi import FastAPI, Body, Depends, HTTPException
from dotenv import load_dotenv
from typing import Any, AsyncIterator, Optional
from types import SimpleNamespace
import uvicorn
from contextlib import asynccontextmanager
from agents.shared_libraries.schema import ChatRequest, ChatResponse
from google.adk.artifacts import GcsArtifactService
from config.settings import Settings
from fastapi.middleware.cors import CORSMiddleware
from google.adk.sessions import Session
from starlette.types import Lifespan
from agents.shared_libraries.utils import (
    extract_attachment_ids_and_sanitize_response,
    extract_thinking_process,
)
import logging

load_dotenv()
logging.basicConfig(level=logging.DEBUG)

class AppContexts(SimpleNamespace):
    """A class to hold application contexts with attribute access"""

    session_service: InMemorySessionService = None
    artifact_service: GcsArtifactService = None
    dtm_review_agent_runner: Runner = None

app_contexts = AppContexts()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handles application startup and shutdown events."""
    try:
        config = Settings.get_settings()
    except Exception as e:
        print(f"Error loading configuration: {e}")
        raise  # Re-raise to prevent app from starting

    # Initialize service contexts during application startup
    app_contexts.session_service =  DatabaseSessionService(db_url=config.SESSION_DB_URL)
    app_contexts.artifact_service = GcsArtifactService(
        bucket_name=config.ARTIFACT_GCS_BUCKET
    )

    app_contexts.dtm_review_agent_runner = Runner(
        agent=dtm_review_agent,
        app_name="dtm_review_app",
        session_service=app_contexts.session_service,
        artifact_service=app_contexts.artifact_service,
    )

    yield
    #Cleanup operations can go here.

async def get_app_contexts() -> AppContexts:
    return app_contexts

app = FastAPI(title="TDD Assistant API", lifespan=lifespan) #Correct FastAPI App
# # # Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post(
    "/apps/{app_name}/users/{user_id}/sessions/{session_id}",
    response_model_exclude_none=True,
)
async def create_session_with_id(
    app_name: str,
    user_id: str,
    session_id: str,
    state: Optional[dict[str, Any]] = None,
) -> None:

    if (
        await app_contexts.session_service.get_session(
            app_name=app_name, user_id=user_id, session_id=session_id
        )
        is not None
    ):
        print("Session already exists: %s", session_id)
        raise HTTPException(
            status_code=400, detail=f"Session already exists: {session_id}"
        )
    print("New session created: %s", session_id)
    return await app_contexts.session_service.create_session(
        app_name=app_name, user_id=user_id, state=state, session_id=session_id
    )
@app.get(
    "/apps/{app_name}/users/{user_id}/sessions/{session_id}",
    response_model_exclude_none=True,
)
async def get_session(
    app_name: str, user_id: str, session_id: str
) -> Session:
    # Connect to managed session if agent_engine_id is set.

    session = await app_contexts.session_service.get_session(
        app_name=app_name, user_id=user_id, session_id=session_id
    )
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return session

@app.get(
    "/apps/{app_name}/users/{user_id}/sessions",
    response_model_exclude_none=True,
)
async def list_sessions(app_name: str, user_id: str) -> list[Session]:
    # Connect to managed session if agent_engine_id is set.
    list_sessions_response = await app_contexts.session_service.list_sessions(
        app_name=app_name, user_id=user_id
    )
    return [
        session
        for session in list_sessions_response.sessions
        # Remove sessions that were generated as a part of Eval.
        if not session.id.startswith("EVAL_SESSION_ID_PREFIX")
    ]

@app.post("/dtm_review", response_model=ChatResponse)
async def dtm_review(
    request: ChatRequest = Body(...),
    app_context: AppContexts = Depends(get_app_contexts),
) -> ChatResponse:
    """Process DTM review request and get response from the agent"""
    final_response_text = "Agent did not produce a final response."
    session_id = request.session_id
    user_id = request.user_id

    if not request.text:
        raise HTTPException(status_code=400, detail="XML content is missing.")

    try:
        events_iterator: AsyncIterator[Event] = (
            app_context.dtm_review_agent_runner.run_async(
                user_id=user_id,
                session_id=session_id,
                new_message={"xml_content": request.text},
            )
        )
        async for event in events_iterator:
            if event.is_final_response():
                if event.content and event.content.parts:
                    final_response_text = event.content.parts[0].text
                elif event.actions and event.actions.escalate:
                    final_response_text = (
                        f"Agent escalated: {event.error_message or 'No specific message.'}"
                    )

        sanitized_text, _ = extract_attachment_ids_and_sanitize_response(
            final_response_text
        )
        sanitized_text, thinking_process = extract_thinking_process(sanitized_text)

        return ChatResponse(
            response=sanitized_text,
            thinking_process=thinking_process,
        )

    except Exception as e:
        logging.error("Error processing DTM review request: %s", e, exc_info=True)
        return ChatResponse(
            response="", error=f"Error in generating DTM review: {str(e)}"
        )

@app.get("/hello")
async def read_root():
    return {"Hello": "World"}



# Only run the server if this file is executed directly
if __name__ == "__main__":
    import os
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))





# import os
# import uvicorn
# from fastapi import FastAPI
# from google.adk.cli.fast_api import get_fast_api_app
# from agents.config_load import GetConf
# from fastapi.middleware.cors import CORSMiddleware



# config = GetConf.load_configs()

# AGENT_DIR = os.path.dirname(os.path.abspath(__file__))




# # app = get_fast_api_app(
# #     agent_dir=AGENT_DIR,
# #     session_db_url=config.SESSION_DB_URL,
# #     artifact_storage_uri = f"gs://{config.ARTIFACT_GCS_BUCKET}",
# #     allow_origins= ["*"],
# #     web=config.SERVE_WEB_INTERFACE,
    
# # )

# app = get_fast_api_app(
#     agent_dir=AGENT_DIR,
#     session_db_url=config.SESSION_DB_URL,
#     allow_origins= ["*"],
#     web=config.SERVE_WEB_INTERFACE,
    
# )
# # # Add CORS middleware
# # app.add_middleware(
# #     CORSMiddleware,
# #     allow_origins=["*"],  # In production, replace with frontend URL
# #     allow_credentials=True,
# #     allow_methods=["*"],
# #     allow_headers=["*"],
# # )

# @app.get("/hello")
# async def read_root():
#     return {"Hello": "World"}


# if __name__ == "__main__":
#     uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
