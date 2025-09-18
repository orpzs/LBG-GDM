from google.adk.agents import LlmAgent, SequentialAgent
from google.adk.agents import Agent
from google.adk.models.lite_llm import LiteLlm
from pydantic import BaseModel, Field
from typing import List
from config.settings import Settings
from google.adk.tools import agent_tool
from google.cloud import storage
from google.adk.artifacts import GcsArtifactService
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.adk.models import google_llm
from agents.shared_libraries import prompt_master
from agents.shared_libraries.rag_tools import *
from google.adk.auth import AuthCredential, AuthCredentialTypes
from google.adk.auth.auth_credential import HttpAuth, HttpCredentials
from google.adk.planners import BuiltInPlanner
from google.oauth2 import credentials
import os
from agents.shared_libraries.utils import proxyModel, customAFC
from agents.sub_agents.standardizing_agent import StandardizationReport
from google.adk.tools.agent_tool import AgentTool
from google.genai import types
import logging
logging.basicConfig(level=logging.DEBUG)



# Set up logging as in the example
logging.basicConfig(level=logging.DEBUG)


# --- Pydantic Models for the Final Report ---

class EntityReview(BaseModel): # This is part of the general review
    """A review for a single entity in the DTM."""
    entity_name: str = Field(description="Name of the entity being reviewed from the DTM XML.")
    review_summary: str = Field(description="A summary of the review findings for the entity, including positives and negatives.")
    suggestions: List[str] = Field(description="A list of concrete suggestions for improving the entity.")

class DtmReviewReport(BaseModel):
    """A complete review report for a DTM XML model."""
    report_title: str = Field(description="Title for the DTM review report, e.g., 'DTM Model Review Report'.")
    overall_summary: str = Field(description="An overall summary of the review of all entities in the model, highlighting key findings.")
    entity_reviews: List[EntityReview] = Field(description="A list of detailed reviews for each entity found in the model.")


# --- Pydantic Models for Agent Sequence Flow ---

class InitialAnalysisOutput(BaseModel):
    """Output of the initial analysis agent."""
    xml_content: str = Field(description="The original DTM XML content that was analyzed.")
    identified_entities: List[str] = Field(description="A list of names of the main entities identified within the XML.")

class StandardizationStepOutput(BaseModel):
    """Output of the standardization step, passing through data for the next step."""
    xml_content: str = Field(description="The original DTM XML content, passed through from the previous step.")
    identified_entities: List[str] = Field(description="List of entity names, passed through from the previous step.")
    standardization_report: StandardizationReport = Field(description="The generated GDM standardization compliance report.")

class FinalCombinedReport(BaseModel):
    """The final combined report containing both general and standardization reviews."""
    general_review: DtmReviewReport = Field(description="The detailed general best-practices review of the DTM model.")
    standardization_report: StandardizationReport = Field(description="The detailed GDM standardization compliance report.")


# --- Prompts for New Agents ---
# These are defined here for clarity, but would ideally be in prompt_master.

STANDARDIZATION_WRAPPER_PROMPT = f"""
You are an expert Data Model Governance Analyst. Your first task is to analyze the provided DTM XML model and assess its compliance with the GDM Standardization Guidelines.
Your second task is to pass through the original XML content and the list of identified entities for the next step in the process.

**XML Content to Analyze:**
{{{{xml_content}}}}

**Guidelines to enforce:**
{prompt_master.DTM_STANDARDIZATION_GUIDELINES}

Based on your analysis, generate a standardization report. Then, structure your entire output to include that report along with the original `xml_content` and `identified_entities`.
"""

FINAL_REPORT_PROMPT = """
You are a senior data architect acting as the final step in an analysis pipeline. You have been given a DTM XML model, a list of entities to review, and a pre-generated GDM standardization report.

Your tasks are:
1.  Perform an in-depth general review of EACH entity from the provided list, based on architectural best practices. For each entity, provide a summary and actionable suggestions.
2.  Compile your findings into a general review report with an overall summary.
3.  Combine your general review report with the provided standardization report into a single, final output.

**Full DTM XML Content:**
{{{{xml_content}}}}

**Entities to Review:**
{{{{identified_entities}}}}

**Pre-generated Standardization Report:**
{{{{standardization_report}}}}

Produce the final combined report.
"""

# --- Sub-Agent Definitions ---

# Sub-agent 1: Identifies entities and passes along the XML
InitialAnalysisAgent = LlmAgent(
    model=proxyModel,
    name="initial_analysis_agent",
    description="Analyzes DTM XML to identify main entities and prepares for detailed review.",
    instruction=prompt_master.DTM_INITIAL_ANALYSIS_PROMPT,
    output_schema=InitialAnalysisOutput,
)

# Sub-agent 2: Performs standardization and passes data through
StandardizationWrapperAgent = LlmAgent(
    model=proxyModel,
    name="standardization_wrapper_agent",
    description="Performs GDM standardization check and passes all data to the final reporting step.",
    instruction=STANDARDIZATION_WRAPPER_PROMPT,
    input_schema=DtmReviewInput,
    output_schema=StandardizationStepOutput,
)

# Sub-agent 3: Performs detailed review and combines with standardization report
FinalReportAgent = LlmAgent(
    model=proxyModel,
    name="final_report_agent",
    description="Performs a detailed review of each DTM entity and generates a final combined report.",
    instruction=FINAL_REPORT_PROMPT,
    output_schema=FinalCombinedReport,
)


# --- Main DTM Review Sequential Agent ---

DtmReviewAgent = SequentialAgent(
    name="dtm_review_agent",
    description="A sequential agent that performs a comprehensive review of a DTM XML model, including a GDM standardization check and a general best-practices review.",
    sub_agents=[
        StandardizationWrapperAgent,
        FinalReportAgent,
    ],
)

root_agent = DtmReviewAgent

config = Settings.get_settings()

gcs_service_py = GcsArtifactService(
    bucket_name=config.ARTIFACT_GCS_BUCKET
)

runner = Runner(
    agent=root_agent,
    app_name="dtm_review_app",
    session_service=InMemorySessionService(),
    artifact_service=gcs_service_py
)    