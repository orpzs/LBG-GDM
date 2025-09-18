from google.adk.agents import LlmAgent
from pydantic import BaseModel, Field
from typing import List

from agents.shared_libraries.utils import proxyModel
from agents.shared_libraries import prompt_master
import logging

logging.basicConfig(level=logging.DEBUG)


# --- Pydantic Models for the Standardization Report ---

class AttributeCompliance(BaseModel):
    """A compliance report for a single data attribute."""
    attribute_name: str = Field(description="Name of the data attribute.")
    is_compliant: bool = Field(description="Whether the attribute meets standardization guidelines.")
    findings: str = Field(description="Detailed findings regarding the attribute's compliance (e.g., naming, description, data type).")

class EntityCompliance(BaseModel):
    """A compliance report for a single data entity."""
    entity_name: str = Field(description="Name of the data entity.")
    is_compliant: bool = Field(description="Whether the entity as a whole meets standardization guidelines.")
    findings: str = Field(description="Detailed findings regarding the entity's compliance (e.g., naming, description, primary key).")
    attributes: List[AttributeCompliance] = Field(description="Compliance report for each attribute within the entity.")

class StandardizationReport(BaseModel):
    """A report on the DTM XML's compliance with GDM standardization guidelines."""
    report_summary: str = Field(description="An overall summary of the model's compliance with standardization guidelines.")
    entities: List[EntityCompliance] = Field(description="A list of compliance reports for each entity in the model.")


# --- Agent Definition ---

StandardizingAgent = LlmAgent(
    model=proxyModel,
    name="standardizing_agent",
    description="Analyzes a DTM XML model to check its compliance against GDM standardization guidelines.",
    instruction=prompt_master.DTM_STANDARDIZATION_PROMPT,
    output_schema=StandardizationReport,
)