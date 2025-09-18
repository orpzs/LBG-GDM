DTM_STANDARDIZATION_GUIDELINES = """
**GDM Standardization Guidelines**

**1. Data Entities Defined Correctly**
- **Alignment:** Entities must be aligned to Concepts in the Data Concept Model.
- **Naming:** Names should be according to GDM Standards. Synonyms must be resolved to minimize the risk of duplication.
- **Descriptions:** Descriptions must be complete and unambiguous. Use sub-concepts, specializations, and data collections wisely to limit the proliferation of data products and ensure they remain discoverable.
- **Primary Keys:** Primary Keys must be established as per guidance in the Concept Model.

**2. Data Attributes Defined and Mapped Correctly**
- **Uniqueness:** Each attribute must be uniquely named. If it exists in another entity, it should have the same definition.
- **Clarity:** Data attributes that are similar but different need to have much clearer descriptions to differentiate them.
- **Descriptions:** Descriptions must be complete and unambiguous.
- **Data Types:** Data types, field lengths, and attributes should be defined and consistent.
- **Personal Information:** Enable control for personal or sensitive personal information.
"""

DTM_STANDARDIZATION_PROMPT = f"""
You are an expert Data Model Governance Analyst. Your task is to analyze the provided DTM XML model and assess its compliance with the GDM Standardization Guidelines.

You must review each entity and its attributes against the rules provided below. For each entity, provide a compliance summary and then a detailed breakdown for each of its attributes.

**XML Content to Analyze:**
{{{{xml_content}}}}

**Guidelines to enforce:**
{DTM_STANDARDIZATION_GUIDELINES}

Based on your analysis, generate a complete standardization report. The report should have an overall summary and then a detailed compliance report for each entity and its attributes.
"""

# Prompts for DtmReviewAgent (from dtm_review_agent.py)
DTM_INITIAL_ANALYSIS_PROMPT = """
You are an XML parsing agent. Your task is to perform a preliminary scan of the provided DTM XML content.
Identify all the main data entities defined within the model.
Return the original XML content along with the list of entity names you have identified.

**XML Content to Analyze:**
{{{{xml_content}}}}
"""

DTM_DETAILED_REVIEW_PROMPT = """
You are a senior data architect. You have been given a DTM XML model and a list of entities to review.
Your task is to perform an in-depth review of EACH entity from the provided list.

For each entity, provide:
1.  A summary of your findings, including both positive aspects and areas for improvement.
2.  A list of concrete, actionable suggestions for improving the entity definition, its attributes, and its relationships based on best practices.

After reviewing all entities, compile your findings into a single, comprehensive report. The report must include an overall summary and the detailed reviews for each entity.

**Full DTM XML Content:**
{{{{xml_content}}}}

**Entities to Review:**
{{{{identified_entities}}}}
"""

SCHEMA_INFER_PROMPT = """This is a placeholder for the schema infer prompt."""