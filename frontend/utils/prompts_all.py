TDD_SECTIONS = [
  {
    "section_id": "context_and_acronyms",
    "section_name": "1. Context & Acronyms",
    "prompt": """Generate the 'Context & Acronyms' section.
- Based on the project brief and requirements documents, provide the business context for the data product.
- Create a two-column table listing all relevant acronyms and technical terms used throughout the document and their full definitions.
"""
  },
  {
    "section_id": "objective",
    "section_name": "2. Objective",
    "prompt": """Generate the 'Objective' section.
- Analyze the provided user requirements or questionnaire (e.g., a CSV file) to define the primary goals of the data product.
- Clearly state the business problem this product solves.
- Identify the target audience (e.g., business analysts, data scientists, executive leadership).
- List the key performance indicators (KPIs) or success metrics for the project.
"""
  },
  {
    "section_id": "overview",
    "section_name": "3. Overview",
    "prompt": """Generate the 'Overview' section.
- Provide a high-level, semi-technical summary of the data product's functionality and scope.
- Describe what the data product does, what data it uses, and what outputs it produces.
- Explain the key features and capabilities of the solution.
"""
  },
  {
    "section_id": "high_level_technical_architecture",
    "section_name": "4. High Level Technical Architecture",
    "prompt": """Generate the 'High Level Technical Architecture' section.
- Based on the technical requirements and architecture discussions, create a diagram of the architecture. **Use mermaid.js syntax for the diagram enclosed in ```mermaid and ```**
- **4.1 Components and Services**: List and briefly describe the main cloud services and components (e.g., Cloud Storage, BigQuery, Cloud Functions, Dataflow).
- **4.2 Architecture Flow**: Describe the end-to-end data flow through the architecture, from data ingestion to final presentation or consumption.
"""
  },
  {
    "section_id": "pre-requisites",
    "section_name": "5. Pre-requisites",
    "prompt": """Generate the 'Pre-requisites' section.
- List all technical and non-technical prerequisites for this project.
- Include required software, specific versions of services, necessary access permissions to source systems, and any required underlying infrastructure.
"""
  },
  {
    "section_id": "logical_model",
    "section_name": "6. Logical Model",
    "prompt": """Generate the 'Logical Model' section.
- Based on the source data schemas and business requirements, define the logical data model.
- Describe the key data entities, their attributes, and the relationships between them, independent of the physical database.
- Present this model in a tabular format or as an Entity-Relationship Diagram using **mermaid.js syntax**.
"""
  },
  {
    "section_id": "physical_model",
    "section_name": "7. Physical Model",
    "prompt": """Generate the 'Physical Model' section.
- Based on the logical model and the chosen database technology (e.g., BigQuery, AlloyDB), define the physical data model.
- Specify table names, column names, exact data types (e.g., STRING, INT64, TIMESTAMP), and constraints.
- Detail partitioning and clustering strategies for performance and cost optimization.
"""
  },
  {
    "section_id": "component_specifications",
    "section_name": "8. Component Specifications",
    "prompt": """Generate the 'Component Specifications' section.
- For each component identified in the architecture (e.g., specific ingestion script, Dataflow job, API endpoint), provide a detailed specification.
- Include its purpose, inputs, outputs, processing logic, and key configuration parameters.
"""
  },
  {
    "section_id": "data_processing",
    "section_name": "9. Data Processing",
    "prompt": """Generate the 'Data Processing' (ETL/ELT) section.
- Describe the detailed steps for data ingestion, transformation, and loading.
- Explain the logic for data cleansing, validation, enrichment, and aggregation.
- **9.1 Infrastructure**: Specify the infrastructure that will run these processes (e.g., Dataflow worker configurations, Cloud Function memory/CPU, orchestration tool like Cloud Composer).
"""
  },
  {
    "section_id": "data_management",
    "section_name": "10. Data Management",
    "prompt": """Generate the 'Data Management' section.
- **10.1 Metadata Management**: Describe the strategy for managing metadata. How will table schemas, column descriptions, and data quality metrics be captured and made accessible (e.g., using Data Catalog)?
- **10.2 Data Lineage**: Explain how data lineage will be tracked from source to destination. If using a tool, specify it and describe the implementation.
"""
  },
  {
    "section_id": "security_considerations",
    "section_name": "11. Security Considerations",
    "prompt": """Generate the 'Security Considerations' section.
- Based on corporate security policies and data classification, detail the security measures.
- **11.1 Data Anonymization**: Describe the techniques for anonymizing or pseudonymizing Personally Identifiable Information (PII) or sensitive data (e.g., using Cloud DLP).
- **11.2 Access Control**: Explain the access control model. Who can access the data and for what purpose?
- **11.3 IAM Roles & Permissions**: Specify the exact IAM roles (predefined or custom) and permissions required for service accounts and user groups.
- **11.4 Data Controls**: Detail controls for data residency, encryption (at-rest and in-transit), and auditing.
"""
  },
  {
    "section_id": "testing_and_validation",
    "section_name": "12. Testing & Validation",
    "prompt": """Generate the 'Testing & Validation' section.
- Outline the overall testing strategy, including unit, integration, and user acceptance testing (UAT).
- **12.1 Test Data Generation**: Describe the process for generating or sourcing realistic test data.
- **12.2 Test Design**: Provide examples of test cases, including data quality checks, business logic validation, and performance testing.
"""
  },
  {
    "section_id": "error_handling_logging_monitoring",
    "section_name": "13. Error Handling, Logging & Monitoring",
    "prompt": """Generate the 'Error Handling, Logging & Monitoring' section.
- Describe the strategy for error handling within the data pipelines. How will failures be retried or escalated?
- Specify what information will be logged at each stage of the process (e.g., using Cloud Logging).
- Detail the monitoring plan, including key metrics to track (e.g., data volume, processing latency, error rates) and the dashboards and alerting mechanisms to be used (e.g., using Cloud Monitoring).
"""
  },
  {
    "section_id": "deployment",
    "section_name": "14. Deployment",
    "prompt": """Generate the 'Deployment' section.
- Describe the CI/CD (Continuous Integration/Continuous Deployment) strategy for this data product.
- Detail the process for promoting code and infrastructure changes through different environments (dev, test, prod).
- Specify the tools to be used (e.g., Terraform, Cloud Build, GitHub Actions).
- Outline the rollback plan in case of a failed deployment.
"""
  },
  {
    "section_id": "technical_debt",
    "section_name": "15. Technical Debt",
    "prompt": """Generate the 'Technical Debt' section.
- Identify any known design compromises, workarounds, or shortcuts taken to meet deadlines.
- For each item of technical debt, explain the potential risks or future work required.
- Propose a plan or timeline for addressing and resolving the identified technical debt.
"""
  }
]