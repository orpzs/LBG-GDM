TDD_SECTIONS = [
  {
    "section_id": "high_level_technical_architecture",
    "section_name": "4. High Level Technical Architecture",
    "prompt": """Generate the 'High Level Technical Architecture' section.
- Based on the technical requirements and architecture discussions, create a diagram of the architecture. **Use mermaid.js syntax for the diagram enclosed in ```mermaid amd ```**
- **4.1 Components and Services**: List and briefly describe the main cloud services and components (e.g., Cloud Storage, BigQuery, Cloud Functions, Dataflow).
- **4.2 Architecture Flow**: Describe the end-to-end data flow through the architecture, from data ingestion to final presentation or consumption.
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
  }
]