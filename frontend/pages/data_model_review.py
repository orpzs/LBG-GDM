import streamlit as st
import json
from utils.api_utils import generate_dtm_review

st.set_page_config(
    page_title="Data Model Review",
    page_icon="🤖",
    layout="wide",
)
st.title("Data Model Review")

st.markdown(
    """
    Paste your DTM (Data-model-to-model) XML/XSD code below to check its compliance against GDM standardization guidelines and get a general best-practices review.
    """
)

xml_input = st.text_area("Paste your DTM XML/XSD code here", height=400)

if "dtm_review_report" not in st.session_state:
    st.session_state.dtm_review_report = None

if st.button("Review Data Model", type="primary", use_container_width=True):
    if xml_input:
        st.session_state.dtm_review_report = None
        with st.spinner("Analyzing your data model... This may take a moment."):
            xml_content = xml_input
            
            response_str = generate_dtm_review(xml_content=xml_content)

            if response_str:
                try:
                    st.session_state.dtm_review_report = json.loads(response_str)
                    st.success("Review complete!")
                except json.JSONDecodeError:
                    st.error("Failed to parse the review report from the server.")
                    st.text_area("Raw Response", response_str, height=300)
            else:
                st.error("Failed to get a response from the review service.")
    else:
        st.warning("Please paste your XML/XSD code into the text area above.")

if st.session_state.dtm_review_report:
    report = st.session_state.dtm_review_report
    
    st.subheader("Review Report")
    
    # Display General Review
    if 'general_review' in report:
        st.subheader("General Best Practices Review")
        st.write(f"**Title:** {report['general_review'].get('report_title', 'N/A')}")
        st.write(f"**Overall Summary:**")
        st.markdown(report['general_review'].get('overall_summary', 'No summary provided.'))
        
        for entity_review in report['general_review'].get('entity_reviews', []):
            with st.expander(f"Entity: {entity_review.get('entity_name')}"):
                st.markdown(f"**Review Summary:** {entity_review.get('review_summary')}")
                st.markdown("**Suggestions:**")
                for suggestion in entity_review.get('suggestions', []):
                    st.markdown(f"- {suggestion}")

    # Display Standardization Report
    if 'standardization_report' in report:
        st.subheader("GDM Standardization Compliance Report")
        st.write(f"**Overall Summary:**")
        st.markdown(report['standardization_report'].get('report_summary', 'No summary provided.'))

        for entity_compliance in report['standardization_report'].get('entities', []):
            with st.expander(f"Entity: {entity_compliance.get('entity_name')}"):
                st.markdown(f"**Compliance:** {'✅ Compliant' if entity_compliance.get('is_compliant') else '❌ Non-Compliant'}")
                st.markdown(f"**Findings:** {entity_compliance.get('findings')}")
                
                st.markdown("**Attribute Compliance:**")
                for attr in entity_compliance.get('attributes', []):
                    st.markdown(f"---")
                    st.markdown(f"**Attribute:** `{attr.get('attribute_name')}`")
                    st.markdown(f"**Compliance:** {'✅ Compliant' if attr.get('is_compliant') else '❌ Non-Compliant'}")
                    st.markdown(f"**Findings:** {attr.get('findings')}")
    
    st.download_button(
        label="Download Full Report as JSON",
        data=json.dumps(report, indent=2),
        file_name="dtm_review_report.json",
        mime="application/json",
    )
