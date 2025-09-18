from agents.shared_libraries.common_rag import query_rag_corpus
from google.adk.tools import FunctionTool

def  lbg_knowledge_base(query: str): 
    
    return query_rag_corpus('2305843009213693952', query_text=query)


def lbg_tdd_samples(query: str): 
    
    return query_rag_corpus('5148740273991319552', query_text=query)