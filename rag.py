from trulens.apps.custom import instrument
from search import CortexSearchRetriever

from snowflake.snowpark.session import Session
from snowflake.cortex import Complete
from trulens.apps.custom import instrument

class RAG_from_scratch:

    def __init__(self, snowpark_session: Session, num_chunks: int = 4):
        self.retriever = CortexSearchRetriever(snowpark_session=snowpark_session, limit_to_retrieve=num_chunks)

    @instrument
    def retrieve_context(self, query: str, filter_obj: dict = None) -> dict:
        """
        Retrieve relevant text from vector store.
        """
        return self.retriever.retrieve(query, filter_obj)

    @instrument
    def generate_completion_with_context(self, query: str, context_str: dict, model_name: str = "mistral-large2") -> str:
        """
        Generate answer from context.
        """
        
        prompt = f"""
          You are an expert assistant extracting information from context provided.
          Answer the question based on the context. Be concise and do not hallucinate.
          If you don´t have the information just say so.
          Context: {context_str}
          Question:
          {query}
          Answer:
        """
        return Complete(model_name, prompt)
    
    @instrument
    def generate_completion(self, prompt: str, model_name: str = "mistral-large2") -> str:
        """
        Generate answer from prompt.
        """
        return Complete(model_name, prompt)

    
    @instrument
    def query(self, query: str) -> str:
        context_str = self.retrieve_context(query)
        return self.generate_completion_with_context(query, context_str)

if __name__ == "__main__":
    try: 
        from snowflake_setup import get_snowpark_session
        session = get_snowpark_session()
        rag = RAG_from_scratch(session)
        query = "What is the rust programming language?"
        answer = rag.query(query)
        print(answer)
    except Exception as e:
        print("Connection failed:", str(e))
