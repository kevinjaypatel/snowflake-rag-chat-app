from trulens.apps.custom import instrument
from search import CortexSearchRetriever
from snowflake_setup import get_snowpark_session
from snowflake.snowpark.session import Session
from snowflake.cortex import Complete

class RAG_from_scratch:

    def __init__(self, snowpark_session: Session):
        self.retriever = CortexSearchRetriever(snowpark_session=snowpark_session, limit_to_retrieve=4)

    @instrument
    def retrieve_context(self, query: str) -> list:
        """
        Retrieve relevant text from vector store.
        """
        return self.retriever.retrieve(query)

    @instrument
    def generate_completion(self, query: str, context_str: list) -> str:
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
        return Complete("mistral-large", prompt)

    @instrument
    def query(self, query: str) -> str:
        context_str = self.retrieve_context(query)
        return self.generate_completion(query, context_str)

if __name__ == "__main__":
    try: 
        session = get_snowpark_session()
        rag = RAG_from_scratch(session)
        query = "What is the rust programming language?"
        answer = rag.query(query)
        print(answer)
    except Exception as e:
        print("Connection failed:", str(e))
