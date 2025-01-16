import os
import json
from snowflake.core import Root
from typing import List
from snowflake_setup import get_snowpark_session
from snowflake.snowpark.session import Session

class CortexSearchRetriever:

    def __init__(self, snowpark_session: Session, limit_to_retrieve: int = 4):
        self._snowpark_session = snowpark_session
        self._limit_to_retrieve = limit_to_retrieve

    def retrieve(self, query: str) -> List[str]:
        root = Root(self._snowpark_session)
        cortex_search_service = (
            root.databases[os.getenv("SNOWFLAKE_DATABASE")]
            .schemas[os.getenv("SNOWFLAKE_SCHEMA")]
            .cortex_search_services[os.getenv("SNOWFLAKE_CORTEX_SEARCH_SERVICE")]
        )
        resp = cortex_search_service.search(
            query=query,
            columns=["chunk", "relative_path", "category"],
            limit=self._limit_to_retrieve,
        )

        if resp.results:
            return [curr["chunk"] for curr in resp.results]
        else:
            return []
   
if __name__ == "__main__":
    try: 
        session = get_snowpark_session()
        retriever = CortexSearchRetriever(session)
        query = "What is the rust programming language?"
        retrieved_context = retriever.retrieve(query)

        print(len(retrieved_context))

    except Exception as e:
        print("Connection failed:", str(e))