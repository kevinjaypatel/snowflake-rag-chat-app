from rag import RAG_from_scratch
from snowflake_setup import get_snowpark_session
from trulens.providers.cortex.provider import Cortex
from trulens.connectors.snowflake import SnowflakeConnector
from trulens.apps.custom import TruCustomApp, instrument
from trulens.core import Feedback, Select, TruSession
from trulens.core.guardrails.base import context_filter
from trulens.dashboard import run_dashboard
import numpy as np 

snowpark_session = get_snowpark_session()

# Connection to TruLens
tru_snowflake_connector = SnowflakeConnector(snowpark_session=snowpark_session)
tru_session = TruSession(tru_snowflake_connector)

# LLM as a judge evaluations 
provider = Cortex(snowpark_session, "mistral-large2")

# RAG Feedback Functions 
f_groundedness = (
    Feedback(provider.groundedness_measure_with_cot_reasons, name="Groundedness")
    .on(Select.RecordCalls.retrieve_context.rets[:].collect())
    .on_output()
)

f_context_relevance = (
    Feedback(provider.context_relevance, name="Context Relevance")
    .on_input()
    .on(Select.RecordCalls.retrieve_context.rets[:])
    .aggregate(np.mean)
)

f_answer_relevance = (
    Feedback(provider.relevance, name="Answer Relevance")
    .on_input()
    .on_output()
    .aggregate(np.mean)
)

# Add guardrails for context relevance
f_context_relevance_score = Feedback(
    provider.context_relevance, name="Context Relevance"
)

class filtered_RAG_from_scratch(RAG_from_scratch):
    @instrument
    @context_filter(f_context_relevance_score, 0.75, keyword_for_prompt="query")
    def retrieve_context(self, query: str) -> list:
        """
        Retrieve relevant text from vector store.
        """
        return self.retriever.retrieve(query)

if __name__ == "__main__":
    try: 
        
        rag = RAG_from_scratch(snowpark_session)
        filtered_rag = filtered_RAG_from_scratch(snowpark_session)

        tru_rag = TruCustomApp(
            rag, 
            app_name="RAG",
            app_version="simple",
            feedbacks=[f_groundedness, f_context_relevance, f_answer_relevance],
        )

        tru_filtered_rag = TruCustomApp(
            filtered_rag, 
            app_name="RAG",
            app_version="filtered",
            feedbacks=[f_groundedness, f_context_relevance, f_answer_relevance],
        )

        prompts = [
            "How can I define a library in Rust and use it inside an executable?",
            "Can I have a library and an executable inside a rust package?",
            "What is the difference between packages and crates?",
            "How can I write both unit tests and integrations tests in Rust?"
            "How can I ignore a test?",
            "What is ownership in Rust?",
            "How can I parse a CLI command?",
            "How can I write a CLI tool? Are there any libraries I can use to help me do this?",
        ]

        with tru_rag as recording:
            for prompt in prompts: 
                rag.query(prompt)

        with tru_filtered_rag as recording:
            for prompt in prompts: 
                filtered_rag.query(prompt)

        # print(tru_session.get_leaderboard())
        run_dashboard(tru_session)
    except Exception as e:
        print("Connection failed:", str(e))
    