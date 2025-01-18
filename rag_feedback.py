from rag import RAG_from_scratch
from snowflake_setup import get_snowpark_session
from trulens.providers.cortex.provider import Cortex
from trulens.connectors.snowflake import SnowflakeConnector
from trulens.apps.custom import TruCustomApp
from trulens.core import Feedback
from trulens.core import Select
from trulens.core import TruSession
import numpy as np

snowpark_session = get_snowpark_session()
conn = SnowflakeConnector(snowpark_session=snowpark_session)
session = TruSession(conn)

provider = Cortex(snowpark_session, "mistral-large2")

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

if __name__ == "__main__":
    try: 
        
        rag = RAG_from_scratch(snowpark_session)

        tru_rag = TruCustomApp(
            rag, 
            app_name="RAG",
            app_version="simple",
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

# search_text = "Show me how to create an object in Rust?"
        with tru_rag as recording:
            for prompt in prompts: 
            # response = rag.query(search_text)
                rag.query(prompt)
                # tru_rag.run()

        print(session.get_leaderboard())
    except Exception as e:
        print("Connection failed:", str(e))
    