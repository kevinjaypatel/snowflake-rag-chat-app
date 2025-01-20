import streamlit as st # Import python packages
import numpy as np 
import pandas as pd
import json

from snowflake_setup import get_snowpark_session
from rag import RAG_from_scratch

from rag_feedback import f_context_relevance


pd.set_option("max_colwidth",None)

### Default Values
NUM_CHUNKS = 5 # Num-chunks provided as context. Play with this to check how it affects your accuracy
slide_window = 7 # How many last conversations to remember

# Connection to Snowflake
snowpark_session = get_snowpark_session() 

# RAG 
rag = RAG_from_scratch(snowpark_session=snowpark_session, num_chunks=NUM_CHUNKS)

### Functions
def config_options():

    st.sidebar.selectbox('Select your model:',(
                                    'mistral-large',
                                    'mistral-large2',
                                     'mistral-7b'), key="model_name")

    categories = snowpark_session.sql("select category from docs_chunks_table group by category").collect()
    
    cat_list = ['ALL']
    for cat in categories:
        cat_list.append(cat.CATEGORY)
            
    st.sidebar.selectbox('Select what chapter you are looking for', cat_list, key = "category_value")
    st.sidebar.checkbox('Enable chat history?', key="use_chat_history", value = True)
    st.sidebar.checkbox('Use documents as context?', key="use_rag" , value=True)
    st.sidebar.checkbox('Debug: Click to see summary generated of previous conversation', key="debug", value = True)
    st.sidebar.button("Start Over", key="clear_conversation", on_click=init_messages)
    st.sidebar.expander("Session State").write(st.session_state)

def init_messages():

    # Initialize chat history
    if st.session_state.clear_conversation or "messages" not in st.session_state:
        st.session_state.messages = []

def get_chat_history(): 
    #Get the history from the st.session_stage.messages according to the slide window parameter
    
    chat_history = []
    
    start_index = max(0, len(st.session_state.messages) - slide_window)
    for i in range (start_index , len(st.session_state.messages) -1):
         chat_history.append(st.session_state.messages[i])

    return chat_history

def summarize_question_with_history(chat_history, question):
    # To get the right context, use the LLM to first summarize the previous conversation
    # This will be used to get embeddings and find similar chunks in the docs for context

    prompt = f"""
        Based on the chat history below and the question, generate a query that extend the question
        with the chat history provided. The query should be in natural language. 
        Answer with only the query. Do not add any explanation.
        
        <chat_history>
        {chat_history}
        </chat_history>
        <question>
        {question}
        </question>
        """
    
    sumary = rag.generate_completion(prompt, st.session_state.model_name)   

    if st.session_state.debug:
        st.sidebar.text("Summary to be used to find similar chunks in the docs:")
        st.sidebar.caption(sumary)

    sumary = sumary.replace("'", "")

    return sumary

def create_prompt (myquestion):

    prompt = ""
    if st.session_state.use_chat_history:
        chat_history = get_chat_history()

        if chat_history != []: #There is chat_history, so not first question
            prompt = summarize_question_with_history(chat_history, myquestion)
        else:
            prompt = myquestion
             
    else:
        prompt = myquestion
    
    prompt_context = rag.retrieve_context(prompt)
    json_data = json.loads(prompt_context)

    relative_paths = set(item['relative_path'] for item in json_data['results'])

    return prompt, relative_paths
    
def answer_question(myquestion):
    try: 
        if st.session_state.use_chat_history:
            chat_history = get_chat_history()

            if chat_history != []: #There is chat_history, so not first question
                myquestion = summarize_question_with_history(chat_history, myquestion)

        prompt_context = None
        if st.session_state.category_value == "ALL":
            prompt_context = rag.retrieve_context(myquestion)
        else:
            filter_obj = {"@eq": {"category": st.session_state.category_value} }
            prompt_context = rag.retrieve_context(myquestion, filter_obj)

        relative_paths = []
        response = None
        context_relevance_score = f_context_relevance(prompt_context, myquestion)
        
        if context_relevance_score == 0:
            response = "I don't have the information to answer your question"
        else:         
            json_data = json.loads(prompt_context)
            relative_paths = set(item['relative_path'] for item in json_data['results'])
            response = rag.generate_completion_with_context(myquestion, prompt_context)
    
        return response, relative_paths
    except Exception as e:
        return "Error: " + str(e), "None"

def main():
    
    st.title(f":speech_balloon: Chat Document Assistant with Snowflake Cortex")
    st.write("This is the list of documents you already have and that will be used to answer your questions:")

    docs_available = snowpark_session.sql("ls @docs").collect()

    list_docs = []
    for doc in docs_available:
        list_docs.append(doc["name"])
    st.dataframe(list_docs)

    config_options()
    init_messages() 

    # Display chat messages from history on app rerun
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
    
    # Accept user input 
    if question := st.chat_input("What do you want to know about Rust?"):
        # Add user message to chat history
        st.session_state.messages.append({"role": "user", "content": question})
        # Display user message in chat message container
        with st.chat_message("user"):
            st.markdown(question)

        # Display assistant response in chat message container
        with st.chat_message("assistant"):
            message_placeholder = st.empty()
            question = question.replace("'","")

            with st.spinner(f"{st.session_state.model_name} thinking..."):
                    response, relative_paths = answer_question(question)            
                    response = response.replace("'", "")
                    message_placeholder.markdown(response)
    
                    if relative_paths != "None":
                        with st.sidebar.expander("Related Documents"):
                            for path in relative_paths:
                                cmd2 = f"select GET_PRESIGNED_URL(@docs, '{path}', 360) as URL_LINK from directory(@docs)"
                                df_url_link = snowpark_session.sql(cmd2).to_pandas()
                                url_link = df_url_link._get_value(0,'URL_LINK')
                    
                                display_url = f"Doc: [{path}]({url_link})"
                                st.sidebar.markdown(display_url)
        
        st.session_state.messages.append({"role": "assistant", "content": response})
        
                
if __name__ == "__main__":
    main()