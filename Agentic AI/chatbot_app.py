import streamlit as st
from snowflake.snowpark.context import get_active_session

# Get the active Snowpark session
session = get_active_session()

# --- Streamlit UI ---
st.set_page_config(layout="wide")
st.title("❄️ Semantic Model Data Chatbot")
st.caption("Powered by Snowpark and your Cortex Analyst Semantic Model.")

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat messages from history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# User input
if prompt := st.chat_input("Ask a question about sales, sentiment, or customer data..."):
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Thinking... Generating SQL..."):
            
            try:
                # Call the Stored Procedure (the core engine we created)
                # The result is the conversational answer
                result_df = session.call('GENERATE_AND_EXECUTE_SQL', prompt)
                
                response = result_df
                
            except Exception as e:
                response = f"Sorry, an error occurred during processing: {e}"
        
        st.markdown(response)
        st.session_state.messages.append({"role": "assistant", "content": response})