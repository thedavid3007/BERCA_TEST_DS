import streamlit as st
from snowflake.snowpark.context import get_active_session

# --- Table Mapping ---
TABLE_MAPPING = {
    "sales_forecast": {"schema": "GOLD", "table": "AI_SALES_PREDICTION"},
    "daily_sales": {"schema": "SILVER", "table": "AGG_DAILY_SALES_REGION"},
    "churn_alert": {"schema": "GOLD", "table": "V_CHURN_RISK_ALERT"},
}

# --- Helper Function ---
def get_table_name(logical_name):
    """
    Returns the fully qualified table name for a logical table name.
    Example: 'sales_forecast' -> 'GOLD.AI_SALES_PREDICTION'
    """
    mapping = TABLE_MAPPING.get(logical_name)
    if not mapping:
        raise ValueError(f"Table mapping not found for '{logical_name}'")
    return f"{mapping['schema']}.{mapping['table']}"

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