USE ROLE SYSADMIN;
USE WAREHOUSE BERCA_WH;
USE DATABASE BERCA_TEST_DS;
USE SCHEMA GOLD;

CREATE OR REPLACE PROCEDURE GENERATE_AND_EXECUTE_SQL(USER_QUESTION VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10' -- Use 3.10 for modern packages
PACKAGES = ('snowflake-snowpark-python', 'pandas')
HANDLER = 'generate_sql_and_answer'
AS
$$
import json
from snowflake.snowpark import Session
from snowflake.snowpark.functions import lit

def get_semantic_model_context():
    # Placeholder for reading your massive YAML file.
    # In a real environment, you would store this YAML in a Snowflake Stage
    # or a dedicated table and fetch it here.
    
    # For demonstration, we will use a small sample of the schema.
    # YOU MUST REPLACE THIS WITH YOUR FULL, ESCAPED YAML CONTENT.
    return """
    tables:
        - name: AGG_DAILY_SALES_REGION
          description: ...daily sales by region...
          facts:
            - name: TOTAL_SALES_AMOUNT
              description: Total sales generated.
          time_dimensions:
            - name: SALES_DATE
        - name: DIM_CUSTOMER
          description: Customer attributes...
          dimensions:
            - name: REGION_NAME
    relationships:
        # Include your critical relationships here!
        ...
    """

def generate_sql_and_answer(session: Session, user_question: str) -> str:
    # 1. RETRIEVE SEMANTIC CONTEXT
    semantic_context = get_semantic_model_context()
    
    # 2. BUILD THE LLM PROMPT FOR SQL GENERATION
    sql_generation_prompt = f"""
    You are an expert Snowflake SQL generator.
    Your task is to convert the following user question into an accurate, runnable Snowflake SQL query.
    The query must only use the tables and columns provided in the SCHEMA CONTEXT below.
    Always use double quotes around table and column names.
    
    SCHEMA CONTEXT:
    ---
    {semantic_context}
    ---
    
    USER QUESTION: '{user_question}'
    
    Generated SQL:
    """

    # 3. CALL LLM TO GENERATE SQL (PLACEHOLDER)
    # This step requires calling Snowflake Cortex AI.
    # In a real Snowflake environment, you would use:
    # sql_query_result = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('llama2-70b-chat', '{sql_generation_prompt}')").collect()
    # generated_sql = sql_query_result[0][0]
    
    # *** SIMULATED LLM RESPONSE for demo purposes ***
    generated_sql = """
    SELECT
      REGION_NAME,
      AVG(AVG_ORDER_VALUE)
    FROM AGG_DAILY_SALES_REGION
    WHERE SALES_DATE = '2025-11-01'::DATE
    GROUP BY REGION_NAME
    """
    # **********************************************
    
    try:
        # 4. EXECUTE THE GENERATED SQL
        df_result = session.sql(generated_sql).to_pandas()
        
        # 5. BUILD THE LLM PROMPT FOR RESULT FORMATTING
        result_formatting_prompt = f"""
        The user asked: '{user_question}'.
        The SQL result in JSON format is: {df_result.to_json()}
        
        Please summarize this data into a conversational, non-technical sentence or paragraph.
        """
        
        # 6. CALL LLM TO FORMAT ANSWER (PLACEHOLDER)
        # This step also uses Snowflake Cortex AI.
        
        # *** SIMULATED LLM RESPONSE for demo purposes ***
        formatted_answer = "The average order value by region on November 1st, 2025 was highest in the AMERICA region at \$2,500."
        # **********************************************
        
        return formatted_answer
        
    except Exception as e:
        return f"Error executing SQL: {generated_sql}. Error message: {str(e)}"
$$;

USE SCHEMA PUBLIC;

CREATE OR REPLACE STAGE STREAML_STAGE
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

CREATE OR REPLACE STREAMLIT BERCA_TEST_DS.PUBLIC.CHATBOT_AGENT
  ROOT_LOCATION = '@BERCA_TEST_DS.PUBLIC.STREAML_STAGE' -- Use a stage to hold the Python file
  MAIN_FILE = 'chatbot_app.py';

USE SCHEMA GOLD;